"""
RucioStager — Harvester stager plugin that uploads worker output to Rucio.

Runs after the worker container exits (Harvester's stager loop). For each
output file listed in the job spec, this plugin:

  1. Reads the file from the per-worker host directory populated by the
     DockerSubmitter (default /tmp/harvester_output/worker-<workerID>/).
  2. Computes real fsize and adler32.
  3. Uploads via rucio.client.uploadclient.UploadClient — this writes the
     bytes to the RSE and creates the file DID under the file's scope.
  4. Attaches the file to the output/log dataset (the destinationDBlock the
     PanDA server pilot API expects to be closed later).
  5. Builds the job_output_report JSON that the panda-server Adder expects
     and stores it on the jobspec as `outputFilesToReport`. Harvester's
     PandaCommunicator then ships it to panda-server automatically in the
     next `update_jobs_bulk` call, which triggers `AdderGen.dump_file_report`
     server-side and the row lands in doma_panda.job_output_report with no
     manual DB writes.

Log files are synthesized — the trivial container commands panda-compose
runs don't produce a real pilot log.tgz, so we write a minimal stub tarball
with the container-run metadata (PandaID, taskID, output file list).

Design notes
------------
* This plugin authenticates as `rucioAccount` (default "root") using the
  credentials in the container's rucio.cfg. In panda-compose's local Rucio
  dev stack, only "root" has add_replica permission on MOCK-POSIX. In a
  real deployment the pilot's own credentials would be used.
* Rucio's `UploadClient.upload([...])` fails the whole batch on any single
  duplicate DID; this plugin uploads items one at a time and skips DIDs
  that already exist so retries are idempotent.
* Harvester fetches the JobSpec in slim mode at stager time, so
  `jobParams` may be None. The plugin uses the pre-extracted attrs
  `jobParamsExtForOutput` and `jobParamsExtForLog` instead.
* JEDI conventions leak two annoying properties into `jobParamsExtForOutput`:
  the scope may arrive as "scope/dataset_name" (glued), and dataset names
  carry a trailing "/". Both are normalized before the Rucio calls.

Queue config example:

    "stager": {
        "name": "RucioStager",
        "module": "rucio_stager",
        "outputBaseDir": "/tmp/harvester_output",
        "rse": "MOCK-POSIX",
        "rucioAccount": "root",
        "defaultScope": "user.hermes"
    }

To opt in, change `stager` in panda_queues.cfg to point here. The shipped
default in panda-compose is still `DummyStager` so a fresh install runs
without any Rucio configured; RucioStager only comes online when you also
have a working Rucio dev stack (e.g. rucio/rucio-dev) reachable from the
harvester container and a rucio.cfg mounted at /opt/rucio/etc/rucio.cfg.
"""

import io
import json
import os
import tarfile
import time
import uuid
import zlib

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvesterstager.base_stager import BaseStager


baseLogger = core_utils.setup_logger("rucio_stager")


def _adler32(path):
    a = 1
    with open(path, "rb") as f:
        while True:
            chunk = f.read(64 * 1024)
            if not chunk:
                break
            a = zlib.adler32(chunk, a)
    return f"{a & 0xFFFFFFFF:08x}"


def _fsize(path):
    return os.path.getsize(path)


class RucioStager(BaseStager):
    def __init__(self, **kwarg):
        self.outputBaseDir = "/tmp/harvester_output"
        self.rse = "MOCK-POSIX"
        # Rucio account used to write to storage. In prod the pilot uses a
        # service account with add_replica permission on the RSE; here we
        # default to the admin account 'root' since the local Rucio dev stack
        # only grants replica-write to root (the panda-dev-user is used as the
        # scope owner but has no RSE write permission).
        self.rucioAccount = "root"
        self.defaultScope = "user.hermes"
        BaseStager.__init__(self, **kwarg)

    # ------------------------------------------------------------------ helpers
    def _worker_output_dir(self, jobspec):
        # Prefer whatever the submitter recorded on the workspec…
        try:
            for wspec in jobspec.get_workspec_list() or []:
                ok, val = wspec.get_work_params("outputHostDir")
                if ok and val:
                    return val
        except Exception:
            pass
        # …else fall back to the workerID-based convention.
        worker_id = None
        try:
            worker_id = jobspec.get_workspec_list()[0].workerID
        except Exception:
            pass
        if worker_id is None:
            return None
        return os.path.join(self.outputBaseDir, f"worker-{worker_id}")

    def _make_log_tgz(self, out_path, jobspec, container_output_dir):
        """Synthesize a minimal panda-style log tarball so the log DID is real
        rather than a placeholder."""
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            info = tarfile.TarInfo(name="job.log")
            log_bytes = (
                f"PandaID={jobspec.PandaID}\n"
                f"taskID={jobspec.taskID}\n"
                f"workerOutput={container_output_dir}\n"
                f"files={[fs.lfn for fs in jobspec.outFiles]}\n"
                f"generated_by=rucio_stager at {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n"
            ).encode()
            info.size = len(log_bytes)
            tar.addfile(info, io.BytesIO(log_bytes))
        data = buf.getvalue()
        with open(out_path, "wb") as f:
            f.write(data)

    def _split_scope(self, lfn):
        """Return (scope, name). ATLAS user LFN convention: user.<name>.* → user.<name>."""
        if lfn.startswith("user."):
            parts = lfn.split(".", 2)
            if len(parts) >= 2:
                return (f"{parts[0]}.{parts[1]}", lfn)
        return (self.defaultScope, lfn)

    # ----------------------------------------------------------- BaseStager API
    def trigger_stage_out(self, jobspec):
        """Actually upload files. Called once per job when the worker finishes."""
        tmpLog = self.make_logger(
            baseLogger, f"PandaID={jobspec.PandaID}", method_name="trigger_stage_out"
        )
        try:
            from rucio.client.uploadclient import UploadClient
            from rucio.client.client import Client as RucioClient
        except Exception as exc:
            msg = f"rucio-clients not importable: {exc}"
            tmpLog.error(msg)
            return False, msg

        # Build a Rucio client bound to our account (the config's default
        # account is 'root', which lacks the write scope).
        try:
            rucio_client = RucioClient(account=self.rucioAccount, ca_cert="/opt/rucio/etc/ca.crt")
        except Exception as exc:
            msg = f"Rucio Client init failed: {exc}"
            tmpLog.error(msg)
            return False, msg

        worker_output = self._worker_output_dir(jobspec)
        if not worker_output or not os.path.isdir(worker_output):
            msg = f"worker output dir not found: {worker_output!r}"
            tmpLog.error(msg)
            return False, msg
        tmpLog.debug(f"worker output dir: {worker_output}")

        # For JEDI-managed jobs, Harvester's FileSpec list is empty; the
        # authoritative output file information lives in jobspec.jobParams.
        try:
            file_plan = self._build_file_plan(jobspec, tmpLog)
        except Exception as exc:
            msg = f"could not derive output file plan: {exc}"
            tmpLog.error(msg)
            return False, msg

        if not file_plan:
            tmpLog.warning("no output files declared for this job")
            return False, "no output files declared"

        upload_items = []
        report = {}

        for entry in file_plan:
            lfn = entry["lfn"]
            file_type = entry["type"]
            scope = entry["scope"]
            dataset_name = entry["dataset"]

            local_path = self._locate_or_make_file(
                worker_output, lfn, file_type, jobspec, tmpLog
            )
            if not local_path or not os.path.exists(local_path):
                tmpLog.error(f"no local file for lfn={lfn} type={file_type}")
                continue

            size = _fsize(local_path)
            adler = _adler32(local_path)
            file_guid = str(uuid.uuid4()).upper()

            item = {
                "path": local_path,
                "rse": self.rse,
                "did_scope": scope,
                "did_name": lfn,
                "guid": file_guid,
                "no_register": False,
                "register_after_upload": True,
                "lifetime": None,
            }
            if dataset_name:
                item["dataset_scope"] = scope
                item["dataset_name"] = dataset_name
            upload_items.append(item)

            report[lfn] = {
                "guid": file_guid,
                "fsize": size,
                "adler32": adler,
                "surl": f"file://localhost{os.path.abspath(local_path)}",
            }

        if not upload_items:
            tmpLog.warning("no files to upload; treating job as failed to stage")
            return False, "no output files"

        # First, ensure output datasets exist in Rucio so that UploadClient can
        # attach uploaded files to them.
        try:
            unique_datasets = {(e["scope"], e["dataset"]) for e in file_plan if e["dataset"]}
            self._ensure_datasets(rucio_client, unique_datasets, tmpLog)
        except Exception as exc:
            tmpLog.warning(f"dataset pre-registration warning: {exc}")

        # Upload one file at a time so a single failure (typically a duplicate
        # DID from a re-run) does not fail the whole job.
        from rucio.common.exception import DataIdentifierNotFound
        uc = UploadClient(_client=rucio_client, logger=None)
        n_ok = 0
        n_skip = 0
        for it in upload_items:
            try:
                # Skip if already registered.
                try:
                    rucio_client.get_metadata(scope=it["did_scope"], name=it["did_name"])
                    tmpLog.debug(f"{it['did_scope']}:{it['did_name']} already in Rucio; skip upload")
                    n_skip += 1
                    continue
                except DataIdentifierNotFound:
                    pass
                ret = uc.upload([it])
                tmpLog.debug(f"uploaded {it['did_scope']}:{it['did_name']} (ret={ret})")
                n_ok += 1
            except Exception as exc:
                tmpLog.error(
                    f"upload failed for {it['did_scope']}:{it['did_name']}: "
                    f"{type(exc).__name__} {exc}"
                )
                return False, f"upload failed for {it['did_name']}: {exc}"

        tmpLog.debug(f"upload summary: uploaded={n_ok} skipped={n_skip}")

        # Stash the report on the jobspec. PandaCommunicator.update_jobs will
        # ship it as job_output_report to panda-server on the next final-status
        # update.
        jobspec.outputFilesToReport = json.dumps(report)
        tmpLog.debug(f"outputFilesToReport set for {len(report)} files")
        return True, ""

    def check_stage_out_status(self, jobspec):
        """Synchronous upload above → always report finished here."""
        for fs in jobspec.get_output_file_specs(skip_done=True):
            fs.status = "finished"
        return True, ""

    # ---------------------------------------------------------------- internals
    def _build_file_plan(self, jobspec, tmpLog):
        """Parse jobspec's output attributes into a list of dicts, one per file:

            [{"lfn": ..., "type": "output"|"log", "scope": ..., "dataset": ...}, ...]

        The stager fetches JobSpec in slim mode; jobParams may be None. Use the
        pre-computed jobParamsExtForOutput and jobParamsExtForLog which are
        populated by db_proxy for every job.
        """
        plan = []

        # jobParamsExtForOutput = {lfn: {scope, dataset, endpoint}}
        out_ext = None
        try:
            out_ext = jobspec.get_output_file_attributes()
        except Exception:
            out_ext = None
        # jobParamsExtForLog = {"lfn": ..., "guid": ...}
        log_ext = None
        try:
            log_ext = jobspec.get_logfile_info()
        except Exception:
            log_ext = None
        log_lfn = (log_ext or {}).get("lfn")

        if out_ext:
            for lfn, meta in out_ext.items():
                # jobParamsExtForOutput scope may come from JEDI as
                # "user.hermes:user.hermes.rucioout.NNN_out.txt" (scope:name
                # combined). Rucio's actual scope is only the prefix before ":".
                raw_scope = meta.get("scope") or ""
                # Parse scope from DID format (scope:name or scope/name) if present
                if ":" in raw_scope:
                    scope = raw_scope.split(":", 1)[0]
                elif "/" in raw_scope:
                    scope = raw_scope.split("/", 1)[0]
                elif raw_scope and raw_scope != "NULL":
                    scope = raw_scope
                else:
                    scope = self._split_scope(lfn)[0]
                if not scope or scope == "NULL":
                    scope = self._split_scope(lfn)[0]
                dataset = meta.get("dataset")
                if dataset in (None, "", "NULL"):
                    dataset = None
                # JEDI convention appends a trailing "/" to dataset container
                # names; Rucio expects the bare name. Strip it.
                if dataset:
                    dataset = dataset.rstrip("/")
                ftype = "log" if lfn == log_lfn or lfn.endswith(".log.tgz") else "output"
                plan.append({"lfn": lfn, "type": ftype, "scope": scope, "dataset": dataset})

        # Final fallback: parse raw jobParams if it's present
        if not plan and jobspec.jobParams:
            params = jobspec.jobParams
            outFiles_str = params.get("outFiles") or ""
            destDblock_str = params.get("destinationDblock") or ""
            logFile = params.get("logFile") or ""
            out_lfns = [x for x in outFiles_str.split(",") if x]
            dblocks = [x for x in destDblock_str.split(",") if x]
            for i, lfn in enumerate(out_lfns):
                ds = dblocks[i] if i < len(dblocks) else None
                if ds == "NULL":
                    ds = None
                scope, _ = self._split_scope(lfn)
                if not scope or scope == "NULL":
                    scope = self.defaultScope
                ftype = "log" if lfn == logFile or lfn.endswith(".log.tgz") else "output"
                plan.append({"lfn": lfn, "type": ftype, "scope": scope, "dataset": ds})

        tmpLog.debug(f"file plan: {plan}")
        return plan

    def _ensure_datasets(self, rucio_client, scope_dataset_pairs, tmpLog):
        """Create the Rucio datasets referenced by destinationDBlock so that
        UploadClient can attach uploaded files to them.

        scope_dataset_pairs is an iterable of (scope, dataset_name) tuples.
        """
        from rucio.common.exception import DataIdentifierAlreadyExists

        for scope, ds_name in scope_dataset_pairs:
            try:
                rucio_client.add_dataset(scope=scope, name=ds_name)
                tmpLog.debug(f"created dataset {scope}:{ds_name}")
            except DataIdentifierAlreadyExists:
                pass
            except Exception as exc:
                tmpLog.debug(f"add_dataset {scope}:{ds_name} → {exc}")

    def _locate_or_make_file(self, worker_output, lfn, file_type, jobspec, tmpLog):
        """Find the physical file for `lfn` under `worker_output`, or create it.

        For type=='output', we accept:
          * exact LFN match, or
          * a file whose extension matches (e.g. lfn ends in .out.txt and worker
            wrote out.txt), or
          * the pattern from jobParams["outputs"].

        For type=='log', we synthesize a minimal .tgz.
        """
        candidates = os.listdir(worker_output) if os.path.isdir(worker_output) else []

        # Exact
        for name in candidates:
            if name == lfn:
                return os.path.join(worker_output, name)

        # Match by "short" name.  ATLAS output LFNs have the form
        # user.<username>.NNN._000001.out.txt — the user's exec produced
        # something like out.txt.  Match by tail token.
        for name in candidates:
            if lfn.endswith(name) or lfn.endswith("." + name):
                path = os.path.join(worker_output, name)
                # rename to the LFN so the upload path is clean
                dest = os.path.join(worker_output, lfn)
                try:
                    os.rename(path, dest)
                    return dest
                except OSError:
                    return path

        if file_type == "log" or lfn.endswith(".log.tgz"):
            dest = os.path.join(worker_output, lfn)
            self._make_log_tgz(dest, jobspec, worker_output)
            tmpLog.debug(f"synthesized log tarball at {dest}")
            return dest

        tmpLog.error(
            f"could not locate file for lfn={lfn} in {worker_output} "
            f"(candidates={candidates})"
        )
        return None
