# Rucio Integration (opt-in)

The panda-compose stack ships two Harvester plugins under
`config/harvester/plugins/`:

- `docker_submitter.py` — runs each PanDA worker as a Docker container.
- `rucio_stager.py` — after each container exits, uploads output
  files to a Rucio dev stack, attaches them to the destination
  dataset, and hands the resulting metadata back to panda-server so
  the Adder can archive the job.

Neither plugin is wired into the default `panda_queues.cfg`: fresh
installs still use `DummyStager` so the stack starts cleanly without
any Rucio configured. This page walks through the opt-in.

## Prerequisites

- A running Rucio dev stack. The rucio/rucio-dev image and its
  compose file provide a fully-functional Rucio server, an
  Auth/Console, and a MOCK-POSIX RSE that writes to `/tmp/rucio_rse`
  on the host. Confirm the Docker network is up:
  ```bash
  docker network ls | grep rucio
  ```
  The default network name is `ruciodevnetwork` — if yours differs,
  adjust the override file below.

- The `panda-dev-user` Rucio account exists and is registered with a
  scope you want the RucioStager to write to (default `user.hermes`
  — change `defaultScope` in the queue config to your own scope).

## Step 1 — Rucio client config

Create `config/rucio/rucio.cfg`:

```ini
[client]
rucio_host = https://rucio:443
auth_host = https://rucio:443
auth_type = userpass
username = ddmlab
password = secret
account = root
ca_cert = /opt/rucio/etc/rucio_ca.pem
```

**Important**: This config is HTTP-only (no `[database]` section). The
`[database]` section is for server-side only; clients must use the
HTTPS API.

Copy the Rucio dev stack's self-signed CA cert next to it, then create
the OpenSSL hash symlink OpenSSL requires for `X509_CERT_DIR` lookups:

```bash
cp path/to/rucio_ca.pem config/rucio/rucio_ca.pem
h=$(openssl x509 -hash -noout -in config/rucio/rucio_ca.pem)
ln -sf rucio_ca.pem config/rucio/${h}.0
```

**Critical**: The hash symlink (e.g., `5fca1cb1.0`) must be installed
into `/etc/grid-security/certificates/` in the container because
panda-jedi's environment sets `X509_CERT_DIR=/etc/grid-security/certificates`
which takes precedence over the config file's `ca_cert` setting. The
example override handles this via a volume mount.

## Step 2 — Copy the override example

```bash
cp docker-compose.override.example.yml docker-compose.override.yml
```

Compose picks up `docker-compose.override.yml` automatically. Read the
file's header for what it does and adjust the network name if
your Rucio dev stack uses a different one.

## Step 3 — Harvester bootstrap (in docker-compose.yml)

The harvester service's bootstrap command must:
1. Install `python3.11`, `python3.11-libs`, `python3.11-devel`, `sqlite-devel` (for `_sqlite3` module)
2. Copy the system `_sqlite3` module to `/usr/local/lib/python3.11/lib-dynload/`
3. Install `rucio-clients`, `apsw`, `docker` via pip
4. Use the system Python 3.11 (`/usr/local/bin/python3.11`) which has the `_sqlite3` module
5. Set `PYTHONPATH` to include `/harvester/plugins` and `/usr/local/lib/python3.11/site-packages`

This is already configured in the base `docker-compose.yml`. See the
harvester service's `command` section.

## Step 4 — Enable RucioStager in the queue config

Edit `config/harvester/panda_queues.cfg`:

```json
"stager": {
    "name": "RucioStager",
    "module": "rucio_stager",
    "outputBaseDir": "/tmp/harvester_output",
    "rse": "MOCK-POSIX",
    "rucioAccount": "root",
    "defaultScope": "user.hermes"
}
```

Replace the shipped `DummyStager` block with the above.

Also ensure `submitter` is `DockerSubmitter` and `monitor` is `DockerMonitor`:

```json
"submitter": {
    "name": "DockerSubmitter",
    "module": "docker_submitter",
    "dockerImage": "alpine:latest",
    "dockerOptions": "--rm --network=host"
},
"monitor": {
    "name": "DockerMonitor",
    "module": "docker_monitor"
}
```

## Step 5 — Bring the stack back up

```bash
docker compose up -d
```

Submit a test task with `prun`. Output files should appear in Rucio:

```bash
# In the Rucio dev container:
rucio did list 'user.hermes:*rucioout.*'
```

And on the RSE:

```bash
find /tmp/rucio_rse/user/hermes -type f
```

## Design notes

See the docstring in `config/harvester/plugins/rucio_stager.py` for
the plugin's design tradeoffs and gotchas — most notably why it
authenticates as `root` in the local dev setup and why it uploads
one file at a time instead of in bulk.

## Troubleshooting

### "No module named 'rucio'"
The harvester bootstrap didn't install `rucio-clients`. Check the
harvester logs for the pip install step.

### "SSL: CERTIFICATE_VERIFY_FAILED"
The Rucio CA hash symlink is missing from `/etc/grid-security/certificates/`
in the container. Verify the override mounts `config/rucio/5fca1cb1.0`
to that path.

### "no such table: rses"
The Rucio client config has a `[database]` section pointing to SQLite.
Remove the `[database]` section — clients must use HTTP API only.

### Jobs stay in `activated` / harvester doesn't fetch
- Check `panda_harvester.cfg` has correct `server_api_url = http://panda-compose-panda-server-1:80/api/v1`
- Verify panda-server is listening on port 80 (not 25080 inside container)
- Check harvester logs: `docker logs panda-compose-harvester-1`

### RucioStager can't find output files
The `DockerSubmitter` writes output to `/tmp/harvester_output/worker-<ID>/`.
Ensure `outputBaseDir` in queue config matches.