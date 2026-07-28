# Phase 0 Reflection — Environment Setup

## What this phase set up
- Python virtual environment + pinned `requirements.txt`
- Project folder scaffold
- Git repo on branch `phase-0-setup`
- Config/secrets pattern: `.env` (gitignored) + `.env.example` (committed template)
- Docker Compose stack with its first service: MinIO object storage

## Key concepts (in my own words)

**venv vs requirements.txt.** The venv is an isolated interpreter + packages, huge and
OS-specific, so it's gitignored. `requirements.txt` is the *recipe* to rebuild it exactly —
small, portable, committed. Reproducibility, not compression, is the point.

**Config vs secrets, and 12-Factor.** Config = per-environment values (broker address, topic,
log level). Secrets = sensitive values (API keys, passwords). Neither is hardcoded; both come
from the environment. Locally that's a `.env` file; in production the platform (Kubernetes,
ECS, Airflow Connections, a secrets manager) injects the same env vars. Same code artifact,
different injected environment. `.env` is gitignored; `.env.example` (fake values) is
committed so teammates know which vars to set. The `!.env.example` negation in `.gitignore`
keeps the template while ignoring the real file.

**Docker: image vs container.** An image is a read-only template (built from a Dockerfile);
a container is a running instance of an image. Many containers can come from one image.

**Docker client vs daemon.** `docker` CLI (client) just sends requests; `dockerd` (daemon,
inside Docker Desktop) does the work. `docker --version` works with the daemon off, but
`docker run` fails until Docker Desktop is running.

**Container networking — host vs internal.** `localhost` means "the machine I'm on," and each
container is its own machine. From the host (browser) I reach MinIO at `localhost:9001` via a
published port (`ports: HOST:CONTAINER`). Between containers I use the **service name**
(`minio:9000`), because a container's own `localhost` points only at itself. Hardcoding
`localhost` in app config is the classic bug — works on the laptop, breaks in a container.

**Volumes / persistence.** Containers are ephemeral: deleting one deletes its internal
filesystem. A **named volume** stores data outside the container lifecycle, so `docker compose
down` then `up` keeps the data. `docker compose down -v` deletes the volume and the data.
This is how stateful services (databases, object stores) keep data across restarts.

## Interview one-liners
- Image = mold, container = what comes out of it.
- Config in the environment (12-Factor) → one artifact runs in every environment.
- Host → `localhost:PUBLISHED_PORT`; container → `service-name:INTERNAL_PORT`.
- Containers ephemeral; volumes persist; `-v` on `down` wipes them.
