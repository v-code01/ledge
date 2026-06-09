# Dogfood: Ledge hosting Ledge

The system that replaces git, hosting its own source code — deployed, verified, running.

```sh
bash dogfood/selfhost.sh
```

This (1) deploys a **persistent self-hosted Ledge** instance, (2) makes Ledge **import its
own source** (via the git-sync feature), (3) **clones that source back out of Ledge** and
asserts the HEAD commit SHA-1 is byte-identical, then leaves the instance running.

Latest verified run — **5 PASS / 0 FAIL** ([`results/2026-06-09.txt`](results/2026-06-09.txt)):
the cloned-back HEAD (`bf3f7e4e…`) equaled the host source HEAD exactly. Ledge is serving
its own 244-commit source over the git wire protocol.

## What it dogfoods

- **Deploy artifacts (Phase 4e):** the `ledge:latest` Docker image, a compose service with a
  named volume + `restart: unless-stopped` + auth + the metrics/health port.
- **Git remote sync:** `POST /sync/import` ingests the Ledge source (`file:///srv/ledge-src`,
  the host repo mounted read-only) into a workspace — delta-safe (`git cat-file
  --batch-all-objects`), preserving commit SHA-1s.
- **The git server (Phase 1):** `git clone http://localhost:3030/ws/<id>` serves the source
  back, byte-identical.

## How "persistent" works (and its honest ceiling)

State lives in the named volume `ledge-selfhost-data`; the container is `restart:
unless-stopped`. So the instance — and the hosted source — **survive container restart,
Docker-daemon restart, and machine reboot**.

**It is NOT off-machine durable.** Everything is on this one SSD. If the disk dies, it's
gone. True durability needs a deploy on a *separate* host (the artifacts support it; out of
scope here). This dogfood proves "Ledge can host its own source persistently," not "your code
survives this laptop dying."

## A real limitation this surfaced (honest)

The script probes a raw `git push` of the source into Ledge — it **fails with HTTP 500**.
Ledge's `receive-pack` decoder (`ledge_git::push::decode_pack_objects`) handles only non-delta
objects, but a real `git push` of a multi-commit repo sends a *delta-compressed* pack. That's
why we ingest via **sync-import** (which delta-expands with `git cat-file`) rather than a raw
push. Making `receive-pack` accept delta packs (OFS/REF delta resolution) is a documented
follow-on — it would let you `git push` straight into Ledge and enable continuous self-hosting.

## Other honest notes

- **Snapshot, not continuous:** Ledge holds the source as of the import. Re-run to re-import a
  fresh snapshot; continuous self-hosting needs delta-receive-pack or a sync-on-commit hook
  (which could ride the webhook surface).
- **Workspace-hosted:** the source lives in a workspace with a 1-year lease (Ledge has no root
  durable git surface; the workspace is the unit). Renew/long-TTL keeps it alive.
- The bootstrap auth token in `docker-compose.yml` is a dev token — fine for a local dogfood,
  not for anything exposed.

## Manage the instance

```sh
docker compose -f dogfood/docker-compose.yml ps        # status
docker compose -f dogfood/docker-compose.yml logs -f   # logs
docker compose -f dogfood/docker-compose.yml down       # stop (keeps the volume/data)
docker compose -f dogfood/docker-compose.yml down -v    # stop + wipe the hosted source
```
