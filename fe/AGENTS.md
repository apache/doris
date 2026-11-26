# AGENTS.md — Apache Doris FE

## Build Instructions

### 0. Verify protoc executable

Ensure `thirdparty/installed/bin/protoc` exists and is executable. If it does not exist, **stop the build** and prompt the user to download the thirdparty libraries first.

### 1. Generate sources

Run from the repository root:

```bash
sh generated-source.sh
```

### 2. Build FE

```bash
cd fe && mvn clean install -DskipTests -Dskip.doc=true -T 1C
```

## Connector Framework

For work under `fe-connector/` (connector plugins / external catalogs), start
with `fe-connector/README.md` (architecture, adding a new connector) and
`fe-connector/AGENTS.md` (build/test recipes, gates, invariants).

## Authorization Framework

For work under `fe-authorization/` (authorization sources / access control),
start with `fe-authorization/README.md` (architecture, adding a new
authorization plugin) and `fe-authorization/AGENTS.md` (build/test recipes,
obligations, invariants). `fe-authorization/fe-authorization-spi/README.md` is
the plugin author's quickstart.
