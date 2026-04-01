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
