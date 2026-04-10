---
name: doris-docker-regression
description: Run Doris docker-based regression tests from a clean package
compatibility: opencode
---

## Purpose

Run Doris docker-based regression tests from a clean package, avoiding contamination from local `conf/` or startup script modifications.

## Prerequisites

### Python Environment

Requires Python 3. Install dependencies from `docker/runtime/doris-compose/requirements.txt`:

```bash
python -m pip install --user -r docker/runtime/doris-compose/requirements.txt
```

If installation fails (especially PyYAML conflicts), use specific versions:
```bash
python -m pip install --user pyyaml==5.3.1 docker==6.1.3
python -m pip install --user -r docker/runtime/doris-compose/requirements.txt
```

Alternatively, use a virtual environment:
```bash
python -m venv doris-compose-env
source doris-compose-env/bin/activate
pip install -r docker/runtime/doris-compose/requirements.txt
```

### Docker Environment

```bash
docker run hello-world       # Docker works
docker compose version       # Compose v2
docker-compose version       # Should resolve to Compose v2
```

If `docker-compose` shows `TypeError: kwargs_from_env()`, make sure it is indeed Docker Compose V2; if not, you need to install and point to the correct version.

Check port availability:
```bash
lsof -nP -iTCP:8030,8040,8050,8060,8070,9010,9020,9030,9050,9060 -sTCP:LISTEN
```
If there is a conflict, ask the user how to handle it.

## Build

```bash
./build.sh --fe --be -j<parallel_jobs> --output <output_directory>
```

## Prepare Clean Package

Sanitize configs from git HEAD:

```bash
git show HEAD:conf/fe.conf > <output_directory>/fe/conf/fe.conf
git show HEAD:conf/be.conf > <output_directory>/be/conf/be.conf
git show HEAD:bin/start_fe.sh > <output_directory>/fe/bin/start_fe.sh
chmod 755 <output_directory>/fe/bin/start_fe.sh
```

Verify clean configs use **default ports**:
- `fe.conf`: 8030, 9020, 9030, 9010, 8070
- `be.conf`: 9060, 8040, 9050, 8060, 8050
- `start_fe.sh`: no `-agentlib:jdwp` debug agent

## Build Docker Image

```bash
docker build \
  --build-arg OUTPUT_PATH=<output_directory> \
  -f docker/runtime/doris-compose/Dockerfile \
  -t <image_name>:latest \
  .
```

## Configure Regression

Create `regression-test/conf/regression-conf-custom.groovy`:

```groovy
image = "<image_name>:latest"
excludeDockerTest = false
testGroups = "docker"
```

## Run Regression

```bash
./run-regression-test.sh --run -d <directory> -s <suite_name>
```

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| 0 suites | `excludeDockerTest=true` or missing `testGroups="docker"` | Check `regression-conf-custom.groovy` |
| JDWP error | `start_fe.sh` has debug agent | Re-sanitize from `git show HEAD:bin/start_fe.sh` |
| Wrong ports | Configs have local edits | Re-sanitize from `git show HEAD:conf/fe.conf` / `be.conf` |
| Port conflict | Processes using default ports | `lsof` then kill |

## Debug Logs

Runtime cluster at `/tmp/doris/<suite_name>/`:
- `fe-1/log/fe.log`, `fe.warn.log`, `fe.out`, `health.out`
- `be-1/log/be.INFO`, `be.WARNING`, `be.out`, `health.out`
- `doris-compose.log`

## Full Command Sequence

```bash
# 1. Check environment
python --version
docker run hello-world
docker compose version
lsof -nP -iTCP:8030,8040,8050,8060,8070,9010,9020,9030,9050,9060 -sTCP:LISTEN

# 2. Build
./build.sh --fe --be -j60

# 3. Sanitize package configs
git show HEAD:conf/fe.conf > <output_directory>/fe/conf/fe.conf
git show HEAD:conf/be.conf > <output_directory>/be/conf/be.conf
git show HEAD:bin/start_fe.sh > <output_directory>/fe/bin/start_fe.sh
chmod 755 <output_directory>/fe/bin/start_fe.sh

# 4. Build image
docker build --build-arg OUTPUT_PATH=<output_directory> -f docker/runtime/doris-compose/Dockerfile -t <image_name>:latest .

# 5. Configure (create regression-conf-custom.groovy)
# 6. Run
./run-regression-test.sh --run -d <directory> -s <suite_name>
```
