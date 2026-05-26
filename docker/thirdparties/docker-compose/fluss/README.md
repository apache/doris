# Fluss thirdparty environment

A fully managed Apache Fluss cluster for end-to-end tests.

## Topology

| Service                       | Role               | Internal port | External port                          |
|-------------------------------|--------------------|---------------|----------------------------------------|
| `doris--fluss-zookeeper`      | Metadata store     | 2181          | `DOCKER_FLUSS_ZOOKEEPER_EXTERNAL_PORT` |
| `doris--fluss-coordinator`    | CoordinatorServer  | 9123          | -                                      |
| `doris--fluss-tablet-server`  | TabletServer       | 9123          | `DOCKER_FLUSS_EXTERNAL_PORT`           |

Defaults (see `fluss_settings.env`): external Fluss port `9123`, external ZK port `12182`, image `apache/fluss:0.7.0`.

## Bring up / tear down

| Action | Command                                                                       |
|--------|-------------------------------------------------------------------------------|
| Start  | `bash docker/thirdparties/run-thirdparties-docker.sh -c fluss`                |
| Stop   | `bash docker/thirdparties/run-thirdparties-docker.sh --stop -c fluss`         |

## Client bootstrap

`bootstrap.servers = <host>:${DOCKER_FLUSS_EXTERNAL_PORT}` (default `localhost:9123`).
