## Vault P0 CI Pipeline

This pipeline deploys Doris in cloud mode with s3 storage vault on a single machine and runs cases in `regression-test/suites/vault_p0/`.

The test case relies on an HDFS Docker container, which is set up using Docker Compose.
