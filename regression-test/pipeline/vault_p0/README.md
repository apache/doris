## Vault P0 CI Pipeline

This pipeline deploys Doris in cloud mode with s3 storage vault on a single machine and runs cases in `regression-test/suites/vault_p0/`.

The Vault P0 CI pipeline uses the same script as Cloud P0. The only difference is that it creates an instance with Vault. To achieve this, you only need to replace the function `create_warehouse` with `create_warehouse_vault`.

```sh
sed -i 's/create_warehouse/create_warehouse_vault/g' regression-test/pipeline/cloud_p0/deploy.sh
```
