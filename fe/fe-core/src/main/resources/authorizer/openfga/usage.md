# Using the OpenFGA access controller

The `openfga-doris` access controller lets Doris delegate authorization to an
[OpenFGA](https://openfga.dev) store (an open source Zanzibar implementation). It implements the
same `CatalogAccessController` SPI that the Apache Ranger controller uses, so no core, grammar or
parser changes are needed to turn it on.

## 1. Load the authorization model

Create an OpenFGA store and write the model from `schema.fga` in this directory. With the OpenFGA
CLI:

```sh
fga store create --name doris
fga model write --store-id <store_id> --file schema.fga
```

Record the returned store id and authorization model id. Then grant privileges by writing tuples,
for example:

```sh
fga tuple write --store-id <store_id> user:analyst can_select table:internal/db1/tbl1
```

See `authorization-model.md` for the object and relation reference.

## 2. Point Doris at the store

The controller reads its configuration from the property map that the SPI passes to the factory.

For an external catalog, set it in the catalog properties:

```sql
CREATE CATALOG my_catalog PROPERTIES (
    "type" = "hms",
    "access_controller.class" = "openfga-doris",
    "access_controller.properties.api_url" = "http://openfga:8080",
    "access_controller.properties.store_id" = "01J...",
    "access_controller.properties.model_id" = "01J...",
    "access_controller.properties.api_token" = "<preshared-key>"
);
```

For the internal catalog, set `access_controller_type = openfga-doris` in `fe.conf` and put the
properties in the file referenced by `authorization_config_file_path`.

### Properties

- `api_url` (required): base URL of the OpenFGA API, for example `http://openfga:8080`.
- `store_id` (required): the OpenFGA store id.
- `model_id` (optional): pin a specific authorization model id. If omitted, OpenFGA uses the latest
  model in the store.
- `token` or `api_token` (optional): preshared key sent as a bearer token when the OpenFGA server
  requires one.
- `connect_timeout_millis` (optional, default 5000).
- `read_timeout_millis` (optional, default 5000). Also bounds how long a single check waits.
- `object_separator` (optional, default `/`): separator used inside object ids.
- `global_object_id` (optional, default `doris`): id of the single `global` object.
- `user_type` (optional, default `user`): the OpenFGA type prefix used for the current user.

## 3. Fail closed behavior

The controller fails closed. If OpenFGA is unreachable, times out, or returns an error, every
affected check returns "not allowed" and the reason is logged at WARN. A security control must not
grant access because the policy service was down. Misconfiguration (a missing `api_url` or
`store_id`, or a client that cannot be built) fails fast at controller creation rather than silently
denying every request later.

Because there is a single OpenFGA client per JVM (the factory returns a shared singleton, matching
the Ranger controller), the first catalog that activates this controller supplies the connection
settings for the process.

## 4. MVP scope

- Relationship authorization only. `evalDataMaskPolicy` and `evalRowFilterPolicies` return empty, so
  column masking and row filtering are not applied by this controller. This matches
  `InternalAccessController` when no native policy is defined.
- The `SHOW` predicate is evaluated on the object and its ancestors, not its descendants. See the
  limitations section in `authorization-model.md`.
- Doris `GRANT` statements are not synced to OpenFGA. When this controller is active, authorization
  is delegated to OpenFGA; tuples are managed in OpenFGA, not through Doris grants. Whether to
  reconcile Doris grants with OpenFGA tuples is an open design question for the maintainers.
