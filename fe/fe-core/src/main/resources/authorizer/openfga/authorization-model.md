# OpenFGA authorization model for Apache Doris

This directory holds the OpenFGA authorization model (`schema.fga`) that the `openfga-doris`
access controller checks against. Doris does not create or own the model at runtime; operators
load it into their OpenFGA store once, and Doris only issues `Check` calls.

## Object types

The model mirrors the Doris object hierarchy:

- `global` is the root. A grant on the global object applies everywhere.
- `catalog` has `parent: [global]`.
- `database` has `parent: [catalog]`.
- `table` has `parent: [database]`.
- `column` has `parent: [table]`.
- `resource`, `workload_group`, `storage_vault` and `compute_group` each have `parent: [global]`,
  so a global grant reaches them but they are otherwise flat.

Object ids encode the full path so they stay unique across catalogs and databases. The controller
builds them like this (the separator is configurable, default `/`):

- `global:doris`
- `catalog:<ctl>`
- `database:<ctl>/<db>`
- `table:<ctl>/<db>/<tbl>`
- `column:<ctl>/<db>/<tbl>/<col>`
- `resource:<name>`, `workload_group:<name>`, `storage_vault:<name>`, `compute_group:<name>`

Users are written as `user:<qualified_user>`, where the type prefix (`user`) is configurable.

## Relations

Relations are derived one to one from `DorisAccessType`, which is itself derived from Doris
privileges:

| Doris privilege        | DorisAccessType | OpenFGA relation |
| ---------------------- | --------------- | ---------------- |
| NODE_PRIV              | NODE            | can_node         |
| ADMIN_PRIV             | ADMIN           | can_admin        |
| GRANT_PRIV             | GRANT           | can_grant        |
| SELECT_PRIV            | SELECT          | can_select       |
| LOAD_PRIV              | LOAD            | can_load         |
| ALTER_PRIV             | ALTER           | can_alter        |
| CREATE_PRIV            | CREATE          | can_create       |
| DROP_PRIV              | DROP            | can_drop         |
| USAGE_PRIV / STAGE_USAGE_PRIV / CLUSTER_USAGE_PRIV | USAGE | can_usage |
| SHOW_VIEW_PRIV         | SHOW_VIEW       | can_show_view    |

The mapping lives in `OpenFgaRelation`. A privilege that has no access type (`NONE`) maps to no
relation and is treated as not allowed.

## Two rules baked into the model

The model is written so that one `Check(user, relation, object)` answers a Doris privilege question
without the controller having to walk the tree itself:

1. `can_admin` on an object implies every other relation on that object. This matches Doris
   `ADMIN_PRIV`, which can do anything on the object it is granted on.
2. Every relation is inherited `from parent`, so a grant made at the catalog level cascades down to
   its databases, tables and columns. This is the top down inheritance the Ranger controller gets
   from its `checkedPrivs` accumulation across levels; here OpenFGA resolves it in one call.

Because global is a distinct object type rather than the tuple parent of a catalog, the controller
still checks the global object explicitly at the start of each `check*Priv`, exactly as the Ranger
controller checks the global level explicitly.

## How a check maps

`checkTblPriv(user, "internal", "db1", "tbl1", SELECT)` becomes, at the table level:

```
Check(user:<user>, can_select, table:internal/db1/tbl1)
```

If OpenFGA returns allowed, access is granted. The `SELECT` predicate in Doris is
`ADMIN_PRIV OR SELECT_PRIV`, so the controller also checks `can_admin` on the same object, and it
first checks the global, catalog and database objects, short circuiting as soon as the predicate is
satisfied. The parent cascade in the model means a `can_select` grant on `database:internal/db1`
already answers the table check through inheritance.

## Example tuples

Grant an analyst read on one table:

```
user:analyst  can_select  table:internal/db1/tbl1
```

Grant a data engineer everything under one catalog (cascades to all its databases and tables):

```
user:engineer  can_admin  catalog:internal
```

Make someone a global admin:

```
user:root  can_admin  global:doris
```

## Known MVP limitations

- The `SHOW` predicate is checked on the object itself and its ancestors, not on descendants. The
  Ranger controller additionally answers "can this user see this database because they have a
  privilege on one of its tables" by matching `SELF_OR_DESCENDANTS`. Reproducing that with OpenFGA
  needs either a dedicated `can_show` relation resolved with `ListObjects`, or a `BatchCheck` over
  the children. It is deliberately out of scope for the MVP and noted so behavior differences are
  explicit.
- Object ids must be valid OpenFGA ids. Catalog, database, table and column names that contain the
  separator character or other characters OpenFGA disallows need encoding before they are used as
  ids. The MVP uses the names as is; encoding is a follow up.
- Masking and row filtering are not modeled. OpenFGA is relationship authorization, not attribute
  masking, so `evalDataMaskPolicy` and `evalRowFilterPolicies` return empty.
