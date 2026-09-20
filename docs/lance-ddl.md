# Lance DDL

Doris can mutate namespaces, tables, and top-level columns in a Lance external
catalog. The catalog can use either the Lance filesystem namespace or the Lance
REST namespace.

## Supported statements

```sql
CREATE DATABASE lance_catalog.analytics;
DROP DATABASE lance_catalog.analytics;
DROP DATABASE lance_catalog.analytics FORCE;

CREATE TABLE lance_catalog.analytics.events (
    id BIGINT NOT NULL,
    name STRING NULL
) ENGINE=LANCE
COMMENT 'events'
PROPERTIES ("owner" = "analytics");

SHOW CREATE TABLE lance_catalog.analytics.events;

ALTER TABLE lance_catalog.analytics.events ADD COLUMN score INT NULL;
ALTER TABLE lance_catalog.analytics.events ADD COLUMN (
    active BOOLEAN NULL,
    note STRING NULL
);
ALTER TABLE lance_catalog.analytics.events
    MODIFY COLUMN score BIGINT NOT NULL;
ALTER TABLE lance_catalog.analytics.events
    RENAME COLUMN score TO ranking;
ALTER TABLE lance_catalog.analytics.events DROP COLUMN note;

ALTER TABLE lance_catalog.analytics.events RENAME renamed_events;
DROP TABLE lance_catalog.analytics.renamed_events;
```

`CREATE DATABASE IF NOT EXISTS`, `DROP DATABASE IF EXISTS`,
`CREATE TABLE IF NOT EXISTS`, and `DROP TABLE IF EXISTS` follow their normal
Doris behavior.

## Column evolution

`ADD COLUMN` and `ADD COLUMN (...)` append nullable scalar columns. Existing
rows are populated with a typed `NULL` through the Lance Namespace add-columns
operation. Supported types are Boolean, signed integers, floating-point types,
strings, binary, date, datetime, and decimal.

`MODIFY COLUMN` can change nullability and the scalar types that Lance
Namespace 0.7.7 can represent without losing type parameters: Boolean, signed
integers, floating-point types, strings, binary, date, and microsecond
datetime. Lance validates whether a requested cast or nullable-to-required
change is legal for the existing data.

The following column operations are not supported:

- Nested column paths
- `FIRST` or `AFTER` column positioning
- Column defaults, generated columns, auto-increment columns, or aggregation
- Adding or modifying column comments
- Adding complex or extension types
- Modifying decimal, complex, extension, time, or timezone-aware types
- Multiple `ALTER` clauses containing a column operation
- `ORDER BY` column reordering
- Explicit Lance backfill or virtual-column operations

These restrictions fail before a mutation is sent when Doris has enough
information to detect them.

## Catalog behavior

The filesystem namespace executes mutations against the Lance dataset and
commits a new table version. The REST namespace sends the corresponding Lance
Namespace request to the configured server. A REST server must implement the
requested operation and authorize it; otherwise Doris returns the server or SDK
failure without updating its metadata cache.

Each successful schema mutation refreshes the Doris table object and external
schema cache. Each statement performs one Lance operation. Concurrent schema or
data writes can conflict according to Lance commit semantics, and the failed
statement can be retried after refreshing metadata.

## Deletion semantics

`DROP TABLE` uses the Lance Namespace `DropTable` operation. It deletes the
underlying Lance dataset; it does not only deregister the catalog entry.

`DROP DATABASE` uses restrictive behavior by default and fails for a non-empty
namespace. `DROP DATABASE ... FORCE` requests cascading deletion. The
configured root database cannot be dropped.
