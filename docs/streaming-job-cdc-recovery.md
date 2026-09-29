# CDC streaming job manual recovery

## Failure details

`ErrorMsg` includes the failure reason and the source table when available.
Doris DDL execution failures also include the failed SQL, any remaining SQL
statements from the same event, and the complete source offset when available.
Pending rows are flushed before executing Doris DDL.

Deserialization failures do not expose a source offset. Earlier rows may still
be buffered when deserialization fails; advancing to the failed record's offset
could skip those rows as well. The absence of an offset must not be worked around
by treating a record position from logs as a safe recovery checkpoint.

## `reload_source_schema`

`ALTER JOB ... PROPERTIES ("reload_source_schema" = "true")` is a one-shot
operation for paused CDC jobs. It clears the persisted source schema and rebuilds
the reader on resume, loading the **current source schema for all included tables**.
It does not retrieve historical schemas or modify Doris tables.

The property can be used alone or together with `offset`. In either case, the
current source schema must match the schema at the effective recovery position
(the committed position when `offset` is omitted). Doris cannot automatically
verify this historical correspondence.

When recovering after a failed DDL, ensure that **none of the included tables has
undergone further schema changes between the recovery position and the time the
reader reloads the schema on resume**. Keep source DDL paused during this recovery
window. This requirement applies to every included table, not just the failing
table. Loading a later schema over earlier row events can cause decoding failures,
missing columns, or incorrectly mapped values. New DDL after recovery can be
consumed normally.

If only Doris DDL execution failed, repair the target schema or permissions and
retry with `RESUME JOB` first. Changing the offset or reloading the source schema
is not required for every DDL failure.

For an intentional skip, verify the connector's restart semantics for the complete
event offset and manually apply the failed and remaining Doris SQL statements as
needed. Skipping an event can omit its changes; verify the affected data and repair
it where necessary. If the recovery position cannot be matched to the current
source schema, do not use `reload_source_schema` with that position.
