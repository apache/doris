# Escaped Variant keys on branch-4.2

`variant_escape_dots_in_keys=true` changes persisted Variant path encoding. V2
execution alone does **not** prove that a backend understands that encoding.

## Upgrade and activation

`enable_variant_escape_dots_in_keys` defaults to `false`, separately from
`enable_variant_v2`. Creation (including inherited policies) and load planning
reject escaped tables while the former is false. RESTORE checks the selected
backup tables before changing local table state or creating replicas, independently
of the query execution switch. Foreground statements over only unescaped
internal tables retain the global V1/V2 query default.

This branch does not advertise escaped-path support in backend heartbeats. Backend
version strings and the block execution version cannot reliably identify this
feature in development builds or backports. The activation switch is therefore an
operator attestation, **not an automatic backend capability check**.

1. Keep the activation switch false throughout a rolling upgrade.
2. Upgrade **every FE and BE**, including inactive backends, other compute groups,
   and nodes used for replica repair or background maintenance. In Cloud mode,
   upgrade every meta-service that persists or forwards tablet schemas as well.
3. Verify that all these binaries preserve the schema property and understand
   `ColumnMetaPB.variant_path_encoding=1`.
4. Only then enable `enable_variant_escape_dots_in_keys=true` on all frontends.
   Escaped tables automatically select V2 for the whole statement; the global
   `enable_variant_v2` query default may remain false.

Once escaped tables exist, do not add an older node or downgrade to binaries
without this feature. Disabling the activation switch does not convert persisted
data, revoke in-flight/cached load plans, or make an older BE safe to query or
compact it. Drain loads before an upgrade or maintenance window. A downgrade
requires exporting and validating the data while the upgraded cluster is still
available. Preserve dotted keys in an external JSON/text representation; rewriting
to an unescaped VARIANT table can merge literal dotted keys with nested paths and
is not a lossless downgrade procedure.

## Execution and maintenance

Queries that request an actual V1 Variant slot from an escaped tablet return
`NotSupported`, including ordinary scans, short-circuit point queries and both
RowId fetch protocols. Queries requesting only ordinary columns remain allowed.
The check uses the execution type before read-schema promotion and does not infer
it from the session/config switch. Storage-reader checks remain in place.

A statement reading or writing an escaped table automatically selects V2 for
all its Variant expressions and slots, including joins with unescaped tables,
CTAS, and stream loads. It does not toggle the global query default, mutate shared
column types, or affect the next unrelated statement. The independent storage
activation gate still applies to creation, inheritance, loads and restore.

Background MV refresh/build, rollup, schema change, index build and compaction
use V2 Variant execution even for unescaped tables. Background RPC serialization
covers expression/function types and slot descriptors together; BE readers,
aggregate functions, output blocks and writers use private V2 execution schemas.
SC/rollup requests from older FEs are normalized on copies of their descriptors
and expressions, so their V1 flags cannot conflict with the V2 maintenance blocks.
The persisted table property still decides physical path encoding: unescaped
history is not relabelled as escaped, and execution flags are not persisted.

Materializations record their Variant execution version separately from encoding.
A V1 query cannot reuse V2 Variant-dependent materializations: CAST and empty-array
semantics can differ. Old materializations without a known execution version are
conservatively excluded from Variant-dependent rewriting; recreate them under the
new policy before relying on that optimization. Scalar-only materializations are
unaffected. Refreshing an old MV alone does not assert that all historical or
partially refreshed partitions were computed with V2.

The per-segment encoding marker remains authoritative for decoding historical
segments. No V1 escaped-key codec, protocol field, format downgrade, or automatic
fallback to a different decoder is introduced by the compatibility guards.

For a historical unescaped segment in an escaped tablet, special-key subpaths
are projected from the V2 logical value instead of binding an encoded spelling
to an unrelated legacy key or index. This also applies to typed scalar targets.
Compaction reconstructs complete values while any input segment uses the old
encoding, then writes them with the current V2 writer. Once all inputs use the
escaped encoding, normal column-group compaction resumes. The transitional
rewrite can use more memory and I/O than column-group compaction; performance
has not been benchmarked here.
