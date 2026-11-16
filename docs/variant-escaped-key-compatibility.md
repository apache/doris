# Escaped Variant keys on branch-4.2

`variant_escape_dots_in_keys=true` changes persisted Variant path encoding. V2
execution alone does **not** prove that a backend understands that encoding.

## Upgrade and activation

`enable_variant_escape_dots_in_keys` defaults to `false`, separately from
`enable_variant_v2`. Creation (including inherited policies) and load planning
reject escaped tables while the former is false. RESTORE checks the selected
backup tables before changing local table state or creating replicas, independently
of the query execution switch. Unescaped tables retain their
existing V1 and V2 behavior.

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
4. Only then enable both FE settings on all frontends:
   `enable_variant_escape_dots_in_keys=true` and `enable_variant_v2=true`.

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

Creating an escaped table and planning user loads require V2. In contrast, an
existing tablet's compaction, replica creation and column schema changes use its
persisted schema property. Switching the FE's query implementation to V1 must not
erase that property or select a V1 maintenance writer. New materialized-view
queries still require V2 execution.

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
