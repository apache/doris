# FIX-L18 — iceberg unknown/v3 type → UNSUPPORTED (accept, not throw)

## Problem (HEAD-verified)
`IcebergTypeMapping` (fe-connector-iceberg) READ direction has two silent `default → UNSUPPORTED` arms:
- `fromIcebergType` nested switch (`:90-91`) — reached by non-primitive `VARIANT` and any future non-primitive typeId.
- `fromPrimitive` switch (`:142-143`) — reached by the v3 primitives `TIMESTAMP_NANO` / `GEOMETRY` / `GEOGRAPHY` / `UNKNOWN` (all `Type$PrimitiveType` in iceberg 1.10.1), plus the explicit `case TIME → UNSUPPORTED` (`:140-141`).

Legacy fe-core (`IcebergUtils.icebergTypeToDorisType` / `icebergPrimitiveTypeToDorisType`) mapped `TIME`/`VARIANT`
to `Type.UNSUPPORTED` explicitly but **threw** `IllegalArgumentException("Cannot transform unknown type: …")`
on the `default` — so today's connector is strictly **more lenient**: an iceberg table with a v3 column
LOADS with that column present-but-unqueryable, whereas legacy failed the whole table load. The genuine
divergence set is the v3 primitives (`TIMESTAMP_NANO`/`GEOMETRY`/`GEOGRAPHY`/`UNKNOWN`); `TIME`+`VARIANT`
are parity (both explicit/effective UNSUPPORTED). Trino also fails loud (`TypeConverter` → `TrinoException(NOT_SUPPORTED)`).

## Decision (user, 2026-07-13)
**Accept the looser behavior — map every unrepresentable column uniformly to UNSUPPORTED, do NOT throw.**
Rationale (user): one exotic column should not make a wide table unloadable; other columns stay usable.
Registered as **DV-051**. This is Option B of the recon; Option A (restore legacy throw) was declined.

## Change
- **No functional change** — the two `default` arms already return `UNSUPPORTED`; that is now the sanctioned
  behavior. Added clarifying comments on both arms documenting the intentional divergence + DV-051, and noting
  the write direction `toIcebergPrimitive:199` still throws (CREATE TABLE must not accept a non-round-trippable type).
- **Guard test** `IcebergTypeMappingReadTest.unknownAndV3TypesDegradeToUnsupportedByDesign` pins the choice:
  asserts `TIMESTAMP_NANO`/`GEOMETRY`/`GEOGRAPHY`/`UNKNOWN`/`VARIANT` → `UNSUPPORTED` (flag-independent). A
  future mutation making either arm throw turns this RED — surfacing that the accepted deviation was reverted
  (Rule 9: the test encodes WHY — the deliberate graceful-degradation decision).
- **DV-051** registered in `plan-doc/deviations-log.md`.

## Iron rule
Clean — entirely in fe-connector-iceberg, switching on iceberg's own `Type.TypeID`. No fe-core touch, no
source-name branching, no property parsing. `DorisConnectorException` (the module idiom) unused here since we
do NOT throw.

## Blast radius
Zero behavior change → no regression. No existing regression fixture uses GEOMETRY/GEOGRAPHY/TIMESTAMP_NANO
(grep clean), consistent with "table loads, column unqueryable". Sole caller of the mapping is
`IcebergConnectorMetadata.parseSchema` on table load/refresh.

## e2e (live-gated)
Register an iceberg v3 table with a GEOMETRY (or TIMESTAMP_NANO) column → table loads, `DESCRIBE` shows the
column as UNSUPPORTED, `SELECT other_col` works, `SELECT geom_col` errors at execution (present-but-unqueryable).
