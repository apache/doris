# P6.2-T02 — iceberg scan: predicate pushdown + createTableScan + planFileScanTask split enumeration (design)

> Branch `catalog-spi-10-iceberg`. Builds on T01 skeleton (`IcebergScanPlanProvider`/`IcebergScanRange`). **Zero behavior change**: iceberg stays out of `SPI_READY_TYPES`; verified by offline UT only. Recon = `plan-doc/research/p6.2-iceberg-scan-recon.md` + workflow `wf_a49c72b0-fb9` (7 readers).

## Scope (migration P6.2-T02)
1. **Predicate pushdown** — self-contained `IcebergPredicateConverter` (`ConnectorExpression` → iceberg `Expression`), no fe-core imports. Mirrors `PaimonPredicateConverter` traversal skeleton + ports legacy `IcebergUtils.convertToIcebergExpr` iceberg-side mapping (operator/literal matrix + `checkConversion` bind-test).
2. **createTableScan** — `table.newScan()` + `.filter(expr)` per converted conjunct (order preserved).
3. **planFileScanTask split enumeration** — iceberg SDK `TableScanUtil.splitFiles(scan.planFiles(), targetSplitSize)` + ported `determineTargetFileSplitSize` heuristic (session vars). One minimal `IcebergScanRange` per `FileScanTask` (path/start/length/fileSize).

**Deferred (T03+):** FileScanTask→typed range params / native-vs-JNI fileFormat / `populateRangeParams` thrift (T03); merge-on-read deletes (T04); COUNT pushdown + batch mode (T05); field-id history dict (T06); MVCC snapshot pin (T07); manifest-cache split path (T08); vended creds + URI normalization (T09).

## Key code-grounded facts
- **Input contract** = `ExprToConnectorExpressionConverter` (fe-core). Emits: `ConnectorComparison`(EQ/NE/LT/LE/GT/GE/EQ_FOR_NULL), `ConnectorAnd`/`ConnectorOr`(flattened), `ConnectorNot`, `ConnectorIn`(negated), `ConnectorIsNull`(negated), `ConnectorLike`, `ConnectorBetween`(NOT-between=`ConnectorNot(Between)`), `ConnectorColumnRef`, `ConnectorLiteral`, `ConnectorFunctionCall`(fallback). **CastExpr unwrapped** → bare column. Literals carry **plain Java values**: `Boolean`/`Long`(IntLiteral)/`Double`(FloatLiteral)/`BigDecimal`/`String`/`LocalDate`/`LocalDateTime`/null; `ConnectorType.getTypeName()` = Doris primitive ("TINYINT".."BIGINT","FLOAT","DOUBLE","DATEV2"…).
- **`FileSplitter` is fe-core** → cannot import. **iceberg does NOT need it** — legacy `IcebergScanNode.splitFiles` delegates to iceberg SDK `TableScanUtil.splitFiles` (iceberg-core 1.10.1, on connector classpath); split byte-offsets come from `FileScanTask.start()/.length()`. (paimon copied `computeFileSplitOffsets` only because paimon slices ORC/Parquet itself — NOT applicable here.)
- **Split-size session vars** read via `ConnectorSession.getSessionProperties()` (string keys, paimon-identical): `file_split_size`(0) / `max_initial_file_split_size`(32MB) / `max_file_split_size`(64MB) / `max_initial_file_split_num`(200) / `max_file_split_num`(100000). `ScanTaskUtil.contentSizeInBytes` absent in 1.10.1 jar → use `DataFile.fileSizeInBytes()` (== content size for data files; T02 is the no-delete path).
- **Session timezone** for timestamptz literals = `ConnectorSession.getTimeZone()` (e.g. "Asia/Shanghai"); null/blank → UTC.

## IcebergPredicateConverter (faithful port of `convertToIcebergExpr`)
Constructor `(Schema schema, ZoneId sessionZone)`. `List<Expression> convert(ConnectorExpression)`: null→[]; flatten top-level `ConnectorAnd` and (per conjunct) `checkConversion(convertSingle(c))`, drop nulls — **mirrors legacy createTableScan per-conjunct loop** (each becomes a separate `scan.filter`).

`convertSingle` instanceof-dispatch (mirrors legacy handled node set, NOT paimon's):
- `ConnectorAnd` → fold `Expressions.and`, drop null arms (keep pushable arm) — legacy AND degradation.
- `ConnectorOr` → all-or-nothing (any null → whole null) — legacy OR.
- `ConnectorNot` → `child=convertSingle(operand)`; non-null → `Expressions.not(child)` — legacy NOT.
- `ConnectorComparison` → col-op-literal only (left=ColumnRef, right=Literal); resolve via `getPushdownField`; `value=extractIcebergLiteral(field.type(), literal)`; if null: `EQ_FOR_NULL && literal.isNull()` → `Expressions.isNull` else null; else EQ/EQ_FOR_NULL→equal, NE→`not(equal)`, GE/GT/LE/LT→`greaterThanOrEqual/greaterThan/lessThanOrEqual/lessThan`.
- `ConnectorIn` → value=ColumnRef; each item ConnectorLiteral with non-null `extractIcebergLiteral` (any null → whole null); negated?`notIn`:`in`.
- `ConnectorLiteral`(Boolean) → `alwaysTrue`/`alwaysFalse` — legacy BoolLiteral.
- else → null. **Dropped (legacy `convertToIcebergExpr` has no case): `ConnectorIsNull`, `ConnectorLike`, `ConnectorBetween`, `ConnectorFunctionCall`** → BE residual filters them. Dropping = safe over-approximation; matches legacy partition-count.

`getPushdownField(colName)`: block `_row_id`/`_last_updated_sequence_number`; else `schema.caseInsensitiveFindField(colName)`; use `field.name()` (canonical case) in `Expressions.*`.

`extractIcebergLiteral(Type icebergType, ConnectorLiteral lit)` — faithful port of `extractDorisLiteral` matrix, dispatch on Java value type (+ `getTypeName()` for int32/int64 & float/double):
| value | iceberg-type acceptances → value |
|---|---|
| Boolean | BOOLEAN→Boolean; STRING→"1"/"0" (BoolLiteral.getStringValue) |
| LocalDate | STRING/DATE→ISO "yyyy-MM-dd"; TIMESTAMP→micros(midnight; tz per `shouldAdjustToUTC`) |
| LocalDateTime | STRING/DATE→Doris-style "yyyy-MM-dd HH:mm:ss[.SSSSSS]"; TIMESTAMP→micros(tz) |
| BigDecimal | DECIMAL→BigDecimal; STRING→toString; DOUBLE→doubleValue |
| Double (FLOAT) | FLOAT/DOUBLE/DECIMAL→double |
| Double (DOUBLE) | DOUBLE/DECIMAL→double |
| Long/Integer (int32: TINYINT/SMALLINT/INT) | INTEGER/LONG/FLOAT/DOUBLE/DATE/DECIMAL→(int) |
| Long/Integer (int64: BIGINT) | INTEGER/LONG/FLOAT/DOUBLE/TIME/TIMESTAMP/DATE/DECIMAL→(long) |
| String | DATE/TIME/TIMESTAMP/STRING/UUID/DECIMAL→String; INTEGER→parseInt(null on fail); LONG→parseLong |
| else | null |

TIMESTAMP micros: `dt.atZone(zone).toInstant()` → `epochSecond*1_000_000 + nano/1000`; zone = sessionZone if `((TimestampType)t).shouldAdjustToUTC()` else UTC (mirrors legacy `getUnixTimestampWithMicroseconds`). The session zone is resolved by `IcebergScanPlanProvider.resolveSessionZone` through a self-contained copy of fe-core `TimeUtils.timeZoneAliasMap` (`ZoneId.SHORT_IDS` + `CST`/`PRC`→`Asia/Shanghai`, `UTC`/`GMT`→`UTC`) via `ZoneId.of(tz, aliasMap)` — **adversarial-review fix**: Doris stores `time_zone` un-canonicalized (`SET time_zone='CST'` keeps "CST"), and a plain `ZoneId.of("CST")` throws → UTC fallback → 8h-shifted timestamptz pushdown → wrong pruning. UT-invisible (the grid has no `withZone()` column); now covered by `resolveSessionZoneHonorsDorisTimezoneAliases` + `timestamptzLiteralUsesSessionZone`.

`checkConversion(Expression)` — verbatim legacy port over iceberg `op()`: AND (recurse, keep bindable arm), OR/NOT (all-or-nothing), TRUE/FALSE pass, default = `((Unbound)e).bind(schema.asStruct(), true)` try/catch→null. Drops un-typecheckable predicates (e.g. out-of-range).

## planScan body
```
Table table = resolveTable(handle)              // existing, auth-wrapped
exprs = filter.present ? new IcebergPredicateConverter(table.schema(), zone(session)).convert(filter) : []
TableScan scan = table.newScan()                // NO metricsReporter (profile dropped), NO planWith (SDK default pool)
for e in exprs: scan = scan.filter(e)
ranges = []
try (it = splitFiles(scan, session)):           // file_split_size>0 → TableScanUtil.splitFiles(planFiles,fss);
  for task in it:                                //   else materialize planFiles → determineTargetFileSplitSize → splitFiles
    f = task.file()
    ranges.add(IcebergScanRange.Builder.path(f.path()).start(task.start()).length(task.length()).fileSize(f.fileSizeInBytes()).build())
return ranges
```
`determineTargetFileSplitSize`: `result=maxInitial`; accumulate `file.fileSizeInBytes()`; if `total >= maxSplit*maxInitialNum` → `result=maxSplit`; if `maxFileSplitNum>0 && total>0` → `result=max(result, ceilDiv(total,maxFileSplitNum))`. (batch mode deferred — paimon parity.)

`IcebergScanRange` unchanged (builder already has path/start/length/fileSize/fileFormat; fileFormat stays default "" until T03).

## Tests (no Mockito, fail-loud)
- **IcebergPredicateConverterTest** (primary parity oracle): port the 9-col×13-literal acceptance grid from fe-core `IcebergPredicateTest` (assert push iff legacy `expects[][]`); AND keep-pushable-arm; OR all-or-nothing; NOT; IN/NOT-IN (one bad element drops all); EQ_FOR_NULL+null→isNull; bool→alwaysTrue/False; reversed-order/col-col dropped; metadata-col block; case-insensitive; checkConversion drop (out-of-range string→int).
- **IcebergScanPlanProviderTest** (extend): real in-memory iceberg table via `InMemoryCatalog` + appended `DataFiles` metadata (no Parquet I/O); assert enumeration count (no predicate), split tiling (`file_split_size` → multiple ranges, correct start/length), partition pruning (filter excludes a partition's files), table resolved inside auth context (kept from T01). Test helper builds the table; `RecordingIcebergCatalogOps.table` returns it.

## Deviations (UT-invisible, P6.6 docker)
- Dropped `metricsReporter` (profile) + `planWith` (use SDK default worker pool) — file-set identical, only planning parallelism/metrics differ (paimon profile-drop parity).
- Reversed comparison `literal OP col` / col-col → dropped (legacy had latent buggy reversed handling that kept opcode; Nereids normalizes so unreachable; dropping = safe over-approx).
- `ConnectorIsNull`/`Like`/`Between` dropped (legacy `convertToIcebergExpr` has no such case; IS NULL still pushed via EQ_FOR_NULL+null).
- LARGEINT arrives as `String` (ExprToConnectorExpressionConverter) not handled as int64 — legacy also didn't push LargeIntLiteral; benign.
- Edge literal string forms (datetime→STRING col, decimal→STRING col) best-effort Doris-style; rare, P6.6 verify.
- batch mode (`isBatchMode`) deferred; non-batch determineTargetFileSplitSize path only.
