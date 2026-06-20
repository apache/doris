# FIX-A2 — EXPLAIN drops legacy `predicatesFromPaimon:` line

> Source: `task-list-P6-deviation-fixes.md` §A2 / `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R2 (scan).
> Severity: **MINOR** (diagnostic-only; no correctness/perf impact). Deviation 2/5.

## Problem

Legacy `PaimonScanNode.getNodeExplainString` (`PaimonScanNode.java:660-668`) printed the list of Paimon
`Predicate` objects **actually pushed to the Paimon SDK** (or ` NONE`):

```
predicatesFromPaimon:
    <predicate-1>
    <predicate-2>
```

The SPI scan path lost it. The connector's `appendExplainInfo` (`PaimonScanPlanProvider.java:1116-1129`)
emits only `paimonNativeReadSplits=` + the VERBOSE `PaimonSplitStats` block, and the generic node emits
`PREDICATES: <sql>` (`PluginDrivenScanNode.java:270-275`) — the **Doris-level** conjuncts rendered as
SQL, NOT the SDK-converted Paimon predicates. So a silently-dropped conjunct (the converter drops what
it can't translate — LTZ / FLOAT / unsupported CAST) is no longer observable: `PREDICATES:` still lists
all conjuncts, but the pushed set is invisible.

## Root Cause

A pure missing-port: the legacy line was never re-emitted on the SPI path. The diagnostic gap matters
because `PaimonPredicateConverter.convert` **silently drops** unconvertible predicates
(`PaimonScanPlanProvider.java:573-578` builds the list; the converter null-skips), so
`predicatesFromPaimon:` can legitimately list fewer entries than `PREDICATES:` — that delta is exactly
what the line exists to surface.

## Key lifecycle / seam facts (grounded)

1. **The provider is re-instantiated per call.** `PaimonConnector.getScanPlanProvider():100-101` returns
   `new PaimonScanPlanProvider(...)` each time, with **no shared instance state** between the SPI methods
   (documented at `PaimonScanPlanProvider.java:168-169`). → A connector **field** cannot carry the
   converted predicates from `getScanNodeProperties` to `appendExplainInfo`. Ruled out.
2. **The seam carries only a `Map<String,String>`.** `ConnectorScanPlanProvider.appendExplainInfo(output,
   prefix, nodeProperties)` — the filter/`ConnectorExpression` is NOT passed (it is available only at
   `planScan`/`getScanNodeProperties` time). → Re-running the converter at explain time would require an
   SPI signature change. Ruled out (keep connector-side, no SPI change).
3. **The pushed predicates are already serialized into the props.** `getScanNodeProperties:579` ALWAYS
   emits `props.put("paimon.predicate", encodeObjectToString(predicates))` (even for the empty list — a
   non-null base64 string; the JNI reader deserializes it unconditionally). `encodeObjectToString`
   (`:1448`) = `InstantiationUtil.serializeObject` + Base64.
4. **`appendExplainInfo` receives those props.** The node does `explainProps = new HashMap<>(props)`
   (`PluginDrivenScanNode.java:324`) where `props = getOrLoadScanNodeProperties()` (`:258`), then injects
   the synthetic `__native_read_splits`/`__total_read_splits`/`__explain_verbose` keys
   (`:325-328`, unconditional). So `paimon.predicate` is **always present** in `nodeProperties` during a
   real EXPLAIN, and `InstantiationUtil.deserializeObject(byte[], ClassLoader)` is the symmetric inverse
   (verified present in the SDK).
5. **Legacy ordering:** `super-body` → `paimonNativeReadSplits=<raw>/<total>` →
   `predicatesFromPaimon:[ NONE | …]` → `[VERBOSE] PaimonSplitStats:` (`PaimonScanNode.java:656-671`).

## Design

In the connector's `appendExplainInfo`, **deserialize the already-present `paimon.predicate` prop** back
to `List<Predicate>` and render the legacy `predicatesFromPaimon:` block, placed **between** the
`paimonNativeReadSplits=` line and the VERBOSE `PaimonSplitStats` block (exact legacy order). This reuses
the exact list pushed to BE — no re-conversion, no new prop, no redundant serialization, no SPI change, no field.

```java
// inside the existing  if (nativeSplits != null && totalSplits != null)  block,
// AFTER the paimonNativeReadSplits= append, BEFORE the VERBOSE PaimonSplitStats block:
String encodedPredicates = nodeProperties.get("paimon.predicate");
if (encodedPredicates != null) {
    appendPredicatesFromPaimon(output, prefix, encodedPredicates);
}
```

Helper (new private static):

```java
private static void appendPredicatesFromPaimon(StringBuilder output, String prefix, String encoded) {
    List<org.apache.paimon.predicate.Predicate> predicates;
    try {
        byte[] bytes = Base64.getDecoder().decode(encoded);
        predicates = InstantiationUtil.deserializeObject(
                bytes, org.apache.paimon.predicate.Predicate.class.getClassLoader());
    } catch (Exception e) {
        // Diagnostic line only — never break EXPLAIN. The prop is produced by us, so a decode failure
        // is a real bug; log + skip the line rather than render a misleading NONE.
        LOG.warn("Failed to decode paimon.predicate for EXPLAIN predicatesFromPaimon", e);
        return;
    }
    if (predicates == null) {
        // unexpected payload — skip (do not render a misleading " NONE"); consistent with the catch path
        return;
    }
    output.append(prefix).append("predicatesFromPaimon:");
    if (predicates.isEmpty()) {
        output.append(" NONE\n");
    } else {
        output.append("\n");
        for (org.apache.paimon.predicate.Predicate predicate : predicates) {
            output.append(prefix).append(prefix).append(predicate).append("\n");
        }
    }
}
```

### Why gate on `paimon.predicate != null` (skip when absent)

`predicatesFromPaimon` renders iff the prop is present. In a real EXPLAIN it is always present (fact 3+4),
so this is full legacy parity. When absent (a unit test that injects only the synthetic keys, or another
connector's props) the line is skipped — which **preserves ALL existing exact-equality `appendExplainInfo`
tests** (none of them set `paimon.predicate`) and mirrors the existing "skip when keys absent" philosophy
of the `paimonNativeReadSplits=` line (`:1112-1114`). Absent ≠ empty-list, so skipping (not rendering
` NONE`) is the correct degenerate behavior.

### Classloader

Decode with `org.apache.paimon.predicate.Predicate.class.getClassLoader()` — the plugin classloader that
loaded the paimon SDK in the connector, guaranteed to have `Predicate` and its dependents. (Connector
code runs under that CL, so `Predicate.class` resolves there.) Avoids any reliance on the thread-context
classloader at explain time.

### Why deserialize, not re-convert (deviates from the task-list's suggested mechanism)

`task-list-P6-deviation-fixes.md` §A2 suggested "re-run `PaimonPredicateConverter` over the pushed
filter." That is not directly possible — the filter is not in the seam (fact 2). Deserializing the
already-serialized `paimon.predicate` is strictly better: same OUTPUT (the rendered pushed predicates),
but it renders **precisely what BE receives** (the serialized list is the source of truth), with the
smallest change (no SPI signature change, no new prop, no BE bloat).

## Risk Analysis

- **No correctness/perf/route impact** — diagnostic EXPLAIN text only.
- **Decode failure** never breaks EXPLAIN (try/catch → LOG.warn + skip the line).
- **No redundant serialization** — reuses the existing `paimon.predicate` blob instead of serializing the
  same `List<Predicate>` into a second prop. (A new explain-only key would NOT have reached BE either:
  `populateScanLevelParams:1078-1103` reads props key-by-key — only `paimon.predicate`/`options_json`/
  `schema_evolution` are set onto the thrift params, there is no bulk `putAll` — so the "BE bloat" worry
  was unfounded; deserialize still wins on minimality.)
- **Backward-compat** — existing exact-equality explain tests keep passing (line skipped when prop absent).
- **toString / render-format parity** — this renders the set the **SPI path actually pushes to BE** (the
  source of truth), NOT a re-derivation of legacy's fe-core converter output. Both converters emit the same
  `org.apache.paimon.predicate.Predicate` class, so `Predicate.toString()` parity holds; the
  serialize→deserialize round-trip is lossless (toString is field-derived); the surrounding format (label,
  ` NONE`, double-prefix indent, newlines) is reproduced verbatim from `PaimonScanNode.java:660-668`.
- **Ordering** — inserted between `paimonNativeReadSplits=` and the VERBOSE block = exact legacy order.

## Test Plan

### Unit Tests (fe-connector-paimon, add to `PaimonScanExplainTest`)

Build the `paimon.predicate` prop exactly as production does (`InstantiationUtil.serializeObject` +
`Base64`), inject alongside the synthetic split keys, call `appendExplainInfo`, assert the rendered text.

1. **Non-empty pushed predicates** — build `List<Predicate>` via paimon `PredicateBuilder`
   (e.g. `equal(0, 5)` over a 1-col RowType), serialize into `paimon.predicate`, set
   `__native_read_splits`/`__total_read_splits`. Assert output contains, in order,
   `paimonNativeReadSplits=…\n` then `predicatesFromPaimon:\n` then `<prefix><prefix><p.toString()>\n`
   (expected predicate text computed from `p.toString()`, not hardcoded). **RED before:** the line is
   absent.
2. **Empty pushed predicates** — serialize `Collections.emptyList()` into `paimon.predicate`. Assert
   output contains `predicatesFromPaimon: NONE\n`.
3. **Ordering** — assert `indexOf("paimonNativeReadSplits=") < indexOf("predicatesFromPaimon:")` and,
   under VERBOSE (`__explain_verbose=true`), `indexOf("predicatesFromPaimon:") < indexOf("PaimonSplitStats:")`.
4. **Backward-compat (existing tests, unchanged)** — all existing exact-equality `appendExplainInfo` tests
   (none set `paimon.predicate`) must still pass byte-for-byte (line skipped when prop absent).
5. **Absent → skip (new dedicated guard)** — `appendExplainInfoSkipsPredicatesFromPaimonWhenPropAbsent`:
   set only `__native_read_splits`/`__total_read_splits` (NO `paimon.predicate`), assert output contains
   `paimonNativeReadSplits=` AND does NOT contain `predicatesFromPaimon` — positively pins the
   absent ≠ empty contract (mirrors the sibling `…SkipsWhenSyntheticKeysAbsent` guard).

### E2E Tests

None added. Existing paimon regression suites that assert EXPLAIN are gated (`enablePaimonTest=false`).
The connector UT pins the rendered string; a live e2e is not warranted for a diagnostic line.

## Build / Verify

- `mvn -f .../fe/pom.xml -pl :fe-connector-paimon -am package -Dassembly.skipAssembly=true
  -Dmaven.build.cache.enabled=false -DfailIfNoTests=false` (checkstyle in `validate`).
- `tools/check-connector-imports.sh` exit 0 (no fe-core import; only paimon SDK + java.util).
- RED→GREEN: new non-empty test fails before the fix (line absent), passes after.
