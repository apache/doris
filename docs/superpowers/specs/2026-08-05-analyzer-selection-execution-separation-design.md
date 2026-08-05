# Separate Analyzer Selection from Execution

Status: Approved for implementation

## Context

`TMatchPredicate.analyzer_name` currently reaches the BE with two different meanings:

1. it is the analyzer key used to select an inverted-index reader; and
2. it is the provider name used to decide and construct query analysis.

Those meanings are not equivalent. `USING ANALYZER none` needs the non-empty key `none` for exact reader selection, but its execution semantics are raw keyword matching. Conversely, a named custom analyzer or normalizer must execute its pipeline even when the legacy parser type is `NONE`.

Commit `a45b72c` changed `InvertedIndexAnalyzerCtx::should_tokenize()` to treat every non-empty analyzer name as requiring analysis. That repairs named analyzers with legacy `NONE`, but it also sends builtin `none` through `SimpleAnalyzer`, breaking raw semantics in SNII and index-bypass slow paths.

The wire already contains enough information to resolve both meanings. This design fixes the internal model without adding a new Thrift protocol.

## Goals

- Preserve the original analyzer key used for reader selection.
- Resolve a separate, canonical execution configuration before creating an analyzer.
- Make builtin `none` raw regardless of the legacy parser fallback value.
- Execute named analyzers and normalizers even when their legacy parser is `NONE`.
- Keep V2, V3, SNII, slow-path, cache, and rolling-upgrade protocols unchanged.
- Fail instead of silently changing semantics when an analysis pipeline cannot be created.

## Non-goals

- Redesigning `TMatchPredicate` or adding selected-index IDs.
- Replacing analyzer-key reader selection.
- Changing index-policy DDL or policy distribution.
- Generalizing analysis fingerprints beyond the existing CommonGrams mechanism.
- Reworking query-cache or single-flight keys.
- Fixing independent `default` alias, normalizer-key, ARRAY fallback, or historical reserved-name issues.

These issues can be handled independently. They do not justify expanding the protocol used by every MATCH query.

## Design

### Internal request model

Introduce one internal result that separates selection from execution:

```cpp
struct ResolvedAnalyzerRequest {
    std::string selection_key;
    std::string provider_name;
    InvertedIndexParserType parser_type;
};
```

The actual type may reuse and clarify the existing `AnalyzerConfig` structure. The important invariant is that `selection_key` is never reused as `provider_name`.

- `selection_key` is the normalized original `analyzer_name` from Thrift. It is consumed only by inverted-index reader selection.
- `provider_name` is non-empty only for a named analyzer or normalizer pipeline.
- `parser_type` describes builtin execution. `PARSER_NONE` with an empty provider means raw keyword execution.

Parser mode, lowercase, stopwords, and char-filter settings remain in the existing analyzer configuration and are not duplicated.

### Resolution precedence

Resolution follows the same name-over-parser precedence already used by the analyzer factory:

```text
if analyzer_name names a builtin analyzer:
    selection_key = normalize(analyzer_name)
    provider_name = empty
    parser_type = parser type represented by analyzer_name

else if analyzer_name is non-empty:
    selection_key = normalize(analyzer_name)
    provider_name = normalize(analyzer_name)
    parser_type = PARSER_NONE

else:
    selection_key = empty
    provider_name = empty
    parser_type = parser type represented by parser_type string
```

This produces the following required truth table:

| Thrift analyzer name | Thrift parser | Selection key | Provider | Effective parser | Execute pipeline |
| --- | --- | --- | --- | --- | --- |
| empty | `none` | empty | empty | `NONE` | no |
| empty | `english` | empty | empty | `ENGLISH` | yes |
| `none` | `none` | `none` | empty | `NONE` | no |
| `none` | `english` | `none` | empty | `NONE` | no |
| `standard` | `none` | `standard` | empty | `STANDARD` | yes |
| custom analyzer | `none` | custom name | custom name | `NONE` | yes |
| named normalizer | `none` | normalizer name | normalizer name | `NONE` | yes |

The resolver treats a non-empty builtin name as authoritative. The parser string is only consulted when the name is empty.

### Runtime context

`InvertedIndexAnalyzerCtx` stores the selection key separately from execution configuration. Its decision helper is renamed to reflect behavior rather than token count:

```cpp
bool requires_analysis() const {
    return !provider_name.empty() ||
           parser_type != InvertedIndexParserType::PARSER_NONE;
}
```

A custom keyword analyzer and a normalizer both return true because their pipelines must run even if they emit one term.

The inverted-index iterator reads only `selection_key`. FunctionMatch and SNII readers read only `requires_analysis()`, the canonical parser, and the resolved provider.

### Analyzer construction

`VMatchPredicate` resolves the request before constructing an analyzer.

- Raw execution does not construct an analyzer provider.
- Builtin execution creates a provider from the canonical parser type.
- Named execution creates a provider from `provider_name` through `IndexPolicyMgr`.
- A required provider that cannot be constructed returns an analyzer error. It does not fall back to raw matching or `SimpleAnalyzer`.

The factory must not receive builtin `none` as a provider name. This prevents `PARSER_NONE` from reaching its historical default `SimpleAnalyzer` branch.

### Production resolver

The existing `AnalyzerConfigParser` is close to the required boundary but is currently unused by production `VMatchPredicate` and gives the parser string precedence when both fields are present.

The implementation will correct `AnalyzerConfigParser` to use name-over-parser precedence,
rename its ambiguous `custom_analyzer` field to `provider_name`, and call it from
`VMatchPredicate`. When the analyzer name is empty, its selection key remains empty even if the
fallback parser is non-`NONE`; the parser is execution configuration, not evidence of an explicit
index selection.

There must be one production resolver and one table-driven unit-test suite. A second helper with different precedence is not allowed.

## Data Flow

```text
TMatchPredicate analyzer_name/parser_type
                |
                v
      resolve analyzer request
        |                  |
        v                  v
  selection_key      execution config
        |                  |
        v                  v
 InvertedIndexIterator  provider / RAW
        |                  |
        +--------+---------+
                 v
          selected reader or
          FunctionMatch fallback
```

No FE, Thrift, catalog, or on-disk metadata changes are required.

## Error Handling

- Unknown non-empty names are treated as named providers; a missing policy is an explicit analyzer error.
- Raw mode permits a null analyzer/provider by construction.
- Analysis-required mode must have a valid builtin analyzer or named provider.
- Index-key mismatch retains the existing index bypass behavior and must not select a differently analyzed reader.
- Analyzer creation failure never changes the query to raw matching.

## Compatibility

This is a BE-local semantic correction using existing wire fields.

- Old FE to new BE uses the same Thrift payload and receives corrected resolution.
- New FE behavior is unchanged.
- No capability negotiation or ordered FE/BE upgrade is required.
- Existing query-cache keys remain valid because the selected physical index continues to determine indexed token semantics.

Schemas or catalog objects that reuse reserved builtin names for a custom policy are already ambiguous under the current builtin-first factory. Auditing or migrating those objects is a separate catalog task; this design does not add hot-path protocol state for that exceptional case.

## Testing

### BE resolver unit tests

Add the complete truth table above. In particular, include both regression directions:

- `none + ENGLISH` resolves to raw;
- custom provider + `NONE` resolves to analysis.

Also verify mixed-case builtin normalization and a named normalizer with `NONE`.

### Runtime unit tests

- Raw resolution creates no provider and preserves the whole query, including empty strings, case, whitespace, and punctuation.
- Named custom keyword analysis runs its lowercase/filter pipeline.
- Reader selection still receives `none`, `standard`, and custom analyzer keys unchanged.
- Missing named policies return an error rather than raw fallback.

### Regression tests

- Run `test_multi_tokenize_index_not_built` to cover explicit `none` through the generic slow path.
- Preserve the custom analyzer plus legacy `NONE` SNII test that motivated `a45b72c`.
- Add one explicit `USING ANALYZER none` SNII built-index case with a multi-word value, because SNII consults the runtime analysis decision even for its string reader.

The existing V2/V3 string reader already ignores the analyzer context for a built raw index; broad storage-format Cartesian coverage is not required for this change.

## Delivery Scope

The implementation should remain one focused BE change:

1. create or correct the production resolver;
2. split selection key from provider name in runtime context;
3. avoid provider construction for raw execution;
4. update iterator and analysis consumers to use their dedicated fields;
5. add focused unit and regression coverage.

The user approved this design on 2026-08-05 and requested TDD-driven implementation.
