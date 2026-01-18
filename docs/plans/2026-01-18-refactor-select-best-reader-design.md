# Refactor select_best_reader: Analyzer Key Matcher Extraction

## Overview

Refactor `InvertedIndexIterator::select_best_reader` to improve extensibility and testability by extracting analyzer matching logic into an independent component.

**Task ID**: #25 from tasks.json
**Priority**: P1
**Status**: Design Complete

## Problem Statement

Current `select_best_reader` (~130 lines) couples three concerns:
1. Analyzer key matching logic
2. Reader type priority (FULLTEXT vs STRING_TYPE vs BKD)
3. Bypass decision (when to return INVERTED_INDEX_BYPASS)

This makes it difficult to:
- Add new index types (e.g., BKD for range queries in search DSL)
- Unit test individual logic components
- Understand and maintain the code

## Design Goals

1. **Extensibility**: Support future index types (BKD, Vector) with minimal changes
2. **Testability**: Each component independently testable
3. **Simplicity**: No over-engineering, use straightforward patterns

## Solution: AnalyzerKeyMatcher + Simple Dispatch

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   AnalyzerKeyMatcher (独立类)                    │
├─────────────────────────────────────────────────────────────────┤
│  struct MatchResult {                                            │
│      vector<ReaderEntry*> candidates;                           │
│      bool used_fallback;                                        │
│  };                                                              │
│                                                                  │
│  static MatchResult match(analyzer_key, entries, index);        │
│  static bool is_explicit(key);  // non __default__              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              select_best_reader (简单分发)                       │
├─────────────────────────────────────────────────────────────────┤
│  Result<ReaderPtr> select_best_reader(...) {                    │
│      auto match = AnalyzerKeyMatcher::match(...);               │
│                                                                  │
│      if (is_string_type(col_type)) {                            │
│          return select_for_text(match, query_type, key);        │
│      }                                                           │
│      if (is_numeric_type(col_type)) {                           │
│          return select_for_numeric(match, query_type);          │
│      }                                                           │
│      return first_available(match);                             │
│  }                                                               │
└─────────────────────────────────────────────────────────────────┘
```

### Component Details

#### 1. AnalyzerKeyMatcher (New Class)

**Location**: `be/src/olap/rowset/segment_v2/analyzer_key_matcher.h`

**Responsibilities**:
- Match analyzer key against reader entries
- Handle fallback logic (exact match → default → all)
- Determine if analyzer key is explicitly specified

```cpp
class AnalyzerKeyMatcher {
public:
    struct MatchResult {
        std::vector<const ReaderEntry*> candidates;
        bool used_fallback = false;
        bool empty() const { return candidates.empty(); }
    };

    // Core matching: exact match → fallback to default → all readers
    static MatchResult match(std::string_view analyzer_key,
                            const std::vector<ReaderEntry>& entries,
                            const KeyIndex& index);

    // Check if user explicitly specified an analyzer (not __default__)
    static bool is_explicit(std::string_view key) {
        return !key.empty() && key != INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }
};
```

#### 2. select_best_reader (Refactored)

**Location**: `be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp`

**Changes**:
- Delegate analyzer matching to `AnalyzerKeyMatcher`
- Dispatch by column type to specialized functions
- Each function handles its own bypass and priority logic

```cpp
Result<ReaderPtr> InvertedIndexIterator::select_best_reader(
        const DataTypePtr& col_type, InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {

    const auto normalized_key = normalize_analyzer_key(analyzer_key);
    auto match = AnalyzerKeyMatcher::match(normalized_key, _reader_entries, _key_to_entries);

    const auto field_type = col_type->get_storage_field_type();

    if (is_string_type(field_type)) {
        return select_for_text(match, query_type, normalized_key);
    }
    if (is_numeric_type(field_type)) {
        return select_for_numeric(match, query_type);
    }

    return match.empty() ? no_reader_error() : match.candidates.front()->reader;
}
```

#### 3. select_for_text (New Private Method)

**Responsibilities**:
- Handle text index selection (FULLTEXT vs STRING_TYPE)
- Bypass when explicit analyzer not found
- Priority: MATCH→FULLTEXT, EQUAL→STRING_TYPE

```cpp
Result<ReaderPtr> InvertedIndexIterator::select_for_text(
        const MatchResult& match, InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {

    // Bypass: explicit analyzer specified but not found
    if (match.empty() && AnalyzerKeyMatcher::is_explicit(analyzer_key)) {
        return bypass_error(analyzer_key);
    }
    if (match.empty()) {
        return no_reader_error();
    }

    // MATCH queries prefer FULLTEXT
    if (is_match_query(query_type)) {
        for (auto* e : match.candidates) {
            if (e->type == InvertedIndexReaderType::FULLTEXT) return e->reader;
        }
    }
    // EQUAL queries prefer STRING_TYPE
    if (is_equal_query(query_type)) {
        for (auto* e : match.candidates) {
            if (e->type == InvertedIndexReaderType::STRING_TYPE) return e->reader;
        }
    }

    return match.candidates.front()->reader;
}
```

#### 4. select_for_numeric (New Private Method)

**Responsibilities**:
- Handle numeric index selection (BKD priority)
- No analyzer key consideration
- Priority: RANGE→BKD, others→BKD>STRING_TYPE

```cpp
Result<ReaderPtr> InvertedIndexIterator::select_for_numeric(
        const MatchResult& match, InvertedIndexQueryType query_type) {

    if (match.empty()) {
        return no_reader_error();
    }

    // RANGE queries prefer BKD
    if (is_range_query(query_type)) {
        for (auto* e : match.candidates) {
            if (e->type == InvertedIndexReaderType::BKD) return e->reader;
        }
    }

    // Fallback priority: BKD > STRING_TYPE
    for (auto* e : match.candidates) {
        if (e->type == InvertedIndexReaderType::BKD) return e->reader;
    }
    for (auto* e : match.candidates) {
        if (e->type == InvertedIndexReaderType::STRING_TYPE) return e->reader;
    }

    return match.candidates.front()->reader;
}
```

## Query Type to Reader Type Mapping

| Column Type | Query Type | Preferred Reader | Fallback |
|-------------|------------|------------------|----------|
| STRING | MATCH_* | FULLTEXT | STRING_TYPE |
| STRING | EQUAL | STRING_TYPE | FULLTEXT |
| STRING | WILDCARD/REGEXP | STRING_TYPE | FULLTEXT |
| NUMERIC | RANGE (LT/GT/LE/GE) | BKD | STRING_TYPE |
| NUMERIC | EQUAL | BKD | STRING_TYPE |

## Future Extensions

### Adding BKD Support in Search DSL

When implementing `field:[1, 100]` or `field:1 to 2` syntax:

1. FE parses DSL and generates `RANGE` clause type
2. `FunctionSearch::clause_type_to_query_type` maps to `RANGE_QUERY`
3. `FieldReaderResolver::resolve` calls `select_best_reader` with numeric column type
4. `select_for_numeric` selects BKD reader

### Adding New Index Type (e.g., Vector)

```cpp
// Add new dispatch branch
if (is_vector_type(field_type)) {
    return select_for_vector(match, query_type);
}

// Implement selection logic
Result<ReaderPtr> select_for_vector(const MatchResult& match,
                                     InvertedIndexQueryType query_type) {
    // ANN query logic
}
```

### Adding Analyzer Selection in Search DSL

When implementing `field$analyzer_name:value` syntax:

1. FE parses and extracts analyzer_name from field specification
2. Pass analyzer_name to `FieldReaderResolver::resolve`
3. `AnalyzerKeyMatcher::match` finds matching reader
4. Existing text selection logic handles priority

## Testing Strategy

### Unit Tests for AnalyzerKeyMatcher

```cpp
TEST(AnalyzerKeyMatcher, ExactMatch) {
    // Setup: entries with ["chinese", "english", "__default__"]
    auto result = AnalyzerKeyMatcher::match("chinese", entries, index);
    EXPECT_EQ(result.candidates.size(), 1);
    EXPECT_EQ(result.candidates[0]->analyzer_key, "chinese");
    EXPECT_FALSE(result.used_fallback);
}

TEST(AnalyzerKeyMatcher, FallbackToDefault) {
    auto result = AnalyzerKeyMatcher::match("__default__", entries, index);
    EXPECT_TRUE(result.used_fallback);
    EXPECT_GT(result.candidates.size(), 1);  // All readers
}

TEST(AnalyzerKeyMatcher, IsExplicit) {
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit("chinese"));
    EXPECT_FALSE(AnalyzerKeyMatcher::is_explicit("__default__"));
    EXPECT_FALSE(AnalyzerKeyMatcher::is_explicit(""));
}
```

### Unit Tests for Selection Functions

```cpp
TEST(SelectForText, MatchQueryPreferFulltext) {
    // Setup: candidates with [FULLTEXT, STRING_TYPE]
    auto result = select_for_text(match, MATCH_ANY_QUERY, "__default__");
    EXPECT_EQ(result.value()->type(), FULLTEXT);
}

TEST(SelectForText, BypassWhenExplicitNotFound) {
    MatchResult empty_match;
    auto result = select_for_text(empty_match, MATCH_ANY_QUERY, "chinese");
    EXPECT_TRUE(result.error().is<INVERTED_INDEX_BYPASS>());
}

TEST(SelectForNumeric, RangeQueryPreferBKD) {
    // Setup: candidates with [BKD, STRING_TYPE]
    auto result = select_for_numeric(match, RANGE_QUERY);
    EXPECT_EQ(result.value()->type(), BKD);
}
```

## File Changes

| File | Change Type | Description |
|------|-------------|-------------|
| `be/src/olap/rowset/segment_v2/analyzer_key_matcher.h` | New | AnalyzerKeyMatcher class |
| `be/src/olap/rowset/segment_v2/analyzer_key_matcher.cpp` | New | Implementation |
| `be/src/olap/rowset/segment_v2/inverted_index_iterator.h` | Modify | Add private methods |
| `be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp` | Modify | Refactor select_best_reader |
| `be/test/olap/rowset/segment_v2/analyzer_key_matcher_test.cpp` | New | Unit tests |

## Implementation Plan

1. **Phase 1**: Extract AnalyzerKeyMatcher
   - Create new class with match() and is_explicit()
   - Add unit tests
   - No functional change to select_best_reader yet

2. **Phase 2**: Refactor select_best_reader
   - Delegate to AnalyzerKeyMatcher
   - Extract select_for_text and select_for_numeric
   - Verify existing tests pass

3. **Phase 3**: Add BKD support (future PR)
   - Implement select_for_numeric with BKD priority
   - Add RANGE/LIST support in search DSL

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Behavior regression | Comprehensive unit tests before refactoring |
| Performance overhead | MatchResult uses pointers, no deep copy |
| Missing edge cases | Review existing code paths carefully |

## References

- Task ID #25 in tasks.json
- Current implementation: `inverted_index_iterator.cpp:195-321`
- Related: ID #24 (InvertedIndexUtil refactoring pattern)
