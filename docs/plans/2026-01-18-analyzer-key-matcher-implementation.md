# AnalyzerKeyMatcher Extraction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract analyzer key matching logic from `select_best_reader` into independent `AnalyzerKeyMatcher` class, then refactor `select_best_reader` to use simple dispatch by column type.

**Architecture:** Create `AnalyzerKeyMatcher` as a stateless utility class with static methods. Refactor `select_best_reader` to delegate analyzer matching to this class, then dispatch to `select_for_text` or `select_for_numeric` based on column type.

**Tech Stack:** C++17, GTest for unit tests

**Working Directory:** `/mnt/disk1/jiangkai/workspace/src/doris/.worktrees/analyzer-key-matcher`

---

## Task 1: Create AnalyzerKeyMatcher Header

**Files:**
- Create: `be/src/olap/rowset/segment_v2/analyzer_key_matcher.h`

**Step 1: Create the header file**

```cpp
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/inverted_index_query_type.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"

namespace doris::segment_v2 {

// Forward declaration
struct ReaderEntry;

// Result of analyzer key matching operation.
// Contains candidate readers that match the requested analyzer key.
struct AnalyzerMatchResult {
    // Pointers to matching reader entries. Valid only within the scope of
    // the InvertedIndexIterator that owns the entries.
    std::vector<const ReaderEntry*> candidates;

    // True if fallback strategy was used (no exact match found)
    bool used_fallback = false;

    // Convenience methods
    bool empty() const { return candidates.empty(); }
    size_t size() const { return candidates.size(); }
};

// Utility class for matching analyzer keys against reader entries.
// Stateless - all methods are static.
//
// Matching strategy:
// 1. Exact match on normalized analyzer key
// 2. If key is __default__, fallback to all available readers
// 3. Otherwise, no candidates (caller decides whether to bypass)
class AnalyzerKeyMatcher {
public:
    // Match analyzer key against reader entries.
    // @param analyzer_key: Normalized analyzer key (lowercase, trimmed)
    // @param entries: Available reader entries
    // @param key_index: Index mapping analyzer_key -> entry indices
    // @return MatchResult with candidates and fallback flag
    static AnalyzerMatchResult match(
            std::string_view analyzer_key,
            const std::vector<ReaderEntry>& entries,
            const std::unordered_map<std::string, std::vector<size_t>>& key_index);

    // Check if analyzer key is explicitly specified (not default).
    // Explicit means user wrote "USING ANALYZER xxx" or "field$xxx:value".
    // @param key: Normalized analyzer key
    // @return true if key is non-empty and not __default__
    static bool is_explicit(std::string_view key) {
        return !key.empty() && key != INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }

    // Check if fallback is allowed for this key.
    // Only __default__ key allows fallback to all readers.
    static bool allows_fallback(std::string_view key) {
        return key.empty() || key == INVERTED_INDEX_DEFAULT_ANALYZER_KEY;
    }
};

} // namespace doris::segment_v2
```

**Step 2: Commit**

```bash
git add be/src/olap/rowset/segment_v2/analyzer_key_matcher.h
git commit -m "[refactor](inverted index) Add AnalyzerKeyMatcher header

Extract analyzer key matching logic into independent utility class.
This is phase 1 of select_best_reader refactoring (task #25).

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 2: Create AnalyzerKeyMatcher Implementation

**Files:**
- Create: `be/src/olap/rowset/segment_v2/analyzer_key_matcher.cpp`
- Reference: `be/src/olap/rowset/segment_v2/inverted_index_iterator.h` (for ReaderEntry struct)

**Step 1: Create the implementation file**

```cpp
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/rowset/segment_v2/analyzer_key_matcher.h"

#include "olap/rowset/segment_v2/inverted_index_iterator.h"

namespace doris::segment_v2 {

AnalyzerMatchResult AnalyzerKeyMatcher::match(
        std::string_view analyzer_key,
        const std::vector<ReaderEntry>& entries,
        const std::unordered_map<std::string, std::vector<size_t>>& key_index) {
    AnalyzerMatchResult result;

    if (entries.empty()) {
        return result;
    }

    // Convert string_view to string for map lookup
    std::string key_str(analyzer_key);

    // Step 1: Try exact match
    auto it = key_index.find(key_str);
    if (it != key_index.end() && !it->second.empty()) {
        result.candidates.reserve(it->second.size());
        for (size_t idx : it->second) {
            if (idx < entries.size()) {
                result.candidates.push_back(&entries[idx]);
            }
        }
        if (!result.candidates.empty()) {
            return result;
        }
    }

    // Step 2: If no exact match and fallback is allowed, return all readers
    if (allows_fallback(analyzer_key)) {
        result.candidates.reserve(entries.size());
        for (const auto& entry : entries) {
            result.candidates.push_back(&entry);
        }
        result.used_fallback = true;
    }

    // Step 3: If explicit key with no match, return empty (caller will bypass)
    return result;
}

} // namespace doris::segment_v2
```

**Step 2: Add to CMakeLists if needed**

Check `be/src/olap/rowset/segment_v2/CMakeLists.txt` - if it uses GLOB, no change needed. Otherwise add the new cpp file.

**Step 3: Commit**

```bash
git add be/src/olap/rowset/segment_v2/analyzer_key_matcher.cpp
git commit -m "[refactor](inverted index) Implement AnalyzerKeyMatcher::match

Implement core matching logic:
1. Exact match on analyzer key using index
2. Fallback to all readers for __default__ key
3. Return empty for explicit key with no match

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 3: Write Unit Tests for AnalyzerKeyMatcher

**Files:**
- Create: `be/test/olap/rowset/segment_v2/analyzer_key_matcher_test.cpp`

**Step 1: Create the test file**

```cpp
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/rowset/segment_v2/analyzer_key_matcher.h"

#include <gtest/gtest.h>

#include "olap/rowset/segment_v2/inverted_index_iterator.h"

namespace doris::segment_v2 {

class AnalyzerKeyMatcherTest : public testing::Test {
protected:
    void SetUp() override {
        // Create test entries
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "chinese", nullptr});
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "english", nullptr});
        entries_.push_back({InvertedIndexReaderType::STRING_TYPE, "__default__", nullptr});

        // Build index
        for (size_t i = 0; i < entries_.size(); ++i) {
            key_index_[entries_[i].analyzer_key].push_back(i);
        }
    }

    std::vector<ReaderEntry> entries_;
    std::unordered_map<std::string, std::vector<size_t>> key_index_;
};

// Test exact match
TEST_F(AnalyzerKeyMatcherTest, ExactMatch) {
    auto result = AnalyzerKeyMatcher::match("chinese", entries_, key_index_);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result.candidates[0]->analyzer_key, "chinese");
    EXPECT_FALSE(result.used_fallback);
}

TEST_F(AnalyzerKeyMatcherTest, ExactMatchEnglish) {
    auto result = AnalyzerKeyMatcher::match("english", entries_, key_index_);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result.candidates[0]->analyzer_key, "english");
    EXPECT_FALSE(result.used_fallback);
}

// Test fallback for default key
TEST_F(AnalyzerKeyMatcherTest, DefaultKeyFallbacksToAll) {
    auto result = AnalyzerKeyMatcher::match("__default__", entries_, key_index_);

    // Should match the __default__ entry exactly first, but since we also
    // have it in the index, it returns just that one
    // Wait - the current impl does exact match first. Let me re-check.
    // Actually __default__ IS in our entries, so it will exact match.
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result.candidates[0]->analyzer_key, "__default__");
    EXPECT_FALSE(result.used_fallback);
}

TEST_F(AnalyzerKeyMatcherTest, EmptyKeyFallbacksToAll) {
    auto result = AnalyzerKeyMatcher::match("", entries_, key_index_);

    // Empty key should fallback to all readers
    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 3);
    EXPECT_TRUE(result.used_fallback);
}

// Test explicit key with no match returns empty
TEST_F(AnalyzerKeyMatcherTest, ExplicitKeyNoMatchReturnsEmpty) {
    auto result = AnalyzerKeyMatcher::match("japanese", entries_, key_index_);

    EXPECT_TRUE(result.empty());
    EXPECT_FALSE(result.used_fallback);
}

// Test is_explicit
TEST_F(AnalyzerKeyMatcherTest, IsExplicit) {
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit("chinese"));
    EXPECT_TRUE(AnalyzerKeyMatcher::is_explicit("english"));
    EXPECT_FALSE(AnalyzerKeyMatcher::is_explicit("__default__"));
    EXPECT_FALSE(AnalyzerKeyMatcher::is_explicit(""));
}

// Test allows_fallback
TEST_F(AnalyzerKeyMatcherTest, AllowsFallback) {
    EXPECT_TRUE(AnalyzerKeyMatcher::allows_fallback("__default__"));
    EXPECT_TRUE(AnalyzerKeyMatcher::allows_fallback(""));
    EXPECT_FALSE(AnalyzerKeyMatcher::allows_fallback("chinese"));
    EXPECT_FALSE(AnalyzerKeyMatcher::allows_fallback("english"));
}

// Test empty entries
TEST_F(AnalyzerKeyMatcherTest, EmptyEntries) {
    std::vector<ReaderEntry> empty_entries;
    std::unordered_map<std::string, std::vector<size_t>> empty_index;

    auto result = AnalyzerKeyMatcher::match("chinese", empty_entries, empty_index);
    EXPECT_TRUE(result.empty());
}

// Test multiple entries with same analyzer key
class AnalyzerKeyMatcherMultiEntryTest : public testing::Test {
protected:
    void SetUp() override {
        // Multiple readers with same analyzer key (different types)
        entries_.push_back({InvertedIndexReaderType::FULLTEXT, "chinese", nullptr});
        entries_.push_back({InvertedIndexReaderType::STRING_TYPE, "chinese", nullptr});

        for (size_t i = 0; i < entries_.size(); ++i) {
            key_index_[entries_[i].analyzer_key].push_back(i);
        }
    }

    std::vector<ReaderEntry> entries_;
    std::unordered_map<std::string, std::vector<size_t>> key_index_;
};

TEST_F(AnalyzerKeyMatcherMultiEntryTest, MatchReturnsAllWithSameKey) {
    auto result = AnalyzerKeyMatcher::match("chinese", entries_, key_index_);

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 2);
    EXPECT_FALSE(result.used_fallback);

    // Both should be "chinese"
    for (const auto* entry : result.candidates) {
        EXPECT_EQ(entry->analyzer_key, "chinese");
    }
}

} // namespace doris::segment_v2
```

**Step 2: Run tests to verify they compile and pass**

```bash
cd /mnt/disk1/jiangkai/workspace/src/doris/.worktrees/analyzer-key-matcher
./run-be-ut.sh --run --filter="AnalyzerKeyMatcher*"
```

Expected: All tests pass

**Step 3: Commit**

```bash
git add be/test/olap/rowset/segment_v2/analyzer_key_matcher_test.cpp
git commit -m "[test](inverted index) Add unit tests for AnalyzerKeyMatcher

Test coverage:
- Exact match by analyzer key
- Fallback to all readers for default/empty key
- No match for explicit non-existent key
- is_explicit() and allows_fallback() helpers
- Edge cases: empty entries, multiple entries with same key

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 4: Extract ReaderEntry to Shared Header

**Files:**
- Modify: `be/src/olap/rowset/segment_v2/inverted_index_iterator.h`
- Create: `be/src/olap/rowset/segment_v2/reader_entry.h` (optional, or keep in iterator.h)

**Step 1: Move ReaderEntry struct before InvertedIndexIterator class**

The struct needs to be visible to analyzer_key_matcher.h. Check if it's already accessible or needs to be moved.

Read the current inverted_index_iterator.h to see if ReaderEntry is public or private.

**Step 2: If ReaderEntry is private, make it public**

Move `struct ReaderEntry` outside the class or to a separate header if needed for better organization.

**Step 3: Update includes in analyzer_key_matcher.cpp**

**Step 4: Commit if changes needed**

```bash
git add be/src/olap/rowset/segment_v2/inverted_index_iterator.h
git commit -m "[refactor](inverted index) Make ReaderEntry accessible to AnalyzerKeyMatcher

Move ReaderEntry definition to be accessible from analyzer_key_matcher.h.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 5: Add select_for_text Private Method

**Files:**
- Modify: `be/src/olap/rowset/segment_v2/inverted_index_iterator.h`
- Modify: `be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp`

**Step 1: Add method declaration to header**

In `inverted_index_iterator.h`, add to private section:

```cpp
// Select best reader for text (string) columns.
// Handles FULLTEXT vs STRING_TYPE priority based on query type.
// Returns BYPASS error if explicit analyzer not found.
[[nodiscard]] Result<InvertedIndexReaderPtr> select_for_text(
        const AnalyzerMatchResult& match,
        InvertedIndexQueryType query_type,
        const std::string& analyzer_key);
```

**Step 2: Add include for AnalyzerKeyMatcher**

```cpp
#include "olap/rowset/segment_v2/analyzer_key_matcher.h"
```

**Step 3: Implement select_for_text in cpp file**

```cpp
Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_for_text(
        const AnalyzerMatchResult& match,
        InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {
    // Bypass: explicit analyzer specified but not found
    if (match.empty() && AnalyzerKeyMatcher::is_explicit(analyzer_key)) {
        return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                "No inverted index reader found for analyzer '{}'. "
                "The index for this analyzer may not be built yet.",
                analyzer_key));
    }

    if (match.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers for text column."));
    }

    // MATCH queries prefer FULLTEXT
    if (is_match_query(query_type)) {
        for (const auto* entry : match.candidates) {
            if (entry->type == InvertedIndexReaderType::FULLTEXT) {
                return entry->reader;
            }
        }
    }

    // EQUAL queries prefer STRING_TYPE
    if (is_equal_query(query_type)) {
        for (const auto* entry : match.candidates) {
            if (entry->type == InvertedIndexReaderType::STRING_TYPE) {
                return entry->reader;
            }
        }
    }

    // Default: return first candidate
    return match.candidates.front()->reader;
}
```

**Step 4: Commit**

```bash
git add be/src/olap/rowset/segment_v2/inverted_index_iterator.h
git add be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp
git commit -m "[refactor](inverted index) Add select_for_text method

Extract text column reader selection logic into dedicated method.
Handles FULLTEXT vs STRING_TYPE priority and bypass for missing analyzer.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 6: Add select_for_numeric Private Method

**Files:**
- Modify: `be/src/olap/rowset/segment_v2/inverted_index_iterator.h`
- Modify: `be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp`

**Step 1: Add method declaration to header**

```cpp
// Select best reader for numeric columns.
// Handles BKD priority for range queries.
[[nodiscard]] Result<InvertedIndexReaderPtr> select_for_numeric(
        const AnalyzerMatchResult& match,
        InvertedIndexQueryType query_type);
```

**Step 2: Implement in cpp file**

```cpp
Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_for_numeric(
        const AnalyzerMatchResult& match,
        InvertedIndexQueryType query_type) {
    if (match.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers for numeric column."));
    }

    // RANGE queries prefer BKD
    if (is_range_query(query_type)) {
        for (const auto* entry : match.candidates) {
            if (entry->type == InvertedIndexReaderType::BKD) {
                return entry->reader;
            }
        }
    }

    // Fallback priority: BKD > STRING_TYPE > others
    for (const auto* entry : match.candidates) {
        if (entry->type == InvertedIndexReaderType::BKD) {
            return entry->reader;
        }
    }
    for (const auto* entry : match.candidates) {
        if (entry->type == InvertedIndexReaderType::STRING_TYPE) {
            return entry->reader;
        }
    }

    // Last resort: first available
    return match.candidates.front()->reader;
}
```

**Step 3: Commit**

```bash
git add be/src/olap/rowset/segment_v2/inverted_index_iterator.h
git add be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp
git commit -m "[refactor](inverted index) Add select_for_numeric method

Extract numeric column reader selection logic into dedicated method.
Handles BKD priority for range queries, with fallback to STRING_TYPE.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 7: Refactor select_best_reader to Use New Components

**Files:**
- Modify: `be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp`

**Step 1: Refactor the main select_best_reader method**

Replace the existing implementation with:

```cpp
Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const vectorized::DataTypePtr& column_type,
        InvertedIndexQueryType query_type,
        const std::string& analyzer_key) {
    if (_reader_entries.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    // Normalize once at entry point
    const std::string normalized_key = ensure_normalized_key(analyzer_key);

    // Single reader optimization
    if (_reader_entries.size() == 1) {
        const auto& entry = _reader_entries.front();
        if (AnalyzerKeyMatcher::is_explicit(normalized_key) &&
            entry.analyzer_key != normalized_key) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "No inverted index reader found for analyzer '{}'. "
                    "Available analyzer: '{}'.",
                    normalized_key, entry.analyzer_key));
        }
        return entry.reader;
    }

    // Match analyzer key
    auto match = AnalyzerKeyMatcher::match(normalized_key, _reader_entries, _key_to_entries);

    // Dispatch by column type
    const auto field_type = column_type->get_storage_field_type();

    if (is_string_type(field_type)) {
        return select_for_text(match, query_type, normalized_key);
    }

    if (is_numeric_type(field_type)) {
        return select_for_numeric(match, query_type);
    }

    // Default: return first candidate or error
    if (match.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers for column type."));
    }
    return match.candidates.front()->reader;
}
```

**Step 2: Add is_numeric_type helper if not exists**

Check if `is_numeric_type` exists in the codebase. If not, add:

```cpp
inline bool is_numeric_type(FieldType type) {
    return type == FieldType::OLAP_FIELD_TYPE_TINYINT ||
           type == FieldType::OLAP_FIELD_TYPE_SMALLINT ||
           type == FieldType::OLAP_FIELD_TYPE_INT ||
           type == FieldType::OLAP_FIELD_TYPE_BIGINT ||
           type == FieldType::OLAP_FIELD_TYPE_LARGEINT ||
           type == FieldType::OLAP_FIELD_TYPE_FLOAT ||
           type == FieldType::OLAP_FIELD_TYPE_DOUBLE ||
           type == FieldType::OLAP_FIELD_TYPE_DECIMAL ||
           type == FieldType::OLAP_FIELD_TYPE_DECIMAL32 ||
           type == FieldType::OLAP_FIELD_TYPE_DECIMAL64 ||
           type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I ||
           type == FieldType::OLAP_FIELD_TYPE_DECIMAL256;
}
```

**Step 3: Also update the overloaded select_best_reader (without column_type)**

```cpp
Result<InvertedIndexReaderPtr> InvertedIndexIterator::select_best_reader(
        const std::string& analyzer_key) {
    if (_reader_entries.empty()) {
        return ResultError(Status::RuntimeError(
                "No available inverted index readers. Check if index is properly initialized."));
    }

    const std::string normalized_key = ensure_normalized_key(analyzer_key);

    // Single reader optimization
    if (_reader_entries.size() == 1) {
        const auto& entry = _reader_entries.front();
        if (AnalyzerKeyMatcher::is_explicit(normalized_key) &&
            entry.analyzer_key != normalized_key) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "No inverted index reader found for analyzer '{}'. "
                    "Available analyzer: '{}'.",
                    normalized_key, entry.analyzer_key));
        }
        return entry.reader;
    }

    // Match and return first candidate
    auto match = AnalyzerKeyMatcher::match(normalized_key, _reader_entries, _key_to_entries);

    if (match.empty()) {
        if (AnalyzerKeyMatcher::is_explicit(normalized_key)) {
            return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "No inverted index reader found for analyzer '{}'.",
                    normalized_key));
        }
        return ResultError(Status::RuntimeError(
                "No available inverted index readers."));
    }

    return match.candidates.front()->reader;
}
```

**Step 4: Remove old find_reader_candidates if now unused**

The old `find_reader_candidates` method can be removed as `AnalyzerKeyMatcher::match` replaces it.

**Step 5: Commit**

```bash
git add be/src/olap/rowset/segment_v2/inverted_index_iterator.cpp
git add be/src/olap/rowset/segment_v2/inverted_index_iterator.h
git commit -m "[refactor](inverted index) Refactor select_best_reader to use AnalyzerKeyMatcher

- Delegate analyzer matching to AnalyzerKeyMatcher::match
- Dispatch by column type to select_for_text or select_for_numeric
- Remove old find_reader_candidates method
- Add is_numeric_type helper for column type dispatch

This completes phase 2 of task #25 refactoring.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 8: Run Existing Tests to Verify No Regression

**Step 1: Run inverted index related unit tests**

```bash
cd /mnt/disk1/jiangkai/workspace/src/doris/.worktrees/analyzer-key-matcher
./run-be-ut.sh --run --filter="*InvertedIndex*"
```

Expected: All existing tests pass

**Step 2: Run the new AnalyzerKeyMatcher tests**

```bash
./run-be-ut.sh --run --filter="AnalyzerKeyMatcher*"
```

Expected: All new tests pass

**Step 3: If any test fails, fix and re-run**

---

## Task 9: Build and Verify Compilation

**Step 1: Build BE**

```bash
cd /mnt/disk1/jiangkai/workspace/src/doris/.worktrees/analyzer-key-matcher
source env.sh
./build.sh --be -j 100
```

Expected: Build succeeds with no errors

**Step 2: If build fails, fix compilation errors**

---

## Task 10: Final Cleanup and Summary Commit

**Step 1: Review all changes**

```bash
git log --oneline -10
git diff HEAD~5..HEAD --stat
```

**Step 2: Create summary commit if needed**

If there were any fixup commits, squash them appropriately.

**Step 3: Push to remote (optional, based on workflow)**

```bash
git push -u origin refactor/analyzer-key-matcher
```

---

## Verification Checklist

Before considering this task complete:

- [ ] `AnalyzerKeyMatcher` class created with `match()`, `is_explicit()`, `allows_fallback()`
- [ ] Unit tests for AnalyzerKeyMatcher all pass
- [ ] `select_for_text` and `select_for_numeric` methods added
- [ ] `select_best_reader` refactored to use new components
- [ ] Old `find_reader_candidates` removed (if applicable)
- [ ] All existing inverted index tests pass
- [ ] BE builds successfully
- [ ] Code follows project style (clang-format)
