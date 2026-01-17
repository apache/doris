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
#include <unordered_map>
#include <vector>

#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_reader.h"

namespace doris::segment_v2 {

struct InvertedIndexParam {
    std::string column_name;
    vectorized::DataTypePtr column_type;
    const void* query_value;
    InvertedIndexQueryType query_type;
    uint32_t num_rows;
    std::shared_ptr<roaring::Roaring> roaring;
    bool skip_try = false;
    // Pointer to analyzer context (can be nullptr if not needed)
    // Used by FullTextIndexReader for tokenization
    const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr;
};

class InvertedIndexIterator : public IndexIterator {
public:
    InvertedIndexIterator();
    ~InvertedIndexIterator() override = default;

    void add_reader(InvertedIndexReaderType type, const InvertedIndexReaderPtr& reader);

    // Note: analyzer_ctx is now passed via InvertedIndexParam.analyzer_ctx
    Status read_from_index(const IndexParam& param) override;

    Status read_null_bitmap(InvertedIndexQueryCacheHandle* cache_handle) override;

    [[nodiscard]] Result<bool> has_null() override;

    IndexReaderPtr get_reader(IndexReaderType reader_type) const override;

    [[nodiscard]] Result<InvertedIndexReaderPtr> select_best_reader(
            const vectorized::DataTypePtr& column_type, InvertedIndexQueryType query_type,
            const std::string& analyzer_key);
    [[nodiscard]] Result<InvertedIndexReaderPtr> select_best_reader(
            const std::string& analyzer_key);

private:
    ENABLE_FACTORY_CREATOR(InvertedIndexIterator);

    Status try_read_from_inverted_index(const InvertedIndexReaderPtr& reader,
                                        const std::string& column_name, const void* query_value,
                                        InvertedIndexQueryType query_type, size_t* count);

    struct ReaderEntry {
        InvertedIndexReaderType type;
        std::string analyzer_key;
        InvertedIndexReaderPtr reader;
    };

    // Result of find_reader_candidates.
    // SAFETY: The pointers in 'candidates' are valid only within the scope of
    // select_best_reader(). Do NOT store CandidateResult beyond this scope.
    // This is safe because:
    // 1. add_reader() is called only during initialization phase
    // 2. read_from_index()/select_best_reader() is called only during query phase
    // 3. These two phases never overlap
    struct CandidateResult {
        std::vector<const ReaderEntry*> candidates;
        bool used_fallback = false;
    };

    // Find candidate readers with fallback strategy:
    // 1. Exact match on analyzer_key
    // 2. Fallback to default analyzer key
    // 3. Fallback to all readers
    // NOTE: analyzer_key is assumed to be already normalized (lowercase).
    [[nodiscard]] CandidateResult find_reader_candidates(const std::string& normalized_key) const;

    // Normalize and validate analyzer_key, returning normalized form.
    // Empty input returns INVERTED_INDEX_DEFAULT_ANALYZER_KEY.
    static std::string ensure_normalized_key(const std::string& analyzer_key);

    // THREAD SAFETY: _reader_entries and _key_to_entries are populated during initialization
    // phase (via add_reader) and only read during query phase (via read_from_index/select_best_reader).
    // These two phases are guaranteed not to overlap, so no synchronization is needed.
    // Do NOT call add_reader() after any read_from_index() call on the same iterator.
    std::vector<ReaderEntry> _reader_entries;

    // Index for O(1) lookup by analyzer_key. Maps normalized key to indices in _reader_entries.
    // Built incrementally in add_reader().
    std::unordered_map<std::string, std::vector<size_t>> _key_to_entries;
};

} // namespace doris::segment_v2