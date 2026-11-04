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

#include <cstdint>
#include <string>

#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_index_storage.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/index_common.h"
#include "storage/index/index_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/once.h"
namespace doris::segment_v2 {

struct AnnTopNParam;
struct AnnRangeSearchParams;
struct AnnRangeSearchResult;
struct IndexSearchResult;

class IndexFileReader;
class IndexIterator;

class AnnIndexReader : public IndexReader {
public:
    AnnIndexReader(const TabletIndex* index_meta,
                   std::shared_ptr<IndexFileReader> index_file_reader, std::string rowset_id = "",
                   uint32_t segment_id = 0, size_t rows_of_segment = 0);
    ~AnnIndexReader() override = default;

    Status load_index(io::IOContext* io_ctx);

    // Try to load index, return true if successful, false if failed
    // This method is used to check if index can be loaded before query
    bool try_load_index(io::IOContext* io_ctx);

    Status query(io::IOContext* io_ctx, AnnTopNParam* param, AnnIndexStats* stats);

    Status range_search(const AnnRangeSearchParams& params,
                        const VectorSearchUserParams& custom_params, AnnRangeSearchResult* result,
                        AnnIndexStats* stats, io::IOContext* io_ctx = nullptr);

    IndexType index_type() override { return IndexType::ANN; }

    uint64_t get_index_id() const override { return _index_meta.index_id(); }

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;

    AnnIndexMetric get_metric_type() const { return _metric_type; }

    size_t get_dimension() const;

private:
    TabletIndex _index_meta;
    std::shared_ptr<IndexFileReader> _index_file_reader;
    // IMPORTANT: _source MUST be declared before _vector_index so that it is
    // destroyed AFTER _vector_index (C++ destroys members in reverse declaration order).
    // For IVF_ON_DISK, the CachedRandomAccessReader inside _vector_index keeps an
    // AnnIndexInput it opened from _source and reads it on every search. An input may
    // use what its source holds -- on V1/V2 it reads through the compound reader's one
    // file stream -- so a source destroyed first would leave that input dangling.
    std::unique_ptr<AnnIndexSource> _source;
    std::unique_ptr<VectorIndex> _vector_index;
    AnnIndexType _index_type;
    AnnIndexMetric _metric_type;
    size_t _dim;
    std::string _rowset_id;
    uint32_t _segment_id;
    size_t _rows_of_segment = 0;
    DorisCallOnce<Status> _load_index_once;
};

using AnnIndexReaderPtr = std::shared_ptr<AnnIndexReader>;
} // namespace doris::segment_v2
