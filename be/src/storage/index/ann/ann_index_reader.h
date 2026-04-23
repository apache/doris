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

#include "runtime/runtime_state.h"
#include "storage/index/ann/ann_index.h"
#include "storage/index/ann/ann_search_params.h"
#include "storage/index/index_reader.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
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
                   std::shared_ptr<IndexFileReader> index_file_reader);
    ~AnnIndexReader() override = default;

    static void update_result(const IndexSearchResult&, std::vector<float>& distance,
                              roaring::Roaring& row_id);

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
    // IMPORTANT: _compound_dir MUST be declared before _vector_index so that it is
    // destroyed AFTER _vector_index (C++ destroys members in reverse declaration order).
    // For IVF_ON_DISK, the CachedRandomAccessReader inside _vector_index holds a cloned
    // CSIndexInput whose `base` raw pointer references the compound directory's underlying
    // stream. If _compound_dir were destroyed first, that `base` would dangle, causing
    // a use-after-free when ~CachedRandomAccessReader() calls _input->close().
    std::unique_ptr<DorisCompoundReader, DirectoryDeleter> _compound_dir;
    std::unique_ptr<VectorIndex> _vector_index;
    AnnIndexType _index_type;
    AnnIndexMetric _metric_type;
    size_t _dim;
    DorisCallOnce<Status> _load_index_once;
};

using AnnIndexReaderPtr = std::shared_ptr<AnnIndexReader>;
} // namespace doris::segment_v2
