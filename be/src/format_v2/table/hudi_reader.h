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

#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "format_v2/table_reader.h"

namespace doris::format::hudi {

class HudiReader final : public format::TableReader {
public:
    ENABLE_FACTORY_CREATOR(HudiReader);
    ~HudiReader() final = default;

    Status prepare_split(const format::SplitReadOptions& options) override;

#ifdef BE_TEST
    void TEST_set_scan_params(TFileScanRangeParams* params) { _scan_params = params; }
    format::TableColumnMappingMode TEST_mapping_mode() const { return mapping_mode(); }
    Status TEST_annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
        return annotate_file_schema(file_schema);
    }
#endif

protected:
    format::TableColumnMappingMode mapping_mode() const override;
    Status annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) override;

private:
    int64_t _split_schema_id = -1;
};

// Hudi MOR scans can contain both JNI splits that need log-file merge semantics and native
// data-file splits without delta logs in the same SplitSource. FileScannerV2 owns one table reader
// for the scanner lifetime, so this reader keeps native and JNI child readers internally and
// dispatches each split to the matching child reader.
class HudiHybridReader final : public format::TableReader {
public:
    ~HudiHybridReader() override = default;

    Status init(format::TableReadOptions&& options) override;
    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    bool current_split_pruned() const override;
    bool current_split_uses_metadata_count() const override;
    Status abort_split() override;
    Status close() override;
    void set_batch_size(size_t batch_size) override;
    Status append_conjuncts(const VExprContextSPtrs& conjuncts) override;
    const format::MaterializedBlockStats& last_materialized_block_stats() const override;
    int64_t condition_cache_hit_count() const override;

#ifdef BE_TEST
    void TEST_install_batch_size_children() {
        _native_reader = std::make_unique<format::TableReader>();
        _jni_reader = std::make_unique<format::TableReader>();
    }
    std::pair<size_t, size_t> TEST_child_batch_sizes() const {
        return {_native_reader->TEST_batch_size(), _jni_reader->TEST_batch_size()};
    }
    void TEST_set_child_condition_cache_hits(int64_t native_hits, int64_t jni_hits) {
        _native_reader->TEST_set_condition_cache_hit_count(native_hits);
        _jni_reader->TEST_set_condition_cache_hit_count(jni_hits);
    }
    void TEST_set_child_reader_factories(
            std::function<std::unique_ptr<format::TableReader>()> native_factory,
            std::function<std::unique_ptr<format::TableReader>()> jni_factory) {
        _test_native_reader_factory = std::move(native_factory);
        _test_jni_reader_factory = std::move(jni_factory);
    }
#endif

private:
    Status _ensure_current_split_reader(const format::SplitReadOptions& options);
    Status _init_child_reader(format::TableReader* reader, format::FileFormat file_format);
    Status _clone_conjuncts(VExprContextSPtrs* conjuncts) const;
    static TFileFormatType::type _range_format_type(const TFileScanRangeParams& params,
                                                    const TFileRangeDesc& range);
    static bool _is_jni_split(const TFileScanRangeParams& params, const TFileRangeDesc& range);
    static Status _to_file_format(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                                  format::FileFormat* file_format);

    std::unique_ptr<format::TableReader> _native_reader; // handle native parquet/orc splits
    std::unique_ptr<format::TableReader> _jni_reader;    // handle MOR JNI splits
    format::TableReader* _current_split_reader = nullptr;
#ifdef BE_TEST
    std::function<std::unique_ptr<format::TableReader>()> _test_native_reader_factory;
    std::function<std::unique_ptr<format::TableReader>()> _test_jni_reader_factory;
#endif
};

} // namespace doris::format::hudi
