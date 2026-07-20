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

#include <utility>

#include "format_v2/table_reader.h"

namespace doris::format {
struct DeleteFileDesc;
}
namespace doris::format::paimon {

class PaimonReader final : public format::TableReader {
public:
    ENABLE_FACTORY_CREATOR(PaimonReader);
    ~PaimonReader() final = default;
    Status prepare_split(const format::SplitReadOptions& options) override;

#ifdef BE_TEST
    void TEST_set_scan_params(TFileScanRangeParams* params) { _scan_params = params; }
    format::TableColumnMappingMode TEST_mapping_mode() const { return mapping_mode(); }
    Status TEST_annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
        return annotate_file_schema(file_schema);
    }
    Status TEST_parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                           bool* has_delete_file) {
        return _parse_deletion_vector_file(t_desc, desc, has_delete_file);
    }
#endif

protected:
    format::TableColumnMappingMode mapping_mode() const override;
    Status annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) override;

    Status _parse_deletion_vector_file(const TTableFormatFileDesc& t_desc, DeleteFileDesc* desc,
                                       bool* has_delete_file) override;

private:
    int64_t _split_schema_id = -1;
};

// Paimon scans can contain both native data-file splits and serialized JNI splits in the same
// SplitSource. FileScannerV2 owns one table reader for the scanner lifetime, so this reader keeps
// native and JNI child readers internally and dispatches each split to the matching child reader.
class PaimonHybridReader final : public format::TableReader {
public:
    ~PaimonHybridReader() override = default;

    Status init(format::TableReadOptions&& options) override;
    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    bool current_split_pruned() const override;
    bool current_split_uses_metadata_count() const override;
    Status abort_split() override;
    Status close() override;
    void set_batch_size(size_t batch_size) override;

#ifdef BE_TEST
    static bool TEST_is_jni_split(const TFileRangeDesc& range) { return _is_jni_split(range); }
    static Status TEST_to_file_format(const TFileRangeDesc& range,
                                      format::FileFormat* file_format) {
        return _to_file_format(range, file_format);
    }
    void TEST_install_batch_size_children() {
        _native_reader = std::make_unique<format::TableReader>();
        _jni_reader = std::make_unique<format::TableReader>();
    }
    std::pair<size_t, size_t> TEST_child_batch_sizes() const {
        return {_native_reader->TEST_batch_size(), _jni_reader->TEST_batch_size()};
    }
#endif

private:
    Status _ensure_current_split_reader(const format::SplitReadOptions& options);
    Status _init_child_reader(format::TableReader* reader, format::FileFormat file_format);
    Status _clone_conjuncts(VExprContextSPtrs* conjuncts) const;
    static bool _is_jni_split(const TFileRangeDesc& range);
    static Status _to_file_format(const TFileRangeDesc& range, format::FileFormat* file_format);

    std::unique_ptr<format::TableReader> _native_reader; // handle parquet/orc native splits
    std::unique_ptr<format::TableReader> _jni_reader;    // handle serialized JNI splits
    format::TableReader* _current_split_reader = nullptr;
};

} // namespace doris::format::paimon
