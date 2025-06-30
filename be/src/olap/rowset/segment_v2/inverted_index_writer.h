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

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/util/bkd/bkd_writer.h>

#include "CLucene/search/Similarity.h"
#include "olap/inverted_index_parser.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/index_writer.h"
#include "olap/types.h"

namespace doris::segment_v2 {

template <FieldType field_type>
class InvertedIndexColumnWriterImpl : public IndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    InvertedIndexColumnWriterImpl(const std::string& field_name, IndexFileWriter* index_file_writer,
                                  const TabletIndex* index_meta, const bool single_field = true);
    ~InvertedIndexColumnWriterImpl() override;

    Status init() override;
    void close_on_error() override;
    Status init_bkd_index();
    Result<std::unique_ptr<lucene::util::Reader>> create_char_string_reader(
            CharFilterMap& char_filter_map);
    Status open_index_directory();
    std::unique_ptr<lucene::index::IndexWriter> create_index_writer();
    Status create_field(lucene::document::Field** field);
    Result<std::shared_ptr<lucene::analysis::Analyzer>> create_analyzer(
            std::shared_ptr<InvertedIndexCtx>& inverted_index_ctx);
    Status init_fulltext_index();
    Status add_document();
    Status add_null_document();
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(const uint8_t* null_map, size_t num_rows) override;
    Status new_inverted_index_field(const char* field_value_data, size_t field_value_size);
    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field);
    void new_field_value(const char* s, size_t len, lucene::document::Field* field);
    void new_field_char_value(const char* s, size_t len, lucene::document::Field* field);
    Status add_values(const std::string fn, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr,
                            const uint8_t* nested_null_map, const uint8_t* offsets_ptr,
                            size_t count) override;
    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override;
    Status add_numeric_values(const void* values, size_t count);
    Status add_value(const CppType& value);
    int64_t size() const override;
    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out);
    Status finish() override;

private:
    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    roaring::Roaring _null_bitmap;
    uint64_t _reverted_index_size;

    std::unique_ptr<lucene::document::Document> _doc = nullptr;
    lucene::document::Field* _field = nullptr;
    bool _single_field = true;
    // Since _index_writer's write.lock is created by _dir.lockFactory,
    // _dir must destruct after _index_writer, so _dir must be defined before _index_writer.
    std::shared_ptr<DorisFSDirectory> _dir = nullptr;
    std::unique_ptr<lucene::index::IndexWriter> _index_writer = nullptr;
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer = nullptr;
    std::unique_ptr<lucene::util::Reader> _char_string_reader = nullptr;
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer = nullptr;
    InvertedIndexCtxSPtr _inverted_index_ctx = nullptr;
    const KeyCoder* _value_key_coder;
    const TabletIndex* _index_meta;
    std::wstring _field_name;
    IndexFileWriter* _index_file_writer;
    uint32_t _ignore_above;
    bool _should_analyzer = false;
};

} // namespace doris::segment_v2