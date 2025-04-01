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

#include "olap/rowset/segment_v2/index_writer.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/util/bkd/bkd_writer.h>
#include <glog/logging.h>

#include <limits>
#include <memory>
#include <ostream>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif

#include "CLucene/analysis/standard95/StandardAnalyzer.h"

#ifdef __clang__
#pragma clang diagnostic pop
#endif

#include "common/config.h"
#include "gutil/strings/strip.h"
#include "olap/field.h"
#include "olap/inverted_index_parser.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/analyzer.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/x_index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/collection_value.h"
#include "runtime/exec_env.h"
#include "util/debug_points.h"
#include "util/faststring.h"
#include "util/slice.h"
#include "util/string_util.h"
#include "olap/rowset/segment_v2/index_writer.h"

namespace doris::segment_v2 {

template <FieldType field_type>
class InvertedIndexColumnWriter : public IndexColumnWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;

    explicit InvertedIndexColumnWriter(const std::string& field_name,
                                      XIndexFileWriter* index_file_writer,
                                      const TabletIndex* index_meta,
                                      const bool single_field = true);

    ~InvertedIndexColumnWriter() override;

    Status init() override;
    void close_on_error() override;
    Status add_nulls(uint32_t count) override;
    Status add_array_nulls(uint32_t row_id) override;
    Status add_values(const std::string fn, const void* values, size_t count) override;
    Status add_array_values(size_t field_size, const void* value_ptr, const uint8_t* null_map,
                            const uint8_t* offsets_ptr, size_t count) override;
    Status add_array_values(size_t field_size, const CollectionValue* values,
                            size_t count) override;
    int64_t size() const override;
    Status finish() override;

private:
    Status init_bkd_index();
    Result<std::unique_ptr<lucene::util::Reader>> create_char_string_reader(
            CharFilterMap& char_filter_map);
    Status open_index_directory();
    std::unique_ptr<lucene::index::IndexWriter> create_index_writer();
    Status create_field(lucene::document::Field** field);
    Result<std::unique_ptr<lucene::analysis::Analyzer>> create_analyzer(
            std::shared_ptr<InvertedIndexCtx>& inverted_index_ctx);
    Status init_fulltext_index();
    Status add_document();
    Status add_null_document();
    Status new_inverted_index_field(const char* field_value_data, size_t field_value_size);
    void new_char_token_stream(const char* s, size_t len, lucene::document::Field* field);
    void new_field_value(const char* s, size_t len, lucene::document::Field* field);
    void new_field_char_value(const char* s, size_t len, lucene::document::Field* field);
    Status add_numeric_values(const void* values, size_t count);
    Status add_value(const CppType& value);
    void write_null_bitmap(lucene::store::IndexOutput* null_bitmap_out);

    rowid_t _rid = 0;
    uint32_t _row_ids_seen_for_bkd = 0;
    roaring::Roaring _null_bitmap;
    uint64_t _reverted_index_size;

    std::unique_ptr<lucene::document::Document> _doc = nullptr;
    lucene::document::Field* _field = nullptr;
    bool _single_field = true;
    std::shared_ptr<DorisFSDirectory> _dir = nullptr;
    std::unique_ptr<lucene::index::IndexWriter> _index_writer = nullptr;
    std::unique_ptr<lucene::analysis::Analyzer> _analyzer = nullptr;
    std::unique_ptr<lucene::util::Reader> _char_string_reader = nullptr;
    std::shared_ptr<lucene::util::bkd::bkd_writer> _bkd_writer = nullptr;
    InvertedIndexCtxSPtr _inverted_index_ctx = nullptr;
    const KeyCoder* _value_key_coder;
    const TabletIndex* _index_meta;
    InvertedIndexParserType _parser_type;
    std::wstring _field_name;
    XIndexFileWriter* _index_file_writer;
    uint32_t _ignore_above;
};

}