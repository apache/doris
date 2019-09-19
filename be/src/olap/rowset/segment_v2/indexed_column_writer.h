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

#include <cstddef>
#include <memory>
#include <vector>

#include "common/status.h"
#include "gen_cpp/segment_v2.pb.h"
#include "gutil/macros.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "util/slice.h"

namespace doris {

class BlockCompressionCodec;
class KeyCoder;
class TypeInfo;
class WritableFile;

namespace segment_v2 {

class IndexPageBuilder;
class PageBuilder;

struct IndexedColumnWriterOptions {
    size_t index_page_size = 64 * 1024;
    bool write_ordinal_index = false;
    bool write_value_index = false;
    EncodingTypePB encoding = DEFAULT_ENCODING;
    CompressionTypePB compression = NO_COMPRESSION;
};

// TODO test with empty input (all values are null)
// TODO support null value
// TODO support value index with duplicated keys
class IndexedColumnWriter {
public:
    explicit IndexedColumnWriter(IndexedColumnWriterOptions options,
                                 const TypeInfo* typeinfo,
                                 WritableFile* output_file);

    ~IndexedColumnWriter();

    Status init();

    // add a single not-null value TODO support batch-write
    Status add(const void* value);

    Status finish(IndexedColumnMetaPB* meta);

private:
    Status _finish_current_data_page();

    // Append the given data page, update ordinal index or value index if they're used.
    Status _append_data_page(const std::vector<Slice>& data_page,
                             rowid_t first_rowid,
                             void* first_value);

    // Append the given page into the file. After return, *pp points to the newly
    // inserted page.
    // Input data will be compressed when compression is enabled.
    // We also compute and append checksum for the page.
    Status _append_page(const std::vector<Slice>& page, PagePointer* pp);

    Status _flush_index(IndexPageBuilder* index_builder, BTreeMetaPB* meta);

    IndexedColumnWriterOptions _options;
    const TypeInfo* _typeinfo;
    WritableFile* _file;

    rowid_t _num_values;
    uint32_t _num_data_pages;
    PagePointer _last_data_page;

    // initialized in init()
    std::unique_ptr<PageBuilder> _data_page_builder;
    std::unique_ptr<IndexPageBuilder> _ordinal_index_builder;
    std::unique_ptr<IndexPageBuilder> _value_index_builder;
    const KeyCoder* _validx_key_coder;
    const BlockCompressionCodec* _compress_codec;

    DISALLOW_COPY_AND_ASSIGN(IndexedColumnWriter);
};

} // namespace segment_v2
} // namespace doris
