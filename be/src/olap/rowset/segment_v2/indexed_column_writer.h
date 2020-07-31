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
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"

namespace doris {

class BlockCompressionCodec;
class KeyCoder;
class TypeInfo;

namespace fs {
class WritableBlock;
}

namespace segment_v2 {

class IndexPageBuilder;
class PageBuilder;

struct IndexedColumnWriterOptions {
    size_t index_page_size = 64 * 1024;
    bool write_ordinal_index = false;
    bool write_value_index = false;
    EncodingTypePB encoding = DEFAULT_ENCODING;
    CompressionTypePB compression = NO_COMPRESSION;
    double compression_min_space_saving = 0.1;
};

// IndexedColumn is a column with an optional "ordinal index" and an optional "value index".
// - "ordinal index" enables us to seek to a particular rowid within the column
// - "value index" enables us to seek to a particular value but requires IndexedColumn to store ordered values
//
// IndexedColumn can be used as the building blocks for implementing other data structures. For example,
// - a bitmap index can be represented by two indexed columns, one for the term dictionary, one for the posting lists.
//   the "dictionary" IndexedColumn contains ordered terms and a value index.
//   the "posting" IndexedColumn contains bitmap for each term and an ordinal index.
// - a bloom filter index can be represented by one indexed column containing bloom filters with an ordinal index
//
// Currently IndexedColumn has the following restrictions but can be extended to solve in the future
// 1. value can't be null
// 2. duplicated values are not supported/tested when storing ordered values
// TODO test with empty input
class IndexedColumnWriter {
public:
    explicit IndexedColumnWriter(const IndexedColumnWriterOptions& options,
                                 const TypeInfo* typeinfo,
                                 fs::WritableBlock* wblock);

    ~IndexedColumnWriter();

    Status init();

    // add a single not-null value
    Status add(const void* value);

    Status finish(IndexedColumnMetaPB* meta);

private:
    Status _finish_current_data_page();

    Status _flush_index(IndexPageBuilder* index_builder, BTreeMetaPB* meta);

    IndexedColumnWriterOptions _options;
    const TypeInfo* _typeinfo;
    fs::WritableBlock* _wblock;
    // only used for `_first_value`
    std::shared_ptr<MemTracker> _mem_tracker;
    MemPool _mem_pool;

    ordinal_t _num_values;
    uint32_t _num_data_pages;
    // remember the first value in current page
    faststring _first_value;
    PagePointer _last_data_page;

    // the following members are initialized in init()
    // -----
    // builder for data pages
    std::unique_ptr<PageBuilder> _data_page_builder;
    // builder for index pages of ordinal index, null if write_ordinal_index == false
    std::unique_ptr<IndexPageBuilder> _ordinal_index_builder;
    // builder for index pages of value index, null if write_value_index == false
    std::unique_ptr<IndexPageBuilder> _value_index_builder;
    // encoder for value index's key
    const KeyCoder* _validx_key_coder;
    const BlockCompressionCodec* _compress_codec;

    DISALLOW_COPY_AND_ASSIGN(IndexedColumnWriter);
};

} // namespace segment_v2
} // namespace doris
