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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_COLUMN_WRITER_H
#define DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_COLUMN_WRITER_H

#include <vector>

#include "gen_cpp/doris.pb.h"
#include "util/slice.h"

namespace doris {

namespace segment_v2 {

// ColumnWriter is used to write data of a column
class ColumnWriter {
public:
    explicit ColumnWriter(BuilderOptions builder_options, ColumnSchemaPB* column_schema)
            : _builder_options(builder_options),
              _column_schema(column_schema) { }

    bool init();

    // close the writer
    bool finish();

    // Caller will loop all the ColumnWriter and call the following get page api
    // to get page data and get the page pointer
    bool get_data_pages(std::vector<doris::Slice*>* data_buffers);

    // Get the dictionary page for under dictionary encoding mode column.
    virtual bool get_dictionary_page(doris::Slice* dictionary_page);

    // Get the bloom filter pages for under bloom filter indexed column.
    virtual bool get_bloom_filter_pages(std::vector<doris::Slice*>* bf_pages);

    // Get the bitmap page for under bitmap indexed column.
    virtual bool get_bitmap_page(doris::Slice* bitmap_page);

    // Get the statistic page for under statistic column.
    virtual bool get_statistic_page(doris::Slice* statistic_page);

    bool write_batch(doris::RowBlock* block);

    size_t written_size() const;

    int written_value_count() const;

private:
    BuilderOptions _builder_options;
    ColumnSchemaPB* _column_schema;
};

} // namespace segment_v2

} // namespace doris


#endif // DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_COLUMN_WRITER_H
