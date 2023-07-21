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

#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris {

class RowsetWriter;

using RowsetWriterUniquePtr = std::unique_ptr<RowsetWriter>;

class IndexBuilder {
public:
    IndexBuilder(const TabletSharedPtr& tablet, const std::vector<TColumn>& columns,
                 const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                 bool is_drop_op = false);
    ~IndexBuilder();

    Status init();
    Status do_build_inverted_index();
    Status update_inverted_index_info();
    Status handle_inverted_index_data();
    Status handle_single_rowset(RowsetMetaSharedPtr output_rowset_meta,
                                std::vector<segment_v2::SegmentSharedPtr>& segments);
    Status modify_rowsets(const Merger::Statistics* stats = nullptr);
    void gc_output_rowset();

private:
    Status _write_inverted_index_data(TabletSchemaSPtr tablet_schema, int32_t segment_idx,
                                      vectorized::Block* block);
    Status _add_data(const std::string& column_name,
                     const std::pair<int64_t, int64_t>& index_writer_sign, Field* field,
                     const uint8_t** ptr, size_t num_rows);
    Status _add_nullable(const std::string& column_name,
                         const std::pair<int64_t, int64_t>& index_writer_sign, Field* field,
                         const uint8_t* null_map, const uint8_t** ptr, size_t num_rows);

private:
    TabletSharedPtr _tablet;
    std::vector<TColumn> _columns;
    std::vector<doris::TOlapTableIndex> _alter_inverted_indexes;
    bool _is_drop_op;
    std::set<int32_t> _alter_index_ids;
    std::vector<RowsetSharedPtr> _input_rowsets;
    std::vector<RowsetSharedPtr> _output_rowsets;
    std::vector<RowsetReaderSharedPtr> _input_rs_readers;
    std::unique_ptr<vectorized::OlapBlockDataConvertor> _olap_data_convertor;
    // "<segment_id, index_id>" -> InvertedIndexColumnWriter
    std::unordered_map<std::pair<int64_t, int64_t>,
                       std::unique_ptr<segment_v2::InvertedIndexColumnWriter>>
            _inverted_index_builders;
};

using IndexBuilderSharedPtr = std::shared_ptr<IndexBuilder>;

} // namespace doris
