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

#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/merger.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/pending_rowset_helper.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/segment/segment.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {
namespace segment_v2 {
class IndexColumnWriter;
class IndexFileWriter;
} // namespace segment_v2
class OlapBlockDataConvertor;

class StorageEngine;
class RowsetWriter;

using RowsetWriterUniquePtr = std::unique_ptr<RowsetWriter>;

class IndexBuilder {
public:
    IndexBuilder(StorageEngine& engine, TabletSharedPtr tablet, const std::vector<TColumn>& columns,
                 const std::vector<doris::TOlapTableIndex>& alter_inverted_indexes,
                 bool is_drop_op = false);
    virtual ~IndexBuilder();

    virtual Status init();
    virtual Status do_build_inverted_index();
    virtual Status update_inverted_index_info();
    virtual Status handle_inverted_index_data();
    virtual Status handle_single_rowset(RowsetMetaSharedPtr output_rowset_meta,
                                        std::vector<segment_v2::SegmentSharedPtr>& segments);
    virtual Status modify_rowsets(const Merger::Statistics* stats = nullptr);
    virtual void gc_output_rowset();

    // How one SNII segment rewrite treats the target schema's logical indexes:
    // inherit_keys are carried over from the source container without decoding a
    // posting; build_columns are built from raw column data, grouped by column
    // unique id so every index on a column is fed from the SAME column read.
    struct SniiIndexRewritePlan {
        std::vector<snii::reader::LogicalIndexKey> inherit_keys;
        std::vector<std::pair<int32_t, std::vector<const TabletIndex*>>> build_columns;
    };

    // Classifies the output schema's inverted indexes for one SNII segment
    // rewrite (design: same key and definition -> inherit; requested or
    // definition-changed -> build; keys the target schema dropped are simply not
    // inherited). `container_has` reports whether the SOURCE container holds a
    // given logical index; static and callback-based so the classification is
    // directly unit-testable without files.
    //
    // `source_container_has_blob` suppresses inheritance for the WHOLE segment:
    // inheritance snapshots the source container, and a container holding a blob
    // logical index cannot be snapshotted at all (SniiSegmentReader rejects it,
    // by directory content rather than by what is being kept). Rebuilding every
    // index is then the only way through, and it is a correct one.
    static Status plan_snii_index_rewrite(
            const TabletSchema& input_schema, const TabletSchema& output_schema,
            const std::set<int64_t>& alter_index_ids,
            const std::function<Status(const TabletIndex&, bool*)>& container_has,
            bool source_container_has_blob, SniiIndexRewritePlan* plan);

private:
    // SNII counterpart of the V1/V2 build branch in handle_single_rowset: plans
    // the rewrite per segment, inherits the source container's physical prefix
    // once, and builds only the missing indexes -- one raw column read per
    // column, shared by every writer on it.
    Status _handle_single_rowset_snii(RowsetMetaSharedPtr output_rowset_meta,
                                      std::vector<segment_v2::SegmentSharedPtr>& segments);
    // One segment of the above: plan, inherit the prefix, build what is missing,
    // and register the container writer for the shared close pass.
    Status _rewrite_single_segment_snii(const io::FileSystemSPtr& fs,
                                        const TabletSchemaSPtr& output_rowset_schema,
                                        const TabletSchema& input_schema,
                                        const std::string& rowset_id,
                                        const segment_v2::SegmentSharedPtr& seg_ptr);
    // The build half of one segment rewrite: creates the writers of every column
    // group, scans each column once and feeds all of its writers.
    Status _build_snii_indexes_for_segment(const TabletSchemaSPtr& output_rowset_schema,
                                           const SniiIndexRewritePlan& plan,
                                           IndexFileWriter* index_file_writer,
                                           const segment_v2::SegmentSharedPtr& seg_ptr);
    // Feeds one converted block into the SNII build writers. group_writer_signs
    // parallels plan.build_columns: entry g holds the writer signs fed from
    // convertor ordinal g.
    Status _write_snii_index_data(
            const TabletSchemaSPtr& tablet_schema, Block* block, const SniiIndexRewritePlan& plan,
            const std::vector<std::vector<std::pair<int64_t, int64_t>>>& group_writer_signs);
    Status _write_inverted_index_data(TabletSchemaSPtr tablet_schema, int64_t segment_idx,
                                      Block* block);
    Status _add_data(const std::string& column_name,
                     const std::pair<int64_t, int64_t>& index_writer_sign,
                     const TabletColumn* column, const uint8_t** ptr, size_t num_rows);
    Status _add_nullable(const std::string& column_name,
                         const std::pair<int64_t, int64_t>& index_writer_sign,
                         const TabletColumn* column, const uint8_t* null_map, const uint8_t** ptr,
                         size_t num_rows);

private:
    StorageEngine& _engine;
    TabletSharedPtr _tablet;
    std::vector<TColumn> _columns;
    std::vector<doris::TOlapTableIndex> _alter_inverted_indexes;
    std::vector<TabletIndex> _dropped_inverted_indexes;
    bool _is_drop_op;
    std::set<int64_t> _alter_index_ids;
    std::vector<RowsetSharedPtr> _input_rowsets;
    std::vector<RowsetSharedPtr> _output_rowsets;
    std::vector<PendingRowsetGuard> _pending_rs_guards;
    std::vector<RowsetReaderSharedPtr> _input_rs_readers;
    std::unique_ptr<OlapBlockDataConvertor> _olap_data_convertor;
    // "<segment_id, index_id>" -> IndexColumnWriter
    std::unordered_map<std::pair<int64_t, int64_t>, std::unique_ptr<segment_v2::IndexColumnWriter>>
            _index_column_writers;
    std::unordered_map<int64_t, std::unique_ptr<IndexFileWriter>> _index_file_writers;
    // <rowset_id, segment_id>
    std::unordered_map<std::pair<std::string, int64_t>, std::unique_ptr<IndexFileReader>>
            _index_file_readers;
    // SNII only: output rowset id -> the INPUT rowset's schema. The rewrite plan
    // compares an index's definition between input and output schema to decide
    // inherit vs rebuild; the output rowset meta no longer carries the input's.
    std::unordered_map<std::string, TabletSchemaSPtr> _input_rowset_schemas;
};

using IndexBuilderSharedPtr = std::shared_ptr<IndexBuilder>;

} // namespace doris
