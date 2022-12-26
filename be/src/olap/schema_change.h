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

#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "olap/column_mapping.h"
#include "olap/delete_handler.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/tablet.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"

namespace doris {
class RowBlockChanger {
public:
    RowBlockChanger(TabletSchemaSPtr tablet_schema, const DeleteHandler* delete_handler,
                    DescriptorTbl desc_tbl);

    ~RowBlockChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    Status change_block(vectorized::Block* ref_block, vectorized::Block* new_block) const;

private:
    Status _check_cast_valid(vectorized::ColumnPtr ref_column,
                             vectorized::ColumnPtr new_column) const;

    // @brief column-mapping specification of new schema
    SchemaMapping _schema_mapping;

    // delete handler for filtering data which use specified in DELETE_DATA
    const DeleteHandler* _delete_handler = nullptr;

    DescriptorTbl _desc_tbl;

    DISALLOW_COPY_AND_ASSIGN(RowBlockChanger);
};

class SchemaChange {
public:
    SchemaChange() : _filtered_rows(0), _merged_rows(0) {}
    virtual ~SchemaChange() = default;

    virtual Status process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                           TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
                           TabletSchemaSPtr base_tablet_schema) {
        if (rowset_reader->rowset()->empty() || rowset_reader->rowset()->num_rows() == 0) {
            RETURN_WITH_WARN_IF_ERROR(
                    rowset_writer->flush(), Status::Error<ErrorCode::INVALID_ARGUMENT>(),
                    fmt::format("create empty version for schema change failed. version= {}-{}",
                                rowset_writer->version().first, rowset_writer->version().second));

            return Status::OK();
        }

        _filtered_rows = 0;
        _merged_rows = 0;

        RETURN_IF_ERROR(
                _inner_process(rowset_reader, rowset_writer, new_tablet, base_tablet_schema));
        _add_filtered_rows(rowset_reader->filtered_rows());

        // Check row num changes
        if (config::row_nums_check && !_check_row_nums(rowset_reader, *rowset_writer)) {
            return Status::Error<ErrorCode::ALTER_STATUS_ERR>();
        }

        LOG(INFO) << "all row nums. source_rows=" << rowset_reader->rowset()->num_rows()
                  << ", merged_rows=" << merged_rows() << ", filtered_rows=" << filtered_rows()
                  << ", new_index_rows=" << rowset_writer->num_rows();
        return Status::OK();
    }

    uint64_t filtered_rows() const { return _filtered_rows; }

    uint64_t merged_rows() const { return _merged_rows; }

protected:
    void _add_filtered_rows(uint64_t filtered_rows) { _filtered_rows += filtered_rows; }

    void _add_merged_rows(uint64_t merged_rows) { _merged_rows += merged_rows; }

    virtual Status _inner_process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                                  TabletSharedPtr new_tablet, TabletSchemaSPtr base_tablet_schema) {
        return Status::NotSupported("inner process unsupported.");
    };

    bool _check_row_nums(RowsetReaderSharedPtr reader, const RowsetWriter& writer) const {
        if (reader->rowset()->num_rows() != writer.num_rows() + _merged_rows + _filtered_rows) {
            LOG(WARNING) << "fail to check row num! "
                         << "source_rows=" << reader->rowset()->num_rows()
                         << ", writer rows=" << writer.num_rows()
                         << ", merged_rows=" << merged_rows()
                         << ", filtered_rows=" << filtered_rows()
                         << ", new_index_rows=" << writer.num_rows();
            return false;
        }
        return true;
    }

private:
    uint64_t _filtered_rows;
    uint64_t _merged_rows;
};

class LinkedSchemaChange : public SchemaChange {
public:
    explicit LinkedSchemaChange(const RowBlockChanger& row_block_changer)
            : _row_block_changer(row_block_changer) {}
    ~LinkedSchemaChange() override = default;

    Status process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                   TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
                   TabletSchemaSPtr base_tablet_schema) override;

private:
    const RowBlockChanger& _row_block_changer;
    DISALLOW_COPY_AND_ASSIGN(LinkedSchemaChange);
};

class VSchemaChangeDirectly : public SchemaChange {
public:
    VSchemaChangeDirectly(const RowBlockChanger& row_block_changer) : _changer(row_block_changer) {}

private:
    Status _inner_process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                          TabletSharedPtr new_tablet, TabletSchemaSPtr base_tablet_schema) override;

    const RowBlockChanger& _changer;
};

// @breif schema change with sorting
class VSchemaChangeWithSorting : public SchemaChange {
public:
    VSchemaChangeWithSorting(const RowBlockChanger& row_block_changer, size_t memory_limitation);
    ~VSchemaChangeWithSorting() override = default;

private:
    Status _inner_process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                          TabletSharedPtr new_tablet, TabletSchemaSPtr base_tablet_schema) override;

    Status _internal_sorting(const std::vector<std::unique_ptr<vectorized::Block>>& blocks,
                             const Version& temp_delta_versions, int64_t oldest_write_timestamp,
                             int64_t newest_write_timestamp, TabletSharedPtr new_tablet,
                             RowsetTypePB new_rowset_type, SegmentsOverlapPB segments_overlap,
                             RowsetSharedPtr* rowset);

    Status _external_sorting(std::vector<RowsetSharedPtr>& src_rowsets, RowsetWriter* rowset_writer,
                             TabletSharedPtr new_tablet);

    const RowBlockChanger& _changer;
    size_t _memory_limitation;
    Version _temp_delta_versions;
    std::unique_ptr<MemTracker> _mem_tracker;
};

class SchemaChangeHandler {
public:
    // schema change v2, it will not set alter task in base tablet
    static Status process_alter_tablet_v2(const TAlterTabletReqV2& request);

    static std::unique_ptr<SchemaChange> get_sc_procedure(const RowBlockChanger& rb_changer,
                                                          bool sc_sorting, bool sc_directly) {
        if (sc_sorting) {
            return std::make_unique<VSchemaChangeWithSorting>(
                    rb_changer, config::memory_limitation_per_thread_for_schema_change_bytes);
        }

        if (sc_directly) {
            return std::make_unique<VSchemaChangeDirectly>(rb_changer);
        }

        return std::make_unique<LinkedSchemaChange>(rb_changer);
    }

    static bool tablet_in_converting(int64_t tablet_id);

private:
    static Status _get_versions_to_be_changed(TabletSharedPtr base_tablet,
                                              std::vector<Version>* versions_to_be_changed,
                                              RowsetSharedPtr* max_rowset);

    struct AlterMaterializedViewParam {
        std::string column_name;
        std::string origin_column_name;
        std::string mv_expr;
        std::shared_ptr<TExpr> expr;
    };

    struct SchemaChangeParams {
        AlterTabletType alter_tablet_type;
        TabletSharedPtr base_tablet;
        TabletSharedPtr new_tablet;
        TabletSchemaSPtr base_tablet_schema = nullptr;
        std::vector<RowsetReaderSharedPtr> ref_rowset_readers;
        DeleteHandler* delete_handler = nullptr;
        std::unordered_map<std::string, AlterMaterializedViewParam> materialized_params_map;
        DescriptorTbl* desc_tbl = nullptr;
        ObjectPool pool;
    };

    static Status _do_process_alter_tablet_v2(const TAlterTabletReqV2& request);

    static Status _validate_alter_result(TabletSharedPtr new_tablet,
                                         const TAlterTabletReqV2& request);

    static Status _convert_historical_rowsets(const SchemaChangeParams& sc_params);

    static Status _parse_request(const SchemaChangeParams& sc_params, RowBlockChanger* rb_changer,
                                 bool* sc_sorting, bool* sc_directly);

    // Initialization Settings for creating a default value
    static Status _init_column_mapping(ColumnMapping* column_mapping,
                                       const TabletColumn& column_schema, const std::string& value);

    static std::shared_mutex _mutex;
    static std::unordered_set<int64_t> _tablet_ids_in_converting;
    static std::set<std::string> _supported_functions;
};
} // namespace doris
