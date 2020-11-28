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

#ifndef DORIS_BE_SRC_OLAP_SCHEMA_CHANGE_H
#define DORIS_BE_SRC_OLAP_SCHEMA_CHANGE_H

#include <deque>
#include <queue>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "olap/column_mapping.h"
#include "olap/delete_handler.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/tablet.h"

namespace doris {
// defined in 'field.h'
class Field;
class FieldInfo;
// defined in 'tablet.h'
class Tablet;
// defined in 'row_block.h'
class RowBlock;
// defined in 'row_cursor.h'
class RowCursor;

bool to_bitmap(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
               int field_idx, int ref_field_idx, MemPool* mem_pool);
bool hll_hash(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
              int field_idx, int ref_field_idx, MemPool* mem_pool);
bool count_field(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
                 int field_idx, int ref_field_idx, MemPool* mem_pool);

class RowBlockChanger {
public:
    RowBlockChanger(const TabletSchema& tablet_schema, const DeleteHandler& delete_handler);

    RowBlockChanger(const TabletSchema& tablet_schema);

    virtual ~RowBlockChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    SchemaMapping get_schema_mapping() const { return _schema_mapping; }

    OLAPStatus change_row_block(const RowBlock* ref_block, int32_t data_version,
                                RowBlock* mutable_block, uint64_t* filtered_rows) const;

private:
    // @brief column-mapping specification of new schema
    SchemaMapping _schema_mapping;

    // delete handler for filtering data which use specified in DELETE_DATA
    DeleteHandler _delete_handler;

    DISALLOW_COPY_AND_ASSIGN(RowBlockChanger);
};

class RowBlockAllocator {
public:
    RowBlockAllocator(const TabletSchema& tablet_schema, size_t memory_limitation);
    virtual ~RowBlockAllocator();

    OLAPStatus allocate(RowBlock** row_block, size_t num_rows, bool null_supported);
    void release(RowBlock* row_block);

private:
    const TabletSchema& _tablet_schema;
    size_t _memory_allocated;
    size_t _row_len;
    size_t _memory_limitation;
};

class SchemaChange {
public:
    SchemaChange() : _filtered_rows(0), _merged_rows(0) {}
    virtual ~SchemaChange() {}

    virtual OLAPStatus process(RowsetReaderSharedPtr rowset_reader,
                               RowsetWriter* new_rowset_builder, TabletSharedPtr tablet,
                               TabletSharedPtr base_tablet) = 0;

    void add_filtered_rows(uint64_t filtered_rows) { _filtered_rows += filtered_rows; }

    void add_merged_rows(uint64_t merged_rows) { _merged_rows += merged_rows; }

    uint64_t filtered_rows() const { return _filtered_rows; }

    uint64_t merged_rows() const { return _merged_rows; }

    void reset_filtered_rows() { _filtered_rows = 0; }

    void reset_merged_rows() { _merged_rows = 0; }

private:
    uint64_t _filtered_rows;
    uint64_t _merged_rows;
};

class LinkedSchemaChange : public SchemaChange {
public:
    explicit LinkedSchemaChange(const RowBlockChanger& row_block_changer)
            : _row_block_changer(row_block_changer) {}
    ~LinkedSchemaChange() {}

    virtual OLAPStatus process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* new_rowset_writer,
                               TabletSharedPtr new_tablet, TabletSharedPtr base_tablet) override;

private:
    const RowBlockChanger& _row_block_changer;
    DISALLOW_COPY_AND_ASSIGN(LinkedSchemaChange);
};

// @brief schema change without sorting.
class SchemaChangeDirectly : public SchemaChange {
public:
    // @params tablet           the instance of tablet which has new schema.
    // @params row_block_changer    changer to modify the data of RowBlock
    explicit SchemaChangeDirectly(const RowBlockChanger& row_block_changer);
    virtual ~SchemaChangeDirectly();

    virtual OLAPStatus process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* new_rowset_writer,
                               TabletSharedPtr new_tablet, TabletSharedPtr base_tablet) override;

private:
    const RowBlockChanger& _row_block_changer;
    RowBlockAllocator* _row_block_allocator;
    RowCursor* _cursor;

    bool _write_row_block(RowsetWriter* rowset_builder, RowBlock* row_block);

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeDirectly);
};

// @breif schema change with sorting
class SchemaChangeWithSorting : public SchemaChange {
public:
    explicit SchemaChangeWithSorting(const RowBlockChanger& row_block_changer,
                                     size_t memory_limitation);
    virtual ~SchemaChangeWithSorting();

    virtual OLAPStatus process(RowsetReaderSharedPtr rowset_reader,
                               RowsetWriter* new_rowset_builder, TabletSharedPtr new_tablet,
                               TabletSharedPtr base_tablet) override;

private:
    bool _internal_sorting(const std::vector<RowBlock*>& row_block_arr,
                           const Version& temp_delta_versions, const VersionHash version_hash,
                           TabletSharedPtr new_tablet, RowsetTypePB new_rowset_type,
                           SegmentsOverlapPB segments_overlap, RowsetSharedPtr* rowset);

    bool _external_sorting(std::vector<RowsetSharedPtr>& src_rowsets, RowsetWriter* rowset_writer,
                           TabletSharedPtr new_tablet);

    const RowBlockChanger& _row_block_changer;
    size_t _memory_limitation;
    Version _temp_delta_versions;
    RowBlockAllocator* _row_block_allocator;

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeWithSorting);
};

class SchemaChangeHandler {
public:
    SchemaChangeHandler() {}
    virtual ~SchemaChangeHandler() {}

    OLAPStatus schema_version_convert(TabletSharedPtr base_tablet, TabletSharedPtr new_tablet,
                                      RowsetSharedPtr* base_rowset, RowsetSharedPtr* new_rowset);

    // schema change v2, it will not set alter task in base tablet
    OLAPStatus process_alter_tablet_v2(const TAlterTabletReqV2& request);

private:
    // 检查schema_change相关的状态:清理"一对"schema_change table间的信息
    // 由于A->B的A的schema_change信息会在后续处理过程中覆盖（这里就没有额外清除）
    // Returns:
    //  成功：如果存在历史信息，没有问题的就清空；或者没有历史信息
    //  失败：否则如果有历史信息且无法清空的（有version还没有完成）
    OLAPStatus _check_and_clear_schema_change_info(TabletSharedPtr tablet,
                                                   const TAlterTabletReq& request);

    OLAPStatus _get_versions_to_be_changed(TabletSharedPtr base_tablet,
                                           std::vector<Version>* versions_to_be_changed);

    struct AlterMaterializedViewParam {
        std::string column_name;
        std::string origin_column_name;
        std::string mv_expr;
    };

    struct SchemaChangeParams {
        AlterTabletType alter_tablet_type;
        TabletSharedPtr base_tablet;
        TabletSharedPtr new_tablet;
        std::vector<RowsetReaderSharedPtr> ref_rowset_readers;
        DeleteHandler delete_handler;
        std::unordered_map<std::string, AlterMaterializedViewParam> materialized_params_map;
    };

    // add alter task to base_tablet and new_tablet.
    // add A->(B|C|...) relation chain to all of them.
    OLAPStatus _add_alter_task(AlterTabletType alter_tablet_type, TabletSharedPtr base_tablet,
                               TabletSharedPtr new_tablet,
                               const std::vector<Version>& versions_to_be_changed);
    OLAPStatus _save_alter_state(AlterTabletState state, TabletSharedPtr base_tablet,
                                 TabletSharedPtr new_tablet);

    OLAPStatus _do_process_alter_tablet_v2(const TAlterTabletReqV2& request);

    OLAPStatus _validate_alter_result(TabletSharedPtr new_tablet, const TAlterTabletReqV2& request);

    static OLAPStatus _convert_historical_rowsets(const SchemaChangeParams& sc_params);

    static OLAPStatus _parse_request(
            TabletSharedPtr base_tablet, TabletSharedPtr new_tablet, RowBlockChanger* rb_changer,
            bool* sc_sorting, bool* sc_directly,
            const std::unordered_map<std::string, AlterMaterializedViewParam>&
                    materialized_function_map);

    // 需要新建default_value时的初始化设置
    static OLAPStatus _init_column_mapping(ColumnMapping* column_mapping,
                                           const TabletColumn& column_schema,
                                           const std::string& value);

private:
    RowsetReaderContext _reader_context;

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeHandler);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_SCHEMA_CHANGE_H
