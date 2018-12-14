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
#include "olap/delete_handler.h"
#include "olap/column_data.h"

namespace doris {
// defined in 'field.h'
class Field;
class FieldInfo;
// defined in 'olap_data.h'
class ColumnData;
// defined in 'olap_table.h'
class OLAPTable;
// defined in 'row_block.h'
class RowBlock;
// defined in 'row_cursor.h'
class RowCursor;
// defined in 'writer.h'
class ColumnDataWriter;

class RowBlockChanger {
public:
    typedef std::vector<ColumnMapping> SchemaMapping;

    RowBlockChanger(const std::vector<FieldInfo>& tablet_schema,
                    const OLAPTablePtr& ref_olap_table,
                    const DeleteHandler& delete_handler);

    RowBlockChanger(const std::vector<FieldInfo>& tablet_schema,
                    const OLAPTablePtr& ref_olap_table);
    
    virtual ~RowBlockChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    SchemaMapping get__schema_mapping() const {
        return _schema_mapping;
    }
    
    bool change_row_block(
            const DataFileType df_type,
            const RowBlock& origin_block,
            int32_t data_version,
            RowBlock* mutable_block,
            uint64_t* filted_rows) const;

private:
    // @brief column-mapping specification of new schema
    SchemaMapping _schema_mapping;

    // delete handler for filtering data which use specified in DELETE_DATA
    DeleteHandler _delete_handler;

    DISALLOW_COPY_AND_ASSIGN(RowBlockChanger);
};

class RowBlockAllocator;
class RowBlockSorter {
public:
    explicit RowBlockSorter(RowBlockAllocator* allocator);
    virtual ~RowBlockSorter();

    bool sort(RowBlock** row_block);

private:
    static bool _row_cursor_comparator(const RowCursor* a, const RowCursor* b) {
        return a->full_key_cmp(*b) < 0;
    }

    RowBlockAllocator* _row_block_allocator;
    RowBlock* _swap_row_block;
};

class RowBlockAllocator {
public:
    RowBlockAllocator(const std::vector<FieldInfo>& tablet_schema, size_t memory_limitation);
    virtual ~RowBlockAllocator();

    OLAPStatus allocate(RowBlock** row_block, size_t num_rows, 
                        DataFileType data_file_type, bool null_supported);
    void release(RowBlock* row_block);

private:
    const std::vector<FieldInfo>& _tablet_schema;
    size_t _memory_allocated;
    size_t _row_len;
    size_t _memory_limitation;
};

class RowBlockMerger {
public:
    explicit RowBlockMerger(OLAPTablePtr olap_table);
    virtual ~RowBlockMerger();

    bool merge(
            const std::vector<RowBlock*>& row_block_arr,
            ColumnDataWriter* writer,
            uint64_t* merged_rows);

private:
    struct MergeElement {
        bool operator<(const MergeElement& other) const {
            return row_cursor->full_key_cmp(*(other.row_cursor)) > 0;
        }
        
        const RowBlock* row_block;
        RowCursor* row_cursor;
        uint32_t row_block_index;
    };

    bool _make_heap(const std::vector<RowBlock*>& row_block_arr);
    bool _pop_heap();

    OLAPTablePtr _olap_table;
    std::priority_queue<MergeElement> _heap;
};

class SchemaChange {
public:
    SchemaChange() : _filted_rows(0), _merged_rows(0) {}
    virtual ~SchemaChange() {}

    virtual bool process(ColumnData* olap_data, SegmentGroup* new_segment_group) = 0;

    void add_filted_rows(uint64_t filted_rows) {
        _filted_rows += filted_rows;
    }

    void add_merged_rows(uint64_t merged_rows) {
        _merged_rows += merged_rows;
    }

    uint64_t filted_rows() const {
        return _filted_rows;
    }

    uint64_t merged_rows() const {
        return _merged_rows;
    }

    void reset_filted_rows() {
        _filted_rows = 0;
    }

    void reset_merged_rows() {
        _merged_rows = 0;
    }

    OLAPStatus create_init_version(
            TTabletId tablet_id,
            TSchemaHash schema_hash,
            Version version,
            VersionHash version_hash,
            SegmentGroup* segment_group);

private:
    uint64_t _filted_rows;
    uint64_t _merged_rows;
};

class LinkedSchemaChange : public SchemaChange {
public:
    explicit LinkedSchemaChange(
                OLAPTablePtr base_olap_table, 
                OLAPTablePtr new_olap_table,
                const RowBlockChanger& row_block_changer);
    ~LinkedSchemaChange() {}

    bool process(ColumnData* olap_data, SegmentGroup* new_segment_group);
private:
    OLAPTablePtr _base_olap_table;
    OLAPTablePtr _new_olap_table;
    const RowBlockChanger& _row_block_changer;
    DISALLOW_COPY_AND_ASSIGN(LinkedSchemaChange);
};

// @brief schema change without sorting.
class SchemaChangeDirectly : public SchemaChange {
public:
    // @params olap_table           the instance of table which has new schema.
    // @params row_block_changer    changer to modifiy the data of RowBlock
    explicit SchemaChangeDirectly(
            OLAPTablePtr olap_table,
            const RowBlockChanger& row_block_changer);
    virtual ~SchemaChangeDirectly();

    virtual bool process(ColumnData* olap_data, SegmentGroup* new_segment_group);

private:
    OLAPTablePtr _olap_table;
    const RowBlockChanger& _row_block_changer;
    RowBlockAllocator* _row_block_allocator;
    RowCursor* _src_cursor;
    RowCursor* _dst_cursor;

    bool _write_row_block(ColumnDataWriter* writer, RowBlock* row_block);

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeDirectly);
};

// @breif schema change with sorting
class SchemaChangeWithSorting : public SchemaChange {
public:
    explicit SchemaChangeWithSorting(
            OLAPTablePtr olap_table,
            const RowBlockChanger& row_block_changer,
            size_t memory_limitation);
    virtual ~SchemaChangeWithSorting();

    virtual bool process(ColumnData* olap_data, SegmentGroup* new_segment_group);

private:
    bool _internal_sorting(
            const std::vector<RowBlock*>& row_block_arr,
            const Version& temp_delta_versions,
            SegmentGroup** temp_segment_group);

    bool _external_sorting(
            std::vector<SegmentGroup*>& src_segment_group_arr,
            SegmentGroup* segment_group);

    OLAPTablePtr _olap_table;
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

    OLAPStatus process_alter_table(AlterTabletType alter_table_type,
                                   const TAlterTabletReq& request);

    OLAPStatus schema_version_convert(OLAPTablePtr ref_olap_table,
                                      OLAPTablePtr new_olap_table,
                                      std::vector<SegmentGroup*>* ref_segment_groups,
                                      std::vector<SegmentGroup*>* new_segment_groups);

    // 清空一个table下的schema_change信息：包括split_talbe以及其他schema_change信息
    //  这里只清理自身的out链，不考虑related的table
    // NOTE 需要外部lock header
    // Params:
    //   alter_table_type
    //     为NULL时，同时检查table_split和其他普通schema_change
    //               否则只检查指定type的信息
    //   only_one:
    //     为true时：如果其out链只有一个，且可删除，才可能进行clear
    //     为false时：如果发现有大于1个out链，不管是否可删除，都不进行删除
    //   check_only:
    //     检查通过也不删除schema
    // Returns:
    //  成功：有的都可以清理（没有就直接跳过）
    //  失败：如果有信息但不能清理（有version没完成）,或不符合only_one条件
    static OLAPStatus clear_schema_change_single_info(TTabletId tablet_id,
                                                      SchemaHash schema_hash,
                                                      AlterTabletType* alter_table_type,
                                                      bool only_one,
                                                      bool check_only);

    static OLAPStatus clear_schema_change_single_info(OLAPTablePtr olap_table,
                                                      AlterTabletType* alter_table_type,
                                                      bool only_one,
                                                      bool check_only);


private:
    // 检查schema_change相关的状态:清理"一对"schema_change table间的信息
    // 由于A->B的A的schema_change信息会在后续处理过程中覆盖（这里就没有额外清除）
    // Returns:
    //  成功：如果存在历史信息，没有问题的就清空；或者没有历史信息
    //  失败：否则如果有历史信息且无法清空的（有version还没有完成）
    OLAPStatus _check_and_clear_schema_change_info(OLAPTablePtr olap_table,
                                                   const TAlterTabletReq& request);

    OLAPStatus _get_versions_to_be_changed(OLAPTablePtr ref_olap_table,
                                           std::vector<Version>& versions_to_be_changed);

    OLAPStatus _do_alter_table(AlterTabletType type,
                               OLAPTablePtr ref_olap_table,
                               const TAlterTabletReq& request);

    struct SchemaChangeParams {
        // 为了让calc_split_key也可使用普通schema_change的线程，才设置了此type
        AlterTabletType alter_table_type;
        OLAPTablePtr ref_olap_table;
        OLAPTablePtr new_olap_table;
        std::vector<ColumnData*> ref_olap_data_arr;
        std::string debug_message;
        DeleteHandler delete_handler;
        // TODO(zc): fuck me please, I don't add mutable here, but no where
        mutable std::string user;
        mutable std::string group;
    };

    // 根据给定的table_desc，创建OLAPTable，并挂接到OLAPEngine中
    OLAPStatus _create_new_olap_table(const OLAPTablePtr ref_olap_table,
                                      const TCreateTabletReq& create_tablet_req,
                                      const std::string* ref_root_path,
                                      OLAPTablePtr* out_new_olap_table);

    // 增加A->(B|C|...) 的schema_change信息
    //  在split table时，增加split-table status相关的信息
    //  其他的都增加在schema-change status中
    OLAPStatus _save_schema_change_info(AlterTabletType alter_table_type,
                                        OLAPTablePtr ref_olap_table,
                                        OLAPTablePtr new_olap_table,
                                        const std::vector<Version>& versions_to_be_changed);

    static OLAPStatus _alter_table(SchemaChangeParams* sc_params);

    static OLAPStatus _parse_request(OLAPTablePtr ref_olap_table,
                                     OLAPTablePtr new_olap_table,
                                     RowBlockChanger* rb_changer,
                                     bool* sc_sorting, 
                                     bool* sc_directly);

    // 需要新建default_value时的初始化设置
    static OLAPStatus _init_column_mapping(ColumnMapping* column_mapping,
                                           const FieldInfo& column_schema,
                                           const std::string& value);

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeHandler);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_SCHEMA_CHANGE_H
