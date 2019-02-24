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

struct ColumnMapping {
    ColumnMapping() : ref_column(-1), default_value(NULL) {}
    virtual ~ColumnMapping() {}

    // <0: use default value
    // >=0: use origin column
    int32_t ref_column;
    // normally for default value. stores values for filters
    WrapperField* default_value;
};

class RowBlockChanger {
public:
    typedef std::vector<ColumnMapping> SchemaMapping;

    RowBlockChanger(const TabletSchema& tablet_schema,
                    const TabletSharedPtr& base_tablet,
                    const DeleteHandler& delete_handler);

    RowBlockChanger(const TabletSchema& tablet_schema,
                    const TabletSharedPtr& base_tablet);
    
    virtual ~RowBlockChanger();

    ColumnMapping* get_mutable_column_mapping(size_t column_index);

    SchemaMapping get_schema_mapping() const {
        return _schema_mapping;
    }
    
    bool change_row_block(
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
    RowBlockAllocator(const TabletSchema& tablet_schema, size_t memory_limitation);
    virtual ~RowBlockAllocator();

    OLAPStatus allocate(RowBlock** row_block, size_t num_rows, 
                        bool null_supported);
    void release(RowBlock* row_block);

private:
    const TabletSchema& _tablet_schema;
    size_t _memory_allocated;
    size_t _row_len;
    size_t _memory_limitation;
};

class RowBlockMerger {
public:
    explicit RowBlockMerger(TabletSharedPtr tablet);
    virtual ~RowBlockMerger();

    bool merge(
            const std::vector<RowBlock*>& row_block_arr,
            RowsetWriterSharedPtr rowset_writer,
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

    TabletSharedPtr _tablet;
    std::priority_queue<MergeElement> _heap;
};

class SchemaChange {
public:
    SchemaChange() : _filted_rows(0), _merged_rows(0) {}
    virtual ~SchemaChange() {}

    virtual bool process(RowsetReaderSharedPtr rowset_reader,
                         RowsetWriterSharedPtr new_rowset_builder,
                         TabletSharedPtr tablet) = 0;

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

private:
    uint64_t _filted_rows;
    uint64_t _merged_rows;
};

class LinkedSchemaChange : public SchemaChange {
public:
    explicit LinkedSchemaChange(
                TabletSharedPtr base_tablet, 
                TabletSharedPtr new_tablet);
    ~LinkedSchemaChange() {}

    bool process(RowsetReaderSharedPtr rowset_reader,
                 RowsetWriterSharedPtr new_rowset_writer,
                 TabletSharedPtr tablet);
private:
    TabletSharedPtr _base_tablet;
    TabletSharedPtr _new_tablet;
    DISALLOW_COPY_AND_ASSIGN(LinkedSchemaChange);
};

// @brief schema change without sorting.
class SchemaChangeDirectly : public SchemaChange {
public:
    // @params tablet           the instance of tablet which has new schema.
    // @params row_block_changer    changer to modifiy the data of RowBlock
    explicit SchemaChangeDirectly(
            TabletSharedPtr tablet,
            const RowBlockChanger& row_block_changer);
    virtual ~SchemaChangeDirectly();

    virtual bool process(RowsetReaderSharedPtr rowset_reader,
                         RowsetWriterSharedPtr new_rowset_writer,
                         TabletSharedPtr tablet);

private:
    TabletSharedPtr _tablet;
    const RowBlockChanger& _row_block_changer;
    RowBlockAllocator* _row_block_allocator;
    RowCursor* _src_cursor;
    RowCursor* _dst_cursor;

    bool _write_row_block(RowsetWriterSharedPtr rowset_builder, RowBlock* row_block);

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeDirectly);
};

// @breif schema change with sorting
class SchemaChangeWithSorting : public SchemaChange {
public:
    explicit SchemaChangeWithSorting(
            TabletSharedPtr tablet,
            const RowBlockChanger& row_block_changer,
            size_t memory_limitation);
    virtual ~SchemaChangeWithSorting();

    virtual bool process(RowsetReaderSharedPtr rowset_reader,
                         RowsetWriterSharedPtr new_rowset_builder,
                         TabletSharedPtr tablet);

private:
    bool _internal_sorting(
            const std::vector<RowBlock*>& row_block_arr,
            const Version& temp_delta_versions,
            const VersionHash version_hash,
            RowsetSharedPtr* rowset);

    bool _external_sorting(
            std::vector<RowsetSharedPtr>& src_rowsets,
            RowsetWriterSharedPtr rowset_writer,
            TabletSharedPtr tablet);

    TabletSharedPtr _tablet;
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

    OLAPStatus process_alter_tablet(AlterTabletType alter_tablet_type,
                                   const TAlterTabletReq& request);

    OLAPStatus schema_version_convert(TabletSharedPtr base_tablet,
                                      TabletSharedPtr new_tablet,
                                      std::vector<RowsetSharedPtr>* old_rowsets,
                                      std::vector<RowsetSharedPtr>* new_rowsets);


private:
    // 检查schema_change相关的状态:清理"一对"schema_change table间的信息
    // 由于A->B的A的schema_change信息会在后续处理过程中覆盖（这里就没有额外清除）
    // Returns:
    //  成功：如果存在历史信息，没有问题的就清空；或者没有历史信息
    //  失败：否则如果有历史信息且无法清空的（有version还没有完成）
    OLAPStatus _check_and_clear_schema_change_info(TabletSharedPtr tablet,
                                                   const TAlterTabletReq& request);

    OLAPStatus _get_versions_to_be_changed(TabletSharedPtr base_tablet,
                                           std::vector<Version>& versions_to_be_changed);

    struct SchemaChangeParams {
        // 为了让calc_split_key也可使用普通schema_change的线程，才设置了此type
        AlterTabletType alter_tablet_type;
        TabletSharedPtr base_tablet;
        TabletSharedPtr new_tablet;
        std::vector<RowsetReaderSharedPtr> ref_rowset_readers;
        std::string debug_message;
        DeleteHandler delete_handler;
        // TODO(zc): fuck me please, I don't add mutable here, but no where
        mutable std::string user;
        mutable std::string group;
    };

    // 根据给定的table_desc，创建Tablet，并挂接到StorageEngine中
    OLAPStatus _create_new_tablet(const TabletSharedPtr base_tablet,
                                  const TCreateTabletReq& create_tablet_req,
                                  TabletSharedPtr* new_tablet);

    // 增加A->(B|C|...) 的schema_change信息
    //  在split table时，增加split-tablet status相关的信息
    //  其他的都增加在schema-change status中
    OLAPStatus _add_alter_tablet_task(AlterTabletType alter_tablet_type,
                                      TabletSharedPtr base_tablet,
                                      TabletSharedPtr new_tablet,
                                      const std::vector<Version>& versions_to_be_changed);
    OLAPStatus _save_alter_tablet_state(AlterTabletState state,
                                        TabletSharedPtr base_tablet,
                                        TabletSharedPtr new_tablet);

    static OLAPStatus _convert_historical_rowsets(const SchemaChangeParams& sc_params);

    static OLAPStatus _parse_request(TabletSharedPtr base_tablet,
                                     TabletSharedPtr new_tablet,
                                     RowBlockChanger* rb_changer,
                                     bool* sc_sorting,
                                     bool* sc_directly);

    // 需要新建default_value时的初始化设置
    static OLAPStatus _init_column_mapping(ColumnMapping* column_mapping,
                                           const TabletColumn& column_schema,
                                           const std::string& value);

    DISALLOW_COPY_AND_ASSIGN(SchemaChangeHandler);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_SCHEMA_CHANGE_H
