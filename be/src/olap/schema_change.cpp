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

#include "olap/schema_change.h"

#include <pthread.h>
#include <signal.h>

#include <algorithm>
#include <vector>

#include "agent/cgroups_mgr.h"
#include "common/resource_tls.h"
#include "olap/merger.h"
#include "olap/row.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/wrapper_field.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/thread_context.h"
#include "util/defer_op.h"

using std::deque;
using std::list;
using std::nothrow;
using std::pair;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(schema_change_mem_consumption, MetricUnit::BYTES, "",
                                   mem_consumption, Labels({{"type", "schema_change"}}));

class RowBlockSorter {
public:
    explicit RowBlockSorter(RowBlockAllocator* allocator);
    virtual ~RowBlockSorter();
    size_t num_rows() { 
        return _swap_row_block != nullptr ? _swap_row_block->capacity() : 0;
    }

    bool sort(RowBlock** row_block);

private:
    static bool _row_cursor_comparator(const std::unique_ptr<RowCursor>& a,
                                       const std::unique_ptr<RowCursor>& b) {
        return compare_row(*a, *b) < 0;
    }

    RowBlockAllocator* _row_block_allocator;
    RowBlock* _swap_row_block;
};

class RowBlockMerger {
public:
    explicit RowBlockMerger(TabletSharedPtr tablet);
    virtual ~RowBlockMerger();

    bool merge(const std::vector<RowBlock*>& row_block_arr, RowsetWriter* rowset_writer,
               uint64_t* merged_rows);

private:
    struct MergeElement {
        bool operator<(const MergeElement& other) const {
            return compare_row(*row_cursor, *other.row_cursor) > 0;
        }

        const RowBlock* row_block;
        RowCursor* row_cursor;
        uint32_t row_block_index;
    };

    bool _make_heap(const std::vector<RowBlock*>& row_block_arr);
    void _pop_heap();

    TabletSharedPtr _tablet;
    std::priority_queue<MergeElement> _heap;
};

RowBlockChanger::RowBlockChanger(const TabletSchema& tablet_schema) {
    _schema_mapping.resize(tablet_schema.num_columns());
}

RowBlockChanger::RowBlockChanger(const TabletSchema& tablet_schema,
                                 const DeleteHandler* delete_handler) {
    _schema_mapping.resize(tablet_schema.num_columns());
    _delete_handler = delete_handler;
}

RowBlockChanger::~RowBlockChanger() {
    SchemaMapping::iterator it = _schema_mapping.begin();
    for (; it != _schema_mapping.end(); ++it) {
        SAFE_DELETE(it->default_value);
    }
    _schema_mapping.clear();
}

ColumnMapping* RowBlockChanger::get_mutable_column_mapping(size_t column_index) {
    if (column_index >= _schema_mapping.size()) {
        return nullptr;
    }

    return &(_schema_mapping[column_index]);
}

#define TYPE_REINTERPRET_CAST(FromType, ToType)                             \
    {                                                                       \
        size_t row_num = ref_block->row_block_info().row_num;               \
        for (size_t row = 0, mutable_row = 0; row < row_num; ++row) {       \
            if (is_data_left_vec[row] != 0) {                               \
                char* ref_ptr = ref_block->field_ptr(row, ref_column);      \
                char* new_ptr = mutable_block->field_ptr(mutable_row++, i); \
                *new_ptr = *ref_ptr;                                        \
                *(ToType*)(new_ptr + 1) = *(FromType*)(ref_ptr + 1);        \
            }                                                               \
        }                                                                   \
        break;                                                              \
    }

#define LARGEINT_REINTERPRET_CAST(FromType, ToType)                         \
    {                                                                       \
        size_t row_num = ref_block->row_block_info().row_num;               \
        for (size_t row = 0, mutable_row = 0; row < row_num; ++row) {       \
            if (is_data_left_vec[row] != 0) {                               \
                char* ref_ptr = ref_block->field_ptr(row, ref_column);      \
                char* new_ptr = mutable_block->field_ptr(mutable_row++, i); \
                *new_ptr = *ref_ptr;                                        \
                ToType new_value = *(FromType*)(ref_ptr + 1);               \
                memcpy(new_ptr + 1, &new_value, sizeof(ToType));            \
            }                                                               \
        }                                                                   \
        break;                                                              \
    }

#define CONVERT_FROM_TYPE(from_type)                                                            \
    {                                                                                           \
        switch (newtype) {                                                                      \
        case OLAP_FIELD_TYPE_TINYINT:                                                           \
            TYPE_REINTERPRET_CAST(from_type, int8_t);                                           \
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:                                                  \
            TYPE_REINTERPRET_CAST(from_type, uint8_t);                                          \
        case OLAP_FIELD_TYPE_SMALLINT:                                                          \
            TYPE_REINTERPRET_CAST(from_type, int16_t);                                          \
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:                                                 \
            TYPE_REINTERPRET_CAST(from_type, uint16_t);                                         \
        case OLAP_FIELD_TYPE_INT:                                                               \
            TYPE_REINTERPRET_CAST(from_type, int32_t);                                          \
        case OLAP_FIELD_TYPE_UNSIGNED_INT:                                                      \
            TYPE_REINTERPRET_CAST(from_type, uint32_t);                                         \
        case OLAP_FIELD_TYPE_BIGINT:                                                            \
            TYPE_REINTERPRET_CAST(from_type, int64_t);                                          \
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:                                                   \
            TYPE_REINTERPRET_CAST(from_type, uint64_t);                                         \
        case OLAP_FIELD_TYPE_LARGEINT:                                                          \
            LARGEINT_REINTERPRET_CAST(from_type, int128_t);                                     \
        case OLAP_FIELD_TYPE_FLOAT:                                                             \
            TYPE_REINTERPRET_CAST(from_type, float);                                            \
        case OLAP_FIELD_TYPE_DOUBLE:                                                            \
            TYPE_REINTERPRET_CAST(from_type, double);                                           \
        default:                                                                                \
            LOG(WARNING) << "the column type which was altered to was unsupported."             \
                         << " origin_type="                                                     \
                         << ref_block->tablet_schema().column(ref_column).type()                \
                         << ", alter_type=" << mutable_block->tablet_schema().column(i).type(); \
            return OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID;                                         \
        }                                                                                       \
        break;                                                                                  \
    }

#define ASSIGN_DEFAULT_VALUE(length)                                            \
    case length: {                                                              \
        for (size_t row = 0; row < ref_block.row_block_info().row_num; ++row) { \
            memcpy(buf, _schema_mapping[i].default_value->ptr(), length);       \
            buf += length;                                                      \
        }                                                                       \
        break;                                                                  \
    }

struct ConvertTypeMapHash {
    size_t operator()(const std::pair<FieldType, FieldType>& pair) const {
        return (pair.first + 31) ^ pair.second;
    }
};

class ConvertTypeResolver {
    DECLARE_SINGLETON(ConvertTypeResolver);

public:
    bool get_convert_type_info(const FieldType from_type, const FieldType to_type) const {
        return _convert_type_set.find(std::make_pair(from_type, to_type)) !=
               _convert_type_set.end();
    }

    template <FieldType from_type, FieldType to_type>
    void add_convert_type_mapping() {
        _convert_type_set.emplace(std::make_pair(from_type, to_type));
    }

private:
    typedef std::pair<FieldType, FieldType> convert_type_pair;
    std::unordered_set<convert_type_pair, ConvertTypeMapHash> _convert_type_set;

    DISALLOW_COPY_AND_ASSIGN(ConvertTypeResolver);
};

ConvertTypeResolver::ConvertTypeResolver() {
    // from char type
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_TINYINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_SMALLINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_INT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_BIGINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_LARGEINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_FLOAT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_DOUBLE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_DATE>();

    // supported type convert should annotate in doc:
    // http://doris.incubator.apache.org/master/zh-CN/sql-reference/sql-statements/Data%20Definition/ALTER%20TABLE.html#description
    // If type convert is supported here, you should check fe/src/main/java/org/apache/doris/catalog/ColumnType.java to supported it either
    // from varchar type
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_TINYINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_SMALLINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_INT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_BIGINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_LARGEINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_FLOAT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DOUBLE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_STRING>();

    // to varchar type
    add_convert_type_mapping<OLAP_FIELD_TYPE_TINYINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_LARGEINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DOUBLE, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_VARCHAR>();

    // from string
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_TINYINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_SMALLINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_INT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_BIGINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_LARGEINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_FLOAT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_DOUBLE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_VARCHAR>();

    // to string
    add_convert_type_mapping<OLAP_FIELD_TYPE_TINYINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_LARGEINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DOUBLE, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_STRING>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE, OLAP_FIELD_TYPE_DATETIME>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DATETIME, OLAP_FIELD_TYPE_DATE>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_DOUBLE>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_DATE>();
}

ConvertTypeResolver::~ConvertTypeResolver() {}

bool to_bitmap(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
               int field_idx, int ref_field_idx, MemPool* mem_pool) {
    write_helper->set_not_null(field_idx);
    BitmapValue bitmap;
    if (!read_helper->is_null(ref_field_idx)) {
        uint64_t origin_value;
        char* src = read_helper->cell_ptr(ref_field_idx);
        switch (ref_column.type()) {
        case OLAP_FIELD_TYPE_TINYINT:
            if (*(int8_t*)src < 0) {
                LOG(WARNING) << "The input: " << *(int8_t*)src
                             << " is not valid, to_bitmap only support bigint value from 0 to "
                                "18446744073709551615 currently";
                return false;
            }
            origin_value = *(int8_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            origin_value = *(uint8_t*)src;
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            if (*(int16_t*)src < 0) {
                LOG(WARNING) << "The input: " << *(int16_t*)src
                             << " is not valid, to_bitmap only support bigint value from 0 to "
                                "18446744073709551615 currently";
                return false;
            }
            origin_value = *(int16_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
            origin_value = *(uint16_t*)src;
            break;
        case OLAP_FIELD_TYPE_INT:
            if (*(int32_t*)src < 0) {
                LOG(WARNING) << "The input: " << *(int32_t*)src
                             << " is not valid, to_bitmap only support bigint value from 0 to "
                                "18446744073709551615 currently";
                return false;
            }
            origin_value = *(int32_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            origin_value = *(uint32_t*)src;
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            if (*(int64_t*)src < 0) {
                LOG(WARNING) << "The input: " << *(int64_t*)src
                             << " is not valid, to_bitmap only support bigint value from 0 to "
                                "18446744073709551615 currently";
                return false;
            }
            origin_value = *(int64_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
            origin_value = *(uint64_t*)src;
            break;
        default:
            LOG(WARNING) << "the column type which was altered from was unsupported."
                         << " from_type=" << ref_column.type();
            return false;
        }
        bitmap.add(origin_value);
    }
    char* buf = reinterpret_cast<char*>(mem_pool->allocate(bitmap.getSizeInBytes()));
    Slice dst(buf, bitmap.getSizeInBytes());
    bitmap.write(dst.data);
    write_helper->set_field_content(field_idx, reinterpret_cast<char*>(&dst), mem_pool);
    return true;
}

bool hll_hash(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
              int field_idx, int ref_field_idx, MemPool* mem_pool) {
    write_helper->set_not_null(field_idx);
    HyperLogLog hll;
    if (!read_helper->is_null(ref_field_idx)) {
        uint64_t hash_value;

        switch (ref_column.type()) {
        case OLAP_FIELD_TYPE_CHAR: {
            int p = ref_column.length() - 1;
            Slice* slice = reinterpret_cast<Slice*>(read_helper->cell_ptr(ref_field_idx));
            char* buf = slice->data;
            while (p >= 0 && buf[p] == '\0') {
                p--;
            }
            slice->size = p + 1;
        }

        case OLAP_FIELD_TYPE_VARCHAR:
        case OLAP_FIELD_TYPE_STRING: {
            Slice slice = *reinterpret_cast<Slice*>(read_helper->cell_ptr(ref_field_idx));
            hash_value = HashUtil::murmur_hash64A(slice.data, slice.size, HashUtil::MURMUR_SEED);
            break;
        }
        case OLAP_FIELD_TYPE_BOOL:
        case OLAP_FIELD_TYPE_TINYINT:
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        case OLAP_FIELD_TYPE_SMALLINT:
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        case OLAP_FIELD_TYPE_INT:
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
        case OLAP_FIELD_TYPE_BIGINT:
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        case OLAP_FIELD_TYPE_LARGEINT:
        case OLAP_FIELD_TYPE_FLOAT:
        case OLAP_FIELD_TYPE_DOUBLE:
        case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        case OLAP_FIELD_TYPE_DATE:
        case OLAP_FIELD_TYPE_DATETIME: {
            std::string ref_column_string =
                    read_helper->column_schema(ref_field_idx)
                            ->type_info()
                            ->to_string(read_helper->cell_ptr(ref_field_idx));
            hash_value = HashUtil::murmur_hash64A(
                    ref_column_string.c_str(), ref_column_string.length(), HashUtil::MURMUR_SEED);
            break;
        }
        default:
            LOG(WARNING) << "fail to hll hash type : " << ref_column.type();
            return false;
        }

        hll.update(hash_value);
    }
    std::string buf;
    buf.resize(hll.max_serialized_size());
    buf.resize(hll.serialize((uint8_t*)buf.c_str()));
    Slice dst(buf);
    write_helper->set_field_content(field_idx, reinterpret_cast<char*>(&dst), mem_pool);
    return true;
}

bool count_field(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
                 int field_idx, int ref_field_idx, MemPool* mem_pool) {
    write_helper->set_not_null(field_idx);
    int64_t count = read_helper->is_null(ref_field_idx) ? 0 : 1;
    write_helper->set_field_content(field_idx, (char*)&count, mem_pool);
    return true;
}

OLAPStatus RowBlockChanger::change_row_block(const RowBlock* ref_block, int32_t data_version,
                                             RowBlock* mutable_block,
                                             uint64_t* filtered_rows) const {
    if (mutable_block == nullptr) {
        LOG(FATAL) << "mutable block is uninitialized.";
        return OLAP_ERR_NOT_INITED;
    } else if (mutable_block->tablet_schema().num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "mutable block does not match with schema mapping rules. "
                     << "block_schema_size=" << mutable_block->tablet_schema().num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return OLAP_ERR_NOT_INITED;
    }

    if (mutable_block->capacity() < ref_block->row_block_info().row_num) {
        LOG(WARNING) << "mutable block is not large enough for storing the changed block. "
                     << "mutable_block_size=" << mutable_block->capacity()
                     << ", ref_block_row_num=" << ref_block->row_block_info().row_num;
        return OLAP_ERR_NOT_INITED;
    }

    mutable_block->clear();

    RowCursor write_helper;
    if (write_helper.init(mutable_block->tablet_schema()) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init rowcursor.";
        return OLAP_ERR_NOT_INITED;
    }

    RowCursor read_helper;
    if (read_helper.init(ref_block->tablet_schema()) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init rowcursor.";
        return OLAP_ERR_NOT_INITED;
    }

    // a.1 First determine whether the data needs to be filtered, and finally only those marked as 1 are left as needed
    // For those without filter, it is equivalent to leave after setting all to 1
    const uint32_t row_num = ref_block->row_block_info().row_num;
    // (0 means no need to filter out, 1 means yes, during the process 2 means that this row needs to be cut and there is no need to compare other columns later)
    std::vector<int8_t> is_data_left_vec(row_num, 1);

    // Compare each row
    for (size_t row_index = 0; row_index < row_num; ++row_index) {
        ref_block->get_row(row_index, &read_helper);

        // filter data according to delete conditions specified in DeleteData command
        if (is_data_left_vec[row_index] == 1) {
            if (_delete_handler != nullptr &&
                _delete_handler->is_filter_data(data_version, read_helper)) {
                is_data_left_vec[row_index] = 0;
                (*filtered_rows)++;
            }
        }
    }

    // a.2 Calculate the left row num
    uint32_t new_row_num = row_num - *filtered_rows;

    const bool need_filter_data = (new_row_num != row_num);
    const bool filter_all = (new_row_num == 0);

    MemPool* mem_pool = mutable_block->mem_pool();
    // b. According to the previous filtering information, only processes that are also marked as 1
    for (size_t i = 0, len = mutable_block->tablet_schema().num_columns(); !filter_all && i < len;
         ++i) {
        int32_t ref_column = _schema_mapping[i].ref_column;
        if (_schema_mapping[i].ref_column >= 0) {
            if (!_schema_mapping[i].materialized_function.empty()) {
                bool (*_do_materialized_transform)(RowCursor*, RowCursor*, const TabletColumn&, int,
                                                   int, MemPool*);
                if (_schema_mapping[i].materialized_function == "to_bitmap") {
                    _do_materialized_transform = to_bitmap;
                } else if (_schema_mapping[i].materialized_function == "hll_hash") {
                    _do_materialized_transform = hll_hash;
                } else if (_schema_mapping[i].materialized_function == "count_field") {
                    _do_materialized_transform = count_field;
                } else {
                    LOG(WARNING) << "error materialized view function : "
                                 << _schema_mapping[i].materialized_function;
                    return OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID;
                }
                VLOG_NOTICE << "_schema_mapping[" << i << "].materialized_function : "
                            << _schema_mapping[i].materialized_function;
                for (size_t row_index = 0, new_row_index = 0;
                     row_index < ref_block->row_block_info().row_num; ++row_index) {
                    // No need row, need to be filter
                    if (need_filter_data && is_data_left_vec[row_index] == 0) {
                        continue;
                    }
                    mutable_block->get_row(new_row_index++, &write_helper);
                    ref_block->get_row(row_index, &read_helper);

                    if (!_do_materialized_transform(&read_helper, &write_helper,
                                                    ref_block->tablet_schema().column(ref_column),
                                                    i, _schema_mapping[i].ref_column, mem_pool)) {
                        return OLAP_ERR_DATA_QUALITY_ERR;
                    }
                }
                continue;
            }

            // new column will be assigned as referenced column
            // check if the type of new column is equal to the older's.
            FieldType reftype = ref_block->tablet_schema().column(ref_column).type();
            FieldType newtype = mutable_block->tablet_schema().column(i).type();
            if (newtype == reftype) {
                // Low efficiency, you can also directly calculate the variable length domain copy, but it will still destroy the package
                for (size_t row_index = 0, new_row_index = 0;
                     row_index < ref_block->row_block_info().row_num; ++row_index) {
                    // Unneeded row, skip every time this row is processed
                    if (need_filter_data && is_data_left_vec[row_index] == 0) {
                        continue;
                    }

                    // Specify the new row index to be written (different from the read row_index)
                    mutable_block->get_row(new_row_index++, &write_helper);
                    ref_block->get_row(row_index, &read_helper);

                    if (true == read_helper.is_null(ref_column)) {
                        write_helper.set_null(i);
                    } else {
                        write_helper.set_not_null(i);
                        if (newtype == OLAP_FIELD_TYPE_CHAR) {
                            // if modify length of CHAR type, the size of slice should be equal
                            // to new length.
                            Slice* src = (Slice*)(read_helper.cell_ptr(ref_column));
                            size_t size = mutable_block->tablet_schema().column(i).length();
                            char* buf = reinterpret_cast<char*>(mem_pool->allocate(size));
                            memset(buf, 0, size);
                            size_t copy_size = (size < src->size) ? size : src->size;
                            memcpy(buf, src->data, copy_size);
                            Slice dst(buf, size);
                            write_helper.set_field_content(i, reinterpret_cast<char*>(&dst),
                                                           mem_pool);
                        } else {
                            char* src = read_helper.cell_ptr(ref_column);
                            write_helper.set_field_content(i, src, mem_pool);
                        }
                    }
                }
            } else if (ConvertTypeResolver::instance()->get_convert_type_info(reftype, newtype)) {
                for (size_t row_index = 0, new_row_index = 0;
                     row_index < ref_block->row_block_info().row_num; ++row_index) {
                    // Skip filtered rows
                    if (need_filter_data && is_data_left_vec[row_index] == 0) {
                        continue;
                    }
                    mutable_block->get_row(new_row_index++, &write_helper);
                    ref_block->get_row(row_index, &read_helper);
                    if (read_helper.is_null(ref_column)) {
                        write_helper.set_null(i);
                    } else {
                        write_helper.set_not_null(i);
                        const Field* ref_field = read_helper.column_schema(ref_column);
                        char* ref_value = read_helper.cell_ptr(ref_column);
                        OLAPStatus st = write_helper.convert_from(i, ref_value,
                                                                  ref_field->type_info().get(), mem_pool);
                        if (st != OLAPStatus::OLAP_SUCCESS) {
                            LOG(WARNING)
                                    << "the column type which was altered from was unsupported."
                                    << "status:" << st << ", from_type=" << reftype
                                    << ", to_type=" << newtype;
                            return st;
                        }
                    }
                }
                // Write column i from ref_column.
            } else {
                // copy and alter the field
                // You can stay here for the time being, the new type does not involve type conversion for the time being
                switch (reftype) {
                case OLAP_FIELD_TYPE_TINYINT:
                    CONVERT_FROM_TYPE(int8_t);
                case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
                    CONVERT_FROM_TYPE(uint8_t);
                case OLAP_FIELD_TYPE_SMALLINT:
                    CONVERT_FROM_TYPE(int16_t);
                case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                    CONVERT_FROM_TYPE(uint16_t);
                case OLAP_FIELD_TYPE_INT:
                    CONVERT_FROM_TYPE(int32_t);
                case OLAP_FIELD_TYPE_UNSIGNED_INT:
                    CONVERT_FROM_TYPE(uint32_t);
                case OLAP_FIELD_TYPE_BIGINT:
                    CONVERT_FROM_TYPE(int64_t);
                case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                    CONVERT_FROM_TYPE(uint64_t);
                case OLAP_FIELD_TYPE_LARGEINT:
                    CONVERT_FROM_TYPE(int128_t);
                default:
                    LOG(WARNING) << "the column type which was altered from was unsupported."
                                 << " from_type="
                                 << ref_block->tablet_schema().column(ref_column).type();
                    return OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID;
                }

                if (newtype < reftype) {
                    VLOG_NOTICE << "type degraded while altering column. "
                                << "column=" << mutable_block->tablet_schema().column(i).name()
                                << ", origin_type="
                                << ref_block->tablet_schema().column(ref_column).type()
                                << ", alter_type="
                                << mutable_block->tablet_schema().column(i).type();
                }
            }
        } else {
            // New column, write default value
            for (size_t row_index = 0, new_row_index = 0;
                 row_index < ref_block->row_block_info().row_num; ++row_index) {
                // Unneeded row, skip every time this row is processed
                if (need_filter_data && is_data_left_vec[row_index] == 0) {
                    continue;
                }

                mutable_block->get_row(new_row_index++, &write_helper);

                if (_schema_mapping[i].default_value->is_null()) {
                    write_helper.set_null(i);
                } else {
                    write_helper.set_not_null(i);
                    write_helper.set_field_content(i, _schema_mapping[i].default_value->ptr(),
                                                   mem_pool);
                }
            }
        }
    }

    // NOTE The current row_num of mutable_block is still as much as ref
    // (Actually, you can re-init into less when init, the new_row_num left by the filter)
    // In split_table, there may be no data due to filtering
    mutable_block->finalize(new_row_num);
    return OLAP_SUCCESS;
}

#undef CONVERT_FROM_TYPE
#undef TYPE_REINTERPRET_CAST
#undef ASSIGN_DEFAULT_VALUE

RowBlockSorter::RowBlockSorter(RowBlockAllocator* row_block_allocator)
        : _row_block_allocator(row_block_allocator), _swap_row_block(nullptr) {}

RowBlockSorter::~RowBlockSorter() {
    if (_swap_row_block) {
        _row_block_allocator->release(_swap_row_block);
        _swap_row_block = nullptr;
    }
}

bool RowBlockSorter::sort(RowBlock** row_block) {
    uint32_t row_num = (*row_block)->row_block_info().row_num;
    bool null_supported = (*row_block)->row_block_info().null_supported;

    if (_swap_row_block == nullptr || _swap_row_block->capacity() < row_num) {
        if (_swap_row_block != nullptr) {
            _row_block_allocator->release(_swap_row_block);
            _swap_row_block = nullptr;
        }

        if (_row_block_allocator->allocate(&_swap_row_block, row_num, null_supported) !=
                    OLAP_SUCCESS) {
            LOG(WARNING) << "fail to allocate memory.";
            return false;
        }
    }

    RowCursor helper_row;
    auto res = helper_row.init(_swap_row_block->tablet_schema());
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "row cursor init failed.res:" << res;
        return false;
    }

    std::vector<std::unique_ptr<RowCursor>> row_cursor_list;
    row_cursor_list.reserve((*row_block)->row_block_info().row_num);
    // create an list of row cursor as long as the number of rows in data block.
    for (size_t i = 0; i < (*row_block)->row_block_info().row_num; ++i) {
        row_cursor_list.emplace_back(new (nothrow) RowCursor());
        if (row_cursor_list[i] == nullptr) {
            LOG(WARNING) << "failed to malloc RowCursor. size=" << sizeof(RowCursor);
            return false;
        }

        if (row_cursor_list[i]->init((*row_block)->tablet_schema()) != OLAP_SUCCESS) {
            return false;
        }

        (*row_block)->get_row(i, row_cursor_list[i].get());
    }

    // Must use 'std::' because this class has a function whose name is sort too
    std::stable_sort(row_cursor_list.begin(), row_cursor_list.end(), _row_cursor_comparator);

    // copy the results sorted to temp row block.
    _swap_row_block->clear();
    for (size_t i = 0; i < row_cursor_list.size(); ++i) {
        _swap_row_block->get_row(i, &helper_row);
        copy_row(&helper_row, *row_cursor_list[i], _swap_row_block->mem_pool());
    }

    _swap_row_block->finalize(row_cursor_list.size());

    // swap the row block for reducing memory allocating.
    std::swap(*row_block, _swap_row_block);

    return true;
}

RowBlockAllocator::RowBlockAllocator(const TabletSchema& tablet_schema, size_t memory_limitation)
        : _tablet_schema(tablet_schema),
          _mem_tracker(MemTracker::create_virtual_tracker(-1, "RowBlockAllocator")),
          _row_len(tablet_schema.row_size()),
          _memory_limitation(memory_limitation) {
    VLOG_NOTICE << "RowBlockAllocator(). row_len=" << _row_len;
}

RowBlockAllocator::~RowBlockAllocator() {
    if (_mem_tracker->consumption() != 0) {
        LOG(WARNING) << "memory lost in RowBlockAllocator. memory_size="
                     << _mem_tracker->consumption();
    }
}

OLAPStatus RowBlockAllocator::allocate(RowBlock** row_block, size_t num_rows, bool null_supported) {
    size_t row_block_size = _row_len * num_rows;

    if (_memory_limitation > 0 &&
        _mem_tracker->consumption() + row_block_size > _memory_limitation) {
        LOG(WARNING) << "RowBlockAllocator::alocate() memory exceeded. "
                     << "m_memory_allocated=" << _mem_tracker->consumption() << " "
                     << "mem limit for schema change=" << _memory_limitation << " "
                     << "You can increase the memory "
                     << "by changing the Config.memory_limitation_per_thread_for_schema_change_bytes";
        *row_block = nullptr;
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    // TODO(lijiao) : Why abandon the original m_row_block_buffer
    *row_block = new (nothrow) RowBlock(&_tablet_schema);

    if (*row_block == nullptr) {
        LOG(WARNING) << "failed to malloc RowBlock. size=" << sizeof(RowBlock);
        return OLAP_ERR_MALLOC_ERROR;
    }

    RowBlockInfo row_block_info(0U, num_rows);
    row_block_info.null_supported = null_supported;
    (*row_block)->init(row_block_info);

    _mem_tracker->consume(row_block_size);
    VLOG_NOTICE << "RowBlockAllocator::allocate() this=" << this << ", num_rows=" << num_rows
                << ", m_memory_allocated=" << _mem_tracker->consumption()
                << ", row_block_addr=" << *row_block;
    return OLAP_SUCCESS;
}

void RowBlockAllocator::release(RowBlock* row_block) {
    if (row_block == nullptr) {
        LOG(INFO) << "null row block released.";
        return;
    }

    _mem_tracker->release(row_block->capacity() * _row_len);

    VLOG_NOTICE << "RowBlockAllocator::release() this=" << this
                << ", num_rows=" << row_block->capacity()
                << ", m_memory_allocated=" << _mem_tracker->consumption()
                << ", row_block_addr=" << row_block;
    delete row_block;
}

bool RowBlockAllocator::is_memory_enough_for_sorting(size_t num_rows, size_t allocated_rows) {
    if (num_rows <= allocated_rows) {
        return true;
    }
    size_t row_block_size = _row_len * (num_rows - allocated_rows);
    return _mem_tracker->consumption() + row_block_size < _memory_limitation;
}

RowBlockMerger::RowBlockMerger(TabletSharedPtr tablet) : _tablet(tablet) {}

RowBlockMerger::~RowBlockMerger() {}

bool RowBlockMerger::merge(const std::vector<RowBlock*>& row_block_arr, RowsetWriter* rowset_writer,
                           uint64_t* merged_rows) {
    uint64_t tmp_merged_rows = 0;
    RowCursor row_cursor;
    std::unique_ptr<MemPool> mem_pool(new MemPool("RowBlockMerger"));
    std::unique_ptr<ObjectPool> agg_object_pool(new ObjectPool());
    if (row_cursor.init(_tablet->tablet_schema()) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init row cursor.";
        goto MERGE_ERR;
    }

    if (!_make_heap(row_block_arr)) {
        // There is error log in _make_heap, so no need to more log.
        goto MERGE_ERR;
    }

    row_cursor.allocate_memory_for_string_type(_tablet->tablet_schema());
    while (_heap.size() > 0) {
        init_row_with_others(&row_cursor, *(_heap.top().row_cursor), mem_pool.get(),
                             agg_object_pool.get());

        _pop_heap();

        if (KeysType::DUP_KEYS == _tablet->keys_type()) {
            if (rowset_writer->add_row(row_cursor) != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to add row to rowset writer.";
                goto MERGE_ERR;
            }
            continue;
        }

        while (!_heap.empty() && compare_row(row_cursor, *_heap.top().row_cursor) == 0) {
            // TODO(zc): Currently we keep nullptr to indicate that this is a query path,
            // we should fix this trick ASAP
            agg_update_row(&row_cursor, *(_heap.top().row_cursor), nullptr);
            ++tmp_merged_rows;
            _pop_heap();
        }
        agg_finalize_row(&row_cursor, mem_pool.get());
        if (rowset_writer->add_row(row_cursor) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add row to rowset writer.";
            goto MERGE_ERR;
        }

        // the memory allocate by mem pool has been copied,
        // so we should release memory immediately
        mem_pool->clear();
        agg_object_pool.reset(new ObjectPool());
    }
    if (rowset_writer->flush() != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to finalizing writer.";
        goto MERGE_ERR;
    }

    *merged_rows = tmp_merged_rows;
    return true;

MERGE_ERR:
    while (_heap.size() > 0) {
        MergeElement element = _heap.top();
        _heap.pop();
        SAFE_DELETE(element.row_cursor);
    }

    return false;
}

bool RowBlockMerger::_make_heap(const std::vector<RowBlock*>& row_block_arr) {
    for (auto row_block : row_block_arr) {
        MergeElement element;
        element.row_block = row_block;
        element.row_block_index = 0;
        element.row_cursor = new (nothrow) RowCursor();

        if (element.row_cursor == nullptr) {
            LOG(FATAL) << "failed to malloc RowCursor. size=" << sizeof(RowCursor);
            return false;
        }

        if (element.row_cursor->init(element.row_block->tablet_schema()) != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to init row cursor.";
            SAFE_DELETE(element.row_cursor);
            return false;
        }

        element.row_block->get_row(element.row_block_index, element.row_cursor);

        _heap.push(element);
    }

    return true;
}

void RowBlockMerger::_pop_heap() {
    MergeElement element = _heap.top();
    _heap.pop();

    if (++element.row_block_index >= element.row_block->row_block_info().row_num) {
        SAFE_DELETE(element.row_cursor);
        return;
    }

    element.row_block->get_row(element.row_block_index, element.row_cursor);

    _heap.push(element);
    return;
}

OLAPStatus LinkedSchemaChange::process(RowsetReaderSharedPtr rowset_reader,
                                       RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet,
                                       TabletSharedPtr base_tablet) {

    // In some cases, there may be more than one type of rowset in a tablet,
    // in which case the conversion cannot be done directly by linked schema change,
    // but requires direct schema change to rewrite the data.
    if (rowset_reader->type() != new_rowset_writer->type()) {
        LOG(INFO) << "the type of rowset " << rowset_reader->rowset()->rowset_id() << " in base tablet " << base_tablet->tablet_id()
                << " is not same as type " << new_rowset_writer->type() << ", use direct schema change.";
        SchemaChangeDirectly scd(_row_block_changer);
        return scd.process(rowset_reader, new_rowset_writer, new_tablet, base_tablet);
    } else {
        OLAPStatus status = new_rowset_writer->add_rowset_for_linked_schema_change(
                rowset_reader->rowset(), _row_block_changer.get_schema_mapping());
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to convert rowset."
                << ", new_tablet=" << new_tablet->full_name()
                << ", base_tablet=" << base_tablet->full_name()
                << ", version=" << new_rowset_writer->version().first << "-"
                << new_rowset_writer->version().second;
        }
        return status;
    }
}

SchemaChangeDirectly::SchemaChangeDirectly(const RowBlockChanger& row_block_changer)
        : SchemaChange(),
          _row_block_changer(row_block_changer),
          _row_block_allocator(nullptr),
          _cursor(nullptr) {}

SchemaChangeDirectly::~SchemaChangeDirectly() {
    VLOG_NOTICE << "~SchemaChangeDirectly()";
    SAFE_DELETE(_row_block_allocator);
    SAFE_DELETE(_cursor);
}

bool SchemaChangeDirectly::_write_row_block(RowsetWriter* rowset_writer, RowBlock* row_block) {
    for (uint32_t i = 0; i < row_block->row_block_info().row_num; i++) {
        row_block->get_row(i, _cursor);
        if (OLAP_SUCCESS != rowset_writer->add_row(*_cursor)) {
            LOG(WARNING) << "fail to write to new rowset for direct schema change";
            return false;
        }
    }

    return true;
}

OLAPStatus reserve_block(std::unique_ptr<RowBlock, RowBlockDeleter>* block_handle_ptr, int row_num,
                         RowBlockAllocator* allocator) {
    auto& block_handle = *block_handle_ptr;
    if (block_handle == nullptr || block_handle->capacity() < row_num) {
        // release old block and alloc new block
        if (block_handle != nullptr) {
            block_handle.reset();
        }
        RowBlock* new_row_block = nullptr;
        auto res = allocator->allocate(&new_row_block, row_num, true);
        RETURN_NOT_OK_LOG(res, "failed to allocate RowBlock.");
        block_handle.reset(new_row_block);
    } else {
        block_handle->clear();
    }
    return OLAP_SUCCESS;
}

OLAPStatus SchemaChangeDirectly::process(RowsetReaderSharedPtr rowset_reader,
                                         RowsetWriter* rowset_writer, TabletSharedPtr new_tablet,
                                         TabletSharedPtr base_tablet) {
    if (_row_block_allocator == nullptr) {
        _row_block_allocator = new RowBlockAllocator(new_tablet->tablet_schema(), 0);
        if (_row_block_allocator == nullptr) {
            LOG(FATAL) << "failed to malloc RowBlockAllocator. size=" << sizeof(RowBlockAllocator);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
    }

    if (nullptr == _cursor) {
        _cursor = new (nothrow) RowCursor();
        if (nullptr == _cursor) {
            LOG(WARNING) << "fail to allocate row cursor.";
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        if (OLAP_SUCCESS != _cursor->init(new_tablet->tablet_schema())) {
            LOG(WARNING) << "fail to init row cursor.";
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
    }

    OLAPStatus res = OLAP_SUCCESS;
    if (rowset_reader->rowset()->empty() ||
        rowset_reader->rowset()->num_rows() == 0) {
        res = rowset_writer->flush();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "create empty version for schema change failed."
                         << "version=" << rowset_writer->version().first << "-"
                         << rowset_writer->version().second;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        return OLAP_SUCCESS;
    }

    VLOG_NOTICE << "init writer. new_tablet=" << new_tablet->full_name()
                << ", block_row_number=" << new_tablet->num_rows_per_row_block();

    std::unique_ptr<RowBlock, RowBlockDeleter> new_row_block(nullptr, [&](RowBlock* block) {
        if (block != nullptr) {
            _row_block_allocator->release(block);
        }
    });

    // Reset filtered_rows and merged_rows statistic
    reset_merged_rows();
    reset_filtered_rows();

    RowBlock* ref_row_block = nullptr;
    rowset_reader->next_block(&ref_row_block);
    while (ref_row_block != nullptr && ref_row_block->has_remaining()) {
        // We will allocate blocks of the same size as before
        // to ensure that the data can be stored
        RETURN_NOT_OK(reserve_block(&new_row_block, ref_row_block->row_block_info().row_num,
                                    _row_block_allocator));

        // Change ref to new. This step is reasonable to say that it does need to wait for a large block, but theoretically it has nothing to do with the writer.
        uint64_t filtered_rows = 0;
        res = _row_block_changer.change_row_block(ref_row_block, rowset_reader->version().second,
                                                  new_row_block.get(), &filtered_rows);
        RETURN_NOT_OK_LOG(res, "failed to change data in row block.");

        // rows filtered by delete handler one by one
        add_filtered_rows(filtered_rows);

        if (!_write_row_block(rowset_writer, new_row_block.get())) {
            res = OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID;
            LOG(WARNING) << "failed to write row block.";
            return res;
        }

        ref_row_block->clear();
        rowset_reader->next_block(&ref_row_block);
    }

    if (OLAP_SUCCESS != rowset_writer->flush()) {
        return OLAP_ERR_ALTER_STATUS_ERR;
    }

    // rows filtered by zone map against delete handler
    add_filtered_rows(rowset_reader->filtered_rows());

    // Check row num changes
    if (config::row_nums_check) {
        if (rowset_reader->rowset()->num_rows() !=
            rowset_writer->num_rows() + merged_rows() + filtered_rows()) {
            LOG(WARNING) << "fail to check row num! "
                         << "source_rows=" << rowset_reader->rowset()->num_rows()
                         << ", merged_rows=" << merged_rows()
                         << ", filtered_rows=" << filtered_rows()
                         << ", new_index_rows=" << rowset_writer->num_rows();
            res = OLAP_ERR_ALTER_STATUS_ERR;
        }
    }
    LOG(INFO) << "all row nums. source_rows=" << rowset_reader->rowset()->num_rows()
              << ", merged_rows=" << merged_rows() << ", filtered_rows=" << filtered_rows()
              << ", new_index_rows=" << rowset_writer->num_rows();
    return res;
}

SchemaChangeWithSorting::SchemaChangeWithSorting(const RowBlockChanger& row_block_changer,
                                                 size_t memory_limitation)
        : SchemaChange(),
          _row_block_changer(row_block_changer),
          _memory_limitation(memory_limitation),
          _row_block_allocator(nullptr) {
    // Every time SchemaChange is used for external rowing, some temporary versions (such as 999, 1000, 1001) will be written, in order to avoid Cache conflicts, temporary
    // The version number takes a BIG NUMBER plus the version number of the current SchemaChange
    _temp_delta_versions.first = (1 << 28);
    _temp_delta_versions.second = (1 << 28);
    // TODO(zyh): remove the magic number
}

SchemaChangeWithSorting::~SchemaChangeWithSorting() {
    VLOG_NOTICE << "~SchemaChangeWithSorting()";
    SAFE_DELETE(_row_block_allocator);
}

OLAPStatus SchemaChangeWithSorting::process(RowsetReaderSharedPtr rowset_reader,
                                            RowsetWriter* new_rowset_writer,
                                            TabletSharedPtr new_tablet,
                                            TabletSharedPtr base_tablet) {
    if (_row_block_allocator == nullptr) {
        _row_block_allocator =
                new (nothrow) RowBlockAllocator(new_tablet->tablet_schema(), _memory_limitation);
        if (_row_block_allocator == nullptr) {
            LOG(FATAL) << "failed to malloc RowBlockAllocator. size=" << sizeof(RowBlockAllocator);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
    }

    OLAPStatus res = OLAP_SUCCESS;
    RowsetSharedPtr rowset = rowset_reader->rowset();

    if (rowset->empty() || rowset->num_rows() == 0) {
        res = new_rowset_writer->flush();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "create empty version for schema change failed."
                         << " version=" << new_rowset_writer->version().first << "-"
                         << new_rowset_writer->version().second;
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }
        return OLAP_SUCCESS;
    }

    RowBlockSorter row_block_sorter(_row_block_allocator);

    // for internal sorting
    RowBlock* new_row_block = nullptr;
    std::vector<RowBlock*> row_block_arr;

    // for external sorting
    // src_rowsets to store the rowset generated by internal sorting
    std::vector<RowsetSharedPtr> src_rowsets;

    Defer defer{[&]() {
        // remove the intermediate rowsets generated by internal sorting
        for (auto& row_set : src_rowsets) {
            StorageEngine::instance()->add_unused_rowset(row_set);
        }

        for (auto block : row_block_arr) {
            _row_block_allocator->release(block);
        }

        row_block_arr.clear();
    }};

    _temp_delta_versions.first = _temp_delta_versions.second;

    // Reset filtered_rows and merged_rows statistic
    reset_merged_rows();
    reset_filtered_rows();

    bool use_beta_rowset = new_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET;

    SegmentsOverlapPB segments_overlap = rowset->rowset_meta()->segments_overlap();
    RowBlock* ref_row_block = nullptr;
    rowset_reader->next_block(&ref_row_block);
    while (ref_row_block != nullptr && ref_row_block->has_remaining()) {
        if (OLAP_SUCCESS != _row_block_allocator->allocate(&new_row_block,
                                                           ref_row_block->row_block_info().row_num,
                                                           true)) {
            LOG(WARNING) << "failed to allocate RowBlock.";
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        } else {
            // do memory check for sorting, in case schema change task fail at row block sorting because of 
            // not doing internal sorting first
            if (!_row_block_allocator->is_memory_enough_for_sorting(ref_row_block->row_block_info().row_num,
                                                                row_block_sorter.num_rows())) {
                if (new_row_block != nullptr) {
                    _row_block_allocator->release(new_row_block);
                    new_row_block = nullptr;
                }
            }
        }

        if (new_row_block == nullptr) {
            if (row_block_arr.size() < 1) {
                LOG(WARNING) << "Memory limitation is too small for Schema Change."
                             << "memory_limitation=" << _memory_limitation;
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            }

            // enter here while memory limitation is reached.
            RowsetSharedPtr rowset;
            RowsetTypePB new_rowset_type = rowset_reader->rowset()->rowset_meta()->rowset_type();
            if (use_beta_rowset) {
                new_rowset_type = BETA_ROWSET;
            }
            if (!_internal_sorting(
                        row_block_arr,
                        Version(_temp_delta_versions.second, _temp_delta_versions.second),
                        new_tablet, new_rowset_type,
                        segments_overlap, &rowset)) {
                LOG(WARNING) << "failed to sorting internally.";
                return OLAP_ERR_ALTER_STATUS_ERR;
            }

            src_rowsets.push_back(rowset);

            for (auto block : row_block_arr) {
                _row_block_allocator->release(block);
            }

            row_block_arr.clear();

            // increase temp version
            ++_temp_delta_versions.second;
            continue;
        }

        uint64_t filtered_rows = 0;
        res = _row_block_changer.change_row_block(ref_row_block, rowset_reader->version().second,
                                                  new_row_block, &filtered_rows);
        if (res != OLAP_SUCCESS) {
            row_block_arr.push_back(new_row_block);
            LOG(WARNING) << "failed to change data in row block.";
            return res;
        }
        add_filtered_rows(filtered_rows);

        if (new_row_block->row_block_info().row_num > 0) {
            if (!row_block_sorter.sort(&new_row_block)) {
                row_block_arr.push_back(new_row_block);
                LOG(WARNING) << "failed to sort row block.";
                return OLAP_ERR_ALTER_STATUS_ERR;
            }
            row_block_arr.push_back(new_row_block);
        } else {
            LOG(INFO) << "new block num rows is: " << new_row_block->row_block_info().row_num;
            _row_block_allocator->release(new_row_block);
            new_row_block = nullptr;
        }

        ref_row_block->clear();
        rowset_reader->next_block(&ref_row_block);
    }

    if (!row_block_arr.empty()) {
        // enter here while memory limitation is reached.
        RowsetSharedPtr rowset = nullptr;

        RowsetTypePB new_rowset_type = rowset_reader->rowset()->rowset_meta()->rowset_type();
        if (use_beta_rowset) {
            new_rowset_type = BETA_ROWSET;
        }
        if (!_internal_sorting(row_block_arr,
                               Version(_temp_delta_versions.second, _temp_delta_versions.second),
                               new_tablet, new_rowset_type,
                               segments_overlap, &rowset)) {
            LOG(WARNING) << "failed to sorting internally.";
            return OLAP_ERR_ALTER_STATUS_ERR;
        }

        src_rowsets.push_back(rowset);

        for (auto block : row_block_arr) {
            _row_block_allocator->release(block);
        }

        row_block_arr.clear();

        // increase temp version
        ++_temp_delta_versions.second;
    }

    if (src_rowsets.empty()) {
        res = new_rowset_writer->flush();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "create empty version for schema change failed."
                         << " version=" << new_rowset_writer->version().first << "-"
                         << new_rowset_writer->version().second;
            return OLAP_ERR_ALTER_STATUS_ERR;
        }
    } else if (!_external_sorting(src_rowsets, new_rowset_writer, new_tablet)) {
        LOG(WARNING) << "failed to sorting externally.";
        return OLAP_ERR_ALTER_STATUS_ERR;
    }

    add_filtered_rows(rowset_reader->filtered_rows());

    // Check row num changes
    if (config::row_nums_check) {
        if (rowset_reader->rowset()->num_rows() !=
            new_rowset_writer->num_rows() + merged_rows() + filtered_rows()) {
            LOG(WARNING) << "fail to check row num!"
                         << " source_rows=" << rowset_reader->rowset()->num_rows()
                         << ", merged_rows=" << merged_rows()
                         << ", filtered_rows=" << filtered_rows()
                         << ", new_index_rows=" << new_rowset_writer->num_rows();
            res = OLAP_ERR_ALTER_STATUS_ERR;
        }
    }
    LOG(INFO) << "all row nums. source_rows=" << rowset_reader->rowset()->num_rows()
              << ", merged_rows=" << merged_rows() << ", filtered_rows=" << filtered_rows()
              << ", new_index_rows=" << new_rowset_writer->num_rows();
    return res;
}

bool SchemaChangeWithSorting::_internal_sorting(const std::vector<RowBlock*>& row_block_arr,
                                                const Version& version,
                                                TabletSharedPtr new_tablet,
                                                RowsetTypePB new_rowset_type,
                                                SegmentsOverlapPB segments_overlap,
                                                RowsetSharedPtr* rowset) {
    uint64_t merged_rows = 0;
    RowBlockMerger merger(new_tablet);

    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = new_tablet->tablet_uid();
    context.tablet_id = new_tablet->tablet_id();
    context.partition_id = new_tablet->partition_id();
    context.tablet_schema_hash = new_tablet->schema_hash();
    context.rowset_type = new_rowset_type;
    context.path_desc = new_tablet->tablet_path_desc();
    context.tablet_schema = &(new_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = version;
    context.segments_overlap = segments_overlap;

    VLOG_NOTICE << "init rowset builder. tablet=" << new_tablet->full_name()
                << ", block_row_size=" << new_tablet->num_rows_per_row_block();

    std::unique_ptr<RowsetWriter> rowset_writer;
    if (RowsetFactory::create_rowset_writer(context, &rowset_writer) != OLAP_SUCCESS) {
        return false;
    }

    if (!merger.merge(row_block_arr, rowset_writer.get(), &merged_rows)) {
        LOG(WARNING) << "failed to merge row blocks.";
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
        return false;
    }
    new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                               rowset_writer->rowset_id().to_string());
    add_merged_rows(merged_rows);
    *rowset = rowset_writer->build();
    return true;
}

bool SchemaChangeWithSorting::_external_sorting(vector<RowsetSharedPtr>& src_rowsets,
                                                RowsetWriter* rowset_writer,
                                                TabletSharedPtr new_tablet) {
    std::vector<RowsetReaderSharedPtr> rs_readers;
    for (auto& rowset : src_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        auto res = rowset->create_reader(&rs_reader);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to create rowset reader.";
            return false;
        }
        rs_readers.push_back(std::move(rs_reader));
    }

    Merger::Statistics stats;
    auto res = Merger::merge_rowsets(new_tablet, READER_ALTER_TABLE, rs_readers, rowset_writer,
                                     &stats);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to merge rowsets. tablet=" << new_tablet->full_name()
                     << ", version=" << rowset_writer->version().first << "-"
                     << rowset_writer->version().second;
        return false;
    }
    add_merged_rows(stats.merged_rows);
    add_filtered_rows(stats.filtered_rows);
    return true;
}

SchemaChangeHandler::SchemaChangeHandler()
        : _mem_tracker(MemTracker::create_tracker(-1, "SchemaChangeHandler", StorageEngine::instance()->schema_change_mem_tracker())) {
    REGISTER_HOOK_METRIC(schema_change_mem_consumption,
                         [this]() { return _mem_tracker->consumption(); });
}

SchemaChangeHandler::~SchemaChangeHandler() {
    DEREGISTER_HOOK_METRIC(schema_change_mem_consumption);
}

OLAPStatus SchemaChangeHandler::process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    LOG(INFO) << "begin to do request alter tablet: base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << request.alter_version;

    TabletSharedPtr base_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.base_tablet_id, request.base_schema_hash);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }
    // Lock schema_change_lock util schema change info is stored in tablet header
    std::unique_lock<std::mutex> schema_change_lock(base_tablet->get_schema_change_lock(), std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "failed to obtain schema change lock. "
                     << "base_tablet=" << request.base_tablet_id;
        return OLAP_ERR_TRY_LOCK_FAILED;
    }

    OLAPStatus res = _do_process_alter_tablet_v2(request);
    LOG(INFO) << "finished alter tablet process, res=" << res;
    return res;
}

// In the past schema change and rollup will create new tablet  and will wait for txns starting before the task to finished
// It will cost a lot of time to wait and the task is very difficult to understand.
// In alter task v2, FE will call BE to create tablet and send an alter task to BE to convert historical data.
// The admin should upgrade all BE and then upgrade FE.
// Should delete the old code after upgrade finished.
OLAPStatus SchemaChangeHandler::_do_process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    OLAPStatus res = OLAP_SUCCESS;
    TabletSharedPtr base_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.base_tablet_id, request.base_schema_hash);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id
                     << ", base_schema_hash=" << request.base_schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // new tablet has to exist
    TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
            request.new_tablet_id, request.new_schema_hash);
    if (new_tablet == nullptr) {
        LOG(WARNING) << "fail to find new tablet."
                     << " new_tablet=" << request.new_tablet_id
                     << ", new_schema_hash=" << request.new_schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // check if tablet's state is not_ready, if it is ready, it means the tablet already finished
    // check whether the tablet's max continuous version == request.version
    if (new_tablet->tablet_state() != TABLET_NOTREADY) {
        res = _validate_alter_result(new_tablet, request);
        LOG(INFO) << "tablet's state=" << new_tablet->tablet_state()
                  << " the convert job already finished, check its version"
                  << " res=" << res;
        return res;
    }

    LOG(INFO) << "finish to validate alter tablet request. begin to convert data from base tablet "
                 "to new tablet"
              << " base_tablet=" << base_tablet->full_name()
              << " new_tablet=" << new_tablet->full_name();

    ReadLock base_migration_rlock(base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return OLAP_ERR_RWLOCK_ERROR;
    }
    ReadLock new_migration_rlock(new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return OLAP_ERR_RWLOCK_ERROR;
    }

    std::vector<Version> versions_to_be_changed;
    std::vector<RowsetReaderSharedPtr> rs_readers;
    // delete handlers for new tablet
    DeleteHandler delete_handler;
    std::vector<ColumnId> return_columns;

    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data
    {
        std::lock_guard<std::mutex> base_tablet_lock(base_tablet->get_push_lock());
        std::lock_guard<std::mutex> new_tablet_lock(new_tablet->get_push_lock());
        WriteLock base_tablet_rdlock(base_tablet->get_header_lock());
        WriteLock new_tablet_rdlock(new_tablet->get_header_lock());
        // check if the tablet has alter task
        // if it has alter task, it means it is under old alter process
        size_t num_cols = base_tablet->tablet_schema().num_columns();
        return_columns.resize(num_cols);
        for (int i = 0; i < num_cols; ++i) {
            return_columns[i] = i;
        }

        // reader_context is stack variables, it's lifetime should keep the same
        // with rs_readers
        RowsetReaderContext reader_context;
        reader_context.reader_type = READER_ALTER_TABLE;
        reader_context.tablet_schema = &base_tablet->tablet_schema();
        reader_context.need_ordered_result = true;
        reader_context.delete_handler = &delete_handler;
        reader_context.return_columns = &return_columns;
        // for schema change, seek_columns is the same to return_columns
        reader_context.seek_columns = &return_columns;
        reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();

        // TODO(zxy) switch to tls mem tracker
        auto mem_tracker = MemTracker::create_tracker(
                -1,
                "AlterTablet:" + std::to_string(base_tablet->tablet_id()) + "-" +
                        std::to_string(new_tablet->tablet_id()),
                _mem_tracker, MemTrackerLevel::TASK);

        do {
            // get history data to be converted and it will check if there is hold in base tablet
            res = _get_versions_to_be_changed(base_tablet, &versions_to_be_changed);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to get version to be changed. res=" << res;
                break;
            }

            // should check the max_version >= request.alter_version, if not the convert is useless
            RowsetSharedPtr max_rowset = base_tablet->rowset_with_max_version();
            if (max_rowset == nullptr || max_rowset->end_version() < request.alter_version) {
                LOG(WARNING) << "base tablet's max version="
                             << (max_rowset == nullptr ? 0 : max_rowset->end_version())
                             << " is less than request version=" << request.alter_version;
                res = OLAP_ERR_VERSION_NOT_EXIST;
                break;
            }
            // before calculating version_to_be_changed,
            // remove all data from new tablet, prevent to rewrite data(those double pushed when wait)
            LOG(INFO) << "begin to remove all data from new tablet to prevent rewrite."
                      << " new_tablet=" << new_tablet->full_name();
            std::vector<RowsetSharedPtr> rowsets_to_delete;
            std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
            new_tablet->acquire_version_and_rowsets(&version_rowsets);
            for (auto& pair : version_rowsets) {
                if (pair.first.second <= max_rowset->end_version()) {
                    rowsets_to_delete.push_back(pair.second);
                }
            }
            std::vector<RowsetSharedPtr> empty_vec;
            new_tablet->modify_rowsets(empty_vec, rowsets_to_delete);
            // inherit cumulative_layer_point from base_tablet
            // check if new_tablet.ce_point > base_tablet.ce_point?
            new_tablet->set_cumulative_layer_point(-1);
            // save tablet meta
            new_tablet->save_meta();
            for (auto& rowset : rowsets_to_delete) {
                // do not call rowset.remove directly, using gc thread to delete it
                StorageEngine::instance()->add_unused_rowset(rowset);
            }

            // init one delete handler
            int32_t end_version = -1;
            for (auto& version : versions_to_be_changed) {
                if (version.second > end_version) {
                    end_version = version.second;
                }
            }

            res = delete_handler.init(base_tablet->tablet_schema(), base_tablet->delete_predicates(),
                                      end_version);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "init delete handler failed. base_tablet=" << base_tablet->full_name()
                             << ", end_version=" << end_version;

                // release delete handlers which have been inited successfully.
                delete_handler.finalize();
                break;
            }

            // acquire data sources correspond to history versions
            base_tablet->capture_rs_readers(versions_to_be_changed, &rs_readers);
            if (rs_readers.size() < 1) {
                LOG(WARNING) << "fail to acquire all data sources. "
                             << "version_num=" << versions_to_be_changed.size()
                             << ", data_source_num=" << rs_readers.size();
                res = OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS;
                break;
            }

            for (auto& rs_reader : rs_readers) {
                res = rs_reader->init(&reader_context);
                if (res != OLAP_SUCCESS) {
                    LOG(WARNING) << "failed to init rowset reader: " << base_tablet->full_name();
                    break;
                }
            }

        } while (0);
    }

    do {
        if (res != OLAP_SUCCESS) {
            break;
        }
        SchemaChangeParams sc_params;
        sc_params.base_tablet = base_tablet;
        sc_params.new_tablet = new_tablet;
        sc_params.ref_rowset_readers = rs_readers;
        sc_params.delete_handler = &delete_handler;
        if (request.__isset.materialized_view_params) {
            for (auto item : request.materialized_view_params) {
                AlterMaterializedViewParam mv_param;
                mv_param.column_name = item.column_name;
                /*
                 * origin_column_name is always be set now,
                 * but origin_column_name may be not set in some materialized view function. eg:count(1)
                */
                if (item.__isset.origin_column_name) {
                    mv_param.origin_column_name = item.origin_column_name;
                }

                /*
                * TODO(lhy)
                * Building the materialized view function for schema_change here based on defineExpr.
                * This is a trick because the current storage layer does not support expression evaluation.
                * We can refactor this part of the code until the uniform expression evaluates the logic.
                * count distinct materialized view will set mv_expr with to_bitmap or hll_hash.
                * count materialized view will set mv_expr with count.
                */
                if (item.__isset.mv_expr) {
                    if (item.mv_expr.nodes[0].node_type == TExprNodeType::FUNCTION_CALL) {
                        mv_param.mv_expr = item.mv_expr.nodes[0].fn.name.function_name;
                    } else if (item.mv_expr.nodes[0].node_type == TExprNodeType::CASE_EXPR) {
                        mv_param.mv_expr = "count_field";
                    }
                }
                sc_params.materialized_params_map.insert(
                        std::make_pair(item.column_name, mv_param));
            }
        }

        res = _convert_historical_rowsets(sc_params);
        if (res != OLAP_SUCCESS) {
            break;
        }
        // set state to ready
        WriteLock new_wlock(new_tablet->get_header_lock());
        res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
        if (res != OLAP_SUCCESS) {
            break;
        }
        new_tablet->save_meta();
    } while (0);

    if (res == OLAP_SUCCESS) {
        // _validate_alter_result should be outside the above while loop.
        // to avoid requiring the header lock twice.
        res = _validate_alter_result(new_tablet, request);
    }

    // if failed convert history data, then just remove the new tablet
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                     << ", drop new_tablet=" << new_tablet->full_name();
        // do not drop the new tablet and its data. GC thread will
    }

    return res;
}

OLAPStatus SchemaChangeHandler::schema_version_convert(TabletSharedPtr base_tablet,
                                                       TabletSharedPtr new_tablet,
                                                       RowsetSharedPtr* base_rowset,
                                                       RowsetSharedPtr* new_rowset) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "begin to convert delta version for schema changing. "
              << "base_tablet=" << base_tablet->full_name()
              << ", new_tablet=" << new_tablet->full_name();

    // a. Parse the Alter request and convert it into an internal representation
    // Do not use the delete condition specified by the DELETE_DATA command
    RowBlockChanger rb_changer(new_tablet->tablet_schema());
    bool sc_sorting = false;
    bool sc_directly = false;

    const std::unordered_map<std::string, AlterMaterializedViewParam> materialized_function_map;
    if (OLAP_SUCCESS != (res = _parse_request(base_tablet, new_tablet, &rb_changer, &sc_sorting,
                                              &sc_directly, materialized_function_map))) {
        LOG(WARNING) << "failed to parse the request. res=" << res;
        return res;
    }

    // NOTE split_table if row_block is used, the original block will become smaller
    // But since the historical data will become normal after the subsequent base/cumulative, it is also possible to use directly
    // b. Generate historical data converter
    SchemaChange* sc_procedure = nullptr;
    if (sc_sorting) {
        LOG(INFO) << "doing schema change with sorting for base_tablet "
                  << base_tablet->full_name();
        sc_procedure = new (nothrow) SchemaChangeWithSorting(
                rb_changer,
                config::memory_limitation_per_thread_for_schema_change_bytes);
    } else if (sc_directly) {
        LOG(INFO) << "doing schema change directly for base_tablet " << base_tablet->full_name();
        sc_procedure = new (nothrow) SchemaChangeDirectly(rb_changer);
    } else {
        LOG(INFO) << "doing linked schema change for base_tablet " << base_tablet->full_name();
        sc_procedure = new (nothrow) LinkedSchemaChange(rb_changer);
    }

    if (sc_procedure == nullptr) {
        LOG(FATAL) << "failed to malloc SchemaChange. size=" << sizeof(SchemaChangeWithSorting);
        return OLAP_ERR_MALLOC_ERROR;
    }

    // c. Convert data
    DeleteHandler delete_handler;
    std::vector<ColumnId> return_columns;
    size_t num_cols = base_tablet->tablet_schema().num_columns();
    return_columns.resize(num_cols);
    for (int i = 0; i < num_cols; ++i) {
        return_columns[i] = i;
    }

    RowsetReaderContext reader_context;
    reader_context.reader_type = READER_ALTER_TABLE;
    reader_context.tablet_schema = &base_tablet->tablet_schema();
    reader_context.need_ordered_result = true;
    reader_context.delete_handler = &delete_handler;
    reader_context.return_columns = &return_columns;
    reader_context.seek_columns = &return_columns;
    reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();

    RowsetReaderSharedPtr rowset_reader;
    RETURN_NOT_OK((*base_rowset)->create_reader(&rowset_reader));
    RETURN_NOT_OK(rowset_reader->init(&reader_context));

    RowsetWriterContext writer_context;
    writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
    writer_context.tablet_uid = new_tablet->tablet_uid();
    writer_context.tablet_id = new_tablet->tablet_id();
    writer_context.partition_id = (*base_rowset)->partition_id();
    writer_context.tablet_schema_hash = new_tablet->schema_hash();
    writer_context.rowset_type = (*base_rowset)->rowset_meta()->rowset_type();
    if (new_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
        writer_context.rowset_type = BETA_ROWSET;
    }
    writer_context.path_desc = new_tablet->tablet_path_desc();
    writer_context.tablet_schema = &(new_tablet->tablet_schema());
    writer_context.rowset_state = PREPARED;
    writer_context.txn_id = (*base_rowset)->txn_id();
    writer_context.load_id.set_hi((*base_rowset)->load_id().hi());
    writer_context.load_id.set_lo((*base_rowset)->load_id().lo());
    writer_context.segments_overlap = (*base_rowset)->rowset_meta()->segments_overlap();

    std::unique_ptr<RowsetWriter> rowset_writer;
    RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);

    if ((res = sc_procedure->process(rowset_reader, rowset_writer.get(), new_tablet,
                                     base_tablet)) != OLAP_SUCCESS) {
        if ((*base_rowset)->is_pending()) {
            LOG(WARNING) << "failed to process the transaction when schema change. "
                         << "tablet=" << new_tablet->full_name() << "'"
                         << ", transaction=" << (*base_rowset)->txn_id();
        } else {
            LOG(WARNING) << "failed to process the version. "
                         << "version=" << (*base_rowset)->version().first << "-"
                         << (*base_rowset)->version().second;
        }
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
        goto SCHEMA_VERSION_CONVERT_ERR;
    }
    *new_rowset = rowset_writer->build();
    new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                               rowset_writer->rowset_id().to_string());
    if (*new_rowset == nullptr) {
        LOG(WARNING) << "build rowset failed.";
        res = OLAP_ERR_MALLOC_ERROR;
        goto SCHEMA_VERSION_CONVERT_ERR;
    }

    SAFE_DELETE(sc_procedure);
    LOG(INFO) << "successfully convert rowsets. "
              << " base_tablet=" << base_tablet->full_name()
              << ", new_tablet=" << new_tablet->full_name();
    return res;

SCHEMA_VERSION_CONVERT_ERR:
    if (*new_rowset != nullptr) {
        StorageEngine::instance()->add_unused_rowset(*new_rowset);
    }

    SAFE_DELETE(sc_procedure);
    LOG(WARNING) << "failed to convert rowsets. "
                 << " base_tablet=" << base_tablet->full_name()
                 << ", new_tablet=" << new_tablet->full_name() << " res = " << res;
    return res;
}

OLAPStatus SchemaChangeHandler::_get_versions_to_be_changed(
        TabletSharedPtr base_tablet, std::vector<Version>* versions_to_be_changed) {
    RowsetSharedPtr rowset = base_tablet->rowset_with_max_version();
    if (rowset == nullptr) {
        LOG(WARNING) << "Tablet has no version. base_tablet=" << base_tablet->full_name();
        return OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS;
    }

    std::vector<Version> span_versions;
    RETURN_NOT_OK(base_tablet->capture_consistent_versions(Version(0, rowset->version().second),
                                                           &span_versions));
    versions_to_be_changed->insert(versions_to_be_changed->end(), span_versions.begin(),
                                   span_versions.end());

    return OLAP_SUCCESS;
}

OLAPStatus SchemaChangeHandler::_convert_historical_rowsets(const SchemaChangeParams& sc_params) {
    LOG(INFO) << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();

    // find end version
    int32_t end_version = -1;
    for (size_t i = 0; i < sc_params.ref_rowset_readers.size(); ++i) {
        if (sc_params.ref_rowset_readers[i]->version().second > end_version) {
            end_version = sc_params.ref_rowset_readers[i]->version().second;
        }
    }

    // Add filter information in change, and filter column information will be set in _parse_request
    // And filter some data every time the row block changes
    RowBlockChanger rb_changer(sc_params.new_tablet->tablet_schema(), sc_params.delete_handler);

    bool sc_sorting = false;
    bool sc_directly = false;
    SchemaChange* sc_procedure = nullptr;

    // a.Parse the Alter request and convert it into an internal representation
    OLAPStatus res = _parse_request(sc_params.base_tablet, sc_params.new_tablet, &rb_changer,
                                    &sc_sorting, &sc_directly, sc_params.materialized_params_map);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to parse the request. res=" << res;
        goto PROCESS_ALTER_EXIT;
    }

    // b. Generate historical data converter
    if (sc_sorting) {
        LOG(INFO) << "doing schema change with sorting for base_tablet "
                  << sc_params.base_tablet->full_name();
        sc_procedure = new (nothrow) SchemaChangeWithSorting(
                rb_changer,
                config::memory_limitation_per_thread_for_schema_change_bytes);
    } else if (sc_directly) {
        LOG(INFO) << "doing schema change directly for base_tablet "
                  << sc_params.base_tablet->full_name();
        sc_procedure = new (nothrow) SchemaChangeDirectly(rb_changer);
    } else {
        LOG(INFO) << "doing linked schema change for base_tablet "
                  << sc_params.base_tablet->full_name();
        sc_procedure = new (nothrow) LinkedSchemaChange(rb_changer);
    }

    if (sc_procedure == nullptr) {
        LOG(WARNING) << "failed to malloc SchemaChange. "
                     << "malloc_size=" << sizeof(SchemaChangeWithSorting);
        res = OLAP_ERR_MALLOC_ERROR;
        goto PROCESS_ALTER_EXIT;
    }

    // c.Convert historical data
    for (auto& rs_reader : sc_params.ref_rowset_readers) {
        VLOG_TRACE << "begin to convert a history rowset. version=" << rs_reader->version().first
                   << "-" << rs_reader->version().second;

        // set status for monitor
        // As long as there is a new_table as running, ref table is set as running
        // NOTE If the first sub_table fails first, it will continue to go as normal here
        TabletSharedPtr new_tablet = sc_params.new_tablet;

        RowsetWriterContext writer_context;
        writer_context.rowset_id = StorageEngine::instance()->next_rowset_id();
        writer_context.tablet_uid = new_tablet->tablet_uid();
        writer_context.tablet_id = new_tablet->tablet_id();
        writer_context.partition_id = new_tablet->partition_id();
        writer_context.tablet_schema_hash = new_tablet->schema_hash();
        // linked schema change can't change rowset type, therefore we preserve rowset type in schema change now
        writer_context.rowset_type = rs_reader->rowset()->rowset_meta()->rowset_type();
        if (sc_params.new_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
            // Use beta rowset to do schema change
            // And in this case, linked schema change will not be used.
            writer_context.rowset_type = BETA_ROWSET;
        }
        writer_context.path_desc = new_tablet->tablet_path_desc();
        writer_context.tablet_schema = &(new_tablet->tablet_schema());
        writer_context.rowset_state = VISIBLE;
        writer_context.version = rs_reader->version();
        writer_context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();

        std::unique_ptr<RowsetWriter> rowset_writer;
        OLAPStatus status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (status != OLAP_SUCCESS) {
            res = OLAP_ERR_ROWSET_BUILDER_INIT;
            goto PROCESS_ALTER_EXIT;
        }

        if ((res = sc_procedure->process(rs_reader, rowset_writer.get(), sc_params.new_tablet,
                                         sc_params.base_tablet)) != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to process the version."
                         << " version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                       rowset_writer->rowset_id().to_string());
            goto PROCESS_ALTER_EXIT;
        }
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
        // Add the new version of the data to the header
        // In order to prevent the occurrence of deadlock, we must first lock the old table, and then lock the new table
        std::lock_guard<std::mutex> lock(sc_params.new_tablet->get_push_lock());
        RowsetSharedPtr new_rowset = rowset_writer->build();
        if (new_rowset == nullptr) {
            LOG(WARNING) << "failed to build rowset, exit alter process";
            goto PROCESS_ALTER_EXIT;
        }
        res = sc_params.new_tablet->add_rowset(new_rowset, false);
        if (res == OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
            LOG(WARNING) << "version already exist, version revert occurred. "
                         << "tablet=" << sc_params.new_tablet->full_name() << ", version='"
                         << rs_reader->version().first << "-" << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            res = OLAP_SUCCESS;
        } else if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to register new version. "
                         << " tablet=" << sc_params.new_tablet->full_name()
                         << ", version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            goto PROCESS_ALTER_EXIT;
        } else {
            VLOG_NOTICE << "register new version. tablet=" << sc_params.new_tablet->full_name()
                        << ", version=" << rs_reader->version().first << "-"
                        << rs_reader->version().second;
        }

        VLOG_TRACE << "succeed to convert a history version."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }
    // XXX:The SchemaChange state should not be canceled at this time, because the new Delta has to be converted to the old and new Schema version
PROCESS_ALTER_EXIT : {
    // save tablet meta here because rowset meta is not saved during add rowset
    WriteLock new_wlock(sc_params.new_tablet->get_header_lock());
    sc_params.new_tablet->save_meta();
}
    if (res == OLAP_SUCCESS) {
        Version test_version(0, end_version);
        res = sc_params.new_tablet->check_version_integrity(test_version);
    }
    SAFE_DELETE(sc_procedure);

    LOG(INFO) << "finish converting rowsets for new_tablet from base_tablet. "
              << "base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();
    return res;
}

// @static
// Analyze the mapping of the column and the mapping of the filter key
OLAPStatus SchemaChangeHandler::_parse_request(
        TabletSharedPtr base_tablet, TabletSharedPtr new_tablet, RowBlockChanger* rb_changer,
        bool* sc_sorting, bool* sc_directly,
        const std::unordered_map<std::string, AlterMaterializedViewParam>&
                materialized_function_map) {
    OLAPStatus res = OLAP_SUCCESS;

    // set column mapping
    for (int i = 0, new_schema_size = new_tablet->tablet_schema().num_columns();
         i < new_schema_size; ++i) {
        const TabletColumn& new_column = new_tablet->tablet_schema().column(i);
        const string& column_name = new_column.name();
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);

        if (new_column.has_reference_column()) {
            int32_t column_index = base_tablet->field_index(new_column.referenced_column());

            if (column_index < 0) {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_index
                             << "]";
                return OLAP_ERR_CE_CMD_PARAMS_ERROR;
            }

            column_mapping->ref_column = column_index;
            VLOG_NOTICE << "A column refered to existed column will be added after schema changing."
                        << "column=" << column_name << ", ref_column=" << column_index;
            continue;
        }

        if (materialized_function_map.find(column_name) != materialized_function_map.end()) {
            AlterMaterializedViewParam mvParam =
                    materialized_function_map.find(column_name)->second;
            column_mapping->materialized_function = mvParam.mv_expr;
            std::string origin_column_name = mvParam.origin_column_name;
            int32_t column_index = base_tablet->field_index(origin_column_name);
            if (column_index >= 0) {
                column_mapping->ref_column = column_index;
                continue;
            } else {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_index
                             << "]";
                return OLAP_ERR_CE_CMD_PARAMS_ERROR;
            }
        }

        int32_t column_index = base_tablet->field_index(column_name);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
            continue;
        }

        // Newly added column go here
        column_mapping->ref_column = -1;

        if (i < base_tablet->num_short_key_columns()) {
            *sc_directly = true;
        }

        if (OLAP_SUCCESS != (res = _init_column_mapping(column_mapping, new_column,
                                                        new_column.default_value()))) {
           return res;
        }

        VLOG_TRACE << "A column with default value will be added after schema changing. "
                   << "column=" << column_name
                   << ", default_value=" << new_column.default_value();
    }

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // If the reference sequence of the Key column is out of order, it needs to be reordered
    int num_default_value = 0;

    for (int i = 0, new_schema_size = new_tablet->num_key_columns(); i < new_schema_size; ++i) {
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);

        if (column_mapping->ref_column < 0) {
            num_default_value++;
            continue;
        }

        if (column_mapping->ref_column != i - num_default_value) {
            *sc_sorting = true;
            return OLAP_SUCCESS;
        }
    }

    const TabletSchema& ref_tablet_schema = base_tablet->tablet_schema();
    const TabletSchema& new_tablet_schema = new_tablet->tablet_schema();
    if (ref_tablet_schema.keys_type() != new_tablet_schema.keys_type()) {
        // only when base table is dup and mv is agg
        // the rollup job must be reagg.
        *sc_sorting = true;
        return OLAP_SUCCESS;
    }

    // If the sort of key has not been changed but the new keys num is less then base's,
    // the new table should be re agg.
    // So we also need to set  sc_sorting = true.
    // A, B, C are keys(sort keys), D is value
    // followings need resort:
    //      old keys:    A   B   C   D
    //      new keys:    A   B
    if (new_tablet_schema.keys_type() != KeysType::DUP_KEYS &&
        new_tablet->num_key_columns() < base_tablet->num_key_columns()) {
        // this is a table with aggregate key type, and num of key columns in new schema
        // is less, which means the data in new tablet should be more aggregated.
        // so we use sorting schema change to sort and merge the data.
        *sc_sorting = true;
        return OLAP_SUCCESS;
    }

    if (base_tablet->num_short_key_columns() != new_tablet->num_short_key_columns()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return OLAP_SUCCESS;
    }

    for (size_t i = 0; i < new_tablet->num_columns(); ++i) {
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);
        if (column_mapping->ref_column < 0) {
            continue;
        } else {
            if (new_tablet_schema.column(i).type() !=
                ref_tablet_schema.column(column_mapping->ref_column).type()) {
                *sc_directly = true;
                return OLAP_SUCCESS;
            } else if ((new_tablet_schema.column(i).type() ==
                        ref_tablet_schema.column(column_mapping->ref_column).type()) &&
                       (new_tablet_schema.column(i).length() !=
                        ref_tablet_schema.column(column_mapping->ref_column).length())) {
                *sc_directly = true;
                return OLAP_SUCCESS;

            } else if (new_tablet_schema.column(i).is_bf_column() !=
                       ref_tablet_schema.column(column_mapping->ref_column).is_bf_column()) {
                *sc_directly = true;
                return OLAP_SUCCESS;
            } else if (new_tablet_schema.column(i).has_bitmap_index() !=
                       ref_tablet_schema.column(column_mapping->ref_column).has_bitmap_index()) {
                *sc_directly = true;
                return OLAP_SUCCESS;
            }
        }
    }

    if (base_tablet->delete_predicates().size() != 0) {
        //there exists delete condition in header, can't do linked schema change
        *sc_directly = true;
    }

    if (base_tablet->tablet_meta()->preferred_rowset_type() !=
        new_tablet->tablet_meta()->preferred_rowset_type()) {
        // If the base_tablet and new_tablet rowset types are different, just use directly type
        *sc_directly = true;
    }

    return OLAP_SUCCESS;
}

OLAPStatus SchemaChangeHandler::_init_column_mapping(ColumnMapping* column_mapping,
                                                     const TabletColumn& column_schema,
                                                     const std::string& value) {
    column_mapping->default_value = WrapperField::create(column_schema);

    if (column_mapping->default_value == nullptr) {
        return OLAP_ERR_MALLOC_ERROR;
    }

    if (column_schema.is_nullable() && value.length() == 0) {
        column_mapping->default_value->set_null();
    } else {
        column_mapping->default_value->from_string(value);
    }

    return OLAP_SUCCESS;
}

OLAPStatus SchemaChangeHandler::_validate_alter_result(TabletSharedPtr new_tablet,
                                                       const TAlterTabletReqV2& request) {
    Version max_continuous_version = {-1, 0};
    new_tablet->max_continuous_version_from_beginning(&max_continuous_version);
    LOG(INFO) << "find max continuous version of tablet=" << new_tablet->full_name()
              << ", start_version=" << max_continuous_version.first
              << ", end_version=" << max_continuous_version.second;
    if (max_continuous_version.second < request.alter_version) {
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
    {
        ReadLock rdlock(new_tablet->get_header_lock());
        new_tablet->acquire_version_and_rowsets(&version_rowsets);
    }
    for (auto& pair : version_rowsets) {
        RowsetSharedPtr rowset = pair.second;
        if (!rowset->check_file_exist()) {
            return OLAP_ERR_FILE_NOT_EXIST;
        }
    }
    return OLAP_SUCCESS;
}

} // namespace doris
