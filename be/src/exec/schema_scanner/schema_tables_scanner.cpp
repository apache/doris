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

#include "exec/schema_scanner/schema_tables_scanner.h"

#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_complex.h"
#include "vec/common/string_ref.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaTablesScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"ENGINE", TYPE_VARCHAR, sizeof(StringRef), true},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_FORMAT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"AUTO_INCREMENT", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"TABLE_COLLATION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_OPTIONS", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_COMMENT", TYPE_VARCHAR, sizeof(StringRef), false},
};

SchemaTablesScanner::SchemaTablesScanner()
        : SchemaScanner(_s_tbls_columns,
                        sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {
    _schema_table_type = TSchemaTableType::SCH_TABLES;
}

SchemaTablesScanner::~SchemaTablesScanner() {}

Status SchemaTablesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetDbsParams db_params;
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->catalog) {
        db_params.__set_catalog(*(_param->catalog));
    }
    if (nullptr != _param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            db_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            db_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaTablesScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    // catalog
    {
        if (!_db_result.__isset.catalogs) {
            tuple->set_null(_tuple_desc->slots()[0]->null_indicator_offset());
        } else {
            void* slot = tuple->get_slot(_tuple_desc->slots()[0]->tuple_offset());
            StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
            std::string catalog_name = _db_result.catalogs[_db_index - 1];
            str_slot->data = (char*)pool->allocate(catalog_name.size());
            str_slot->size = catalog_name.size();
            memcpy(const_cast<char*>(str_slot->data), catalog_name.c_str(), str_slot->size);
        }
    }
    // schema
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[1]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        str_slot->data = (char*)pool->allocate(db_name.size());
        str_slot->size = db_name.size();
        memcpy(const_cast<char*>(str_slot->data), db_name.c_str(), str_slot->size);
    }
    // name
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[2]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        const std::string* src = &tbl_status.name;
        str_slot->size = src->length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memory failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), src->c_str(), str_slot->size);
    }
    // type
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[3]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        const std::string* src = &tbl_status.type;
        str_slot->size = src->length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memory failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), src->c_str(), str_slot->size);
    }
    // engine
    if (tbl_status.__isset.engine) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[4]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        const std::string* src = &tbl_status.engine;
        str_slot->size = src->length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memory failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), src->c_str(), str_slot->size);
    } else {
        tuple->set_null(_tuple_desc->slots()[4]->null_indicator_offset());
    }
    // version
    { tuple->set_null(_tuple_desc->slots()[5]->null_indicator_offset()); }
    // row_format
    { tuple->set_null(_tuple_desc->slots()[6]->null_indicator_offset()); }
    // rows
    if (tbl_status.__isset.rows) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[7]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = tbl_status.rows;
    } else {
        tuple->set_null(_tuple_desc->slots()[7]->null_indicator_offset());
    }
    // avg_row_length
    if (tbl_status.__isset.avg_row_length) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[8]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = tbl_status.avg_row_length;
    } else {
        tuple->set_null(_tuple_desc->slots()[8]->null_indicator_offset());
    }
    // data_length
    if (tbl_status.__isset.avg_row_length) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[9]->tuple_offset());
        *(reinterpret_cast<int64_t*>(slot)) = tbl_status.data_length;
    } else {
        tuple->set_null(_tuple_desc->slots()[9]->null_indicator_offset());
    } // max_data_length
    { tuple->set_null(_tuple_desc->slots()[10]->null_indicator_offset()); }
    // index_length
    { tuple->set_null(_tuple_desc->slots()[11]->null_indicator_offset()); }
    // data_free
    { tuple->set_null(_tuple_desc->slots()[12]->null_indicator_offset()); }
    // auto_increment
    { tuple->set_null(_tuple_desc->slots()[13]->null_indicator_offset()); }
    // creation_time
    if (tbl_status.__isset.create_time) {
        int64_t create_time = tbl_status.create_time;
        if (create_time <= 0) {
            tuple->set_null(_tuple_desc->slots()[14]->null_indicator_offset());
        } else {
            tuple->set_not_null(_tuple_desc->slots()[14]->null_indicator_offset());
            void* slot = tuple->get_slot(_tuple_desc->slots()[14]->tuple_offset());
            DateTimeValue* time_slot = reinterpret_cast<DateTimeValue*>(slot);
            time_slot->from_unixtime(create_time, TimezoneUtils::default_time_zone);
        }
    }
    // update_time
    if (tbl_status.__isset.update_time) {
        int64_t update_time = tbl_status.update_time;
        if (update_time <= 0) {
            tuple->set_null(_tuple_desc->slots()[15]->null_indicator_offset());
        } else {
            tuple->set_not_null(_tuple_desc->slots()[15]->null_indicator_offset());
            void* slot = tuple->get_slot(_tuple_desc->slots()[15]->tuple_offset());
            DateTimeValue* time_slot = reinterpret_cast<DateTimeValue*>(slot);
            time_slot->from_unixtime(update_time, TimezoneUtils::default_time_zone);
        }
    }
    // check_time
    if (tbl_status.__isset.last_check_time) {
        int64_t check_time = tbl_status.last_check_time;
        if (check_time <= 0) {
            tuple->set_null(_tuple_desc->slots()[16]->null_indicator_offset());
        } else {
            tuple->set_not_null(_tuple_desc->slots()[16]->null_indicator_offset());
            void* slot = tuple->get_slot(_tuple_desc->slots()[16]->tuple_offset());
            DateTimeValue* time_slot = reinterpret_cast<DateTimeValue*>(slot);
            time_slot->from_unixtime(check_time, TimezoneUtils::default_time_zone);
        }
    }
    // collation
    if (tbl_status.__isset.collation) {
        void* slot = tuple->get_slot(_tuple_desc->slots()[17]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        const std::string* src = &tbl_status.collation;
        str_slot->size = src->length();
        str_slot->data = (char*)pool->allocate(str_slot->size);
        if (nullptr == str_slot->data) {
            return Status::InternalError("Allocate memory failed.");
        }
        memcpy(const_cast<char*>(str_slot->data), src->c_str(), str_slot->size);
    } else {
        tuple->set_null(_tuple_desc->slots()[17]->null_indicator_offset());
    }
    // checksum
    { tuple->set_null(_tuple_desc->slots()[18]->null_indicator_offset()); }
    // create_options
    { tuple->set_null(_tuple_desc->slots()[19]->null_indicator_offset()); }
    // create_comment
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[20]->tuple_offset());
        StringRef* str_slot = reinterpret_cast<StringRef*>(slot);
        const std::string* src = &tbl_status.comment;
        str_slot->size = src->length();
        if (str_slot->size == 0) {
            str_slot->data = nullptr;
        } else {
            str_slot->data = (char*)pool->allocate(str_slot->size);
            if (nullptr == str_slot->data) {
                return Status::InternalError("Allocate memory failed.");
            }
            memcpy(const_cast<char*>(str_slot->data), src->c_str(), str_slot->size);
        }
    }
    _table_index++;
    return Status::OK();
}

Status SchemaTablesScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index]);
    if (_db_result.__isset.catalogs) {
        table_params.__set_catalog(_db_result.catalogs[_db_index]);
    }
    _db_index++;
    if (nullptr != _param->wild) {
        table_params.__set_pattern(*(_param->wild));
    }
    if (nullptr != _param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            table_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            table_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->ip), _param->port, table_params,
                                                        &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaTablesScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    while (_table_index >= _table_result.tables.size()) {
        if (_db_index < _db_result.dbs.size()) {
            RETURN_IF_ERROR(get_new_table());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return fill_one_row(tuple, pool);
}

Status SchemaTablesScanner::fill_dest_column(vectorized::Block* block, void* data,
                                             const SlotDescriptor* slot_desc) {
    if (!block->has(slot_desc->col_name())) {
        return Status::OK();
    }
    vectorized::IColumn* col_ptr = const_cast<vectorized::IColumn*>(
            block->get_by_name(slot_desc->col_name()).column.get());

    if (data == nullptr) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(col_ptr);
        nullable_column->get_null_map_data().push_back(1);
        nullable_column->insert_data(nullptr, 0);
        return Status::OK();
    }
    if (slot_desc->is_nullable()) {
        auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(col_ptr);
        nullable_column->get_null_map_data().push_back(0);
        // if (data == nullptr) {
        //     nullable_column->insert_data(nullptr, 0);
        //     return Status::OK();
        // }
        col_ptr = &nullable_column->get_nested_column();
    }
    switch (slot_desc->type().type) {
    case TYPE_HLL: {
        HyperLogLog* hll_slot = reinterpret_cast<HyperLogLog*>(data);
        reinterpret_cast<vectorized::ColumnHLL*>(col_ptr)->get_data().emplace_back(*hll_slot);
        break;
    }
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_STRING: {
        StringRef* str_slot = reinterpret_cast<StringRef*>(data);
        reinterpret_cast<vectorized::ColumnString*>(col_ptr)->insert_data(str_slot->data,
                                                                          str_slot->size);
        break;
    }

    case TYPE_BOOLEAN: {
        uint8_t num = *reinterpret_cast<bool*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt8>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_TINYINT: {
        int8_t num = *reinterpret_cast<int8_t*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int8>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_SMALLINT: {
        int16_t num = *reinterpret_cast<int16_t*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int16>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_INT: {
        int32_t num = *reinterpret_cast<int32_t*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int32>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_BIGINT: {
        int64_t num = *reinterpret_cast<int64_t*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_LARGEINT: {
        __int128 num;
        memcpy(&num, data, sizeof(__int128));
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int128>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_FLOAT: {
        float num = *reinterpret_cast<float*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float32>*>(col_ptr)->insert_value(
                num);
        break;
    }

    case TYPE_DOUBLE: {
        double num = *reinterpret_cast<double*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Float64>*>(col_ptr)->insert_value(
                num);
        break;
    }

    case TYPE_DATE: {
        vectorized::VecDateTimeValue value;
        DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(data);
        value.convert_dt_to_vec_dt(ts_slot);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                reinterpret_cast<char*>(&value), 0);
        break;
    }

    case TYPE_DATEV2: {
        uint32_t num = *reinterpret_cast<uint32_t*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt32>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_DATETIME: {
        vectorized::VecDateTimeValue value;
        DateTimeValue* ts_slot = reinterpret_cast<DateTimeValue*>(data);
        value.convert_dt_to_vec_dt(ts_slot);
        reinterpret_cast<vectorized::ColumnVector<vectorized::Int64>*>(col_ptr)->insert_data(
                reinterpret_cast<char*>(&value), 0);
        break;
    }

    case TYPE_DATETIMEV2: {
        uint32_t num = *reinterpret_cast<uint64_t*>(data);
        reinterpret_cast<vectorized::ColumnVector<vectorized::UInt64>*>(col_ptr)->insert_value(num);
        break;
    }

    case TYPE_DECIMALV2: {
        const vectorized::Int128 num = (reinterpret_cast<PackedInt128*>(data))->value;
        reinterpret_cast<vectorized::ColumnDecimal128*>(col_ptr)->insert_data(
                reinterpret_cast<const char*>(&num), 0);
        break;
    }
    case TYPE_DECIMAL128I: {
        const vectorized::Int128 num = (reinterpret_cast<PackedInt128*>(data))->value;
        reinterpret_cast<vectorized::ColumnDecimal128I*>(col_ptr)->insert_data(
                reinterpret_cast<const char*>(&num), 0);
        break;
    }

    case TYPE_DECIMAL32: {
        const int32_t num = *reinterpret_cast<int32_t*>(data);
        reinterpret_cast<vectorized::ColumnDecimal32*>(col_ptr)->insert_data(
                reinterpret_cast<const char*>(&num), 0);
        break;
    }

    case TYPE_DECIMAL64: {
        const int64_t num = *reinterpret_cast<int64_t*>(data);
        reinterpret_cast<vectorized::ColumnDecimal64*>(col_ptr)->insert_data(
                reinterpret_cast<const char*>(&num), 0);
        break;
    }

    default: {
        DCHECK(false) << "bad slot type: " << slot_desc->type();
        std::stringstream ss;
        ss << "Fail to convert schema type:'" << slot_desc->type() << " on column:`"
           << slot_desc->col_name() + "`";
        return Status::InternalError(ss.str());
    }
    }

    return Status::OK();
}

Status SchemaTablesScanner::fill_one_row(vectorized::Block* block) {
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    // catalog
    {
        std::string catalog_name = _db_result.catalogs[_db_index - 1];
        StringRef str_slot = StringRef(catalog_name.c_str(), catalog_name.size());
        fill_dest_column(block, &str_slot, _tuple_desc->slots()[0]);
    }
    // schema
    {
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        StringRef str_slot = StringRef(db_name.c_str(), db_name.size());
        fill_dest_column(block, &str_slot, _tuple_desc->slots()[1]);
    }
    // name
    {
        const std::string* src = &tbl_status.name;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _tuple_desc->slots()[2]);
    }
    // type
    {
        const std::string* src = &tbl_status.type;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _tuple_desc->slots()[3]);
    }
    // engine
    if (tbl_status.__isset.engine) {
        const std::string* src = &tbl_status.engine;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _tuple_desc->slots()[4]);
    } else {
        fill_dest_column(block, nullptr, _tuple_desc->slots()[4]);
    }
    // version
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[5]); }
    // row_format
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[6]); }
    // rows
    if (tbl_status.__isset.rows) {
        int64_t src = tbl_status.rows;
        fill_dest_column(block, &src, _tuple_desc->slots()[7]);
    } else {
        fill_dest_column(block, nullptr, _tuple_desc->slots()[7]);
    }
    // avg_row_length
    if (tbl_status.__isset.avg_row_length) {
        int64_t src = tbl_status.avg_row_length;
        fill_dest_column(block, &src, _tuple_desc->slots()[8]);
    } else {
        fill_dest_column(block, nullptr, _tuple_desc->slots()[8]);
    }
    // data_length
    if (tbl_status.__isset.avg_row_length) {
        int64_t src = tbl_status.data_length;
        fill_dest_column(block, &src, _tuple_desc->slots()[9]);
    } else {
        fill_dest_column(block, nullptr, _tuple_desc->slots()[9]);
    } // max_data_length
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[10]); }
    // index_length
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[11]); }
    // data_free
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[12]); }
    // auto_increment
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[13]); }
    // creation_time
    if (tbl_status.__isset.create_time) {
        int64_t create_time = tbl_status.create_time;
        if (create_time <= 0) {
            fill_dest_column(block, nullptr, _tuple_desc->slots()[14]);
        } else {
            DateTimeValue time_slot;
            time_slot.from_unixtime(create_time, TimezoneUtils::default_time_zone);
            fill_dest_column(block, &time_slot, _tuple_desc->slots()[14]);
        }
    }
    // update_time
    if (tbl_status.__isset.update_time) {
        int64_t update_time = tbl_status.update_time;
        if (update_time <= 0) {
            fill_dest_column(block, nullptr, _tuple_desc->slots()[15]);
        } else {
            DateTimeValue time_slot;
            time_slot.from_unixtime(update_time, TimezoneUtils::default_time_zone);
            fill_dest_column(block, &time_slot, _tuple_desc->slots()[15]);
        }
    }
    // check_time
    if (tbl_status.__isset.last_check_time) {
        int64_t check_time = tbl_status.last_check_time;
        if (check_time <= 0) {
            fill_dest_column(block, nullptr, _tuple_desc->slots()[16]);
        } else {
            DateTimeValue time_slot;
            time_slot.from_unixtime(check_time, TimezoneUtils::default_time_zone);
            fill_dest_column(block, &time_slot, _tuple_desc->slots()[16]);
        }
    }
    // collation
    if (tbl_status.__isset.collation) {
        const std::string* src = &tbl_status.collation;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _tuple_desc->slots()[17]);

    } else {
        fill_dest_column(block, nullptr, _tuple_desc->slots()[17]);
    }
    // checksum
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[18]); }
    // create_options
    { fill_dest_column(block, nullptr, _tuple_desc->slots()[19]); }
    // create_comment
    {
        const std::string* src = &tbl_status.comment;
        StringRef str_slot = StringRef(src->c_str(), src->size());
        fill_dest_column(block, &str_slot, _tuple_desc->slots()[20]);
    }
    _table_index++;
    return Status::OK();
}

Status SchemaTablesScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    while (_table_index >= _table_result.tables.size()) {
        if (_db_index < _db_result.dbs.size()) {
            RETURN_IF_ERROR(get_new_table());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return fill_one_row(block);
}

} // namespace doris
