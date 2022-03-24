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

#include "exec/schema_scanner/schema_columns_scanner.h"

#include <sstream>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/datetime_value.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace doris {

SchemaScanner::ColumnDesc SchemaColumnsScanner::_s_col_columns[] = {
        //   name,       type,          size,                     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ORDINAL_POSITION", TYPE_BIGINT, sizeof(int64_t), false},
        {"COLUMN_DEFAULT", TYPE_VARCHAR, sizeof(StringValue), true},
        {"IS_NULLABLE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATA_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_MAXIMUM_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"CHARACTER_OCTET_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUMERIC_PRECISION", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUMERIC_SCALE", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATETIME_PRECISION", TYPE_BIGINT, sizeof(int64_t), true},
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"COLLATION_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"COLUMN_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_KEY", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EXTRA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIVILEGES", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_COMMENT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"DECIMAL_DIGITS", TYPE_BIGINT, sizeof(int64_t), true},
        {"GENERATION_EXPRESSION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SRS_ID", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaColumnsScanner::SchemaColumnsScanner()
        : SchemaScanner(_s_col_columns, sizeof(_s_col_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0),
          _column_index(0) {}

SchemaColumnsScanner::~SchemaColumnsScanner() {}

Status SchemaColumnsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("schema columns scanner not inited.");
    }
    // get all database
    TGetDbsParams db_params;
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        db_params.__set_current_user_ident(*_param->current_user_ident);
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

//For compatibility with mysql the result of DATA_TYPE in information_schema.columns
std::string SchemaColumnsScanner::to_mysql_data_type_string(TColumnDesc& desc) {
    switch (desc.columnType) {
    case TPrimitiveType::BOOLEAN:
        return "tinyint";
    case TPrimitiveType::TINYINT:
        return "tinyint";
    case TPrimitiveType::SMALLINT:
        return "smallint";
    case TPrimitiveType::INT:
        return "int";
    case TPrimitiveType::BIGINT:
        return "bigint";
    case TPrimitiveType::LARGEINT:
        return "bigint unsigned";
    case TPrimitiveType::FLOAT:
        return "float";
    case TPrimitiveType::DOUBLE:
        return "double";
    case TPrimitiveType::VARCHAR:
    case TPrimitiveType::STRING:
        return "varchar";
    case TPrimitiveType::CHAR:
        return "char";
    case TPrimitiveType::DATE:
        return "date";
    case TPrimitiveType::DATETIME:
        return "datetime";
    case TPrimitiveType::DECIMALV2: {
        return "decimal";
    }
    default:
        return "unknown";
    }
}

std::string SchemaColumnsScanner::type_to_string(TColumnDesc& desc) {
    switch (desc.columnType) {
    case TPrimitiveType::BOOLEAN:
        return "tinyint(1)";
    case TPrimitiveType::TINYINT:
        return "tinyint(4)";
    case TPrimitiveType::SMALLINT:
        return "smallint(6)";
    case TPrimitiveType::INT:
        return "int(11)";
    case TPrimitiveType::BIGINT:
        return "bigint(20)";
    case TPrimitiveType::LARGEINT:
        return "bigint(20) unsigned";
    case TPrimitiveType::FLOAT:
        return "float";
    case TPrimitiveType::DOUBLE:
        return "double";
    case TPrimitiveType::VARCHAR:
        if (desc.__isset.columnLength) {
            return "varchar(" + std::to_string(desc.columnLength) + ")";
        } else {
            return "varchar(20)";
        }
    case TPrimitiveType::STRING:
        return "string";
    case TPrimitiveType::CHAR:
        if (desc.__isset.columnLength) {
            return "char(" + std::to_string(desc.columnLength) + ")";
        } else {
            return "char(20)";
        }
    case TPrimitiveType::DATE:
        return "date";
    case TPrimitiveType::DATETIME:
        return "datetime";
    case TPrimitiveType::DECIMALV2: {
        std::stringstream stream;
        stream << "decimal(";
        if (desc.__isset.columnPrecision) {
            stream << desc.columnPrecision;
        } else {
            stream << 27;
        }
        stream << ",";
        if (desc.__isset.columnScale) {
            stream << desc.columnScale;
        } else {
            stream << 9;
        }
        stream << ")";
        return stream.str();
    }
    default:
        return "unknown";
    }
}

//fill row in the "INFORMATION_SCHEMA COLUMNS"
//Reference from https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html
Status SchemaColumnsScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());

    // TABLE_CATALOG
    { tuple->set_null(_tuple_desc->slots()[0]->null_indicator_offset()); }
    // TABLE_SCHEMA
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[1]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        str_slot->ptr = (char*)pool->allocate(db_name.size());
        str_slot->len = db_name.size();
        memcpy(str_slot->ptr, db_name.c_str(), str_slot->len);
    }
    // TABLE_NAME
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[2]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = (char*)pool->allocate(_table_result.tables[_table_index - 1].length());
        str_slot->len = _table_result.tables[_table_index - 1].length();
        memcpy(str_slot->ptr, _table_result.tables[_table_index - 1].c_str(), str_slot->len);
    }
    // COLUMN_NAME
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[3]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = (char*)pool->allocate(
                _desc_result.columns[_column_index].columnDesc.columnName.length());
        str_slot->len = _desc_result.columns[_column_index].columnDesc.columnName.length();
        memcpy(str_slot->ptr, _desc_result.columns[_column_index].columnDesc.columnName.c_str(),
               str_slot->len);
    }
    // ORDINAL_POSITION
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[4]->tuple_offset());
        int64_t* bigint_slot = reinterpret_cast<int64_t*>(slot);
        *bigint_slot = _column_index + 1;
    }
    // COLUMN_DEFAULT
    { tuple->set_null(_tuple_desc->slots()[5]->null_indicator_offset()); }
    // IS_NULLABLE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[6]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);

        if (_desc_result.columns[_column_index].columnDesc.__isset.isAllowNull) {
            if (_desc_result.columns[_column_index].columnDesc.isAllowNull) {
                str_slot->len = strlen("YES");
                str_slot->ptr = (char*)pool->allocate(str_slot->len);
                memcpy(str_slot->ptr, "YES", str_slot->len);
            } else {
                str_slot->len = strlen("NO");
                str_slot->ptr = (char*)pool->allocate(str_slot->len);
                memcpy(str_slot->ptr, "NO", str_slot->len);
            }
        } else {
            str_slot->len = strlen("NO");
            str_slot->ptr = (char*)pool->allocate(str_slot->len);
            memcpy(str_slot->ptr, "NO", str_slot->len);
        }
    }
    // DATA_TYPE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[7]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        std::string buffer =
                to_mysql_data_type_string(_desc_result.columns[_column_index].columnDesc);
        str_slot->len = buffer.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        memcpy(str_slot->ptr, buffer.c_str(), str_slot->len);
    }
    // CHARACTER_MAXIMUM_LENGTH
    // For string columns, the maximum length in characters.
    {
        int data_type = _desc_result.columns[_column_index].columnDesc.columnType;
        if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR ||
            data_type == TPrimitiveType::STRING) {
            void* slot = tuple->get_slot(_tuple_desc->slots()[8]->tuple_offset());
            int64_t* str_slot = reinterpret_cast<int64_t*>(slot);
            if (_desc_result.columns[_column_index].columnDesc.__isset.columnLength) {
                *str_slot = _desc_result.columns[_column_index].columnDesc.columnLength;
            } else {
                tuple->set_null(_tuple_desc->slots()[8]->null_indicator_offset());
            }
        } else {
            tuple->set_null(_tuple_desc->slots()[8]->null_indicator_offset());
        }
    }
    // CHARACTER_OCTET_LENGTH
    // For string columns, the maximum length in bytes.
    {
        int data_type = _desc_result.columns[_column_index].columnDesc.columnType;
        if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR ||
            data_type == TPrimitiveType::STRING) {
            void* slot = tuple->get_slot(_tuple_desc->slots()[9]->tuple_offset());
            int64_t* str_slot = reinterpret_cast<int64_t*>(slot);
            if (_desc_result.columns[_column_index].columnDesc.__isset.columnLength) {
                *str_slot = _desc_result.columns[_column_index].columnDesc.columnLength * 4;
            } else {
                tuple->set_null(_tuple_desc->slots()[9]->null_indicator_offset());
            }
        } else {
            tuple->set_null(_tuple_desc->slots()[9]->null_indicator_offset());
        }
    }
    // NUMERIC_PRECISION
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[10]->tuple_offset());
        int64_t* str_slot = reinterpret_cast<int64_t*>(slot);
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnPrecision) {
            *str_slot = _desc_result.columns[_column_index].columnDesc.columnPrecision;
        } else {
            tuple->set_null(_tuple_desc->slots()[10]->null_indicator_offset());
        }
    }
    // NUMERIC_SCALE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[11]->tuple_offset());
        int64_t* str_slot = reinterpret_cast<int64_t*>(slot);
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnScale) {
            *str_slot = _desc_result.columns[_column_index].columnDesc.columnScale;
        } else {
            tuple->set_null(_tuple_desc->slots()[11]->null_indicator_offset());
        }
    }
    // DATETIME_PRECISION
    { tuple->set_null(_tuple_desc->slots()[12]->null_indicator_offset()); }
    // CHARACTER_SET_NAME
    { tuple->set_null(_tuple_desc->slots()[13]->null_indicator_offset()); }
    // COLLATION_NAME
    { tuple->set_null(_tuple_desc->slots()[14]->null_indicator_offset()); }
    // COLUMN_TYPE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[15]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        std::string buffer = type_to_string(_desc_result.columns[_column_index].columnDesc);
        str_slot->len = buffer.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        memcpy(str_slot->ptr, buffer.c_str(), str_slot->len);
    }
    // COLUMN_KEY
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[16]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->len = strlen("") + 1;
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        memcpy(str_slot->ptr, "", str_slot->len);
    }
    // EXTRA
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[17]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->len = strlen("") + 1;
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        memcpy(str_slot->ptr, "", str_slot->len);
    }
    // PRIVILEGES
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[18]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->len = strlen("") + 1;
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        memcpy(str_slot->ptr, "", str_slot->len);
    }
    // COLUMN_COMMENT
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[19]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        str_slot->ptr = (char*)pool->allocate(_desc_result.columns[_column_index].comment.length());
        str_slot->len = _desc_result.columns[_column_index].comment.length();
        memcpy(str_slot->ptr, _desc_result.columns[_column_index].comment.c_str(), str_slot->len);
    }
    // COLUMN_SIZE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[20]->tuple_offset());
        int64_t* str_slot = reinterpret_cast<int64_t*>(slot);
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnLength) {
            *str_slot = _desc_result.columns[_column_index].columnDesc.columnLength;
        } else {
            tuple->set_null(_tuple_desc->slots()[20]->null_indicator_offset());
        }
    }
    // DECIMAL_DIGITS
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[21]->tuple_offset());
        int64_t* str_slot = reinterpret_cast<int64_t*>(slot);
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnScale) {
            *str_slot = _desc_result.columns[_column_index].columnDesc.columnScale;
        } else {
            tuple->set_null(_tuple_desc->slots()[21]->null_indicator_offset());
        }
    }
    // GENERATION_EXPRESSION
    { tuple->set_null(_tuple_desc->slots()[22]->null_indicator_offset()); }
    // SRS_ID
    { tuple->set_null(_tuple_desc->slots()[23]->null_indicator_offset()); }
    _column_index++;
    return Status::OK();
}

Status SchemaColumnsScanner::get_new_desc() {
    TDescribeTableParams desc_params;
    desc_params.__set_db(_db_result.dbs[_db_index - 1]);
    desc_params.__set_table_name(_table_result.tables[_table_index++]);
    if (nullptr != _param->current_user_ident) {
        desc_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            desc_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            desc_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::describe_table(*(_param->ip), _param->port, desc_params,
                                                     &_desc_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _column_index = 0;

    return Status::OK();
}

Status SchemaColumnsScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
    if (nullptr != _param->table) {
        table_params.__set_pattern(*(_param->table));
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
        RETURN_IF_ERROR(SchemaHelper::get_table_names(*(_param->ip), _param->port, table_params,
                                                      &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaColumnsScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("use this class before inited.");
    }
    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input parameter is nullptr.");
    }
    while (_column_index >= _desc_result.columns.size()) {
        if (_table_index >= _table_result.tables.size()) {
            if (_db_index < _db_result.dbs.size()) {
                RETURN_IF_ERROR(get_new_table());
            } else {
                *eos = true;
                return Status::OK();
            }
        } else {
            RETURN_IF_ERROR(get_new_desc());
        }
    }

    *eos = false;
    return fill_one_row(tuple, pool);
}

} // namespace doris
