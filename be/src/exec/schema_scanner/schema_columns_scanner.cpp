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

#include <cstdint>
#include <memory>
#include <sstream>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "vec/common/string_ref.h"

namespace doris {

std::vector<SchemaScanner::ColumnDesc> SchemaColumnsScanner::_s_col_columns = {
        //   name,       type,          size,                     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringRef), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringRef), false},
        {"ORDINAL_POSITION", TYPE_BIGINT, sizeof(int64_t), false},
        {"COLUMN_DEFAULT", TYPE_VARCHAR, sizeof(StringRef), true},
        {"IS_NULLABLE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"DATA_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"CHARACTER_MAXIMUM_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"CHARACTER_OCTET_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUMERIC_PRECISION", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUMERIC_SCALE", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATETIME_PRECISION", TYPE_BIGINT, sizeof(int64_t), true},
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLLATION_NAME", TYPE_VARCHAR, sizeof(StringRef), true},
        {"COLUMN_TYPE", TYPE_VARCHAR, sizeof(StringRef), false},
        {"COLUMN_KEY", TYPE_VARCHAR, sizeof(StringRef), false},
        {"EXTRA", TYPE_VARCHAR, sizeof(StringRef), false},
        {"PRIVILEGES", TYPE_VARCHAR, sizeof(StringRef), false},
        {"COLUMN_COMMENT", TYPE_VARCHAR, sizeof(StringRef), false},
        {"COLUMN_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"DECIMAL_DIGITS", TYPE_BIGINT, sizeof(int64_t), true},
        {"GENERATION_EXPRESSION", TYPE_VARCHAR, sizeof(StringRef), true},
        {"SRS_ID", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaColumnsScanner::SchemaColumnsScanner()
        : SchemaScanner(_s_col_columns, TSchemaTableType::SCH_COLUMNS),
          _db_index(0),
          _table_index(0) {}

SchemaColumnsScanner::~SchemaColumnsScanner() = default;

Status SchemaColumnsScanner::start(RuntimeState* state) {
    SCOPED_TIMER(_get_db_timer);
    if (!_is_init) {
        return Status::InternalError("schema columns scanner not inited.");
    }
    // get all database
    TGetDbsParams db_params;
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->catalog) {
        db_params.__set_catalog(*(_param->catalog));
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
std::string SchemaColumnsScanner::_to_mysql_data_type_string(TColumnDesc& desc) {
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
    case TPrimitiveType::DATEV2:
        return "date";
    case TPrimitiveType::DATETIME:
    case TPrimitiveType::DATETIMEV2:
        return "datetime";
    case TPrimitiveType::DECIMAL32:
    case TPrimitiveType::DECIMAL64:
    case TPrimitiveType::DECIMAL128I:
    case TPrimitiveType::DECIMALV2: {
        return "decimal";
    }
    case TPrimitiveType::HLL: {
        return "hll";
    }
    case TPrimitiveType::OBJECT: {
        return "bitmap";
    }
    case TPrimitiveType::JSONB: {
        return "json";
    }
    default:
        return "unknown";
    }
}

std::string SchemaColumnsScanner::_type_to_string(TColumnDesc& desc) {
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
        return "largeint";
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
    case TPrimitiveType::DECIMAL32:
    case TPrimitiveType::DECIMAL64:
    case TPrimitiveType::DECIMAL128I: {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(
                debug_string_buffer, "decimalv3({}, {})",
                desc.__isset.columnPrecision ? std::to_string(desc.columnPrecision) : "UNKNOWN",
                desc.__isset.columnScale ? std::to_string(desc.columnScale) : "UNKNOWN");
        return fmt::to_string(debug_string_buffer);
    }
    case TPrimitiveType::DATEV2:
        return "datev2";
    case TPrimitiveType::DATETIMEV2: {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "datetimev2({})",
                       desc.__isset.columnScale ? std::to_string(desc.columnScale) : "UNKNOWN");
        return fmt::to_string(debug_string_buffer);
    }
    case TPrimitiveType::HLL: {
        return "hll";
    }
    case TPrimitiveType::OBJECT: {
        return "bitmap";
    }
    case TPrimitiveType::JSONB: {
        return "json";
    }
    default:
        return "unknown";
    }
}

Status SchemaColumnsScanner::_get_new_desc() {
    SCOPED_TIMER(_get_describe_timer);
    TDescribeTableParams desc_params;
    desc_params.__set_db(_db_result.dbs[_db_index - 1]);
    if (_db_result.__isset.catalogs) {
        desc_params.__set_catalog(_db_result.catalogs[_db_index - 1]);
    }
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

    return Status::OK();
}

Status SchemaColumnsScanner::_get_new_table() {
    SCOPED_TIMER(_get_table_timer);
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index]);
    if (_db_result.__isset.catalogs) {
        table_params.__set_catalog(_db_result.catalogs[_db_index]);
    }
    _db_index++;
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

Status SchemaColumnsScanner::get_next_block(vectorized::Block* block, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("use this class before inited.");
    }
    if (nullptr == block || nullptr == eos) {
        return Status::InternalError("input parameter is nullptr.");
    }

    while (_table_index >= _table_result.tables.size()) {
        if (_db_index < _db_result.dbs.size()) {
            RETURN_IF_ERROR(_get_new_table());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(_get_new_desc());

    *eos = false;
    return _fill_block_impl(block);
}

Status SchemaColumnsScanner::_fill_block_impl(vectorized::Block* block) {
    SCOPED_TIMER(_fill_block_timer);
    auto columns_num = _desc_result.columns.size();

    // TABLE_CATALOG
    {
        if (!_db_result.__isset.catalogs) {
            for (int i = 0; i < columns_num; ++i) {
                fill_dest_column(block, nullptr, _s_col_columns[0]);
            }
        } else {
            std::string catalog_name = _db_result.catalogs[_db_index - 1];
            StringRef str = StringRef(catalog_name.c_str(), catalog_name.size());
            for (int i = 0; i < columns_num; ++i) {
                fill_dest_column(block, &str, _s_col_columns[0]);
            }
        }
    }
    // TABLE_SCHEMA
    {
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        StringRef str = StringRef(db_name.c_str(), db_name.size());
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, &str, _s_col_columns[1]);
        }
    }
    // TABLE_NAME
    {
        StringRef str = StringRef(_table_result.tables[_table_index - 1].c_str(),
                                  _table_result.tables[_table_index - 1].length());
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, &str, _s_col_columns[2]);
        }
    }
    // COLUMN_NAME
    {
        for (int i = 0; i < columns_num; ++i) {
            StringRef str = StringRef(_desc_result.columns[i].columnDesc.columnName.c_str(),
                                      _desc_result.columns[i].columnDesc.columnName.length());
            fill_dest_column(block, &str, _s_col_columns[3]);
        }
    }
    // ORDINAL_POSITION
    {
        for (int i = 0; i < columns_num; ++i) {
            int64_t src = i + 1;
            fill_dest_column(block, &src, _s_col_columns[4]);
        }
    }
    // COLUMN_DEFAULT
    {
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, nullptr, _s_col_columns[5]);
        }
    }
    // IS_NULLABLE
    {
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.isAllowNull) {
                if (_desc_result.columns[i].columnDesc.isAllowNull) {
                    StringRef str = StringRef("YES", 3);
                    fill_dest_column(block, &str, _s_col_columns[6]);
                } else {
                    StringRef str = StringRef("NO", 2);
                    fill_dest_column(block, &str, _s_col_columns[6]);
                }
            } else {
                StringRef str = StringRef("NO", 2);
                fill_dest_column(block, &str, _s_col_columns[6]);
            }
        }
    }
    // DATA_TYPE
    {
        for (int i = 0; i < columns_num; ++i) {
            std::string buffer = _to_mysql_data_type_string(_desc_result.columns[i].columnDesc);
            StringRef str = StringRef(buffer.c_str(), buffer.length());
            fill_dest_column(block, &str, _s_col_columns[7]);
        }
    }
    // CHARACTER_MAXIMUM_LENGTH
    // For string columns, the maximum length in characters.
    {
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR ||
                data_type == TPrimitiveType::STRING) {
                if (_desc_result.columns[i].columnDesc.__isset.columnLength) {
                    int64_t src = _desc_result.columns[i].columnDesc.columnLength;
                    fill_dest_column(block, &src, _s_col_columns[8]);
                } else {
                    fill_dest_column(block, nullptr, _s_col_columns[8]);
                }
            } else {
                fill_dest_column(block, nullptr, _s_col_columns[8]);
            }
        }
    }
    // CHARACTER_OCTET_LENGTH
    // For string columns, the maximum length in bytes.
    {
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR ||
                data_type == TPrimitiveType::STRING) {
                if (_desc_result.columns[i].columnDesc.__isset.columnLength) {
                    int64_t src = _desc_result.columns[i].columnDesc.columnLength * 4;
                    fill_dest_column(block, &src, _s_col_columns[9]);
                } else {
                    fill_dest_column(block, nullptr, _s_col_columns[9]);
                }
            } else {
                fill_dest_column(block, nullptr, _s_col_columns[9]);
            }
        }
    }
    // NUMERIC_PRECISION
    {
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.columnPrecision) {
                int64_t src = _desc_result.columns[i].columnDesc.columnPrecision;
                fill_dest_column(block, &src, _s_col_columns[10]);
            } else {
                fill_dest_column(block, nullptr, _s_col_columns[10]);
            }
        }
    }
    // NUMERIC_SCALE
    {
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.columnScale) {
                int64_t src = _desc_result.columns[i].columnDesc.columnScale;
                fill_dest_column(block, &src, _s_col_columns[11]);
            } else {
                fill_dest_column(block, nullptr, _s_col_columns[11]);
            }
        }
    }
    // DATETIME_PRECISION
    {
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, nullptr, _s_col_columns[12]);
        }
    }
    // CHARACTER_SET_NAME
    {
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, nullptr, _s_col_columns[13]);
        }
    }
    // COLLATION_NAME
    {
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, nullptr, _s_col_columns[14]);
        }
    }
    // COLUMN_TYPE
    {
        for (int i = 0; i < columns_num; ++i) {
            std::string buffer = _type_to_string(_desc_result.columns[i].columnDesc);
            StringRef str = StringRef(buffer.c_str(), buffer.length());
            fill_dest_column(block, &str, _s_col_columns[15]);
        }
    }
    // COLUMN_KEY
    {
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.columnKey) {
                StringRef str = StringRef(_desc_result.columns[i].columnDesc.columnKey.c_str(),
                                          _desc_result.columns[i].columnDesc.columnKey.length());
                fill_dest_column(block, &str, _s_col_columns[16]);
            } else {
                StringRef str = StringRef("", 0);
                fill_dest_column(block, &str, _s_col_columns[16]);
            }
        }
    }
    // EXTRA
    {
        for (int i = 0; i < columns_num; ++i) {
            StringRef str = StringRef("", 0);
            fill_dest_column(block, &str, _s_col_columns[17]);
        }
    }
    // PRIVILEGES
    {
        for (int i = 0; i < columns_num; ++i) {
            StringRef str = StringRef("", 0);
            fill_dest_column(block, &str, _s_col_columns[18]);
        }
    }
    // COLUMN_COMMENT
    {
        for (int i = 0; i < columns_num; ++i) {
            StringRef str = StringRef(_desc_result.columns[i].comment.c_str(),
                                      _desc_result.columns[i].comment.length());
            fill_dest_column(block, &str, _s_col_columns[19]);
        }
    }
    // COLUMN_SIZE
    {
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.columnLength) {
                int64_t src = _desc_result.columns[i].columnDesc.columnLength;
                fill_dest_column(block, &src, _s_col_columns[20]);
            } else {
                fill_dest_column(block, nullptr, _s_col_columns[20]);
            }
        }
    }
    // DECIMAL_DIGITS
    {
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.columnScale) {
                int64_t src = _desc_result.columns[i].columnDesc.columnScale;
                fill_dest_column(block, &src, _s_col_columns[21]);
            } else {
                fill_dest_column(block, nullptr, _s_col_columns[21]);
            }
        }
    }
    // GENERATION_EXPRESSION
    {
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, nullptr, _s_col_columns[22]);
        }
    }
    // SRS_ID
    {
        for (int i = 0; i < columns_num; ++i) {
            fill_dest_column(block, nullptr, _s_col_columns[23]);
        }
    }
    return Status::OK();
}

} // namespace doris
