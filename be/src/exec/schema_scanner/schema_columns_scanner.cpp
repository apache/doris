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

#include <fmt/format.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Types_types.h>

#include <cstdint>

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"
#include "vec/common/string_ref.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

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
    if (nullptr != _param->common_param->db) {
        db_params.__set_pattern(*(_param->common_param->db));
    }
    if (nullptr != _param->common_param->catalog) {
        db_params.__set_catalog(*(_param->common_param->catalog));
    }
    if (nullptr != _param->common_param->current_user_ident) {
        db_params.__set_current_user_ident(*_param->common_param->current_user_ident);
    } else {
        if (nullptr != _param->common_param->user) {
            db_params.__set_user(*(_param->common_param->user));
        }
        if (nullptr != _param->common_param->user_ip) {
            db_params.__set_user_ip(*(_param->common_param->user_ip));
        }
    }

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(
                *(_param->common_param->ip), _param->common_param->port, db_params, &_db_result));
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
    case TPrimitiveType::DECIMAL256:
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
    case TPrimitiveType::MAP: {
        return "map";
    }
    case TPrimitiveType::ARRAY: {
        return "array";
    }
    case TPrimitiveType::STRUCT: {
        return "struct";
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
        return fmt::format(
                "decimal({}, {})",
                desc.__isset.columnPrecision ? std::to_string(desc.columnPrecision) : "27",
                desc.__isset.columnScale ? std::to_string(desc.columnScale) : "9");
    }
    case TPrimitiveType::DECIMAL32:
    case TPrimitiveType::DECIMAL64:
    case TPrimitiveType::DECIMAL128I:
    case TPrimitiveType::DECIMAL256: {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(
                debug_string_buffer, "decimalv3({}, {})",
                desc.__isset.columnPrecision ? std::to_string(desc.columnPrecision) : "UNKNOWN",
                desc.__isset.columnScale ? std::to_string(desc.columnScale) : "UNKNOWN");
        return fmt::to_string(debug_string_buffer);
    }
    case TPrimitiveType::DATEV2:
        return "date";
    case TPrimitiveType::DATETIMEV2: {
        fmt::memory_buffer debug_string_buffer;
        if (!desc.__isset.columnScale || desc.columnScale == 0) {
            fmt::format_to(debug_string_buffer, "datetime");
        } else {
            fmt::format_to(debug_string_buffer, "datetime({})",
                           desc.__isset.columnScale ? std::to_string(desc.columnScale) : "UNKNOWN");
        }
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
    case TPrimitiveType::MAP: {
        // for old be service we should compatible
        std::string ret = "map<";
        if (!desc.children.empty()) {
            for (int i = 0; i < desc.children.size() - 1; ++i) {
                ret += _type_to_string(desc.children[i]) + ",";
            }
            ret += _type_to_string(desc.children[desc.children.size() - 1]);
        }
        ret += ">";
        return ret;
    }
    case TPrimitiveType::ARRAY: {
        // for old be service we should compitable
        std::string ret = "array<";
        if (!desc.children.empty()) {
            ret += _type_to_string(desc.children[0]);
        }
        ret += ">";
        return ret;
    }
    case TPrimitiveType::STRUCT: {
        // for old be service we should compitable
        std::string ret = "struct<";
        if (!desc.children.empty()) {
            for (int i = 0; i < desc.children.size() - 1; ++i) {
                ret += _type_to_string(desc.children[i]) + ",";
            }
            ret += _type_to_string(desc.children[desc.children.size() - 1]);
        }
        ret += ">";
        return ret;
    }

    default:
        return "unknown";
    }
}

Status SchemaColumnsScanner::_get_new_desc() {
    SCOPED_TIMER(_get_describe_timer);
    TDescribeTablesParams desc_params;
    desc_params.__set_db(_db_result.dbs[_db_index - 1]);
    if (_db_result.__isset.catalogs) {
        desc_params.__set_catalog(_db_result.catalogs[_db_index - 1]);
    }
    for (int i = 0; i < 100; ++i) {
        if (_table_index >= _table_result.tables.size()) {
            break;
        }
        desc_params.tables_name.push_back(_table_result.tables[_table_index++]);
    }

    if (nullptr != _param->common_param->current_user_ident) {
        desc_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    } else {
        if (nullptr != _param->common_param->user) {
            desc_params.__set_user(*(_param->common_param->user));
        }
        if (nullptr != _param->common_param->user_ip) {
            desc_params.__set_user_ip(*(_param->common_param->user_ip));
        }
    }

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::describe_tables(*(_param->common_param->ip),
                                                      _param->common_param->port, desc_params,
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
    if (nullptr != _param->common_param->table) {
        table_params.__set_pattern(*(_param->common_param->table));
    }
    if (nullptr != _param->common_param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->common_param->current_user_ident));
    } else {
        if (nullptr != _param->common_param->user) {
            table_params.__set_user(*(_param->common_param->user));
        }
        if (nullptr != _param->common_param->user_ip) {
            table_params.__set_user_ip(*(_param->common_param->user_ip));
        }
    }

    if (nullptr != _param->common_param->ip && 0 != _param->common_param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_table_names(*(_param->common_param->ip),
                                                      _param->common_param->port, table_params,
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
    std::vector<void*> null_datas(columns_num, nullptr);
    std::vector<void*> datas(columns_num);
    // TABLE_CATALOG
    {
        if (!_db_result.__isset.catalogs) {
            RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, null_datas));
        } else {
            std::string catalog_name = _db_result.catalogs[_db_index - 1];
            StringRef str = StringRef(catalog_name.c_str(), catalog_name.size());
            for (int i = 0; i < columns_num; ++i) {
                datas[i] = &str;
            }
            RETURN_IF_ERROR(fill_dest_column_for_range(block, 0, datas));
        }
    }
    // TABLE_SCHEMA
    {
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        StringRef str = StringRef(db_name.c_str(), db_name.size());
        for (int i = 0; i < columns_num; ++i) {
            datas[i] = &str;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 1, datas));
    }
    // TABLE_NAME
    {
        std::vector<StringRef> strs(columns_num);
        int offset_index = 0;
        int cur_table_index = _table_index - _desc_result.tables_offset.size();

        for (int i = 0; i < columns_num; ++i) {
            while (_desc_result.tables_offset[offset_index] <= i) {
                ++offset_index;
                ++cur_table_index;
            }
            strs[i] = StringRef(_table_result.tables[cur_table_index].c_str(),
                                _table_result.tables[cur_table_index].length());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 2, datas));
    }
    // COLUMN_NAME
    {
        std::vector<StringRef> strs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            strs[i] = StringRef(_desc_result.columns[i].columnDesc.columnName.c_str(),
                                _desc_result.columns[i].columnDesc.columnName.length());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 3, datas));
    }
    // ORDINAL_POSITION
    {
        std::vector<int64_t> srcs(columns_num);
        int offset_index = 0;
        int columns_index = 1;
        for (int i = 0; i < columns_num; ++i) {
            while (_desc_result.tables_offset[offset_index] <= i) {
                ++offset_index;
                columns_index = 1;
            }
            srcs[i] = columns_index++;
            datas[i] = srcs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 4, datas));
    }
    // COLUMN_DEFAULT
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 5, null_datas)); }
    // IS_NULLABLE
    {
        StringRef str_yes = StringRef("YES", 3);
        StringRef str_no = StringRef("NO", 2);
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.isAllowNull) {
                if (_desc_result.columns[i].columnDesc.isAllowNull) {
                    datas[i] = &str_yes;
                } else {
                    datas[i] = &str_no;
                }
            } else {
                datas[i] = &str_no;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 6, datas));
    }
    // DATA_TYPE
    {
        std::vector<std::string> buffers(columns_num);
        std::vector<StringRef> strs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            buffers[i] = _to_mysql_data_type_string(_desc_result.columns[i].columnDesc);
            strs[i] = StringRef(buffers[i].c_str(), buffers[i].length());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 7, datas));
    }
    // CHARACTER_MAXIMUM_LENGTH
    // For string columns, the maximum length in characters.
    {
        std::vector<int64_t> srcs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR ||
                data_type == TPrimitiveType::STRING) {
                if (_desc_result.columns[i].columnDesc.__isset.columnLength) {
                    srcs[i] = _desc_result.columns[i].columnDesc.columnLength;
                    datas[i] = srcs.data() + i;
                } else {
                    datas[i] = nullptr;
                }
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 8, datas));
    }
    // CHARACTER_OCTET_LENGTH
    // For string columns, the maximum length in bytes.
    {
        std::vector<int64_t> srcs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR ||
                data_type == TPrimitiveType::STRING) {
                if (_desc_result.columns[i].columnDesc.__isset.columnLength) {
                    srcs[i] = _desc_result.columns[i].columnDesc.columnLength * 4L;
                    datas[i] = srcs.data() + i;
                } else {
                    datas[i] = nullptr;
                }
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 9, datas));
    }
    // NUMERIC_PRECISION
    {
        std::vector<int64_t> srcs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (_desc_result.columns[i].columnDesc.__isset.columnPrecision &&
                data_type != TPrimitiveType::DATETIMEV2) {
                srcs[i] = _desc_result.columns[i].columnDesc.columnPrecision;
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 10, datas));
    }
    // NUMERIC_SCALE
    {
        std::vector<int64_t> srcs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (_desc_result.columns[i].columnDesc.__isset.columnScale &&
                data_type != TPrimitiveType::DATETIMEV2) {
                srcs[i] = _desc_result.columns[i].columnDesc.columnScale;
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 11, datas));
    }
    // DATETIME_PRECISION
    {
        std::vector<int64_t> srcs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (_desc_result.columns[i].columnDesc.__isset.columnScale &&
                data_type == TPrimitiveType::DATETIMEV2) {
                srcs[i] = _desc_result.columns[i].columnDesc.columnScale;
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 12, datas));
    }
    // CHARACTER_SET_NAME
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 13, null_datas)); }
    // COLLATION_NAME
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 14, null_datas)); }
    // COLUMN_TYPE
    {
        std::vector<std::string> buffers(columns_num);
        std::vector<StringRef> strs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            buffers[i] = _type_to_string(_desc_result.columns[i].columnDesc);
            strs[i] = StringRef(buffers[i].c_str(), buffers[i].length());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 15, datas));
    }
    // COLUMN_KEY
    {
        StringRef str = StringRef("", 0);
        std::vector<StringRef> strs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.columnKey) {
                strs[i] = StringRef(_desc_result.columns[i].columnDesc.columnKey.c_str(),
                                    _desc_result.columns[i].columnDesc.columnKey.length());
                datas[i] = strs.data() + i;
            } else {
                datas[i] = &str;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 16, datas));
    }
    // EXTRA
    {
        StringRef str = StringRef("", 0);
        std::vector<void*> datas(columns_num, &str);
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 17, datas));
    }
    // PRIVILEGES
    {
        StringRef str = StringRef("", 0);
        std::vector<void*> datas(columns_num, &str);
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 18, datas));
    }
    // COLUMN_COMMENT
    {
        std::vector<StringRef> strs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            strs[i] = StringRef(_desc_result.columns[i].comment.c_str(),
                                _desc_result.columns[i].comment.length());
            datas[i] = strs.data() + i;
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 19, datas));
    }
    // COLUMN_SIZE
    {
        std::vector<int64_t> srcs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            if (_desc_result.columns[i].columnDesc.__isset.columnLength) {
                srcs[i] = _desc_result.columns[i].columnDesc.columnLength;
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 20, datas));
    }
    // DECIMAL_DIGITS
    {
        std::vector<int64_t> srcs(columns_num);
        for (int i = 0; i < columns_num; ++i) {
            int data_type = _desc_result.columns[i].columnDesc.columnType;
            if (_desc_result.columns[i].columnDesc.__isset.columnScale &&
                data_type != TPrimitiveType::DATETIMEV2) {
                srcs[i] = _desc_result.columns[i].columnDesc.columnScale;
                datas[i] = srcs.data() + i;
            } else {
                datas[i] = nullptr;
            }
        }
        RETURN_IF_ERROR(fill_dest_column_for_range(block, 21, datas));
    }
    // GENERATION_EXPRESSION
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 22, null_datas)); }
    // SRS_ID
    { RETURN_IF_ERROR(fill_dest_column_for_range(block, 23, null_datas)); }
    return Status::OK();
}

} // namespace doris
