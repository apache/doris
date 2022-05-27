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

#include "exec/schema_scanner.h"

#include "exec/schema_scanner/schema_charsets_scanner.h"
#include "exec/schema_scanner/schema_collations_scanner.h"
#include "exec/schema_scanner/schema_columns_scanner.h"
#include "exec/schema_scanner/schema_dummy_scanner.h"
#include "exec/schema_scanner/schema_files_scanner.h"
#include "exec/schema_scanner/schema_partitions_scanner.h"
#include "exec/schema_scanner/schema_schema_privileges_scanner.h"
#include "exec/schema_scanner/schema_schemata_scanner.h"
#include "exec/schema_scanner/schema_statistics_scanner.h"
#include "exec/schema_scanner/schema_table_privileges_scanner.h"
#include "exec/schema_scanner/schema_tables_scanner.h"
#include "exec/schema_scanner/schema_user_privileges_scanner.h"
#include "exec/schema_scanner/schema_variables_scanner.h"
#include "exec/schema_scanner/schema_views_scanner.h"

namespace doris {

DorisServer* SchemaScanner::_s_doris_server;

SchemaScanner::SchemaScanner(ColumnDesc* columns, int column_num)
        : _is_init(false),
          _param(nullptr),
          _columns(columns),
          _column_num(column_num),
          _tuple_desc(nullptr) {}

SchemaScanner::~SchemaScanner() {}

Status SchemaScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("call Start before Init.");
    }

    return Status::OK();
}

Status SchemaScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    if (nullptr == tuple || nullptr == pool || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    return Status::OK();
}

Status SchemaScanner::init(SchemaScannerParam* param, ObjectPool* pool) {
    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == param || nullptr == pool || nullptr == _columns) {
        return Status::InternalError("invalid parameter");
    }

    RETURN_IF_ERROR(create_tuple_desc(pool));
    _param = param;
    _is_init = true;

    return Status::OK();
}

SchemaScanner* SchemaScanner::create(TSchemaTableType::type type) {
    switch (type) {
    case TSchemaTableType::SCH_TABLES:
        return new (std::nothrow) SchemaTablesScanner();
    case TSchemaTableType::SCH_SCHEMATA:
        return new (std::nothrow) SchemaSchemataScanner();
    case TSchemaTableType::SCH_COLUMNS:
        return new (std::nothrow) SchemaColumnsScanner();
    case TSchemaTableType::SCH_CHARSETS:
        return new (std::nothrow) SchemaCharsetsScanner();
    case TSchemaTableType::SCH_COLLATIONS:
        return new (std::nothrow) SchemaCollationsScanner();
    case TSchemaTableType::SCH_GLOBAL_VARIABLES:
        return new (std::nothrow) SchemaVariablesScanner(TVarType::GLOBAL);
    case TSchemaTableType::SCH_SESSION_VARIABLES:
    case TSchemaTableType::SCH_VARIABLES:
        return new (std::nothrow) SchemaVariablesScanner(TVarType::SESSION);
    case TSchemaTableType::SCH_VIEWS:
        return new (std::nothrow) SchemaViewsScanner();
    case TSchemaTableType::SCH_TABLE_PRIVILEGES:
        return new (std::nothrow) SchemaTablePrivilegesScanner();
    case TSchemaTableType::SCH_SCHEMA_PRIVILEGES:
        return new (std::nothrow) SchemaSchemaPrivilegesScanner();
    case TSchemaTableType::SCH_USER_PRIVILEGES:
        return new (std::nothrow) SchemaUserPrivilegesScanner();
    case TSchemaTableType::SCH_FILES:
        return new (std::nothrow) SchemaFilesScanner();
    case TSchemaTableType::SCH_PARTITIONS:
        return new (std::nothrow) SchemaPartitionsScanner();
    default:
        return new (std::nothrow) SchemaDummyScanner();
        break;
    }
}

Status SchemaScanner::create_tuple_desc(ObjectPool* pool) {
    int null_column = 0;

    for (int i = 0; i < _column_num; ++i) {
        if (_columns[i].is_null) {
            null_column++;
        }
    }

    int offset = (null_column + 7) / 8;
    std::vector<SlotDescriptor*> slots;
    int null_byte = 0;
    int null_bit = 0;

    for (int i = 0; i < _column_num; ++i) {
        TSlotDescriptor t_slot_desc;
        if (_columns[i].type == TYPE_DECIMALV2) {
            t_slot_desc.__set_slotType(TypeDescriptor::create_decimalv2_type(27, 9).to_thrift());
        } else {
            t_slot_desc.__set_slotType(TypeDescriptor(_columns[i].type).to_thrift());
        }
        t_slot_desc.__set_colName(_columns[i].name);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);

        if (_columns[i].is_null) {
            t_slot_desc.__set_nullIndicatorByte(null_byte);
            t_slot_desc.__set_nullIndicatorBit(null_bit);
            null_bit = (null_bit + 1) % 8;

            if (0 == null_bit) {
                null_byte++;
            }
        } else {
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
        }

        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);

        SlotDescriptor* slot = pool->add(new (std::nothrow) SlotDescriptor(t_slot_desc));

        if (nullptr == slot) {
            return Status::InternalError("no memory for _tuple_desc.");
        }

        slots.push_back(slot);
        offset += _columns[i].size;
    }

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.__set_byteSize(offset);
    t_tuple_desc.__set_numNullBytes((null_byte * 8 + null_bit + 7) / 8);
    _tuple_desc = pool->add(new (std::nothrow) TupleDescriptor(t_tuple_desc));

    if (nullptr == _tuple_desc) {
        return Status::InternalError("no memory for _tuple_desc.");
    }

    for (int i = 0; i < slots.size(); ++i) {
        _tuple_desc->add_slot(slots[i]);
    }

    return Status::OK();
}

} // namespace doris
