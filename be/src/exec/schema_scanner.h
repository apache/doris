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

#include <gen_cpp/Descriptors_types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "util/runtime_profile.h"

namespace doris {

// forehead declare class, because jni function init in DorisServer.
class DorisServer;
class RuntimeState;
class ObjectPool;
class TUserIdentity;

namespace vectorized {
class Block;
}

struct SchemaScannerCommonParam {
    SchemaScannerCommonParam()
            : db(nullptr),
              table(nullptr),
              wild(nullptr),
              user(nullptr),
              user_ip(nullptr),
              current_user_ident(nullptr),
              ip(nullptr),
              port(0),
              catalog(nullptr) {}
    const std::string* db;
    const std::string* table;
    const std::string* wild;
    const std::string* user;                 // deprecated
    const std::string* user_ip;              // deprecated
    const TUserIdentity* current_user_ident; // to replace the user and user ip
    const std::string* ip;                   // frontend ip
    int32_t port;                            // frontend thrift port
    int64_t thread_id;
    const std::string* catalog;
};

// scanner parameter from frontend
struct SchemaScannerParam {
    std::shared_ptr<SchemaScannerCommonParam> common_param;
    std::unique_ptr<RuntimeProfile> profile;

    SchemaScannerParam() : common_param(new SchemaScannerCommonParam()) {}
};

// virtual scanner for all schema table
class SchemaScanner {
    ENABLE_FACTORY_CREATOR(SchemaScanner);

public:
    struct ColumnDesc {
        const char* name;
        PrimitiveType type;
        int size;
        bool is_null;
        /// Only set if type == TYPE_DECIMAL or DATETIMEV2
        int precision = -1;
        int scale = -1;
    };
    SchemaScanner(const std::vector<ColumnDesc>& columns);
    SchemaScanner(const std::vector<ColumnDesc>& columns, TSchemaTableType::type type);
    virtual ~SchemaScanner();

    // init object need information, schema etc.
    virtual Status init(SchemaScannerParam* param, ObjectPool* pool);
    // Start to work
    virtual Status start(RuntimeState* state);
    virtual Status get_next_block(vectorized::Block* block, bool* eos);
    const std::vector<ColumnDesc>& get_column_desc() const { return _columns; }
    // factory function
    static std::unique_ptr<SchemaScanner> create(TSchemaTableType::type type);
    TSchemaTableType::type type() const { return _schema_table_type; }

    static void set_doris_server(DorisServer* doris_server) { _s_doris_server = doris_server; }

protected:
    Status fill_dest_column_for_range(vectorized::Block* block, size_t pos,
                                      const std::vector<void*>& datas);

    bool _is_init;
    // this is used for sub class
    SchemaScannerParam* _param;
    // schema table's column desc
    std::vector<ColumnDesc> _columns;

    static DorisServer* _s_doris_server;

    TSchemaTableType::type _schema_table_type;

    RuntimeProfile::Counter* _get_db_timer = nullptr;
    RuntimeProfile::Counter* _get_table_timer = nullptr;
    RuntimeProfile::Counter* _get_describe_timer = nullptr;
    RuntimeProfile::Counter* _fill_block_timer = nullptr;
};

} // namespace doris
