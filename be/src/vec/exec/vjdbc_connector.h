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

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <jni.h>
#include <stdint.h>

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/table_connector.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/data_types/data_type.h"

namespace doris {
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;

namespace vectorized {

class Block;
class IColumn;
class VExprContext;

struct JdbcConnectorParam {
    // use -1 as default value to find error earlier.
    int64_t catalog_id = -1;
    std::string driver_path;
    std::string driver_class;
    std::string resource_name;
    std::string driver_checksum;
    std::string jdbc_url;
    std::string user;
    std::string passwd;
    std::string query_string;
    std::string table_name;
    bool use_transaction = false;
    TOdbcTableType::type table_type;
    int32_t connection_pool_min_size = -1;
    int32_t connection_pool_max_size = -1;
    int32_t connection_pool_max_wait_time = -1;
    int32_t connection_pool_max_life_time = -1;
    bool connection_pool_keep_alive = false;

    const TupleDescriptor* tuple_desc = nullptr;
};

class JdbcConnector : public TableConnector {
public:
    struct JdbcStatistic {
        int64_t _load_jar_timer = 0;
        int64_t _init_connector_timer = 0;
        int64_t _get_data_timer = 0;
        int64_t _get_block_address_timer = 0;
        int64_t _fill_block_timer = 0;
        int64_t _check_type_timer = 0;
        int64_t _execte_read_timer = 0;
        int64_t _connector_close_timer = 0;
    };

    JdbcConnector(const JdbcConnectorParam& param);

    ~JdbcConnector() override;

    Status open(RuntimeState* state, bool read = false);

    Status query() override;

    Status get_next(bool* eos, Block* block, int batch_size);

    Status append(vectorized::Block* block, const vectorized::VExprContextSPtrs& _output_vexpr_ctxs,
                  uint32_t start_send_row, uint32_t* num_rows_sent,
                  TOdbcTableType::type table_type = TOdbcTableType::MYSQL) override;

    Status exec_write_sql(const std::u16string& insert_stmt,
                          const fmt::memory_buffer& insert_stmt_buffer) override {
        return Status::OK();
    }

    Status exec_stmt_write(Block* block, const VExprContextSPtrs& output_vexpr_ctxs,
                           uint32_t* num_rows_sent) override;

    // use in JDBC transaction
    Status begin_trans() override; // should be call after connect and before query or init_to_write
    Status abort_trans() override; // should be call after transaction abort
    Status finish_trans() override; // should be call after transaction commit

    Status init_to_write(doris::RuntimeProfile* profile) override {
        init_profile(profile);
        return Status::OK();
    }

    JdbcStatistic& get_jdbc_statistic() { return _jdbc_statistic; }

    Status close(Status s = Status::OK()) override;

    Status test_connection();
    Status clean_datasource();

protected:
    JdbcConnectorParam _conn_param;

private:
    Status _register_func_id(JNIEnv* env);

    jobject _get_reader_params(Block* block, JNIEnv* env, size_t column_size);

    Status _cast_string_to_special(Block* block, JNIEnv* env, size_t column_size);
    Status _cast_string_to_hll(const SlotDescriptor* slot_desc, Block* block, int column_index,
                               int rows);
    Status _cast_string_to_bitmap(const SlotDescriptor* slot_desc, Block* block, int column_index,
                                  int rows);
    Status _cast_string_to_json(const SlotDescriptor* slot_desc, Block* block, int column_index,
                                int rows);
    jobject _get_java_table_type(JNIEnv* env, TOdbcTableType::type tableType);

    bool _closed = false;
    jclass _executor_factory_clazz;
    jclass _executor_clazz;
    jobject _executor_obj;
    jmethodID _executor_factory_ctor_id;
    jmethodID _executor_ctor_id;
    jmethodID _executor_stmt_write_id;
    jmethodID _executor_read_id;
    jmethodID _executor_has_next_id;
    jmethodID _executor_get_block_address_id;
    jmethodID _executor_block_rows_id;
    jmethodID _executor_close_id;
    jmethodID _executor_begin_trans_id;
    jmethodID _executor_finish_trans_id;
    jmethodID _executor_abort_trans_id;
    jmethodID _executor_test_connection_id;
    jmethodID _executor_clean_datasource_id;

    std::map<int, int> _map_column_idx_to_cast_idx_hll;
    std::vector<DataTypePtr> _input_hll_string_types;

    std::map<int, int> _map_column_idx_to_cast_idx_bitmap;
    std::vector<DataTypePtr> _input_bitmap_string_types;

    std::map<int, int> _map_column_idx_to_cast_idx_json;
    std::vector<DataTypePtr> _input_json_string_types;

    JdbcStatistic _jdbc_statistic;
};

} // namespace vectorized
} // namespace doris
