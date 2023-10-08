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
    std::string driver_path;
    std::string driver_class;
    std::string resource_name;
    std::string driver_checksum;
    std::string jdbc_url;
    std::string user;
    std::string passwd;
    std::string query_string;
    std::string table_name;
    bool use_transaction;
    TOdbcTableType::type table_type;

    const TupleDescriptor* tuple_desc;
};

class JdbcConnector : public TableConnector {
public:
    struct JdbcStatistic {
        int64_t _load_jar_timer = 0;
        int64_t _init_connector_timer = 0;
        int64_t _get_data_timer = 0;
        int64_t _call_jni_next_timer = 0;
        int64_t _convert_batch_timer = 0;
        int64_t _check_type_timer = 0;
        int64_t _execte_read_timer = 0;
        int64_t _connector_close_timer = 0;
    };

    JdbcConnector(const JdbcConnectorParam& param);

    ~JdbcConnector() override;

    Status open(RuntimeState* state, bool read = false);

    Status query() override;

    Status append(vectorized::Block* block, const vectorized::VExprContextSPtrs& _output_vexpr_ctxs,
                  uint32_t start_send_row, uint32_t* num_rows_sent,
                  TOdbcTableType::type table_type = TOdbcTableType::MYSQL) override;

    Status exec_write_sql(const std::u16string& insert_stmt,
                          const fmt::memory_buffer& insert_stmt_buffer) override {
        return Status::OK();
    }

    Status exec_stmt_write(Block* block, const VExprContextSPtrs& output_vexpr_ctxs,
                           uint32_t* num_rows_sent) override;

    Status get_next(bool* eos, std::vector<MutableColumnPtr>& columns, Block* block,
                    int batch_size);

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

protected:
    JdbcConnectorParam _conn_param;

private:
    Status _register_func_id(JNIEnv* env);
    Status _check_column_type();
    Status _check_type(SlotDescriptor*, const std::string& type_str, int column_index);
    std::string _jobject_to_string(JNIEnv* env, jobject jobj);
    Status _cast_string_to_array(const SlotDescriptor* slot_desc, Block* block, int column_index,
                                 int rows);
    Status _cast_string_to_hll(const SlotDescriptor* slot_desc, Block* block, int column_index,
                               int rows);
    Status _cast_string_to_bitmap(const SlotDescriptor* slot_desc, Block* block, int column_index,
                                  int rows);
    Status _cast_string_to_json(const SlotDescriptor* slot_desc, Block* block, int column_index,
                                int rows);
    Status _convert_batch_result_set(JNIEnv* env, jobject jobj, const SlotDescriptor* slot_desc,
                                     vectorized::IColumn* column_ptr, int num_rows,
                                     int column_index);

    bool _closed = false;
    jclass _executor_clazz;
    jclass _executor_list_clazz;
    jclass _executor_object_clazz;
    jclass _executor_string_clazz;
    jobject _executor_obj;
    jmethodID _executor_ctor_id;
    jmethodID _executor_write_id;
    jmethodID _executor_stmt_write_id;
    jmethodID _executor_read_id;
    jmethodID _executor_has_next_id;
    jmethodID _executor_block_rows_id;
    jmethodID _executor_get_blocks_id;
    jmethodID _executor_get_blocks_new_id;
    jmethodID _executor_get_boolean_result;
    jmethodID _executor_get_tinyint_result;
    jmethodID _executor_get_smallint_result;
    jmethodID _executor_get_int_result;
    jmethodID _executor_get_bigint_result;
    jmethodID _executor_get_largeint_result;
    jmethodID _executor_get_float_result;
    jmethodID _executor_get_double_result;
    jmethodID _executor_get_char_result;
    jmethodID _executor_get_string_result;
    jmethodID _executor_get_date_result;
    jmethodID _executor_get_datev2_result;
    jmethodID _executor_get_datetime_result;
    jmethodID _executor_get_datetimev2_result;
    jmethodID _executor_get_decimalv2_result;
    jmethodID _executor_get_decimal32_result;
    jmethodID _executor_get_decimal64_result;
    jmethodID _executor_get_decimal128_result;
    jmethodID _executor_get_array_result;
    jmethodID _executor_get_json_result;
    jmethodID _executor_get_hll_result;
    jmethodID _executor_get_bitmap_result;
    jmethodID _executor_get_types_id;
    jmethodID _executor_close_id;
    jmethodID _executor_get_list_id;
    jmethodID _get_bytes_id;
    jmethodID _to_string_id;
    jmethodID _executor_begin_trans_id;
    jmethodID _executor_finish_trans_id;
    jmethodID _executor_abort_trans_id;
    std::map<int, int> _map_column_idx_to_cast_idx;
    std::vector<DataTypePtr> _input_array_string_types;
    std::vector<MutableColumnPtr>
            str_array_cols; // for array type to save data like big string [1,2,3]

    std::map<int, int> _map_column_idx_to_cast_idx_hll;
    std::vector<DataTypePtr> _input_hll_string_types;
    std::vector<MutableColumnPtr> str_hll_cols; // for hll type to save data like string

    std::map<int, int> _map_column_idx_to_cast_idx_bitmap;
    std::vector<DataTypePtr> _input_bitmap_string_types;
    std::vector<MutableColumnPtr> str_bitmap_cols; // for bitmap type to save data like string

    std::map<int, int> _map_column_idx_to_cast_idx_json;
    std::vector<DataTypePtr> _input_json_string_types;
    std::vector<MutableColumnPtr> str_json_cols; // for json type to save data like string

    JdbcStatistic _jdbc_statistic;
};

} // namespace vectorized
} // namespace doris
