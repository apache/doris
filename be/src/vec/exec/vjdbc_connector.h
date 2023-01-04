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

#include <jni.h>

#include <string_view>

#include "common/status.h"
#include "exec/table_connector.h"
#include "runtime/define_primitive_type.h"
#include "vec/data_types/data_type.h"

namespace doris {
namespace vectorized {
struct JdbcConnectorParam {
    std::string driver_path;
    std::string driver_class;
    std::string resource_name;
    std::string driver_checksum;
    std::string jdbc_url;
    std::string user;
    std::string passwd;
    std::string query_string;
    TOdbcTableType::type table_type;

    const TupleDescriptor* tuple_desc;
};

class JdbcConnector : public TableConnector {
public:
    JdbcConnector(const JdbcConnectorParam& param);

    ~JdbcConnector() override;

    Status open(RuntimeState* state, bool read = false) override;

    Status query() override;

    Status exec_write_sql(const std::u16string& insert_stmt,
                          const fmt::memory_buffer& insert_stmt_buffer) override;

    Status get_next(bool* eos, std::vector<MutableColumnPtr>& columns, Block* block,
                    int batch_size);

    // use in JDBC transaction
    Status begin_trans() override; // should be call after connect and before query or init_to_write
    Status abort_trans() override; // should be call after transaction abort
    Status finish_trans() override; // should be call after transaction commit

    Status close() override;

private:
    Status _register_func_id(JNIEnv* env);
    Status _check_column_type();
    Status _check_type(SlotDescriptor*, const std::string& type_str, int column_index);
    Status _convert_column_data(JNIEnv* env, jobject jobj, const SlotDescriptor* slot_desc,
                                vectorized::IColumn* column_ptr, int column_index,
                                std::string_view column_name);
    Status _insert_column_data(JNIEnv* env, jobject jobj, const TypeDescriptor& type,
                               vectorized::IColumn* column_ptr, int column_index,
                               std::string_view column_name);
    Status _insert_arr_column_data(JNIEnv* env, jobject jobj, const TypeDescriptor& type, int nums,
                                   vectorized::IColumn* column_ptr, int column_index,
                                   std::string_view column_name);
    std::string _jobject_to_string(JNIEnv* env, jobject jobj);
    int64_t _jobject_to_date(JNIEnv* env, jobject jobj, bool is_date_v2);
    int64_t _jobject_to_datetime(JNIEnv* env, jobject jobj, bool is_datetime_v2);
    Status _cast_string_to_array(const SlotDescriptor* slot_desc, Block* block, int column_index,
                                 int rows);

    const JdbcConnectorParam& _conn_param;
    //java.sql.Types: https://docs.oracle.com/javase/7/docs/api/constant-values.html#java.sql.Types.INTEGER
    std::map<int, PrimitiveType> _arr_jdbc_map {
            {-7, TYPE_BOOLEAN}, {-6, TYPE_TINYINT},  {5, TYPE_SMALLINT}, {4, TYPE_INT},
            {-5, TYPE_BIGINT},  {12, TYPE_STRING},   {7, TYPE_FLOAT},    {8, TYPE_DOUBLE},
            {91, TYPE_DATE},    {93, TYPE_DATETIME}, {2, TYPE_DECIMALV2}};
    bool _closed;
    jclass _executor_clazz;
    jclass _executor_list_clazz;
    jclass _executor_object_clazz;
    jclass _executor_string_clazz;
    jobject _executor_obj;
    jmethodID _executor_ctor_id;
    jmethodID _executor_write_id;
    jmethodID _executor_read_id;
    jmethodID _executor_has_next_id;
    jmethodID _executor_get_blocks_id;
    jmethodID _executor_get_types_id;
    jmethodID _executor_get_arr_list_id;
    jmethodID _executor_get_arr_type_id;
    jmethodID _executor_close_id;
    jmethodID _executor_get_list_id;
    jmethodID _executor_get_list_size_id;
    jmethodID _executor_convert_date_id;
    jmethodID _executor_convert_datetime_id;
    jmethodID _get_bytes_id;
    jmethodID _to_string_id;
    jmethodID _executor_begin_trans_id;
    jmethodID _executor_finish_trans_id;
    jmethodID _executor_abort_trans_id;
    bool _need_cast_array_type;
    std::map<int, int> _map_column_idx_to_cast_idx;
    std::vector<DataTypePtr> _input_array_string_types;
    std::vector<MutableColumnPtr>
            str_array_cols; // for array type to save data like big string [1,2,3]

#define FUNC_VARI_DECLARE(RETURN_TYPE)                                \
    RETURN_TYPE _jobject_to_##RETURN_TYPE(JNIEnv* env, jobject jobj); \
    jclass _executor_##RETURN_TYPE##_clazz;

    FUNC_VARI_DECLARE(uint8_t)
    FUNC_VARI_DECLARE(int8_t)
    FUNC_VARI_DECLARE(int16_t)
    FUNC_VARI_DECLARE(int32_t)
    FUNC_VARI_DECLARE(int64_t)
    FUNC_VARI_DECLARE(float)
    FUNC_VARI_DECLARE(double)
};

} // namespace vectorized
} // namespace doris
