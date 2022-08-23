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
#ifdef LIBJVM

#include <string>

#include "jni.h"
#include "runtime/descriptors.h"

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

    const TupleDescriptor* tuple_desc;
};

class JdbcConnector {
public:
    JdbcConnector(const JdbcConnectorParam& param);

    ~JdbcConnector();

    Status open();

    Status query(const std::string& table, const std::vector<std::string>& fields,
                 const std::vector<std::string>& filters, const int64_t limit);

    Status get_next(bool* eos, std::vector<MutableColumnPtr>& columns, int batch_size);

private:
    Status _query_exec(const std::string& query);
    Status _register_func_id(JNIEnv* env);
    Status _convert_column_data(JNIEnv* env, jobject jobj, const SlotDescriptor* slot_desc,
                                vectorized::IColumn* column_ptr);
    std::string _jobject_to_string(JNIEnv* env, jobject jobj);
    int64_t _jobject_to_date(JNIEnv* env, jobject jobj);
    int64_t _jobject_to_datetime(JNIEnv* env, jobject jobj);

    bool _is_open;
    std::string _sql_str;
    const TupleDescriptor* _tuple_desc;
    const JdbcConnectorParam _conn_param;
    jclass _executor_clazz;
    jclass _executor_list_clazz;
    jclass _executor_object_clazz;
    jobject _executor_obj;
    jmethodID _executor_ctor_id;
    jmethodID _executor_query_id;
    jmethodID _executor_has_next_id;
    jmethodID _executor_get_blocks_id;
    jmethodID _executor_close_id;
    jmethodID _executor_get_list_id;
    jmethodID _executor_get_list_size_id;
    jmethodID _executor_convert_date_id;
    jmethodID _executor_convert_datetime_id;

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

#endif