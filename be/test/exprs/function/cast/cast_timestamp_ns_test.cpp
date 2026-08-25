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

#include "core/data_type/data_type_timestamp_ns.h"
#include "exprs/function/cast/cast_test.h"

namespace doris {
using namespace ut_type;

TEST_F(FunctionCastTest, string_to_timestamp_ns) {
    const InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    const DataSet data_set = {
            {{std::string("1677-09-21 00:12:43.145224192")},
             std::string("1677-09-21 00:12:43.145224192")},
            {{std::string("1969-12-31 23:59:59.999999999")},
             std::string("1969-12-31 23:59:59.999999999")},
            {{std::string("1970-01-01 00:00:00.000000000")},
             std::string("1970-01-01 00:00:00.000000000")},
            {{std::string("2024-02-29 12:34:56.1234567894")},
             std::string("2024-02-29 12:34:56.123456789")},
            {{std::string("2024-02-29 12:34:56.1234567895")},
             std::string("2024-02-29 12:34:56.123456790")},
            {{std::string("2024-02-29 12:34:56.9999999995")},
             std::string("2024-02-29 12:34:57.000000000")},
            {{std::string("2024-02-29 12:34:56.123456789+08:00")},
             std::string("2024-02-29 12:34:56.123456789")},
            {{std::string("2262-04-11 23:47:16.854775807")},
             std::string("2262-04-11 23:47:16.854775807")},
            {{std::string("1677-09-21 00:12:43.145224191")}, Null()},
            {{std::string("2262-04-11 23:47:16.8547758075")}, Null()},
            {{std::string("2024-01-01 00:00:00.123.456")}, Null()},
            {{Null()}, Null()},
    };
    check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
}

TEST_F(FunctionCastTest, numeric_to_timestamp_ns) {
    {
        const InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
        const DataSet data_set = {
                {{int64_t(16770921001243)}, Null()},
                {{int64_t(19691231235959)}, std::string("1969-12-31 23:59:59.000000000")},
                {{int64_t(19700101000000)}, std::string("1970-01-01 00:00:00.000000000")},
                {{int64_t(22620411234716)}, std::string("2262-04-11 23:47:16.000000000")},
                {{int64_t(22620412000000)}, Null()},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
    {
        const InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
        const DataSet data_set = {
                {{double(19700101000000.125)}, std::string("1970-01-01 00:00:00.125000000")},
                {{double(20240229123456.5)}, std::string("2024-02-29 12:34:56.500000000")},
                {{double(22620412000000.0)}, Null()},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
    {
        const InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL128I, 10, 24}};
        const DataSet data_set = {
                {{DECIMAL128V3(16770921001243, 1452241914, 10)}, Null()},
                {{DECIMAL128V3(16770921001243, 1452241915, 10)},
                 std::string("1677-09-21 00:12:43.145224192")},
                {{DECIMAL128V3(19700101000000, 1234567895, 10)},
                 std::string("1970-01-01 00:00:00.123456790")},
                {{DECIMAL128V3(19700101000000, 9999999995, 10)},
                 std::string("1970-01-01 00:00:01.000000000")},
                {{DECIMAL128V3(22620411234716, 8547758074, 10)},
                 std::string("2262-04-11 23:47:16.854775807")},
                {{DECIMAL128V3(22620411234716, 8547758075, 10)}, Null()},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, datelike_to_timestamp_ns) {
    {
        const InputTypeSet input_types = {{PrimitiveType::TYPE_DATE}};
        const DataSet data_set = {
                {{std::string("1677-09-21")}, Null()},
                {{std::string("1970-01-01")}, std::string("1970-01-01 00:00:00.000000000")},
                {{std::string("2262-04-11")}, std::string("2262-04-11 00:00:00.000000000")},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
    {
        const InputTypeSet input_types = {{PrimitiveType::TYPE_DATEV2}};
        const DataSet data_set = {
                {{std::string("1677-09-21")}, Null()},
                {{std::string("1677-09-22")}, std::string("1677-09-22 00:00:00.000000000")},
                {{std::string("1970-01-01")}, std::string("1970-01-01 00:00:00.000000000")},
                {{std::string("2262-04-11")}, std::string("2262-04-11 00:00:00.000000000")},
                {{std::string("2262-04-12")}, Null()},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
    {
        const InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIME}};
        const DataSet data_set = {
                {{std::string("1677-09-21 00:12:43")}, Null()},
                {{std::string("1970-01-01 00:00:00")},
                 std::string("1970-01-01 00:00:00.000000000")},
                {{std::string("2262-04-11 23:47:16")},
                 std::string("2262-04-11 23:47:16.000000000")},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
    {
        const InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 6}};
        const DataSet data_set = {
                {{std::string("1677-09-21 00:12:43.145224")}, Null()},
                {{std::string("1677-09-21 00:12:43.145225")},
                 std::string("1677-09-21 00:12:43.145225000")},
                {{std::string("1970-01-01 00:00:00.000000")},
                 std::string("1970-01-01 00:00:00.000000000")},
                {{std::string("2262-04-11 23:47:16.854775")},
                 std::string("2262-04-11 23:47:16.854775000")},
                {{std::string("2262-04-11 23:47:16.854776")}, Null()},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
    {
        const InputTypeSet input_types = {{PrimitiveType::TYPE_TIMEV2, 6}};
        const DataSet data_set = {
                {{std::string("23:59:59.999999")}, std::string("2019-08-06 23:59:59.999999000")},
                {{std::string("-128:00:00")}, std::string("2019-07-31 16:00:00.000000000")},
        };
        check_function_for_cast<DataTypeTimeStampNs>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, datetimev2_to_timestamp_ns_strict_overflow) {
    const InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 6}};
    const DataSet valid_data_set = {
            {{std::string("1677-09-21 00:12:43.145225")},
             std::string("1677-09-21 00:12:43.145225000")},
            {{std::string("1970-01-01 00:00:00.000000")},
             std::string("1970-01-01 00:00:00.000000000")},
            {{std::string("2262-04-11 23:47:16.854775")},
             std::string("2262-04-11 23:47:16.854775000")},
    };
    check_function_for_cast_strict_mode<DataTypeTimeStampNs>(input_types, valid_data_set);

    const DataSet overflow_data_set = {
            {{std::string("1677-09-21 00:12:43.145224")}, Null()},
            {{std::string("2262-04-11 23:47:16.854776")}, Null()},
    };
    check_function_for_cast_strict_mode<DataTypeTimeStampNs>(input_types, overflow_data_set,
                                                             "TIMESTAMP_NS overflow");
}

TEST_F(FunctionCastTest, numeric_to_timestamp_ns_strict_overflow) {
    const InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
    const DataSet valid_data_set = {
            {{int64_t(19700101000000)}, std::string("1970-01-01 00:00:00.000000000")},
            {{int64_t(22620411234716)}, std::string("2262-04-11 23:47:16.000000000")},
    };
    check_function_for_cast_strict_mode<DataTypeTimeStampNs>(input_types, valid_data_set);

    const DataSet overflow_data_set = {
            {{int64_t(16770921001243)}, Null()},
            {{int64_t(22620412000000)}, Null()},
    };
    check_function_for_cast_strict_mode<DataTypeTimeStampNs>(
            input_types, overflow_data_set, "outside the signed epoch-nanosecond range");
}

TEST_F(FunctionCastTest, timestamp_ns_to_supported_scalar_types) {
    const InputTypeSet input_types = {{PrimitiveType::TYPE_TIMESTAMP_NS}};
    const DataSet datetime_data = {
            {{std::string("1677-09-21 00:12:43.145224192")},
             std::string("1677-09-21 00:12:43.145224")},
            {{std::string("1969-12-31 23:59:59.999999999")},
             std::string("1970-01-01 00:00:00.000000")},
            {{std::string("2024-02-29 12:34:56.123456789")},
             std::string("2024-02-29 12:34:56.123457")},
            {{std::string("2262-04-11 23:47:16.854775807")},
             std::string("2262-04-11 23:47:16.854776")},
    };
    check_function_for_cast<DataTypeDateTimeV2>(input_types, datetime_data, 6);
    check_function_for_cast<DataTypeDateTimeV2, true>(input_types, datetime_data, 6);

    const DataSet date_data = {
            {{std::string("1677-09-21 00:12:43.145224192")}, std::string("1677-09-21")},
            {{std::string("1970-01-01 00:00:00.000000000")}, std::string("1970-01-01")},
            {{std::string("2262-04-11 23:47:16.854775807")}, std::string("2262-04-11")},
    };
    check_function_for_cast<DataTypeDateV2>(input_types, date_data);
    check_function_for_cast<DataTypeDate>(input_types, date_data);

    const DataSet legacy_datetime_data = {
            {{std::string("1677-09-21 00:12:43.145224192")}, std::string("1677-09-21 00:12:43")},
            {{std::string("1970-01-01 00:00:00.000000000")}, std::string("1970-01-01 00:00:00")},
            {{std::string("2262-04-11 23:47:16.854775807")}, std::string("2262-04-11 23:47:16")},
    };
    check_function_for_cast<DataTypeDateTime>(input_types, legacy_datetime_data);

    const DataSet time_data = {
            {{std::string("1677-09-21 00:12:43.145224192")}, std::string("00:12:43.145224")},
            {{std::string("1969-12-31 23:59:59.999999999")}, std::string("24:00:00.000000")},
            {{std::string("2024-02-29 12:34:56.123456789")}, std::string("12:34:56.123457")},
    };
    check_function_for_cast<DataTypeTimeV2>(input_types, time_data, 6);

    const DataSet integer_data = {
            {{std::string("1677-09-21 00:12:43.145224192")}, int64_t(16770921001243)},
            {{std::string("1970-01-01 00:00:00.000000000")}, int64_t(19700101000000)},
            {{std::string("2262-04-11 23:47:16.854775807")}, int64_t(22620411234716)},
    };
    check_function_for_cast<DataTypeInt64>(input_types, integer_data);
    check_function_for_cast<DataTypeInt128>(
            input_types,
            {{{std::string("1970-01-01 00:00:00.000000000")}, int128_t(19700101000000)}});
    check_function_for_cast<DataTypeFloat64>(
            input_types,
            {{{std::string("1970-01-01 00:00:00.000000000")}, double(19700101000000)}});
}

TEST_F(FunctionCastTest, unsupported_timestamp_ns_cast) {
    const InputTypeSet input_types = {{PrimitiveType::TYPE_TIMESTAMP_NS}};
    const DataSet data_set = {
            {{std::string("2024-02-29 12:34:56.123456789")}, int32_t(0)},
    };
    check_function_for_cast<DataTypeInt32>(input_types, data_set, -1, -1, true, true);
}

} // namespace doris
