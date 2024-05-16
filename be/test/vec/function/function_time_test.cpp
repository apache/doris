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

#include <gen_cpp/types.pb.h>
#include <stdint.h>

#include <iomanip>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "runtime/runtime_state.h"
#include "testutil/any_type.h"
#include "util/timezone_utils.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(VTimestampFunctionsTest, day_of_week_test) {
    std::string func_name = "dayofweek";

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(1)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(3)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(5)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(3)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(7)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(5)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(7)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(4)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(6)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(2)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};

        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int8_t(3)},
                {{std::string("2020-12-15")}, std::int8_t(3)},
                {{std::string("2021-01-01")}, std::int8_t(6)},
                {{std::string("2021-01-31")}, std::int8_t(1)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(7)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(5)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(6)},
                {{std::string("0001-01-01")}, std::int8_t(2)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(2)},

        };

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_of_month_test) {
    std::string func_name = "dayofmonth";

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(31)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(28)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(29)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(4)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(1)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(30)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(15)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(31)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(29)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}

        };

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};

        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int8_t(15)},
                {{std::string("2021-01-01")}, std::int8_t(1)},
                {{std::string("2021-01-31")}, std::int8_t(31)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(29)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(31)},
                {{std::string("0001-01-01")}, std::int8_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(1)},
        };

        check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, day_of_year_test) {
    std::string func_name = "dayofyear";

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2023-01-01 00:00:00")}, std::int16_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int16_t(365)},
                            {{std::string("2023-02-28 12:34:56")}, std::int16_t(59)},
                            {{std::string("2024-02-29 15:30:45")}, std::int16_t(60)},
                            {{std::string("2023-07-04 07:07:07")}, std::int16_t(185)},
                            {{std::string("2023-04-01 00:00:01")}, std::int16_t(91)},
                            {{std::string("2023-11-30 23:59:00")}, std::int16_t(334)},
                            {{std::string("2023-04-15 12:00:00")}, std::int16_t(105)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int16_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int16_t(365)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int16_t(60)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}

        };

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt16, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};

        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int16_t(350)},
                {{std::string("2021-01-01")}, std::int16_t(1)},
                {{std::string("2021-01-31")}, std::int16_t(31)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int16_t(60)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int16_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int16_t(365)},
                {{std::string("0001-01-01")}, std::int16_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int16_t(1)},
        };

        check_function_all_arg_comb<DataTypeInt16, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, week_of_year_test) {
    std::string func_name = "weekofyear";
    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2020-00-01 00:00:00")}, Null()},
                            {{std::string("2020-01-00 00:00:00")}, Null()},
                            {{std::string("2020-02-29 00:00:00")}, int8_t {9}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(52)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(52)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(9)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(9)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(27)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(13)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(48)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(15)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(52)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(9)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};
        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int8_t(51)},
                {{std::string("2021-01-01")}, std::int8_t(53)},
                {{std::string("2021-01-31")}, std::int8_t(4)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(9)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(52)},
                {{std::string("0001-01-01")}, std::int8_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(1)},
        };

        check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, year_test) {
    std::string func_name = "year";

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int16_t {2021}},
                            {{std::string("2021-01-00 00:00:00")}, Null()},
                            {{std::string("2025-05-01 00:00:00")}, int16_t {2025}},
                            {{std::string("2023-01-01 00:00:00")}, std::int16_t(2023)},
                            {{std::string("2023-12-31 23:59:59")}, std::int16_t(2023)},
                            {{std::string("2023-02-28 12:34:56")}, std::int16_t(2023)},
                            {{std::string("2024-02-29 15:30:45")}, std::int16_t(2024)},
                            {{std::string("2023-07-04 07:07:07")}, std::int16_t(2023)},
                            {{std::string("2023-04-01 00:00:01")}, std::int16_t(2023)},
                            {{std::string("2023-11-30 23:59:00")}, std::int16_t(2023)},
                            {{std::string("2023-04-15 12:00:00")}, std::int16_t(2023)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int16_t(1000)},
                            {{std::string("9999-12-31 23:59:59")}, std::int16_t(9999)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int16_t(1904)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}

        };

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt16, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};

        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int16_t(2020)},
                {{std::string("2021-01-01")}, std::int16_t(2021)},
                {{std::string("2021-01-31")}, std::int16_t(2021)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int16_t(2020)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int16_t(1970)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int16_t(9999)},
                {{std::string("0001-01-01")}, std::int16_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int16_t(1)},

        };

        check_function_all_arg_comb<DataTypeInt16, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, quarter_test) {
    std::string func_name = "quarter";

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-10-23 00:00:00")}, int8_t {4}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(4)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(1)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(1)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(3)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(2)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(4)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(2)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(4)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(1)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};

        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int8_t(4)},
                {{std::string("2021-01-01")}, std::int8_t(1)},
                {{std::string("2021-01-31")}, std::int8_t(1)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(1)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(4)},
                {{std::string("0001-01-01")}, std::int8_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(1)},
        };

        check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, month_test) {
    std::string func_name = "month";

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {5}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(12)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(2)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(2)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(4)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(11)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(4)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(12)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(2)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};

        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int8_t(12)},
                {{std::string("2021-01-01")}, std::int8_t(1)},
                {{std::string("2021-01-31")}, std::int8_t(1)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(2)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(12)},
                {{std::string("0001-01-01")}, std::int8_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(1)},
        };

        check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, day_test) {
    std::string func_name = "day";

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {23}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(31)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(28)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(29)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(4)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(1)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(30)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(15)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(31)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(29)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::Date};
        DataSet data_set = {
                {{std::string("2020-12-15")}, std::int8_t(15)},
                {{std::string("2021-01-01")}, std::int8_t(1)},
                {{std::string("2021-01-31")}, std::int8_t(31)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(29)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(31)},
                {{std::string("0001-01-01")}, std::int8_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(1)},
        };

        check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, hour_test) {
    std::string func_name = "hour";

    BaseInputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:59:59")}, int8_t {23}},
                        {{std::string("2021-01-13 16:56:00")}, int8_t {16}},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()},
                        {{std::string("2023-01-01 00:00:00")}, std::int8_t(0)},
                        {{std::string("2023-12-31 23:59:59")}, std::int8_t(23)},
                        {{std::string("2023-02-28 12:34:56")}, std::int8_t(12)},
                        {{std::string("2024-02-29 15:30:45")}, std::int8_t(15)},
                        {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                        {{std::string("2023-04-01 00:00:01")}, std::int8_t(0)},
                        {{std::string("2023-11-30 23:59:00")}, std::int8_t(23)},
                        {{std::string("2023-04-15 12:00:00")}, std::int8_t(12)},
                        {{std::string("2023-02-30 12:00:00")}, Null()},
                        {{std::string("2023-01-01 25:00:00")}, Null()},
                        {{std::string("1000-01-01 00:00:00")}, std::int8_t(0)},
                        {{std::string("9999-12-31 23:59:59")}, std::int8_t(23)},
                        {{std::string("2023-01-01 24:00:00")}, Null()},
                        {{std::string("2023-12-31 00:60:00")}, Null()},
                        {{std::string("2023-12-31 00:00:60")}, Null()},
                        {{std::string("1904-02-29 12:34:56")}, std::int8_t(12)},
                        {{std::string("1900-02-29 12:34:56")}, Null()},
                        {{Null()}, Null()}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, minute_test) {
    std::string func_name = "minute";

    BaseInputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:59:50")}, int8_t {59}},
                        {{std::string("2021-01-13 16:20:00")}, int8_t {20}},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()},
                        {{std::string("2023-01-01 00:00:00")}, std::int8_t(0)},
                        {{std::string("2023-12-31 23:59:59")}, std::int8_t(59)},
                        {{std::string("2023-02-28 12:34:56")}, std::int8_t(34)},
                        {{std::string("2024-02-29 15:30:45")}, std::int8_t(30)},
                        {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                        {{std::string("2023-04-01 00:00:01")}, std::int8_t(0)},
                        {{std::string("2023-11-30 23:59:00")}, std::int8_t(59)},
                        {{std::string("2023-04-15 12:00:00")}, std::int8_t(0)},
                        {{std::string("2023-02-30 12:00:00")}, Null()},
                        {{std::string("2023-01-01 25:00:00")}, Null()},
                        {{std::string("1000-01-01 00:00:00")}, std::int8_t(0)},
                        {{std::string("9999-12-31 23:59:59")}, std::int8_t(59)},
                        {{std::string("2023-01-01 24:00:00")}, Null()},
                        {{std::string("2023-12-31 00:60:00")}, Null()},
                        {{std::string("2023-12-31 00:00:60")}, Null()},
                        {{std::string("1904-02-29 12:34:56")}, std::int8_t(34)},
                        {{std::string("1900-02-29 12:34:56")}, Null()},
                        {{Null()}, Null()}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, second_test) {
    std::string func_name = "second";

    BaseInputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {{{std::string("2021-01-01 23:50:59")}, int8_t {59}},
                        {{std::string("2021-01-13 16:20:00")}, int8_t {0}},
                        {{std::string("")}, Null()},
                        {{std::string("2025-05-23 24:00:00")}, Null()},
                        {{std::string("2023-01-01 00:00:00")}, std::int8_t(0)},
                        {{std::string("2023-12-31 23:59:59")}, std::int8_t(59)},
                        {{std::string("2023-02-28 12:34:56")}, std::int8_t(56)},
                        {{std::string("2024-02-29 15:30:45")}, std::int8_t(45)},
                        {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                        {{std::string("2023-04-01 00:00:01")}, std::int8_t(1)},
                        {{std::string("2023-11-30 23:59:00")}, std::int8_t(0)},
                        {{std::string("2023-04-15 12:00:00")}, std::int8_t(0)},
                        {{std::string("2023-02-30 12:00:00")}, Null()},
                        {{std::string("2023-01-01 25:00:00")}, Null()},
                        {{std::string("1000-01-01 00:00:00")}, std::int8_t(0)},
                        {{std::string("9999-12-31 23:59:59")}, std::int8_t(59)},
                        {{std::string("2023-01-01 24:00:00")}, Null()},
                        {{std::string("2023-12-31 00:60:00")}, Null()},
                        {{std::string("2023-12-31 00:00:60")}, Null()},
                        {{std::string("1904-02-29 12:34:56")}, std::int8_t(56)},
                        {{std::string("1900-02-29 12:34:56")}, Null()},
                        {{Null()}, Null()}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, from_unix_test) {
    std::string func_name = "from_unixtime";
    {
        TimezoneUtils::load_timezone_names();

        BaseInputTypeSet input_types = {TypeIndex::Int64};

        DataSet data_set = {
                {{int64_t(1565080737)}, std::string("2019-08-06 16:38:57")},
                {{int64_t(-123)}, Null()},
                {{std::int64_t(0)}, std::string("1970-01-01 08:00:00")},
                {{std::int64_t(32536771199)}, std::string("3001-01-19 07:59:59")},
                {{std::int64_t(-1)}, Null()},
                {{std::int64_t(32536771200)}, Null()},
                {{std::int64_t(1616161616)}, std::string("2021-03-19 21:46:56")},
                {{Null()}, Null()},
        };

        static_cast<void>(check_function_all_arg_comb<DataTypeString, true>(func_name, input_types,
                                                                            data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::Int64, Consted {TypeIndex::String}};

        DataSet data_set = {
                {{std::int64_t(0), std::string("%Y-%m-%d")}, std::string("1970-01-01")},
                {{std::int64_t(0), std::string("%H:%i:%s")}, std::string("08:00:00")},
                {{std::int64_t(32536771199), std::string("%Y-%m-%d")}, std::string("3001-01-19")},
                {{std::int64_t(32536771199), std::string("%H:%i:%s")}, std::string("07:59:59")},
                {{std::int64_t(-1), std::string("%Y-%m-%d")}, Null()},
                {{std::int64_t(-1), std::string("%H:%i:%s")}, Null()},
                {{std::int64_t(-1), Null()}, Null()},
                {{std::int64_t(32536771200), std::string("%Y-%m-%d")}, Null()},
                {{std::int64_t(32536771200), std::string("%H:%i:%s")}, Null()},
                {{std::int64_t(32536771200), Null()}, Null()},
                {{std::int64_t(1616161616), std::string("%Y-%m-%d")}, std::string("2021-03-19")},
                {{std::int64_t(1616161616), std::string("%H:%i:%s")}, std::string("21:46:56")},
                {{Null(), std::string("%Y-%m-%d")}, Null()},
                {{Null(), std::string("%H:%i:%s")}, Null()},
                {{Null(), Null()}, Null()},

        };

        for (const auto& line : data_set) {
            DataSet tmp {line};
            static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, tmp));
        }
    }
}

TEST(VTimestampFunctionsTest, timediff_test) {
    std::string func_name = "timediff";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 12:00:00")}, 0.0},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 13:01:02")},
             (double)-3662.0 * 1e6},
            {{std::string("2019-00-18 12:00:00"), std::string("2019-07-18 13:01:02")}, Null()},
            {{std::string("2019-07-18 12:00:00"), std::string("2019-07-00 13:01:02")}, Null()},
            {{std::string("2023-05-07 10:00:00"), std::string("2023-05-07 09:00:00")},
             (double)3600.0 * 1e6},
            {{std::string("2023-05-07 00:00:00"), std::string("2023-05-08 00:00:00")},
             (double)-86400.0 * 1e6},
            {{std::string("2023-05-01 15:30:00"), std::string("2023-05-07 15:30:00")},
             (double)-518400.0 * 1e6},
            {{std::string("2019-02-28 23:59:59"), std::string("2019-03-01 00:00:00")},
             (double)-1.0 * 1e6},
            {{std::string("not-a-datetime"), std::string("2023-05-07 09:00:00")}, Null()},
            {{std::string("2023-05-07 09:00:00"), std::string("not-a-datetime")}, Null()},
            {{std::string("2023-05-07 09:00:00"), Null()}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeTimeV2, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, convert_tz_test) {
    std::string func_name = "convert_tz";

    TimezoneUtils::clear_timezone_caches();

    InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::String, TypeIndex::String};

    bool case_sensitive = true;
    cctz::time_zone tz {};
    if (TimezoneUtils::find_cctz_time_zone("Asia/SHANGHAI", tz)) {
        case_sensitive = false;
    }

    if (case_sensitive) {
        DataSet data_set = {{{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/SHANGHAI"},
                              std::string {"america/Los_angeles"}},
                             Null()}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, false));
    }

    {
        DataSet data_set = {{{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             str_to_datetime_v2("2019-07-31 18:18:27", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             str_to_datetime_v2("2019-07-31 18:18:27", "%Y-%m-%d %H:%i:%s.%f")}};
        if (case_sensitive) {
            data_set.push_back(Row {{std::string {"2019-08-01 02:18:27"},
                                     std::string {"Asia/Shanghai"}, std::string {"Utc"}},
                                    Null()});
            data_set.push_back(
                    Row {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/SHANGHAI"},
                          std::string {"america/Los_angeles"}},
                         Null()});
        }
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, false));
    }

    {
        DataSet data_set = {{{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             str_to_datetime_v2("2019-07-31 18:18:27", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"Utc"}},
                             str_to_datetime_v2("2019-07-31 18:18:27", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/Shanghai"},
                              std::string {"UTC"}},
                             str_to_datetime_v2("2019-07-31 18:18:27", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string {"2019-08-01 02:18:27"}, std::string {"Asia/SHANGHAI"},
                              std::string {"america/Los_angeles"}},
                             str_to_datetime_v2("2019-07-31 11:18:27", "%Y-%m-%d %H:%i:%s.%f")}};
        TimezoneUtils::load_timezone_names();
        TimezoneUtils::load_timezones_to_cache();
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set, false));
    }
}

TEST(VTimestampFunctionsTest, date_format_test) {
    std::string func_name = "date_format";

    InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
    {
        DataSet data_set = {{{std::string("2009-10-04 22:23:00"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {{{std::string("2007-10-04 22:23:00"), std::string("%H:%i:%s")},
                             std::string("22:23:00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("1900-10-04 22:23:00"), std::string("%D %y %a %d %m %b %j")},
                 std::string("4th 00 Thu 04 10 Oct 277")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("1997-10-04 22:23:00"), std::string("%H %k %I %r %T %S %w")},
                 std::string("22 22 10 10:23:00 PM 22:23:00 00 6")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}
TEST(VTimestampFunctionsTest, years_add_test) {
    std::string func_name = "years_add";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 5}, str_to_date_time("2025-05-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -5}, str_to_date_time("2015-05-23 00:00:00")},
            {{std::string(""), 5}, Null()},
            {{std::string("2020-05-23 00:00:00"), 8000}, Null()},
            {{Null(), 5}, Null()},
            {{std::string("2023-01-01 00:00:00"), 1}, str_to_date_time("2024-01-01 00:00:00")},
            {{std::string("2023-01-01 00:00:00"), -1}, str_to_date_time("2022-01-01 00:00:00")},
            {{std::string("2020-02-29 00:00:00"), 1}, str_to_date_time("2021-02-28 00:00:00")},
            {{std::string("9999-12-31 23:59:59"), 1}, Null()},
            {{std::string("0000-01-01 00:00:00"), -1}, Null()},
            {{std::string("2020-05-23 00:00:00"), 0}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string("not-a-valid-date"), 5}, Null()},
            {{Null(), 1}, Null()},
            {{std::string("2020-05-23 00:00:00"), 100}, str_to_date_time("2120-05-23 00:00:00")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, years_sub_test) {
    std::string func_name = "years_sub";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 5}, str_to_date_time("2015-05-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -5}, str_to_date_time("2025-05-23 00:00:00")},
            {{std::string(""), 5}, Null()},
            {{std::string("2020-05-23 00:00:00"), 3000}, Null()},
            {{Null(), 5}, Null()},
            {{std::string("2025-05-23 00:00:00"), 5}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string("2015-05-23 00:00:00"), -5}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string(""), 5}, Null()},
            {{std::string("9999-12-31 23:59:59"), -8000}, Null()},
            {{std::string("2023-01-01 00:00:00"), -1}, str_to_date_time("2024-01-01 00:00:00")},
            {{std::string("2023-01-01 00:00:00"), 1}, str_to_date_time("2022-01-01 00:00:00")},
            {{std::string("2021-02-28 00:00:00"), 1}, str_to_date_time("2020-02-28 00:00:00")},
            {{std::string("0001-01-01 00:00:00"), 1}, str_to_date_time("0000-01-01 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), 0}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string("not-a-valid-date"), 5}, Null()},
            {{std::string("2120-05-23 00:00:00"), 100}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string("1920-05-23 00:00:00"), -100}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string("2024-02-29 00:00:00"), 1}, str_to_date_time("2023-02-28 00:00:00")},
            {{std::string("2024-02-29 00:00:00"), 4}, str_to_date_time("2020-02-29 00:00:00")},
            {{std::string("2048-02-29 00:00:00"), 3}, str_to_date_time("2045-02-28 00:00:00")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, months_add_test) {
    std::string func_name = "months_add";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 00:00:00"), -4}, str_to_date_time("2020-06-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_date_time("2020-09-23 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 10}, str_to_date_time("2021-03-23 00:00:00")},
            {{std::string("2021-01-31"), 1}, str_to_date_time("2021-02-28 00:00:00")},
            {{std::string("2021-02-30 00:00:00"), 1}, Null()},
            {{std::string("1970-01-01 00:00:00"), 600}, str_to_date_time("2020-01-01 00:00:00")},
            {{std::string("9999-12-31 00:00:00"), 1}, Null()},
            {{std::string("2020-12-31 00:00:00"), -2400}, str_to_date_time("1820-12-31 00:00:00")},
            {{std::string("2021-05-31 00:00:00"), 25}, str_to_date_time("2023-06-30 00:00:00")},
            {{std::string("2021-05-3a 00:00:00"), 1}, Null()},
            {{std::string("0000-01-01 :00:00:00"), -1}, Null()}

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, months_sub_test) {
    std::string func_name = "months_sub";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_date_time("2020-01-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -4}, str_to_date_time("2020-09-23 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 10}, str_to_date_time("2019-07-23 00:00:00")},
            {{Null(), 4}, Null()},
            {{std::string("2023-12-15 00:00:00"), 3}, str_to_date_time("2023-09-15 00:00:00")},
            {{std::string("2023-03-31 00:00:00"), 1}, str_to_date_time("2023-02-28 00:00:00")},
            {{std::string("2020-01-31 00:00:00"), 12}, str_to_date_time("2019-01-31 00:00:00")},
            {{std::string("2023-01-31 00:00:00"), -2}, str_to_date_time("2023-03-31 00:00:00")},
            {{std::string("0001-01-01 00:00:00"), 1}, str_to_date_time("0000-12-01 00:00:00")},

    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, days_add_test) {
    std::string func_name = "days_add";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 00:00:00"), -4}, str_to_date_time("2020-10-19 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_date_time("2020-05-27 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 10}, str_to_date_time("2020-06-2 00:00:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-10-23 00:00:00"), 10}, str_to_date_time("2020-11-02 00:00:00")},
            {{std::string("2020-10-23 00:00:00"), -10}, str_to_date_time("2020-10-13 00:00:00")},
            {{std::string(""), 10}, Null()},
            {{std::string("2020-13-23 00:00:00"), 5}, Null()},
            {{Null(), 10}, Null()},
            {{std::string("2020-12-28 00:00:00"), 5}, str_to_date_time("2021-01-02 00:00:00")},
            {{std::string("2020-01-05 00:00:00"), -10}, str_to_date_time("2019-12-26 00:00:00")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, days_sub_test) {
    std::string func_name = "days_sub";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 00:00:00"), 4}, str_to_date_time("2020-05-19 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), -4}, str_to_date_time("2020-05-27 00:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 31}, str_to_date_time("2020-04-22 00:00:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-10-23 00:00:00"), 10}, str_to_date_time("2020-10-13 00:00:00")},
            {{std::string("2020-10-23 00:00:00"), -10}, str_to_date_time("2020-11-02 00:00:00")},
            {{std::string(""), 10}, Null()},
            {{std::string("2020-13-23 00:00:00"), 5}, Null()},
            {{Null(), 10}, Null()},
            {{std::string("2020-01-05 00:00:00"), 10}, str_to_date_time("2019-12-26 00:00:00")},
            {{std::string("2020-12-28 00:00:00"), -5}, str_to_date_time("2021-01-02 00:00:00")},
            {{std::string("2020-02-29 00:00:00"), -1}, str_to_date_time("2020-03-01 00:00:00")},
            {{std::string("2019-02-28 00:00:00"), -1}, str_to_date_time("2019-03-01 00:00:00")},
            {{std::string("9999-12-31 00:00:00"), -1}, Null()},
            {{std::string("0000-01-01 00:00:00"), 1}, Null()},
            {{std::string("2020-03-01 00:00:00"), 1}, str_to_date_time("2020-02-29 00:00:00")},
            {{std::string("2020-03-01 00:00:00"), 1}, str_to_date_time("2020-02-29 00:00:00")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, hours_add_test) {
    std::string func_name = "hours_add";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), -4}, str_to_date_time("2020-10-23 06:00:00")},
            {{std::string("2020-05-23 10:00:00"), 4}, str_to_date_time("2020-05-23 14:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_date_time("2020-05-27 14:00:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-10-23 00:00:00"), 10}, str_to_date_time("2020-10-23 10:00:00")},
            {{std::string("2020-10-23 10:00:00"), -5}, str_to_date_time("2020-10-23 05:00:00")},
            {{std::string("2020-10-23 20:00:00"), 6}, str_to_date_time("2020-10-24 02:00:00")},
            {{std::string("2020-02-29 23:00:00"), 2}, str_to_date_time("2020-03-01 01:00:00")},
            {{std::string("2019-02-28 23:00:00"), 2}, str_to_date_time("2019-03-01 01:00:00")},
            {{std::string(""), 24}, Null()},
            {{Null(), 24}, Null()},
            {{std::string("9999-12-31 23:00:00"), 1}, Null()},
            {{std::string("0000-01-01 00:00:00"), -1}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, hours_sub_test) {
    std::string func_name = "hours_sub";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 4}, str_to_date_time("2020-05-23 06:00:00")},
            {{std::string("2020-05-23 10:00:00"), -4}, str_to_date_time("2020-05-23 14:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 31}, str_to_date_time("2020-05-22 03:00:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-10-23 10:00:00"), 5}, str_to_date_time("2020-10-23 05:00:00")},
            {{std::string("2020-10-23 00:00:00"), -6}, str_to_date_time("2020-10-23 06:00:00")},
            {{std::string("2020-10-24 02:00:00"), 6}, str_to_date_time("2020-10-23 20:00:00")},
            {{std::string("2020-03-01 01:00:00"), 3}, str_to_date_time("2020-02-29 22:00:00")},
            {{std::string("2019-03-01 01:00:00"), 3}, str_to_date_time("2019-02-28 22:00:00")},
            {{std::string(""), 24}, Null()}};

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, minutes_add_test) {
    std::string func_name = "minutes_add";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), 40}, str_to_date_time("2020-10-23 10:40:00")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_date_time("2020-05-23 09:20:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_date_time("2020-05-23 11:40:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 30}, str_to_date_time("2020-05-23 00:30:00")},
            {{std::string("2020-05-23 00:30:00"), -30}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string(""), 30}, Null()},
            {{Null(), 30}, Null()},
            {{std::string("9999-12-31 23:59:00"), 1}, Null()},
            {{std::string("0000-01-01 00:00:00"), -1}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, minutes_sub_test) {
    std::string func_name = "minutes_sub";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 40}, str_to_date_time("2020-05-23 09:20:00")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_date_time("2020-05-23 10:40:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_date_time("2020-05-23 08:20:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-10-23 10:30:00"), 30}, str_to_date_time("2020-10-23 10:00:00")},
            {{std::string("2020-10-23 10:00:00"), -30}, str_to_date_time("2020-10-23 10:30:00")},
            {{std::string("2020-10-24 00:15:00"), 45}, str_to_date_time("2020-10-23 23:30:00")},
            {{std::string("2020-05-01 00:10:00"), 15}, str_to_date_time("2020-04-30 23:55:00")},
            {{std::string(""), 30}, Null()},
            {{Null(), 30}, Null()},
            {{std::string("0000-01-01 00:01:00"), 2}, Null()},
            {{std::string("9999-12-31 23:59:00"), -1}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, seconds_add_test) {
    std::string func_name = "seconds_add";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), 40}, str_to_date_time("2020-10-23 10:00:40")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_date_time("2020-05-23 09:59:20")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_date_time("2020-05-23 10:01:40")},
            {{Null(), 4}, Null()},
            {{std::string("2020-05-23 00:00:00"), 30}, str_to_date_time("2020-05-23 00:00:30")},
            {{std::string("2020-05-23 00:00:30"), -30}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string("2020-05-23 00:00:45"), 20}, str_to_date_time("2020-05-23 00:01:05")},
            {{std::string("2020-05-23 23:59:50"), 20}, str_to_date_time("2020-05-24 00:00:10")},
            {{std::string("2020-05-23 23:59:50"), 3600}, str_to_date_time("2020-05-24 00:59:50")},
            {{std::string("2020-04-30 23:59:50"), 10}, str_to_date_time("2020-05-01 00:00:00")},
            {{std::string("2020-12-31 23:59:50"), 15}, str_to_date_time("2021-01-01 00:00:05")},
            {{std::string(""), 30}, Null()},
            {{Null(), 30}, Null()},
            {{std::string("9999-12-31 23:59:59"), 2}, Null()},
            {{std::string("0000-01-01 00:00:00"), -1}, Null()},
            {{std::string("2020-02-29 23:59:50"), 20}, str_to_date_time("2020-03-01 00:00:10")},
            {{std::string("2019-02-28 23:59:50"), 20}, str_to_date_time("2019-03-01 00:00:10")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, seconds_sub_test) {
    std::string func_name = "seconds_sub";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 40}, str_to_date_time("2020-05-23 09:59:20")},
            {{std::string("2020-05-23 10:00:00"), -40}, str_to_date_time("2020-05-23 10:00:40")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_date_time("2020-05-23 09:58:20")},
            {{Null(), 4}, Null()},
            {{std::string("2020-05-23 01:00:30"), 30}, str_to_date_time("2020-05-23 01:00:00")},
            {{std::string("2020-05-23 01:00:00"), -30}, str_to_date_time("2020-05-23 01:00:30")},
            {{std::string("2020-05-23 01:01:15"), 75}, str_to_date_time("2020-05-23 01:00:00")},
            {{std::string("2020-05-23 01:00:00"), 3600}, str_to_date_time("2020-05-23 00:00:00")},
            {{std::string("2020-05-23 00:00:00"), 86400}, str_to_date_time("2020-05-22 00:00:00")},
            {{std::string("2020-05-01 00:01:00"), 60}, str_to_date_time("2020-05-01 00:00:00")},
            {{std::string("2021-01-01 00:00:30"), 30}, str_to_date_time("2021-01-01 00:00:00")},
            {{std::string(""), 30}, Null()},
            {{Null(), 30}, Null()},
            {{std::string("9999-12-31 23:59:59"), 1}, str_to_date_time("9999-12-31 23:59:58")},
            {{std::string("0000-01-01 00:00:01"), 2}, Null()},
            {{std::string("2020-03-01 00:00:00"), 86400}, str_to_date_time("2020-02-29 00:00:00")},
            {{std::string("2020-03-01 00:00:00"), 30}, str_to_date_time("2020-02-29 23:59:30")},
            {{std::string("2020-02-29 23:59:59"), 60}, str_to_date_time("2020-02-29 23:58:59")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, weeks_add_test) {
    std::string func_name = "weeks_add";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-10-23 10:00:00"), 5}, str_to_date_time("2020-11-27 10:00:00")},
            {{std::string("2020-05-23 10:00:00"), -5}, str_to_date_time("2020-04-18 10:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_date_time("2022-04-23 10:00:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 1}, str_to_date_time("2020-05-30 10:00:00")},
            {{std::string("2020-05-23 10:00:00"), -1}, str_to_date_time("2020-05-16 10:00:00")},
            {{std::string(""), 1}, Null()},
            {{Null(), 1}, Null()},
            {{std::string("2020-12-24 10:00:00"), 2}, str_to_date_time("2021-01-07 10:00:00")},
            {{std::string("2020-02-20 10:00:00"), 1}, str_to_date_time("2020-02-27 10:00:00")},
            {{std::string("2020-02-29 10:00:00"), 1}, str_to_date_time("2020-03-07 10:00:00")},
            {{std::string("2020-02-01 10:00:00"), 4}, str_to_date_time("2020-02-29 10:00:00")},
            {{std::string("2020-02-01 10:00:00"), 9}, str_to_date_time("2020-04-04 10:00:00")},
            {{std::string("9999-12-31 23:59:59"), 1}, Null()},
            {{std::string("0000-01-01 00:00:00"), 1}, str_to_date_time("0000-01-08 00:00:00")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, weeks_sub_test) {
    std::string func_name = "weeks_sub";

    BaseInputTypeSet input_types = {TypeIndex::DateTime, TypeIndex::Int32};

    DataSet data_set = {
            {{std::string("2020-05-23 10:00:00"), 5}, str_to_date_time("2020-04-18 10:00:00")},
            {{std::string("2020-05-23 10:00:00"), -5}, str_to_date_time("2020-6-27 10:00:00")},
            {{std::string(""), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 100}, str_to_date_time("2018-06-23 10:00:00")},
            {{Null(), 4}, Null()},
            {{std::string("2020-05-23 10:00:00"), 1}, str_to_date_time("2020-05-16 10:00:00")},
            {{std::string("2020-05-23 10:00:00"), -1}, str_to_date_time("2020-05-30 10:00:00")},
            {{std::string(""), 1}, Null()},
            {{Null(), 1}, Null()},
            {{std::string("2020-01-07 00:00:00"), 1}, str_to_date_time("2019-12-31 00:00:00")},
            {{std::string("2020-03-07 00:00:00"), 1}, str_to_date_time("2020-02-29 00:00:00")},
            {{std::string("2020-04-04 00:00:00"), 9}, str_to_date_time("2020-02-01 00:00:00")},
            {{std::string("2020-03-01 00:00:00"), 1}, str_to_date_time("2020-02-23 00:00:00")},
            {{std::string("2021-01-03 00:00:00"), 1}, str_to_date_time("2020-12-27 00:00:00")},
            {{std::string("9999-12-31 23:59:59"), -1}, Null()},
            {{std::string("0001-01-08 00:00:00"), 1}, str_to_date_time("0001-01-01 00:00:00")},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDateTime, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, to_days_test) {
    std::string func_name = "to_days";

    BaseInputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("2021-01-01 00:00:00")}, 738156},
            {{std::string("")}, Null()},
            {{std::string("2021-01-32 00:00:00")}, Null()},
            {{std::string("2024-05-07 08:17:26")}, 739378},
            {{std::string("0000-01-01 00:00:00")}, 1},
            {{std::string("")}, Null()},
            {{std::string("2024-13-32 37:67:80")}, Null()},
            {{std::string("2020-02-29 08:17:26")}, 737849},
            {{std::string("2024-12-31 23:59:59")}, 739616},
            {{std::string("2024-01-01 00:00:00")}, 739251},
            {{std::string("0000-01-01 00:00:00")}, 1},
            {{std::string("9999-12-31 23:59:59")}, 3652424},
            {{std::string("0000-12-31 23:59:59")}, 365},
            {{std::string("9999-01-01 00:00:00")}, 3652060},
            {{std::string("10000-01-01 00:00:00")}, Null()},
            {{std::string("-0001-12-31 23:59:59")}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, date_test) {
    std::string func_name = "date";

    BaseInputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("2021-01-01 06:00:00")}, str_to_date_time("2021-01-01", false)},
            {{std::string("")}, Null()},
            {{Null()}, Null()},
            {{std::string("2024-05-07 08:17:26")}, str_to_date_time("2024-05-07", false)},
            {{std::string("0000-01-01 00:00:00")}, str_to_date_time("0000-01-01", false)},
            {{std::string("")}, Null()},
            {{std::string("2024-13-32 37:67:80")}, Null()},
            {{std::string("2020-02-29 08:17:26")}, str_to_date_time("2020-02-29", false)},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeDate, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, week_test) {
    std::string func_name = "week";

    BaseInputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("1989-03-21 06:00:00")}, int8_t {12}},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12 00:00:00")}, int8_t {50}},
            {{std::string("2024-05-07 08:17:26")}, int8_t {18}},
            {{std::string("")}, Null()},
            {{std::string("2024-13-32 37:67:80")}, Null()},
            {{std::string("2020-02-29 08:17:26")}, int8_t {8}},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt8, true>(func_name, input_types, data_set));

    BaseInputTypeSet new_input_types = {TypeIndex::Date};
    DataSet new_data_set = {
            {{std::string("1989-03-21")}, int8_t {12}},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12")}, int8_t {50}},
            {{std::string("2024-05-07")}, int8_t {18}},
            {{std::string("")}, Null()},
            {{std::string("2024-13-32")}, Null()},
            {{std::string("2020-02-29")}, int8_t {8}},
    };

    static_cast<void>(check_function_all_arg_comb<DataTypeInt8, true>(func_name, new_input_types,
                                                                      new_data_set));
}

TEST(VTimestampFunctionsTest, yearweek_test) {
    std::string func_name = "yearweek";

    InputTypeSet input_types = {TypeIndex::DateTime};

    DataSet data_set = {
            {{std::string("1989-03-21 06:00:00")}, 198912},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12 00:00:00")}, 999950},
            {{std::string("2015-04-15 12:30:45")}, std::int32_t(201515)},
            {{std::string("2020-07-01 00:00:00")}, std::int32_t(202026)},
            {{std::string("2015-01-01 00:00:00")}, std::int32_t(201452)},
            {{std::string("2015-12-31 23:59:59")}, std::int32_t(201552)},
            {{std::string("2016-02-29 08:20:00")}, std::int32_t(201609)},
            {{std::string("2021-02-11 00:00:00")}, std::int32_t(202106)},
            {{std::string("not-a-date")}, Null()},
            {{std::string("2021-13-01 00:00:00")}, Null()},
            {{std::string("2021-02-30 00:00:00")}, Null()},
            {{std::string("1989-03-21 06:00:00")}, std::int32_t(198912)},
            {{Null()}, Null()},
    };

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));

    InputTypeSet new_input_types = {TypeIndex::Date};
    DataSet new_data_set = {
            {{std::string("1989-03-21")}, 198912},
            {{std::string("")}, Null()},
            {{std::string("9999-12-12")}, 999950},
            {{std::string("1989-03-21")}, std::int32_t(198912)},
            {{std::string("2015-01-01")}, std::int32_t(201452)},
            {{std::string("2015-12-31")}, std::int32_t(201552)},
            {{std::string("2016-02-29")}, std::int32_t(201609)},
            {{std::string("2021-02-11")}, std::int32_t(202106)},
            {{std::string("not-a-date")}, Null()},
            {{std::string("2021-13-01")}, Null()},
            {{std::string("2021-02-30")}, Null()},
            {{Null()}, Null()},
            {{std::string("2020-12-15")}, std::int32_t(202050)},
            {{std::string("2021-01-01")}, std::int32_t(202052)},
            {{std::string("2021-01-31")}, std::int32_t(202105)},
            {{std::string("2020-02-30")}, Null()},
            {{std::string("2020-13-01")}, Null()},
            {{std::string("2020-02-29")}, std::int32_t(202008)},
            {{std::string("2021-02-29")}, Null()},
            {{std::string("1970-01-01")}, std::int32_t(196952)},
            {{std::string("YYYY/MM/DD")}, Null()},
            {{std::string("MM-DD-YYYY")}, Null()},
            {{std::string("9999-12-31")}, std::int32_t(999952)},
            {{std::string("0001-01-01")}, std::int32_t(53)},
            {{std::string("")}, Null()},
            {{Null()}, Null()},
            {{std::string("10000-01-01")}, Null()},
            {{std::string("0001-1-1")}, std::int32_t(53)},
    };

    static_cast<void>(
            check_function<DataTypeInt32, true>(func_name, new_input_types, new_data_set));
}

TEST(VTimestampFunctionsTest, makedate_test) {
    std::string func_name = "makedate";

    InputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32};

    DataSet data_set = {{{2021, 3}, str_to_date_time("2021-01-03", false)},
                        {{2021, 95}, str_to_date_time("2021-04-05", false)},
                        {{2021, 400}, str_to_date_time("2022-02-04", false)},
                        {{2021, 0}, Null()},
                        {{2021, -10}, Null()},
                        {{-1, 3}, Null()},
                        {{12345, 3}, Null()}};

    static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, weekday_test) {
    std::string func_name = "weekday";

    {
        InputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {
                {{std::string("2001-02-03 12:34:56")}, int8_t {5}},
                {{std::string("2019-06-25")}, int8_t {1}},
                {{std::string("2020-00-01 00:00:00")}, Null()},
                {{std::string("2020-01-00 00:00:00")}, Null()},
                {{std::string("2015-04-15 12:30:45")}, std::int8_t(2)},
                {{std::string("2020-07-01 00:00:00")}, std::int8_t(2)},
                {{std::string("2015-01-01 00:00:00")}, std::int8_t(3)},
                {{std::string("2015-12-31 23:59:59")}, std::int8_t(3)},
                {{std::string("2016-02-29 08:20:00")}, std::int8_t(0)},
                {{std::string("2021-02-11 00:00:00")}, std::int8_t(3)},
                {{std::string("not-a-date")}, Null()},
                {{std::string("2021-13-01 00:00:00")}, Null()},
                {{std::string("2021-02-30 00:00:00")}, Null()},
                {{std::string("1989-03-21 06:00:00")}, std::int8_t(1)},
                {{Null()}, Null()},
        };

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    InputTypeSet input_types = {TypeIndex::Date};

    DataSet data_set = {
            {{std::string("2001-02-03")}, int8_t {5}},
            {{std::string("2019-06-25")}, int8_t {1}},
            {{std::string("2020-00-01")}, Null()},
            {{std::string("2020-01-00")}, Null()},
            {{std::string("1989-03-21")}, std::int8_t(1)},
            {{std::string("2015-01-01")}, std::int8_t(3)},
            {{std::string("2015-12-31")}, std::int8_t(3)},
            {{std::string("2016-02-29")}, std::int8_t(0)},
            {{std::string("2021-02-11")}, std::int8_t(3)},
            {{std::string("not-a-date")}, Null()},
            {{std::string("2021-13-01")}, Null()},
            {{std::string("2021-02-30")}, Null()},
            {{Null()}, Null()},
            {{std::string("2020-12-15")}, std::int8_t(1)},
            {{std::string("2021-01-01")}, std::int8_t(4)},
            {{std::string("2021-01-31")}, std::int8_t(6)},
            {{std::string("2020-02-30")}, Null()},
            {{std::string("2020-13-01")}, Null()},
            {{std::string("2020-02-29")}, std::int8_t(5)},
            {{std::string("2021-02-29")}, Null()},
            {{std::string("1970-01-01")}, std::int8_t(3)},
            {{std::string("YYYY/MM/DD")}, Null()},
            {{std::string("MM-DD-YYYY")}, Null()},
            {{std::string("9999-12-31")}, std::int8_t(4)},
            {{std::string("0001-01-01")}, std::int8_t(0)},
            {{std::string("")}, Null()},
            {{Null()}, Null()},
            {{std::string("10000-01-01")}, Null()},
            {{std::string("0001-1-1")}, std::int8_t(0)},

    };

    static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
}

TEST(VTimestampFunctionsTest, day_of_week_v2_test) {
    std::string func_name = "dayofweek";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2001-02-03")}, int8_t {7}},
                            {{std::string("2020-00-01")}, Null()},
                            {{std::string("2020-01-00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2001-02-03 01:00:00")}, int8_t {7}},
                            {{std::string("2001-02-03 01:00:00.213")}, int8_t {7}},
                            {{std::string("2001-02-03 01:00:00.123213")}, int8_t {7}},
                            {{std::string("2001-02-03 01:00:00.123123213")}, int8_t {7}},
                            {{std::string("2001-02-03 25:00:00.123123213")}, Null()},
                            {{std::string("2001-02-03 01:61:00.123123213")}, Null()},
                            {{std::string("2001-02-03 01:00:61.123123213")}, Null()},
                            {{std::string("2020-00-01 01:00:00")}, Null()},
                            {{std::string("2020-01-00 01:00:00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_of_month_v2_test) {
    std::string func_name = "dayofmonth";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2020-00-01")}, Null()},
                {{std::string("2020-01-01")}, int8_t {1}},
                {{std::string("2020-02-29")}, int8_t {29}},
                {{std::string("2020-12-15")}, std::int8_t(15)},
                {{std::string("2021-01-01")}, std::int8_t(1)},
                {{std::string("2021-01-31")}, std::int8_t(31)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(29)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{Null()}, Null()},

        };

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {
                {{std::string("2020-00-01 01:00:00")}, Null()},
                {{std::string("2020-01-01 01:00:00")}, int8_t {1}},
                {{std::string("2020-02-29 01:00:00.123123")}, int8_t {29}},
                {{std::string("2015-04-15 12:30:45")}, std::int8_t(15)},
                {{std::string("2020-07-01 00:00:00")}, std::int8_t(1)},
                {{std::string("2015-01-01 00:00:00")}, std::int8_t(1)},
                {{std::string("2015-12-31 23:59:59")}, std::int8_t(31)},
                {{std::string("2016-02-29 08:20:00")}, std::int8_t(29)},
                {{std::string("2021-02-11 00:00:00")}, std::int8_t(11)},
                {{std::string("not-a-date")}, Null()},
                {{std::string("2021-13-01 00:00:00")}, Null()},
                {{std::string("2021-02-30 00:00:00")}, Null()},
                {{std::string("1989-03-21 06:00:00")}, std::int8_t(21)},
                {{Null()}, Null()},
        };

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_of_year_v2_test) {
    std::string func_name = "dayofyear";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2020-00-01")}, Null()},
                {{std::string("2020-01-00")}, Null()},
                {{std::string("2020-02-29")}, int16_t {60}},
                {{std::string("2020-12-15")}, std::int16_t(350)},
                {{std::string("2021-01-01")}, std::int16_t(1)},
                {{std::string("2021-01-31")}, std::int16_t(31)},
                {{std::string("2020-00-01")}, Null()},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int16_t(60)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int16_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{Null()}, Null()},
        };

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2020-00-01 01:00:00")}, Null()},
                            {{std::string("2020-01-00 01:00:00")}, Null()},
                            {{std::string("2020-02-29 01:00:00.1232")}, int16_t {60}},
                            {{std::string("2023-01-01 00:00:00")}, std::int16_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int16_t(365)},
                            {{std::string("2023-02-28 12:34:56")}, std::int16_t(59)},
                            {{std::string("2024-02-29 15:30:45")}, std::int16_t(60)},
                            {{std::string("2023-07-04 07:07:07")}, std::int16_t(185)},
                            {{std::string("2023-04-01 00:00:01")}, std::int16_t(91)},
                            {{std::string("2023-11-30 23:59:00")}, std::int16_t(334)},
                            {{std::string("2023-04-15 12:00:00")}, std::int16_t(105)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int16_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int16_t(365)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int16_t(60)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, week_of_year_v2_test) {
    std::string func_name = "weekofyear";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2020-00-01")}, Null()},
                            {{std::string("2020-01-00")}, Null()},
                            {{std::string("2020-02-29")}, int8_t {9}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2020-00-01 01:00:00")}, Null()},
                            {{std::string("2020-01-00 01:00:00")}, Null()},
                            {{std::string("2020-02-29 01:00:00")}, int8_t {9}},
                            {{std::string("2020-02-29 01:00:00.12312")}, int8_t {9}},
                            {{std::string("2020-00-01 00:00:00")}, Null()},
                            {{std::string("2020-01-00 00:00:00")}, Null()},
                            {{std::string("2020-02-29 00:00:00")}, int8_t {9}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(52)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(52)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(9)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(9)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(27)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(13)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(48)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(15)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(52)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(9)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, year_v2_test) {
    std::string func_name = "year";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int16_t {2021}},
                            {{std::string("2021-01-00")}, Null()},
                            {{std::string("2025-05-01")}, int16_t {2025}}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 01:00:00")}, int16_t {2021}},
                            {{std::string("2021-01-00 01:00:00")}, Null()},
                            {{std::string("2025-05-01 01:00:00.123")}, int16_t {2025}},
                            {{std::string("2021-01-01 00:00:00")}, int16_t {2021}},
                            {{std::string("2021-01-00 00:00:00")}, Null()},
                            {{std::string("2025-05-01 00:00:00")}, int16_t {2025}},
                            {{std::string("2023-01-01 00:00:00")}, std::int16_t(2023)},
                            {{std::string("2023-12-31 23:59:59")}, std::int16_t(2023)},
                            {{std::string("2023-02-28 12:34:56")}, std::int16_t(2023)},
                            {{std::string("2024-02-29 15:30:45")}, std::int16_t(2024)},
                            {{std::string("2023-07-04 07:07:07")}, std::int16_t(2023)},
                            {{std::string("2023-04-01 00:00:01")}, std::int16_t(2023)},
                            {{std::string("2023-11-30 23:59:00")}, std::int16_t(2023)},
                            {{std::string("2023-04-15 12:00:00")}, std::int16_t(2023)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int16_t(1000)},
                            {{std::string("9999-12-31 23:59:59")}, std::int16_t(9999)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int16_t(1904)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt16, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, quarter_v2_test) {
    std::string func_name = "quarter";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32")}, Null()},
                            {{std::string("2025-10-23")}, int8_t {4}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-10-23 00:00:00")}, int8_t {4}},
                            {{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-10-23 00:00:00")}, int8_t {4}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(4)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(1)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(1)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(3)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(2)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(4)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(2)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(4)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(1)},
                            {{std::string("1900-02-29 12:34:56.123")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, month_v2_test) {
    std::string func_name = "month";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32")}, Null()},
                            {{std::string("2025-05-23")}, int8_t {5}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {5}},
                            {{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {5}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(12)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(2)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(2)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(4)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(11)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(4)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(12)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(2)},
                            {{std::string("1900-02-29 12:34:56.789")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, day_v2_test) {
    std::string func_name = "day";
    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2021-01-01")}, int8_t {1}},
                {{std::string("")}, Null()},
                {{std::string("2021-01-32")}, Null()},
                {{std::string("2025-05-23")}, int8_t {23}},
                {{std::string("2020-12-15")}, std::int8_t(15)},
                {{std::string("2021-01-01")}, std::int8_t(1)},
                {{std::string("2021-01-31")}, std::int8_t(31)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(29)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(1)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(31)},
                {{std::string("0001-01-01")}, std::int8_t(1)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(1)},
        };

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {23}},
                            {{std::string("2021-01-01 00:00:00")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:00")}, Null()},
                            {{std::string("2025-05-23 00:00:00")}, int8_t {23}},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(31)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(28)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(29)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(4)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(1)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(30)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(15)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(1)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(31)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(29)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, hour_v2_test) {
    std::string func_name = "hour";
    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2021-01-01")}, int8_t {0}},
                {{std::string("2021-01-13")}, int8_t {0}},
                {{std::string("")}, Null()},
                {{std::string("2025-05-23")}, int8_t {0}},
                {{std::string("2020-12-15")}, std::int8_t(0)},
                {{std::string("2021-01-01")}, std::int8_t(0)},
                {{std::string("2021-01-31")}, std::int8_t(0)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(0)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(0)},
                {{std::string("0001-01-01")}, std::int8_t(0)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(0)},
        };
        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00.123")}, int8_t {0}},
                            {{std::string("2021-01-13 01:00:00.123")}, int8_t {1}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23 23:00:00.123")}, int8_t {23}},
                            {{std::string("2025-05-23 25:00:00.123")}, Null()},
                            {{std::string("2021-01-01 23:59:59")}, int8_t {23}},
                            {{std::string("2021-01-13 16:56:00")}, int8_t {16}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23 24:00:00")}, Null()},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(0)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(23)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(12)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(15)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(0)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(23)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(12)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(0)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(23)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(12)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};
        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, minute_v2_test) {
    std::string func_name = "minute";
    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {0}},
                            {{std::string("2021-01-13")}, int8_t {0}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23")}, int8_t {0}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:00.123")}, int8_t {0}},
                            {{std::string("2021-01-13 00:11:00.123")}, int8_t {11}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23 00:22:22.123")}, int8_t {22}},
                            {{std::string("2025-05-23 00:60:22.123")}, Null()},
                            {{std::string("2021-01-01 23:59:50")}, int8_t {59}},
                            {{std::string("2021-01-13 16:20:00")}, int8_t {20}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23 24:00:00")}, Null()},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(0)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(59)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(34)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(30)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(0)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(59)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(0)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(0)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(59)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(34)},
                            {{std::string("1900-02-29 12:34:56")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, second_v2_test) {
    std::string func_name = "second";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2021-01-01")}, int8_t {0}},
                            {{std::string("2021-01-13")}, int8_t {0}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23")}, int8_t {0}}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:01.123")}, int8_t {1}},
                            {{std::string("2021-01-13 00:00:02.123")}, int8_t {2}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23 00:00:63.123")}, Null()},
                            {{std::string("2025-05-23 00:00:00.123")}, int8_t {0}},
                            {{std::string("2021-01-01 23:50:59")}, int8_t {59}},
                            {{std::string("2021-01-13 16:20:00")}, int8_t {0}},
                            {{std::string("")}, Null()},
                            {{std::string("2025-05-23 24:00:00")}, Null()},
                            {{std::string("2023-01-01 00:00:00")}, std::int8_t(0)},
                            {{std::string("2023-12-31 23:59:59")}, std::int8_t(59)},
                            {{std::string("2023-02-28 12:34:56")}, std::int8_t(56)},
                            {{std::string("2024-02-29 15:30:45")}, std::int8_t(45)},
                            {{std::string("2023-07-04 07:07:07")}, std::int8_t(7)},
                            {{std::string("2023-04-01 00:00:01")}, std::int8_t(1)},
                            {{std::string("2023-11-30 23:59:00")}, std::int8_t(0)},
                            {{std::string("2023-04-15 12:00:00")}, std::int8_t(0)},
                            {{std::string("2023-02-30 12:00:00")}, Null()},
                            {{std::string("2023-01-01 25:00:00")}, Null()},
                            {{std::string("1000-01-01 00:00:00")}, std::int8_t(0)},
                            {{std::string("9999-12-31 23:59:59")}, std::int8_t(59)},
                            {{std::string("2023-01-01 24:00:00")}, Null()},
                            {{std::string("2023-12-31 00:60:00")}, Null()},
                            {{std::string("2023-12-31 00:00:60")}, Null()},
                            {{std::string("1904-02-29 12:34:56")}, std::int8_t(56)},
                            {{std::string("1900-02-29 12:34:56.789")}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, timediff_v2_test) {
    std::string func_name = "timediff";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::DateV2};

        DataSet data_set = {{{std::string("2019-07-18"), std::string("2019-07-18")}, 0.0},
                            {{std::string("2019-07-18"), std::string("2019-07-18")}, -0.0},
                            {{std::string("2019-00-18"), std::string("2019-07-18")}, Null()},
                            {{std::string("2019-07-18"), std::string("2019-07-00")}, Null()}};

        static_cast<void>(check_function<DataTypeTimeV2, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::DateTimeV2};

        DataSet data_set = {
                {{std::string("2019-07-18 00:00:00"), std::string("2019-07-18 00:00:00")}, 0.0},
                {{std::string("2019-07-18 00:00:10"), std::string("2019-07-18 00:00:00")},
                 10000000.0},
                {{std::string("2019-00-18 00:00:00"), std::string("2019-07-18 00:00:00")}, Null()},
                {{std::string("2019-07-18 00:00:00"), std::string("2019-07-00 00:00:00")}, Null()},
                {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 12:00:00")}, 0.0},
                {{std::string("2019-07-18 12:00:00"), std::string("2019-07-18 13:01:02")},
                 (double)-3662.0 * 1e6},
                {{std::string("2019-00-18 12:00:00"), std::string("2019-07-18 13:01:02")}, Null()},
                {{std::string("2019-07-18 12:00:00"), std::string("2019-07-00 13:01:02")}, Null()},
                {{std::string("2023-05-07 10:00:00"), std::string("2023-05-07 09:00:00")},
                 (double)3600.0 * 1e6},
                {{std::string("2023-05-07 00:00:00.000"), std::string("2023-05-08 00:00:00.000")},
                 (double)-86400.0 * 1e6},
                {{std::string("2023-05-01 15:30:00"), std::string("2023-05-07 15:30:00")},
                 (double)-518400.0 * 1e6},
                {{std::string("2019-02-28 23:59:59"), std::string("2019-03-01 00:00:00")},
                 (double)-1.0 * 1e6},
                {{std::string("not-a-datetime"), std::string("2023-05-07 09:00:00")}, Null()},
                {{std::string("2023-05-07 09:00:00"), std::string("not-a-datetime")}, Null()},
                {{std::string("2023-05-07 09:00:00"), Null()}, Null()},
        };

        static_cast<void>(check_function<DataTypeTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, datediff_v2_test) {
    std::string func_name = "datediff";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2019-07-18"), std::string("2019-07-19")}, -1},
                {{std::string("2019-07-18"), std::string("2019-07-17")}, 1},
                {{std::string("2019-00-18"), std::string("2019-07-18")}, Null()},
                {{std::string("2019-07-18"), std::string("2019-07-00")}, Null()}, // 测试相同日期
                {{std::string("2019-07-18"), std::string("2019-07-18")}, 0},
                // 测试隔天
                {{std::string("2019-07-18"), std::string("2019-07-19")}, -1},
                {{std::string("2019-07-19"), std::string("2019-07-18")}, 1},
                // 测试日期为月初和月末情况
                {{std::string("2019-07-01"), std::string("2019-07-31")}, -30},
                {{std::string("2019-07-31"), std::string("2019-07-01")}, 30},
                // 测试跨年份
                {{std::string("2019-12-31"), std::string("2020-01-01")}, -1},
                {{std::string("2020-01-01"), std::string("2019-12-31")}, 1},
                // 测试含非法日期（如月或日为0），预期返回空值
                {{std::string("2019-00-18"), std::string("2019-07-18")}, Null()},
                {{std::string("2019-07-18"), std::string("2019-07-00")}, Null()},
                // 测试闰年情况
                {{std::string("2020-02-28"), std::string("2020-03-01")}, -2},
                {{std::string("2020-03-01"), std::string("2020-02-28")}, 2},
                // 测试较大的日期跨度
                {{std::string("2000-01-01"), std::string("2020-01-01")}, -7305},
                {{std::string("2020-01-01"), std::string("2000-01-01")}, 7305},
        };

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2019-07-18 00:00:00.123"), std::string("2019-07-19")}, -1},
                {{std::string("2019-07-18 00:00:00.123"), std::string("2019-07-17")}, 1},
                {{std::string("2019-00-18 00:00:00.123"), std::string("2019-07-18")}, Null()},
                {{std::string("2019-07-18 00:00:00.123"), std::string("2019-07-00")}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::DateTimeV2};

        DataSet data_set = {
                // 测试日期和时间，预期返回日期差异，不考虑时间差
                {{std::string("2019-07-18 23:59:59.999"), std::string("2019-07-19 00:00:00.000")},
                 -1},
                {{std::string("2019-07-18 00:00:00.000"), std::string("2019-07-17 23:59:59.999")},
                 1},
                // 测试跨越多日的情况，预期返回正确的日差
                {{std::string("2019-07-01 00:00:00.123"), std::string("2019-07-31 23:59:59.876")},
                 -30},
                {{std::string("2019-07-31 23:59:59.876"), std::string("2019-07-01 00:00:00.123")},
                 30},
                // 测试跨年份的情况
                {{std::string("2019-12-31 23:59:59"), std::string("2020-01-01 00:00:00")}, -1},
                {{std::string("2020-01-01 00:00:00"), std::string("2019-12-31 23:59:59")}, 1},
                // 测试含有非法日期时间
                {{std::string("2019-00-18 00:00:00"), std::string("2019-07-18 12:00:00")}, Null()},
                {{std::string("2019-07-18 12:00:00"), std::string("2019-07-00 00:00:00")}, Null()},
                // 测试闰年情况
                {{std::string("2020-02-28 00:00:00"), std::string("2020-03-01 23:59:59")}, -2},
                {{std::string("2020-03-01 23:59:59"), std::string("2020-02-28 00:00:00")}, 2},
                // 测试较大的日期时间跨度
                {{std::string("2000-01-01 00:00:00"), std::string("2020-01-01 23:59:59")}, -7305},
                {{std::string("2020-01-01 23:59:59"), std::string("2000-01-01 00:00:00")}, 7305},
        };

        check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
    }
}

TEST(VTimestampFunctionsTest, date_format_v2_test) {
    std::string func_name = "date_format";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2009-10-04"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("2007-10-04"), std::string("%H:%i:%s")}, std::string("00:00:00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("1900-10-04"), std::string("%D %y %a %d %m %b %j")},
                             std::string("4th 00 Thu 04 10 Oct 277")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2009-10-04 22:23:00"), std::string("%W %M %Y")},
                             std::string("Sunday October 2009")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2007-10-04 22:23:00"), std::string("%H:%i:%s")},
                             std::string("22:23:00")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("1900-10-04 22:23:00"), std::string("%D %y %a %d %m %b %j")},
                 std::string("4th 00 Thu 04 10 Oct 277")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("1997-10-04 22:23:00"), std::string("%H %k %I %r %T %S %w")},
                 std::string("22 22 10 10:23:00 PM 22:23:00 00 6")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("1999-01-01 00:00:00"), std::string("%X %V")},
                             std::string("1998 52")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%d")}, std::string("01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("2006-06-01 00:00:00"), std::string("%%%d")}, std::string("%01")}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, years_add_v2_test) {
    std::string func_name = "years_add";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23"), 5}, str_to_date_v2("2025-05-23", "%Y-%m-%d")},
                {{std::string("2020-05-23"), -5}, str_to_date_v2("2015-05-23", "%Y-%m-%d")},
                {{std::string(""), 5}, Null()},
                {{std::string("2020-05-23"), 8000}, Null()},
                {{Null(), 5}, Null()},
                // 测试闰年加上一定年份，是否返回正确的日期
                {{std::string("2020-02-29"), 4}, str_to_date_v2("2024-02-29", "%Y-%m-%d")},
                // 测试闰年加上非倍数的年份，日期应当调整到2月28日
                {{std::string("2020-02-29"), 1}, str_to_date_v2("2021-02-28", "%Y-%m-%d")},
                // 测试年底日期加上一定年份，是否正确
                {{std::string("2019-12-31"), 1}, str_to_date_v2("2020-12-31", "%Y-%m-%d")},
                // 测试日期加上0年，应当返回同一日期
                {{std::string("2020-05-23"), 0}, str_to_date_v2("2020-05-23", "%Y-%m-%d")},
                // 测试负数年份，相当于日期减法
                {{std::string("2025-05-23"), -10}, str_to_date_v2("2015-05-23", "%Y-%m-%d")},
                // 测试非法日期加上一定年份，应当返回Null
                {{std::string("2020-00-23"), 5}, Null()},
                // 测试日期加上一个非常大的负数年份，超出范围后应当返回Null
                {{std::string("2021-01-10"), -5000}, Null()},
                // 测试日期减去一个大于其年份数的整数
                {{std::string("2021-01-10"), -2022}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-05-23 00:00:11.123"), 5},
                             str_to_datetime_v2("2025-05-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 00:00:11.123"), -5},
                             str_to_datetime_v2("2015-05-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 5}, Null()},
                            {{std::string("2020-05-23 00:00:11.123"), 8000}, Null()},
                            {{Null(), 5}, Null()},
                            {{std::string("2020-05-23 00:00:00"), 5},
                             str_to_datetime_v2("2025-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 00:00:00"), -5},
                             str_to_datetime_v2("2015-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 5}, Null()},
                            {{std::string("2020-05-23 00:00:00"), 8000}, Null()},
                            {{Null(), 5}, Null()},
                            {{std::string("2023-01-01 00:00:00"), 1},
                             str_to_datetime_v2("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2023-01-01 00:00:00"), -1},
                             str_to_datetime_v2("2022-01-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-02-29 00:00:00"), 1},
                             str_to_datetime_v2("2021-02-28 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("9999-12-31 23:59:59"), 1}, Null()},
                            {{std::string("0000-01-01 00:00:00"), -1}, Null()},
                            {{std::string("2020-05-23 00:00:00"), 0},
                             str_to_datetime_v2("2020-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("not-a-valid-date"), 5}, Null()},
                            {{Null(), 1}, Null()},
                            {{std::string("2020-05-23 00:00:00"), 100},
                             str_to_datetime_v2("2120-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 00:00:00"), -100},
                             str_to_datetime_v2("1920-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, years_sub_v2_test) {
    std::string func_name = "years_sub";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23"), 5}, str_to_date_v2("2015-05-23", "%Y-%m-%d")},
                {{std::string("2020-05-23"), -5}, str_to_date_v2("2025-05-23", "%Y-%m-%d")},
                {{std::string(""), 5}, Null()},
                {{std::string("2020-05-23"), 3000}, Null()},
                {{Null(), 5}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23 00:00:11.123"), 5},
                 str_to_datetime_v2("2015-05-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:11.123"), -5},
                 str_to_datetime_v2("2025-05-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 5}, Null()},
                {{std::string("2020-05-23 00:00:11.123"), 3000}, Null()},
                {{Null(), 5}, Null()},
                {{std::string("2020-05-23 00:00:00"), 5},
                 str_to_datetime_v2("2015-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:00"), -5},
                 str_to_datetime_v2("2025-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 5}, Null()},
                {{std::string("2020-05-23 00:00:00"), 3000}, Null()},
                {{Null(), 5}, Null()},
                {{std::string("2025-05-23 00:00:00"), 5},
                 str_to_datetime_v2("2020-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2015-05-23 00:00:00"), -5},
                 str_to_datetime_v2("2020-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 5}, Null()},
                {{std::string("9999-12-31 23:59:59"), -8000}, Null()},
                {{std::string("2023-01-01 00:00:00"), -1},
                 str_to_datetime_v2("2024-01-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2023-01-01 00:00:00"), 1},
                 str_to_datetime_v2("2022-01-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2021-02-28 00:00:00"), 1},
                 str_to_datetime_v2("2020-02-28 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("0001-01-01 00:00:00"), 1},
                 str_to_datetime_v2("0000-01-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:00"), 0},
                 str_to_datetime_v2("2020-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("not-a-valid-date"), 5}, Null()},
                {{std::string("2120-05-23 00:00:00"), 100},
                 str_to_datetime_v2("2020-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("1920-05-23 00:00:00"), -100},
                 str_to_datetime_v2("2020-05-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2024-02-29 00:00:00"), 1},
                 str_to_datetime_v2("2023-02-28 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2024-02-29 00:00:00"), 4},
                 str_to_datetime_v2("2020-02-29 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2048-02-29 00:00:00"), 3},
                 str_to_datetime_v2("2045-02-28 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
        };

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, months_add_v2_test) {
    std::string func_name = "months_add";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-10-23"), -4}, str_to_date_v2("2020-06-23", "%Y-%m-%d")},
                {{std::string("2020-05-23"), 4}, str_to_date_v2("2020-09-23", "%Y-%m-%d")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23"), 10}, str_to_date_v2("2021-03-23", "%Y-%m-%d")},
                {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-10-23 00:00:11.123"), -4},
                             str_to_datetime_v2("2020-06-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 00:00:11.123"), 4},
                             str_to_datetime_v2("2020-09-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 00:00:11.123"), 10},
                             str_to_datetime_v2("2021-03-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()},
                            {{std::string("2020-10-23 00:00:00"), -4},
                             str_to_datetime_v2("2020-06-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 00:00:00"), 4},
                             str_to_datetime_v2("2020-09-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 00:00:00"), 10},
                             str_to_datetime_v2("2021-03-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2021-01-31"), 1},
                             str_to_datetime_v2("2021-02-28 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2021-02-30 00:00:00"), 1}, Null()},
                            {{std::string("1970-01-01 00:00:00"), 600},
                             str_to_datetime_v2("2020-01-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("9999-12-31 00:00:00"), 1}, Null()},
                            {{std::string("2020-12-31 00:00:00"), -2400},
                             str_to_datetime_v2("1820-12-31 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2021-05-31 00:00:00"), 25},
                             str_to_datetime_v2("2023-06-30 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2021-05-3a 00:00:00"), 1}, Null()},
                            {{std::string("0000-01-01 :00:00:00"), -1}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, months_sub_v2_test) {
    std::string func_name = "months_sub";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23"), 4}, str_to_date_v2("2020-01-23", "%Y-%m-%d")},
                {{std::string("2020-05-23"), -4}, str_to_date_v2("2020-09-23", "%Y-%m-%d")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23"), 10}, str_to_date_v2("2019-07-23", "%Y-%m-%d")},
                {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23 00:00:11.123"), 4},
                 str_to_datetime_v2("2020-01-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:11.123"), -4},
                 str_to_datetime_v2("2020-09-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:11.123"), 10},
                 str_to_datetime_v2("2019-07-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-05-23 00:00:00"), 4},
                 str_to_datetime_v2("2020-01-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:00"), -4},
                 str_to_datetime_v2("2020-09-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:00"), 10},
                 str_to_datetime_v2("2019-07-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2023-12-15 00:00:00"), 3},
                 str_to_datetime_v2("2023-09-15 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2023-03-31 00:00:00"), 1},
                 str_to_datetime_v2("2023-02-28 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-01-31 00:00:00"), 12},
                 str_to_datetime_v2("2019-01-31 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2023-01-31 00:00:00"), -2},
                 str_to_datetime_v2("2023-03-31 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("0001-01-01 00:00:00"), 1},
                 str_to_datetime_v2("0000-12-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
        };

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, days_add_v2_test) {
    std::string func_name = "days_add";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-10-23"), -4}, str_to_date_v2("2020-10-19", "%Y-%m-%d")},
                {{std::string("2020-05-23"), 4}, str_to_date_v2("2020-05-27", "%Y-%m-%d")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23"), 10}, str_to_date_v2("2020-06-02", "%Y-%m-%d")},
                {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-10-23 00:00:11.123"), -4},
                 str_to_datetime_v2("2020-10-19 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:11.123"), 4},
                 str_to_datetime_v2("2020-05-27 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:11.123"), 10},
                 str_to_datetime_v2("2020-06-02 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-10-23 00:00:00"), -4},
                 str_to_datetime_v2("2020-10-19 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:00"), 4},
                 str_to_datetime_v2("2020-05-27 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:00"), 10},
                 str_to_datetime_v2("2020-06-2 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-10-23 00:00:00"), 10},
                 str_to_datetime_v2("2020-11-02 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-10-23 00:00:00"), -10},
                 str_to_datetime_v2("2020-10-13 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 10}, Null()},
                {{std::string("2020-13-23 00:00:00"), 5}, Null()},
                {{Null(), 10}, Null()},
                {{std::string("2020-12-28 00:00:00"), 5},
                 str_to_datetime_v2("2021-01-02 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-01-05 00:00:00"), -10},
                 str_to_datetime_v2("2019-12-26 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
        };

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, days_sub_v2_test) {
    std::string func_name = "days_sub";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23"), 4}, str_to_date_v2("2020-05-19", "%Y-%m-%d")},
                {{std::string("2020-05-23"), -4}, str_to_date_v2("2020-05-27", "%Y-%m-%d")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23"), 31}, str_to_date_v2("2020-04-22", "%Y-%m-%d")},
                {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23 00:00:11.123"), 4},
                 str_to_datetime_v2("2020-05-19 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:11.123"), -4},
                 str_to_datetime_v2("2020-05-27 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:11.123"), 31},
                 str_to_datetime_v2("2020-04-22 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-05-23 00:00:00"), 4},
                 str_to_datetime_v2("2020-05-19 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:00"), -4},
                 str_to_datetime_v2("2020-05-27 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:00"), 31},
                 str_to_datetime_v2("2020-04-22 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-10-23 00:00:00"), 10},
                 str_to_datetime_v2("2020-10-13 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-10-23 00:00:00"), -10},
                 str_to_datetime_v2("2020-11-02 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 10}, Null()},
                {{std::string("2020-13-23 00:00:00"), 5}, Null()},
                {{Null(), 10}, Null()},
                {{std::string("2020-01-05 00:00:00"), 10},
                 str_to_datetime_v2("2019-12-26 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-12-28 00:00:00"), -5},
                 str_to_datetime_v2("2021-01-02 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-02-29 00:00:00"), -1},
                 str_to_datetime_v2("2020-03-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2019-02-28 00:00:00"), -1},
                 str_to_datetime_v2("2019-03-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("9999-12-31 00:00:00"), -1}, Null()},
                {{std::string("0000-01-01 00:00:00"), 1}, Null()},
                {{std::string("2020-03-01 00:00:00"), 1},
                 str_to_datetime_v2("2020-02-29 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-03-01 00:00:00"), 1},
                 str_to_datetime_v2("2020-02-29 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
        };

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, weeks_add_v2_test) {
    std::string func_name = "weeks_add";

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-10-23"), 5}, str_to_date_v2("2020-11-27", "%Y-%m-%d")},
                {{std::string("2020-05-23"), -5}, str_to_date_v2("2020-04-18", "%Y-%m-%d")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23"), 100}, str_to_date_v2("2022-04-23", "%Y-%m-%d")},
                {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-10-23 00:00:11.123"), 5},
                 str_to_datetime_v2("2020-11-27 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:11.123"), -5},
                 str_to_datetime_v2("2020-04-18 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:11.123"), 100},
                 str_to_datetime_v2("2022-04-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-10-23 10:00:00"), 5},
                 str_to_datetime_v2("2020-11-27 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 10:00:00"), -5},
                 str_to_datetime_v2("2020-04-18 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 10:00:00"), 100},
                 str_to_datetime_v2("2022-04-23 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-05-23 10:00:00"), 1},
                 str_to_datetime_v2("2020-05-30 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 10:00:00"), -1},
                 str_to_datetime_v2("2020-05-16 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 1}, Null()},
                {{Null(), 1}, Null()},
                {{std::string("2020-12-24 10:00:00"), 2},
                 str_to_datetime_v2("2021-01-07 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-02-20 10:00:00"), 1},
                 str_to_datetime_v2("2020-02-27 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-02-29 10:00:00"), 1},
                 str_to_datetime_v2("2020-03-07 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-02-01 10:00:00"), 4},
                 str_to_datetime_v2("2020-02-29 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-02-01 10:00:00"), 9},
                 str_to_datetime_v2("2020-04-04 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("9999-12-31 23:59:59"), 1}, Null()},
                {{std::string("0000-01-01 00:00:00"), 1},
                 str_to_datetime_v2("0000-01-08 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
        };

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, weeks_sub_v2_test) {
    std::string func_name = "weeks_sub";
    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23"), 5}, str_to_date_v2("2020-04-18", "%Y-%m-%d")},
                {{std::string("2020-05-23"), -5}, str_to_date_v2("2020-06-27", "%Y-%m-%d")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23"), 100}, str_to_date_v2("2018-06-23", "%Y-%m-%d")},
                {{Null(), 4}, Null()}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("2020-05-23 00:00:11.123"), 5},
                 str_to_datetime_v2("2020-04-18 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 00:00:11.123"), -5},
                 str_to_datetime_v2("2020-06-27 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 00:00:11.123"), 100},
                 str_to_datetime_v2("2018-06-23 00:00:11.123", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-05-23 10:00:00"), 5},
                 str_to_datetime_v2("2020-04-18 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 10:00:00"), -5},
                 str_to_datetime_v2("2020-6-27 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 4}, Null()},
                {{std::string("2020-05-23 10:00:00"), 100},
                 str_to_datetime_v2("2018-06-23 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{Null(), 4}, Null()},
                {{std::string("2020-05-23 10:00:00"), 1},
                 str_to_datetime_v2("2020-05-16 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-05-23 10:00:00"), -1},
                 str_to_datetime_v2("2020-05-30 10:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string(""), 1}, Null()},
                {{Null(), 1}, Null()},
                {{std::string("2020-01-07 00:00:00"), 1},
                 str_to_datetime_v2("2019-12-31 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-03-07 00:00:00"), 1},
                 str_to_datetime_v2("2020-02-29 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-04-04 00:00:00"), 9},
                 str_to_datetime_v2("2020-02-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2020-03-01 00:00:00"), 1},
                 str_to_datetime_v2("2020-02-23 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("2021-01-03 00:00:00"), 1},
                 str_to_datetime_v2("2020-12-27 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                {{std::string("9999-12-31 23:59:59"), -1}, Null()},
                {{std::string("0001-01-08 00:00:00"), 1},
                 str_to_datetime_v2("0001-01-01 00:00:00", "%Y-%m-%d %H:%i:%s.%f")},
        };

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, to_days_v2_test) {
    std::string func_name = "to_days";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2021-01-01")}, 738156},
                {{std::string("")}, Null()},
                {{std::string("2021-01-32")}, Null()},
                {{std::string("0000-01-01")}, 1},
                {{std::string("2021-01-01 00:00:00")}, 738156},
                {{std::string("")}, Null()},
                {{std::string("2021-01-32 00:00:00")}, Null()},
                {{std::string("2024-05-07 08:17:26")}, 739378},
                {{std::string("0000-01-01 00:00:00")}, 1},
                {{std::string("")}, Null()},
                {{std::string("2024-13-32 37:67:80")}, Null()},
                {{std::string("2020-02-29 08:17:26")}, 737849},
                {{std::string("2024-12-31 23:59:59")}, 739616},
                {{std::string("2024-01-01 00:00:00")}, 739251},
                {{std::string("0000-01-01 00:00:00")}, 1},
                {{std::string("9999-12-31 23:59:59")}, 3652424},
                {{std::string("0000-12-31 23:59:59")}, 365},
                {{std::string("9999-01-01 00:00:00")}, 3652060},
                {{std::string("10000-01-01 00:00:00")}, Null()},
                {{std::string("-0001-12-31 23:59:59")}, Null()},
        };
        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:11.123")}, 738156},
                            {{std::string("")}, Null()},
                            {{std::string("2021-01-32 00:00:11.123")}, Null()},
                            {{std::string("0000-01-01 00:00:11.123")}, 1}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, date_v2_test) {
    std::string func_name = "date";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2021-01-01")}, str_to_date_v2("2021-01-01", "%Y-%m-%d")},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("0000-01-01")}, str_to_date_v2("0000-01-01", "%Y-%m-%d")}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2021-01-01 00:00:11.123")},
                             str_to_date_v2("2021-01-01", "%Y-%m-%d")},
                            {{std::string("")}, Null()},
                            {{Null()}, Null()},
                            {{std::string("0000-01-01 00:00:11.123")},
                             str_to_date_v2("0000-01-01", "%Y-%m-%d")}};

        static_cast<void>(check_function<DataTypeDateV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, week_v2_test) {
    std::string func_name = "week";

    {
        InputTypeSet new_input_types = {TypeIndex::DateV2};
        DataSet new_data_set = {
                {{std::string("1989-03-21")}, int8_t {12}},
                {{std::string("")}, Null()},
                {{std::string("9999-12-12")}, int8_t {50}},
                {{std::string("2020-12-15")}, std::int8_t(50)},
                {{std::string("2021-01-01")}, std::int8_t(0)},
                {{std::string("2021-01-31")}, std::int8_t(5)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(8)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(0)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(52)},
                {{std::string("0001-01-01")}, std::int8_t(0)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(0)},
        };

        static_cast<void>(
                check_function<DataTypeInt8, true>(func_name, new_input_types, new_data_set));
    }
    {
        InputTypeSet new_input_types = {TypeIndex::DateTimeV2};
        DataSet new_data_set = {{{std::string("1989-03-21 00:00:11.123")}, int8_t {12}},
                                {{std::string("")}, Null()},
                                {{std::string("9999-12-12 00:00:11.123")}, int8_t {50}}};

        static_cast<void>(
                check_function<DataTypeInt8, true>(func_name, new_input_types, new_data_set));
    }
}

TEST(VTimestampFunctionsTest, yearweek_v2_test) {
    std::string func_name = "yearweek";

    {
        InputTypeSet new_input_types = {TypeIndex::DateV2};
        DataSet new_data_set = {{{std::string("1989-03-21")}, 198912},
                                {{std::string("")}, Null()},
                                {{std::string("9999-12-12")}, 999950}};

        static_cast<void>(
                check_function<DataTypeInt32, true>(func_name, new_input_types, new_data_set));
    }
    {
        InputTypeSet new_input_types = {TypeIndex::DateTimeV2};
        DataSet new_data_set = {
                {{std::string("1989-03-21 00:00:11.123")}, 198912},
                {{std::string("")}, Null()},
                {{std::string("9999-12-12 00:00:11.123")}, 999950},
                {{std::string("1989-03-21 06:00:00")}, 198912},
                {{std::string("")}, Null()},
                {{std::string("9999-12-12 00:00:00")}, 999950},
                {{std::string("2015-04-15 12:30:45")}, std::int32_t(201515)},
                {{std::string("2020-07-01 00:00:00")}, std::int32_t(202026)},
                {{std::string("2015-01-01 00:00:00")}, std::int32_t(201452)},
                {{std::string("2015-12-31 23:59:59")}, std::int32_t(201552)},
                {{std::string("2016-02-29 08:20:00")}, std::int32_t(201609)},
                {{std::string("2021-02-11 00:00:00")}, std::int32_t(202106)},
                {{std::string("not-a-date")}, Null()},
                {{std::string("2021-13-01 00:00:00")}, Null()},
                {{std::string("2021-02-30 00:00:00")}, Null()},
                {{std::string("1989-03-21 06:00:00")}, std::int32_t(198912)},
                {{Null()}, Null()},
        };

        static_cast<void>(
                check_function<DataTypeInt32, true>(func_name, new_input_types, new_data_set));
    }
}

TEST(VTimestampFunctionsTest, from_days_test) {
    std::string func_name = "from_days";

    InputTypeSet input_types = {TypeIndex::Int32};

    {
        DataSet data_set = {{{730669}, str_to_date_time("2000-07-03", false)},
                            {{0}, Null()},
                            {{Null()}, Null()}};

        static_cast<void>(check_function<DataTypeDate, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, weekday_v2_test) {
    std::string func_name = "weekday";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2001-02-03")}, int8_t {5}},
                {{std::string("2019-06-25")}, int8_t {1}},
                {{std::string("2020-00-01")}, Null()},
                {{std::string("2020-01-00")}, Null()},
                {{std::string("2020-12-15")}, std::int8_t(1)},
                {{std::string("2021-01-01")}, std::int8_t(4)},
                {{std::string("2021-01-31")}, std::int8_t(6)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::int8_t(5)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::int8_t(3)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::int8_t(4)},
                {{std::string("0001-01-01")}, std::int8_t(0)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::int8_t(0)},
        };

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {
                {{std::string("2001-02-03 00:00:11.123")}, int8_t {5}},
                {{std::string("2019-06-25 00:00:11.123")}, int8_t {1}},
                {{std::string("2020-00-01 00:00:11.123")}, Null()},
                {{std::string("2020-01-00 00:00:11.123")}, Null()},
                {{std::string("2001-02-03 12:34:56")}, int8_t {5}},
                {{std::string("2019-06-25")}, int8_t {1}},
                {{std::string("2020-00-01 00:00:00")}, Null()},
                {{std::string("2020-01-00 00:00:00")}, Null()},
                {{std::string("2015-04-15 12:30:45")}, std::int8_t(2)},
                {{std::string("2020-07-01 00:00:00")}, std::int8_t(2)},
                {{std::string("2015-01-01 00:00:00")}, std::int8_t(3)},
                {{std::string("2015-12-31 23:59:59")}, std::int8_t(3)},
                {{std::string("2016-02-29 08:20:00")}, std::int8_t(0)},
                {{std::string("2021-02-11 00:00:00")}, std::int8_t(3)},
                {{std::string("not-a-date")}, Null()},
                {{std::string("2021-13-01 00:00:00")}, Null()},
                {{std::string("2021-02-30 00:00:00")}, Null()},
                {{std::string("1989-03-21 06:00:00")}, std::int8_t(1)},
                {{Null()}, Null()},
        };

        static_cast<void>(check_function<DataTypeInt8, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, dayname_test) {
    std::string func_name = "dayname";

    {
        InputTypeSet input_types = {TypeIndex::DateV2};

        DataSet data_set = {
                {{std::string("2007-02-03")}, std::string("Saturday")},
                {{std::string("2020-01-00")}, Null()},
                {{std::string("2020-12-15")}, std::string("Tuesday")},
                {{std::string("2021-01-01")}, std::string("Friday")},
                {{std::string("2021-01-31")}, std::string("Sunday")},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::string("Saturday")},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::string("Thursday")},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::string("Friday")},
                {{std::string("0001-01-01")}, std::string("Monday")},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::string("Monday")},
        };

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateTime};

        DataSet data_set = {{{std::string("2007-02-03 00:00:00")}, std::string("Saturday")},
                            {{std::string("2020-01-00 00:00:00")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::Date};

        DataSet data_set = {
                {{std::string("2007-02-03")}, std::string("Saturday")},
                {{std::string("2020-01-00")}, Null()},
                {{std::string("2020-12-15")}, std::string("Tuesday")},
                {{std::string("2021-01-01")}, std::string("Friday")},
                {{std::string("2021-01-31")}, std::string("Sunday")},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, std::string("Saturday")},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, std::string("Thursday")},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, std::string("Friday")},
                {{std::string("0001-01-01")}, std::string("Monday")},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, std::string("Monday")},
        };

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2};

        DataSet data_set = {{{std::string("2007-02-03 00:00:11.123")}, std::string("Saturday")},
                            {{std::string("2020-01-00 00:00:11.123")}, Null()}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, datetrunc_test) {
    std::string func_name = "date_trunc";
    {
        InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("second")},
                             str_to_date_time("2022-10-08 11:44:23")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("minute")},
                             str_to_date_time("2022-10-08 11:44:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("hour")},
                             str_to_date_time("2022-10-08 11:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("day")},
                             str_to_date_time("2022-10-08 00:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("month")},
                             str_to_date_time("2022-10-01 00:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTime, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("year")},
                             str_to_date_time("2022-01-01 00:00:00")}};
        static_cast<void>(check_function<DataTypeDateTime, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {
                {{std::string("2022-10-08 11:44:23.123"), std::string("second")},
                 str_to_datetime_v2("2022-10-08 11:44:23.000", "%Y-%m-%d %H:%i:%s.%f")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("minute")},
                             str_to_datetime_v2("2022-10-08 11:44:00", "%Y-%m-%d %H:%i:%s")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("hour")},
                             str_to_datetime_v2("2022-10-08 11:00:00", "%Y-%m-%d %H:%i:%s")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("day")},
                             str_to_datetime_v2("2022-10-08 00:00:00", "%Y-%m-%d %H:%i:%s")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("month")},
                             str_to_datetime_v2("2022-10-01 00:00:00", "%Y-%m-%d %H:%i:%s")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, Consted {TypeIndex::String}};
        DataSet data_set = {{{std::string("2022-10-08 11:44:23"), std::string("year")},
                             str_to_datetime_v2("2022-01-01 00:00:00", "%Y-%m-%d %H:%i:%s")}};
        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, hours_add_v2_test) {
    std::string func_name = "hours_add";

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-10-23 10:00:00.123"), -4},
                             str_to_datetime_v2("2020-10-23 06:00:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 10:00:00.123"), 4},
                             str_to_datetime_v2("2020-05-23 14:00:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 10:00:00.123"), 100},
                             str_to_datetime_v2("2020-05-27 14:00:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-10-23"), -4},
                             str_to_datetime_v2("2020-10-22 20:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23"), 4},
                             str_to_datetime_v2("2020-05-23 04:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23"), 100},
                             str_to_datetime_v2("2020-05-27 04:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, hours_sub_v2_test) {
    std::string func_name = "hours_sub";

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-05-23 10:00:00.123"), 4},
                             str_to_datetime_v2("2020-05-23 06:00:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 10:00:00.123"), -4},
                             str_to_datetime_v2("2020-05-23 14:00:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 10:00:00.123"), 31},
                             str_to_datetime_v2("2020-05-22 03:00:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-05-23"), 4},
                             str_to_datetime_v2("2020-05-22 20:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23"), -4},
                             str_to_datetime_v2("2020-05-23 04:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23"), 31},
                             str_to_datetime_v2("2020-05-21 17:00:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, minutes_add_v2_test) {
    std::string func_name = "minutes_add";

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-10-23 10:00:00.123"), 40},
                             str_to_datetime_v2("2020-10-23 10:40:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 10:00:00.123"), -40},
                             str_to_datetime_v2("2020-05-23 09:20:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 10:00:00.123"), 100},
                             str_to_datetime_v2("2020-05-23 11:40:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-10-23"), 40},
                             str_to_datetime_v2("2020-10-23 00:40:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23"), -40},
                             str_to_datetime_v2("2020-05-22 23:20:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23"), 100},
                             str_to_datetime_v2("2020-05-23 01:40:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, minutes_sub_v2_test) {
    std::string func_name = "minutes_sub";

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-05-23 10:00:00.123"), 40},
                             str_to_datetime_v2("2020-05-23 09:20:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 10:00:00.123"), -40},
                             str_to_datetime_v2("2020-05-23 10:40:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 10:00:00.123"), 100},
                             str_to_datetime_v2("2020-05-23 08:20:00.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-05-23"), 40},
                             str_to_datetime_v2("2020-05-22 23:20:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23"), -40},
                             str_to_datetime_v2("2020-05-23 00:40:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23"), 100},
                             str_to_datetime_v2("2020-05-22 22:20:00", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, seconds_add_v2_test) {
    std::string func_name = "seconds_add";

    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-10-23 10:00:00.123"), 40},
                             str_to_datetime_v2("2020-10-23 10:00:40.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 10:00:00.123"), -40},
                             str_to_datetime_v2("2020-05-23 09:59:20.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 10:00:00.123"), 100},
                             str_to_datetime_v2("2020-05-23 10:01:40.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-10-23"), 40},
                             str_to_datetime_v2("2020-10-23 00:00:40", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23"), -40},
                             str_to_datetime_v2("2020-05-22 23:59:20", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23"), 100},
                             str_to_datetime_v2("2020-05-23 00:01:40", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, seconds_sub_v2_test) {
    std::string func_name = "seconds_sub";
    {
        InputTypeSet input_types = {TypeIndex::DateTimeV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-05-23 10:00:00.123"), 40},
                             str_to_datetime_v2("2020-05-23 09:59:20.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23 10:00:00.123"), -40},
                             str_to_datetime_v2("2020-05-23 10:00:40.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23 10:00:00.123"), 100},
                             str_to_datetime_v2("2020-05-23 09:58:20.123", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {TypeIndex::DateV2, TypeIndex::Int32};

        DataSet data_set = {{{std::string("2020-05-23"), 40},
                             str_to_datetime_v2("2020-05-22 23:59:20", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string("2020-05-23"), -40},
                             str_to_datetime_v2("2020-05-23 00:00:40", "%Y-%m-%d %H:%i:%s.%f")},
                            {{std::string(""), 4}, Null()},
                            {{std::string("2020-05-23"), 100},
                             str_to_datetime_v2("2020-05-22 23:58:20", "%Y-%m-%d %H:%i:%s.%f")},
                            {{Null(), 4}, Null()}};

        static_cast<void>(
                check_function<DataTypeDateTimeV2, true>(func_name, input_types, data_set));
    }
}

TEST(VTimestampFunctionsTest, month_name) {
    std::string func_name = "monthname";
    {
        DataSet data_set = {{{std::string("2024-01-15")}, std::string("January")},
                            {{std::string("2024-02-20")}, std::string("February")},
                            {{std::string("2024-03-31")}, std::string("March")},
                            {{std::string("2024-04-30")}, std::string("April")},
                            {{std::string("2024-05-01")}, std::string("May")},
                            {{std::string("2024-06-22")}, std::string("June")},
                            {{std::string("2024-07-11")}, std::string("July")},
                            {{std::string("2024-08-08")}, std::string("August")},
                            {{std::string("2024-09-09")}, std::string("September")},
                            {{std::string("2024-10-31")}, std::string("October")},
                            {{std::string("2024-11-30")}, std::string("November")},
                            {{std::string("2024-12-25")}, std::string("December")},
                            {{std::string("2024-01-01")}, std::string("January")},
                            {{std::string("2024-12-31")}, std::string("December")},
                            {{std::string("2024-02-29")}, std::string("February")},
                            {{std::string("2024-02-30")}, Null()},
                            {{std::string("2024-13-01")}, Null()},
                            {{std::string("2024-00-10")}, Null()},
                            {{std::string("2024-04-31")}, Null()},
                            {{std::string("NULL")}, Null()},
                            {{std::string("")}, Null()}};
        {
            BaseInputTypeSet input_types = {TypeIndex::Date};
            check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
        }
        {
            BaseInputTypeSet input_types = {TypeIndex::DateV2};
            check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
        }
    }

    {
        DataSet data_set = {{{std::string("2024-01-15 00:00:00")}, std::string("January")},
                            {{std::string("2024-02-20 00:00:00")}, std::string("February")},
                            {{std::string("2024-03-31 00:00:00")}, std::string("March")},
                            {{std::string("2024-04-30 00:00:00")}, std::string("April")},
                            {{std::string("2024-05-01 00:00:00")}, std::string("May")},
                            {{std::string("2024-06-22 00:00:00")}, std::string("June")},
                            {{std::string("2024-07-11 00:00:00")}, std::string("July")},
                            {{std::string("2024-08-08 00:00:00")}, std::string("August")},
                            {{std::string("2024-09-09 00:00:00")}, std::string("September")},
                            {{std::string("2024-10-31 00:00:00")}, std::string("October")},
                            {{std::string("2024-11-30 00:00:00")}, std::string("November")},
                            {{std::string("2024-12-25 00:00:00")}, std::string("December")},
                            {{std::string("2024-01-01 00:00:00")}, std::string("January")},
                            {{std::string("2024-12-31 00:00:00")}, std::string("December")},
                            {{std::string("2024-02-29 00:00:00")}, std::string("February")},
                            {{std::string("2024-02-30 00:00:00")}, Null()},
                            {{std::string("2024-13-01 00:00:00")}, Null()},
                            {{std::string("2024-00-10 00:00:00")}, Null()},
                            {{std::string("NULL")}, Null()},
                            {{std::string("")}, Null()}};
        {
            BaseInputTypeSet input_types = {TypeIndex::DateTime};
            check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
        }
        {
            BaseInputTypeSet input_types = {TypeIndex::DateTimeV2};
            check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
        }
    }
}

TEST(VTimestampFunctionsTest, last_day) {
    std::string func_name = "last_day";
    {
        DataSet data_set = {
                {{std::string("2020-12-15")}, str_to_date_time("2020-12-31", false)},
                {{std::string("2021-01-01")}, str_to_date_time("2021-01-31", false)},
                {{std::string("2021-01-31")}, str_to_date_time("2021-01-31", false)},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, str_to_date_time("2020-02-29", false)},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, str_to_date_time("1970-01-31", false)},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, str_to_date_time("9999-12-31", false)},
                {{std::string("0001-01-01")}, str_to_date_time("0001-01-31", false)},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, str_to_date_time("0001-01-31", false)},
        };

        BaseInputTypeSet input_types = {TypeIndex::Date};
        check_function_all_arg_comb<DataTypeDate, true>(func_name, input_types, data_set);
    }

    {
        DataSet data_set = {
                {{std::string("2020-12-15")}, str_to_date_v2("2020-12-31", "%Y-%m-%d")},
                {{std::string("2021-01-01")}, str_to_date_v2("2021-01-31", "%Y-%m-%d")},
                {{std::string("2021-01-31")}, str_to_date_v2("2021-01-31", "%Y-%m-%d")},
                {{std::string("2020-02-30")}, Null()},
                {{std::string("2020-13-01")}, Null()},
                {{std::string("2020-02-29")}, str_to_date_v2("2020-02-29", "%Y-%m-%d")},
                {{std::string("2021-02-29")}, Null()},
                {{std::string("1970-01-01")}, str_to_date_v2("1970-01-31", "%Y-%m-%d")},
                {{std::string("YYYY/MM/DD")}, Null()},
                {{std::string("MM-DD-YYYY")}, Null()},
                {{std::string("9999-12-31")}, str_to_date_v2("9999-12-31", "%Y-%m-%d")},
                {{std::string("0001-01-01")}, str_to_date_v2("0001-01-31", "%Y-%m-%d")},
                {{std::string("")}, Null()},
                {{Null()}, Null()},
                {{std::string("10000-01-01")}, Null()},
                {{std::string("0001-1-1")}, str_to_date_v2("0001-01-31", "%Y-%m-%d")},
        };

        BaseInputTypeSet input_types = {TypeIndex::DateV2};
        check_function_all_arg_comb<DataTypeDateV2, true>(func_name, input_types, data_set);
    }
}

} // namespace doris::vectorized