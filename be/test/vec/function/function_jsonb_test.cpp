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

#include <stdint.h>

#include <iomanip>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "testutil/any_type.h"
#include "testutil/column_helper.h"
#include "udf/udf.h"
#include "util/jsonb_writer.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(FunctionJsonbTEST, JsonbParseTest) {
    std::string func_name = "json_parse";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR}};

    DataSet data_set_valid = {
            {{Null()}, Null()},
            {{STRING("null")}, STRING("null")},
            {{STRING("true")}, STRING("true")},
            {{STRING("false")}, STRING("false")},
            {{STRING("100")}, STRING("100")},                                 //int8
            {{STRING("10000")}, STRING("10000")},                             // int16
            {{STRING("1000000000")}, STRING("1000000000")},                   // int32
            {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
            {{STRING("6.18")}, STRING("6.18")},                               // double
            {{STRING(R"("abcd")")}, STRING(R"("abcd")")},                     // string
            {{STRING("{}")}, STRING("{}")},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
            {{STRING("[]")}, STRING("[]")},                              // empty array
            {{STRING("[123, 456]")}, STRING("[123,456]")},               // int array
            {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])")},
             STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")},
             STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };

    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set_valid));

    DataSet data_set_invalid = {
            {{STRING("abc")}, Null()}, // invalid string
    };
    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid,
                                                          -1, -1, true));

    data_set_invalid = {
            {{STRING("'abc'")}, Null()}, // invalid string
    };
    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid,
                                                          -1, -1, true));

    data_set_invalid = {
            {{STRING("100x")}, Null()}, // invalid int
    };
    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid,
                                                          -1, -1, true));

    data_set_invalid = {
            {{STRING("6.a8")}, Null()}, // invalid double
    };
    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid,
                                                          -1, -1, true));

    data_set_invalid = {
            {{STRING("{x")}, Null()}, // invalid object
    };
    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid,
                                                          -1, -1, true));

    data_set_invalid = {
            {{STRING("[123, abc]")}, Null()} // invalid array
    };
    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid,
                                                          -1, -1, true));
}

TEST(FunctionJsonbTEST, JsonbParseErrorToNullTest) {
    std::string func_name = "json_parse_error_to_null";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR}};

    DataSet data_set = {
            {{Null()}, Null()},
            {{STRING("null")}, STRING("null")},
            {{STRING("true")}, STRING("true")},
            {{STRING("false")}, STRING("false")},
            {{STRING("100")}, STRING("100")},                                 //int8
            {{STRING("10000")}, STRING("10000")},                             // int16
            {{STRING("1000000000")}, STRING("1000000000")},                   // int32
            {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
            {{STRING("6.18")}, STRING("6.18")},                               // double
            {{STRING(R"("abcd")")}, STRING(R"("abcd")")},                     // string
            {{STRING("{}")}, STRING("{}")},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
            {{STRING("[]")}, STRING("[]")},                              // empty array
            {{STRING("[123, 456]")}, STRING("[123,456]")},               // int array
            {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])")},
             STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")},
             STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
            {{STRING("abc")}, Null()},                         // invalid string
            {{STRING("'abc'")}, Null()},                       // invalid string
            {{STRING("100x")}, Null()},                        // invalid int
            {{STRING("6.a8")}, Null()},                        // invalid double
            {{STRING("{x")}, Null()},                          // invalid object
            {{STRING("[123, abc]")}, Null()}                   // invalid array
    };

    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonbTEST, JsonbParseErrorToValueTest) {
    std::string func_name = "json_parse_error_to_value";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_VARCHAR}, PrimitiveType::TYPE_JSONB};

    DataSet data_set = {
            {{Null(), STRING("{}")}, Null()},
            {{STRING("null"), STRING("{}")}, STRING("null")},
            {{STRING("true"), STRING("{}")}, STRING("true")},
            {{STRING("false"), STRING("{}")}, STRING("false")},
            {{STRING("100"), STRING("{}")}, STRING("100")},                                 //int8
            {{STRING("10000"), STRING("{}")}, STRING("10000")},                             // int16
            {{STRING("1000000000"), STRING("{}")}, STRING("1000000000")},                   // int32
            {{STRING("1152921504606846976"), STRING("{}")}, STRING("1152921504606846976")}, // int64
            {{STRING("6.18"), STRING("{}")}, STRING("6.18")},           // double
            {{STRING(R"("abcd")"), STRING("{}")}, STRING(R"("abcd")")}, // string
            {{STRING("{}"), STRING("{}")}, STRING("{}")},               // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("{}")},
             STRING(R"({"k1":"v31","k2":300})")},                        // object
            {{STRING("[]"), STRING("{}")}, STRING("[]")},                // empty array
            {{STRING("[123, 456]"), STRING("{}")}, STRING("[123,456]")}, // int array
            {{STRING(R"(["abc", "def"])"), STRING("{}")},
             STRING(R"(["abc","def"])")}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("{}")},
             STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("{}")},
             STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")},           // complex array
            {{STRING("abc"), STRING(R"("abc")")}, STRING(R"("abc")")},   // invalid string
            {{STRING("'abc'"), STRING(R"("abc")")}, STRING(R"("abc")")}, // invalid string
            {{STRING("100x"), STRING("100")}, STRING("100")},            // invalid int
            {{STRING("6.a8"), STRING("6.18")}, STRING("6.18")},          // invalid double
            {{STRING("{x"), STRING("{}")}, STRING("{}")},                // invalid object
            {{STRING("[123, abc]"), STRING(R"([123,"abc"])")},
             STRING(R"([123,"abc"])")} // invalid array
    };

    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonbTEST, JsonbExtractTest) {
    std::string func_name = "jsonb_extract";
    InputTypeSet input_types = {PrimitiveType::TYPE_JSONB, PrimitiveType::TYPE_VARCHAR};

    // json_extract root
    DataSet data_set = {
            {{Null(), STRING("$")}, Null()},
            {{STRING("null"), STRING("$")}, STRING("null")},
            {{STRING("true"), STRING("$")}, STRING("true")},
            {{STRING("false"), STRING("$")}, STRING("false")},
            {{STRING("100"), STRING("$")}, STRING("100")},                                 //int8
            {{STRING("10000"), STRING("$")}, STRING("10000")},                             // int16
            {{STRING("1000000000"), STRING("$")}, STRING("1000000000")},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, STRING("1152921504606846976")}, // int64
            {{STRING("6.18"), STRING("$")}, STRING("6.18")},                               // double
            {{STRING(R"("abcd")"), STRING("$")}, STRING(R"("abcd")")},                     // string
            {{STRING("{}"), STRING("$")}, STRING("{}")}, // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")},
             STRING(R"({"k1":"v31","k2":300})")},                       // object
            {{STRING("[]"), STRING("$")}, STRING("[]")},                // empty array
            {{STRING("[123, 456]"), STRING("$")}, STRING("[123,456]")}, // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")},
             STRING(R"(["abc","def"])")}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
             STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
             STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };

    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set));

    // json_extract obejct
    data_set = {
            {{Null(), STRING("$.k1")}, Null()},
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                 //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},               // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},          // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},           // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                  // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, STRING(R"("v31")")}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},        // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
             Null()}, // complex array
    };

    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set));

    // json_extract array
    data_set = {
            {{Null(), STRING("$[0]")}, Null()},
            {{STRING("null"), STRING("$[0]")}, STRING("null")},
            {{STRING("true"), STRING("$[0]")}, STRING("true")},
            {{STRING("false"), STRING("$[0]")}, STRING("false")},
            {{STRING("100"), STRING("$[0]")}, STRING("100")},               //int8
            {{STRING("10000"), STRING("$[0]")}, STRING("10000")},           // int16
            {{STRING("1000000000"), STRING("$[0]")}, STRING("1000000000")}, // int32
            {{STRING("1152921504606846976"), STRING("$[0]")},
             STRING("1152921504606846976")},                             // int64
            {{STRING("6.18"), STRING("$[0]")}, STRING("6.18")},          // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, STRING("\"abcd\"")}, // string
            {{STRING("{}"), STRING("$[0]")}, STRING("{}")},              // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")},
             STRING(R"({"k1":"v31","k2":300})")},     // object
            {{STRING("[]"), STRING("$[0]")}, Null()}, // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                           //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                         // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                    // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()},           // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                          // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                            // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()},    // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                            // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, STRING("123")},             // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, STRING("456")},             // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                    // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, STRING(R"("abc")")}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, STRING(R"("def")")}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},             // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0]")},
             STRING("null")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[1]")},
             STRING("true")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[2]")},
             STRING("false")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[3]")},
             STRING("100")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[4]")},
             STRING("6.18")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[5]")},
             STRING(R"("abc")")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[6]")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0]")},
             STRING(R"({"k1":"v41","k2":400})")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[1]")},
             STRING("1")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[2]")},
             STRING(R"("a")")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[3]")},
             STRING("3.14")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[4]")},
             Null()}, // complex array
    };

    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set));

    // json_extract $[0].k1
    data_set = {
            {{Null(), STRING("$[0].k1")}, Null()},
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                 //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},               // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},          // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},           // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                  // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")},
             STRING(R"("v31")")},                                       // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},        // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
             STRING(R"("v41")")}, // complex array
    };

    static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonbTEST, JsonbCastToOtherTest) {
    std::string func_name = "CAST";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_JSONB},
                                Consted {PrimitiveType::TYPE_BOOLEAN}};

    // cast to boolean
    DataSet data_set = {
            {{STRING("null"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)}, Null()},
            {{STRING("true"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)}, BOOLEAN(1)},
            {{STRING("false"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)}, BOOLEAN(0)},
            {{STRING("100"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)}, BOOLEAN(1)}, //int8
            {{STRING("10000"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             BOOLEAN(1)}, // int16
            {{STRING("1000000000"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             BOOLEAN(1)}, // int32
            {{STRING("1152921504606846976"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             BOOLEAN(1)}, // int64
            {{STRING("6.18"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             BOOLEAN(1)}, // double
            {{STRING(R"("abcd")"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // string
            {{STRING("{}"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"),
              static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // object
            {{STRING("[]"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // empty array
            {{STRING("[123, 456]"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // int array
            {{STRING(R"(["abc", "def"])"), static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"),
              static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"),
              static_cast<uint8_t>(PrimitiveType::TYPE_BOOLEAN)},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        static_cast<void>(
                check_function<DataTypeUInt8, true>(func_name, input_types, const_dataset));
    }
    input_types = {Nullable {PrimitiveType::TYPE_JSONB}, Consted {PrimitiveType::TYPE_TINYINT}};
    // cast to TINYINT
    data_set = {
            {{STRING("null"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)}, Null()},
            {{STRING("true"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)}, TINYINT(1)},
            {{STRING("false"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)}, TINYINT(0)},
            {{STRING("100"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             TINYINT(100)},                                                                //int8
            {{STRING("10000"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)}, Null()}, // int16
            {{STRING("1000000000"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // int32
            {{STRING("1152921504606846976"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // int64
            {{STRING("6.18"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             TINYINT(6)}, // double
            {{STRING(R"("abcd")"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // string
            {{STRING("{}"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"),
              static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // object
            {{STRING("[]"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // empty array
            {{STRING("[123, 456]"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // int array
            {{STRING(R"(["abc", "def"])"), static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"),
              static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"),
              static_cast<int8_t>(PrimitiveType::TYPE_TINYINT)},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        static_cast<void>(
                check_function<DataTypeInt8, true>(func_name, input_types, const_dataset));
    }

    input_types = {Nullable {PrimitiveType::TYPE_JSONB}, Consted {PrimitiveType::TYPE_SMALLINT}};
    // cast to SMALLINT
    data_set = {
            {{STRING("null"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)}, Null()},
            {{STRING("true"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)}, SMALLINT(1)},
            {{STRING("false"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)}, SMALLINT(0)},
            {{STRING("100"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             SMALLINT(100)}, //int8
            {{STRING("10000"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             SMALLINT(10000)}, // int16
            {{STRING("1000000000"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // int32
            {{STRING("1152921504606846976"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // int64
            {{STRING("6.18"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             SMALLINT(6)}, // double
            {{STRING(R"("abcd")"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // string
            {{STRING("{}"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"),
              static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // object
            {{STRING("[]"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // empty array
            {{STRING("[123, 456]"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // int array
            {{STRING(R"(["abc", "def"])"), static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"),
              static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"),
              static_cast<int16_t>(PrimitiveType::TYPE_SMALLINT)},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        static_cast<void>(
                check_function<DataTypeInt16, true>(func_name, input_types, const_dataset));
    }

    input_types = {Nullable {PrimitiveType::TYPE_JSONB}, Consted {PrimitiveType::TYPE_INT}};
    // cast to INT
    data_set = {
            {{STRING("null"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, Null()},
            {{STRING("true"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, INT(1)},
            {{STRING("false"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, INT(0)},
            {{STRING("100"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, INT(100)},     //int8
            {{STRING("10000"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, INT(10000)}, // int16
            {{STRING("1000000000"), static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             INT(1000000000)}, // int32
            {{STRING("1152921504606846976"), static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             Null()},                                                                  // int64
            {{STRING("6.18"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, INT(6)}, // double
            {{STRING(R"("abcd")"), static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             Null()},                                                                // string
            {{STRING("{}"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, Null()}, // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             Null()},                                                                // object
            {{STRING("[]"), static_cast<int32_t>(PrimitiveType::TYPE_INT)}, Null()}, // empty array
            {{STRING("[123, 456]"), static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             Null()}, // int array
            {{STRING(R"(["abc", "def"])"), static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"),
              static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"),
              static_cast<int32_t>(PrimitiveType::TYPE_INT)},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        static_cast<void>(
                check_function<DataTypeInt32, true>(func_name, input_types, const_dataset));
    }

    input_types = {Nullable {PrimitiveType::TYPE_JSONB}, Consted {PrimitiveType::TYPE_BIGINT}};
    // cast to BIGINT
    data_set = {
            {{STRING("null"), BIGINT(1)}, Null()},
            {{STRING("true"), BIGINT(1)}, BIGINT(1)},
            {{STRING("false"), BIGINT(1)}, BIGINT(0)},
            {{STRING("100"), BIGINT(1)}, BIGINT(100)},                                 //int8
            {{STRING("10000"), BIGINT(1)}, BIGINT(10000)},                             // int16
            {{STRING("1000000000"), BIGINT(1)}, BIGINT(1000000000)},                   // int32
            {{STRING("1152921504606846976"), BIGINT(1)}, BIGINT(1152921504606846976)}, // int64
            {{STRING("6.18"), BIGINT(1)}, BIGINT(6)},                                  // double
            {{STRING(R"("abcd")"), BIGINT(1)}, Null()},                                // string
            {{STRING("{}"), BIGINT(1)}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), BIGINT(1)}, Null()}, // object
            {{STRING("[]"), BIGINT(1)}, Null()},                         // empty array
            {{STRING("[123, 456]"), BIGINT(1)}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), BIGINT(1)}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), BIGINT(1)},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), BIGINT(1)},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        static_cast<void>(
                check_function<DataTypeInt64, true>(func_name, input_types, const_dataset));
    }

    input_types = {Nullable {PrimitiveType::TYPE_JSONB}, Consted {PrimitiveType::TYPE_DOUBLE}};
    // cast to DOUBLE
    data_set = {
            {{STRING("null"), DOUBLE(1)}, Null()},
            {{STRING("true"), DOUBLE(1)}, DOUBLE(1)},
            {{STRING("false"), DOUBLE(1)}, DOUBLE(0)},
            {{STRING("100"), DOUBLE(1)}, DOUBLE(100)},                                 //int8
            {{STRING("10000"), DOUBLE(1)}, DOUBLE(10000)},                             // int16
            {{STRING("1000000000"), DOUBLE(1)}, DOUBLE(1000000000)},                   // int32
            {{STRING("1152921504606846976"), DOUBLE(1)}, DOUBLE(1152921504606846976)}, // int64
            {{STRING("6.18"), DOUBLE(1)}, DOUBLE(6.18)},                               // double
            {{STRING(R"("abcd")"), DOUBLE(1)}, Null()},                                // string
            {{STRING("{}"), DOUBLE(1)}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), DOUBLE(1)}, Null()}, // object
            {{STRING("[]"), DOUBLE(1)}, Null()},                         // empty array
            {{STRING("[123, 456]"), DOUBLE(1)}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), DOUBLE(1)}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), DOUBLE(1)},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), DOUBLE(1)},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        static_cast<void>(
                check_function<DataTypeFloat64, true>(func_name, input_types, const_dataset));
    }

    input_types = {Nullable {PrimitiveType::TYPE_JSONB}, Consted {PrimitiveType::TYPE_VARCHAR}};
    // cast to STRING
    data_set = {
            {{STRING("null"), STRING("1")}, Null()},
            {{STRING("true"), STRING("1")}, STRING("true")},
            {{STRING("false"), STRING("1")}, STRING("false")},
            {{STRING("100"), STRING("1")}, STRING("100")},                                 //int8
            {{STRING("10000"), STRING("1")}, STRING("10000")},                             // int16
            {{STRING("1000000000"), STRING("1")}, STRING("1000000000")},                   // int32
            {{STRING("1152921504606846976"), STRING("1")}, STRING("1152921504606846976")}, // int64
            {{STRING("6.18"), STRING("1")}, STRING("6.18")},                               // double
            {{STRING(R"("abcd")"), STRING("1")}, STRING(R"(abcd)")},                       // string
            {{STRING("{}"), STRING("1")}, STRING("{}")}, // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("1")},
             STRING(R"({"k1":"v31","k2":300})")},                       // object
            {{STRING("[]"), STRING("1")}, STRING("[]")},                // empty array
            {{STRING("[123, 456]"), STRING("1")}, STRING("[123,456]")}, // int array
            {{STRING(R"(["abc", "def"])"), STRING("1")},
             STRING(R"(["abc","def"])")}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("1")},
             STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("1")},
             STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        static_cast<void>(
                check_function<DataTypeString, true>(func_name, input_types, const_dataset));
    }
}

TEST(FunctionJsonbTEST, JsonbCastFromOtherTest) {
    // CAST Nullable(X) to Nullable(JSONB)
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_BOOLEAN}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{BOOLEAN(1), Null()}, STRING("true")}}));
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_BOOLEAN}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{BOOLEAN(0), Null()}, STRING("false")}}));
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_TINYINT}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{TINYINT(100), Null()}, STRING("100")}}));
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_SMALLINT}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{SMALLINT(10000), Null()}, STRING("10000")}}));
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_INT}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{INT(1000000000), Null()}, STRING("1000000000")}}));
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_BIGINT}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{BIGINT(1152921504606846976), Null()}, STRING("1152921504606846976")}}));
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_DOUBLE}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{DOUBLE(6.18), Null()}, STRING("6.18")}}));
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_VARCHAR}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{STRING(R"(abcd)"), Null()}, Null()}})); // should fail
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {PrimitiveType::TYPE_VARCHAR}, Consted {PrimitiveType::TYPE_JSONB}},
            {{{STRING(R"("abcd")"), Null()}, STRING(R"("abcd")")}}));

    // CAST X to JSONB. the second argument is just a dummy because result type is not nullable so we need it
    // rather than a Null.
    static_cast<void>(check_function<DataTypeJsonb, false>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_BOOLEAN}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{BOOLEAN(1), STRING()}, STRING("true")}}));
    static_cast<void>(check_function<DataTypeJsonb, false>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_BOOLEAN}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{BOOLEAN(0), STRING()}, STRING("false")}}));
    static_cast<void>(check_function<DataTypeJsonb, false>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_TINYINT}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{TINYINT(100), STRING()}, STRING("100")}}));
    static_cast<void>(check_function<DataTypeJsonb, false>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_SMALLINT}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{SMALLINT(10000), STRING()}, STRING("10000")}}));
    static_cast<void>(check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {PrimitiveType::TYPE_INT}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{INT(1000000000), STRING()}, STRING("1000000000")}}));
    static_cast<void>(check_function<DataTypeJsonb, false>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_BIGINT}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{BIGINT(1152921504606846976), STRING()}, STRING("1152921504606846976")}}));
    static_cast<void>(check_function<DataTypeJsonb, false>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_DOUBLE}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{DOUBLE(6.18), STRING()}, STRING("6.18")}}));
    // String to JSONB should always be Nullable
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_VARCHAR}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{STRING(R"(abcd)"), STRING()}, Null()}})); // should fail
    static_cast<void>(check_function<DataTypeJsonb, true>(
            "CAST",
            {Notnull {PrimitiveType::TYPE_VARCHAR}, ConstedNotnull {PrimitiveType::TYPE_JSONB}},
            {{{STRING(R"("abcd")"), STRING()}, STRING(R"("abcd")")}}));
}

TEST(FunctionJsonbTEST, JsonbToJson) {
    std::string func_name = "to_json";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_BOOLEAN};

        DataSet data_set = {
                {{Null()}, Null()},
                {{UInt8(1)}, STRING("true")},
                {{UInt8(0)}, STRING("false")},
        };
        static_cast<void>(check_function<DataTypeJsonb, true>(func_name, input_types, data_set));
    }

    static_cast<void>(
            check_function<DataTypeJsonb, true>("to_json", {Nullable {PrimitiveType::TYPE_INT}},
                                                {{{INT(1000000000)}, STRING("1000000000")}}));

    static_cast<void>(
            check_function<DataTypeJsonb, true>("to_json", {Nullable {PrimitiveType::TYPE_VARCHAR}},
                                                {{{STRING("hello")}, STRING(R"("hello")")}}));
}

TEST(FunctionJsonbTEST, JsonArray) {
    std::string func_name = "json_array";

    InputTypeSet input_types = {PrimitiveType::TYPE_JSONB};

    DataSet data_set = {
            {{Null()}, STRING("[null]")},
            {{STRING("null")}, STRING("[null]")},
            {{STRING("true")}, STRING("[true]")},
            {{STRING("false")}, STRING("[false]")},
            {{STRING("100")}, STRING("[100]")},                                 //int8
            {{STRING("10000")}, STRING("[10000]")},                             // int16
            {{STRING("1000000000")}, STRING("[1000000000]")},                   // int32
            {{STRING("1152921504606846976")}, STRING("[1152921504606846976]")}, // int64
            {{STRING("6.18")}, STRING("[6.18]")},                               // double
            {{STRING(R"("abcd")")}, STRING(R"(["abcd"])")},                     // string
    };

    static_cast<void>(check_function<DataTypeJsonb>(func_name, input_types, data_set));

    func_name = "json_array_ignore_null";

    data_set = {
            {{Null()}, STRING("[]")},
            {{STRING("null")}, STRING("[null]")},
            {{STRING("true")}, STRING("[true]")},
            {{STRING("false")}, STRING("[false]")},
            {{STRING("100")}, STRING("[100]")},                                 //int8
            {{STRING("10000")}, STRING("[10000]")},                             // int16
            {{STRING("1000000000")}, STRING("[1000000000]")},                   // int32
            {{STRING("1152921504606846976")}, STRING("[1152921504606846976]")}, // int64
            {{STRING("6.18")}, STRING("[6.18]")},                               // double
            {{STRING(R"("abcd")")}, STRING(R"(["abcd"])")},                     // string
    };

    static_cast<void>(check_function<DataTypeJsonb>(func_name, input_types, data_set));
}

TEST(FunctionJsonbTEST, JsonLength) {
    std::string func_name = "json_length";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_JSONB, PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {
                {{STRING(R"({"k1":"v1", "k2": 2})"), Null()}, Null()},
                {{STRING(R"([1, 2, 3])"), STRING("$")}, INT(3)},
                {{STRING(R"("string")"), STRING("$")}, INT(1)},
                {{STRING("null"), STRING("$")}, INT(1)},
        };
        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    auto json_data_type = std::make_shared<DataTypeJsonb>();
    auto json_column = json_data_type->create_column();

    JsonbWriter writer;
    writer.writeStartArray();
    writer.writeString("hello");
    writer.writeString("world");
    writer.writeEndArray();

    json_column->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(json_column), json_data_type, "json_col"));

    auto path_column_ = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    path_column_->insert_default();
    auto path_column = ColumnConst::create(std::move(path_column_), 1);
    block.insert(ColumnWithTypeAndName(
            std::move(path_column),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "path_col"));

    auto return_type = make_nullable(std::make_shared<DataTypeInt32>());
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    ASSERT_TRUE(func != nullptr);

    std::vector<DataTypePtr> arg_types;
    arg_types.emplace_back(json_data_type);
    arg_types.emplace_back(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));

    FunctionUtils fn_utils(return_type, arg_types, 0);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    //     fn_ctx->set_constant_cols(constant_cols);
    auto st = func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();
    st = func->open(fn_ctx, FunctionContext::THREAD_LOCAL);
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    block.insert({nullptr, return_type, "result"});

    auto result = block.columns() - 1;
    st = func->execute(fn_ctx, block, {0, 1}, result, 1);
    ASSERT_TRUE(st.ok()) << "execute failed: " << st.to_string();
}

TEST(FunctionJsonbTEST, JsonContains) {
    std::string func_name = "json_contains";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_JSONB, PrimitiveType::TYPE_JSONB,
                                    PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {
                {{STRING(R"({"k1":"v1", "k2": 2})"), Null(), Null()}, Null()},
                {{STRING(R"([1, 2, 3])"), STRING("1"), STRING("$")}, BOOLEAN(true)},
                {{STRING(R"("string")"), STRING("string"), STRING("$")}, Null()},
                {{STRING(R"("string")"), STRING(R"("string")"), STRING("$")}, BOOLEAN(true)},
        };
        static_cast<void>(check_function<DataTypeBool, true>(func_name, input_types, data_set));
    }

    auto json_data_type = std::make_shared<DataTypeJsonb>();
    auto json_column = json_data_type->create_column();

    JsonbWriter writer;
    writer.writeStartArray();
    writer.writeString("hello");
    writer.writeString("world");
    writer.writeEndArray();

    json_column->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());

    auto json_column2 = json_data_type->create_column();
    writer.reset();
    writer.writeStartString();
    writer.writeString("hello");
    writer.writeEndString();

    Block block;
    block.insert(ColumnWithTypeAndName(std::move(json_column), json_data_type, "json_col"));
    block.insert(ColumnWithTypeAndName(std::move(json_column2), json_data_type, "json_col2"));

    auto path_column_ = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    path_column_->insert_default();
    auto path_column = ColumnConst::create(std::move(path_column_), 1);
    block.insert(ColumnWithTypeAndName(
            std::move(path_column),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "path_col"));

    auto return_type = make_nullable(std::make_shared<DataTypeBool>());
    FunctionBasePtr func = SimpleFunctionFactory::instance().get_function(
            func_name, block.get_columns_with_type_and_name(), return_type);
    ASSERT_TRUE(func != nullptr);

    std::vector<DataTypePtr> arg_types;
    arg_types.emplace_back(json_data_type);
    arg_types.emplace_back(json_data_type);
    arg_types.emplace_back(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));

    FunctionUtils fn_utils(return_type, arg_types, 0);
    auto* fn_ctx = fn_utils.get_fn_ctx();
    //     fn_ctx->set_constant_cols(constant_cols);
    auto st = func->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();
    st = func->open(fn_ctx, FunctionContext::THREAD_LOCAL);
    ASSERT_TRUE(st.ok()) << "open failed: " << st.to_string();

    block.insert({nullptr, return_type, "result"});

    auto result = block.columns() - 1;
    st = func->execute(fn_ctx, block, {0, 1, 2}, result, 1);
    ASSERT_TRUE(st.ok()) << "execute failed: " << st.to_string();
}

} // namespace doris::vectorized
