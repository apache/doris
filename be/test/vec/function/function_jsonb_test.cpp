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

#include <gtest/gtest.h>

#include "function_test_util.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(FunctionJsonbTEST, JsonbParseTest) {
    std::string func_name = "jsonb_parse";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set_valid = {
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

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_valid);
    EXPECT_EQ(Status::OK(), st);

    DataSet data_set_invalid = {
            {{STRING("abc")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("'abc'")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("100x")}, Null()}, // invalid int
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("6.a8")}, Null()}, // invalid double
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("{x")}, Null()}, // invalid object
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("[123, abc]")}, Null()} // invalid array
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseErrorToNullTest) {
    std::string func_name = "jsonb_parse_error_to_null";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
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

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseErrorToValueTest) {
    std::string func_name = "jsonb_parse_error_to_value";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
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

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseErrorToInvalidTest) {
    std::string func_name = "jsonb_parse_error_to_invalid";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
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
            {{STRING("abc")}, STRING("")},                     // invalid string
            {{STRING("'abc'")}, STRING("")},                   // invalid string
            {{STRING("100x")}, STRING("")},                    // invalid int
            {{STRING("6.a8")}, STRING("")},                    // invalid double
            {{STRING("{x")}, STRING("")},                      // invalid object
            {{STRING("[123, abc]")}, STRING("")}               // invalid array
    };

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNullableTest) {
    std::string func_name = "jsonb_parse_nullable";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set_valid = {
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

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_valid);
    EXPECT_EQ(Status::OK(), st);

    DataSet data_set_invalid = {
            {{STRING("abc")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("'abc'")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("100x")}, Null()}, // invalid int
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("6.a8")}, Null()}, // invalid double
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("{x")}, Null()}, // invalid object
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("[123, abc]")}, Null()} // invalid array
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNullableErrorToNullTest) {
    std::string func_name = "jsonb_parse_nullable_error_to_null";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
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

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNullableErrorToValueTest) {
    std::string func_name = "jsonb_parse_nullable_error_to_value";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
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

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNullableErrorToInvalidTest) {
    std::string func_name = "jsonb_parse_nullable_error_to_invalid";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
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
            {{STRING("abc")}, STRING("")},                     // invalid string
            {{STRING("'abc'")}, STRING("")},                   // invalid string
            {{STRING("100x")}, STRING("")},                    // invalid int
            {{STRING("6.a8")}, STRING("")},                    // invalid double
            {{STRING("{x")}, STRING("")},                      // invalid object
            {{STRING("[123, abc]")}, STRING("")}               // invalid array
    };

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNotnullTest) {
    std::string func_name = "jsonb_parse_notnull";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set_valid = {
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

    auto st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_valid);
    EXPECT_EQ(Status::OK(), st);

    DataSet data_set_invalid = {
            {{STRING("abc")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("'abc'")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("100x")}, Null()}, // invalid int
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("6.a8")}, Null()}, // invalid double
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("{x")}, Null()}, // invalid object
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
            {{STRING("[123, abc]")}, Null()} // invalid array
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);
    EXPECT_NE(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNotnullErrorToValueTest) {
    std::string func_name = "jsonb_parse_notnull_error_to_value";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
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

    auto st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNotnullErrorToInvalidTest) {
    std::string func_name = "jsonb_parse_notnull_error_to_invalid";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
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
            {{STRING("abc")}, STRING("")},                     // invalid string
            {{STRING("'abc'")}, STRING("")},                   // invalid string
            {{STRING("100x")}, STRING("")},                    // invalid int
            {{STRING("6.a8")}, STRING("")},                    // invalid double
            {{STRING("{x")}, STRING("")},                      // invalid object
            {{STRING("[123, abc]")}, STRING("")}               // invalid array
    };

    auto st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbExtractTest) {
    std::string func_name = "jsonb_extract";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{STRING("null"), STRING("$")}, STRING("null")},
            {{STRING("true"), STRING("$")}, STRING("true")},
            {{STRING("false"), STRING("$")}, STRING("false")},
            {{STRING("100"), STRING("$")}, STRING("100")},                                 //int8
            {{STRING("10000"), STRING("$")}, STRING("10000")},                             // int16
            {{STRING("1000000000"), STRING("$")}, STRING("1000000000")},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, STRING("1152921504606846976")}, // int64
            {{STRING("6.18"), STRING("$")}, STRING("6.18")},                               // double
            {{STRING(R"("abcd")"), STRING("$")}, STRING(R"("abcd")")},                     // string
            {{STRING("{}"), STRING("$")}, STRING("{}")},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, STRING(R"({"k1":"v31","k2":300})")}, // object
            {{STRING("[]"), STRING("$")}, STRING("[]")},                              // empty array
            {{STRING("[123, 456]"), STRING("$")}, STRING("[123,456]")},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, STRING(R"(["abc","def"])")}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
             STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
             STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract obejct
    data_set = {
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, STRING(R"("v31")")}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
              Null()}, // complex array
    };

    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract array
    data_set = {
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                              // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                              // empty array
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

    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract $[0].k1
    data_set = {
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
              STRING(R"("v41")")}, // complex array
    };

    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbExtractStringTest) {
    std::string func_name = "jsonb_extract_string";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{STRING("null"), STRING("$")}, Null()},
            {{STRING("true"), STRING("$")}, Null()},
            {{STRING("false"), STRING("$")}, Null()},
            {{STRING("100"), STRING("$")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, Null()}, // int64
            {{STRING("6.18"), STRING("$")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$")}, STRING("abcd")},                     // string
            {{STRING("{}"), STRING("$")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, Null()}, // object
            {{STRING("[]"), STRING("$")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
              Null()}, // complex array
    };

    auto st = check_function<DataTypeString, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract obejct
    data_set = {
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, STRING("v31")}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
              Null()}, // complex array
    };

    st = check_function<DataTypeString, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
    

    // jsonb_extract array
    data_set = {
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                              // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, Null()},             // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, Null()},             // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                    // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, STRING("abc")}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, STRING("def")}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},             // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[1]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[2]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[3]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[4]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[5]")},
              STRING("abc")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[6]")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[1]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[2]")},
              STRING("a")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[3]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[4]")},
              Null()}, // complex array
    };

    st = check_function<DataTypeString, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract $[0].k1
    data_set = {
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
              STRING("v41")}, // complex array
    };

    st = check_function<DataTypeString, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbExtractIntTest) {
    std::string func_name = "jsonb_extract_int";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{STRING("null"), STRING("$")}, Null()},
            {{STRING("true"), STRING("$")}, Null()},
            {{STRING("false"), STRING("$")}, Null()},
            {{STRING("100"), STRING("$")}, INT(100)},                                 //int8
            {{STRING("10000"), STRING("$")}, INT(10000)},                             // int16
            {{STRING("1000000000"), STRING("$")}, INT(1000000000)},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, Null()}, // int64
            {{STRING("6.18"), STRING("$")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$")}, Null()},                     // string
            {{STRING("{}"), STRING("$")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, Null()}, // object
            {{STRING("[]"), STRING("$")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
              Null()}, // complex array
    };

    auto st = check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract obejct
    data_set = {
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, Null()}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
              Null()}, // complex array
    };

    st = check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract array
    data_set = {
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                              // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, INT(123)},             // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, INT(456)},             // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                    // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, Null()}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, Null()}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},             // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[1]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[2]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[3]")},
              INT(100)}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[4]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[5]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[6]")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[1]")},
              INT(1)}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[2]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[3]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[4]")},
              Null()}, // complex array
    };

    st = check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract $[0].k1
    data_set = {
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k2")},
              INT(400)}, // complex array
    };

    st = check_function<DataTypeInt32, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbExtractBigIntTest) {
    std::string func_name = "jsonb_extract_bigint";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{STRING("null"), STRING("$")}, Null()},
            {{STRING("true"), STRING("$")}, Null()},
            {{STRING("false"), STRING("$")}, Null()},
            {{STRING("100"), STRING("$")}, BIGINT(100)},                                 //int8
            {{STRING("10000"), STRING("$")}, BIGINT(10000)},                             // int16
            {{STRING("1000000000"), STRING("$")}, BIGINT(1000000000)},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, BIGINT(1152921504606846976)}, // int64
            {{STRING("6.18"), STRING("$")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$")}, Null()},                     // string
            {{STRING("{}"), STRING("$")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, Null()}, // object
            {{STRING("[]"), STRING("$")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
              Null()}, // complex array
    };

    auto st = check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract obejct
    data_set = {
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, Null()}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
              Null()}, // complex array
    };

    st = check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract array
    data_set = {
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                              // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, BIGINT(123)},             // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, BIGINT(456)},             // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                    // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, Null()}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, Null()}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},             // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[1]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[2]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[3]")},
              BIGINT(100)}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[4]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[5]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[6]")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[1]")},
              BIGINT(1)}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[2]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[3]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[4]")},
              Null()}, // complex array
    };

    st = check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract $[0].k1
    data_set = {
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k2")},
              BIGINT(400)}, // complex array
    };

    st = check_function<DataTypeInt64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbExtractDoubleTest) {
    std::string func_name = "jsonb_extract_double";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{STRING("null"), STRING("$")}, Null()},
            {{STRING("true"), STRING("$")}, Null()},
            {{STRING("false"), STRING("$")}, Null()},
            {{STRING("100"), STRING("$")}, DOUBLE(100)},                                 //int8
            {{STRING("10000"), STRING("$")}, DOUBLE(10000)},                             // int16
            {{STRING("1000000000"), STRING("$")}, DOUBLE(1000000000)},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, DOUBLE(1152921504606846976)}, // int64
            {{STRING("6.18"), STRING("$")}, DOUBLE(6.18)},                               // double
            {{STRING(R"("abcd")"), STRING("$")}, Null()},                     // string
            {{STRING("{}"), STRING("$")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, Null()}, // object
            {{STRING("[]"), STRING("$")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
              Null()}, // complex array
    };

    auto st = check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract obejct
    data_set = {
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, Null()}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
              Null()}, // complex array
    };

    st = check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract array
    data_set = {
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                              // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                     // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, DOUBLE(123)},             // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, DOUBLE(456)},             // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                    // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, Null()}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, Null()}, // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},             // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[1]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[2]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[3]")},
              DOUBLE(100)}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[4]")},
              DOUBLE(6.18)}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[5]")},
              Null()}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[6]")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[1]")},
              DOUBLE(1)}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[2]")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[3]")},
              DOUBLE(3.14)}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[4]")},
              Null()}, // complex array
    };

    st = check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);


    // jsonb_extract $[0].k1
    data_set = {
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                                 //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                   // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()}, // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                               // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                     // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                                   // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                              // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},               // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
              Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
              Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k2")},
              DOUBLE(400)}, // complex array
    };

    st = check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

} // namespace doris::vectorized
