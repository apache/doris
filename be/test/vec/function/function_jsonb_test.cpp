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
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_jsonb.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(FunctionJsonbTEST, JsonbParseTest) {
    std::string func_name = "jsonb_parse";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set_valid = {
        {{STRING("null")}, STRING("null")},
        {{STRING("true")}, STRING("true")},
        {{STRING("false")}, STRING("false")},
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_valid);
    EXPECT_EQ(Status::OK(), st);

    DataSet data_set_invalid = {
        {{STRING("abc")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("'abc'")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("100x")}, Null()}, // invalid int
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("6.a8")}, Null()}, // invalid double
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("{x")}, Null()}, // invalid object
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("[123, abc]")}, Null()} // invalid array
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseErrorToNullTest) {
    std::string func_name = "jsonb_parse_error_to_null";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
        {{STRING("null")}, STRING("null")},
        {{STRING("true")}, STRING("true")},
        {{STRING("false")}, STRING("false")},
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc")}, Null()}, // invalid string
        {{STRING("'abc'")}, Null()}, // invalid string
        {{STRING("100x")}, Null()}, // invalid int
        {{STRING("6.a8")}, Null()}, // invalid double
        {{STRING("{x")}, Null()}, // invalid object
        {{STRING("[123, abc]")}, Null()} // invalid array
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
        {{STRING("100"), STRING("{}")}, STRING("100")}, //int8
        {{STRING("10000"), STRING("{}")}, STRING("10000")}, // int16
        {{STRING("1073741820"), STRING("{}")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976"), STRING("{}")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18"), STRING("{}")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")"), STRING("{}")}, STRING(R"("abcd")")}, // string
        {{STRING("{}"), STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("{}")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]"), STRING("{}")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]"), STRING("{}")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])"), STRING("{}")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("{}")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("{}")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc"), STRING(R"("abc")")}, STRING(R"("abc")")}, // invalid string
        {{STRING("'abc'"), STRING(R"("abc")")}, STRING(R"("abc")")}, // invalid string
        {{STRING("100x"), STRING("100")}, STRING("100")}, // invalid int
        {{STRING("6.a8"), STRING("6.18")}, STRING("6.18")}, // invalid double
        {{STRING("{x"), STRING("{}")}, STRING("{}")}, // invalid object
        {{STRING("[123, abc]"), STRING(R"([123,"abc"])")}, STRING(R"([123,"abc"])")} // invalid array
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
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc")}, STRING("")}, // invalid string
        {{STRING("'abc'")}, STRING("")}, // invalid string
        {{STRING("100x")}, STRING("")}, // invalid int
        {{STRING("6.a8")}, STRING("")}, // invalid double
        {{STRING("{x")}, STRING("")}, // invalid object
        {{STRING("[123, abc]")}, STRING("")} // invalid array
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
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };

    auto st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_valid);
    EXPECT_EQ(Status::OK(), st);

    DataSet data_set_invalid = {
        {{STRING("abc")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("'abc'")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("100x")}, Null()}, // invalid int
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("6.a8")}, Null()}, // invalid double
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("{x")}, Null()}, // invalid object
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("[123, abc]")}, Null()} // invalid array
    };
    st = check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNullableErrorToNullTest) {
    std::string func_name = "jsonb_parse_nullable_error_to_null";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
        {{STRING("null")}, STRING("null")},
        {{STRING("true")}, STRING("true")},
        {{STRING("false")}, STRING("false")},
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc")}, Null()}, // invalid string
        {{STRING("'abc'")}, Null()}, // invalid string
        {{STRING("100x")}, Null()}, // invalid int
        {{STRING("6.a8")}, Null()}, // invalid double
        {{STRING("{x")}, Null()}, // invalid object
        {{STRING("[123, abc]")}, Null()} // invalid array
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
        {{STRING("100"), STRING("{}")}, STRING("100")}, //int8
        {{STRING("10000"), STRING("{}")}, STRING("10000")}, // int16
        {{STRING("1073741820"), STRING("{}")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976"), STRING("{}")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18"), STRING("{}")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")"), STRING("{}")}, STRING(R"("abcd")")}, // string
        {{STRING("{}"), STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("{}")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]"), STRING("{}")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]"), STRING("{}")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])"), STRING("{}")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("{}")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("{}")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc"), STRING(R"("abc")")}, STRING(R"("abc")")}, // invalid string
        {{STRING("'abc'"), STRING(R"("abc")")}, STRING(R"("abc")")}, // invalid string
        {{STRING("100x"), STRING("100")}, STRING("100")}, // invalid int
        {{STRING("6.a8"), STRING("6.18")}, STRING("6.18")}, // invalid double
        {{STRING("{x"), STRING("{}")}, STRING("{}")}, // invalid object
        {{STRING("[123, abc]"), STRING(R"([123,"abc"])")}, STRING(R"([123,"abc"])")} // invalid array
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
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc")}, STRING("")}, // invalid string
        {{STRING("'abc'")}, STRING("")}, // invalid string
        {{STRING("100x")}, STRING("")}, // invalid int
        {{STRING("6.a8")}, STRING("")}, // invalid double
        {{STRING("{x")}, STRING("")}, // invalid object
        {{STRING("[123, abc]")}, STRING("")} // invalid array
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
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };

    auto st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_valid);
    EXPECT_EQ(Status::OK(), st);

    DataSet data_set_invalid = {
        {{STRING("abc")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("'abc'")}, Null()}, // invalid string
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("100x")}, Null()}, // invalid int
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("6.a8")}, Null()}, // invalid double
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("{x")}, Null()}, // invalid object
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);

    data_set_invalid = {
        {{STRING("[123, abc]")}, Null()} // invalid array
    };
    st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid);
    EXPECT_NE(Status::OK(), st);
}

TEST(FunctionJsonbTEST, JsonbParseNotnullErrorToValueTest) {
    std::string func_name = "jsonb_parse_notnull_error_to_value";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
        {{STRING("null"), STRING("{}")}, STRING("null")},
        {{STRING("true"), STRING("{}")}, STRING("true")},
        {{STRING("false"), STRING("{}")}, STRING("false")},
        {{STRING("100"), STRING("{}")}, STRING("100")}, //int8
        {{STRING("10000"), STRING("{}")}, STRING("10000")}, // int16
        {{STRING("1073741820"), STRING("{}")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976"), STRING("{}")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18"), STRING("{}")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")"), STRING("{}")}, STRING(R"("abcd")")}, // string
        {{STRING("{}"), STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("{}")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]"), STRING("{}")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]"), STRING("{}")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])"), STRING("{}")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("{}")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("{}")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc"), STRING(R"("abc")")}, STRING(R"("abc")")}, // invalid string
        {{STRING("'abc'"), STRING(R"("abc")")}, STRING(R"("abc")")}, // invalid string
        {{STRING("100x"), STRING("100")}, STRING("100")}, // invalid int
        {{STRING("6.a8"), STRING("6.18")}, STRING("6.18")}, // invalid double
        {{STRING("{x"), STRING("{}")}, STRING("{}")}, // invalid object
        {{STRING("[123, abc]"), STRING(R"([123,"abc"])")}, STRING(R"([123,"abc"])")} // invalid array
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
        {{STRING("100")}, STRING("100")}, //int8
        {{STRING("10000")}, STRING("10000")}, // int16
        {{STRING("1073741820")}, STRING("1073741820")}, // int32
        {{STRING("1152921504606846976")}, STRING("1152921504606846976")}, // int64
        {{STRING("6.18")}, STRING("6.18")}, // double
        {{STRING(R"("abcd")")}, STRING(R"("abcd")")}, // string
        {{STRING("{}")}, STRING("{}")}, // empty object
        {{STRING(R"({"k1":"v31", "k2": 300})")}, STRING(R"({"k1":"v31","k2":300})")}, // object
        {{STRING("[]")}, STRING("[]")}, // empty array
        {{STRING("[123, 456]")}, STRING("[123,456]")}, // int array
        {{STRING(R"(["abc", "def"])")}, STRING(R"(["abc","def"])")}, // string array
        {{STRING(R"([null, true, false, 100, 6.18, "abc"])")}, STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
        {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])")}, STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
        {{STRING("abc")}, STRING("")}, // invalid string
        {{STRING("'abc'")}, STRING("")}, // invalid string
        {{STRING("100x")}, STRING("")}, // invalid int
        {{STRING("6.a8")}, STRING("")}, // invalid double
        {{STRING("{x")}, STRING("")}, // invalid object
        {{STRING("[123, abc]")}, STRING("")} // invalid array
    };

    auto st = check_function<DataTypeJsonb, false>(func_name, input_types, data_set);
    EXPECT_EQ(Status::OK(), st);
}

} // namespace doris::vectorized
