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
    InputTypeSet input_types = {Nullable {TypeIndex::String}};

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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_valid);

    DataSet data_set_invalid = {
            {{STRING("abc")}, Null()}, // invalid string
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("'abc'")}, Null()}, // invalid string
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("100x")}, Null()}, // invalid int
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("6.a8")}, Null()}, // invalid double
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("{x")}, Null()}, // invalid object
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("[123, abc]")}, Null()} // invalid array
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
}

TEST(FunctionJsonbTEST, JsonbParseErrorToNullTest) {
    std::string func_name = "jsonb_parse_error_to_null";
    InputTypeSet input_types = {Nullable {TypeIndex::String}};

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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbParseErrorToValueTest) {
    std::string func_name = "jsonb_parse_error_to_value";
    InputTypeSet input_types = {Nullable {TypeIndex::String}, TypeIndex::String};

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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbParseErrorToInvalidTest) {
    std::string func_name = "jsonb_parse_error_to_invalid";
    InputTypeSet input_types = {Nullable {TypeIndex::String}};

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
            {{STRING("abc")}, STRING("")},                     // invalid string
            {{STRING("'abc'")}, STRING("")},                   // invalid string
            {{STRING("100x")}, STRING("")},                    // invalid int
            {{STRING("6.a8")}, STRING("")},                    // invalid double
            {{STRING("{x")}, STRING("")},                      // invalid object
            {{STRING("[123, abc]")}, STRING("")}               // invalid array
    };

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbParseNullableTest) {
    std::string func_name = "jsonb_parse_nullable";
    InputTypeSet input_types = {TypeIndex::String};

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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_valid);

    DataSet data_set_invalid = {
            {{STRING("abc")}, Null()}, // invalid string
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("'abc'")}, Null()}, // invalid string
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("100x")}, Null()}, // invalid int
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("6.a8")}, Null()}, // invalid double
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("{x")}, Null()}, // invalid object
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("[123, abc]")}, Null()} // invalid array
    };
    check_function<DataTypeJsonb, true>(func_name, input_types, data_set_invalid, true);
}

TEST(FunctionJsonbTEST, JsonbParseNullableErrorToNullTest) {
    std::string func_name = "jsonb_parse_nullable_error_to_null";
    InputTypeSet input_types = {TypeIndex::String};

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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbParseNullableErrorToValueTest) {
    std::string func_name = "jsonb_parse_nullable_error_to_value";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbParseNullableErrorToInvalidTest) {
    std::string func_name = "jsonb_parse_nullable_error_to_invalid";
    InputTypeSet input_types = {TypeIndex::String};

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
            {{STRING("abc")}, STRING("")},                     // invalid string
            {{STRING("'abc'")}, STRING("")},                   // invalid string
            {{STRING("100x")}, STRING("")},                    // invalid int
            {{STRING("6.a8")}, STRING("")},                    // invalid double
            {{STRING("{x")}, STRING("")},                      // invalid object
            {{STRING("[123, abc]")}, STRING("")}               // invalid array
    };

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
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

    check_function<DataTypeJsonb, false>(func_name, input_types, data_set_valid);

    DataSet data_set_invalid = {
            {{STRING("abc")}, Null()}, // invalid string
    };
    check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("'abc'")}, Null()}, // invalid string
    };
    check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("100x")}, Null()}, // invalid int
    };
    check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("6.a8")}, Null()}, // invalid double
    };
    check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("{x")}, Null()}, // invalid object
    };
    check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);

    data_set_invalid = {
            {{STRING("[123, abc]")}, Null()} // invalid array
    };
    check_function<DataTypeJsonb, false>(func_name, input_types, data_set_invalid, true);
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

    check_function<DataTypeJsonb, false>(func_name, input_types, data_set);
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

    check_function<DataTypeJsonb, false>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbExtractTest) {
    std::string func_name = "jsonb_extract";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);

    // jsonb_extract obejct
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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);

    // jsonb_extract array
    data_set = {
            {{Null(), STRING("$[0]")}, Null()},
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                         // empty array
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

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);

    // jsonb_extract $[0].k1
    data_set = {
            {{Null(), STRING("$[0].k1")}, Null()},
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
             STRING(R"("v41")")}, // complex array
    };

    check_function<DataTypeJsonb, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbExtractStringTest) {
    std::string func_name = "jsonb_extract_string";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
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
            {{STRING(R"("abcd")"), STRING("$")}, STRING("abcd")},                          // string
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

    check_function<DataTypeString, true>(func_name, input_types, data_set);

    // jsonb_extract obejct
    data_set = {
            {{Null(), STRING("$.k1")}, Null()},
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                               //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                             // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                        // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()},               // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                              // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                         // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                                // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, STRING("v31")}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                                // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},                        // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()},                 // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
             Null()}, // complex array
    };

    check_function<DataTypeString, true>(func_name, input_types, data_set);

    // jsonb_extract array
    data_set = {
            {{Null(), STRING("$[0]")}, Null()},
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                         // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, STRING("123")},          // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, STRING("456")},          // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, STRING("abc")},   // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, STRING("def")},   // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},          // string array
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
             STRING("abc")}, // multi type array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[6]")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0]")},
             STRING(R"({"k1":"v41","k2":400})")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[1]")},
             STRING("1")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[2]")},
             STRING("a")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[3]")},
             STRING("3.14")}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[4]")},
             Null()}, // complex array
    };

    check_function<DataTypeString, true>(func_name, input_types, data_set);

    // jsonb_extract $[0].k1
    data_set = {
            {{Null(), STRING("$[0].k1")}, Null()},
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
             STRING("v41")}, // complex array
    };

    check_function<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbExtractIntTest) {
    std::string func_name = "jsonb_extract_int";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{Null(), STRING("$")}, Null()},
            {{STRING("null"), STRING("$")}, Null()},
            {{STRING("true"), STRING("$")}, Null()},
            {{STRING("false"), STRING("$")}, Null()},
            {{STRING("100"), STRING("$")}, INT(100)},                      //int8
            {{STRING("10000"), STRING("$")}, INT(10000)},                  // int16
            {{STRING("1000000000"), STRING("$")}, INT(1000000000)},        // int32
            {{STRING("1152921504606846976"), STRING("$")}, Null()},        // int64
            {{STRING("6.18"), STRING("$")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$")}, Null()},                  // string
            {{STRING("{}"), STRING("$")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, Null()}, // object
            {{STRING("[]"), STRING("$")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
             Null()}, // complex array
    };

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);

    // jsonb_extract obejct
    data_set = {
            {{Null(), STRING("$.k1")}, Null()},
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, Null()}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
             Null()}, // complex array
    };

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);

    // jsonb_extract array
    data_set = {
            {{Null(), STRING("$[0]")}, Null()},
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                         // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, INT(123)},               // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, INT(456)},               // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, Null()},          // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, Null()},          // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},          // string array
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

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);

    // jsonb_extract $[0].k1
    data_set = {
            {{Null(), STRING("$[0].k1")}, Null()},
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
             Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k2")},
             INT(400)}, // complex array
    };

    check_function<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbExtractBigIntTest) {
    std::string func_name = "jsonb_extract_bigint";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{Null(), STRING("$")}, Null()},
            {{STRING("null"), STRING("$")}, Null()},
            {{STRING("true"), STRING("$")}, Null()},
            {{STRING("false"), STRING("$")}, Null()},
            {{STRING("100"), STRING("$")}, BIGINT(100)},                                 //int8
            {{STRING("10000"), STRING("$")}, BIGINT(10000)},                             // int16
            {{STRING("1000000000"), STRING("$")}, BIGINT(1000000000)},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, BIGINT(1152921504606846976)}, // int64
            {{STRING("6.18"), STRING("$")}, Null()},                                     // double
            {{STRING(R"("abcd")"), STRING("$")}, Null()},                                // string
            {{STRING("{}"), STRING("$")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, Null()}, // object
            {{STRING("[]"), STRING("$")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
             Null()}, // complex array
    };

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);

    // jsonb_extract obejct
    data_set = {
            {{Null(), STRING("$.k1")}, Null()},
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, Null()}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
             Null()}, // complex array
    };

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);

    // jsonb_extract array
    data_set = {
            {{Null(), STRING("$[0]")}, Null()},
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                         // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, BIGINT(123)},            // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, BIGINT(456)},            // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, Null()},          // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, Null()},          // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},          // string array
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

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);

    // jsonb_extract $[0].k1
    data_set = {
            {{Null(), STRING("$[0].k1")}, Null()},
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
             Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k2")},
             BIGINT(400)}, // complex array
    };

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbExtractDoubleTest) {
    std::string func_name = "jsonb_extract_double";
    InputTypeSet input_types = {TypeIndex::JSONB, TypeIndex::String};

    // jsonb_extract root
    DataSet data_set = {
            {{Null(), STRING("$")}, Null()},
            {{STRING("null"), STRING("$")}, Null()},
            {{STRING("true"), STRING("$")}, Null()},
            {{STRING("false"), STRING("$")}, Null()},
            {{STRING("100"), STRING("$")}, DOUBLE(100)},                                 //int8
            {{STRING("10000"), STRING("$")}, DOUBLE(10000)},                             // int16
            {{STRING("1000000000"), STRING("$")}, DOUBLE(1000000000)},                   // int32
            {{STRING("1152921504606846976"), STRING("$")}, DOUBLE(1152921504606846976)}, // int64
            {{STRING("6.18"), STRING("$")}, DOUBLE(6.18)},                               // double
            {{STRING(R"("abcd")"), STRING("$")}, Null()},                                // string
            {{STRING("{}"), STRING("$")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$")}, Null()}, // object
            {{STRING("[]"), STRING("$")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$")},
             Null()}, // complex array
    };

    check_function<DataTypeFloat64, true>(func_name, input_types, data_set);

    // jsonb_extract obejct
    data_set = {
            {{STRING("null"), STRING("$.k1")}, Null()},
            {{STRING("true"), STRING("$.k1")}, Null()},
            {{STRING("false"), STRING("$.k1")}, Null()},
            {{STRING("100"), STRING("$.k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$.k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$.k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$.k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$.k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$.k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$.k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$.k1")}, Null()}, // object
            {{STRING("[]"), STRING("$.k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$.k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$.k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$.k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$.k1")},
             Null()}, // complex array
    };

    check_function<DataTypeFloat64, true>(func_name, input_types, data_set);

    // jsonb_extract array
    data_set = {
            {{STRING("null"), STRING("$[0]")}, Null()},
            {{STRING("true"), STRING("$[0]")}, Null()},
            {{STRING("false"), STRING("$[0]")}, Null()},
            {{STRING("100"), STRING("$[0]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0]")}, Null()}, // object
            {{STRING("[]"), STRING("$[0]")}, Null()},                         // empty array
            {{STRING("null"), STRING("$[1]")}, Null()},
            {{STRING("true"), STRING("$[1]")}, Null()},
            {{STRING("false"), STRING("$[1]")}, Null()},
            {{STRING("100"), STRING("$[1]")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[1]")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[1]")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[1]")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[1]")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[1]")}, Null()},                  // string
            {{STRING("{}"), STRING("$[1]")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[1]")}, Null()}, // object
            {{STRING("[]"), STRING("$[1]")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0]")}, DOUBLE(123)},            // int array
            {{STRING("[123, 456]"), STRING("$[1]")}, DOUBLE(456)},            // int array
            {{STRING("[123, 456]"), STRING("$[2]")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0]")}, Null()},          // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[1]")}, Null()},          // string array
            {{STRING(R"(["abc", "def"])"), STRING("$[2]")}, Null()},          // string array
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

    check_function<DataTypeFloat64, true>(func_name, input_types, data_set);

    // jsonb_extract $[0].k1
    data_set = {
            {{STRING("null"), STRING("$[0].k1")}, Null()},
            {{STRING("true"), STRING("$[0].k1")}, Null()},
            {{STRING("false"), STRING("$[0].k1")}, Null()},
            {{STRING("100"), STRING("$[0].k1")}, Null()},                        //int8
            {{STRING("10000"), STRING("$[0].k1")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("$[0].k1")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("$[0].k1")}, Null()},        // int64
            {{STRING("6.18"), STRING("$[0].k1")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("$[0].k1")}, Null()},                  // string
            {{STRING("{}"), STRING("$[0].k1")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("$[0].k1")}, Null()}, // object
            {{STRING("[]"), STRING("$[0].k1")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("$[0].k1")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("$[0].k1")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("$[0].k1")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k1")},
             Null()}, // complex array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("$[0].k2")},
             DOUBLE(400)}, // complex array
    };

    check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
}

TEST(FunctionJsonbTEST, JsonbCastToOtherTest) {
    std::string func_name = "CAST";
    InputTypeSet input_types = {Nullable {TypeIndex::JSONB}, ConstedNotnull {TypeIndex::String}};

    // cast to boolean
    DataSet data_set = {
            {{STRING("null"), STRING("UInt8")}, Null()},
            {{STRING("true"), STRING("UInt8")}, BOOLEAN(1)},
            {{STRING("false"), STRING("UInt8")}, BOOLEAN(0)},
            {{STRING("100"), STRING("UInt8")}, Null()},                        //int8
            {{STRING("10000"), STRING("UInt8")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("UInt8")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("UInt8")}, Null()},        // int64
            {{STRING("6.18"), STRING("UInt8")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("UInt8")}, Null()},                  // string
            {{STRING("{}"), STRING("UInt8")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("UInt8")}, Null()}, // object
            {{STRING("[]"), STRING("UInt8")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("UInt8")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("UInt8")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("UInt8")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("UInt8")},
             Null()}, // complex array
    };

    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        check_function<DataTypeUInt8, true>(func_name, input_types, const_dataset);
    }

    // cast to TINYINT
    data_set = {
            {{STRING("null"), STRING("Int8")}, Null()},
            {{STRING("true"), STRING("Int8")}, Null()},
            {{STRING("false"), STRING("Int8")}, Null()},
            {{STRING("100"), STRING("Int8")}, TINYINT(100)},                  //int8
            {{STRING("10000"), STRING("Int8")}, Null()},                      // int16
            {{STRING("1000000000"), STRING("Int8")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("Int8")}, Null()},        // int64
            {{STRING("6.18"), STRING("Int8")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("Int8")}, Null()},                  // string
            {{STRING("{}"), STRING("Int8")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("Int8")}, Null()}, // object
            {{STRING("[]"), STRING("Int8")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("Int8")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("Int8")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("Int8")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("Int8")},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        check_function<DataTypeInt8, true>(func_name, input_types, const_dataset);
    }

    // cast to SMALLINT
    data_set = {
            {{STRING("null"), STRING("Int16")}, Null()},
            {{STRING("true"), STRING("Int16")}, Null()},
            {{STRING("false"), STRING("Int16")}, Null()},
            {{STRING("100"), STRING("Int16")}, SMALLINT(100)},                 //int8
            {{STRING("10000"), STRING("Int16")}, SMALLINT(10000)},             // int16
            {{STRING("1000000000"), STRING("Int16")}, Null()},                 // int32
            {{STRING("1152921504606846976"), STRING("Int16")}, Null()},        // int64
            {{STRING("6.18"), STRING("Int16")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("Int16")}, Null()},                  // string
            {{STRING("{}"), STRING("Int16")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("Int16")}, Null()}, // object
            {{STRING("[]"), STRING("Int16")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("Int16")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("Int16")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("Int16")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("Int16")},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        check_function<DataTypeInt16, true>(func_name, input_types, const_dataset);
    }

    // cast to INT
    data_set = {
            {{STRING("null"), STRING("Int32")}, Null()},
            {{STRING("true"), STRING("Int32")}, Null()},
            {{STRING("false"), STRING("Int32")}, Null()},
            {{STRING("100"), STRING("Int32")}, INT(100)},                      //int8
            {{STRING("10000"), STRING("Int32")}, INT(10000)},                  // int16
            {{STRING("1000000000"), STRING("Int32")}, INT(1000000000)},        // int32
            {{STRING("1152921504606846976"), STRING("Int32")}, Null()},        // int64
            {{STRING("6.18"), STRING("Int32")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("Int32")}, Null()},                  // string
            {{STRING("{}"), STRING("Int32")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("Int32")}, Null()}, // object
            {{STRING("[]"), STRING("Int32")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("Int32")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("Int32")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("Int32")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("Int32")},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        check_function<DataTypeInt32, true>(func_name, input_types, const_dataset);
    }

    // cast to BIGINT
    data_set = {
            {{STRING("null"), STRING("Int64")}, Null()},
            {{STRING("true"), STRING("Int64")}, Null()},
            {{STRING("false"), STRING("Int64")}, Null()},
            {{STRING("100"), STRING("Int64")}, BIGINT(100)},               //int8
            {{STRING("10000"), STRING("Int64")}, BIGINT(10000)},           // int16
            {{STRING("1000000000"), STRING("Int64")}, BIGINT(1000000000)}, // int32
            {{STRING("1152921504606846976"), STRING("Int64")},
             BIGINT(1152921504606846976)},                                     // int64
            {{STRING("6.18"), STRING("Int64")}, Null()},                       // double
            {{STRING(R"("abcd")"), STRING("Int64")}, Null()},                  // string
            {{STRING("{}"), STRING("Int64")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("Int64")}, Null()}, // object
            {{STRING("[]"), STRING("Int64")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("Int64")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("Int64")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("Int64")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("Int64")},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        check_function<DataTypeInt64, true>(func_name, input_types, const_dataset);
    }

    // cast to DOUBLE
    data_set = {
            {{STRING("null"), STRING("Float64")}, Null()},
            {{STRING("true"), STRING("Float64")}, Null()},
            {{STRING("false"), STRING("Float64")}, Null()},
            {{STRING("100"), STRING("Float64")}, DOUBLE(100)},               //int8
            {{STRING("10000"), STRING("Float64")}, DOUBLE(10000)},           // int16
            {{STRING("1000000000"), STRING("Float64")}, DOUBLE(1000000000)}, // int32
            {{STRING("1152921504606846976"), STRING("Float64")},
             DOUBLE(1152921504606846976)},                                       // int64
            {{STRING("6.18"), STRING("Float64")}, DOUBLE(6.18)},                 // double
            {{STRING(R"("abcd")"), STRING("Float64")}, Null()},                  // string
            {{STRING("{}"), STRING("Float64")}, Null()},                         // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("Float64")}, Null()}, // object
            {{STRING("[]"), STRING("Float64")}, Null()},                         // empty array
            {{STRING("[123, 456]"), STRING("Float64")}, Null()},                 // int array
            {{STRING(R"(["abc", "def"])"), STRING("Float64")}, Null()},          // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("Float64")},
             Null()}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("Float64")},
             Null()}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        check_function<DataTypeFloat64, true>(func_name, input_types, const_dataset);
    }

    // cast to STRING
    data_set = {
            {{STRING("null"), STRING("String")}, STRING("null")},
            {{STRING("true"), STRING("String")}, STRING("true")},
            {{STRING("false"), STRING("String")}, STRING("false")},
            {{STRING("100"), STRING("String")}, STRING("100")},               //int8
            {{STRING("10000"), STRING("String")}, STRING("10000")},           // int16
            {{STRING("1000000000"), STRING("String")}, STRING("1000000000")}, // int32
            {{STRING("1152921504606846976"), STRING("String")},
             STRING("1152921504606846976")},                                // int64
            {{STRING("6.18"), STRING("String")}, STRING("6.18")},           // double
            {{STRING(R"("abcd")"), STRING("String")}, STRING(R"("abcd")")}, // string
            {{STRING("{}"), STRING("String")}, STRING("{}")},               // empty object
            {{STRING(R"({"k1":"v31", "k2": 300})"), STRING("String")},
             STRING(R"({"k1":"v31","k2":300})")},                            // object
            {{STRING("[]"), STRING("String")}, STRING("[]")},                // empty array
            {{STRING("[123, 456]"), STRING("String")}, STRING("[123,456]")}, // int array
            {{STRING(R"(["abc", "def"])"), STRING("String")},
             STRING(R"(["abc","def"])")}, // string array
            {{STRING(R"([null, true, false, 100, 6.18, "abc"])"), STRING("String")},
             STRING(R"([null,true,false,100,6.18,"abc"])")}, // multi type array
            {{STRING(R"([{"k1":"v41", "k2": 400}, 1, "a", 3.14])"), STRING("String")},
             STRING(R"([{"k1":"v41","k2":400},1,"a",3.14])")}, // complex array
    };
    for (const auto& row : data_set) {
        DataSet const_dataset = {row};
        check_function<DataTypeString, true>(func_name, input_types, const_dataset);
    }
}

TEST(FunctionJsonbTEST, JsonbCastFromOtherTest) {
    // CAST Nullable(X) to Nullable(JSONB)
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::UInt8}, ConstedNotnull {TypeIndex::String}},
            {{{BOOLEAN(1), STRING("Jsonb")}, STRING("true")}});
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::UInt8}, ConstedNotnull {TypeIndex::String}},
            {{{BOOLEAN(0), STRING("Jsonb")}, STRING("false")}});
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::Int8}, ConstedNotnull {TypeIndex::String}},
            {{{TINYINT(100), STRING("Jsonb")}, STRING("100")}});
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::Int16}, ConstedNotnull {TypeIndex::String}},
            {{{SMALLINT(10000), STRING("Jsonb")}, STRING("10000")}});
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::Int32}, ConstedNotnull {TypeIndex::String}},
            {{{INT(1000000000), STRING("Jsonb")}, STRING("1000000000")}});
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::Int64}, ConstedNotnull {TypeIndex::String}},
            {{{BIGINT(1152921504606846976), STRING("Jsonb")}, STRING("1152921504606846976")}});
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::Float64}, ConstedNotnull {TypeIndex::String}},
            {{{DOUBLE(6.18), STRING("Jsonb")}, STRING("6.18")}});
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::String}, ConstedNotnull {TypeIndex::String}},
            {{{STRING(R"(abcd)"), STRING("Jsonb")}, Null()}}); // should fail
    check_function<DataTypeJsonb, true>(
            "CAST", {Nullable {TypeIndex::String}, ConstedNotnull {TypeIndex::String}},
            {{{STRING(R"("abcd")"), STRING("Jsonb")}, STRING(R"("abcd")")}});

    // CAST X to JSONB
    check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {TypeIndex::UInt8}, ConstedNotnull {TypeIndex::String}},
            {{{BOOLEAN(1), STRING("Jsonb")}, STRING("true")}});
    check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {TypeIndex::UInt8}, ConstedNotnull {TypeIndex::String}},
            {{{BOOLEAN(0), STRING("Jsonb")}, STRING("false")}});
    check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {TypeIndex::Int8}, ConstedNotnull {TypeIndex::String}},
            {{{TINYINT(100), STRING("Jsonb")}, STRING("100")}});
    check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {TypeIndex::Int16}, ConstedNotnull {TypeIndex::String}},
            {{{SMALLINT(10000), STRING("Jsonb")}, STRING("10000")}});
    check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {TypeIndex::Int32}, ConstedNotnull {TypeIndex::String}},
            {{{INT(1000000000), STRING("Jsonb")}, STRING("1000000000")}});
    check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {TypeIndex::Int64}, ConstedNotnull {TypeIndex::String}},
            {{{BIGINT(1152921504606846976), STRING("Jsonb")}, STRING("1152921504606846976")}});
    check_function<DataTypeJsonb, false>(
            "CAST", {Notnull {TypeIndex::Float64}, ConstedNotnull {TypeIndex::String}},
            {{{DOUBLE(6.18), STRING("Jsonb")}, STRING("6.18")}});
    // String to JSONB should always be Nullable
    check_function<DataTypeJsonb, true>(
            "CAST", {Notnull {TypeIndex::String}, ConstedNotnull {TypeIndex::String}},
            {{{STRING(R"(abcd)"), STRING("Jsonb")}, Null()}}); // should fail
    check_function<DataTypeJsonb, true>(
            "CAST", {Notnull {TypeIndex::String}, ConstedNotnull {TypeIndex::String}},
            {{{STRING(R"("abcd")"), STRING("Jsonb")}, STRING(R"("abcd")")}});
}

} // namespace doris::vectorized
