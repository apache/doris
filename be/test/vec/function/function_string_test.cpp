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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "function_test_util.h"
#include "util/encryption_util.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(function_string_test, function_string_substr_test) {
    std::string func_name = "substr";

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(1)}, std::string("A")},
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(5)}, std::string("AbCdE")},
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(100)},
                 std::string("AbCdEfg")},
                {{std::string("AbCdEfg"), std::int32_t(1), Null()}, Null()},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(1)}, std::string("E")},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(5)}, std::string("Efg")},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(100)}, std::string("Efg")},
                {{std::string("AbCdEfg"), std::int32_t(5), Null()}, Null()},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(1)}, std::string("g")},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(5)}, std::string("g")},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(100)}, std::string("g")},
                {{std::string("AbCdEfg"), std::int32_t(-1), Null()}, Null()},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), Null()}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(1)}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(5)}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(100)}, Null()},
                {{std::string("AbCdEfg"), Null(), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(1)}, std::string("H")},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(5)}, std::string("HELLO")},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(100)},
                 std::string("HELLO123")},
                {{std::string("HELLO123"), std::int32_t(1), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(1)}, std::string("O")},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(5)}, std::string("O123")},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(100)},
                 std::string("O123")},
                {{std::string("HELLO123"), std::int32_t(5), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(1)}, std::string("3")},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(5)}, std::string("3")},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(100)}, std::string("3")},
                {{std::string("HELLO123"), std::int32_t(-1), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), Null()}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(1)}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(5)}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(100)}, Null()},
                {{std::string("HELLO123"), Null(), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(1)}, std::string("你")},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(5)},
                 std::string("你好HEL")},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(100)},
                 std::string("你好HELLO")},
                {{std::string("你好HELLO"), std::int32_t(1), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(1)}, std::string("L")},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(5)}, std::string("LLO")},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(100)},
                 std::string("LLO")},
                {{std::string("你好HELLO"), std::int32_t(5), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(1)}, std::string("O")},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(5)}, std::string("O")},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(100)}, std::string("O")},
                {{std::string("你好HELLO"), std::int32_t(-1), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), Null()}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(1)}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(5)}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(100)}, Null()},
                {{std::string("你好HELLO"), Null(), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(1)}, std::string("1")},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(5)}, std::string("123AB")},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(100)},
                 std::string("123ABC_")},
                {{std::string("123ABC_"), std::int32_t(1), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(1)}, std::string("B")},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(5)}, std::string("BC_")},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(100)}, std::string("BC_")},
                {{std::string("123ABC_"), std::int32_t(5), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(1)}, std::string("_")},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(5)}, std::string("_")},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(100)}, std::string("_")},
                {{std::string("123ABC_"), std::int32_t(-1), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), Null()}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(1)}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(5)}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(100)}, Null()},
                {{std::string("123ABC_"), Null(), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(1)}, std::string("M")},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(5)},
                 std::string("MYtes")},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(100)},
                 std::string("MYtestSTR")},
                {{std::string("MYtestSTR"), std::int32_t(1), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(1)}, std::string("s")},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(5)},
                 std::string("stSTR")},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(100)},
                 std::string("stSTR")},
                {{std::string("MYtestSTR"), std::int32_t(5), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(1)}, std::string("R")},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(5)}, std::string("R")},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(100)}, std::string("R")},
                {{std::string("MYtestSTR"), std::int32_t(-1), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), Null()}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(1)}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(5)}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(100)}, Null()},
                {{std::string("MYtestSTR"), Null(), Null()}, Null()},
                {{std::string(""), std::int32_t(1), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(1), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(1), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(1), Null()}, Null()},
                {{std::string(""), std::int32_t(5), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(5), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(5), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(5), Null()}, Null()},
                {{std::string(""), std::int32_t(-1), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(-1), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(-1), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(-1), Null()}, Null()},
                {{std::string(""), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(100), Null()}, Null()},
                {{std::string(""), Null(), std::int32_t(1)}, Null()},
                {{std::string(""), Null(), std::int32_t(5)}, Null()},
                {{std::string(""), Null(), std::int32_t(-1)}, Null()},
                {{std::string(""), Null(), std::int32_t(100)}, Null()},
                {{std::string(""), Null(), Null()}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(1), Null()}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(5), Null()}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(-1), Null()}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(100), Null()}, Null()},
                {{Null(), Null(), std::int32_t(1)}, Null()},
                {{Null(), Null(), std::int32_t(5)}, Null()},
                {{Null(), Null(), std::int32_t(-1)}, Null()},
                {{Null(), Null(), std::int32_t(100)}, Null()},
                {{Null(), Null(), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(1)}, std::string("A")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(5)},
                 std::string("A,b,C")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(100)},
                 std::string("A,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(1)}, std::string("C")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(5)},
                 std::string("C,D,_")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(100)},
                 std::string("C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(1)}, std::string("E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(5)}, std::string("E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(100)},
                 std::string("E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(100)},
                 std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(1)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(5)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(100)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(1)},
                 std::string("1")},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(5)},
                 std::string("12343")},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(100)},
                 std::string("1234321312312")},
                {{std::string("1234321312312"), std::int32_t(1), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(1)},
                 std::string("3")},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(5)},
                 std::string("32131")},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(100)},
                 std::string("321312312")},
                {{std::string("1234321312312"), std::int32_t(5), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(1)},
                 std::string("2")},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(5)},
                 std::string("2")},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(100)},
                 std::string("2")},
                {{std::string("1234321312312"), std::int32_t(-1), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(5)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(100)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), Null()}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(1)}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(5)}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(100)}, Null()},
                {{std::string("1234321312312"), Null(), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(1)},
                 std::string("h")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(5)},
                 std::string("heh1h")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(100)},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(1)},
                 std::string("h")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(5)},
                 std::string("h2_!u")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(100)},
                 std::string("h2_!u@_u@i$o%ll_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(1)},
                 std::string("_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(5)},
                 std::string("_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(100)},
                 std::string("_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(5)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(100)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(1)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(5)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(100)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), Null()}, Null()},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("ABC"), std::int32_t(123)}, std::string("")},
                {{std::string("ABC"), std::int32_t(321)}, std::string("")},
                {{std::string("ABC"), std::int32_t(1)}, std::string("ABC")},
                {{std::string("ABC"), std::int32_t(-1)}, std::string("C")},
                {{std::string("ABC"), Null()}, Null()},
                {{std::string("ABC"), std::int32_t(100)}, std::string("")},
                {{std::string("ABC"), std::int32_t(-100)}, std::string("")},
                {{std::string("ABB"), std::int32_t(123)}, std::string("")},
                {{std::string("ABB"), std::int32_t(321)}, std::string("")},
                {{std::string("ABB"), std::int32_t(1)}, std::string("ABB")},
                {{std::string("ABB"), std::int32_t(-1)}, std::string("B")},
                {{std::string("ABB"), Null()}, Null()},
                {{std::string("ABB"), std::int32_t(100)}, std::string("")},
                {{std::string("ABB"), std::int32_t(-100)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(123)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(321)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(1)}, std::string("HEHE")},
                {{std::string("HEHE"), std::int32_t(-1)}, std::string("E")},
                {{std::string("HEHE"), Null()}, Null()},
                {{std::string("HEHE"), std::int32_t(100)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(-100)}, std::string("")},
                {{Null(), std::int32_t(123)}, Null()},
                {{Null(), std::int32_t(321)}, Null()},
                {{Null(), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(-1)}, Null()},
                {{Null(), Null()}, Null()},
                {{Null(), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(-100)}, Null()},

        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_string_substr_for_zero_test) {
    std::string func_name = "substr_for_zero";

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("AbCdEfg"), std::int32_t(0), std::int32_t(1)}, std::string("A")},
                {{std::string("AbCdEfg"), std::int32_t(0), std::int32_t(5)}, std::string("AbCdE")},
                {{std::string("AbCdEfg"), std::int32_t(0), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(0), std::int32_t(100)},
                 std::string("AbCdEfg")},
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(1)}, std::string("A")},
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(5)}, std::string("AbCdE")},
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(1), std::int32_t(100)},
                 std::string("AbCdEfg")},
                {{std::string("AbCdEfg"), std::int32_t(1), Null()}, Null()},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(1)}, std::string("E")},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(5)}, std::string("Efg")},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(5), std::int32_t(100)}, std::string("Efg")},
                {{std::string("AbCdEfg"), std::int32_t(5), Null()}, Null()},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(1)}, std::string("g")},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(5)}, std::string("g")},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(-1), std::int32_t(100)}, std::string("g")},
                {{std::string("AbCdEfg"), std::int32_t(-1), Null()}, Null()},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(100), Null()}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(1)}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(5)}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("AbCdEfg"), Null(), std::int32_t(100)}, Null()},
                {{std::string("AbCdEfg"), Null(), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(1)}, std::string("H")},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(5)}, std::string("HELLO")},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(1), std::int32_t(100)},
                 std::string("HELLO123")},
                {{std::string("HELLO123"), std::int32_t(1), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(1)}, std::string("O")},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(5)}, std::string("O123")},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(5), std::int32_t(100)},
                 std::string("O123")},
                {{std::string("HELLO123"), std::int32_t(5), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(1)}, std::string("3")},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(5)}, std::string("3")},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(-1), std::int32_t(100)}, std::string("3")},
                {{std::string("HELLO123"), std::int32_t(-1), Null()}, Null()},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(100), Null()}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(1)}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(5)}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("HELLO123"), Null(), std::int32_t(100)}, Null()},
                {{std::string("HELLO123"), Null(), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(1)}, std::string("你")},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(5)},
                 std::string("你好HEL")},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(1), std::int32_t(100)},
                 std::string("你好HELLO")},
                {{std::string("你好HELLO"), std::int32_t(1), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(1)}, std::string("L")},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(5)}, std::string("LLO")},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(5), std::int32_t(100)},
                 std::string("LLO")},
                {{std::string("你好HELLO"), std::int32_t(5), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(1)}, std::string("O")},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(5)}, std::string("O")},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(-1), std::int32_t(100)}, std::string("O")},
                {{std::string("你好HELLO"), std::int32_t(-1), Null()}, Null()},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(100), Null()}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(1)}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(5)}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("你好HELLO"), Null(), std::int32_t(100)}, Null()},
                {{std::string("你好HELLO"), Null(), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(1)}, std::string("1")},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(5)}, std::string("123AB")},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(1), std::int32_t(100)},
                 std::string("123ABC_")},
                {{std::string("123ABC_"), std::int32_t(1), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(1)}, std::string("B")},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(5)}, std::string("BC_")},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(5), std::int32_t(100)}, std::string("BC_")},
                {{std::string("123ABC_"), std::int32_t(5), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(1)}, std::string("_")},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(5)}, std::string("_")},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(-1), std::int32_t(100)}, std::string("_")},
                {{std::string("123ABC_"), std::int32_t(-1), Null()}, Null()},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(100), Null()}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(1)}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(5)}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("123ABC_"), Null(), std::int32_t(100)}, Null()},
                {{std::string("123ABC_"), Null(), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(1)}, std::string("M")},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(5)},
                 std::string("MYtes")},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(1), std::int32_t(100)},
                 std::string("MYtestSTR")},
                {{std::string("MYtestSTR"), std::int32_t(1), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(1)}, std::string("s")},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(5)},
                 std::string("stSTR")},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(5), std::int32_t(100)},
                 std::string("stSTR")},
                {{std::string("MYtestSTR"), std::int32_t(5), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(1)}, std::string("R")},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(5)}, std::string("R")},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(-1), std::int32_t(100)}, std::string("R")},
                {{std::string("MYtestSTR"), std::int32_t(-1), Null()}, Null()},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(100), Null()}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(1)}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(5)}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("MYtestSTR"), Null(), std::int32_t(100)}, Null()},
                {{std::string("MYtestSTR"), Null(), Null()}, Null()},
                {{std::string(""), std::int32_t(1), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(1), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(1), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(1), Null()}, Null()},
                {{std::string(""), std::int32_t(5), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(5), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(5), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(5), Null()}, Null()},
                {{std::string(""), std::int32_t(-1), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(-1), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(-1), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(-1), Null()}, Null()},
                {{std::string(""), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string(""), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(100), std::int32_t(100)}, std::string("")},
                {{std::string(""), std::int32_t(100), Null()}, Null()},
                {{std::string(""), Null(), std::int32_t(1)}, Null()},
                {{std::string(""), Null(), std::int32_t(5)}, Null()},
                {{std::string(""), Null(), std::int32_t(-1)}, Null()},
                {{std::string(""), Null(), std::int32_t(100)}, Null()},
                {{std::string(""), Null(), Null()}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(1), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(1), Null()}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(5), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(5), Null()}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(-1), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(-1), Null()}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(5)}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(-1)}, Null()},
                {{Null(), std::int32_t(100), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(100), Null()}, Null()},
                {{Null(), Null(), std::int32_t(1)}, Null()},
                {{Null(), Null(), std::int32_t(5)}, Null()},
                {{Null(), Null(), std::int32_t(-1)}, Null()},
                {{Null(), Null(), std::int32_t(100)}, Null()},
                {{Null(), Null(), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(1)}, std::string("A")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(5)},
                 std::string("A,b,C")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), std::int32_t(100)},
                 std::string("A,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(1), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(1)}, std::string("C")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(5)},
                 std::string("C,D,_")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), std::int32_t(100)},
                 std::string("C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(5), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(1)}, std::string("E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(5)}, std::string("E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), std::int32_t(100)},
                 std::string("E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(5)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(-1)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), std::int32_t(100)},
                 std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(100), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(1)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(5)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), std::int32_t(100)}, Null()},
                {{std::string("A,b,C,D,_E"), Null(), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(1)},
                 std::string("1")},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(5)},
                 std::string("12343")},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(1), std::int32_t(100)},
                 std::string("1234321312312")},
                {{std::string("1234321312312"), std::int32_t(1), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(1)},
                 std::string("3")},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(5)},
                 std::string("32131")},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(5), std::int32_t(100)},
                 std::string("321312312")},
                {{std::string("1234321312312"), std::int32_t(5), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(1)},
                 std::string("2")},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(5)},
                 std::string("2")},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(-1), std::int32_t(100)},
                 std::string("2")},
                {{std::string("1234321312312"), std::int32_t(-1), Null()}, Null()},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(5)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(-1)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), std::int32_t(100)},
                 std::string("")},
                {{std::string("1234321312312"), std::int32_t(100), Null()}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(1)}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(5)}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("1234321312312"), Null(), std::int32_t(100)}, Null()},
                {{std::string("1234321312312"), Null(), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(1)},
                 std::string("h")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(5)},
                 std::string("heh1h")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), std::int32_t(100)},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(1), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(1)},
                 std::string("h")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(5)},
                 std::string("h2_!u")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), std::int32_t(100)},
                 std::string("h2_!u@_u@i$o%ll_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(5), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(1)},
                 std::string("_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(5)},
                 std::string("_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), std::int32_t(100)},
                 std::string("_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(-1), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(5)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(-1)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), std::int32_t(100)},
                 std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::int32_t(100), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(1)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(5)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), std::int32_t(100)}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null(), Null()}, Null()},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("ABC"), std::int32_t(0)}, std::string("ABC")},
                {{std::string("ABC"), std::int32_t(123)}, std::string("")},
                {{std::string("ABC"), std::int32_t(321)}, std::string("")},
                {{std::string("ABC"), std::int32_t(1)}, std::string("ABC")},
                {{std::string("ABC"), std::int32_t(-1)}, std::string("C")},
                {{std::string("ABC"), Null()}, Null()},
                {{std::string("ABC"), std::int32_t(100)}, std::string("")},
                {{std::string("ABC"), std::int32_t(-100)}, std::string("")},
                {{std::string("ABB"), std::int32_t(123)}, std::string("")},
                {{std::string("ABB"), std::int32_t(321)}, std::string("")},
                {{std::string("ABB"), std::int32_t(1)}, std::string("ABB")},
                {{std::string("ABB"), std::int32_t(-1)}, std::string("B")},
                {{std::string("ABB"), Null()}, Null()},
                {{std::string("ABB"), std::int32_t(100)}, std::string("")},
                {{std::string("ABB"), std::int32_t(-100)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(123)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(321)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(1)}, std::string("HEHE")},
                {{std::string("HEHE"), std::int32_t(-1)}, std::string("E")},
                {{std::string("HEHE"), Null()}, Null()},
                {{std::string("HEHE"), std::int32_t(100)}, std::string("")},
                {{std::string("HEHE"), std::int32_t(-100)}, std::string("")},
                {{Null(), std::int32_t(123)}, Null()},
                {{Null(), std::int32_t(321)}, Null()},
                {{Null(), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(-1)}, Null()},
                {{Null(), Null()}, Null()},
                {{Null(), std::int32_t(100)}, Null()},
                {{Null(), std::int32_t(-100)}, Null()},

        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_string_strright_test) {
    std::string func_name = "strright";

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("asd"), 1}, std::string("d")},
                {{std::string("hello word"), -2}, std::string("ello word")},
                {{std::string("hello word"), 20}, std::string("hello word")},
                {{std::string("HELLO,!^%"), 2}, std::string("^%")},
                {{std::string(""), 3}, std::string("")},
                {{Null(), 3}, Null()},
                {{std::string("12345"), 10}, std::string("12345")},
                {{std::string("12345"), -10}, std::string("")},
                {{std::string(""), Null()}, Null()},
                {{Null(), -100}, Null()},
                {{std::string("12345"), 12345}, std::string("12345")},
                {{std::string(""), 1}, std::string()},
                {{std::string("a b c d _ %"), -3}, std::string("b c d _ %")},
                {{std::string(""), Null()}, Null()},
                {{std::string("hah hah"), -1}, std::string("hah hah")},
                {{std::string("🤣"), -1}, std::string("🤣")},
                {{std::string("🤣😃😄"), -2}, std::string("😃😄")},
                {{std::string("12345"), 6}, std::string("12345")},
                {{std::string("12345"), 12345}, std::string("12345")},
                {{std::string("-12345"), -1}, std::string("-12345")},
                {{std::string("-12345"), -12345}, std::string()},
                {{Null(), -12345}, Null()},
                {{std::string("😡"), Null()}, Null()},
                {{std::string("🤣"), 0}, std::string()},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_string_strleft_test) {
    std::string func_name = "strleft";
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("asd"), 1}, std::string("a")},
                {{std::string("hel  lo  "), 5}, std::string("hel  ")},
                {{std::string("hello word"), 20}, std::string("hello word")},
                {{std::string("HELLO,!^%"), 7}, std::string("HELLO,!")},
                {{std::string(""), 2}, std::string("")},
                {{std::string(""), -2}, std::string("")},
                {{std::string(""), 0}, std::string("")},
                {{std::string("123"), 0}, std::string("")},
                {{Null(), 3}, Null()},
                {{std::string("12321"), 3}, std::string("123")},
                {{std::string("123"), 0}, std::string()},
                {{std::string("123"), -1}, std::string()},
                {{std::string("123"), Null()}, Null()},
                {{Null(), 0}, Null()},
                {{std::string("🫢"), 0}, std::string()},
                {{std::string("123"), 4}, std::string("123")},
                {{std::string("哈哈hh🤣"), 1}, std::string("哈")},
                {{std::string("哈哈hh🤣"), 100}, std::string("哈哈hh🤣")},
                {{std::string("mnzxv"), -1}, std::string()},
                {{std::string("123"), Null()}, Null()},
                {{std::string(1e6, 'a'), Null()}, Null()},
                {{std::string(""), -100}, std::string()},
                {{std::string("abcdef"), 4}, std::string("abcd")},
                {{std::string("NULL"), 3}, std::string("NUL")},
                {{std::string("NuLl"), 4}, std::string("NuLl")},
                {{Null(), 123}, Null()},
                {{std::string("Includes numbers 123456."), 10}, std::string("Includes n")},
                {{std::string("CapsAndLowercase"), 6}, std::string("CapsAn")},
                {{std::string("1234567890"), 3}, std::string("123")},
                {{std::string("Punctuation, too!"), 13}, std::string("Punctuation, ")},
                {{std::string("Short"), 10}, std::string("Short")},
                {{std::string("No more than needed"), 18}, std::string("No more than neede")},
                {{std::string("1234567890"), -5}, std::string("")},
                {{std::string("Contains space at end "), 21}, std::string("Contains space at end")},
                {{std::string("Chinese字符"), 7}, std::string("Chinese")},
                {{std::string("日本語"), 2}, std::string("日本")},
                {{std::string("Emoji 😊😂🤣"), 5}, std::string("Emoji")},
                {{std::string("SpecialCharacters#@!"), 11}, std::string("SpecialChar")},
                {{std::string("Numbers123456"), 7}, std::string("Numbers")},
                {{std::string("1234567890"), 0}, std::string("")},
                {{std::string("Empty"), Null()}, Null()},
                {{Null(), 5}, Null()},
                {{std::string("Leading and trailing "), 7}, std::string("Leading")},
                {{Null(), -10}, Null()},
                {{std::string("One Unicode 🔥"), 12}, std::string("One Unicode ")},
                {{std::string("🌟💫✨"), 2}, std::string("🌟💫")},
                {{std::string("New"), 1}, std::string("N")},
                {{std::string("New"), Null()}, Null()},
                {{Null(), 1}, Null()},
                {{std::string("Two words"), 9}, std::string("Two words")},
                {{std::string("Boundary case"), 13}, std::string("Boundary case")},
                {{std::string("Boundary case"), 14}, std::string("Boundary case")},
                {{std::string("Multi\nLine\nString"), 5}, std::string("Multi")},
                {{std::string(" SingleQuote'"), 12}, std::string(" SingleQuote")},
                {{std::string("\"DoubleQuote"), 12}, std::string("\"DoubleQuote")}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_string_lower_test) {
    std::string func_name = "lower";
    {
        BaseInputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("AbCdEfg")}, std::string("abcdefg")},
                {{std::string("HELLO123")}, std::string("hello123")},
                {{std::string("你好HELLO")}, std::string("你好hello")},
                {{std::string("ÀÇ")}, std::string("àç")},
                {{std::string("ÀÇAC123")}, std::string("àçac123")},
                {{std::string("İstanbul")}, std::string("i̇stanbul")},
                {{std::string("KIZILAY")}, std::string("kizilay")},
                {{std::string("GROSSE")}, std::string("grosse")},
                {{std::string("Å")}, std::string("å")},
                {{std::string("ΣΟΦΟΣ")}, std::string("σοφος")},
                {{std::string("123ABC_")}, std::string("123abc_")},
                {{std::string("MYtestSTR")}, std::string("myteststr")},
                {{std::string("")}, std::string("")},
                {{Null()}, Null()},
                //bug{{std::string("ΔΟΚΙΜΑΣΤΙΚΌ ΚΕΊΜΕΝΟ")}, std::string("δοκιμαστικό κείμενο")},
        };
        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
        check_function_all_arg_comb<DataTypeString, true>(std::string("lcase"), input_types,
                                                          data_set);
    }
}

TEST(function_string_test, function_string_upper_test) {
    std::string func_name = "upper";
    {
        BaseInputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("AbCdEfg")}, std::string("ABCDEFG")},
                {{std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("MYtestSTR")}, std::string("MYTESTSTR")},
                {{std::string("àç")}, std::string("ÀÇ")},
                {{std::string("straße")}, std::string("STRASSE")},
                {{std::string("àçac123")}, std::string("ÀÇAC123")},
                {{std::string("ﬃ")}, std::string("FFI")},
                {{std::string("ǅ")}, std::string("Ǆ")},
                {{std::string("Ångström")}, std::string("ÅNGSTRÖM")},
                {{std::string("")}, std::string("")},
                {{Null()}, Null()},
                {{std::string("abcdefghijklmnopqrstuvwxyz")},
                 std::string("ABCDEFGHIJKLMNOPQRSTUVWXYZ")},
                {{std::string("`~!@#$%^&*()_-+=|\\{}[]:\";'<>?,./")},
                 std::string("`~!@#$%^&*()_-+=|\\{}[]:\";'<>?,./")},
                {{std::string("MixedCASEabcdefghijklmnopqrstuvwxyz")},
                 std::string("MIXEDCASEABCDEFGHIJKLMNOPQRSTUVWXYZ")},
                {{std::string("1234567890")}, std::string("1234567890")},
                {{std::string("!@#$%^&*( ) _+-={}[]|\\:;\"'<>?,./")},
                 std::string("!@#$%^&*( ) _+-={}[]|\\:;\"'<>?,./")},
                {{std::string("Text with spaces")}, std::string("TEXT WITH SPACES")},
                //bug{{std::string("текст на русском")}, std::string("ТЕКСТ НА РУССКОМ")},
                {{std::string("文字列のテスト")}, std::string("文字列のテスト")},
                {{std::string("測試字符串")}, std::string("測試字符串")},
                {{std::string("테스트 문자열")}, std::string("테스트 문자열")},
                {{std::string("أحرف مختلفة")}, std::string("أحرف مختلفة")},
                //bug{{std::string("Δοκιµαστικό κείµενο")}, std::string("ΔΟΚΙΜΑΣΤΙΚΌ ΚΕΊΜΕΝΟ")},
                {{std::string("โพสต์ทดสอบ")}, std::string("โพสต์ทดสอบ")},
                {{std::string("יידיש טעקסט")}, std::string("יידיש טעקסט")},
                //bug{{std::string("Exámplè wïth âccents")}, std::string("EXÁMPLÈ WÏTH ÂCCENTS")},
                {{std::string("ⓔⓧⓐⓜⓟⓛⓔ ⓦⓘⓣⓗ ⓒⓘⓡⓒⓛⓔ ⓛⓔⓣⓣⓔⓡⓢ")},
                 std::string("ⒺⓍⒶⓂⓅⓁⒺ ⓌⒾⓉⒽ ⒸⒾⓇⒸⓁⒺ ⓁⒺⓉⓉⒺⓇⓈ")},
                {{std::string("🅴🆇🅰🅼🅿🅻🅴 🆆🅸🆃🅷 🆂🆀🆄🅰🆁🅴 🅻🅴🆃🆃🅴🆁🆂")},
                 std::string("🅴🆇🅰🅼🅿🅻🅴 🆆🅸🆃🅷 🆂🆀🆄🅰🆁🅴 🅻🅴🆃🆃🅴🆁🆂")},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
        check_function_all_arg_comb<DataTypeString, true>(std::string("ucase"), input_types,
                                                          data_set);
    }
}

TEST(function_string_test, function_string_trim_test) {
    std::string func_name = "trim";
    {
        BaseInputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("    paddedStringNoEscape   ")}, std::string("paddedStringNoEscape")},
                {{std::string("singleWord")}, std::string("singleWord")},
                {{std::string("   leadingSpaceStringNoEscape")},
                 std::string("leadingSpaceStringNoEscape")},
                {{std::string("trailingSpaceStringNoEscape   ")},
                 std::string("trailingSpaceStringNoEscape")},
                {{std::string("    spaced out string    ")}, std::string("spaced out string")},
                {{std::string("123   spaced number 321  ")},
                 std::string("123   spaced number 321")},
                {{std::string("   mixed123NUMBERSandSymbols!!!   ")},
                 std::string("mixed123NUMBERSandSymbols!!!")},
                {{std::string("   ")}, std::string("")},
                {{std::string("123")}, std::string("123")},
                {{std::string("symbolic string #$*&^%   ")}, std::string("symbolic string #$*&^%")},
                {{std::string("   Another Test Case")}, std::string("Another Test Case")},
                // 多语言字符串
                {{std::string("   极客时间   ")}, std::string("极客时间")},
                {{std::string("   テストケース   ")}, std::string("テストケース")},
                {{std::string("   테스트케이스   ")}, std::string("테스트케이스")},
                {{std::string("   Дело   ")}, std::string("Дело")},
                {{std::string("   Próba   ")}, std::string("Próba")},
                {{std::string("   Épreuve   ")}, std::string("Épreuve")},
                {{std::string("   αξιολόγηση   ")}, std::string("αξιολόγηση")},
                {{std::string("   prova   ")}, std::string("prova")},
                {{std::string("   prova   ")}, std::string("prova")},
                // 纯数字和标点符号组合
                {{std::string("   0.1234567890!@#$%^&*()_+   ")},
                 std::string("0.1234567890!@#$%^&*()_+")},
                // 额外的单词和符号组合
                {{std::string("   hello, world!   ")}, std::string("hello, world!")},
                {{std::string("   this is a test   ")}, std::string("this is a test")},
                {{std::string("   spaces in the middle    are fine   ")},
                 std::string("spaces in the middle    are fine")},
                {{std::string("   string_with_underscores   ")},
                 std::string("string_with_underscores")},
                {{std::string("   string-with-dashes   ")}, std::string("string-with-dashes")},
                {{std::string("   string with special: chars; here¡   ")},
                 std::string("string with special: chars; here¡")},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_string_ltrim_test) {
    std::string func_name = "ltrim";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("AbCdEfg")}, std::string("AbCdEfg")},
            {{std::string("你好HELLO")}, std::string("你好HELLO")},
            {{std::string("")}, std::string("")},
            {{Null()}, Null()},
            {{std::string("!@#$@* (!&#")}, std::string("!@#$@* (!&#")},
            {{std::string("JSKAB(Q@__!")}, std::string("JSKAB(Q@__!")},
            {{std::string("MY test Str你好  ")}, std::string("MY test Str你好  ")},
            {{std::string("                 ")}, std::string("")},
            {{std::string("23 12 --!__!_!__!")}, std::string("23 12 --!__!_!__!")},
            {{std::string("112+ + +")}, std::string("112+ + +")},
            {{std::string("     +       23 ")}, std::string("+       23 ")},
    };
    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_rtrim_test) {
    std::string func_name = "rtrim";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {{
            {{std::string("AbCdEfg")}, std::string("AbCdEfg")},
            {{std::string("你好HELLO")}, std::string("你好HELLO")},
            {{std::string("")}, std::string("")},
            {{Null()}, Null()},
            {{std::string("!@#$@* (!&#")}, std::string("!@#$@* (!&#")},
            {{std::string("JSKAB(Q@__!")}, std::string("JSKAB(Q@__!")},
            {{std::string("MY test Str你好  ")}, std::string("MY test Str你好")},
            {{std::string("                 ")}, std::string("")},
            {{std::string("23 12 --!__!_!__!")}, std::string("23 12 --!__!_!__!")},
            {{std::string("112+ + +")}, std::string("112+ + +")},
            {{std::string("     +       23 ")}, std::string("     +       23")},
    }};

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_repeat_test) {
    std::string func_name = "repeat";
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("AbCdEfg"), std::int32_t(1)}, std::string("AbCdEfg")},
                {{std::string("AbCdEfg"), std::int32_t(2)}, std::string("AbCdEfgAbCdEfg")},
                {{std::string("AbCdEfg"), std::int32_t(0)}, std::string("")},
                {{std::string("AbCdEfg"), std::int32_t(6)},
                 std::string("AbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfg")},
                {{std::string("AbCdEfg"), std::int32_t(10)},
                 std::string(
                         "AbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfgAbCdEfg")},
                {{std::string("AbCdEfg"), std::int32_t(-1)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(1)}, std::string("HELLO123")},
                {{std::string("HELLO123"), std::int32_t(2)}, std::string("HELLO123HELLO123")},
                {{std::string("HELLO123"), std::int32_t(0)}, std::string("")},
                {{std::string("HELLO123"), std::int32_t(6)},
                 std::string("HELLO123HELLO123HELLO123HELLO123HELLO123HELLO123")},
                {{std::string("HELLO123"), std::int32_t(10)},
                 std::string("HELLO123HELLO123HELLO123HELLO123HELLO123HELLO123HELLO123HELLO123HELLO"
                             "123HELLO123")},
                {{std::string("HELLO123"), std::int32_t(-1)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(1)}, std::string("你好HELLO")},
                {{std::string("你好HELLO"), std::int32_t(2)}, std::string("你好HELLO你好HELLO")},
                {{std::string("你好HELLO"), std::int32_t(0)}, std::string("")},
                {{std::string("你好HELLO"), std::int32_t(6)},
                 std::string("你好HELLO你好HELLO你好HELLO你好HELLO你好HELLO你好HELLO")},
                {{std::string("你好HELLO"), std::int32_t(10)},
                 std::string("你好HELLO你好HELLO你好HELLO你好HELLO你好HELLO你好HELLO你好HELLO你好HE"
                             "LLO你好HELLO你好HELLO")},
                {{std::string("你好HELLO"), std::int32_t(-1)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(1)}, std::string("123ABC_")},
                {{std::string("123ABC_"), std::int32_t(2)}, std::string("123ABC_123ABC_")},
                {{std::string("123ABC_"), std::int32_t(0)}, std::string("")},
                {{std::string("123ABC_"), std::int32_t(6)},
                 std::string("123ABC_123ABC_123ABC_123ABC_123ABC_123ABC_")},
                {{std::string("123ABC_"), std::int32_t(10)},
                 std::string(
                         "123ABC_123ABC_123ABC_123ABC_123ABC_123ABC_123ABC_123ABC_123ABC_123ABC_")},
                {{std::string("123ABC_"), std::int32_t(-1)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(1)}, std::string("MYtestSTR")},
                {{std::string("MYtestSTR"), std::int32_t(2)}, std::string("MYtestSTRMYtestSTR")},
                {{std::string("MYtestSTR"), std::int32_t(0)}, std::string("")},
                {{std::string("MYtestSTR"), std::int32_t(6)},
                 std::string("MYtestSTRMYtestSTRMYtestSTRMYtestSTRMYtestSTRMYtestSTR")},
                {{std::string("MYtestSTR"), std::int32_t(10)},
                 std::string("MYtestSTRMYtestSTRMYtestSTRMYtestSTRMYtestSTRMYtestSTRMYtestSTRMYtest"
                             "STRMYtestSTRMYtestSTR")},
                {{std::string("MYtestSTR"), std::int32_t(-1)}, std::string("")},
                {{std::string(""), std::int32_t(1)}, std::string("")},
                {{std::string(""), std::int32_t(2)}, std::string("")},
                {{std::string(""), std::int32_t(0)}, std::string("")},
                {{std::string(""), std::int32_t(6)}, std::string("")},
                {{std::string(""), std::int32_t(10)}, std::string("")},
                {{std::string(""), std::int32_t(-1)}, std::string("")},
                {{Null(), std::int32_t(1)}, Null()},
                {{Null(), std::int32_t(2)}, Null()},
                {{Null(), std::int32_t(0)}, Null()},
                {{Null(), std::int32_t(6)}, Null()},
                {{Null(), std::int32_t(10)}, Null()},
                {{Null(), std::int32_t(-1)}, Null()},
                {{std::string("A,b,C,D,_E"), std::int32_t(1)}, std::string("A,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(2)}, std::string("A,b,C,D,_EA,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(0)}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::int32_t(6)},
                 std::string("A,b,C,D,_EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(10)},
                 std::string("A,b,C,D,_EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_"
                             "EA,b,C,D,_EA,b,C,D,_EA,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::int32_t(-1)}, std::string("")},
                {{std::string("1234321312312"), std::int32_t(1)}, std::string("1234321312312")},
                {{std::string("1234321312312"), std::int32_t(2)},
                 std::string("12343213123121234321312312")},
                {{std::string("1234321312312"), std::int32_t(0)}, std::string("")},
                {{std::string("1234321312312"), std::int32_t(6)},
                 std::string("123432131231212343213123121234321312312123432131231212343213123121234"
                             "321312312")},
                {{std::string("1234321312312"), std::int32_t(10)},
                 std::string("123432131231212343213123121234321312312123432131231212343213123121234"
                             "3213123121234321312312123432131231212343213123121234321312312")},
                {{std::string("1234321312312"), std::int32_t(-1)}, std::string("")},
                {{std::string("heh1h2!u@u@i$o%ll_"), std::int32_t(1)},
                 std::string("heh1h2!u@u@i$o%ll_")},
                {{std::string("heh1h2!u@u@i$o%ll_"), std::int32_t(2)},
                 std::string("heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_")},
                {{std::string("heh1h2!u@u@i$o%ll_"), std::int32_t(0)}, std::string("")},
                {{std::string("heh1h2!u@u@i$o%ll_"), std::int32_t(6)},
                 std::string("heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%"
                             "ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_")},
                {{std::string("heh1h2!u@u@i$o%ll_"), std::int32_t(10)},
                 std::string("heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%"
                             "ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@"
                             "i$o%ll_heh1h2!u@u@i$o%ll_heh1h2!u@u@i$o%ll_")},
                {{std::string("heh1h2!u@u@i$o%ll_"), std::int32_t(-1)}, std::string("")},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_string_reverse_test) {
    std::string func_name = "reverse";
    {
        BaseInputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("AbCdEfg")}, std::string("gfEdCbA")},
                {{std::string("HELLO123")}, std::string("321OLLEH")},
                {{std::string("你好HELLO")}, std::string("OLLEH好你")},
                {{std::string("123ABC_")}, std::string("_CBA321")},
                {{std::string("MYtestSTR")}, std::string("RTStsetYM")},
                {{std::string("")}, std::string("")},
                {{Null()}, Null()},
                {{std::string("A,b,C,D,_E")}, std::string("E_,D,C,b,A")},
                {{std::string("1234321312312")}, std::string("2132131234321")},
                {{std::string("heh1h2_!u@_u@i$o%ll_")}, std::string("_ll%o$i@u_@u!_2h1heh")},
                {{std::string("AbCdEfg")}, std::string("gfEdCbA")},
                {{std::string("HELLO123")}, std::string("321OLLEH")},
                {{std::string("你好HELLO")}, std::string("OLLEH好你")},
                {{std::string("123ABC_")}, std::string("_CBA321")},
                {{std::string("MYtestSTR")}, std::string("RTStsetYM")},
                {{std::string("")}, std::string("")},
                {{Null()}, Null()},
                {{std::string("A,b,C,D,_E")}, std::string("E_,D,C,b,A")},
                {{std::string("1234321312312")}, std::string("2132131234321")},
                {{std::string("heh1h2_!u@_u@i$o%ll_")}, std::string("_ll%o$i@u_@u!_2h1heh")},
                {{std::string("A1!B2@C3#")}, std::string("#3C@2B!1A")},
                {{std::string("~!@#$%^&*()_+")}, std::string("+_)(*&^%$#@!~")},
                {{std::string("😊😂😍❤️👍")}, std::string("👍️❤😍😂😊")},
                {{std::string("äöüß")}, std::string("ßüöä")},
                {{std::string("👨‍👩‍👧‍👦")},
                 std::string("👦‍👧‍👩‍👨")},
                {{std::string("안녕하세요")}, std::string("요세하녕안")},
                {{std::string("Tab\tSeparated")},
                 std::string("detarapeS\tbaT")}, // 包含制表符的字符串
                {{std::string("\nNewLine")}, std::string("eniLweN\n")}, // 包含换行符的字符串
                {{std::string("123\n456")}, std::string("654\n321")},   // 混合数字和换行符
                {{std::string("\x01\x02\x03\x04")},
                 std::string("\x04\x03\x02\x01")}, // 包含非打印字符的字符串
                {{std::string("Level【中】")}, std::string("】中【leveL")}, // 混合英文和中文括号
                {{std::string("360° Rotation")}, std::string("noitatoR °063")}, // 包含特殊字符 °
                {{std::string("👩‍👩‍👧‍👦")},
                 std::string("👦‍👧‍👩‍👩")}, // 家庭emoji序列
                {{std::string("👨‍🎓👩‍🎓")},
                 std::string("🎓‍👩🎓‍👨")}, // 毕业生emoji序列
                {{std::string("ASCII 👨‍👨‍👧‍👦 UNICODE")},
                 std::string("EDOCINU 👦‍👧‍👨‍👨 IICSA")}, // 混合ASCII和UNICODE字符
                {{std::string("💻 Programming 💾")},
                 std::string("💾 gnimmargorP 💻")},                     // 编程相关emoji
                {{std::string("0010110")}, std::string("0110100")},     // 二进制序列
                {{std::string("readme.md")}, std::string("dm.emdaer")}, // 包含点的文件名
                {{std::string("info@example.com")},
                 std::string("moc.elpmaxe@ofni")},                            // 电子邮件地址
                {{std::string("1234567890")}, std::string("0987654321")},     // 纯数字
                {{std::string("!@#$%^&*()_+")}, std::string("+_)(*&^%$#@!")}, // 纯特殊字符
                {{std::string("UPPERlower123")}, std::string("321rewolREPPU")}, // 大小写和数字
                {{std::string("测试中文字符")}, std::string("符字文中试测")},   // 中文字符
                {{std::string("日本語テスト")}, std::string("トステ語本日")},   // 日文测试
        };
        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_string_length_test) {
    std::string func_name = "length";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("YXNk5L2g5aW9")}, std::int32_t(12)},
            {{std::string("aGVsbG8gd29ybGQ")}, std::int32_t(15)},
            {{std::string("SEVMTE8sIV4l")}, std::int32_t(12)},
            {{std::string("__123hehe1")}, std::int32_t(10)},
            {{std::string("")}, std::int32_t(0)},
            {{std::string("5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh")}, std::int32_t(36)},
            {{std::string("ò&ø")}, std::int32_t(5)},
            {{std::string("TVl0ZXN0U1RS")}, std::int32_t(12)},
            {{Null()}, Null()},
            {{std::string("123321!@#@$!@%!@#!@$!@")}, std::int32_t(22)},
            {{std::string("123")}, std::int32_t(3)},
            {{std::string("Hello, World!")}, std::int32_t(13)}, // 正常ASCII字符
            {{std::string("Привет, мир!")}, std::int32_t(21)}, // 俄文，使用Cyrillic characters
            {{std::string("こんにちは世界")}, std::int32_t(21)}, // 日文，每个字符通常3字节
            {{std::string("안녕하세요세계")}, std::int32_t(21)}, // 韩文字符
            {{std::string("你好，世界！")}, std::int32_t(18)}, // 简体中文，每个字符通常3字节
            {{std::string("مرحبا بالعالم!")}, std::int32_t(26)},            // 阿拉伯语
            {{std::string("1234567890")}, std::int32_t(10)},                // 数字
            {{std::string("👨‍👨‍👧‍👦")}, std::int32_t(25)}, // 家庭成员Emoji
            {{std::string("🇺🇸🇨🇳🇯🇵🇰🇷")}, std::int32_t(32)},                  // 国旗Emoji
            {{std::string("\u00F1")},
             std::int32_t(2)}, // ñ，为拉丁字母n with tilde，UTF-8中占用2字节
            {{std::string("\u65E5\u672C\u8A9E")}, std::int32_t(9)}, // 日本语，每个字符通常3个字节
            {{std::string("Hello, 世界！")}, std::int32_t(16)}, // 混合ASCII和非ASCII字符
            {{std::string("😀😃😄😁")}, std::int32_t(16)},      // Emoji，每个通常4个字节
            {{std::string("Quick brown 狐 jumps over a lazy 狗.")}, std::int32_t(38)}, // 混合字符串
            {{std::string("Löwe 老虎 Léopard")}, std::int32_t(21)}, // 欧洲文字和中文的混合
            {{std::string("Café 美丽")}, std::int32_t(12)},         // 带重音的字符
            {{std::string("Björk")}, std::int32_t(6)},              // 北欧名称
            {{std::string("¿Dónde está la biblioteca?")}, std::int32_t(29)}, // 西班牙语句子
            {{std::string("Zażółć gęślą jaźń")}, std::int32_t(26)}, // 波兰语句子，含特殊字符
            {{Null()}, Null()},                                     // 空值
            {{std::string(" ")}, std::int32_t(1)},                  // 空格
            {{std::string("  ")}, std::int32_t(2)},                 // 双空格

    };

    check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_string_quote_test) {
    std::string func_name = "quote";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("hello")}, std::string(R"('hello')")},
            {{std::string("hello\t\n\nworld")}, std::string("'hello\t\n\nworld'")},
            {{std::string("HELLO,!^%")}, std::string("'HELLO,!^%'")},
            {{std::string("MYtestStr\\t\\n")}, std::string("'MYtestStr\\t\\n'")},
            {{std::string("")}, std::string("''")},
            {{Null()}, Null()},
            {{std::string("A")}, std::string("'A'")},
            {{std::string(",")}, std::string("','")},
            {{std::string("")}, std::string("''")},
            {{std::string(",ABC,")}, std::string("',ABC,'")},
            {{std::string("123ABC!@# _")}, std::string("'123ABC!@# _'")},
            {{std::string("10@()*()$*!@")}, std::string("'10@()*()$*!@'")},
    };
    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_append_trailing_char_if_absent_test) {
    std::string func_name = "append_trailing_char_if_absent";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("ASD"), std::string("D")}, std::string("ASD")},
                        {{std::string("AS"), std::string("D")}, std::string("ASD")},
                        {{std::string(""), std::string("")}, Null()},
                        {{std::string(""), std::string("A")}, std::string("A")},
                        {{std::string("AC"), std::string("BACBAC")}, Null()},
                        {{Null(), Null()}, Null()},
                        {{std::string("ABC"), Null()}, Null()},
                        {{Null(), std::string("ABC")}, Null()},
                        {{std::string(""), Null()}, Null()},
                        {{Null(), std::string("")}, Null()}};

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_starts_with_test) {
    std::string func_name = "starts_with";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("hello world"), std::string("hello")}, uint8_t(1)},
                        {{std::string("hello world"), std::string("world")}, uint8_t(0)},
                        {{std::string("你好"), std::string("你")}, uint8_t(1)},
                        {{std::string(""), std::string("")}, uint8_t(1)},
                        {{std::string("你好"), Null()}, Null()},
                        {{Null(), std::string("")}, Null()}};

    check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_ends_with_test) {
    std::string func_name = "ends_with";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {{{std::string("hello world"), std::string("hello")}, uint8_t(0)},
                        {{std::string("hello world"), std::string("world")}, uint8_t(1)},
                        {{std::string("你好"), std::string("好")}, uint8_t(1)},
                        {{std::string(""), std::string("")}, uint8_t(1)},
                        {{std::string("你好"), Null()}, Null()},
                        {{Null(), std::string("")}, Null()},
                        {{Null(), Null()}, Null()},
                        {{std::string(1000, 'a'), std::string("a")}, uint8_t(1)},
                        // 特殊的UTF-8字符
                        {{std::string("This is a pencil ✏"), std::string("✏")}, uint8_t(1)},
                        // 重复字符
                        {{std::string("aaaaab"), std::string("b")}, uint8_t(1)},
                        // 特殊格式的字符串
                        {{std::string("user@example.com"), std::string("example.com")}, uint8_t(1)},
                        {{std::string("https://example.com"), std::string(".com")}, uint8_t(1)},
                        // 非常长的后缀
                        {{std::string("hello"), std::string(10, 'o')}, uint8_t(0)},
                        // 大小写不匹配
                        {{std::string("CaseSensitive"), std::string("sensitive")}, uint8_t(0)},
                        {{std::string("UpperCase"), std::string("CASE")}, uint8_t(0)},
                        // 字符边界
                        {{std::string("BoundaryTest"), std::string("Test")}, uint8_t(1)},
                        // 空字符串
                        {{std::string(""), std::string("")}, uint8_t(1)},
                        // Null 情景
                        {{std::string("Doris"), Null()}, Null()},
                        {{Null(), std::string("Ends")}, Null()},
                        {{Null(), Null()}, Null()}};

    check_function_all_arg_comb<DataTypeUInt8, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_ascii_test) {
    std::string func_name = "ascii";

    BaseInputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
            {{std::string("YXNk5L2g5aW9")}, std::int32_t(89)},
            {{std::string("aGVsbG8gd29ybGQ")}, std::int32_t(97)},
            {{std::string("SEVMTE8sIV4l")}, std::int32_t(83)},
            {{std::string("__123hehe1")}, std::int32_t(95)},
            {{std::string("")}, std::int32_t(0)},
            {{std::string("5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh")}, std::int32_t(53)},
            {{std::string("ò&ø")}, std::int32_t(195)},
            {{std::string("TVl0ZXN0U1RS")}, std::int32_t(84)},
            {{Null()}, Null()},
            {{std::string("123321!@#@$!@%!@#!@$!@")}, std::int32_t(49)},
            {{std::string("123")}, std::int32_t(49)},
    };

    check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_char_length_test) {
    std::string func_name = "char_length";

    BaseInputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
            {{std::string("YXNk5L2g5aW9")}, std::int32_t(12)},
            {{std::string("aGVsbG8gd29ybGQ")}, std::int32_t(15)},
            {{std::string("SEVMTE8sIV4l")}, std::int32_t(12)},
            {{std::string("__123hehe1")}, std::int32_t(10)},
            {{std::string("")}, std::int32_t(0)},
            {{std::string("5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh")}, std::int32_t(36)},
            {{std::string("ò&ø")}, std::int32_t(3)},
            {{std::string("TVl0ZXN0U1RS")}, std::int32_t(12)},
            {{Null()}, Null()},
            {{std::string("123321!@#@$!@%!@#!@$!@")}, std::int32_t(22)},
            {{std::string("123")}, std::int32_t(3)},
    };

    check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_concat_test) {
    std::string func_name = "concat";
    {
        BaseInputTypeSet input_types = {TypeIndex::String};

        DataSet data_set = {{{std::string("")}, std::string("")},
                            {{std::string("123")}, std::string("123")},
                            {{Null()}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("AbCdEfg"), std::string("AbCdEfg")}, std::string("AbCdEfgAbCdEfg")},
                {{std::string("AbCdEfg"), std::string("HELLO123")}, std::string("AbCdEfgHELLO123")},
                {{std::string("AbCdEfg"), std::string("你好HELLO")},
                 std::string("AbCdEfg你好HELLO")},
                {{std::string("AbCdEfg"), std::string("123ABC_")}, std::string("AbCdEfg123ABC_")},
                {{std::string("AbCdEfg"), std::string("MYtestSTR")},
                 std::string("AbCdEfgMYtestSTR")},
                {{std::string("AbCdEfg"), std::string("")}, std::string("AbCdEfg")},
                {{std::string("AbCdEfg"), Null()}, Null()},
                {{std::string("AbCdEfg"), std::string("A,b,C,D,_E")},
                 std::string("AbCdEfgA,b,C,D,_E")},
                {{std::string("AbCdEfg"), std::string("1234321312312")},
                 std::string("AbCdEfg1234321312312")},
                {{std::string("AbCdEfg"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("AbCdEfgheh1h2_!u@_u@i$o%ll_")},
                {{std::string("HELLO123"), std::string("AbCdEfg")}, std::string("HELLO123AbCdEfg")},
                {{std::string("HELLO123"), std::string("HELLO123")},
                 std::string("HELLO123HELLO123")},
                {{std::string("HELLO123"), std::string("你好HELLO")},
                 std::string("HELLO123你好HELLO")},
                {{std::string("HELLO123"), std::string("123ABC_")}, std::string("HELLO123123ABC_")},
                {{std::string("HELLO123"), std::string("MYtestSTR")},
                 std::string("HELLO123MYtestSTR")},
                {{std::string("HELLO123"), std::string("")}, std::string("HELLO123")},
                {{std::string("HELLO123"), Null()}, Null()},
                {{std::string("HELLO123"), std::string("A,b,C,D,_E")},
                 std::string("HELLO123A,b,C,D,_E")},
                {{std::string("HELLO123"), std::string("1234321312312")},
                 std::string("HELLO1231234321312312")},
                {{std::string("HELLO123"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("HELLO123heh1h2_!u@_u@i$o%ll_")},
                {{std::string("你好HELLO"), std::string("AbCdEfg")},
                 std::string("你好HELLOAbCdEfg")},
                {{std::string("你好HELLO"), std::string("HELLO123")},
                 std::string("你好HELLOHELLO123")},
                {{std::string("你好HELLO"), std::string("你好HELLO")},
                 std::string("你好HELLO你好HELLO")},
                {{std::string("你好HELLO"), std::string("123ABC_")},
                 std::string("你好HELLO123ABC_")},
                {{std::string("你好HELLO"), std::string("MYtestSTR")},
                 std::string("你好HELLOMYtestSTR")},
                {{std::string("你好HELLO"), std::string("")}, std::string("你好HELLO")},
                {{std::string("你好HELLO"), Null()}, Null()},
                {{std::string("你好HELLO"), std::string("A,b,C,D,_E")},
                 std::string("你好HELLOA,b,C,D,_E")},
                {{std::string("你好HELLO"), std::string("1234321312312")},
                 std::string("你好HELLO1234321312312")},
                {{std::string("你好HELLO"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("你好HELLOheh1h2_!u@_u@i$o%ll_")},
                {{std::string("123ABC_"), std::string("AbCdEfg")}, std::string("123ABC_AbCdEfg")},
                {{std::string("123ABC_"), std::string("HELLO123")}, std::string("123ABC_HELLO123")},
                {{std::string("123ABC_"), std::string("你好HELLO")},
                 std::string("123ABC_你好HELLO")},
                {{std::string("123ABC_"), std::string("123ABC_")}, std::string("123ABC_123ABC_")},
                {{std::string("123ABC_"), std::string("MYtestSTR")},
                 std::string("123ABC_MYtestSTR")},
                {{std::string("123ABC_"), std::string("")}, std::string("123ABC_")},
                {{std::string("123ABC_"), Null()}, Null()},
                {{std::string("123ABC_"), std::string("A,b,C,D,_E")},
                 std::string("123ABC_A,b,C,D,_E")},
                {{std::string("123ABC_"), std::string("1234321312312")},
                 std::string("123ABC_1234321312312")},
                {{std::string("123ABC_"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("123ABC_heh1h2_!u@_u@i$o%ll_")},
                {{std::string("MYtestSTR"), std::string("AbCdEfg")},
                 std::string("MYtestSTRAbCdEfg")},
                {{std::string("MYtestSTR"), std::string("HELLO123")},
                 std::string("MYtestSTRHELLO123")},
                {{std::string("MYtestSTR"), std::string("你好HELLO")},
                 std::string("MYtestSTR你好HELLO")},
                {{std::string("MYtestSTR"), std::string("123ABC_")},
                 std::string("MYtestSTR123ABC_")},
                {{std::string("MYtestSTR"), std::string("MYtestSTR")},
                 std::string("MYtestSTRMYtestSTR")},
                {{std::string("MYtestSTR"), std::string("")}, std::string("MYtestSTR")},
                {{std::string("MYtestSTR"), Null()}, Null()},
                {{std::string("MYtestSTR"), std::string("A,b,C,D,_E")},
                 std::string("MYtestSTRA,b,C,D,_E")},
                {{std::string("MYtestSTR"), std::string("1234321312312")},
                 std::string("MYtestSTR1234321312312")},
                {{std::string("MYtestSTR"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("MYtestSTRheh1h2_!u@_u@i$o%ll_")},
                {{std::string(""), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string(""), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string(""), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string(""), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string(""), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string(""), std::string("")}, std::string("")},
                {{std::string(""), Null()}, Null()},
                {{std::string(""), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string(""), std::string("1234321312312")}, std::string("1234321312312")},
                {{std::string(""), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{Null(), std::string("AbCdEfg")}, Null()},
                {{Null(), std::string("HELLO123")}, Null()},
                {{Null(), std::string("你好HELLO")}, Null()},
                {{Null(), std::string("123ABC_")}, Null()},
                {{Null(), std::string("MYtestSTR")}, Null()},
                {{Null(), std::string("")}, Null()},
                {{Null(), Null()}, Null()},
                {{Null(), std::string("A,b,C,D,_E")}, Null()},
                {{Null(), std::string("1234321312312")}, Null()},
                {{Null(), std::string("heh1h2_!u@_u@i$o%ll_")}, Null()},
                {{std::string("A,b,C,D,_E"), std::string("AbCdEfg")},
                 std::string("A,b,C,D,_EAbCdEfg")},
                {{std::string("A,b,C,D,_E"), std::string("HELLO123")},
                 std::string("A,b,C,D,_EHELLO123")},
                {{std::string("A,b,C,D,_E"), std::string("你好HELLO")},
                 std::string("A,b,C,D,_E你好HELLO")},
                {{std::string("A,b,C,D,_E"), std::string("123ABC_")},
                 std::string("A,b,C,D,_E123ABC_")},
                {{std::string("A,b,C,D,_E"), std::string("MYtestSTR")},
                 std::string("A,b,C,D,_EMYtestSTR")},
                {{std::string("A,b,C,D,_E"), std::string("")}, std::string("A,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), Null()}, Null()},
                {{std::string("A,b,C,D,_E"), std::string("A,b,C,D,_E")},
                 std::string("A,b,C,D,_EA,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::string("1234321312312")},
                 std::string("A,b,C,D,_E1234321312312")},
                {{std::string("A,b,C,D,_E"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("A,b,C,D,_Eheh1h2_!u@_u@i$o%ll_")},
                {{std::string("1234321312312"), std::string("AbCdEfg")},
                 std::string("1234321312312AbCdEfg")},
                {{std::string("1234321312312"), std::string("HELLO123")},
                 std::string("1234321312312HELLO123")},
                {{std::string("1234321312312"), std::string("你好HELLO")},
                 std::string("1234321312312你好HELLO")},
                {{std::string("1234321312312"), std::string("123ABC_")},
                 std::string("1234321312312123ABC_")},
                {{std::string("1234321312312"), std::string("MYtestSTR")},
                 std::string("1234321312312MYtestSTR")},
                {{std::string("1234321312312"), std::string("")}, std::string("1234321312312")},
                {{std::string("1234321312312"), Null()}, Null()},
                {{std::string("1234321312312"), std::string("A,b,C,D,_E")},
                 std::string("1234321312312A,b,C,D,_E")},
                {{std::string("1234321312312"), std::string("1234321312312")},
                 std::string("12343213123121234321312312")},
                {{std::string("1234321312312"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("1234321312312heh1h2_!u@_u@i$o%ll_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("AbCdEfg")},
                 std::string("heh1h2_!u@_u@i$o%ll_AbCdEfg")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("HELLO123")},
                 std::string("heh1h2_!u@_u@i$o%ll_HELLO123")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("你好HELLO")},
                 std::string("heh1h2_!u@_u@i$o%ll_你好HELLO")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("123ABC_")},
                 std::string("heh1h2_!u@_u@i$o%ll_123ABC_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("MYtestSTR")},
                 std::string("heh1h2_!u@_u@i$o%ll_MYtestSTR")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null()}, Null()},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("A,b,C,D,_E")},
                 std::string("heh1h2_!u@_u@i$o%ll_A,b,C,D,_E")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("1234321312312")},
                 std::string("heh1h2_!u@_u@i$o%ll_1234321312312")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_heh1h2_!u@_u@i$o%ll_")},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string(""), std::string("1"), std::string("")}, std::string("1")},
                {{std::string("123"), std::string("456"), std::string("789")},
                 std::string("123456789")},
                {{std::string("123"), Null(), std::string("789")}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };
}

TEST(function_string_test, function_elt_test) {
    std::string func_name = "elt";

    {
        BaseInputTypeSet input_types = {
                TypeIndex::Int32,
                TypeIndex::String,
                TypeIndex::String,
        };

        DataSet data_set = {
                {{Null(), std::string(""), std::string("")}, Null()},
                {{Null(), std::string(""), Null()}, Null()},
                {{Null(), std::string(""), std::string("!@#$@* (!&#")}, Null()},
                {{Null(), Null(), std::string("")}, Null()},
                {{Null(), Null(), Null()}, Null()},
                {{Null(), Null(), std::string("!@#$@* (!&#")}, Null()},
                {{Null(), std::string("!@#$@* (!&#"), std::string("")}, Null()},
                {{Null(), std::string("!@#$@* (!&#"), Null()}, Null()},
                {{Null(), std::string("!@#$@* (!&#"), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(1), std::string(""), std::string("")}, std::string("")},
                {{std::int32_t(1), std::string(""), Null()}, std::string("")},
                {{std::int32_t(1), std::string(""), std::string("!@#$@* (!&#")}, std::string("")},
                // {{std::int32_t(1), Null(), std::string("")}, Null()},
                // {{std::int32_t(1), Null(), Null()}, Null()},
                // {{std::int32_t(1), Null(), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(1), std::string("!@#$@* (!&#"), std::string("")},
                 std::string("!@#$@* (!&#")},
                {{std::int32_t(1), std::string("!@#$@* (!&#"), Null()}, std::string("!@#$@* (!&#")},
                {{std::int32_t(1), std::string("!@#$@* (!&#"), std::string("!@#$@* (!&#")},
                 std::string("!@#$@* (!&#")},
                {{std::int32_t(0), std::string(""), std::string("")}, Null()},
                {{std::int32_t(0), std::string(""), Null()}, Null()},
                {{std::int32_t(0), std::string(""), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(0), Null(), std::string("")}, Null()},
                {{std::int32_t(0), Null(), Null()}, Null()},
                {{std::int32_t(0), Null(), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(0), std::string("!@#$@* (!&#"), std::string("")}, Null()},
                {{std::int32_t(0), std::string("!@#$@* (!&#"), Null()}, Null()},
                {{std::int32_t(0), std::string("!@#$@* (!&#"), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(100), std::string(""), std::string("")}, Null()},
                {{std::int32_t(100), std::string(""), Null()}, Null()},
                {{std::int32_t(100), std::string(""), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(100), Null(), std::string("")}, Null()},
                {{std::int32_t(100), Null(), Null()}, Null()},
                {{std::int32_t(100), Null(), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(100), std::string("!@#$@* (!&#"), std::string("")}, Null()},
                {{std::int32_t(100), std::string("!@#$@* (!&#"), Null()}, Null()},
                {{std::int32_t(100), std::string("!@#$@* (!&#"), std::string("!@#$@* (!&#")},
                 Null()},
                {{std::int32_t(-1), std::string(""), std::string("")}, Null()},
                {{std::int32_t(-1), std::string(""), Null()}, Null()},
                {{std::int32_t(-1), std::string(""), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(-1), Null(), std::string("")}, Null()},
                {{std::int32_t(-1), Null(), Null()}, Null()},
                {{std::int32_t(-1), Null(), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(-1), std::string("!@#$@* (!&#"), std::string("")}, Null()},
                {{std::int32_t(-1), std::string("!@#$@* (!&#"), Null()}, Null()},
                {{std::int32_t(-1), std::string("!@#$@* (!&#"), std::string("!@#$@* (!&#")},
                 Null()},
                {{std::int32_t(0), std::string(""), std::string("")}, Null()},
                {{std::int32_t(0), std::string(""), Null()}, Null()},
                {{std::int32_t(0), std::string(""), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(0), Null(), std::string("")}, Null()},
                {{std::int32_t(0), Null(), Null()}, Null()},
                {{std::int32_t(0), Null(), std::string("!@#$@* (!&#")}, Null()},
                {{std::int32_t(0), std::string("!@#$@* (!&#"), std::string("")}, Null()},
                {{std::int32_t(0), std::string("!@#$@* (!&#"), Null()}, Null()},
                {{std::int32_t(0), std::string("!@#$@* (!&#"), std::string("!@#$@* (!&#")}, Null()},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };
}

TEST(function_string_test, function_concat_ws_test) {
    std::string func_name = "concat_ws";
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("AbCdEfg"), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string("AbCdEfg"), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("AbCdEfg"), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string("AbCdEfg"), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("AbCdEfg"), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string("AbCdEfg"), std::string("")}, std::string("")},
                {{std::string("AbCdEfg"), Null()}, std::string("")},
                {{std::string("AbCdEfg"), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string("AbCdEfg"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("AbCdEfg"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("HELLO123"), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string("HELLO123"), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("HELLO123"), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string("HELLO123"), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("HELLO123"), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string("HELLO123"), std::string("")}, std::string("")},
                {{std::string("HELLO123"), Null()}, std::string("")},
                {{std::string("HELLO123"), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string("HELLO123"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("HELLO123"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("你好HELLO"), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string("你好HELLO"), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("你好HELLO"), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string("你好HELLO"), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("你好HELLO"), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string("你好HELLO"), std::string("")}, std::string("")},
                {{std::string("你好HELLO"), Null()}, std::string("")},
                {{std::string("你好HELLO"), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string("你好HELLO"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("你好HELLO"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("123ABC_"), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string("123ABC_"), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("123ABC_"), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string("123ABC_"), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("123ABC_"), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string("123ABC_"), std::string("")}, std::string("")},
                {{std::string("123ABC_"), Null()}, std::string("")},
                {{std::string("123ABC_"), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string("123ABC_"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("123ABC_"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("MYtestSTR"), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string("MYtestSTR"), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("MYtestSTR"), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string("MYtestSTR"), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("MYtestSTR"), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string("MYtestSTR"), std::string("")}, std::string("")},
                {{std::string("MYtestSTR"), Null()}, std::string("")},
                {{std::string("MYtestSTR"), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string("MYtestSTR"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("MYtestSTR"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string(""), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string(""), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string(""), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string(""), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string(""), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string(""), std::string("")}, std::string("")},
                {{std::string(""), Null()}, std::string("")},
                {{std::string(""), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string(""), std::string("1234321312312")}, std::string("1234321312312")},
                {{std::string(""), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{Null(), std::string("AbCdEfg")}, Null()},
                {{Null(), std::string("HELLO123")}, Null()},
                {{Null(), std::string("你好HELLO")}, Null()},
                {{Null(), std::string("123ABC_")}, Null()},
                {{Null(), std::string("MYtestSTR")}, Null()},
                {{Null(), std::string("")}, Null()},
                {{Null(), Null()}, Null()},
                {{Null(), std::string("A,b,C,D,_E")}, Null()},
                {{Null(), std::string("1234321312312")}, Null()},
                {{Null(), std::string("heh1h2_!u@_u@i$o%ll_")}, Null()},
                {{std::string("A,b,C,D,_E"), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string("A,b,C,D,_E"), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("A,b,C,D,_E"), std::string("你好HELLO")}, std::string("你好HELLO")},
                {{std::string("A,b,C,D,_E"), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("A,b,C,D,_E"), std::string("MYtestSTR")}, std::string("MYtestSTR")},
                {{std::string("A,b,C,D,_E"), std::string("")}, std::string("")},
                {{std::string("A,b,C,D,_E"), Null()}, std::string("")},
                {{std::string("A,b,C,D,_E"), std::string("A,b,C,D,_E")}, std::string("A,b,C,D,_E")},
                {{std::string("A,b,C,D,_E"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("A,b,C,D,_E"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("1234321312312"), std::string("AbCdEfg")}, std::string("AbCdEfg")},
                {{std::string("1234321312312"), std::string("HELLO123")}, std::string("HELLO123")},
                {{std::string("1234321312312"), std::string("你好HELLO")},
                 std::string("你好HELLO")},
                {{std::string("1234321312312"), std::string("123ABC_")}, std::string("123ABC_")},
                {{std::string("1234321312312"), std::string("MYtestSTR")},
                 std::string("MYtestSTR")},
                {{std::string("1234321312312"), std::string("")}, std::string("")},
                {{std::string("1234321312312"), Null()}, std::string("")},
                {{std::string("1234321312312"), std::string("A,b,C,D,_E")},
                 std::string("A,b,C,D,_E")},
                {{std::string("1234321312312"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("1234321312312"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("AbCdEfg")},
                 std::string("AbCdEfg")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("HELLO123")},
                 std::string("HELLO123")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("你好HELLO")},
                 std::string("你好HELLO")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("123ABC_")},
                 std::string("123ABC_")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("MYtestSTR")},
                 std::string("MYtestSTR")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("")}, std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), Null()}, std::string("")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("A,b,C,D,_E")},
                 std::string("A,b,C,D,_E")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("1234321312312")},
                 std::string("1234321312312")},
                {{std::string("heh1h2_!u@_u@i$o%ll_"), std::string("heh1h2_!u@_u@i$o%ll_")},
                 std::string("heh1h2_!u@_u@i$o%ll_")},
        };

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("-"), std::string(""), std::string("")}, std::string("-")},
                {{std::string(""), std::string("123"), std::string("456")}, std::string("123456")},
                {{std::string(""), std::string(""), std::string("")}, std::string("")},
                {{Null(), std::string(""), std::string("")}, Null()},
                {{Null(), std::string(""), Null()}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                        TypeIndex::String};

        DataSet data_set = {
                {{std::string("-"), std::string(""), std::string(""), std::string("")},
                 std::string("--")},
                {{std::string(""), std::string("123"), std::string("456"), std::string("789")},
                 std::string("123456789")},
                {{std::string("-"), std::string(""), std::string("?"), std::string("")},
                 std::string("-?-")},
                {{Null(), std::string(""), std::string("?"), std::string("")}, Null()},
                {{std::string("-"), std::string("123"), Null(), std::string("456")},
                 std::string("123-456")}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Array, TypeIndex::String};

        Array vec1 = {Field(String("", 0)), Field(String("", 0)), Field(String("", 0))};
        Array vec2 = {Field(String("123", 3)), Field(String("456", 3)), Field(String("789", 3))};
        Array vec3 = {Field(String("", 0)), Field(String("?", 1)), Field(String("", 0))};
        Array vec4 = {Field(String("abc", 3)), Field(String("", 0)), Field(String("def", 3))};
        Array vec5 = {Field(String("abc", 3)), Field(String("def", 3)), Field(String("ghi", 3))};
        DataSet data_set = {{{std::string("-"), vec1}, std::string("--")},
                            {{std::string(""), vec2}, std::string("123456789")},
                            {{std::string("-"), vec3}, std::string("-?-")},
                            {{Null(), vec4}, Null()},
                            {{std::string("-"), vec5}, std::string("abc-def-ghi")}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    };
}

TEST(function_string_test, function_null_or_empty_test) {
    std::string func_name = "null_or_empty";

    BaseInputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("")}, uint8(true)},
                        {{std::string("aa")}, uint8(false)},
                        {{std::string("我")}, uint8(false)},
                        {{Null()}, uint8(true)}};

    check_function_all_arg_comb<DataTypeUInt8, false>(func_name, input_types, data_set);
}

TEST(function_string_test, function_to_base64_test) {
    std::string func_name = "to_base64";
    BaseInputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
            {{std::string("ABC")}, std::string("QUJD")},
            {{std::string("ABB")}, std::string("QUJC")},
            {{std::string("HEHE")}, std::string("SEVIRQ==")},
            {{std::string("__123hehe1")}, std::string("X18xMjNoZWhlMQ==")},
            {{std::string("")}, std::string("")},
            {{std::string("5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh")},
             std::string("NVpXSzVaT0k1Wk9JNVpPSThKK1loQ0RqZ0lMaWdKVGlnSlFo")},
            {{std::string("ò&ø")}, std::string("w7Imw7g=")},
            {{std::string("hehe")}, std::string("aGVoZQ==")},
            // // 特殊字符
            {{std::string("`~!@#$%^&*()-_=+")}, std::string("YH4hQCMkJV4mKigpLV89Kw==")},
            // // 末尾空格，这对 base64 编码意义重大
            {{std::string("test ")}, std::string("dGVzdCA=")},
            // // 空字符串
            {{std::string("")}, std::string("")},
    };

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_from_base64_test) {
    std::string func_name = "from_base64";
    BaseInputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
            {{std::string("YXNk5L2g5aW9")}, std::string("asd你好")},
            {{std::string("aGVsbG8gd29ybGQ")}, Null()},
            {{std::string("SEVMTE8sIV4l")}, std::string("HELLO,!^%")},
            {{std::string("__123hehe1")}, Null()},
            {{std::string("")}, std::string("")},
            {{std::string("5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh")},
             std::string("啊哈哈哈😄 。——!")},
            {{std::string("ò&ø")}, Null()},
            {{std::string("TVl0ZXN0U1RS")}, std::string("MYtestSTR")},
            {{Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_reverse_test) {
    std::string func_name = "reverse";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("AbCdEfg")}, std::string("gfEdCbA")},
            {{std::string("HELLO123")}, std::string("321OLLEH")},
            {{std::string("你好HELLO")}, std::string("OLLEH好你")},
            {{std::string("123ABC_")}, std::string("_CBA321")},
            {{std::string("MYtestSTR")}, std::string("RTStsetYM")},
            {{std::string("")}, std::string("")},
            {{Null()}, Null()},
            {{std::string("A,b,C,D,_E")}, std::string("E_,D,C,b,A")},
            {{std::string("1234321312312")}, std::string("2132131234321")},
            {{std::string("heh1h2_!u@_u@i$o%ll_")}, std::string("_ll%o$i@u_@u!_2h1heh")},
    };

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_instr_test) {
    std::string func_name = "instr";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
            {{std::string("ABC"), std::string("ABC")}, std::int32_t(1)},
            {{std::string("ABC"), std::string("B")}, std::int32_t(2)},
            {{std::string("ABC"), std::string("!@3$%^&*(")}, std::int32_t(0)},
            {{std::string("ABC"), std::string("ABCABCACBACBACBACBABCBACBACB")}, std::int32_t(0)},
            {{std::string("ABC"), Null()}, Null()},
            {{std::string("ABC"), std::string("1233213214126434563")}, std::int32_t(0)},
            {{std::string("ABC"), std::string("")}, std::int32_t(1)},
            {{std::string("ABC"), Null()}, Null()},
            {{std::string("B"), std::string("ABC")}, std::int32_t(0)},
            {{std::string("B"), std::string("B")}, std::int32_t(1)},
            {{std::string("B"), std::string("!@3$%^&*(")}, std::int32_t(0)},
            {{std::string("B"), std::string("ABCABCACBACBACBACBABCBACBACB")}, std::int32_t(0)},
            {{std::string("B"), Null()}, Null()},
            {{std::string("B"), std::string("1233213214126434563")}, std::int32_t(0)},
            {{std::string("B"), std::string("")}, std::int32_t(1)},
            {{std::string("B"), Null()}, Null()},
            {{std::string("!@3$%^&*("), std::string("ABC")}, std::int32_t(0)},
            {{std::string("!@3$%^&*("), std::string("B")}, std::int32_t(0)},
            {{std::string("!@3$%^&*("), std::string("!@3$%^&*(")}, std::int32_t(1)},
            {{std::string("!@3$%^&*("), std::string("ABCABCACBACBACBACBABCBACBACB")},
             std::int32_t(0)},
            {{std::string("!@3$%^&*("), Null()}, Null()},
            {{std::string("!@3$%^&*("), std::string("1233213214126434563")}, std::int32_t(0)},
            {{std::string("!@3$%^&*("), std::string("")}, std::int32_t(1)},
            {{std::string("!@3$%^&*("), Null()}, Null()},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("ABC")}, std::int32_t(1)},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("B")}, std::int32_t(2)},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("!@3$%^&*(")},
             std::int32_t(0)},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"),
              std::string("ABCABCACBACBACBACBABCBACBACB")},
             std::int32_t(1)},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"), Null()}, Null()},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("1233213214126434563")},
             std::int32_t(0)},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("")}, std::int32_t(1)},
            {{std::string("ABCABCACBACBACBACBABCBACBACB"), Null()}, Null()},
            {{Null(), std::string("ABC")}, Null()},
            {{Null(), std::string("B")}, Null()},
            {{Null(), std::string("!@3$%^&*(")}, Null()},
            {{Null(), std::string("ABCABCACBACBACBACBABCBACBACB")}, Null()},
            {{Null(), Null()}, Null()},
            {{Null(), std::string("1233213214126434563")}, Null()},
            {{Null(), std::string("")}, Null()},
            {{Null(), Null()}, Null()},
            {{std::string("1233213214126434563"), std::string("ABC")}, std::int32_t(0)},
            {{std::string("1233213214126434563"), std::string("B")}, std::int32_t(0)},
            {{std::string("1233213214126434563"), std::string("!@3$%^&*(")}, std::int32_t(0)},
            {{std::string("1233213214126434563"), std::string("ABCABCACBACBACBACBABCBACBACB")},
             std::int32_t(0)},
            {{std::string("1233213214126434563"), Null()}, Null()},
            {{std::string("1233213214126434563"), std::string("1233213214126434563")},
             std::int32_t(1)},
            {{std::string("1233213214126434563"), std::string("")}, std::int32_t(1)},
            {{std::string("1233213214126434563"), Null()}, Null()},
            {{std::string(""), std::string("ABC")}, std::int32_t(0)},
            {{std::string(""), std::string("B")}, std::int32_t(0)},
            {{std::string(""), std::string("!@3$%^&*(")}, std::int32_t(0)},
            {{std::string(""), std::string("ABCABCACBACBACBACBABCBACBACB")}, std::int32_t(0)},
            {{std::string(""), Null()}, Null()},
            {{std::string(""), std::string("1233213214126434563")}, std::int32_t(0)},
            {{std::string(""), std::string("")}, std::int32_t(1)},
            {{std::string(""), Null()}, Null()},
            {{Null(), std::string("ABC")}, Null()},
            {{Null(), std::string("B")}, Null()},
            {{Null(), std::string("!@3$%^&*(")}, Null()},
            {{Null(), std::string("ABCABCACBACBACBACBABCBACBACB")}, Null()},
            {{Null(), Null()}, Null()},
            {{Null(), std::string("1233213214126434563")}, Null()},
            {{Null(), std::string("")}, Null()},
            {{Null(), Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_locate_test) {
    std::string func_name = "locate";

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

        DataSet data_set = {
                {{std::string("ABC"), std::string("ABC")}, std::int32_t(1)},
                {{std::string("ABC"), std::string("B")}, std::int32_t(0)},
                {{std::string("ABC"), std::string("!@3$%^&*(")}, std::int32_t(0)},
                {{std::string("ABC"), std::string("ABCABCACBACBACBACBABCBACBACB")},
                 std::int32_t(1)},
                {{std::string("ABC"), Null()}, Null()},
                {{std::string("ABC"), std::string("1233213214126434563")}, std::int32_t(0)},
                {{std::string("ABC"), std::string("")}, std::int32_t(0)},
                {{std::string("B"), std::string("ABC")}, std::int32_t(2)},
                {{std::string("B"), std::string("B")}, std::int32_t(1)},
                {{std::string("B"), std::string("!@3$%^&*(")}, std::int32_t(0)},
                {{std::string("B"), std::string("ABCABCACBACBACBACBABCBACBACB")}, std::int32_t(2)},
                {{std::string("B"), Null()}, Null()},
                {{std::string("B"), std::string("1233213214126434563")}, std::int32_t(0)},
                {{std::string("B"), std::string("")}, std::int32_t(0)},
                {{std::string("!@3$%^&*("), std::string("ABC")}, std::int32_t(0)},
                {{std::string("!@3$%^&*("), std::string("B")}, std::int32_t(0)},
                {{std::string("!@3$%^&*("), std::string("!@3$%^&*(")}, std::int32_t(1)},
                {{std::string("!@3$%^&*("), std::string("ABCABCACBACBACBACBABCBACBACB")},
                 std::int32_t(0)},
                {{std::string("!@3$%^&*("), Null()}, Null()},
                {{std::string("!@3$%^&*("), std::string("1233213214126434563")}, std::int32_t(0)},
                {{std::string("!@3$%^&*("), std::string("")}, std::int32_t(0)},
                {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("ABC")},
                 std::int32_t(0)},
                {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("B")}, std::int32_t(0)},
                {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("!@3$%^&*(")},
                 std::int32_t(0)},
                {{std::string("ABCABCACBACBACBACBABCBACBACB"),
                  std::string("ABCABCACBACBACBACBABCBACBACB")},
                 std::int32_t(1)},
                {{std::string("ABCABCACBACBACBACBABCBACBACB"), Null()}, Null()},
                {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("1233213214126434563")},
                 std::int32_t(0)},
                {{std::string("ABCABCACBACBACBACBABCBACBACB"), std::string("")}, std::int32_t(0)},
                {{Null(), std::string("ABC")}, Null()},
                {{Null(), std::string("B")}, Null()},
                {{Null(), std::string("!@3$%^&*(")}, Null()},
                {{Null(), std::string("ABCABCACBACBACBACBABCBACBACB")}, Null()},
                {{Null(), Null()}, Null()},
                {{Null(), std::string("1233213214126434563")}, Null()},
                {{Null(), std::string("")}, Null()},
                {{std::string("1233213214126434563"), std::string("ABC")}, std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("B")}, std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("!@3$%^&*(")}, std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("ABCABCACBACBACBACBABCBACBACB")},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), Null()}, Null()},
                {{std::string("1233213214126434563"), std::string("1233213214126434563")},
                 std::int32_t(1)},
                {{std::string("1233213214126434563"), std::string("")}, std::int32_t(0)},
                {{std::string(""), std::string("ABC")}, std::int32_t(1)},
                {{std::string(""), std::string("B")}, std::int32_t(1)},
                {{std::string(""), std::string("!@3$%^&*(")}, std::int32_t(1)},
                {{std::string(""), std::string("ABCABCACBACBACBACBABCBACBACB")}, std::int32_t(1)},
                {{std::string(""), Null()}, Null()},
                {{std::string(""), std::string("1233213214126434563")}, std::int32_t(1)},
                {{std::string(""), std::string("")}, std::int32_t(1)},
        };

        check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::Int32};

        DataSet data_set = {
                {{std::string("ABC"), std::string("ABC"), std::int32_t(100)}, std::int32_t(0)},
                {{std::string("ABC"), std::string("ABC"), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("ABC"), std::string("ABC"), std::int32_t(1)}, std::int32_t(1)},
                {{std::string("ABC"), std::string("B"), std::int32_t(100)}, std::int32_t(0)},
                {{std::string("ABC"), std::string("B"), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("ABC"), std::string("B"), std::int32_t(1)}, std::int32_t(0)},
                {{std::string("ABC"), std::string("!@3$%^&*("), std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("ABC"), std::string("!@3$%^&*("), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("ABC"), std::string("!@3$%^&*("), std::int32_t(1)}, std::int32_t(0)},
                {{std::string("ABC"), std::string("ABCABCACBACBACBACBABCBACBACB"),
                  std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("ABC"), std::string("ABCABCACBACBACBACBABCBACBACB"),
                  std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string("ABC"), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(1)},
                 std::int32_t(1)},
                {{std::string("ABC"), Null(), std::int32_t(100)}, Null()},
                {{std::string("ABC"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("ABC"), Null(), std::int32_t(1)}, Null()},
                {{std::string("ABC"), std::string(""), std::int32_t(100)}, std::int32_t(0)},
                {{std::string("ABC"), std::string(""), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("ABC"), std::string(""), std::int32_t(1)}, std::int32_t(0)},
                {{std::string("B"), std::string("ABC"), std::int32_t(100)}, std::int32_t(0)},
                {{std::string("B"), std::string("ABC"), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("B"), std::string("ABC"), std::int32_t(1)}, std::int32_t(2)},
                {{std::string("B"), std::string("B"), std::int32_t(100)}, std::int32_t(0)},
                {{std::string("B"), std::string("B"), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("B"), std::string("B"), std::int32_t(1)}, std::int32_t(1)},
                {{std::string("B"), std::string("!@3$%^&*("), std::int32_t(100)}, std::int32_t(0)},
                {{std::string("B"), std::string("!@3$%^&*("), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("B"), std::string("!@3$%^&*("), std::int32_t(1)}, std::int32_t(0)},
                {{std::string("B"), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("B"), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string("B"), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(1)},
                 std::int32_t(2)},
                {{std::string("B"), Null(), std::int32_t(100)}, Null()},
                {{std::string("B"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("B"), Null(), std::int32_t(1)}, Null()},
                {{std::string("B"), std::string(""), std::int32_t(100)}, std::int32_t(0)},
                {{std::string("B"), std::string(""), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string("B"), std::string(""), std::int32_t(1)}, std::int32_t(0)},
                {{Null(), std::string("ABC"), std::int32_t(100)}, Null()},
                {{Null(), std::string("ABC"), std::int32_t(-1)}, Null()},
                {{Null(), std::string("ABC"), std::int32_t(1)}, Null()},
                {{Null(), std::string("B"), std::int32_t(100)}, Null()},
                {{Null(), std::string("B"), std::int32_t(-1)}, Null()},
                {{Null(), std::string("B"), std::int32_t(1)}, Null()},
                {{Null(), std::string("!@3$%^&*("), std::int32_t(100)}, Null()},
                {{Null(), std::string("!@3$%^&*("), std::int32_t(-1)}, Null()},
                {{Null(), std::string("!@3$%^&*("), std::int32_t(1)}, Null()},
                {{Null(), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(100)}, Null()},
                {{Null(), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(-1)}, Null()},
                {{Null(), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(1)}, Null()},
                {{Null(), Null(), std::int32_t(100)}, Null()},
                {{Null(), Null(), std::int32_t(-1)}, Null()},
                {{Null(), Null(), std::int32_t(1)}, Null()},
                {{Null(), std::string(""), std::int32_t(100)}, Null()},
                {{Null(), std::string(""), std::int32_t(-1)}, Null()},
                {{Null(), std::string(""), std::int32_t(1)}, Null()},
                {{std::string("1233213214126434563"), std::string("ABC"), std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("ABC"), std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("ABC"), std::int32_t(1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("B"), std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("B"), std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("B"), std::int32_t(1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("!@3$%^&*("), std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("!@3$%^&*("), std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("!@3$%^&*("), std::int32_t(1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("ABCABCACBACBACBACBABCBACBACB"),
                  std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("ABCABCACBACBACBACBABCBACBACB"),
                  std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string("ABCABCACBACBACBACBABCBACBACB"),
                  std::int32_t(1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), Null(), std::int32_t(100)}, Null()},
                {{std::string("1233213214126434563"), Null(), std::int32_t(-1)}, Null()},
                {{std::string("1233213214126434563"), Null(), std::int32_t(1)}, Null()},
                {{std::string("1233213214126434563"), std::string(""), std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string(""), std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string("1233213214126434563"), std::string(""), std::int32_t(1)},
                 std::int32_t(0)},
                {{std::string(""), std::string("ABC"), std::int32_t(100)}, std::int32_t(0)},
                {{std::string(""), std::string("ABC"), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string(""), std::string("ABC"), std::int32_t(1)}, std::int32_t(1)},
                {{std::string(""), std::string("B"), std::int32_t(100)}, std::int32_t(0)},
                {{std::string(""), std::string("B"), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string(""), std::string("B"), std::int32_t(1)}, std::int32_t(1)},
                {{std::string(""), std::string("!@3$%^&*("), std::int32_t(100)}, std::int32_t(0)},
                {{std::string(""), std::string("!@3$%^&*("), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string(""), std::string("!@3$%^&*("), std::int32_t(1)}, std::int32_t(1)},
                {{std::string(""), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(100)},
                 std::int32_t(0)},
                {{std::string(""), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(-1)},
                 std::int32_t(0)},
                {{std::string(""), std::string("ABCABCACBACBACBACBABCBACBACB"), std::int32_t(1)},
                 std::int32_t(1)},
                {{std::string(""), Null(), std::int32_t(100)}, Null()},
                {{std::string(""), Null(), std::int32_t(-1)}, Null()},
                {{std::string(""), Null(), std::int32_t(1)}, Null()},
                {{std::string(""), std::string(""), std::int32_t(100)}, std::int32_t(0)},
                {{std::string(""), std::string(""), std::int32_t(-1)}, std::int32_t(0)},
                {{std::string(""), std::string(""), std::int32_t(1)}, std::int32_t(1)},
        };

        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
    }
}

TEST(function_string_test, function_find_in_set_test) {
    std::string func_name = "find_in_set";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    DataSet data_set = {
            {{std::string("ABC"), std::string("A,B,C")}, std::int32_t(0)},
            {{std::string("ABC"), std::string(",,,,,,")}, std::int32_t(0)},
            {{std::string("ABC"), std::string(",ABC,")}, std::int32_t(2)},
            {{std::string("ABC"), Null()}, Null()},
            {{std::string("ABC"), std::string("")}, std::int32_t(0)},
            {{std::string("A"), std::string("A,B,C")}, std::int32_t(1)},
            {{std::string("A"), std::string(",,,,,,")}, std::int32_t(0)},
            {{std::string("A"), std::string(",ABC,")}, std::int32_t(0)},
            {{std::string("A"), Null()}, Null()},
            {{std::string("A"), std::string("")}, std::int32_t(0)},
            {{std::string(","), std::string("A,B,C")}, std::int32_t(0)},
            {{std::string(","), std::string(",,,,,,")}, std::int32_t(0)},
            {{std::string(","), std::string(",ABC,")}, std::int32_t(0)},
            {{std::string(","), Null()}, Null()},
            {{std::string(","), std::string("")}, std::int32_t(0)},
            {{std::string(""), std::string("A,B,C")}, std::int32_t(0)},
            {{std::string(""), std::string(",,,,,,")}, std::int32_t(1)},
            {{std::string(""), std::string(",ABC,")}, std::int32_t(1)},
            {{std::string(""), Null()}, Null()},
            {{std::string(""), std::string("")}, std::int32_t(1)},
            {{Null(), std::string("A,B,C")}, Null()},
            {{Null(), std::string(",,,,,,")}, Null()},
            {{Null(), std::string(",ABC,")}, Null()},
            {{Null(), Null()}, Null()},
            {{Null(), std::string("")}, Null()},
    };

    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_md5sum_test) {
    std::string func_name = "md5sum";

    {
        BaseInputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("asd你好")}, {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                {{std::string("hello world")}, {std::string("5eb63bbbe01eeed093cb22bb8f5acdc3")}},
                {{std::string("HELLO,!^%")}, {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                {{std::string("")}, {std::string("d41d8cd98f00b204e9800998ecf8427e")}},
                {{std::string(" ")}, {std::string("7215ee9c7d9dc229d2921a40e899ec5f")}},
                {{Null()}, {Null()}},
                {{std::string("MYtestSTR")}, {std::string("cd24c90b3fc1192eb1879093029e87d4")}},
                {{std::string("ò&ø")}, {std::string("fd157b4cb921fa91acc667380184d59c")}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {{{std::string("asd"), std::string("你好")},
                             {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                            {{std::string("hello "), std::string("world")},
                             {std::string("5eb63bbbe01eeed093cb22bb8f5acdc3")}},
                            {{std::string("HELLO"), std::string(",!^%")},
                             {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                            {{Null(), std::string("HELLO")}, {Null()}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
        DataSet data_set = {{{std::string("a"), std::string("sd"), std::string("你好")},
                             {std::string("a38c15675555017e6b8ea042f2eb24f5")}},
                            {{std::string(""), std::string(""), std::string("")},
                             {std::string("d41d8cd98f00b204e9800998ecf8427e")}},
                            {{std::string("HEL"), std::string("LO,!"), std::string("^%")},
                             {std::string("b8e6e34d1cc3dc76b784ddfdfb7df800")}},
                            {{Null(), std::string("HELLO"), Null()}, {Null()}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_sm3sum_test) {
    std::string func_name = "sm3sum";

    {
        BaseInputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("asd你好")},
                 {std::string("0d6b9dfa8fe5708eb0dccfbaff4f2964abaaa976cc4445a7ecace49c0ceb31d3")}},
                {{std::string("hello world")},
                 {std::string("44f0061e69fa6fdfc290c494654a05dc0c053da7e5c52b84ef93a9d67d3fff88")}},
                {{std::string("HELLO,!^%")},
                 {std::string("5fc6e38f40b31a659a59e1daba9b68263615f20c02037b419d9deb3509e6b5c6")}},
                {{std::string("")},
                 {std::string("1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b")}},
                {{std::string(" ")},
                 {std::string("2ae1d69bb8483e5944310c877573b21d0a420c3bf4a2a91b1a8370d760ba67c5")}},
                {{Null()}, {Null()}},
                {{std::string("MYtestSTR")},
                 {std::string("3155ae9f834cae035385fc15b69b6f2c051b91de943ea9a03ab8bfd497aef4c6")}},
                {{std::string("ò&ø")},
                 {std::string(
                         "aa47ac31c85aa819d4cc80c932e7900fa26a3073a67aa7eb011bc2ba4924a066")}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("asd"), std::string("你好")},
                 {std::string("0d6b9dfa8fe5708eb0dccfbaff4f2964abaaa976cc4445a7ecace49c0ceb31d3")}},
                {{std::string("hello "), std::string("world")},
                 {std::string("44f0061e69fa6fdfc290c494654a05dc0c053da7e5c52b84ef93a9d67d3fff88")}},
                {{std::string("HELLO "), std::string(",!^%")},
                 {std::string("1f5866e786ebac9ffed0dbd8f2586e3e99d1d05f7efe7c5915478b57b7423570")}},
                {{Null(), std::string("HELLO")}, {Null()}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("a"), std::string("sd"), std::string("你好")},
                 {std::string("0d6b9dfa8fe5708eb0dccfbaff4f2964abaaa976cc4445a7ecace49c0ceb31d3")}},
                {{std::string(""), std::string(""), std::string("")},
                 {std::string("1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b")}},
                {{std::string("HEL"), std::string("LO,!"), std::string("^%")},
                 {std::string("5fc6e38f40b31a659a59e1daba9b68263615f20c02037b419d9deb3509e6b5c6")}},
                {{Null(), std::string("HELLO"), Null()}, {Null()}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_aes_encrypt_test) {
    std::string func_name = "aes_encrypt";
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        const char* mode = "AES_128_ECB";
        const char* key = "doris";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);

            int outlen = EncryptionUtil::encrypt(
                    EncryptionMode::AES_128_ECB, (unsigned char*)src[i], strlen(src[i]),
                    (unsigned char*)key, strlen(key), nullptr, 0, true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }

        DataSet data_set = {{{std::string(src[0]), std::string(key), std::string(mode)}, r[0]},
                            {{std::string(src[1]), std::string(key), std::string(mode)}, r[1]},
                            {{std::string(src[2]), std::string(key), std::string(mode)}, r[2]},
                            {{std::string(src[3]), std::string(key), std::string(mode)}, r[3]},
                            {{std::string(src[4]), std::string(key), std::string(mode)}, r[4]},
                            {{std::string(src[5]), std::string(key), std::string(mode)}, Null()},
                            {{Null(), std::string(key), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                        TypeIndex::String};
        const char* iv = "0123456789abcdef";
        const char* mode = "AES_256_ECB";
        const char* key = "vectorized";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen = EncryptionUtil::encrypt(EncryptionMode::AES_256_ECB,
                                                 (unsigned char*)src[i], strlen(src[i]),
                                                 (unsigned char*)key, strlen(key), init_vec.get(),
                                                 strlen(iv), true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }

        DataSet data_set = {
                {{std::string(src[0]), std::string(key), std::string(iv), std::string(mode)}, r[0]},
                {{std::string(src[1]), std::string(key), std::string(iv), std::string(mode)}, r[1]},
                {{std::string(src[2]), std::string(key), std::string(iv), std::string(mode)}, r[2]},
                {{std::string(src[3]), std::string(key), std::string(iv), std::string(mode)}, r[3]},
                {{std::string(src[4]), std::string(key), std::string(iv), std::string(mode)}, r[4]},
                {{std::string(src[5]), std::string(key), std::string(iv), std::string(mode)},
                 Null()},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_aes_decrypt_test) {
    std::string func_name = "aes_decrypt";
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};

        const char* mode = "AES_128_ECB";
        const char* key = "doris";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);

            int outlen = EncryptionUtil::encrypt(
                    EncryptionMode::AES_128_ECB, (unsigned char*)src[i], strlen(src[i]),
                    (unsigned char*)key, strlen(key), nullptr, 0, true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }

        DataSet data_set = {{{r[0], std::string(key), std::string(mode)}, std::string(src[0])},
                            {{r[1], std::string(key), std::string(mode)}, std::string(src[1])},
                            {{r[2], std::string(key), std::string(mode)}, std::string(src[2])},
                            {{r[3], std::string(key), std::string(mode)}, std::string(src[3])},
                            {{r[4], std::string(key), std::string(mode)}, std::string(src[4])},
                            {{Null(), std::string(key), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                        TypeIndex::String};
        const char* key = "vectorized";
        const char* iv = "0123456789abcdef";
        const char* mode = "AES_128_OFB";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};

        std::string r[5];
        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen = EncryptionUtil::encrypt(EncryptionMode::AES_128_OFB,
                                                 (unsigned char*)src[i], strlen(src[i]),
                                                 (unsigned char*)key, strlen(key), init_vec.get(),
                                                 strlen(iv), true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }
        DataSet data_set = {
                {{r[0], std::string(key), std::string(iv), std::string(mode)}, std::string(src[0])},
                {{r[1], std::string(key), std::string(iv), std::string(mode)}, std::string(src[1])},
                {{r[2], std::string(key), std::string(iv), std::string(mode)}, std::string(src[2])},
                {{r[3], std::string(key), std::string(iv), std::string(mode)}, std::string(src[3])},
                {{r[4], std::string(key), std::string(iv), std::string(mode)}, std::string(src[4])},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_sm4_encrypt_test) {
    std::string func_name = "sm4_encrypt";
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                        TypeIndex::String};

        const char* key = "doris";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_ECB";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen = EncryptionUtil::encrypt(EncryptionMode::SM4_128_ECB,
                                                 (unsigned char*)src[i], strlen(src[i]),
                                                 (unsigned char*)key, strlen(key), init_vec.get(),
                                                 strlen(iv), true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }

        DataSet data_set = {
                {{std::string(src[0]), std::string(key), std::string(iv), std::string(mode)}, r[0]},
                {{std::string(src[1]), std::string(key), std::string(iv), std::string(mode)}, r[1]},
                {{std::string(src[2]), std::string(key), std::string(iv), std::string(mode)}, r[2]},
                {{std::string(src[3]), std::string(key), std::string(iv), std::string(mode)}, r[3]},
                {{std::string(src[4]), std::string(key), std::string(iv), std::string(mode)}, r[4]},
                {{std::string(src[5]), std::string(key), std::string(iv), std::string(mode)},
                 Null()},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                        TypeIndex::String};

        const char* key = "vectorized";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_CTR";
        const char* src[6] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee", ""};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen = EncryptionUtil::encrypt(EncryptionMode::SM4_128_CTR,
                                                 (unsigned char*)src[i], strlen(src[i]),
                                                 (unsigned char*)key, strlen(key), init_vec.get(),
                                                 strlen(iv), true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }

        DataSet data_set = {
                {{std::string(src[0]), std::string(key), std::string(iv), std::string(mode)}, r[0]},
                {{std::string(src[1]), std::string(key), std::string(iv), std::string(mode)}, r[1]},
                {{std::string(src[2]), std::string(key), std::string(iv), std::string(mode)}, r[2]},
                {{std::string(src[3]), std::string(key), std::string(iv), std::string(mode)}, r[3]},
                {{std::string(src[4]), std::string(key), std::string(iv), std::string(mode)}, r[4]},
                {{std::string(src[5]), std::string(key), std::string(iv), std::string(mode)},
                 Null()},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_sm4_decrypt_test) {
    std::string func_name = "sm4_decrypt";
    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                        TypeIndex::String};

        const char* key = "doris";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_ECB";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen = EncryptionUtil::encrypt(EncryptionMode::SM4_128_ECB,
                                                 (unsigned char*)src[i], strlen(src[i]),
                                                 (unsigned char*)key, strlen(key), init_vec.get(),
                                                 strlen(iv), true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }

        DataSet data_set = {
                {{r[0], std::string(key), std::string(iv), std::string(mode)}, std::string(src[0])},
                {{r[1], std::string(key), std::string(iv), std::string(mode)}, std::string(src[1])},
                {{r[2], std::string(key), std::string(iv), std::string(mode)}, std::string(src[2])},
                {{r[3], std::string(key), std::string(iv), std::string(mode)}, std::string(src[3])},
                {{r[4], std::string(key), std::string(iv), std::string(mode)}, std::string(src[4])},
                {{Null(), std::string(key), std::string(iv), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String,
                                        TypeIndex::String};

        const char* key = "vectorized";
        const char* iv = "0123456789abcdef";
        const char* mode = "SM4_128_OFB";
        const char* src[5] = {"aaaaaa", "bbbbbb", "cccccc", "dddddd", "eeeeee"};
        std::string r[5];

        for (int i = 0; i < 5; i++) {
            int cipher_len = strlen(src[i]) + 16;
            std::vector<char> p(cipher_len);
            int iv_len = 32;
            std::unique_ptr<char[]> init_vec;
            init_vec.reset(new char[iv_len]);
            std::memset(init_vec.get(), 0, strlen(iv) + 1);
            memcpy(init_vec.get(), iv, strlen(iv));
            int outlen = EncryptionUtil::encrypt(EncryptionMode::SM4_128_OFB,
                                                 (unsigned char*)src[i], strlen(src[i]),
                                                 (unsigned char*)key, strlen(key), init_vec.get(),
                                                 strlen(iv), true, (unsigned char*)p.data());
            r[i] = std::string(p.data(), outlen);
        }

        DataSet data_set = {
                {{r[0], std::string(key), std::string(iv), std::string(mode)}, std::string(src[0])},
                {{r[1], std::string(key), std::string(iv), std::string(mode)}, std::string(src[1])},
                {{r[2], std::string(key), std::string(iv), std::string(mode)}, std::string(src[2])},
                {{r[3], std::string(key), std::string(iv), std::string(mode)}, std::string(src[3])},
                {{r[4], std::string(key), std::string(iv), std::string(mode)}, std::string(src[4])},
                {{Null(), Null(), std::string(iv), std::string(mode)}, Null()}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_extract_url_parameter_test) {
    std::string func_name = "extract_url_parameter";
    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    DataSet data_set = {
            {{VARCHAR(""), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa"), VARCHAR("")}, {VARCHAR("")}},
            {{VARCHAR("https://doris.apache.org/"), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?"), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa"), VARCHAR("k1")}, {VARCHAR("aa")}},
            {{VARCHAR("http://doris.apache.org:8080?k1&k2=bb#99"), VARCHAR("k1")}, {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa#999"), VARCHAR("k1")}, {VARCHAR("aa")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("k1")},
             {VARCHAR("aa")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("k2")},
             {VARCHAR("bb")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("999")},
             {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("k3")},
             {VARCHAR("")}},
            {{VARCHAR("http://doris.apache.org?k1=aa&k2=bb&test=dd#999/"), VARCHAR("test")},
             {VARCHAR("dd")}}};

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_parse_url_test) {
    std::string func_name = "parse_url";

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("zhangsan"), std::string("HOST")}, {Null()}},
                {{std::string("facebook.com/path/p1"), std::string("HOST")}, {Null()}},
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("HOST")},
                 {std::string("fb.com")}},
                {{std::string("https://www.facebook.com/aa/bb?returnpage=https://www.facebook.com/"
                              "aa/bb/cc"),
                  std::string("HOST")},
                 {std::string("www.facebook.com")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("AUTHORITY")},
                 {std::string("facebook.com")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("authority")},
                 {std::string("facebook.com")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("FILE")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("file")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("PATH")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://www.baidu.com:9090/a/b/c.php"), std::string("path")},
                 {std::string("/a/b/c.php")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("PROTOCOL")},
                 {std::string("http")}},
                {{std::string("http://facebook.com/path/p1.php?query=1"), std::string("protocol")},
                 {std::string("http")}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("QUERY")},
                 {std::string("a=b")}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("query")},
                 {std::string("a=b")}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("REF")}, {Null()}},
                {{std::string("http://www.baidu.com:9090?a=b"), std::string("ref")}, {Null()}},
                {{std::string("http://www.baidu.com:9090/a/b/c?a=b"), std::string("PORT")},
                 {std::string("9090")}},
                {{std::string("http://www.baidu.com/a/b/c?a=b"), std::string("PORT")}, {Null()}},
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("QUERY")},
                 {std::string("q=1")}},
                {{std::string(
                          "https://www.facebook.com/aa/bb?returnpage=https://www.facebook.com/"),
                  std::string("HosT")},
                 std::string("www.facebook.com")},
                {{std::string("http://www.baidu.com"), std::string("FILE")}, {std::string("")}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("QUERY"),
                  std::string("q")},
                 {std::string("1")}},
                {{std::string("fb.com/path/p1.p?q=1#f"), std::string("QUERY"), std::string("q")},
                 {std::string("1")}},
                {{std::string("http://facebook.com/path/p1"), std::string("QUERY"),
                  std::string("q")},
                 {Null()}},
                {{std::string("http://fb.com/path/p1.p?q=1#f"), std::string("HOST"),
                  std::string("q")},
                 {Null()}}};

        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_hex_test) {
    std::string func_name = "hex";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("AbCdEfg")}, std::string("41624364456667")},
            {{std::string("你好HELLO")}, std::string("E4BDA0E5A5BD48454C4C4F")},
            {{std::string("")}, std::string("")},
            {{Null()}, Null()},
            {{std::string("!@#$@* (!&#")}, std::string("21402324402A2028212623")},
            {{std::string("JSKAB(Q@__!")}, std::string("4A534B41422851405F5F21")},
            {{std::string("MY test Str你好  ")},
             std::string("4D59207465737420537472E4BDA0E5A5BD2020")},
            {{std::string("                 ")}, std::string("2020202020202020202020202020202020")},
            {{std::string("23 12 --!__!_!__!")}, std::string("3233203132202D2D215F5F215F215F5F21")},
            {{std::string("112+ + +")}, std::string("3131322B202B202B")},
            {{std::string("     +       23 ")}, std::string("20202020202B20202020202020323320")},
    };
    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_unhex_test) {
    std::string func_name = "unhex";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("41624364456667")}, std::string("AbCdEfg")},
            {{std::string("E4BDA0E5A5BD48454C4C4F")}, std::string("你好HELLO")},
            {{std::string("")}, std::string("")},
            {{Null()}, Null()},
            {{std::string("21402324402A2028212623")}, std::string("!@#$@* (!&#")},
            {{std::string("4A534B41422851405F5F21")}, std::string("JSKAB(Q@__!")},
            // {{std::string("M4D59207465737420537472E4BDA0E5A5BD2020")}, Null()},
            {{std::string("2020202020202020202020202020202020")}, std::string("                 ")},
            {{std::string("3233203132202D2D215F5F215F215F5F21")}, std::string("23 12 --!__!_!__!")},
            {{std::string("3131322B202B202B")}, std::string("112+ + +")},
            {{std::string("20202020202B20202020202020323320")}, std::string("     +       23 ")},
            // {{std::string("!")}, Null()},
    };
    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_coalesce_test) {
    std::string func_name = "coalesce";
    {
        BaseInputTypeSet input_types = {TypeIndex::Int32, TypeIndex::Int32, TypeIndex::Int32};
        DataSet data_set = {{{Null(), Null(), (int32_t)1}, {(int32_t)1}},
                            {{Null(), Null(), (int32_t)2}, {(int32_t)2}},
                            {{Null(), Null(), (int32_t)3}, {(int32_t)3}},
                            {{Null(), Null(), (int32_t)4}, {(int32_t)4}}};
        static_cast<void>(
                check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::Int32};
        DataSet data_set = {
                {{std::string("qwer"), Null(), (int32_t)1}, {std::string("qwer")}},
                {{std::string("asdf"), Null(), (int32_t)2}, {std::string("asdf")}},
                {{std::string("zxcv"), Null(), (int32_t)3}, {std::string("zxcv")}},
                {{std::string("vbnm"), Null(), (int32_t)4}, {std::string("vbnm")}},
        };
        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }

    {
        BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{Null(), std::string("abc"), std::string("hij")}, {std::string("abc")}},
                {{Null(), std::string("def"), std::string("klm")}, {std::string("def")}},
                {{Null(), std::string(""), std::string("xyz")}, {std::string("")}},
                {{Null(), Null(), std::string("uvw")}, {std::string("uvw")}}};
        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_replace) {
    std::string func_name = "replace";
    BaseInputTypeSet input_types = {
            TypeIndex::String,
            TypeIndex::String,
            TypeIndex::String,
    };
    DataSet data_set = {
            {{std::string("A"), std::string("A"), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string("A"), std::string(",")}, std::string(",")},
            {{std::string("A"), std::string("A"), std::string("")}, std::string("")},
            {{std::string("A"), std::string("A"), Null()}, Null()},
            {{std::string("A"), std::string("A"), std::string(",ABC,")}, std::string(",ABC,")},
            {{std::string("A"), std::string("A"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string("A"), std::string(","), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string(","), std::string(",")}, std::string("A")},
            {{std::string("A"), std::string(","), std::string("")}, std::string("A")},
            {{std::string("A"), std::string(","), Null()}, Null()},
            {{std::string("A"), std::string(","), std::string(",ABC,")}, std::string("A")},
            {{std::string("A"), std::string(","), std::string("123ABC!@# _")}, std::string("A")},
            {{std::string("A"), std::string(""), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string(""), std::string(",")}, std::string("A")},
            {{std::string("A"), std::string(""), std::string("")}, std::string("A")},
            {{std::string("A"), std::string(""), Null()}, Null()},
            {{std::string("A"), std::string(""), std::string(",ABC,")}, std::string("A")},
            {{std::string("A"), std::string(""), std::string("123ABC!@# _")}, std::string("A")},
            {{std::string("A"), Null(), std::string("A")}, Null()},
            {{std::string("A"), Null(), std::string(",")}, Null()},
            {{std::string("A"), Null(), std::string("")}, Null()},
            {{std::string("A"), Null(), Null()}, Null()},
            {{std::string("A"), Null(), std::string(",ABC,")}, Null()},
            {{std::string("A"), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string("A"), std::string(",ABC,"), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), std::string(",")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), std::string("")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), Null()}, Null()},
            {{std::string("A"), std::string(",ABC,"), std::string(",ABC,")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string(",")}, std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string("")}, std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string("A"), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string("A")},
            {{std::string(","), std::string("A"), std::string("A")}, std::string(",")},
            {{std::string(","), std::string("A"), std::string(",")}, std::string(",")},
            {{std::string(","), std::string("A"), std::string("")}, std::string(",")},
            {{std::string(","), std::string("A"), Null()}, Null()},
            {{std::string(","), std::string("A"), std::string(",ABC,")}, std::string(",")},
            {{std::string(","), std::string("A"), std::string("123ABC!@# _")}, std::string(",")},
            {{std::string(","), std::string(","), std::string("A")}, std::string("A")},
            {{std::string(","), std::string(","), std::string(",")}, std::string(",")},
            {{std::string(","), std::string(","), std::string("")}, std::string("")},
            {{std::string(","), std::string(","), Null()}, Null()},
            {{std::string(","), std::string(","), std::string(",ABC,")}, std::string(",ABC,")},
            {{std::string(","), std::string(","), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string(","), std::string(""), std::string("A")}, std::string(",")},
            {{std::string(","), std::string(""), std::string(",")}, std::string(",")},
            {{std::string(","), std::string(""), std::string("")}, std::string(",")},
            {{std::string(","), std::string(""), Null()}, Null()},
            {{std::string(","), std::string(""), std::string(",ABC,")}, std::string(",")},
            {{std::string(","), std::string(""), std::string("123ABC!@# _")}, std::string(",")},
            {{std::string(","), Null(), std::string("A")}, Null()},
            {{std::string(","), Null(), std::string(",")}, Null()},
            {{std::string(","), Null(), std::string("")}, Null()},
            {{std::string(","), Null(), Null()}, Null()},
            {{std::string(","), Null(), std::string(",ABC,")}, Null()},
            {{std::string(","), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string(","), std::string(",ABC,"), std::string("A")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), std::string(",")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), std::string("")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), Null()}, Null()},
            {{std::string(","), std::string(",ABC,"), std::string(",ABC,")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string("A")}, std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string(",")}, std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string("")}, std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string(","), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string(",")},
            {{std::string(""), std::string("A"), std::string("A")}, std::string("")},
            {{std::string(""), std::string("A"), std::string(",")}, std::string("")},
            {{std::string(""), std::string("A"), std::string("")}, std::string("")},
            {{std::string(""), std::string("A"), Null()}, Null()},
            {{std::string(""), std::string("A"), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string("A"), std::string("123ABC!@# _")}, std::string("")},
            {{std::string(""), std::string(","), std::string("A")}, std::string("")},
            {{std::string(""), std::string(","), std::string(",")}, std::string("")},
            {{std::string(""), std::string(","), std::string("")}, std::string("")},
            {{std::string(""), std::string(","), Null()}, Null()},
            {{std::string(""), std::string(","), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string(","), std::string("123ABC!@# _")}, std::string("")},
            {{std::string(""), std::string(""), std::string("A")}, std::string("")},
            {{std::string(""), std::string(""), std::string(",")}, std::string("")},
            {{std::string(""), std::string(""), std::string("")}, std::string("")},
            {{std::string(""), std::string(""), Null()}, Null()},
            {{std::string(""), std::string(""), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string(""), std::string("123ABC!@# _")}, std::string("")},
            {{std::string(""), Null(), std::string("A")}, Null()},
            {{std::string(""), Null(), std::string(",")}, Null()},
            {{std::string(""), Null(), std::string("")}, Null()},
            {{std::string(""), Null(), Null()}, Null()},
            {{std::string(""), Null(), std::string(",ABC,")}, Null()},
            {{std::string(""), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string(""), std::string(",ABC,"), std::string("A")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), std::string(",")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), std::string("")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), Null()}, Null()},
            {{std::string(""), std::string(",ABC,"), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), std::string("123ABC!@# _")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string("A")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string(",")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string("")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string(""), std::string("123ABC!@# _"), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string("")},
            {{Null(), std::string("A"), std::string("A")}, Null()},
            {{Null(), std::string("A"), std::string(",")}, Null()},
            {{Null(), std::string("A"), std::string("")}, Null()},
            {{Null(), std::string("A"), Null()}, Null()},
            {{Null(), std::string("A"), std::string(",ABC,")}, Null()},
            {{Null(), std::string("A"), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string(","), std::string("A")}, Null()},
            {{Null(), std::string(","), std::string(",")}, Null()},
            {{Null(), std::string(","), std::string("")}, Null()},
            {{Null(), std::string(","), Null()}, Null()},
            {{Null(), std::string(","), std::string(",ABC,")}, Null()},
            {{Null(), std::string(","), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string(""), std::string("A")}, Null()},
            {{Null(), std::string(""), std::string(",")}, Null()},
            {{Null(), std::string(""), std::string("")}, Null()},
            {{Null(), std::string(""), Null()}, Null()},
            {{Null(), std::string(""), std::string(",ABC,")}, Null()},
            {{Null(), std::string(""), std::string("123ABC!@# _")}, Null()},
            {{Null(), Null(), std::string("A")}, Null()},
            {{Null(), Null(), std::string(",")}, Null()},
            {{Null(), Null(), std::string("")}, Null()},
            {{Null(), Null(), Null()}, Null()},
            {{Null(), Null(), std::string(",ABC,")}, Null()},
            {{Null(), Null(), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string(",ABC,"), std::string("A")}, Null()},
            {{Null(), std::string(",ABC,"), std::string(",")}, Null()},
            {{Null(), std::string(",ABC,"), std::string("")}, Null()},
            {{Null(), std::string(",ABC,"), Null()}, Null()},
            {{Null(), std::string(",ABC,"), std::string(",ABC,")}, Null()},
            {{Null(), std::string(",ABC,"), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string("A")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string(",")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string("")}, Null()},
            {{Null(), std::string("123ABC!@# _"), Null()}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string(",ABC,")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string("123ABC!@# _")}, Null()},
            {{std::string(",ABC,"), std::string("A"), std::string("A")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("A"), std::string(",")}, std::string(",,BC,")},
            {{std::string(",ABC,"), std::string("A"), std::string("")}, std::string(",BC,")},
            {{std::string(",ABC,"), std::string("A"), Null()}, Null()},
            {{std::string(",ABC,"), std::string("A"), std::string(",ABC,")},
             std::string(",,ABC,BC,")},
            {{std::string(",ABC,"), std::string("A"), std::string("123ABC!@# _")},
             std::string(",123ABC!@# _BC,")},
            {{std::string(",ABC,"), std::string(","), std::string("A")}, std::string("AABCA")},
            {{std::string(",ABC,"), std::string(","), std::string(",")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(","), std::string("")}, std::string("ABC")},
            {{std::string(",ABC,"), std::string(","), Null()}, Null()},
            {{std::string(",ABC,"), std::string(","), std::string(",ABC,")},
             std::string(",ABC,ABC,ABC,")},
            {{std::string(",ABC,"), std::string(","), std::string("123ABC!@# _")},
             std::string("123ABC!@# _ABC123ABC!@# _")},
            {{std::string(",ABC,"), std::string(""), std::string("A")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(""), std::string(",")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(""), std::string("")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(""), Null()}, Null()},
            {{std::string(",ABC,"), std::string(""), std::string(",ABC,")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(""), std::string("123ABC!@# _")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), Null(), std::string("A")}, Null()},
            {{std::string(",ABC,"), Null(), std::string(",")}, Null()},
            {{std::string(",ABC,"), Null(), std::string("")}, Null()},
            {{std::string(",ABC,"), Null(), Null()}, Null()},
            {{std::string(",ABC,"), Null(), std::string(",ABC,")}, Null()},
            {{std::string(",ABC,"), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string("A")}, std::string("A")},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string(",")}, std::string(",")},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string("")}, std::string("")},
            {{std::string(",ABC,"), std::string(",ABC,"), Null()}, Null()},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string(",ABC,")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string("A")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string(",")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string("")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string(",ABC,")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string("A")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string(",")},
             std::string("123,BC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string("")},
             std::string("123BC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string("A"), std::string(",ABC,")},
             std::string("123,ABC,BC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string("123ABC!@# _")},
             std::string("123123ABC!@# _BC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string("A")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string(",")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string("")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string(","), std::string(",ABC,")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(""), std::string("A")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(""), std::string(",")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(""), std::string("")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(""), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string(""), std::string(",ABC,")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(""), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), Null(), std::string("A")}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string(",")}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string("")}, Null()},
            {{std::string("123ABC!@# _"), Null(), Null()}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string(",ABC,")}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string("A")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string(",")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string("")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string(",ABC,")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string("A")},
             std::string("A")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string(",")},
             std::string(",")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string("")},
             std::string("")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string(",ABC,")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
    };
    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_replace_empty) {
    std::string func_name = "replace_empty";
    BaseInputTypeSet input_types = {
            TypeIndex::String,
            TypeIndex::String,
            TypeIndex::String,
    };
    DataSet data_set = {
            {{std::string("A"), std::string("A"), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string("A"), std::string(",")}, std::string(",")},
            {{std::string("A"), std::string("A"), std::string("")}, std::string("")},
            {{std::string("A"), std::string("A"), Null()}, Null()},
            {{std::string("A"), std::string("A"), std::string(",ABC,")}, std::string(",ABC,")},
            {{std::string("A"), std::string("A"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string("A"), std::string(","), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string(","), std::string(",")}, std::string("A")},
            {{std::string("A"), std::string(","), std::string("")}, std::string("A")},
            {{std::string("A"), std::string(","), Null()}, Null()},
            {{std::string("A"), std::string(","), std::string(",ABC,")}, std::string("A")},
            {{std::string("A"), std::string(","), std::string("123ABC!@# _")}, std::string("A")},
            {{std::string("A"), std::string(""), std::string("A")}, std::string("AAA")},
            {{std::string("A"), std::string(""), std::string(",")}, std::string(",A,")},
            {{std::string("A"), std::string(""), std::string("")}, std::string("A")},
            {{std::string("A"), std::string(""), Null()}, Null()},
            {{std::string("A"), std::string(""), std::string(",ABC,")}, std::string(",ABC,A,ABC,")},
            {{std::string("A"), std::string(""), std::string("123ABC!@# _")},
             std::string("123ABC!@# _A123ABC!@# _")},
            {{std::string("A"), Null(), std::string("A")}, Null()},
            {{std::string("A"), Null(), std::string(",")}, Null()},
            {{std::string("A"), Null(), std::string("")}, Null()},
            {{std::string("A"), Null(), Null()}, Null()},
            {{std::string("A"), Null(), std::string(",ABC,")}, Null()},
            {{std::string("A"), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string("A"), std::string(",ABC,"), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), std::string(",")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), std::string("")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), Null()}, Null()},
            {{std::string("A"), std::string(",ABC,"), std::string(",ABC,")}, std::string("A")},
            {{std::string("A"), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string("A")}, std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string(",")}, std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string("")}, std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string("A"), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string("A")},
            {{std::string("A"), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string("A")},
            {{std::string(","), std::string("A"), std::string("A")}, std::string(",")},
            {{std::string(","), std::string("A"), std::string(",")}, std::string(",")},
            {{std::string(","), std::string("A"), std::string("")}, std::string(",")},
            {{std::string(","), std::string("A"), Null()}, Null()},
            {{std::string(","), std::string("A"), std::string(",ABC,")}, std::string(",")},
            {{std::string(","), std::string("A"), std::string("123ABC!@# _")}, std::string(",")},
            {{std::string(","), std::string(","), std::string("A")}, std::string("A")},
            {{std::string(","), std::string(","), std::string(",")}, std::string(",")},
            {{std::string(","), std::string(","), std::string("")}, std::string("")},
            {{std::string(","), std::string(","), Null()}, Null()},
            {{std::string(","), std::string(","), std::string(",ABC,")}, std::string(",ABC,")},
            {{std::string(","), std::string(","), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string(","), std::string(""), std::string("A")}, std::string("A,A")},
            {{std::string(","), std::string(""), std::string(",")}, std::string(",,,")},
            {{std::string(","), std::string(""), std::string("")}, std::string(",")},
            {{std::string(","), std::string(""), Null()}, Null()},
            {{std::string(","), std::string(""), std::string(",ABC,")}, std::string(",ABC,,,ABC,")},
            {{std::string(","), std::string(""), std::string("123ABC!@# _")},
             std::string("123ABC!@# _,123ABC!@# _")},
            {{std::string(","), Null(), std::string("A")}, Null()},
            {{std::string(","), Null(), std::string(",")}, Null()},
            {{std::string(","), Null(), std::string("")}, Null()},
            {{std::string(","), Null(), Null()}, Null()},
            {{std::string(","), Null(), std::string(",ABC,")}, Null()},
            {{std::string(","), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string(","), std::string(",ABC,"), std::string("A")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), std::string(",")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), std::string("")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), Null()}, Null()},
            {{std::string(","), std::string(",ABC,"), std::string(",ABC,")}, std::string(",")},
            {{std::string(","), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string("A")}, std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string(",")}, std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string("")}, std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string(","), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string(",")},
            {{std::string(","), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string(",")},
            {{std::string(""), std::string("A"), std::string("A")}, std::string("")},
            {{std::string(""), std::string("A"), std::string(",")}, std::string("")},
            {{std::string(""), std::string("A"), std::string("")}, std::string("")},
            {{std::string(""), std::string("A"), Null()}, Null()},
            {{std::string(""), std::string("A"), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string("A"), std::string("123ABC!@# _")}, std::string("")},
            {{std::string(""), std::string(","), std::string("A")}, std::string("")},
            {{std::string(""), std::string(","), std::string(",")}, std::string("")},
            {{std::string(""), std::string(","), std::string("")}, std::string("")},
            {{std::string(""), std::string(","), Null()}, Null()},
            {{std::string(""), std::string(","), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string(","), std::string("123ABC!@# _")}, std::string("")},
            {{std::string(""), std::string(""), std::string("A")}, std::string("A")},
            {{std::string(""), std::string(""), std::string(",")}, std::string(",")},
            {{std::string(""), std::string(""), std::string("")}, std::string("")},
            {{std::string(""), std::string(""), Null()}, Null()},
            {{std::string(""), std::string(""), std::string(",ABC,")}, std::string(",ABC,")},
            {{std::string(""), std::string(""), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string(""), Null(), std::string("A")}, Null()},
            {{std::string(""), Null(), std::string(",")}, Null()},
            {{std::string(""), Null(), std::string("")}, Null()},
            {{std::string(""), Null(), Null()}, Null()},
            {{std::string(""), Null(), std::string(",ABC,")}, Null()},
            {{std::string(""), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string(""), std::string(",ABC,"), std::string("A")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), std::string(",")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), std::string("")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), Null()}, Null()},
            {{std::string(""), std::string(",ABC,"), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string(",ABC,"), std::string("123ABC!@# _")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string("A")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string(",")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string("")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string(""), std::string("123ABC!@# _"), std::string(",ABC,")}, std::string("")},
            {{std::string(""), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string("")},
            {{Null(), std::string("A"), std::string("A")}, Null()},
            {{Null(), std::string("A"), std::string(",")}, Null()},
            {{Null(), std::string("A"), std::string("")}, Null()},
            {{Null(), std::string("A"), Null()}, Null()},
            {{Null(), std::string("A"), std::string(",ABC,")}, Null()},
            {{Null(), std::string("A"), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string(","), std::string("A")}, Null()},
            {{Null(), std::string(","), std::string(",")}, Null()},
            {{Null(), std::string(","), std::string("")}, Null()},
            {{Null(), std::string(","), Null()}, Null()},
            {{Null(), std::string(","), std::string(",ABC,")}, Null()},
            {{Null(), std::string(","), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string(""), std::string("A")}, Null()},
            {{Null(), std::string(""), std::string(",")}, Null()},
            {{Null(), std::string(""), std::string("")}, Null()},
            {{Null(), std::string(""), Null()}, Null()},
            {{Null(), std::string(""), std::string(",ABC,")}, Null()},
            {{Null(), std::string(""), std::string("123ABC!@# _")}, Null()},
            {{Null(), Null(), std::string("A")}, Null()},
            {{Null(), Null(), std::string(",")}, Null()},
            {{Null(), Null(), std::string("")}, Null()},
            {{Null(), Null(), Null()}, Null()},
            {{Null(), Null(), std::string(",ABC,")}, Null()},
            {{Null(), Null(), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string(",ABC,"), std::string("A")}, Null()},
            {{Null(), std::string(",ABC,"), std::string(",")}, Null()},
            {{Null(), std::string(",ABC,"), std::string("")}, Null()},
            {{Null(), std::string(",ABC,"), Null()}, Null()},
            {{Null(), std::string(",ABC,"), std::string(",ABC,")}, Null()},
            {{Null(), std::string(",ABC,"), std::string("123ABC!@# _")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string("A")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string(",")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string("")}, Null()},
            {{Null(), std::string("123ABC!@# _"), Null()}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string(",ABC,")}, Null()},
            {{Null(), std::string("123ABC!@# _"), std::string("123ABC!@# _")}, Null()},
            {{std::string(",ABC,"), std::string("A"), std::string("A")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("A"), std::string(",")}, std::string(",,BC,")},
            {{std::string(",ABC,"), std::string("A"), std::string("")}, std::string(",BC,")},
            {{std::string(",ABC,"), std::string("A"), Null()}, Null()},
            {{std::string(",ABC,"), std::string("A"), std::string(",ABC,")},
             std::string(",,ABC,BC,")},
            {{std::string(",ABC,"), std::string("A"), std::string("123ABC!@# _")},
             std::string(",123ABC!@# _BC,")},
            {{std::string(",ABC,"), std::string(","), std::string("A")}, std::string("AABCA")},
            {{std::string(",ABC,"), std::string(","), std::string(",")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(","), std::string("")}, std::string("ABC")},
            {{std::string(",ABC,"), std::string(","), Null()}, Null()},
            {{std::string(",ABC,"), std::string(","), std::string(",ABC,")},
             std::string(",ABC,ABC,ABC,")},
            {{std::string(",ABC,"), std::string(","), std::string("123ABC!@# _")},
             std::string("123ABC!@# _ABC123ABC!@# _")},
            {{std::string(",ABC,"), std::string(""), std::string("A")}, std::string("A,AAABACA,A")},
            {{std::string(",ABC,"), std::string(""), std::string(",")}, std::string(",,,A,B,C,,,")},
            {{std::string(",ABC,"), std::string(""), std::string("")}, std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(""), Null()}, Null()},
            {{std::string(",ABC,"), std::string(""), std::string(",ABC,")},
             std::string(",ABC,,,ABC,A,ABC,B,ABC,C,ABC,,,ABC,")},
            {{std::string(",ABC,"), std::string(""), std::string("123ABC!@# _")},
             std::string(
                     "123ABC!@# _,123ABC!@# _A123ABC!@# _B123ABC!@# _C123ABC!@# _,123ABC!@# _")},
            {{std::string(",ABC,"), Null(), std::string("A")}, Null()},
            {{std::string(",ABC,"), Null(), std::string(",")}, Null()},
            {{std::string(",ABC,"), Null(), std::string("")}, Null()},
            {{std::string(",ABC,"), Null(), Null()}, Null()},
            {{std::string(",ABC,"), Null(), std::string(",ABC,")}, Null()},
            {{std::string(",ABC,"), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string("A")}, std::string("A")},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string(",")}, std::string(",")},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string("")}, std::string("")},
            {{std::string(",ABC,"), std::string(",ABC,"), Null()}, Null()},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string(",ABC,")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string("A")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string(",")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string("")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string(",ABC,")},
            {{std::string(",ABC,"), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string(",ABC,")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string("A")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string(",")},
             std::string("123,BC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string("")},
             std::string("123BC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string("A"), std::string(",ABC,")},
             std::string("123,ABC,BC!@# _")},
            {{std::string("123ABC!@# _"), std::string("A"), std::string("123ABC!@# _")},
             std::string("123123ABC!@# _BC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string("A")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string(",")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string("")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string(","), std::string(",ABC,")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(","), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(""), std::string("A")},
             std::string("A1A2A3AAABACA!A@A#A A_A")},
            {{std::string("123ABC!@# _"), std::string(""), std::string(",")},
             std::string(",1,2,3,A,B,C,!,@,#, ,_,")},
            {{std::string("123ABC!@# _"), std::string(""), std::string("")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(""), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string(""), std::string(",ABC,")},
             std::string(
                     ",ABC,1,ABC,2,ABC,3,ABC,A,ABC,B,ABC,C,ABC,!,ABC,@,ABC,#,ABC, ,ABC,_,ABC,")},
            {{std::string("123ABC!@# _"), std::string(""), std::string("123ABC!@# _")},
             std::string(
                     "123ABC!@# _1123ABC!@# _2123ABC!@# _3123ABC!@# _A123ABC!@# _B123ABC!@# "
                     "_C123ABC!@# _!123ABC!@# _@123ABC!@# _#123ABC!@# _ 123ABC!@# __123ABC!@# _")},
            {{std::string("123ABC!@# _"), Null(), std::string("A")}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string(",")}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string("")}, Null()},
            {{std::string("123ABC!@# _"), Null(), Null()}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string(",ABC,")}, Null()},
            {{std::string("123ABC!@# _"), Null(), std::string("123ABC!@# _")}, Null()},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string("A")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string(",")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string("")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string(",ABC,")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string(",ABC,"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string("A")},
             std::string("A")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string(",")},
             std::string(",")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string("")},
             std::string("")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), Null()}, Null()},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string(",ABC,")},
             std::string(",ABC,")},
            {{std::string("123ABC!@# _"), std::string("123ABC!@# _"), std::string("123ABC!@# _")},
             std::string("123ABC!@# _")},
    };
    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_bit_length_test) {
    std::string func_name = "bit_length";
    BaseInputTypeSet input_types = {TypeIndex::String};
    DataSet data_set = {
            {{std::string("YXNk5L2g5aW9")}, std::int32_t(96)},
            {{std::string("aGVsbG8gd29ybGQ")}, std::int32_t(120)},
            {{std::string("SEVMTE8sIV4l")}, std::int32_t(96)},
            {{std::string("__123hehe1")}, std::int32_t(80)},
            {{std::string("")}, std::int32_t(0)},
            {{std::string("5ZWK5ZOI5ZOI5ZOI8J+YhCDjgILigJTigJQh")}, std::int32_t(288)},
            {{std::string("ò&ø")}, std::int32_t(40)},
            {{std::string("TVl0ZXN0U1RS")}, std::int32_t(96)},
            {{Null()}, Null()},
            {{std::string("123321!@#@$!@%!@#!@$!@")}, std::int32_t(176)},
            {{std::string("123")}, std::int32_t(24)},
    };
    static_cast<void>(
            check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(function_string_test, function_uuid_test) {
    {
        std::string func_name = "uuid_to_int";
        BaseInputTypeSet input_types = {TypeIndex::String};
        uint64_t high = 9572195551486940809ULL;
        uint64_t low = 1759290071393952876ULL;
        __int128 result = (__int128)high * (__int128)10000000000000000000ULL + (__int128)low;
        DataSet data_set = {{{Null()}, Null()},
                            {{std::string("6ce4766f-6783-4b30-b357-bba1c7600348")}, result},
                            {{std::string("6ce4766f67834b30b357bba1c7600348")}, result},
                            {{std::string("ffffffff-ffff-ffff-ffff-ffffffffffff")}, (__int128)-1},
                            {{std::string("00000000-0000-0000-0000-000000000000")}, (__int128)0},
                            {{std::string("123")}, Null()}};
        check_function_all_arg_comb<DataTypeInt128, true>(func_name, input_types, data_set);
    }
    {
        std::string func_name = "int_to_uuid";
        BaseInputTypeSet input_types = {TypeIndex::Int128};
        uint64_t high = 9572195551486940809ULL;
        uint64_t low = 1759290071393952876ULL;
        __int128 value = (__int128)high * (__int128)10000000000000000000ULL + (__int128)low;
        DataSet data_set = {{{Null()}, Null()},
                            {{value}, std::string("6ce4766f-6783-4b30-b357-bba1c7600348")},
                            {{(__int128)-1}, std::string("ffffffff-ffff-ffff-ffff-ffffffffffff")},
                            {{(__int128)0}, std::string("00000000-0000-0000-0000-000000000000")}};
        check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
    }
}

TEST(function_string_test, function_overlay_test) {
    std::string func_name = "overlay";
    {
        InputTypeSet input_types = {
                TypeIndex::String,
                TypeIndex::Int32,
                TypeIndex::Int32,
                TypeIndex::String,
        };
        DataSet data_set = {
                {{
                         Null(),
                         INT(7),
                         INT(5),
                         VARCHAR("9090"),
                 },
                 {Null()}},
                {{
                         VARCHAR("Test"),
                         Null(),
                         INT(5),
                         VARCHAR("9090"),
                 },
                 {Null()}},
                {{
                         VARCHAR("Test"),
                         INT(7),
                         Null(),
                         VARCHAR("9090"),
                 },
                 {Null()}},
                {{
                         VARCHAR("Test"),
                         INT(7),
                         INT(5),
                         Null(),
                 },
                 {Null()}},
                {{VARCHAR("http://www.baidu.com:9090"), INT(22), INT(4), VARCHAR("")},
                 {VARCHAR("http://www.baidu.com:")}},
                {{VARCHAR("aaaaa"), INT(0), INT(50), VARCHAR("bbbbb")}, {VARCHAR("aaaaa")}},
                {{VARCHAR("aaaaa"), INT(2), INT(3), VARCHAR("bbbbb")}, {VARCHAR("abbbbba")}},
                {{VARCHAR("aaaaa"), INT(6), INT(2), VARCHAR("bbbbb")}, {VARCHAR("aaaaa")}},
                {{VARCHAR("aaaaa"), INT(-10), INT(2), VARCHAR("bbbbb")}, {VARCHAR("aaaaa")}},
                {{VARCHAR("こaaaa"), INT(-1), INT(2), VARCHAR("にちは")}, {VARCHAR("こaaaa")}},
                {{VARCHAR("こaaaa"), INT(2), INT(2), VARCHAR("にちは")}, {VARCHAR("こにちはaa")}},
                {{VARCHAR("你好123世界"), INT(2), INT(2), VARCHAR("我的")},
                 {VARCHAR("你我的23世界")}},
                {{VARCHAR("你好123世界"), INT(-1), INT(2), VARCHAR("我的")},
                 {VARCHAR("你好123世界")}},
                {{VARCHAR("你好123世界"), INT(10), INT(2), VARCHAR("我的")},
                 {VARCHAR("你好123世界")}},
                {{VARCHAR("你好123世界"), INT(2), INT(10), VARCHAR("我的")}, {VARCHAR("你我的")}},
                {{VARCHAR("aaaaa"), INT(2), INT(-1), VARCHAR("bbbbb")}, {VARCHAR("abbbbb")}}};
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
    {
        InputTypeSet input_types = {
                TypeIndex::String,
                Consted {TypeIndex::Int32},
                Consted {TypeIndex::Int32},
                Consted {TypeIndex::String},
        };
        DataSet data_set = {
                {{
                         Null(),
                         INT(7),
                         INT(5),
                         VARCHAR("9090"),
                 },
                 {Null()}},
                {{
                         VARCHAR("Test"),
                         Null(),
                         INT(5),
                         VARCHAR("9090"),
                 },
                 {Null()}},
                {{
                         VARCHAR("Test"),
                         INT(7),
                         Null(),
                         VARCHAR("9090"),
                 },
                 {Null()}},
                {{
                         VARCHAR("Test"),
                         INT(7),
                         INT(5),
                         Null(),
                 },
                 {Null()}},
                {{VARCHAR("http://www.baidu.com:9090"), INT(22), INT(4), VARCHAR("")},
                 {VARCHAR("http://www.baidu.com:")}},
                {{VARCHAR("aaaaa"), INT(0), INT(50), VARCHAR("bbbbb")}, {VARCHAR("aaaaa")}},
                {{VARCHAR("aaaaa"), INT(2), INT(3), VARCHAR("bbbbb")}, {VARCHAR("abbbbba")}},
                {{VARCHAR("aaaaa"), INT(6), INT(2), VARCHAR("bbbbb")}, {VARCHAR("aaaaa")}},
                {{VARCHAR("aaaaa"), INT(-10), INT(2), VARCHAR("bbbbb")}, {VARCHAR("aaaaa")}},
                {{VARCHAR("こaaaa"), INT(-1), INT(2), VARCHAR("にちは")}, {VARCHAR("こaaaa")}},
                {{VARCHAR("こaaaa"), INT(2), INT(2), VARCHAR("にちは")}, {VARCHAR("こにちはaa")}},
                {{VARCHAR("你好123世界"), INT(2), INT(2), VARCHAR("我的")},
                 {VARCHAR("你我的23世界")}},
                {{VARCHAR("你好123世界"), INT(-1), INT(2), VARCHAR("我的")},
                 {VARCHAR("你好123世界")}},
                {{VARCHAR("你好123世界"), INT(10), INT(2), VARCHAR("我的")},
                 {VARCHAR("你好123世界")}},
                {{VARCHAR("你好123世界"), INT(2), INT(10), VARCHAR("我的")}, {VARCHAR("你我的")}},
                {{VARCHAR("aaaaa"), INT(2), INT(-1), VARCHAR("bbbbb")}, {VARCHAR("abbbbb")}}};
        for (const auto& line : data_set) {
            DataSet const_dataset = {line};
            static_cast<void>(
                    check_function<DataTypeString, true>(func_name, input_types, const_dataset));
        }
    }
}

TEST(function_string_test, function_initcap) {
    std::string func_name {"initcap"};

    BaseInputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {{{std::string("SKJ_ASD_SAD _1A")}, std::string("Skj_Asd_Sad _1a")},
                        {{std::string("BC'S aaaaA'' 'S")}, std::string("Bc'S Aaaaa'' 'S")},
                        {{std::string("NULL")}, std::string("Null")},
                        {{Null()}, Null()},
                        {{std::string("HELLO, WORLD!")}, std::string("Hello, World!")},
                        {{std::string("HHHH+-1; asAAss__!")}, std::string("Hhhh+-1; Asaass__!")},
                        {{std::string("a,B,C,D")}, std::string("A,B,C,D")}};

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_lpad_test) {
    std::string func_name = "lpad";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::String};

    DataSet data_set = {
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("__123hehe1")},
             std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("")}, std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("ò&ø")}, std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("TVl0ZXN0U1RS")},
             std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), Null()}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), Null()}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("TVl0ZXN0U1RS")},
             Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), Null()}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("__123hehe1")},
             std::string("")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("TVl0ZXN0U1RS")},
             std::string("")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), Null()}, Null()},
            {{std::string(""), std::int32_t(1), std::string("__123hehe1")}, std::string("_")},
            // {{std::string(""), std::int32_t(1), std::string("")}, std::string("")},
            // {{std::string(""), std::int32_t(1), std::string("ò&ø")}, std::string("ò")},
            {{std::string(""), std::int32_t(1), std::string("TVl0ZXN0U1RS")}, std::string("T")},
            {{std::string(""), std::int32_t(1), Null()}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("")}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string(""), std::int32_t(-1), Null()}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("")}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string(""), std::int32_t(-100), Null()}, Null()},
            {{std::string(""), std::int32_t(0), std::string("__123hehe1")}, std::string("")},
            {{std::string(""), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string(""), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string(""), std::int32_t(0), std::string("TVl0ZXN0U1RS")}, std::string("")},
            {{std::string(""), std::int32_t(0), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(1), std::string("__123hehe1")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), std::string("")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), std::string("ò&ø")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), std::string("TVl0ZXN0U1RS")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(0), std::string("__123hehe1")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), std::string("TVl0ZXN0U1RS")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("__123hehe1")},
             std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("")}, std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("ò&ø")}, std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("TVl0ZXN0U1RS")},
             std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("TVl0ZXN0U1RS")},
             Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("__123hehe1")},
             std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("TVl0ZXN0U1RS")},
             std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), Null()}, Null()},
            {{Null(), std::int32_t(1), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(1), std::string("")}, Null()},
            {{Null(), std::int32_t(1), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(1), Null()}, Null()},
            {{Null(), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(-1), std::string("")}, Null()},
            {{Null(), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(-1), Null()}, Null()},
            {{Null(), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(-100), std::string("")}, Null()},
            {{Null(), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(-100), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(-100), Null()}, Null()},
            {{Null(), std::int32_t(0), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(0), std::string("")}, Null()},
            {{Null(), std::int32_t(0), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(0), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(0), Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

TEST(function_string_test, function_rpad_test) {
    std::string func_name = "rpad";

    BaseInputTypeSet input_types = {TypeIndex::String, TypeIndex::Int32, TypeIndex::String};

    DataSet data_set = {
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("__123hehe1")},
             std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("")}, std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("ò&ø")}, std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), std::string("TVl0ZXN0U1RS")},
             std::string("Y")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(1), Null()}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-1), Null()}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), std::string("TVl0ZXN0U1RS")},
             Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(-100), Null()}, Null()},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("__123hehe1")},
             std::string("")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string("YXNk5L2g5aW9"), std::int32_t(0), std::string("TVl0ZXN0U1RS")},
             std::string("")},
            // {{std::string("YXNk5L2g5aW9"), std::int32_t(0), Null()}, Null()},
            // {{std::string(""), std::int32_t(1), std::string("__123hehe1")}, std::string("_")},
            // {{std::string(""), std::int32_t(1), std::string("")}, std::string("")},
            {{std::string(""), std::int32_t(1), std::string("ò&ø")}, std::string("ò")},
            {{std::string(""), std::int32_t(1), std::string("TVl0ZXN0U1RS")}, std::string("T")},
            {{std::string(""), std::int32_t(1), Null()}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("")}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string(""), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string(""), std::int32_t(-1), Null()}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("")}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string(""), std::int32_t(-100), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string(""), std::int32_t(-100), Null()}, Null()},
            {{std::string(""), std::int32_t(0), std::string("__123hehe1")}, std::string("")},
            {{std::string(""), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string(""), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string(""), std::int32_t(0), std::string("TVl0ZXN0U1RS")}, std::string("")},
            {{std::string(""), std::int32_t(0), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(1), std::string("__123hehe1")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), std::string("")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), std::string("ò&ø")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), std::string("TVl0ZXN0U1RS")}, std::string("ò")},
            {{std::string("ò&ø"), std::int32_t(1), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-1), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("ò&ø"), std::int32_t(-100), Null()}, Null()},
            {{std::string("ò&ø"), std::int32_t(0), std::string("__123hehe1")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), std::string("TVl0ZXN0U1RS")}, std::string("")},
            {{std::string("ò&ø"), std::int32_t(0), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("__123hehe1")},
             std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("")}, std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("ò&ø")}, std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), std::string("TVl0ZXN0U1RS")},
             std::string("T")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(1), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-1), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), std::string("TVl0ZXN0U1RS")},
             Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(-100), Null()}, Null()},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("__123hehe1")},
             std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("")}, std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("ò&ø")}, std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), std::string("TVl0ZXN0U1RS")},
             std::string("")},
            {{std::string("TVl0ZXN0U1RS"), std::int32_t(0), Null()}, Null()},
            {{Null(), std::int32_t(1), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(1), std::string("")}, Null()},
            {{Null(), std::int32_t(1), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(1), Null()}, Null()},
            {{Null(), std::int32_t(-1), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(-1), std::string("")}, Null()},
            {{Null(), std::int32_t(-1), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(-1), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(-1), Null()}, Null()},
            {{Null(), std::int32_t(-100), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(-100), std::string("")}, Null()},
            {{Null(), std::int32_t(-100), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(-100), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(-100), Null()}, Null()},
            {{Null(), std::int32_t(0), std::string("__123hehe1")}, Null()},
            {{Null(), std::int32_t(0), std::string("")}, Null()},
            {{Null(), std::int32_t(0), std::string("ò&ø")}, Null()},
            {{Null(), std::int32_t(0), std::string("TVl0ZXN0U1RS")}, Null()},
            {{Null(), std::int32_t(0), Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

} // namespace doris::vectorized
