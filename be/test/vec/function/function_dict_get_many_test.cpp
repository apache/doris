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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "function_test_util.h"
#include "testutil/column_helper.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/complex_hash_map_dictionary.h"
#include "vec/functions/dictionary_factory.h"

namespace doris::vectorized {

TEST(FunctionDictGetManyTest, test_without_dict_function) {
    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            "dict_get_many", ColumnsWithTypeAndName {}, {}, {.enable_decimal256 = false},
            BeExecVersionManager::get_newest_version());

    EXPECT_TRUE(function);

    FunctionContext fn_context {};
    fn_context._arg_types.resize(3);
    std::vector<std::shared_ptr<doris::ColumnPtrWrapper>> constant_cols {
            std::make_shared<doris::ColumnPtrWrapper>(
                    ColumnHelper::create_column<DataTypeString>({"test"})),
    };

    fn_context.set_constant_cols(constant_cols);

    try {
        auto st = function->open(&fn_context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        FAIL() << "FunctionDictGet should fail without dictionary";
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
    }
}

TEST(FunctionDictGetManyTest, test_without_dict_id) {
    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            "dict_get_many", ColumnsWithTypeAndName {}, {}, {.enable_decimal256 = false},
            BeExecVersionManager::get_newest_version());

    EXPECT_TRUE(function);

    FunctionContext fn_context {};
    fn_context._arg_types.resize(3);
    std::vector<std::shared_ptr<doris::ColumnPtrWrapper>> constant_cols {
            std::make_shared<doris::ColumnPtrWrapper>(
                    ColumnHelper::create_column<DataTypeString>({"test"})),
    };

    fn_context.set_constant_cols(constant_cols);

    TDictFunction dict_fn;
    dict_fn.dictionary_id = 0;
    dict_fn.version_id = 0;

    fn_context.set_dict_function(dict_fn);

    std::unique_ptr<DictionaryFactory> dict_factory = std::make_unique<DictionaryFactory>();
    ExecEnv::GetInstance()->_dict_factory = dict_factory.get();
    try {
        auto st = function->open(&fn_context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        FAIL() << "FunctionDictGet should fail without dictionary";
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
    }
}
} // namespace doris::vectorized
