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

#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(FunctionUrlTEST, DomainTest) {
    std::string func_name = "domain";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
            {{Null()}, Null()},
            {{STRING("http://paul@www.example.com:80/")}, STRING("www.example.com")},
            {{STRING("http:/paul/example/com")}, STRING("")},
            {{STRING("http://www.example.com?q=4")}, STRING("www.example.com")},
            {{STRING("http://127.0.0.1:443/")}, STRING("127.0.0.1")},
            {{STRING("//www.example.com")}, STRING("www.example.com")},
            {{STRING("//paul@www.example.com")}, STRING("www.example.com")},
            {{STRING("www.example.com")}, STRING("www.example.com")},
            {{STRING("example.com")}, STRING("example.com")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionUrlTEST, DomainWithoutWWWTest) {
    std::string func_name = "domain_without_www";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
            {{Null()}, Null()},
            {{STRING("http://paul@www.example.com:80/")}, STRING("example.com")},
            {{STRING("http:/paul/example/com")}, STRING("")},
            {{STRING("http://www.example.com?q=4")}, STRING("example.com")},
            {{STRING("http://127.0.0.1:443/")}, STRING("127.0.0.1")},
            {{STRING("//www.example.com")}, STRING("example.com")},
            {{STRING("//paul@www.example.com")}, STRING("example.com")},
            {{STRING("www.example.com")}, STRING("example.com")},
            {{STRING("example.com")}, STRING("example.com")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(FunctionUrlTEST, ProtocolTest) {
    std::string func_name = "protocol";
    InputTypeSet input_types = {TypeIndex::String};

    DataSet data_set = {
            {{Null()}, Null()},
            {{STRING("http://paul@www.example.com:80/")}, STRING("http")},
            {{STRING("http:/paul/example/com")}, STRING("http")},
            {{STRING("http://www.example.com?q=4")}, STRING("http")},
            {{STRING("http://127.0.0.1:443/")}, STRING("http")},
            {{STRING("//www.example.com")}, STRING("")},
            {{STRING("//paul@www.example.com")}, STRING("")},
            {{STRING("www.example.com")}, STRING("")},
            {{STRING("example.com")}, STRING("")},
            {{STRING("https://example.com/")}, STRING("https")},
            {{STRING("svn+ssh://example.com?q=hello%20world")}, STRING("svn+ssh")},
            {{STRING("ftp://example.com/")}, STRING("ftp")},
            {{STRING("ftp!://example.com/")}, STRING("")},
            {{STRING("http://127.0.0.1:443/")}, STRING("http")},
            {{STRING("https!://example.com/")}, STRING("")},
            {{STRING("http!://example.com/")}, STRING("")},
    };

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
