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

#include "function_test_util.h"
#include "vec/functions/uuid.cpp"

namespace doris::vectorized {

using namespace ut_type;

TEST(function_uuid_test, function_is_uuid_test) {
    std::string func_name = "is_uuid";
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{STRING("6ccd780c-baba-1026-9564-5b8c656024db")}, BOOLEAN(1)},
            {{STRING("6ccd780c-baba-1026-9564-5b8c656024dbaaaa")}, BOOLEAN(0)},
            {{STRING("6ccd780c-baba-1026-9564-5b8c656024gg")}, BOOLEAN(0)},
            {{STRING("6ccd780-cbaba-1026-9564-5b8c656024db")}, BOOLEAN(0)},
            {{STRING("6ccd780-cbaba-1026-95645-b8c656024db")}, BOOLEAN(0)},
            {{STRING("6ccd780-cbaba-1026-95645-b8c65602")}, BOOLEAN(0)},
            {{STRING("{6ccd780c-baba-1026-9564-5b8c656024db}")}, BOOLEAN(1)},
            {{STRING("{6ccd780c-baba-1026-95645b8c656024db}")}, BOOLEAN(0)},
            {{STRING("{6ccd780c-baba-1026-95645-b8c656024db}")}, BOOLEAN(0)},
            {{STRING("6ccd780c-baba-1026-95645-b8c656024db}")}, BOOLEAN(0)},
            {{STRING("6ccd780cbaba102695645b8c656024db")}, BOOLEAN(1)},
            {{STRING("6ccd780cbaba102695645b8c656024dz")}, BOOLEAN(0)},
            {{STRING("6ccd780cbaba102")}, BOOLEAN(0)},
            {{STRING("{6ccd780cbaba102}")}, BOOLEAN(0)},
            {{Null()}, Null()},
    };

    check_function_all_arg_comb<DataTypeBool, true>(func_name, input_types, data_set);
}

} // namespace doris::vectorized
