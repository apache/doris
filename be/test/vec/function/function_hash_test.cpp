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

#include "vec/functions/function_hash.h"

#include <stdint.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "util/murmur_hash3.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(HashFunctionTest, murmur_hash_3_test) {
    std::string func_name = "murmur_hash3_32";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{Null()}, Null()}, {{std::string("hello")}, (int32_t)1321743225}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{std::string("hello"), std::string("world")}, (int32_t)984713481},
                            {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int32_t)-666935433},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, murmur_hash_3_64_test) {
    std::string func_name = "murmur_hash3_64";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{Null()}, Null()},
                            {{std::string("hello")}, (int64_t)-3215607508166160593}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {
                {{std::string("hello"), std::string("world")}, (int64_t)3583109472027628045},
                {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int64_t)1887828212617890932},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, murmur_hash_3_64_v2_test) {
    std::string func_name = "murmur_hash3_64_v2";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{std::string("1000209601_1756808272")}, (int64_t)4038800892574899471},
                            {{std::string("hello world")}, (int64_t)5998619086395760910},
                            {{std::string("apache doris")}, (int64_t)3669213779466221743}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, murmur_hash_get_name_test) {
    EXPECT_STREQ(murmur_hash3_get_name_type_int_for_test(), "murmur_hash3_32");
    EXPECT_STREQ(murmur_hash3_get_name_type_bigint_for_test(), "murmur_hash3_64");
    EXPECT_STREQ(murmur_hash3_get_name_type_bigint_v2_for_test(), "murmur_hash3_64_v2");
}

TEST(HashFunctionTest, xxhash_32_test) {
    std::string func_name = "xxhash_32";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{Null()}, Null()}, {{std::string("hello")}, (int32_t)-83855367}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{std::string("hello"), std::string("world")}, (int32_t)-920844969},
                            {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int32_t)352087701},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY};

        DataSet data_set = {{{Null()}, Null()}, {{VARBINARY("hello")}, (int32_t)-83855367}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARBINARY};

        DataSet data_set = {{{VARBINARY("hello"), VARBINARY("world")}, (int32_t)-920844969},
                            {{VARBINARY("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARBINARY,
                                    PrimitiveType::TYPE_VARBINARY};

        DataSet data_set = {
                {{VARBINARY("hello"), VARBINARY("world"), VARBINARY("!")}, (int32_t)352087701},
                {{VARBINARY("hello"), VARBINARY("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, xxhash_64_test) {
    std::string func_name = "xxhash_64";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{Null()}, Null()},
                            {{std::string("hello")}, (int64_t)-7685981735718036227}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {
                {{std::string("hello"), std::string("world")}, (int64_t)7001965798170371843},
                {{std::string("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                                    PrimitiveType::TYPE_VARCHAR};

        DataSet data_set = {{{std::string("hello"), std::string("world"), std::string("!")},
                             (int64_t)6796829678999971400},
                            {{std::string("hello"), std::string("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY};

        DataSet data_set = {{{Null()}, Null()},
                            {{VARBINARY("hello")}, (int64_t)-7685981735718036227}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARBINARY};

        DataSet data_set = {
                {{VARBINARY("hello"), VARBINARY("world")}, (int64_t)7001965798170371843},
                {{VARBINARY("hello"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARBINARY, PrimitiveType::TYPE_VARBINARY,
                                    PrimitiveType::TYPE_VARBINARY};

        DataSet data_set = {{{VARBINARY("hello"), VARBINARY("world"), VARBINARY("!")},
                             (int64_t)6796829678999971400},
                            {{VARBINARY("hello"), VARBINARY("world"), Null()}, Null()}};

        static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
    };
}

TEST(HashFunctionTest, murmur_hash3_helper_functions_test) {
    {
        std::string input = "hello world";
        uint64_t h1 = 0;
        uint64_t h2 = 0;
        murmur_hash3_x64_process(input.data(), input.size(), h1, h2);
        EXPECT_EQ(h1, 5998619086395760910ULL);
        EXPECT_EQ(h2, 12364428806279881649ULL);
    }

    {
        std::string input = "hello world";
        uint64_t out[2] = {0, 0};
        murmur_hash3_x64_128(input.data(), input.size(), 0, out);
        EXPECT_TRUE(out[0] == 5998619086395760910ULL && out[1] == 12364428806279881649ULL);
    }

    {
        std::string input = "hello world";
        uint64_t out = 0;
        murmur_hash3_x64_64_shared(input.data(), input.size(), 0, &out);
        EXPECT_EQ(out, 5998619086395760910ULL);
    }

    {
        std::string input = "hello";
        uint64_t out = 0;
        murmur_hash3_x64_64(input.data(), input.size(), 0, &out);
        EXPECT_EQ(out, static_cast<uint64_t>(-3215607508166160593LL));
    }

    {
        std::string input = "";
        uint64_t h1 = 0, h2 = 0;
        murmur_hash3_x64_process(input.data(), input.size(), h1, h2);
        EXPECT_EQ(h1, 0ULL);
        EXPECT_EQ(h2, 0ULL);
    }
}

} // namespace doris::vectorized
