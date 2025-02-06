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

#include "function_hash_map_dict_test.h"

#include <tuple>

#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/dictionary.h"

namespace doris::vectorized {

using KeyTypes =
        std::tuple<DataTypeUInt8, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                   DataTypeInt128, DataTypeFloat32, DataTypeFloat64, DataTypeIPv4, DataTypeIPv6,
                   DataTypeString, DataTypeDateV2, DataTypeDateTimeV2>;

using ValueTypes =
        std::tuple<DataTypeUInt8, DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64,
                   DataTypeInt128, DataTypeFloat32, DataTypeFloat64, DataTypeIPv4, DataTypeIPv6,
                   DataTypeString, DataTypeDateV2, DataTypeDateTimeV2>;

template <typename Keys, typename Values, size_t size, bool use_string64, size_t KIndex = 0,
          size_t VIndex = 0>
struct CartesianProduct {
    static void execute() {
        using Key = std::tuple_element_t<KIndex, Keys>;
        using Value = std::tuple_element_t<VIndex, Values>;
        test_hash_map_dict<Key, Value, size, use_string64>();

        if constexpr (VIndex + 1 < std::tuple_size_v<Values>) {
            CartesianProduct<Keys, Values, size, use_string64, KIndex, VIndex + 1>::execute();
        } else if constexpr (KIndex + 1 < std::tuple_size_v<Keys>) {
            CartesianProduct<Keys, Values, size, use_string64, KIndex + 1, 0>::execute();
        }
    }
};

TEST(HashMapDictTest, Test1) {
    CartesianProduct<KeyTypes, ValueTypes, 10, false>::execute();
}
TEST(HashMapDictTest, Test2) {
    CartesianProduct<KeyTypes, ValueTypes, 100, false>::execute();
}
TEST(HashMapDictTest, Test3) {
    CartesianProduct<KeyTypes, ValueTypes, 10, true>::execute();
}
TEST(HashMapDictTest, Test4) {
    CartesianProduct<KeyTypes, ValueTypes, 100, true>::execute();
}

} // namespace doris::vectorized
