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

#include <fstream>
#include <memory>

#include "cast_test.h"
#include "cast_to_decimal.h"
#include "common/exception.h"
#include "olap/olap_common.h"
#include "testutil/test_util.h"
#include "vec/core/extended_types.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/number_traits.h"

namespace doris::vectorized {

TEST_F(FunctionCastToDecimalTest, test_to_decimal64_from_decimal128_overflow) {
    between_decimal_overflow_test_func<Decimal128V3, Decimal64>();
}
} // namespace doris::vectorized
