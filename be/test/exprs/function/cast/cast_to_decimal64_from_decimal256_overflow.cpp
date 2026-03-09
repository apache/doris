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

#include "common/exception.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/number_traits.h"
#include "core/extended_types.h"
#include "core/types.h"
#include "exprs/function/cast/cast_test.h"
#include "exprs/function/cast/cast_to_decimal_test.h"
#include "storage/olap_common.h"
#include "testutil/test_util.h"

namespace doris::vectorized {

TEST_F(FunctionCastToDecimalTest, test_to_decimal64_from_decimal256_overflow) {
    between_decimal_overflow_test_func<Decimal256, Decimal64>();
}
} // namespace doris::vectorized
