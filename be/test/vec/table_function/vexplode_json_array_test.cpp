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

#include "vec/exprs/table_function/vexplode_json_array.h"

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

namespace doris::vectorized {
TEST(VExplodeJsonArrayTest, test_double) {
    ParsedDataDouble parsed_data_double;

    JsonbWriter jsonb_writer;

    jsonb_writer.writeStartArray();

    jsonb_writer.writeDouble(1.23);
    jsonb_writer.writeDouble(4.56);
    jsonb_writer.writeDouble(7.89);
    jsonb_writer.writeInt(100);

    Decimal32 decimal_value32(int32_t(99999999));
    jsonb_writer.writeDecimal(decimal_value32, 9, 4);

    Decimal64 decimal_value64(int64_t(999999999999999999ULL));
    jsonb_writer.writeDecimal(decimal_value64, 18, 4);

    Decimal128V3 decimal_value((__int128_t(std::numeric_limits<uint64_t>::max())));
    jsonb_writer.writeDecimal(decimal_value, 30, 8);

    wide::Int256 int256_value(wide::Int256(std::numeric_limits<__int128_t>::max()) * 2);
    vectorized::Decimal256 decimal256_value(int256_value);
    jsonb_writer.writeDecimal(decimal256_value, 40, 8);

    jsonb_writer.writeEndArray();

    auto* jsonb_doc = jsonb_writer.getDocument();

    parsed_data_double.set_output(*jsonb_doc->getValue()->unpack<ArrayVal>(),
                                  jsonb_doc->getValue()->unpack<ArrayVal>()->numElem());

    ASSERT_EQ(parsed_data_double._backup_data.size(),
              jsonb_doc->getValue()->unpack<ArrayVal>()->numElem());
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[0], 1.23);
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[1], 4.56);
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[2], 7.89);
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[3], 100);
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[4], 9999.9999);
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[5], 99999999999999.9999);
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[6], 184467440737.09551615);
    ASSERT_DOUBLE_EQ(parsed_data_double._backup_data[7], 3.4028236692093847e+30);
}
} // namespace doris::vectorized