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

#include "exec/connector/jni_connector.h"

#include <gtest/gtest.h>

#include <array>
#include <limits>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_timestamp_ns.h"

namespace doris {

TEST(JniConnectorTest, TimestampNsPreservesSignedValuesAndBatchOffsets) {
    auto type = make_nullable(std::make_shared<DataTypeTimeStampNs>());
    EXPECT_EQ(JniConnector::get_jni_type(type), "timestamp_ns");
    EXPECT_EQ(JniConnector::get_jni_type_with_different_string(type), "timestamp_ns");
    Block block({ColumnWithTypeAndName(type->create_column(), type, "ts")});
    std::array<uint8_t, 4> nulls {0, 0, 1, 0};
    std::array<int64_t, 4> values {std::numeric_limits<int64_t>::min(), -1, 0,
                                   std::numeric_limits<int64_t>::max()};
    std::array<long, 3> meta {4, reinterpret_cast<long>(nulls.data()),
                              reinterpret_cast<long>(values.data())};
    // Appending a second batch must advance by timestamp elements, not raw bytes.
    for (int batch = 0; batch < 2; ++batch) {
        ASSERT_TRUE(
                JniConnector::fill_block(&block, {0}, reinterpret_cast<long>(meta.data())).ok());
    }
    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& data =
            assert_cast<const ColumnTimeStampNs&>(nullable.get_nested_column()).get_data();
    ASSERT_EQ(data.size(), 8);
    for (size_t i = 0; i < data.size(); ++i) {
        EXPECT_EQ(data[i].epoch_nanos(), values[i % values.size()]);
        EXPECT_EQ(nullable.get_null_map_data()[i], nulls[i % nulls.size()]);
    }
}

} // namespace doris
