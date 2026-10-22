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

#include "core/data_type_serde/orc_timestamp_decode_context.h"

#include <gtest/gtest.h>

#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/orc_serde_utils.h"

namespace doris {

TEST(OrcTimestampDecodeContextTest, SessionIsTheDefaultReaderTimezone) {
    const OrcTimestampDecodeContext context(cctz::fixed_time_zone(cctz::seconds(8 * 3600)));
    const auto plain = orc::createPrimitiveType(orc::TIMESTAMP);
    const auto instant = orc::createPrimitiveType(orc::TIMESTAMP_INSTANT);
    EXPECT_EQ(context.timezone_for(*plain).name(), context.session_timezone.name());
    EXPECT_EQ(context.timezone_for(*instant).name(), context.session_timezone.name());
}

TEST(OrcTimestampDecodeContextTest, PhysicalTypeSelectsCivilOrInstantTimezone) {
    OrcTimestampDecodeContext context(cctz::fixed_time_zone(cctz::seconds(8 * 3600)));
    ASSERT_TRUE(cctz::load_time_zone("America/New_York", &context.reader_timezone));
    orc::TimestampVectorBatch batch(1, *orc::getDefaultPool());
    batch.numElements = 1;
    batch.notNull[0] = 1;
    batch.data[0] = 1;
    batch.nanoseconds[0] = 0;
    const DataTypeDateTimeV2 type(6);
    for (const auto kind : {orc::TIMESTAMP, orc::TIMESTAMP_INSTANT}) {
        auto file_type = orc::createPrimitiveType(kind);
        OrcDecodedColumnView view {.file_type = file_type.get(),
                                   .batch = &batch,
                                   .rows = 1,
                                   .timestamp_context = &context};
        auto column = type.create_column();
        auto status = type.get_serde()->read_column_from_orc(*column, view);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(type.to_string(*column, 0), kind == orc::TIMESTAMP
                                                      ? "1969-12-31 19:00:01.000000"
                                                      : "1970-01-01 08:00:01.000000");
    }
}

TEST(OrcTimestampDecodeContextTest, NestedViewsKeepContextAcrossStripeTimezoneChanges) {
    OrcTimestampDecodeContext context(cctz::fixed_time_zone(cctz::seconds(8 * 3600)));
    auto schema = orc::createStructType();
    schema->addStructField("plain", orc::createPrimitiveType(orc::TIMESTAMP));
    schema->addStructField("instant", orc::createPrimitiveType(orc::TIMESTAMP_INSTANT));
    orc::TimestampVectorBatch batch(1, *orc::getDefaultPool());
    batch.numElements = 1;
    batch.notNull[0] = 1;
    batch.data[0] = 1;
    batch.nanoseconds[0] = 123;
    const std::vector<size_t> selected_rows {0};
    OrcDecodedColumnView parent {.file_type = schema.get(),
                                 .selected_type = schema.get(),
                                 .rows = 1,
                                 .timestamp_context = &context};
    auto plain_view = orc_serde_utils::make_child_orc_view(
            parent, schema->getSubtype(0), schema->getSubtype(0), &batch, 1, &selected_rows);
    auto instant_view = orc_serde_utils::make_child_orc_view(
            parent, schema->getSubtype(1), schema->getSubtype(1), &batch, 1, &selected_rows);
    ASSERT_EQ(plain_view.timestamp_context, &context);
    ASSERT_EQ(instant_view.timestamp_context, &context);

    const DataTypeTimeStampNs plain_type;
    const DataTypeDateTimeV2 instant_type(6);
    for (const std::string zone : {"America/New_York", "UTC"}) {
        ASSERT_TRUE(cctz::load_time_zone(zone, &context.reader_timezone));
        auto plain_column = plain_type.create_column();
        auto status = plain_type.get_serde()->read_column_from_orc(*plain_column, plain_view);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(
                plain_type.to_string(*plain_column, 0),
                zone == "UTC" ? "1970-01-01 00:00:01.000000123" : "1969-12-31 19:00:01.000000123");
        auto instant_column = instant_type.create_column();
        status = instant_type.get_serde()->read_column_from_orc(*instant_column, instant_view);
        ASSERT_TRUE(status.ok()) << status;
        // Changing a stripe's reader zone must never change an instant sibling's display zone.
        EXPECT_EQ(instant_type.to_string(*instant_column, 0), "1970-01-01 08:00:01.000000");
    }
}

} // namespace doris
