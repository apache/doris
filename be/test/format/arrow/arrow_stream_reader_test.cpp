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

#include "format/arrow/arrow_stream_reader.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_nullable.h"
#include "io/fs/stream_load_pipe.h"
#include "load/stream_load/new_load_stream_mgr.h"
#include "load/stream_load/stream_load_context.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/timezone_utils.h"

namespace doris {

class ArrowStreamReaderTest : public testing::Test {
protected:
    static void SetUpTestSuite() { TimezoneUtils::load_timezones_to_cache(); }

    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        _owns_load_stream_mgr = exec_env->new_load_stream_mgr() == nullptr;
        if (_owns_load_stream_mgr) {
            exec_env->set_new_load_stream_mgr(NewLoadStreamMgr::create_unique());
        }
    }

    void TearDown() override {
        if (_owns_load_stream_mgr) {
            ExecEnv::GetInstance()->clear_new_load_stream_mgr();
        }
    }

    // 2026-10-13 16:37:00 UTC, and the same calendar day as epoch days / milliseconds.
    static constexpr int64_t kInstantMicros = 1791909420000000;
    static constexpr int32_t kDays = 20739;

    static std::shared_ptr<arrow::Array> timestamp_array(const std::string& arrow_timezone) {
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO, arrow_timezone),
                                        arrow::default_memory_pool());
        EXPECT_TRUE(builder.Append(kInstantMicros).ok());
        return builder.Finish().ValueOrDie();
    }

    static std::shared_ptr<arrow::Array> date32_array() {
        arrow::Date32Builder builder;
        EXPECT_TRUE(builder.Append(kDays).ok());
        return builder.Finish().ValueOrDie();
    }

    static std::shared_ptr<arrow::Array> date64_array() {
        arrow::Date64Builder builder;
        EXPECT_TRUE(builder.Append(int64_t {kDays} * 24 * 60 * 60 * 1000).ok());
        return builder.Finish().ValueOrDie();
    }

    // Sends `array` as a one-column Arrow IPC stream through ArrowStreamReader in a load whose
    // time zone is `load_timezone`, into a nullable column of `type`, and returns the stored value.
    static std::string read_one(const std::string& load_timezone,
                                const std::shared_ptr<arrow::Array>& array, DataTypePtr type) {
        auto schema = arrow::schema({arrow::field("c", array->type())});
        auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
        auto writer = arrow::ipc::MakeStreamWriter(sink, schema).ValueOrDie();
        EXPECT_TRUE(writer->WriteRecordBatch(*arrow::RecordBatch::Make(schema, 1, {array})).ok());
        EXPECT_TRUE(writer->Close().ok());
        auto body = sink->Finish().ValueOrDie();

        auto ctx = std::make_shared<StreamLoadContext>(ExecEnv::GetInstance());
        ctx->pipe = std::make_shared<io::StreamLoadPipe>(io::kMaxPipeBufferedBytes, 64 * 1024,
                                                         body->size());
        EXPECT_TRUE(ctx->pipe
                            ->append(reinterpret_cast<const char*>(body->data()),
                                     static_cast<size_t>(body->size()))
                            .ok());
        EXPECT_TRUE(ctx->pipe->finish().ok());
        auto* load_stream_mgr = ExecEnv::GetInstance()->new_load_stream_mgr();
        EXPECT_TRUE(load_stream_mgr->put(ctx->id, ctx).ok());
        Defer remove_ctx {[&]() { load_stream_mgr->remove(ctx->id); }};

        RuntimeState state;
        state.set_timezone(load_timezone);
        TFileScanRangeParams params;
        TFileRangeDesc range;
        range.__set_load_id(ctx->id.to_thrift());
        std::vector<SlotDescriptor*> slots;
        ArrowStreamReader reader(&state, nullptr, nullptr, params, range, slots, nullptr);
        EXPECT_TRUE(reader.init_reader().ok());

        Block block;
        auto nullable_type = make_nullable(type);
        block.insert({nullable_type->create_column(), nullable_type, "c"});
        size_t read_rows = 0;
        bool eof = false;
        Status st = reader.get_next_block(&block, &read_rows, &eof);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(1, read_rows);
        const auto& column = *block.get_by_position(0).column;
        EXPECT_FALSE(column.is_null_at(0));
        return nullable_type->to_string(column, 0);
    }

    static std::string read_datetimev2(const std::string& load_timezone,
                                       const std::string& arrow_timezone) {
        return read_one(load_timezone, timestamp_array(arrow_timezone),
                        std::make_shared<DataTypeDateTimeV2>(6));
    }

private:
    bool _owns_load_stream_mgr = false;
};

// A time-zone-aware Arrow timestamp is an instant; it is stored as wall-clock time in the load's
// time zone (the Stream Load `timezone` header), not in a hard-coded default zone. The Arrow
// field's own zone name only marks the value as an instant.
TEST_F(ArrowStreamReaderTest, ZonedTimestampUsesLoadTimezone) {
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_datetimev2("UTC", "UTC"));
    EXPECT_EQ("2026-10-14 00:37:00.000000", read_datetimev2("Asia/Shanghai", "UTC"));
    EXPECT_EQ("2026-10-13 09:37:00.000000", read_datetimev2("America/Los_Angeles", "UTC"));
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_datetimev2("UTC", "Asia/Tokyo"));
}

// A naive Arrow timestamp already holds wall-clock fields; the load time zone must not shift it.
TEST_F(ArrowStreamReaderTest, NaiveTimestampKeepsWallClock) {
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_datetimev2("UTC", ""));
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_datetimev2("Asia/Shanghai", ""));
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_datetimev2("America/Los_Angeles", ""));
}

// Legacy DATETIME columns follow the same rules as DATETIMEV2.
TEST_F(ArrowStreamReaderTest, LegacyDatetimeColumn) {
    auto datetime = std::make_shared<DataTypeDateTime>();
    EXPECT_EQ("2026-10-13 16:37:00", read_one("UTC", timestamp_array("UTC"), datetime));
    EXPECT_EQ("2026-10-14 00:37:00", read_one("Asia/Shanghai", timestamp_array("UTC"), datetime));
    EXPECT_EQ("2026-10-13 09:37:00",
              read_one("America/Los_Angeles", timestamp_array("UTC"), datetime));
    EXPECT_EQ("2026-10-13 16:37:00", read_one("Asia/Shanghai", timestamp_array(""), datetime));
    EXPECT_EQ("2026-10-13 16:37:00",
              read_one("America/Los_Angeles", timestamp_array(""), datetime));
    EXPECT_EQ("2026-10-13 00:00:00", read_one("Asia/Shanghai", date64_array(), datetime));
}

// Arrow DATE32/DATE64 are calendar days; a load time zone west of UTC must not move them to the
// previous day, for legacy DATE as for DATEV2.
TEST_F(ArrowStreamReaderTest, DateKeepsCalendarDay) {
    for (const auto& load_timezone : {"UTC", "Asia/Shanghai", "America/Los_Angeles"}) {
        for (const auto& array : {date32_array(), date64_array()}) {
            EXPECT_EQ("2026-10-13",
                      read_one(load_timezone, array, std::make_shared<DataTypeDate>()))
                    << load_timezone << " " << array->type()->ToString();
            EXPECT_EQ("2026-10-13",
                      read_one(load_timezone, array, std::make_shared<DataTypeDateV2>()))
                    << load_timezone << " " << array->type()->ToString();
        }
    }
}

// A TIMESTAMP loaded into a legacy DATE column keeps only the date, in the same zone rules.
TEST_F(ArrowStreamReaderTest, LegacyDateFromTimestamp) {
    auto date = std::make_shared<DataTypeDate>();
    EXPECT_EQ("2026-10-13", read_one("UTC", timestamp_array("UTC"), date));
    EXPECT_EQ("2026-10-14", read_one("Asia/Shanghai", timestamp_array("UTC"), date));
    EXPECT_EQ("2026-10-13", read_one("Asia/Shanghai", timestamp_array(""), date));
}

} // namespace doris
