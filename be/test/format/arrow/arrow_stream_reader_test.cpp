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
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
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

    // Reads one Arrow IPC stream holding ts = 2026-10-13 16:37:00 UTC as a timestamp[us] with
    // the given Arrow time zone ("" for a naive timestamp), in a load whose time zone is
    // `load_timezone`, and returns the DATETIMEV2(6) value the reader produced.
    static std::string read_one_timestamp(const std::string& load_timezone,
                                          const std::string& arrow_timezone) {
        auto ts_arrow_type = arrow::timestamp(arrow::TimeUnit::MICRO, arrow_timezone);
        auto schema = arrow::schema({arrow::field("ts", ts_arrow_type)});
        arrow::TimestampBuilder builder(ts_arrow_type, arrow::default_memory_pool());
        EXPECT_TRUE(builder.Append(1791909420000000).ok());
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(builder.Finish(&array).ok());
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
        auto ts_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
        block.insert({ts_type->create_column(), ts_type, "ts"});
        size_t read_rows = 0;
        bool eof = false;
        Status st = reader.get_next_block(&block, &read_rows, &eof);
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(1, read_rows);
        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        EXPECT_FALSE(nullable.is_null_at(0));
        const auto& values = assert_cast<const ColumnDateTimeV2&>(nullable.get_nested_column());
        char buf[64];
        values.get_element(0).to_string(buf, 6);
        return std::string(buf);
    }

private:
    bool _owns_load_stream_mgr = false;
};

// A time-zone-aware Arrow timestamp is an instant; it is stored as wall-clock time in the load's
// time zone (the Stream Load `timezone` header), not in a hard-coded default zone.
TEST_F(ArrowStreamReaderTest, ZonedTimestampUsesLoadTimezone) {
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_one_timestamp("UTC", "UTC"));
    EXPECT_EQ("2026-10-14 00:37:00.000000", read_one_timestamp("Asia/Shanghai", "UTC"));
    EXPECT_EQ("2026-10-13 09:37:00.000000", read_one_timestamp("America/Los_Angeles", "UTC"));
}

// A naive Arrow timestamp already holds wall-clock fields; the load time zone must not shift it.
TEST_F(ArrowStreamReaderTest, NaiveTimestampKeepsWallClock) {
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_one_timestamp("UTC", ""));
    EXPECT_EQ("2026-10-13 16:37:00.000000", read_one_timestamp("Asia/Shanghai", ""));
}

} // namespace doris
