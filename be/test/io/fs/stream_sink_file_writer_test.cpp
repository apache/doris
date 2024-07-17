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

#include "io/fs/stream_sink_file_writer.h"

#include <brpc/channel.h>
#include <brpc/server.h>

#include "gtest/gtest_pred_impl.h"
#include "olap/olap_common.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/faststring.h"
#include "vec/sink/load_stream_stub.h"

namespace doris {

#ifndef CHECK_STATUS_OK
#define CHECK_STATUS_OK(stmt)                   \
    do {                                        \
        Status _status_ = (stmt);               \
        ASSERT_TRUE(_status_.ok()) << _status_; \
    } while (false)
#endif

constexpr int64_t LOAD_ID_LO = 1;
constexpr int64_t LOAD_ID_HI = 2;
constexpr int64_t NUM_STREAM = 3;
constexpr int64_t PARTITION_ID = 1234;
constexpr int64_t INDEX_ID = 2345;
constexpr int64_t TABLET_ID = 3456;
constexpr int32_t SEGMENT_ID = 4567;
const std::string DATA0 = "segment data";
const std::string DATA1 = "hello world";

static std::atomic<int64_t> g_num_request;

class StreamSinkFileWriterTest : public testing::Test {
    class MockStreamStub : public LoadStreamStub {
    public:
        MockStreamStub(PUniqueId load_id, int64_t src_id)
                : LoadStreamStub(load_id, src_id, std::make_shared<IndexToTabletSchema>(),
                                 std::make_shared<IndexToEnableMoW>()) {};

        virtual ~MockStreamStub() = default;

        // APPEND_DATA
        virtual Status append_data(int64_t partition_id, int64_t index_id, int64_t tablet_id,
                                   int64_t segment_id, uint64_t offset, std::span<const Slice> data,
                                   bool segment_eos = false,
                                   FileType file_type = FileType::SEGMENT_FILE) override {
            EXPECT_EQ(PARTITION_ID, partition_id);
            EXPECT_EQ(INDEX_ID, index_id);
            EXPECT_EQ(TABLET_ID, tablet_id);
            EXPECT_EQ(SEGMENT_ID, segment_id);
            if (segment_eos) {
                EXPECT_EQ(0, data.size());
                EXPECT_EQ(DATA0.length() + DATA1.length(), offset);
            } else {
                EXPECT_EQ(2, data.size());
                EXPECT_EQ(DATA0, data[0].to_string());
                EXPECT_EQ(DATA1, data[1].to_string());
                EXPECT_EQ(0, offset);
            }
            g_num_request++;
            return Status::OK();
        }
    };

public:
    StreamSinkFileWriterTest() = default;
    ~StreamSinkFileWriterTest() = default;

protected:
    virtual void SetUp() {
        _load_id.set_hi(LOAD_ID_HI);
        _load_id.set_lo(LOAD_ID_LO);
        for (int src_id = 0; src_id < NUM_STREAM; src_id++) {
            _streams.emplace_back(new MockStreamStub(_load_id, src_id));
        }
    }

    virtual void TearDown() {}

    PUniqueId _load_id;
    std::vector<std::shared_ptr<LoadStreamStub>> _streams;
};

TEST_F(StreamSinkFileWriterTest, Test) {
    g_num_request = 0;
    io::StreamSinkFileWriter writer(_streams);
    writer.init(_load_id, PARTITION_ID, INDEX_ID, TABLET_ID, SEGMENT_ID);
    std::vector<Slice> slices {DATA0, DATA1};

    CHECK_STATUS_OK(writer.appendv(&(*slices.begin()), slices.size()));
    EXPECT_EQ(NUM_STREAM, g_num_request);
    CHECK_STATUS_OK(writer.close());
    EXPECT_EQ(NUM_STREAM * 2, g_num_request);
}

} // namespace doris
