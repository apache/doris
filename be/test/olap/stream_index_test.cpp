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

#include <gtest/gtest.h>

#include <sstream>

#include "olap/file_stream.h"
#include "olap/olap_common.h"
#include "olap/olap_cond.h"
#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/storage_engine.h"
#include "olap/stream_index_common.h"
#include "olap/stream_index_reader.h"
#include "olap/stream_index_writer.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/wrapper_field.h"
#include "util/logging.h"

using namespace std;

namespace doris {

class TestStreamIndex : public testing::Test {
public:
    virtual ~TestStreamIndex() {}

    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(TestStreamIndex, index_write) {
    StreamIndexWriter writer(OLAP_FIELD_TYPE_INT);
    PositionEntryWriter entry;
    ColumnStatistics stat;
    stat.init(OLAP_FIELD_TYPE_INT, true);

    EXPECT_EQ(Status::OK(), stat.init(OLAP_FIELD_TYPE_INT, true));

    static const uint32_t loop = 10;
    uint32_t i = 0;
    for (; i < loop; i++) {
        entry.add_position(i);
        entry.add_position(i * 2);
        entry.add_position(i * 3);

        entry.set_statistic(&stat);
        writer.add_index_entry(entry);
        entry.reset_write_offset();
    }

    size_t output_size = sizeof(StreamIndexHeader) + i * sizeof(uint32_t) * 3;
    // for statistics
    output_size += (sizeof(int) + 1) * 2 * loop;

    EXPECT_EQ(output_size, writer.output_size());

    char* buffer = new char[output_size];

    EXPECT_EQ(Status::OK(), writer.write_to_buffer(buffer, output_size));

    StreamIndexReader reader;
    EXPECT_EQ(Status::OK(), reader.init(buffer, output_size, OLAP_FIELD_TYPE_INT, true, true));

    EXPECT_EQ(loop, reader.entry_count());

    for (i = 0; i < loop; i++) {
        const PositionEntryReader& e = reader.entry(i);
        EXPECT_EQ(e.positions(0), i);
        EXPECT_EQ(e.positions(1), i * 2);
        EXPECT_EQ(e.positions(2), i * 3);
    }
    delete[] buffer;
}

TEST_F(TestStreamIndex, remove_written_position) {
    StreamIndexWriter writer(OLAP_FIELD_TYPE_INT);
    PositionEntryWriter entry;
    ColumnStatistics stat;
    stat.init(OLAP_FIELD_TYPE_INT, true);

    static const uint32_t loop = 10;
    //test 1
    {
        uint32_t i = 0;
        for (; i < loop; i++) {
            entry.add_position(i);
            entry.add_position(i * 2);
            entry.add_position(i * 3);
            entry.add_position(i * 4);
            entry.add_position(i * 5);
            entry.add_position(i * 6);
            entry.add_position(i * 7);

            entry.set_statistic(&stat);
            writer.add_index_entry(entry);
            entry.reset_write_offset();
        }

        for (i = 0; i < loop; i++) {
            PositionEntryWriter* e = writer.mutable_entry(i);
            e->remove_written_position(0, 4);
        }

        size_t output_size = writer.output_size();
        char* buffer = new char[output_size];

        EXPECT_EQ(Status::OK(), writer.write_to_buffer(buffer, output_size));

        StreamIndexReader reader;
        EXPECT_EQ(Status::OK(), reader.init(buffer, output_size, OLAP_FIELD_TYPE_INT, true, true));

        EXPECT_EQ(loop, reader.entry_count());

        for (i = 0; i < loop; i++) {
            const PositionEntryReader& e = reader.entry(i);
            EXPECT_EQ(e.positions(0), i * 5);
            EXPECT_EQ(e.positions(1), i * 6);
            EXPECT_EQ(e.positions(2), i * 7);
        }
        delete[] buffer;
    }
    writer.reset();

    // test 2
    {
        uint32_t i = 0;
        for (; i < loop; i++) {
            entry.add_position(i);
            entry.add_position(i * 2);
            entry.add_position(i * 3);
            entry.add_position(i * 4);
            entry.add_position(i * 5);
            entry.add_position(i * 6);
            entry.add_position(i * 7);

            entry.set_statistic(&stat);
            writer.add_index_entry(entry);
            entry.reset_write_offset();
        }

        for (i = 0; i < loop; i++) {
            PositionEntryWriter* e = writer.mutable_entry(i);
            e->remove_written_position(0, 2);
        }

        size_t output_size = writer.output_size();
        char* buffer = new char[output_size];

        EXPECT_EQ(Status::OK(), writer.write_to_buffer(buffer, output_size));

        StreamIndexReader reader;
        EXPECT_EQ(Status::OK(), reader.init(buffer, output_size, OLAP_FIELD_TYPE_INT, true, true));

        EXPECT_EQ(loop, reader.entry_count());

        for (i = 0; i < loop; i++) {
            const PositionEntryReader& e = reader.entry(i);
            EXPECT_EQ(e.positions(0), i * 3);
            EXPECT_EQ(e.positions(1), i * 4);
            EXPECT_EQ(e.positions(2), i * 5);
            EXPECT_EQ(e.positions(3), i * 6);
            EXPECT_EQ(e.positions(4), i * 7);
        }
        delete[] buffer;
    }
    writer.reset();
    // test 3
    {
        uint32_t i = 0;
        for (; i < loop; i++) {
            entry.add_position(i);
            entry.add_position(i * 2);
            entry.add_position(i * 3);
            entry.add_position(i * 4);
            entry.add_position(i * 5);
            entry.add_position(i * 6);
            entry.add_position(i * 7);

            entry.set_statistic(&stat);
            writer.add_index_entry(entry);
            entry.reset_write_offset();
        }

        for (i = 0; i < loop; i++) {
            PositionEntryWriter* e = writer.mutable_entry(i);
            e->remove_written_position(3, 2);
        }

        size_t output_size = writer.output_size();
        char* buffer = new char[output_size];

        EXPECT_EQ(Status::OK(), writer.write_to_buffer(buffer, output_size));

        StreamIndexReader reader;
        EXPECT_EQ(Status::OK(), reader.init(buffer, output_size, OLAP_FIELD_TYPE_INT, true, true));

        EXPECT_EQ(loop, reader.entry_count());

        for (i = 0; i < loop; i++) {
            const PositionEntryReader& e = reader.entry(i);
            EXPECT_EQ(e.positions(0), i * 1);
            EXPECT_EQ(e.positions(1), i * 2);
            EXPECT_EQ(e.positions(2), i * 3);
            EXPECT_EQ(e.positions(3), i * 6);
            EXPECT_EQ(e.positions(4), i * 7);
        }
        delete[] buffer;
    }
    writer.reset();
    // test 4
    {
        uint32_t i = 0;
        for (; i < loop; i++) {
            entry.add_position(i);
            entry.add_position(i * 2);
            entry.add_position(i * 3);
            entry.add_position(i * 4);
            entry.add_position(i * 5);
            entry.add_position(i * 6);
            entry.add_position(i * 7);

            entry.set_statistic(&stat);
            writer.add_index_entry(entry);
            entry.reset_write_offset();
        }

        for (i = 0; i < loop; i++) {
            PositionEntryWriter* e = writer.mutable_entry(i);
            e->remove_written_position(4, 3);
        }

        size_t output_size = writer.output_size();

        char* buffer = new char[output_size];
        EXPECT_EQ(Status::OK(), writer.write_to_buffer(buffer, output_size));

        StreamIndexReader reader;
        EXPECT_EQ(Status::OK(), reader.init(buffer, output_size, OLAP_FIELD_TYPE_INT, true, true));

        EXPECT_EQ(loop, reader.entry_count());

        for (i = 0; i < loop; i++) {
            const PositionEntryReader& e = reader.entry(i);
            EXPECT_EQ(e.positions(0), i * 1);
            EXPECT_EQ(e.positions(1), i * 2);
            EXPECT_EQ(e.positions(2), i * 3);
            EXPECT_EQ(e.positions(3), i * 4);
        }
        delete[] buffer;
    }
    writer.reset();
}

TEST_F(TestStreamIndex, test_statistic) {
    ColumnStatistics stat;
    EXPECT_EQ(Status::OK(), stat.init(OLAP_FIELD_TYPE_INT, true));

    WrapperField* field = WrapperField::create_by_type(OLAP_FIELD_TYPE_INT);

    // start
    EXPECT_STREQ(stat.minimum()->to_string().c_str(), "2147483647");
    EXPECT_STREQ(stat.maximum()->to_string().c_str(), "-2147483648");

    // 1
    field->from_string("3");
    stat.add(*field);
    EXPECT_STREQ(stat.minimum()->to_string().c_str(), "3");
    EXPECT_STREQ(stat.maximum()->to_string().c_str(), "3");

    // 2
    field->from_string("5");
    stat.add(*field);
    EXPECT_STREQ(stat.minimum()->to_string().c_str(), "3");
    EXPECT_STREQ(stat.maximum()->to_string().c_str(), "5");

    // 3
    field->from_string("899");
    stat.add(*field);
    EXPECT_STREQ(stat.minimum()->to_string().c_str(), "3");
    EXPECT_STREQ(stat.maximum()->to_string().c_str(), "899");

    // 4
    field->from_string("-111");
    stat.add(*field);
    EXPECT_STREQ(stat.minimum()->to_string().c_str(), "-111");
    EXPECT_STREQ(stat.maximum()->to_string().c_str(), "899");

    stat.reset();
    // start
    EXPECT_STREQ(stat.minimum()->to_string().c_str(), "2147483647");
    EXPECT_STREQ(stat.maximum()->to_string().c_str(), "-2147483648");

    field->from_string("3");
    stat.add(*field);
    field->from_string("6");
    stat.add(*field);
    EXPECT_STREQ(stat.minimum()->to_string().c_str(), "3");
    EXPECT_STREQ(stat.maximum()->to_string().c_str(), "6");

    ColumnStatistics stat2;
    EXPECT_EQ(Status::OK(), stat2.init(OLAP_FIELD_TYPE_INT, true));

    char buf[256];
    stat.write_to_buffer(buf, sizeof(buf));
    stat2.attach(buf);

    EXPECT_STREQ(stat2.minimum()->to_string().c_str(), "3");
    EXPECT_STREQ(stat2.maximum()->to_string().c_str(), "6");
    delete field;
}

TEST_F(TestStreamIndex, statistic) {
    StreamIndexWriter writer(OLAP_FIELD_TYPE_INT);
    PositionEntryWriter entry;
    ColumnStatistics stat;

    EXPECT_EQ(Status::OK(), stat.init(OLAP_FIELD_TYPE_INT, true));

    WrapperField* field = WrapperField::create_by_type(OLAP_FIELD_TYPE_INT);
    EXPECT_TRUE(nullptr != field);
    char string_buffer[256];

    static const uint32_t loop = 10;
    uint32_t i = 0;
    for (; i < loop; i++) {
        entry.add_position(i);
        entry.add_position(i * 2);
        entry.add_position(i * 3);

        snprintf(string_buffer, sizeof(string_buffer), "%d", i * 9);
        field->from_string(string_buffer);
        stat.add(*field);

        snprintf(string_buffer, sizeof(string_buffer), "%d", i * 2);
        field->from_string(string_buffer);
        stat.add(*field);

        entry.set_statistic(&stat);

        writer.add_index_entry(entry);
        entry.reset_write_offset();
    }

    size_t output_size = sizeof(StreamIndexHeader) + loop * sizeof(uint32_t) * 3 +
                         (1 + sizeof(int32_t)) * loop * 2;
    EXPECT_EQ(output_size, writer.output_size());

    char* buffer = new char[output_size];

    EXPECT_EQ(Status::OK(), writer.write_to_buffer(buffer, output_size));

    StreamIndexReader reader;
    EXPECT_EQ(Status::OK(), reader.init(buffer, output_size, OLAP_FIELD_TYPE_INT, true, true));

    EXPECT_EQ(loop, reader.entry_count());

    for (i = 0; i < loop; i++) {
        const PositionEntryReader& e = reader.entry(i);
        EXPECT_EQ(e.positions(0), i);
        EXPECT_EQ(e.positions(1), i * 2);
        EXPECT_EQ(e.positions(2), i * 3);
    }
    delete[] buffer;
    delete field;
}

} // namespace doris
