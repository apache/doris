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

#include "olap/memory/column.h"

#include <gtest/gtest.h>

#include "olap/memory/column_reader.h"
#include "olap/memory/column_writer.h"
#include "test_util/test_util.h"

namespace doris {
namespace memory {

static const size_t InsertCount = LOOP_LESS_OR_MORE(1000, 1000000);
static const size_t UpdateTime = 70;
static const size_t UpdateCount = LOOP_LESS_OR_MORE(10, 10000);

template <class CppType, ColumnType CT>
struct ColumnTest {
    static bool is_null(CppType v) { return ((int64_t)v) % 10 == 0; }

    static void test_not_null() {
        ColumnSchema cs(1, "col", CT, false, false);
        scoped_refptr<Column> c(new Column(cs, CT, 1));
        std::unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->create_writer(&writer).ok());
        std::vector<CppType> values(InsertCount, 0);
        for (size_t i = 0; i < values.size(); i++) {
            values[i] = (CppType)rand();
            EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]).ok());
        }
        scoped_refptr<Column> newc;
        ASSERT_TRUE(writer->finalize(2).ok());
        ASSERT_TRUE(writer->get_new_column(&newc).ok());
        // The less `InsertCount` won't make COW performed，
        // expect the new column object only when inserting more。
        if (AllowSlowTests()) {
            EXPECT_TRUE(c.get() != newc.get());
        }
        std::unique_ptr<ColumnReader> readc;
        ASSERT_TRUE(newc->create_reader(2, &readc).ok());
        for (uint32_t i = 0; i < values.size(); i++) {
            CppType value = *reinterpret_cast<const CppType*>(readc->get(i));
            EXPECT_EQ(value, values[i]);
        }
    }

    static void test_nullable() {
        ColumnSchema cs(1, "col", CT, true, false);
        scoped_refptr<Column> c(new Column(cs, CT, 1));
        std::unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->create_writer(&writer).ok());
        std::vector<CppType> values(InsertCount, 0);
        for (size_t i = 0; i < values.size(); i++) {
            values[i] = (CppType)rand();
            if (is_null(values[i])) {
                // set to null
                EXPECT_TRUE(writer->insert((uint32_t)i, nullptr).ok());
            } else {
                EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]).ok());
            }
        }
        scoped_refptr<Column> newc;
        ASSERT_TRUE(writer->finalize(2).ok());
        ASSERT_TRUE(writer->get_new_column(&newc).ok());
        if (AllowSlowTests()) {
            EXPECT_TRUE(c.get() != newc.get());
        }
        std::unique_ptr<ColumnReader> readc;
        ASSERT_TRUE(newc->create_reader(2, &readc).ok());
        for (uint32_t i = 0; i < values.size(); i++) {
            if (is_null(values[i])) {
                EXPECT_TRUE(readc->get(i) == nullptr);
            } else {
                CppType value = *reinterpret_cast<const CppType*>(readc->get(i));
                EXPECT_EQ(value, values[i]);
            }
        }
    }

    static void test_not_null_update() {
        srand(1);
        uint64_t version = 1;
        // insert
        ColumnSchema cs(1, "col", CT, false, false);
        scoped_refptr<Column> c(new Column(cs, CT, 1));
        std::unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->create_writer(&writer).ok());
        std::vector<CppType> values(InsertCount, 0);
        for (size_t i = 0; i < values.size(); i++) {
            values[i] = (CppType)rand();
            EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]).ok());
        }
        ASSERT_TRUE(writer->finalize(++version).ok());
        ASSERT_TRUE(writer->get_new_column(&c).ok());
        writer.reset();
        scoped_refptr<Column> oldc = c;
        for (size_t u = 0; u < UpdateTime; u++) {
            ASSERT_TRUE(c->create_writer(&writer).ok());
            std::vector<uint32_t> update_idxs;
            for (size_t i = 0; i < UpdateCount; i++) {
                uint32_t idx = rand() % values.size();
                //CppType oldv = values[idx];
                values[idx] = (CppType)rand();
                EXPECT_TRUE(writer->update(idx, &values[idx]).ok());
                update_idxs.push_back(idx);
            }
            ASSERT_TRUE(writer->finalize(++version).ok());
            ASSERT_TRUE(writer->get_new_column(&c).ok());
            //DLOG(INFO) << Format("update %zu writer: %s", u, writer->to_string().c_str());
            writer.reset();
            std::unique_ptr<ColumnReader> readc;
            ASSERT_TRUE(c->create_reader(version, &readc).ok());
            //DLOG(INFO) << Format("read %zu reader: %s", u, readc->to_string().c_str());
            for (uint32_t i : update_idxs) {
                CppType value = *reinterpret_cast<const CppType*>(readc->get(i));
                EXPECT_EQ(value, values[i]) << StringPrintf("values[%u]", i);
            }
        }
        if (UpdateTime > 64) {
            ASSERT_TRUE(oldc != c);
        }
    }

    static void test_nullable_update() {
        srand(1);
        uint64_t version = 1;
        // insert
        ColumnSchema cs(1, "col", CT, false, false);
        scoped_refptr<Column> c(new Column(cs, CT, 1));
        std::unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->create_writer(&writer).ok());
        std::vector<CppType> values(InsertCount, 0);
        for (size_t i = 0; i < values.size(); i++) {
            values[i] = (CppType)rand();
            if (is_null(values[i])) {
                // set to null
                EXPECT_TRUE(writer->insert((uint32_t)i, nullptr).ok());
            } else {
                EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]).ok());
            }
        }
        ASSERT_TRUE(writer->finalize(++version).ok());
        ASSERT_TRUE(writer->get_new_column(&c).ok());
        writer.reset();
        scoped_refptr<Column> oldc = c;
        for (size_t u = 0; u < UpdateTime; u++) {
            ASSERT_TRUE(c->create_writer(&writer).ok());
            std::vector<uint32_t> update_idxs;
            for (size_t i = 0; i < UpdateCount; i++) {
                uint32_t idx = rand() % values.size();
                //CppType oldv = values[idx];
                values[idx] = (CppType)rand();
                if (is_null(values[idx])) {
                    EXPECT_TRUE(writer->update(idx, nullptr).ok());
                } else {
                    EXPECT_TRUE(writer->update(idx, &values[idx]).ok());
                }
                update_idxs.push_back(idx);
            }
            ASSERT_TRUE(writer->finalize(++version).ok());
            ASSERT_TRUE(writer->get_new_column(&c).ok());
            //DLOG(INFO) << Format("update %zu writer: %s", u, writer->to_string().c_str());
            writer.reset();
            std::unique_ptr<ColumnReader> readc;
            ASSERT_TRUE(c->create_reader(version, &readc).ok());
            //DLOG(INFO) << Format("read %zu reader: %s", u, readc->to_string().c_str());
            for (uint32_t i : update_idxs) {
                CppType value = *reinterpret_cast<const CppType*>(readc->get(i));
                if (is_null(values[i])) {
                    EXPECT_TRUE(readc->get(i) == nullptr);
                } else {
                    CppType value = *reinterpret_cast<const CppType*>(readc->get(i));
                    EXPECT_EQ(value, values[i]) << StringPrintf("values[%u]", i);
                }
            }
        }
        if (UpdateTime > 64) {
            ASSERT_TRUE(oldc != c);
        }
    }

    static void test() {
        test_not_null();
        test_nullable();
    }

    static void test_update() {
        test_not_null_update();
        test_nullable_update();
    }
};

TEST(Column, insert) {
    ColumnTest<int16_t, ColumnType::OLAP_FIELD_TYPE_SMALLINT>::test();
    ColumnTest<int64_t, ColumnType::OLAP_FIELD_TYPE_BIGINT>::test();
    ColumnTest<float, ColumnType::OLAP_FIELD_TYPE_FLOAT>::test();
}

TEST(Column, update) {
    ColumnTest<int8_t, ColumnType::OLAP_FIELD_TYPE_TINYINT>::test();
    ColumnTest<int32_t, ColumnType::OLAP_FIELD_TYPE_INT>::test();
    ColumnTest<int128_t, ColumnType::OLAP_FIELD_TYPE_LARGEINT>::test();
    ColumnTest<double, ColumnType::OLAP_FIELD_TYPE_DOUBLE>::test();
}

} // namespace memory
} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
