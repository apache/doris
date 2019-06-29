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

#include "olap/short_key_index.h"

#include <gtest/gtest.h>

#include "olap/lru_cache.h"
#include "olap/file_helper.h"

namespace doris {

class ShortKeyIndexTest : public testing::Test {
public:
    ShortKeyIndexTest() { }
    virtual ~ShortKeyIndexTest() {
    }
};

static void test_encode_key(char* data, int k1, int64_t k2, int64_t k3) {
    // 0 - 4
    *(int*)(data + 1) = k1;
    *(int64_t*)(data + 2 + 4) = k2;
    *(int64_t*)(data + 3 + 4 + 8) = k3;
}

TEST_F(ShortKeyIndexTest, read_file) {
    std::vector<FieldInfo> schema;

    {
        FieldInfo field;
        field.type = OLAP_FIELD_TYPE_INT;
        field.length = 4;
        field.index_length = 4;
        field.is_allow_null = true;

        schema.push_back(field);
    }
    {
        FieldInfo field;
        field.type = OLAP_FIELD_TYPE_BIGINT;
        field.length = 8;
        field.index_length = 8;
        field.is_allow_null = true;

        schema.push_back(field);
    }
    {
        FieldInfo field;
        field.type = OLAP_FIELD_TYPE_BIGINT;
        field.length = 8;
        field.index_length = 8;
        field.is_allow_null = true;

        schema.push_back(field);
    }

    MemIndex index;
    auto olap_st = index.init(23, 23, 3, &schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);


    RowCursor key;
    key.init(schema);
    char data[64];
    key.attach(data);

    // from [10, 60)
    for (int segment = 0; segment < 5; ++segment) {
        ShortKeyIndexBuilder builder(schema, segment, 1024, false);
        auto st = builder.init();
        ASSERT_TRUE(st.ok());

        memset(data, 0, sizeof(data));
        for (int block = 0; block < 10; ++block) {
            int idx = segment * 10 + block + 10;
            test_encode_key(data, idx, 10 * idx, 100 * idx);

            st = builder.add_item(key, block);
            ASSERT_TRUE(st.ok());
        }

        std::vector<Slice> slices;
        st = builder.finalize(1024 * 1024 *100, 1024 * 1024, &slices);
        ASSERT_TRUE(st.ok());

        std::string encoded_buf;
        encoded_buf.append(slices[0].data, slices[0].size);
        encoded_buf.append(slices[1].data, slices[1].size);

        ShortKeyIndexDecoder decoder(encoded_buf);
        st = decoder.parse();
        ASSERT_TRUE(st.ok());

        auto& header = decoder.header();

        ASSERT_EQ(1024 * 1024 * 100, header.extra().data_length);
        ASSERT_EQ(1024 * 1024, header.extra().num_rows);

        ASSERT_EQ(segment, header.message().segment());
        ASSERT_EQ(1024, header.message().num_rows_per_block());
        ASSERT_EQ(true, header.message().null_supported());
        ASSERT_EQ(false, header.message().delete_flag());


        st = index.load_segment(decoder.header(), decoder.index_data());
        ASSERT_TRUE(st.ok());
    }

    ShortKeyIndexIterator iter(&index);

    // nomal seek
    {
        test_encode_key(data, 9, 0, 0);
        LOG(INFO) << "add key, key=" << key.to_string();
        iter.seek_to(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.segment());
        ASSERT_EQ(0, iter.block());

        iter.prev();
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.segment());
        ASSERT_EQ(0, iter.block());

        iter.next();
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.segment());
        ASSERT_EQ(1, iter.block());

        iter.seek_before(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.segment());
        ASSERT_EQ(0, iter.block());

        iter.seek_after(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(0, iter.segment());
        ASSERT_EQ(0, iter.block());
    }

    // nomal seek
    {
        test_encode_key(data, 35, 349, 0);
        LOG(INFO) << "add key, key=" << key.to_string();
        iter.seek_to(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(2, iter.segment());
        ASSERT_EQ(5, iter.block());

        iter.seek_before(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(2, iter.segment());
        ASSERT_EQ(4, iter.block());

        iter.seek_after(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(2, iter.segment());
        ASSERT_EQ(5, iter.block());
    }

    // nomal seek
    {
        test_encode_key(data, 35, 350, 3500);
        LOG(INFO) << "add key, key=" << key.to_string();
        iter.seek_to(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(2, iter.segment());
        ASSERT_EQ(5, iter.block());

        iter.seek_before(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(2, iter.segment());
        ASSERT_EQ(4, iter.block());

        iter.seek_after(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(2, iter.segment());
        ASSERT_EQ(6, iter.block());
    }

    // nomal seek
    {
        test_encode_key(data, 40, 390, 0);
        LOG(INFO) << "add key, key=" << key.to_string();
        iter.seek_to(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(3, iter.segment());
        ASSERT_EQ(0, iter.block());

        iter.seek_before(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(2, iter.segment());
        ASSERT_EQ(9, iter.block());

        iter.seek_after(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(3, iter.segment());
        ASSERT_EQ(0, iter.block());
    }
    // nomal seek
    {
        test_encode_key(data, 49, 490, 4900);
        LOG(INFO) << "add key, key=" << key.to_string();
        iter.seek_to(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(3, iter.segment());
        ASSERT_EQ(9, iter.block());

        iter.seek_before(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(3, iter.segment());
        ASSERT_EQ(8, iter.block());

        iter.seek_after(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(4, iter.segment());
        ASSERT_EQ(0, iter.block());
    }
    // nomal seek
    {
        test_encode_key(data, 59, 590, 5900);
        LOG(INFO) << "add key, key=" << key.to_string();
        iter.seek_to(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(4, iter.segment());
        ASSERT_EQ(9, iter.block());

        iter.seek_before(key);
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(4, iter.segment());
        ASSERT_EQ(8, iter.block());
        LOG(INFO) << "key=" << iter.to_string();

        iter.seek_after(key);
        ASSERT_FALSE(iter.valid());
    }
}

TEST_F(ShortKeyIndexTest, file_header) {
    std::vector<FieldInfo> schema;

    {
        FieldInfo field;
        field.name = "c1";
        field.type = OLAP_FIELD_TYPE_INT;
        field.length = 4;
        field.index_length = 4;
        field.is_key = true;
        field.is_allow_null = true;

        schema.push_back(field);
    }
    {
        FieldInfo field;
        field.name = "c2";
        field.type = OLAP_FIELD_TYPE_INT;
        field.length = 4;
        field.index_length = 4;
        field.is_key = true;
        field.is_allow_null = true;

        schema.push_back(field);
    }
    
    MemIndex index;
    // 4 + 1 + 4 + 1
    auto olap_st = index.init(10, 10, 2, &schema);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);

    auto cache = new_lru_cache(10);
    FileHandler::set_fd_cache(cache);
    olap_st = index.load_segment("./be/test/olap/test_data/short_key/test.idx", nullptr);
    ASSERT_EQ(OLAP_SUCCESS, olap_st);
}

}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

