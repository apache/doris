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

#include "vec/data_types/data_type_varbinary.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "gen_cpp/types.pb.h"
#include "testutil/test_util.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_varbinary.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/common/string_view.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

class DataTypeVarbinaryTest : public ::testing::Test {
protected:
    static std::string make_bytes(size_t n, uint8_t seed = 0x33) {
        std::string s;
        s.resize(n);
        for (size_t i = 0; i < n; ++i) {
            s[i] = static_cast<char>(seed + i);
        }
        if (n >= 3) {
            s[n / 3] = '\0';
            s[(2 * n) / 3] = '\0';
        }
        return s;
    }
};

TEST_F(DataTypeVarbinaryTest, MetaInfoAndEquals) {
    DataTypeVarbinary dt1(16);
    DataTypeVarbinary dt2(32);

    EXPECT_EQ(dt1.get_family_name(), std::string("VarBinary"));
    EXPECT_EQ(dt1.get_primitive_type(), PrimitiveType::TYPE_VARBINARY);
    EXPECT_EQ(dt1.len(), 16);

    EXPECT_TRUE(dt1.equals(dt2));
    DataTypeString dts;
    EXPECT_FALSE(dt1.equals(dts));
}

TEST_F(DataTypeVarbinaryTest, CreateColumnAndCheckColumn) {
    DataTypeVarbinary dt;

    auto col = dt.create_column();
    ASSERT_TRUE(col.get() != nullptr);
    ASSERT_EQ(col->size(), 0U);
    ASSERT_NO_FATAL_FAILURE({ (void)assert_cast<ColumnVarbinary&>(*col); });
    EXPECT_TRUE(dt.check_column(*col).ok());
    auto wrong = ColumnString::create();
    EXPECT_FALSE(dt.check_column(*wrong).ok());
}

TEST_F(DataTypeVarbinaryTest, GetDefaultField) {
    DataTypeVarbinary dt;
    Field def = dt.get_default();
    const auto& sv = get<const doris::StringView&>(def);
    EXPECT_EQ(sv.size(), 0U);
}

TEST_F(DataTypeVarbinaryTest, ToStringAndToStringBufferWritable) {
    DataTypeVarbinary dt;
    auto col = dt.create_column();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());

    std::vector<std::string> vals = {make_bytes(0), make_bytes(3), std::string("ABC", 3)};
    for (auto& v : vals) {
        vb->insert_data(v.data(), v.size());
    }

    for (size_t i = 0; i < vals.size(); ++i) {
        auto s = dt.to_string(*col, i);
        ASSERT_EQ(s.size(), vals[i].size());
        ASSERT_EQ(memcmp(s.data(), vals[i].data(), s.size()), 0);
    }

    auto out_col = ColumnString::create();
    for (size_t i = 0; i < vals.size(); ++i) {
        BufferWritable bw(*out_col);
        dt.to_string(*col, i, bw);
        bw.commit();
    }
    ASSERT_EQ(out_col->size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        auto r = out_col->get_data_at(i);
        ASSERT_EQ(r.size, vals[i].size());
        ASSERT_EQ(memcmp(r.data, vals[i].data(), r.size), 0);
    }
}

TEST_F(DataTypeVarbinaryTest, SerializeDeserializeAndSize) {
    DataTypeVarbinary dt;
    auto col = dt.create_column();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());

    std::vector<std::string> vals = {make_bytes(1, 0x11), make_bytes(2, 0x22), make_bytes(7, 0x33),
                                     make_bytes(0, 0x00)};
    for (auto& v : vals) {
        vb->insert_data(v.data(), v.size());
    }

    int ver = BeExecVersionManager::get_newest_version();

    // 计算未压缩大小：bool + size_t + size_t + sizes[] + payload
    size_t expected = sizeof(bool) + sizeof(size_t) + sizeof(size_t) + sizeof(size_t) * vals.size();
    size_t payload = 0;
    for (auto& v : vals) {
        payload += v.size();
    }
    expected += payload;

    auto sz = dt.get_uncompressed_serialized_bytes(*col, ver);
    EXPECT_EQ(static_cast<size_t>(sz), expected);

    std::string buf;
    buf.resize(expected);
    char* p = buf.data();
    char* end = dt.serialize(*col, p, ver);
    ASSERT_EQ(static_cast<size_t>(end - p), expected);

    MutableColumnPtr deser = dt.create_column();
    const char* p2 = buf.data();
    const char* end2 = dt.deserialize(p2, &deser, ver);
    ASSERT_EQ(static_cast<size_t>(end2 - p2), expected);

    auto* vb2 = assert_cast<ColumnVarbinary*>(deser.get());
    ASSERT_EQ(vb2->size(), vals.size());
    for (size_t i = 0; i < vals.size(); ++i) {
        auto r = vb2->get_data_at(i);
        ASSERT_EQ(r.size, vals[i].size());
        ASSERT_EQ(memcmp(r.data, vals[i].data(), r.size), 0);
    }
}

TEST_F(DataTypeVarbinaryTest, GetFieldWithDataType) {
    DataTypeVarbinary dt;
    auto col = dt.create_column();
    auto* vb = assert_cast<ColumnVarbinary*>(col.get());

    std::string v = make_bytes(5, 0x44);
    vb->insert_data(v.data(), v.size());

    auto fwd = dt.get_field_with_data_type(*col, 0);
    EXPECT_EQ(fwd.base_scalar_type_id, PrimitiveType::TYPE_VARBINARY);
    const auto& sv = get<const doris::StringView&>(fwd.field);
    ASSERT_EQ(sv.size(), v.size());
    ASSERT_EQ(memcmp(sv.data(), v.data(), sv.size()), 0);
}

TEST_F(DataTypeVarbinaryTest, GetFieldFromTExprNode) {
    DataTypeVarbinary dt;
    TExprNode node;
    node.node_type = TExprNodeType::VARBINARY_LITERAL;
    node.varbinary_literal.value = std::string("hello", 5);
    node.__isset.varbinary_literal = true;

    Field f = dt.get_field(node);
    const auto& sv = get<const doris::StringView&>(f);
    ASSERT_EQ(sv.size(), 5U);
    ASSERT_EQ(memcmp(sv.data(), "hello", 5), 0);
}

TEST_F(DataTypeVarbinaryTest, ToProtobufLen) {
    DataTypeVarbinary dt(123);
    PTypeDesc ptype;
    PTypeNode pnode;
    PScalarType scalar;
    dt.to_protobuf(&ptype, &pnode, &scalar);
    EXPECT_EQ(scalar.len(), 123);
}

} // namespace doris::vectorized