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

#include "vec/json/path_in_data.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "gen_cpp/segment_v2.pb.h"

namespace doris::vectorized {

class PathInDataTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(PathInDataTest, DefaultConstructorIsTyped) {
    {
        PathInData path;
        EXPECT_FALSE(path.get_is_typed());
    }

    {
        PathInData path("a.b.c");
        EXPECT_FALSE(path.get_is_typed());
        EXPECT_EQ(path.get_path(), "a.b.c");
        EXPECT_EQ(path.get_parts().size(), 3);
    }

    {
        PathInData path("a.b.c", false);
        EXPECT_FALSE(path.get_is_typed());
        EXPECT_EQ(path.get_path(), "a.b.c");
    }

    {
        PathInData path("a.b.c", true);
        EXPECT_TRUE(path.get_is_typed());
        EXPECT_EQ(path.get_path(), "a.b.c");
    }

    PathInData::Parts parts;
    parts.emplace_back("a", false, 0);
    parts.emplace_back("b", false, 0);
    parts.emplace_back("c", false, 0);

    {
        PathInData path(parts);
        EXPECT_FALSE(path.get_is_typed());
        EXPECT_EQ(path.get_path(), "a.b.c");
        EXPECT_EQ(path.get_parts().size(), 3);
    }

    std::vector<std::string> paths = {"a", "b", "c"};

    {
        PathInData path(paths);
        EXPECT_FALSE(path.get_is_typed());
        EXPECT_EQ(path.get_path(), "a.b.c");
        EXPECT_EQ(path.get_parts().size(), 3);
    }

    {
        PathInData path("a.b.c", parts, true);
        EXPECT_TRUE(path.get_is_typed());
        EXPECT_EQ(path.get_path(), "a.b.c");
    }

    {
        std::string root = "root";
        PathInData path(root, paths);
        EXPECT_FALSE(path.get_is_typed());
        EXPECT_EQ(path.get_path(), "root.a.b.c");
        EXPECT_EQ(path.get_parts().size(), 4);
    }
}

TEST_F(PathInDataTest, CopyConstructorIsTyped) {
    {
        PathInData original("a.b.c", false);
        PathInData copy(original);
        EXPECT_FALSE(copy.get_is_typed());
        EXPECT_EQ(copy.get_path(), original.get_path());
        EXPECT_EQ(copy.get_parts().size(), original.get_parts().size());
    }

    {
        PathInData original("a.b.c", true);
        PathInData copy(original);
        EXPECT_TRUE(copy.get_is_typed());
        EXPECT_EQ(copy.get_path(), original.get_path());
        EXPECT_EQ(copy.get_parts().size(), original.get_parts().size());
    }
}

TEST_F(PathInDataTest, AssignmentOperatorIsTyped) {
    {
        PathInData source("a.b.c", true);
        PathInData target("x.y.z", false);

        target = source;
        EXPECT_TRUE(target.get_is_typed());
        EXPECT_EQ(target.get_path(), source.get_path());
        EXPECT_EQ(target.get_parts().size(), source.get_parts().size());
    }

    {
        PathInData source("a.b.c", false);
        PathInData target("x.y.z", true);

        target = source;
        EXPECT_FALSE(target.get_is_typed());
        EXPECT_EQ(target.get_path(), source.get_path());
        EXPECT_EQ(target.get_parts().size(), source.get_parts().size());
    }
}

TEST_F(PathInDataTest, CopyPopFrontIsTyped) {
    PathInData original("a.b.c.d", true);
    PathInData result = original.copy_pop_front();

    EXPECT_TRUE(result.get_is_typed());
    EXPECT_EQ(result.get_path(), "b.c.d");
    EXPECT_EQ(result.get_parts().size(), 3);
}

TEST_F(PathInDataTest, CopyPopBackIsTyped) {
    PathInData original("a.b.c.d", true);
    PathInData result = original.copy_pop_back();

    EXPECT_TRUE(result.get_is_typed());
    EXPECT_EQ(result.get_path(), "a.b.c");
    EXPECT_EQ(result.get_parts().size(), 3);

    PathInData single("a", true);
    PathInData empty_result = single.copy_pop_back();
    EXPECT_TRUE(empty_result.empty());
}

TEST_F(PathInDataTest, CopyPopNFrontIsTyped) {
    PathInData original("a.b.c.d.e", true);

    PathInData result = original.copy_pop_nfront(2);
    EXPECT_TRUE(result.get_is_typed());
    EXPECT_EQ(result.get_path(), "c.d.e");
    EXPECT_EQ(result.get_parts().size(), 3);

    PathInData empty_result = original.copy_pop_nfront(5);
    EXPECT_TRUE(empty_result.empty());
}

TEST_F(PathInDataTest, GetNestedPrefixPathIsTyped) {
    PathInDataBuilder builder;
    builder.append("a", false);
    builder.append("b", true);
    builder.append("c", false);

    PathInData::Parts parts = builder.get_parts();
    PathInData original("a.b.c", parts, true);

    EXPECT_TRUE(original.has_nested_part());

    PathInData prefix = original.get_nested_prefix_path();
    EXPECT_TRUE(prefix.get_is_typed());
    EXPECT_EQ(prefix.get_parts().size(), 1);
}

TEST_F(PathInDataTest, ProtobufSerializationIsTyped) {
    {
        PathInData original("a.b.c", true);

        segment_v2::ColumnPathInfo pb;
        original.to_protobuf(&pb, 123);

        EXPECT_TRUE(pb.is_typed());
        EXPECT_EQ(pb.path(), "a.b.c");
        EXPECT_EQ(pb.parrent_column_unique_id(), 123);

        PathInData deserialized;
        deserialized.from_protobuf(pb);

        EXPECT_TRUE(deserialized.get_is_typed());
        EXPECT_EQ(deserialized.get_path(), original.get_path());
        EXPECT_EQ(deserialized.get_parts().size(), original.get_parts().size());
    }

    {
        PathInData original("a.b.c", false);

        segment_v2::ColumnPathInfo pb;
        original.to_protobuf(&pb, 456);

        EXPECT_FALSE(pb.is_typed());
        EXPECT_EQ(pb.path(), "a.b.c");
        EXPECT_EQ(pb.parrent_column_unique_id(), 456);

        PathInData deserialized;
        deserialized.from_protobuf(pb);

        EXPECT_FALSE(deserialized.get_is_typed());
        EXPECT_EQ(deserialized.get_path(), original.get_path());
        EXPECT_EQ(deserialized.get_parts().size(), original.get_parts().size());
    }
}

TEST_F(PathInDataTest, PathInDataBuilderIsTyped) {
    PathInDataBuilder builder;
    builder.append("a", false);
    builder.append("b", true);
    builder.append("c", false);

    PathInData path = builder.build();
    EXPECT_FALSE(path.get_is_typed());

    PathInData typed_path("a.b.c", builder.get_parts(), true);
    EXPECT_TRUE(typed_path.get_is_typed());
}

TEST_F(PathInDataTest, ComparisonWithIsTyped) {
    PathInData path1("a.b.c", true);
    PathInData path2("a.b.c", false);
    PathInData path3("a.b.c", true);

    EXPECT_TRUE(path1 != path2);
    EXPECT_TRUE(path1 == path3);

    PathInData::Hash hasher;
    EXPECT_NE(hasher(path1), hasher(path2));
    EXPECT_EQ(hasher(path1), hasher(path3));
}

} // namespace doris::vectorized
