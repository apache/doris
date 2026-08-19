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

#include "exec/rowid_fetcher.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"

namespace doris {

// source_column_key() decides which projected slots collapse onto a single scanned
// column. Two slots that name the same physical column must produce the same key, and
// any two slots that do not must produce different ones -- a false merge drops a column
// the result block still expects, which is the crash this keying was introduced to fix.
class RowIdStorageReaderTest : public testing::Test {
public:
    static std::string key_of(const SlotDescriptor& slot, uint32_t column_idx) {
        return RowIdStorageReader::source_column_key(slot, column_idx);
    }

protected:
    struct SlotSpec {
        std::string col_name = "c";
        int32_t col_unique_id = 1;
        std::vector<std::string> column_paths = {};
        TColumnAccessPaths access_paths = {};
    };

    static SlotDescriptor make_slot(const SlotSpec& spec) {
        TSlotDescriptor tdesc = TSlotDescriptorBuilder()
                                        .type(TYPE_INT)
                                        .nullable(true)
                                        .column_name(spec.col_name)
                                        .column_pos(0)
                                        .build();
        tdesc.__set_col_unique_id(spec.col_unique_id);
        tdesc.__set_column_paths(spec.column_paths);
        if (!spec.access_paths.empty()) {
            tdesc.__set_all_access_paths(spec.access_paths);
        }
        return SlotDescriptor(tdesc);
    }

    static TColumnAccessPath data_path(const std::vector<std::string>& path) {
        TColumnAccessPath access_path;
        access_path.type = TAccessPathType::DATA;
        TDataAccessPath data;
        data.__set_path(path);
        access_path.__set_data_access_path(data);
        return access_path;
    }

    static TColumnAccessPath bare_path() {
        TColumnAccessPath access_path;
        access_path.type = TAccessPathType::DATA;
        return access_path;
    }
};

TEST_F(RowIdStorageReaderTest, SameSourceColumnSharesKey) {
    // The bug case: one physical column projected twice must dedup onto one scan column.
    const SlotDescriptor first = make_slot({});
    const SlotDescriptor second = make_slot({});
    EXPECT_EQ(key_of(first, 3), key_of(second, 3));
}

TEST_F(RowIdStorageReaderTest, ColumnIndexSeparatesKeys) {
    const SlotDescriptor slot = make_slot({});
    EXPECT_NE(key_of(slot, 3), key_of(slot, 4));
}

TEST_F(RowIdStorageReaderTest, ColumnNameSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.col_name = "a"}), 0), key_of(make_slot({.col_name = "b"}), 0));
}

TEST_F(RowIdStorageReaderTest, UniqueIdSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.col_unique_id = 1}), 0),
              key_of(make_slot({.col_unique_id = 2}), 0));
}

TEST_F(RowIdStorageReaderTest, NameAndIndexBoundaryIsNotAmbiguous) {
    // Without length prefixes, "a" + idx 12 and "a1" + idx 2 both flatten to "a12".
    EXPECT_NE(key_of(make_slot({.col_name = "a"}), 12), key_of(make_slot({.col_name = "a1"}), 2));
}

TEST_F(RowIdStorageReaderTest, PathComponentBoundaryIsNotAmbiguous) {
    // The concatenation hazard the length prefix exists for: ["a", "b"] and ["a:b"] are
    // different nested columns but share the naive ':'-joined spelling.
    EXPECT_NE(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"a:b"}}), 0));
}

TEST_F(RowIdStorageReaderTest, EmptyPathIsNotTheSameAsNoPath) {
    EXPECT_NE(key_of(make_slot({.column_paths = {}}), 0),
              key_of(make_slot({.column_paths = {""}}), 0));
}

TEST_F(RowIdStorageReaderTest, PathOrderMatters) {
    EXPECT_NE(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"b", "a"}}), 0));
}

TEST_F(RowIdStorageReaderTest, EqualPathsShareKey) {
    EXPECT_EQ(key_of(make_slot({.column_paths = {"a", "b"}}), 0),
              key_of(make_slot({.column_paths = {"a", "b"}}), 0));
}

TEST_F(RowIdStorageReaderTest, AccessPathSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.access_paths = {data_path({"a"})}}), 0),
              key_of(make_slot({.access_paths = {data_path({"b"})}}), 0));
}

TEST_F(RowIdStorageReaderTest, AbsentAccessPathIsNotAnEmptyOne) {
    // The presence bit: an unset data_access_path must not collide with one that is set
    // but carries no components.
    EXPECT_NE(key_of(make_slot({.access_paths = {bare_path()}}), 0),
              key_of(make_slot({.access_paths = {data_path({})}}), 0));
}

TEST_F(RowIdStorageReaderTest, AccessPathCountSeparatesKeys) {
    EXPECT_NE(key_of(make_slot({.access_paths = {data_path({"a"})}}), 0),
              key_of(make_slot({.access_paths = {data_path({"a"}), data_path({"b"})}}), 0));
}

} // namespace doris
