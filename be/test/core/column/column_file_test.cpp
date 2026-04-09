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

#include "core/column/column_file.h"

#include <gtest/gtest.h>

#include "core/arena.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/file_schema_descriptor.h"
#include "exec/common/sip_hash.h"
#include "util/jsonb_writer.h"

namespace doris {

// Helper: create a JSONB blob representing a FileMetadata
static std::string make_file_jsonb(const FileMetadata& meta) {
    JsonbWriter writer;
    FileSchemaDescriptor::write_file_jsonb(writer, meta);
    return std::string(writer.getOutput()->getBuffer(),
                       static_cast<size_t>(writer.getOutput()->getSize()));
}

static FileMetadata sample_metadata(int idx = 0) {
    return FileMetadata {.uri = "s3://bucket/dir/file" + std::to_string(idx) + ".csv",
                         .file_name = "file" + std::to_string(idx) + ".csv",
                         .content_type = "text/csv",
                         .size = 1000 + idx,
                         .region = "us-east-1",
                         .endpoint = "s3.amazonaws.com",
                         .ak = "AKIAEXAMPLE",
                         .sk = "secretkey",
                         .role_arn = "arn:aws:iam::123456789012:role/example-role",
                         .external_id = "external-id-123"};
}

class ColumnFileTest : public ::testing::Test {
protected:
    const FileSchemaDescriptor& schema_ = FileSchemaDescriptor::instance();
};

// ---------- Creation ----------

TEST_F(ColumnFileTest, CreateFromSchema) {
    auto col = ColumnFile::create(schema_);
    EXPECT_EQ(col->size(), 0);
    EXPECT_EQ(col->get_name(), "File");
}

TEST_F(ColumnFileTest, CreateFromJsonb) {
    auto jsonb_col = DataTypeJsonb().create_column();
    auto blob = make_file_jsonb(sample_metadata());
    jsonb_col->insert_data(blob.data(), blob.size());

    auto file_col = ColumnFile::create_from_jsonb(std::move(jsonb_col));
    EXPECT_EQ(file_col->size(), 1);
}

// ---------- Insert operations ----------

TEST_F(ColumnFileTest, InsertDefault) {
    auto col = ColumnFile::create(schema_);
    col->insert_default();
    EXPECT_EQ(col->size(), 1);
}

TEST_F(ColumnFileTest, InsertFrom) {
    auto src = ColumnFile::create(schema_);
    auto blob = make_file_jsonb(sample_metadata(0));
    auto& src_jsonb = src->get_jsonb_column();
    const_cast<IColumn&>(src_jsonb).insert_data(blob.data(), blob.size());

    auto dst = ColumnFile::create(schema_);
    dst->insert_from(*src, 0);
    EXPECT_EQ(dst->size(), 1);
    EXPECT_EQ(dst->get_data_at(0), src->get_data_at(0));
}

TEST_F(ColumnFileTest, InsertRangeFrom) {
    auto src = ColumnFile::create(schema_);
    for (int i = 0; i < 5; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& src_jsonb = src->get_jsonb_column();
        const_cast<IColumn&>(src_jsonb).insert_data(blob.data(), blob.size());
    }

    auto dst = ColumnFile::create(schema_);
    dst->insert_range_from(*src, 1, 3);
    EXPECT_EQ(dst->size(), 3);
    EXPECT_EQ(dst->get_data_at(0), src->get_data_at(1));
    EXPECT_EQ(dst->get_data_at(2), src->get_data_at(3));
}

TEST_F(ColumnFileTest, InsertManyFrom) {
    auto src = ColumnFile::create(schema_);
    auto blob = make_file_jsonb(sample_metadata(0));
    auto& src_jsonb = src->get_jsonb_column();
    const_cast<IColumn&>(src_jsonb).insert_data(blob.data(), blob.size());

    auto dst = ColumnFile::create(schema_);
    dst->insert_many_from(*src, 0, 4);
    EXPECT_EQ(dst->size(), 4);
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(dst->get_data_at(i), src->get_data_at(0));
    }
}

// ---------- PopBack ----------

TEST_F(ColumnFileTest, PopBack) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 3; ++i) {
        col->insert_default();
    }
    EXPECT_EQ(col->size(), 3);
    col->pop_back(2);
    EXPECT_EQ(col->size(), 1);
}

// ---------- Clone / Resize ----------

TEST_F(ColumnFileTest, CloneResized) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 3; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = col->get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    auto cloned = col->clone_resized(2);
    EXPECT_EQ(cloned->size(), 2);
    EXPECT_EQ(cloned->get_data_at(0), col->get_data_at(0));
    EXPECT_EQ(cloned->get_data_at(1), col->get_data_at(1));
}

TEST_F(ColumnFileTest, CloneResizedLarger) {
    auto col = ColumnFile::create(schema_);
    auto blob = make_file_jsonb(sample_metadata());
    auto& jsonb = col->get_jsonb_column();
    const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());

    auto cloned = col->clone_resized(3);
    EXPECT_EQ(cloned->size(), 3);
    EXPECT_EQ(cloned->get_data_at(0), col->get_data_at(0));
}

// ---------- Filter ----------

TEST_F(ColumnFileTest, FilterByMask) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 4; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = col->get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    IColumn::Filter filter = {1, 0, 1, 0};
    auto filtered = col->filter(filter, -1);
    EXPECT_EQ(filtered->size(), 2);
    EXPECT_EQ(filtered->get_data_at(0), col->get_data_at(0));
    EXPECT_EQ(filtered->get_data_at(1), col->get_data_at(2));
}

TEST_F(ColumnFileTest, FilterInPlace) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 4; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = col->get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    auto orig_row0 = col->get_data_at(0).to_string();
    auto orig_row2 = col->get_data_at(2).to_string();

    IColumn::Filter filter = {1, 0, 1, 0};
    size_t result = col->filter(filter);
    EXPECT_EQ(result, 2);
    EXPECT_EQ(col->size(), 2);
    EXPECT_EQ(col->get_data_at(0).to_string(), orig_row0);
    EXPECT_EQ(col->get_data_at(1).to_string(), orig_row2);
}

// ---------- Permute ----------

TEST_F(ColumnFileTest, Permute) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 3; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = col->get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    IColumn::Permutation perm = {2, 0, 1};
    auto permuted = col->permute(perm, 0);
    EXPECT_EQ(permuted->size(), 3);
    EXPECT_EQ(permuted->get_data_at(0), col->get_data_at(2));
    EXPECT_EQ(permuted->get_data_at(1), col->get_data_at(0));
    EXPECT_EQ(permuted->get_data_at(2), col->get_data_at(1));
}

// ---------- Compare ----------

TEST_F(ColumnFileTest, CompareAt) {
    auto col1 = ColumnFile::create(schema_);
    auto blob1 = make_file_jsonb(sample_metadata(0));
    auto& jsonb1 = col1->get_jsonb_column();
    const_cast<IColumn&>(jsonb1).insert_data(blob1.data(), blob1.size());

    auto col2 = ColumnFile::create(schema_);
    auto blob2 = make_file_jsonb(sample_metadata(1));
    auto& jsonb2 = col2->get_jsonb_column();
    const_cast<IColumn&>(jsonb2).insert_data(blob2.data(), blob2.size());

    // Same row should compare equal to itself
    EXPECT_EQ(col1->compare_at(0, 0, *col1, 1), 0);
    // Different rows
    int cmp = col1->compare_at(0, 0, *col2, 1);
    EXPECT_NE(cmp, 0);
}

// ---------- Hash ----------

TEST_F(ColumnFileTest, UpdateHashWithValue) {
    auto col = ColumnFile::create(schema_);
    auto blob = make_file_jsonb(sample_metadata());
    auto& jsonb = col->get_jsonb_column();
    const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());

    SipHash hash1;
    col->update_hash_with_value(0, hash1);
    uint64_t h1 = hash1.get64();

    SipHash hash2;
    col->update_hash_with_value(0, hash2);
    uint64_t h2 = hash2.get64();

    EXPECT_EQ(h1, h2);
}

TEST_F(ColumnFileTest, UpdateXxHashWithValue) {
    auto col = ColumnFile::create(schema_);
    auto blob = make_file_jsonb(sample_metadata());
    auto& jsonb = col->get_jsonb_column();
    const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());

    uint64_t hash = 0;
    col->update_xxHash_with_value(0, 1, hash, nullptr);
    EXPECT_NE(hash, 0);
}

// ---------- Structure equals ----------

TEST_F(ColumnFileTest, StructureEquals) {
    auto col1 = ColumnFile::create(schema_);
    auto col2 = ColumnFile::create(schema_);
    EXPECT_TRUE(col1->structure_equals(*col2));

    auto string_col = ColumnString::create();
    EXPECT_FALSE(col1->structure_equals(*string_col));
}

// ---------- has_enough_capacity ----------

TEST_F(ColumnFileTest, HasEnoughCapacity) {
    auto col = ColumnFile::create(schema_);
    col->reserve(100);

    auto small = ColumnFile::create(schema_);
    small->insert_default();
    EXPECT_TRUE(col->has_enough_capacity(*small));
}

// ---------- JSONB column accessors ----------

TEST_F(ColumnFileTest, GetJsonbColumn) {
    auto col = ColumnFile::create(schema_);
    col->insert_default();

    const IColumn& jsonb = col->get_jsonb_column();
    EXPECT_EQ(jsonb.size(), 1);
}

TEST_F(ColumnFileTest, SetJsonbColumnPtr) {
    auto col = ColumnFile::create(schema_);
    auto new_jsonb = DataTypeJsonb().create_column();
    auto blob = make_file_jsonb(sample_metadata());
    new_jsonb->insert_data(blob.data(), blob.size());
    new_jsonb->insert_data(blob.data(), blob.size());

    col->set_jsonb_column_ptr(std::move(new_jsonb));
    EXPECT_EQ(col->size(), 2);
}

// ---------- check_schema ----------

TEST_F(ColumnFileTest, CheckSchemaValid) {
    auto col = ColumnFile::create(schema_);
    col->insert_default();
    auto st = col->check_schema(schema_);
    EXPECT_TRUE(st.ok());
}

// ---------- Erase ----------

TEST_F(ColumnFileTest, Erase) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 5; ++i) {
        col->insert_default();
    }
    col->erase(1, 2);
    EXPECT_EQ(col->size(), 3);
}

// ---------- Clear ----------

TEST_F(ColumnFileTest, Clear) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 5; ++i) {
        col->insert_default();
    }
    col->clear();
    EXPECT_EQ(col->size(), 0);
}

// ---------- Byte size ----------

TEST_F(ColumnFileTest, ByteSize) {
    auto col = ColumnFile::create(schema_);
    auto empty_size = col->byte_size();

    auto blob = make_file_jsonb(sample_metadata());
    auto& jsonb = col->get_jsonb_column();
    const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());

    EXPECT_GT(col->byte_size(), empty_size);
    EXPECT_GT(col->allocated_bytes(), 0);
}

// ---------- InsertIndicesFrom ----------

TEST_F(ColumnFileTest, InsertIndicesFrom) {
    auto src = ColumnFile::create(schema_);
    for (int i = 0; i < 4; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = src->get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    std::vector<uint32_t> indices = {3, 1, 0};
    auto dst = ColumnFile::create(schema_);
    dst->insert_indices_from(*src, indices.data(), indices.data() + indices.size());
    EXPECT_EQ(dst->size(), 3);
    EXPECT_EQ(dst->get_data_at(0), src->get_data_at(3));
    EXPECT_EQ(dst->get_data_at(1), src->get_data_at(1));
    EXPECT_EQ(dst->get_data_at(2), src->get_data_at(0));
}

// ---------- ReplaceColumnData ----------

TEST_F(ColumnFileTest, ReplaceColumnDataThrows) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 3; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = col->get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    auto src = ColumnFile::create(schema_);
    auto blob = make_file_jsonb(sample_metadata(99));
    auto& src_jsonb = src->get_jsonb_column();
    const_cast<IColumn&>(src_jsonb).insert_data(blob.data(), blob.size());

    // ColumnString does not support replace_column_data
    EXPECT_THROW(col->replace_column_data(*src, 0, 1), doris::Exception);
}

// ---------- Serialize / Deserialize (binary) ----------

TEST_F(ColumnFileTest, SerializeDeserializeBinary) {
    auto col = ColumnFile::create(schema_);
    for (int i = 0; i < 3; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = col->get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    // Test serialize_size_at and serialize_impl / deserialize_impl for each row
    for (size_t r = 0; r < col->size(); ++r) {
        size_t ser_size = col->serialize_size_at(r);
        EXPECT_GT(ser_size, 0);

        std::string buf(ser_size + 64, '\0');
        size_t written = col->serialize_impl(buf.data(), r);
        EXPECT_GT(written, 0);

        auto col2 = ColumnFile::create(schema_);
        size_t read = col2->deserialize_impl(buf.data());
        EXPECT_EQ(read, written);
        EXPECT_EQ(col2->size(), 1);
        EXPECT_EQ(col2->get_data_at(0), col->get_data_at(r));
    }
}

// ---------- Arena serialization ----------

TEST_F(ColumnFileTest, SerializeDeserializeArena) {
    auto col = ColumnFile::create(schema_);
    auto blob = make_file_jsonb(sample_metadata());
    auto& jsonb = col->get_jsonb_column();
    const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());

    Arena arena;
    const char* begin = nullptr;
    StringRef ref = col->serialize_value_into_arena(0, arena, begin);
    EXPECT_GT(ref.size, 0);

    auto col2 = ColumnFile::create(schema_);
    const char* pos = col2->deserialize_and_insert_from_arena(begin);
    EXPECT_NE(pos, nullptr);
    EXPECT_EQ(col2->size(), 1);
    EXPECT_EQ(col2->get_data_at(0), col->get_data_at(0));
}

} // namespace doris
