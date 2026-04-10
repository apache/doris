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

#include "core/data_type/data_type_file.h"

#include <gen_cpp/data.pb.h>
#include <gtest/gtest.h>
#include <streamvbyte.h>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/column/column_file.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/file_schema_descriptor.h"
#include "core/data_type_serde/data_type_file_serde.h"
#include "core/string_buffer.hpp"
#include "util/jsonb_utils.h"
#include "util/jsonb_writer.h"

namespace doris {

static std::string make_file_jsonb(const FileMetadata& meta) {
    JsonbWriter writer;
    FileSchemaDescriptor::write_file_jsonb(writer, meta);
    return std::string(writer.getOutput()->getBuffer(),
                       static_cast<size_t>(writer.getOutput()->getSize()));
}

// Compare two JSONB blobs by their JSON text representation, since JSONB
// integer encoding width may change during a JSON roundtrip (e.g. int64 → int32).
static std::string jsonb_to_json(const StringRef& ref) {
    return JsonbToJson::jsonb_to_json_string(ref.data, ref.size);
}

static FileMetadata sample_metadata(int idx = 0) {
    return FileMetadata {
            .uri = "s3://bucket/dir/file" + std::to_string(idx) + ".csv",
            .file_name = "file" + std::to_string(idx) + ".csv",
            .content_type = "text/csv",
            .size = 1000 + idx,
            .region = "us-east-1",
            .endpoint = "s3.amazonaws.com",
            .ak = "AKIAEXAMPLE",
            .sk = "secretkey",
            .role_arn = {},
            .external_id = {},
    };
}

class DataTypeFileTest : public ::testing::Test {
protected:
    DataTypeFile dt_file;
};

// ---------- Type metadata ----------

TEST_F(DataTypeFileTest, PrimitiveType) {
    EXPECT_EQ(dt_file.get_primitive_type(), TYPE_FILE);
}

TEST_F(DataTypeFileTest, StorageFieldType) {
    EXPECT_EQ(dt_file.get_storage_field_type(), FieldType::OLAP_FIELD_TYPE_FILE);
}

TEST_F(DataTypeFileTest, FamilyName) {
    EXPECT_EQ(dt_file.get_family_name(), "File");
}

TEST_F(DataTypeFileTest, TypeName) {
    EXPECT_EQ(dt_file.do_get_name(), "File");
}

// ---------- Equality ----------

TEST_F(DataTypeFileTest, EqualsWithSameType) {
    DataTypeFile other;
    EXPECT_TRUE(dt_file.equals(other));
}

TEST_F(DataTypeFileTest, EqualsWithDifferentType) {
    DataTypeJsonb jsonb_type;
    EXPECT_FALSE(dt_file.equals(jsonb_type));
}

// ---------- Column creation ----------

TEST_F(DataTypeFileTest, CreateColumn) {
    auto col = dt_file.create_column();
    EXPECT_EQ(col->size(), 0);
    // Verify it's a ColumnFile
    auto* file_col = check_and_get_column<ColumnFile>(col.get());
    ASSERT_TRUE(file_col != nullptr);
}

// ---------- check_column ----------

TEST_F(DataTypeFileTest, CheckColumnValidColumnFile) {
    auto col = dt_file.create_column();
    col->insert_default();
    auto st = dt_file.check_column(*col);
    EXPECT_TRUE(st.ok());
}

TEST_F(DataTypeFileTest, CheckColumnInvalidColumnType) {
    auto col = ColumnString::create();
    col->insert_default();
    auto st = dt_file.check_column(*col);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("ColumnFile") != std::string::npos);
}

// ---------- get_field throws ----------

TEST_F(DataTypeFileTest, GetFieldThrowsNotImplemented) {
    TExprNode node;
    EXPECT_THROW(dt_file.get_field(node), doris::Exception);
}

// ---------- Serialize / Deserialize ----------

TEST_F(DataTypeFileTest, SerializeDeserializeEmptyColumn) {
    int be_exec_version = BeExecVersionManager::get_newest_version();
    auto col = dt_file.create_column();

    int64_t bytes = dt_file.get_uncompressed_serialized_bytes(*col, be_exec_version);
    EXPECT_GT(bytes, 0);

    std::string buf(bytes + STREAMVBYTE_PADDING, '\0');
    char* end = dt_file.serialize(*col, buf.data(), be_exec_version);
    EXPECT_NE(end, nullptr);

    MutableColumnPtr deser_col;
    const char* next = dt_file.deserialize(buf.data(), &deser_col, be_exec_version);
    EXPECT_NE(next, nullptr);
    EXPECT_EQ(deser_col->size(), 0);
}

TEST_F(DataTypeFileTest, SerializeDeserializeWithData) {
    int be_exec_version = BeExecVersionManager::get_newest_version();
    auto col = dt_file.create_column();
    auto& file_col = assert_cast<ColumnFile&>(*col);

    for (int i = 0; i < 5; ++i) {
        auto blob = make_file_jsonb(sample_metadata(i));
        auto& jsonb = file_col.get_jsonb_column();
        const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
    }

    int64_t bytes = dt_file.get_uncompressed_serialized_bytes(*col, be_exec_version);
    std::string buf(bytes + STREAMVBYTE_PADDING, '\0');
    char* end = dt_file.serialize(*col, buf.data(), be_exec_version);
    EXPECT_NE(end, nullptr);

    MutableColumnPtr deser_col;
    const char* next = dt_file.deserialize(buf.data(), &deser_col, be_exec_version);
    EXPECT_NE(next, nullptr);
    EXPECT_EQ(deser_col->size(), 5);
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(deser_col->get_data_at(i), col->get_data_at(i));
    }
}

// ---------- DataTypeFactory ----------

TEST_F(DataTypeFileTest, CreateFromPrimitiveType) {
    auto dt = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_FILE, false);
    EXPECT_NE(dt, nullptr);
    EXPECT_EQ(dt->get_primitive_type(), TYPE_FILE);
}

TEST_F(DataTypeFileTest, CreateFromFieldType) {
    auto dt = DataTypeFactory::instance().create_data_type(FieldType::OLAP_FIELD_TYPE_FILE, false,
                                                           0, 0);
    EXPECT_NE(dt, nullptr);
    EXPECT_EQ(dt->get_primitive_type(), TYPE_FILE);
}

// ---------- to_pb_column_meta ----------

TEST_F(DataTypeFileTest, ToPbColumnMeta) {
    PColumnMeta meta;
    dt_file.to_pb_column_meta(&meta);
    // Delegates to DataTypeJsonb, so it should set something
    EXPECT_TRUE(meta.IsInitialized());
}

// =====================================================
// DataTypeFileSerDe tests
// =====================================================

class DataTypeFileSerDeTest : public ::testing::Test {
protected:
    DataTypeFile dt_file;

    MutableColumnPtr create_populated_column(int count = 3) {
        auto col = dt_file.create_column();
        auto& file_col = assert_cast<ColumnFile&>(*col);
        for (int i = 0; i < count; ++i) {
            auto blob = make_file_jsonb(sample_metadata(i));
            auto& jsonb = file_col.get_jsonb_column();
            const_cast<IColumn&>(jsonb).insert_data(blob.data(), blob.size());
        }
        return col;
    }
};

TEST_F(DataTypeFileSerDeTest, GetName) {
    auto serde = dt_file.get_serde();
    EXPECT_EQ(serde->get_name(), "File");
}

TEST_F(DataTypeFileSerDeTest, WriteReadPb) {
    auto serde = dt_file.get_serde();
    auto col = create_populated_column(3);

    PValues pv;
    auto st = serde->write_column_to_pb(*col, pv, 0, 3);
    EXPECT_TRUE(st.ok());

    auto col2 = dt_file.create_column();
    st = serde->read_column_from_pb(*col2, pv);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(col2->size(), 3);
    for (size_t i = 0; i < 3; ++i) {
        // Compare via JSON text because JSONB integer encoding width may differ
        // after a PB roundtrip (JSONB→JSON text→JSONB re-encodes int types).
        EXPECT_EQ(jsonb_to_json(col2->get_data_at(i)), jsonb_to_json(col->get_data_at(i)));
    }
}

TEST_F(DataTypeFileSerDeTest, SerializeColumnToJsonbVector) {
    auto serde = dt_file.get_serde();
    auto col = create_populated_column(2);

    auto to_col = ColumnString::create();
    auto st = serde->serialize_column_to_jsonb_vector(*col, *to_col);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(to_col->size(), 2);

    // Each entry should contain the JSONB blob
    for (size_t i = 0; i < 2; ++i) {
        auto ref = to_col->get_data_at(i);
        EXPECT_GT(ref.size, 0);
        EXPECT_EQ(ref, col->get_data_at(i));
    }
}

TEST_F(DataTypeFileSerDeTest, SetReturnObjectAsString) {
    auto serde = dt_file.get_serde();
    // Should not crash
    serde->set_return_object_as_string(true);
    serde->set_return_object_as_string(false);
}

TEST_F(DataTypeFileSerDeTest, GetNestedSerdes) {
    auto serde = dt_file.get_serde();
    // FILE serde delegates to JSONB serde which does not support get_nested_serdes
    EXPECT_THROW(serde->get_nested_serdes(), doris::Exception);
}

TEST_F(DataTypeFileSerDeTest, FromString) {
    auto serde = dt_file.get_serde();
    auto col = dt_file.create_column();

    // Construct a simple JSONB string (this would normally come from a JSONB serialization)
    auto blob = make_file_jsonb(sample_metadata(0));
    StringRef str(blob.data(), blob.size());
    DataTypeSerDe::FormatOptions opts;
    auto st = serde->from_string(str, *col, opts);
    // from_string delegates to JSONB serde; the result depends on format compatibility
    // Just verify it doesn't crash and returns some status
    (void)st;
}

TEST_F(DataTypeFileSerDeTest, SerializeOneCell) {
    auto serde = dt_file.get_serde();
    auto col = create_populated_column(1);

    auto output_col = ColumnString::create();
    BufferWritable bw(*output_col);
    DataTypeSerDe::FormatOptions opts;
    auto st = serde->serialize_one_cell_to_json(*col, 0, bw, opts);
    bw.commit();
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(output_col->size(), 1);
    EXPECT_GT(output_col->get_data_at(0).size, 0);
}

// ---------- to_string ----------

TEST_F(DataTypeFileSerDeTest, ToString) {
    auto serde = dt_file.get_serde();
    auto col = create_populated_column(1);

    auto output_col = ColumnString::create();
    BufferWritable bw(*output_col);
    DataTypeSerDe::FormatOptions opts;
    serde->to_string(*col, 0, bw, opts);
    bw.commit();
    EXPECT_EQ(output_col->size(), 1);
    EXPECT_GT(output_col->get_data_at(0).size, 0);
}

} // namespace doris
