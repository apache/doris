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

#include <gen_cpp/data.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"

namespace doris {
namespace {

Status serialize_variant_block(const DataTypePtr& type, int exec_version,
                               segment_v2::CompressionTypePB compression, PBlock* wire) {
    auto column = type->create_column();
    column->insert_many_defaults(64);
    Block block({{std::move(column), type, "v"}});
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compression_time = 0;
    return block.serialize(exec_version, wire, &uncompressed_bytes, &compressed_bytes,
                           &compression_time, compression);
}

Status deserialize_variant_block(const PBlock& wire, Block* block) {
    size_t uncompressed_bytes = 0;
    int64_t decompression_time = 0;
    return block->deserialize(wire, &uncompressed_bytes, &decompression_time);
}

} // namespace

TEST(BlockVariantWireFormatTest, RejectsLegacyV1MarkerInsideNullableStruct) {
    const auto type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeVariant>())}, Strings {"nested"});
    for (auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
        PBlock wire;
        // Keep a decodable payload so the pre-fix test fails without an out-of-bounds read.
        // 4.0.8 sends exec version 8 and omits the V1 doc-mode metadata field.
        ASSERT_TRUE(serialize_variant_block(type, USE_CONST_SERDE, compression, &wire).ok());
        ASSERT_EQ(wire.compressed(), compression == segment_v2::SNAPPY);
        auto* variant = wire.mutable_column_metas(0)->mutable_children(0);
        ASSERT_TRUE(variant->has_variant_enable_doc_mode());
        ASSERT_FALSE(variant->variant_enable_doc_mode());
        variant->clear_variant_enable_doc_mode();

        Block decoded;
        const Status status = deserialize_variant_block(wire, &decoded);
        EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
        EXPECT_NE(status.to_string().find("Unsupported VARIANT block format"), std::string::npos)
                << status;
        EXPECT_EQ(decoded.columns(), 0);
    }
}

TEST(BlockVariantWireFormatTest, RoundTripsCurrentVariantFormats) {
    const DataTypes types {std::make_shared<DataTypeVariant>(0, false),
                           std::make_shared<DataTypeVariant>(0, true),
                           std::make_shared<DataTypeVariantV2>()};
    for (const auto& type : types) {
        SCOPED_TRACE(type->get_name());
        PBlock wire;
        ASSERT_TRUE(serialize_variant_block(type, BeExecVersionManager::get_newest_version(),
                                            segment_v2::NO_COMPRESSION, &wire)
                            .ok());
        const auto& meta = wire.column_metas(0);
        ASSERT_TRUE(meta.has_variant_enable_doc_mode());
        EXPECT_EQ(meta.variant_is_v2(),
                  dynamic_cast<const DataTypeVariantV2*>(type.get()) != nullptr);

        Block decoded;
        const Status status = deserialize_variant_block(wire, &decoded);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(decoded.rows(), 64);
        EXPECT_TRUE(decoded.get_by_position(0).type->equals(*type));
    }
}

} // namespace doris
