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

#include <cstring>
#include <memory>
#include <string>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"

namespace doris {
namespace {
PBlock serialize_wire(const Block& block, segment_v2::CompressionTypePB compression) {
    PBlock wire;
    size_t uncompressed = 0;
    size_t compressed = 0;
    int64_t time = 0;
    EXPECT_TRUE(block.serialize(BeExecVersionManager::get_newest_version(), &wire, &uncompressed,
                                &compressed, &time, compression)
                        .ok());
    return wire;
}
Status decode_wire(const PBlock& wire, Block& result) {
    size_t bytes = 0;
    int64_t time = 0;
    return result.deserialize(wire, &bytes, &time);
}
void expect_legacy_rejected(const PBlock& wire) {
    Block result;
    const Status status = decode_wire(wire, result);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("Unsupported VARIANT block format"), std::string::npos);
}
} // namespace

TEST(BlockVariantWireFormatTest, RejectsMissingDocValueFormatMarker) {
    for (bool doc_mode : {false, true}) {
        DataTypePtr type = std::make_shared<DataTypeVariant>(0, doc_mode);
        auto column = type->create_column();
        column->insert_default();
        Block source({{std::move(column), type, "v"}});
        for (auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
            PBlock wire = serialize_wire(source, compression);
            ASSERT_TRUE(wire.column_metas(0).has_variant_enable_doc_mode());
            wire.mutable_column_metas(0)->clear_variant_enable_doc_mode();
            expect_legacy_rejected(wire);
        }
    }
}

TEST(BlockVariantWireFormatTest, RejectsActualPreDocValuePayload) {
    // The pre-doc-value V1 wire layout has only subcolumn count, row count and sparse Map.
    const auto& map_type = ColumnVariant::get_binary_column_type();
    const auto sparse = map_type->create_column();
    const int version = BeExecVersionManager::get_newest_version();
    std::string buffer(
            2 * sizeof(uint32_t) + map_type->get_uncompressed_serialized_bytes(*sparse, version),
            '\0');
    char* end = map_type->serialize(*sparse, buffer.data() + 2 * sizeof(uint32_t), version);
    PBlock wire;
    wire.set_be_exec_version(version);
    auto* meta = wire.add_column_metas();
    meta->set_name("legacy_v1");
    meta->set_type(PGenericType::VARIANT);
    // Copy just the actual old payload, without a serialized doc-value Map or spare capacity.
    wire.set_column_values(buffer.data(), end - buffer.data());
    expect_legacy_rejected(wire);
}

TEST(BlockVariantWireFormatTest, RejectsLegacyV1InsideComplexTypes) {
    const DataTypePtr variant = std::make_shared<DataTypeVariant>();
    const DataTypePtr array = std::make_shared<DataTypeArray>(make_nullable(variant));
    const DataTypes types = {
            std::make_shared<DataTypeArray>(array),
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), array),
            std::make_shared<DataTypeStruct>(DataTypes {array}, Strings {"v"})};
    for (const auto& type : types) {
        auto column = type->create_column();
        column->insert_default();
        Block source({{std::move(column), type, "nested"}});
        for (auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
            PBlock wire = serialize_wire(source, compression);
            auto* root = wire.mutable_column_metas(0);
            auto* child = root->mutable_children(root->type() == PGenericType::MAP ? 1 : 0);
            child->mutable_children(0)->clear_variant_enable_doc_mode();
            expect_legacy_rejected(wire);
        }
    }
}

TEST(BlockVariantWireFormatTest, RoundTripsSupportedV1AndV2) {
    for (bool v2 : {false, true}) {
        for (bool doc_mode : {false, true}) {
            for (bool nullable : {false, true}) {
                DataTypePtr type =
                        v2 ? DataTypePtr(std::make_shared<DataTypeVariantV2>(0, doc_mode))
                           : DataTypePtr(std::make_shared<DataTypeVariant>(0, doc_mode));
                if (nullable) {
                    type = make_nullable(type);
                }
                auto column = type->create_column();
                column->insert_default();
                Block source({{std::move(column), type, "v"}});
                for (auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
                    PBlock wire = serialize_wire(source, compression);
                    if (v2) {
                        // V2 has its own marker and does not require a V1 doc-value marker.
                        wire.mutable_column_metas(0)->clear_variant_enable_doc_mode();
                    }
                    Block decoded;
                    ASSERT_TRUE(decode_wire(wire, decoded).ok());
                    ASSERT_EQ(decoded.rows(), source.rows());
                    // 4.1 does not implement compare_at for Variant columns, so compare the
                    // re-serialized payload with the payload that was sent instead.
                    PBlock reserialized = serialize_wire(decoded, compression);
                    EXPECT_EQ(reserialized.column_values(), wire.column_values());
                }
            }
        }
    }
}
} // namespace doris
