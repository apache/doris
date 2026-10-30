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
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant_v2.h"

namespace doris {
namespace {

PBlock serialize_block(const Block& block, segment_v2::CompressionTypePB compression) {
    PBlock wire;
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compression_time = 0;
    EXPECT_TRUE(block.serialize(BeExecVersionManager::get_newest_version(), &wire,
                                &uncompressed_bytes, &compressed_bytes, &compression_time,
                                compression)
                        .ok());
    return wire;
}

Block variant_block(bool nullable) {
    auto numbers = ColumnInt32::create();
    numbers->insert_value(42);
    numbers->insert_value(7);
    ColumnPtr column = ColumnVariantV2::create_typed(make_nullable(std::move(numbers)),
                                                     std::make_shared<DataTypeInt32>());
    DataTypePtr type = std::make_shared<DataTypeVariantV2>();
    if (nullable) {
        column = make_nullable(column);
        type = make_nullable(type);
    }
    return Block({{std::move(column), type, "v"}});
}

Status deserialize_block(const PBlock& wire, Block& block) {
    size_t uncompressed_bytes = 0;
    int64_t decompression_time = 0;
    return block.deserialize(wire, &uncompressed_bytes, &decompression_time);
}

void expect_unsupported_variant(const PBlock& wire, size_t preceding_columns = 0) {
    Block decoded;
    const Status status = deserialize_block(wire, decoded);
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_NE(status.to_string().find("Unsupported VARIANT block format"), std::string::npos)
            << status;
    EXPECT_EQ(decoded.columns(), preceding_columns);
}

} // namespace

TEST(BlockVariantWireFormatTest, RejectsLegacyVariantMetadata) {
    for (bool nullable : {false, true}) {
        const Block source = variant_block(nullable);
        const PBlock serialized = serialize_block(source, segment_v2::NO_COMPRESSION);
        // Keep a valid payload so the unfixed decoder demonstrates that it ignores the format
        // marker, without relying on an out-of-bounds read to reproduce the missing rejection.
        for (int legacy_format = 0; legacy_format < 3; ++legacy_format) {
            PBlock wire = serialized;
            auto* meta = wire.mutable_column_metas(0);
            meta->clear_variant_is_v2();
            if (legacy_format == 0) {
                meta->clear_variant_enable_doc_mode();
            } else {
                meta->set_variant_is_v2(false);
                meta->set_variant_enable_doc_mode(legacy_format == 2);
            }
            expect_unsupported_variant(wire);
        }
    }
}

TEST(BlockVariantWireFormatTest, RejectsLegacyVariantAfterPrecedingColumn) {
    auto numbers = ColumnInt32::create();
    numbers->insert_value(1);
    numbers->insert_value(2);
    Block source = variant_block(false);
    source.insert(0, {std::move(numbers), std::make_shared<DataTypeInt32>(), "n"});
    for (auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
        PBlock wire = serialize_block(source, compression);
        wire.mutable_column_metas(1)->clear_variant_is_v2();
        expect_unsupported_variant(wire, 1);
    }
}

TEST(BlockVariantWireFormatTest, RejectsLegacyVariantInsideComplexTypes) {
    const DataTypePtr variant = std::make_shared<DataTypeVariantV2>();
    const DataTypePtr array = std::make_shared<DataTypeArray>(make_nullable(variant));
    const DataTypes types = {
            std::make_shared<DataTypeArray>(array),
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), array),
            std::make_shared<DataTypeStruct>(DataTypes {array}, Strings {"v"})};
    for (const auto& type : types) {
        auto column = type->create_column();
        column->insert_default();
        Block source({{std::move(column), type, "nested_variant"}});
        for (auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
            PBlock wire = serialize_block(source, compression);
            auto* meta = wire.mutable_column_metas(0);
            if (meta->type() == PGenericType::MAP) {
                meta = meta->mutable_children(1);
            } else {
                meta = meta->mutable_children(0);
            }
            meta->mutable_children(0)->clear_variant_is_v2();
            expect_unsupported_variant(wire);
        }
    }
}

TEST(BlockVariantWireFormatTest, RoundTripsVariantV2) {
    for (bool nullable : {false, true}) {
        const Block source = variant_block(nullable);
        for (auto compression : {segment_v2::NO_COMPRESSION, segment_v2::SNAPPY}) {
            PBlock wire = serialize_block(source, compression);
            ASSERT_TRUE(wire.column_metas(0).variant_is_v2());
            Block decoded;
            ASSERT_TRUE(deserialize_block(wire, decoded).ok());
            ASSERT_EQ(decoded.rows(), source.rows());
            for (size_t row = 0; row < source.rows(); ++row) {
                EXPECT_EQ(decoded.get_by_position(0).column->compare_at(
                                  row, row, *source.get_by_position(0).column, 1),
                          0);
            }
        }
    }
}

TEST(BlockVariantWireFormatTest, RoundTripsVariantV2InsideComplexTypes) {
    const DataTypePtr variant = std::make_shared<DataTypeVariantV2>();
    const DataTypes types = {
            std::make_shared<DataTypeArray>(make_nullable(variant)),
            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                          make_nullable(variant)),
            std::make_shared<DataTypeStruct>(DataTypes {variant, std::make_shared<DataTypeInt32>()},
                                             Strings {"v", "n"})};
    for (const auto& type : types) {
        auto column = type->create_column();
        column->insert_default();
        Block source({{std::move(column), type, "nested_v2"}});
        PBlock wire = serialize_block(source, segment_v2::SNAPPY);
        Block decoded;
        ASSERT_TRUE(deserialize_block(wire, decoded).ok());
        ASSERT_EQ(decoded.rows(), 1);
        EXPECT_EQ(decoded.get_by_position(0).column->compare_at(
                          0, 0, *source.get_by_position(0).column, 1),
                  0);
    }
}

} // namespace doris
