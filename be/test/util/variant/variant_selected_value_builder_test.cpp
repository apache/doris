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

#include "core/value/variant/variant_selected_value_builder.h"

#include <gtest/gtest.h>

#include <string_view>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_scalar.h"

namespace doris {
namespace {

StringRef string_ref(std::string_view value) {
    return {value.data(), value.size()};
}

const ColumnNullable& nullable_of(const ColumnPtr& result) {
    return assert_cast<const ColumnNullable&>(*result);
}

const ColumnVariantV2& variant_of(const ColumnPtr& result) {
    return assert_cast<const ColumnVariantV2&>(nullable_of(result).get_nested_column());
}

// A result may be typed or encoded. Copying it into a fresh encoded column yields the canonical
// bytes either way, which is what every consumer that reads Variant values observes.
ColumnVariantV2::MutablePtr canonical(const ColumnPtr& result) {
    const ColumnVariantV2& values = variant_of(result);
    auto copy = ColumnVariantV2::create();
    copy->insert_range_from(values, 0, values.size());
    return copy;
}

// Builds one Variant row per callback so tests can hand real VariantRef values to the builder.
class Rows {
public:
    template <typename Callback>
    void add(Callback&& callback) {
        auto row = _builder.begin_row();
        callback(row);
        row.finish();
    }

    void finish() { _batch = _builder.finish_batch(); }

    VariantRef at(size_t row) const { return _batch.value_at(row); }

private:
    VariantBatchBuilder _builder;
    VariantBatchBuilder _batch;
};

TEST(VariantSelectedValueBuilderTest, HomogeneousStringsProduceOneTypedColumn) {
    Rows rows;
    rows.add([](auto& row) { row.add_string(string_ref("alpha")); });
    rows.add([](auto& row) { row.add_string(string_ref("beta")); });
    rows.finish();

    VariantSelectedValueBuilder builder(3);
    builder.append_selected(rows.at(0));
    builder.append_missing();
    builder.append_selected(rows.at(1));
    ColumnPtr result = builder.finish();

    EXPECT_FALSE(builder.degraded());
    EXPECT_EQ(builder.typed_identity(), TYPE_STRING);
    ASSERT_EQ(result->size(), 3);
    EXPECT_TRUE(variant_of(result).is_typed());
    EXPECT_EQ(nullable_of(result).get_null_map_data()[0], 0);
    EXPECT_EQ(nullable_of(result).get_null_map_data()[1], 1);
    EXPECT_EQ(nullable_of(result).get_null_map_data()[2], 0);

    auto values = canonical(result);
    EXPECT_EQ(values->get_value_ref(0).get_string(), string_ref("alpha"));
    EXPECT_TRUE(values->get_value_ref(1).is_null());
    EXPECT_EQ(values->get_value_ref(2).get_string(), string_ref("beta"));
}

TEST(VariantSelectedValueBuilderTest, VariantNullStaysSqlNonNullAndDoesNotDecideTheIdentity) {
    Rows rows;
    rows.add([](auto& row) { row.add_null(); });
    rows.add([](auto& row) { row.add_string(string_ref("late")); });
    rows.finish();

    VariantSelectedValueBuilder builder(3);
    // A leading Variant null must not pin the identity, and the row it produces stays SQL
    // non-NULL so a STRING cast still renders it as the 'null' text.
    builder.append_selected(rows.at(0));
    builder.append_missing();
    builder.append_selected(rows.at(1));
    ColumnPtr result = builder.finish();

    EXPECT_FALSE(builder.degraded());
    EXPECT_EQ(builder.typed_identity(), TYPE_STRING);
    EXPECT_EQ(nullable_of(result).get_null_map_data()[0], 0);
    EXPECT_EQ(nullable_of(result).get_null_map_data()[1], 1);
    EXPECT_EQ(nullable_of(result).get_null_map_data()[2], 0);

    auto values = canonical(result);
    EXPECT_TRUE(values->get_value_ref(0).is_null());
    EXPECT_EQ(values->get_value_ref(2).get_string(), string_ref("late"));
}

TEST(VariantSelectedValueBuilderTest, MixedScalarKindsDegradeAndReplayEarlierRows) {
    Rows rows;
    rows.add([](auto& row) { row.add_string(string_ref("text")); });
    rows.add([](auto& row) { row.add_null(); });
    rows.add([](auto& row) { row.add_int(42); });
    rows.finish();

    VariantSelectedValueBuilder builder(4);
    builder.append_selected(rows.at(0));
    builder.append_selected(rows.at(1));
    builder.append_missing();
    builder.append_selected(rows.at(2));
    ColumnPtr result = builder.finish();

    EXPECT_TRUE(builder.degraded());
    EXPECT_EQ(builder.typed_identity(), INVALID_TYPE);
    EXPECT_FALSE(variant_of(result).is_typed());
    ASSERT_EQ(result->size(), 4);

    auto values = canonical(result);
    EXPECT_EQ(values->get_value_ref(0).get_string(), string_ref("text"));
    EXPECT_TRUE(values->get_value_ref(1).is_null());
    EXPECT_EQ(nullable_of(result).get_null_map_data()[2], 1);
    EXPECT_EQ(values->get_value_ref(3).get_int(), 42);
}

TEST(VariantSelectedValueBuilderTest, ContainerSelectionDegrades) {
    Rows rows;
    rows.add([](auto& row) {
        auto object = row.start_object();
        object.add_key(string_ref("inner"));
        row.add_int(5);
        object.finish();
    });
    rows.finish();

    VariantSelectedValueBuilder builder(1);
    builder.append_selected(rows.at(0));
    ColumnPtr result = builder.finish();

    EXPECT_TRUE(builder.degraded());
    EXPECT_FALSE(variant_of(result).is_typed());
    auto values = canonical(result);
    EXPECT_EQ(values->get_value_ref(0).basic_type(), VariantBasicType::OBJECT);
    VariantRef inner;
    ASSERT_TRUE(values->get_value_ref(0).object_find(string_ref("inner"), &inner));
    EXPECT_EQ(inner.get_int(), 5);
}

TEST(VariantSelectedValueBuilderTest, NarrowestIntegersOfMixedWidthShareOneTypedColumn) {
    Rows rows;
    rows.add([](auto& row) { row.add_int(7); });
    rows.add([](auto& row) { row.add_int(300); });
    rows.add([](auto& row) { row.add_int(100000); });
    rows.finish();
    ASSERT_EQ(rows.at(0).primitive_id(), VariantPrimitiveId::INT8);
    ASSERT_EQ(rows.at(1).primitive_id(), VariantPrimitiveId::INT16);
    ASSERT_EQ(rows.at(2).primitive_id(), VariantPrimitiveId::INT32);

    VariantSelectedValueBuilder builder(3);
    for (size_t row = 0; row < 3; ++row) {
        builder.append_selected(rows.at(row));
    }
    ColumnPtr result = builder.finish();

    EXPECT_FALSE(builder.degraded());
    EXPECT_EQ(builder.typed_identity(), TYPE_BIGINT);

    // A typed BIGINT column re-encodes through the narrowest width, so every physical integer
    // identity the source used must come back unchanged.
    auto values = canonical(result);
    EXPECT_EQ(values->get_value_ref(0).primitive_id(), VariantPrimitiveId::INT8);
    EXPECT_EQ(values->get_value_ref(1).primitive_id(), VariantPrimitiveId::INT16);
    EXPECT_EQ(values->get_value_ref(2).primitive_id(), VariantPrimitiveId::INT32);
    EXPECT_EQ(values->get_value_ref(0).get_int(), 7);
    EXPECT_EQ(values->get_value_ref(1).get_int(), 300);
    EXPECT_EQ(values->get_value_ref(2).get_int(), 100000);
}

TEST(VariantSelectedValueBuilderTest, IntegerWiderThanItsValueDegrades) {
    Rows rows;
    // Spec-legal but non-narrowest: a typed BIGINT column would republish this as INT8 and change
    // what variant_type() reports, so the whole batch must stay on canonical encoded rows.
    rows.add([](auto& row) { row.add_scalar(VariantScalarRef::integer(5, 8)); });
    rows.finish();
    ASSERT_EQ(rows.at(0).primitive_id(), VariantPrimitiveId::INT64);

    VariantSelectedValueBuilder builder(1);
    builder.append_selected(rows.at(0));
    ColumnPtr result = builder.finish();

    EXPECT_TRUE(builder.degraded());
    auto values = canonical(result);
    EXPECT_EQ(values->get_value_ref(0).primitive_id(), VariantPrimitiveId::INT64);
    EXPECT_EQ(values->get_value_ref(0).get_int(), 5);
}

TEST(VariantSelectedValueBuilderTest, BooleanAndFloatingIdentitiesSurviveTheTypedRoundTrip) {
    Rows booleans;
    booleans.add([](auto& row) { row.add_bool(true); });
    booleans.add([](auto& row) { row.add_bool(false); });
    booleans.finish();
    VariantSelectedValueBuilder boolean_builder(2);
    boolean_builder.append_selected(booleans.at(0));
    boolean_builder.append_selected(booleans.at(1));
    ColumnPtr boolean_result = boolean_builder.finish();
    EXPECT_EQ(boolean_builder.typed_identity(), TYPE_BOOLEAN);
    auto boolean_values = canonical(boolean_result);
    EXPECT_TRUE(boolean_values->get_value_ref(0).get_bool());
    EXPECT_FALSE(boolean_values->get_value_ref(1).get_bool());

    Rows floats;
    floats.add([](auto& row) { row.add_float(1.5F); });
    floats.finish();
    VariantSelectedValueBuilder float_builder(1);
    float_builder.append_selected(floats.at(0));
    ColumnPtr float_result = float_builder.finish();
    EXPECT_EQ(float_builder.typed_identity(), TYPE_FLOAT);
    auto float_values = canonical(float_result);
    EXPECT_EQ(float_values->get_value_ref(0).primitive_id(), VariantPrimitiveId::FLOAT);
    EXPECT_FLOAT_EQ(float_values->get_value_ref(0).get_float(), 1.5F);

    Rows doubles;
    doubles.add([](auto& row) { row.add_double(2.25); });
    doubles.finish();
    VariantSelectedValueBuilder double_builder(1);
    double_builder.append_selected(doubles.at(0));
    ColumnPtr double_result = double_builder.finish();
    EXPECT_EQ(double_builder.typed_identity(), TYPE_DOUBLE);
    auto double_values = canonical(double_result);
    EXPECT_EQ(double_values->get_value_ref(0).primitive_id(), VariantPrimitiveId::DOUBLE);
    EXPECT_DOUBLE_EQ(double_values->get_value_ref(0).get_double(), 2.25);
}

TEST(VariantSelectedValueBuilderTest, DecimalAndTemporalKindsStayEncoded) {
    Rows rows;
    rows.add([](auto& row) { row.add_decimal(12345, 2); });
    rows.finish();

    VariantSelectedValueBuilder builder(1);
    builder.append_selected(rows.at(0));
    ColumnPtr result = builder.finish();

    EXPECT_TRUE(builder.degraded());
    auto values = canonical(result);
    EXPECT_EQ(values->get_value_ref(0).get_decimal().unscaled, 12345);
    EXPECT_EQ(values->get_value_ref(0).get_decimal().scale, 2);
}

TEST(VariantSelectedValueBuilderTest, BatchWithoutAnyScalarProducesEncodedNulls) {
    VariantSelectedValueBuilder builder(2);
    builder.append_missing();
    builder.append_missing();
    ColumnPtr result = builder.finish();

    EXPECT_TRUE(builder.degraded());
    EXPECT_EQ(builder.typed_identity(), INVALID_TYPE);
    ASSERT_EQ(result->size(), 2);
    EXPECT_FALSE(variant_of(result).is_typed());
    EXPECT_EQ(nullable_of(result).get_null_map_data()[0], 1);
    EXPECT_EQ(nullable_of(result).get_null_map_data()[1], 1);
}

TEST(VariantSelectedValueBuilderTest, EmptyBatchProducesEmptyColumn) {
    VariantSelectedValueBuilder builder(0);
    ColumnPtr result = builder.finish();
    EXPECT_EQ(result->size(), 0);
}

} // namespace
} // namespace doris
