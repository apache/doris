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

#include <gtest/gtest.h>
#include <sys/mman.h>

#include <cstdlib>
#include <cstring>

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_map.h"
#include "core/column/column_struct.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/file_value.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/array/function_array_element.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/table_function/table_function_factory.h"
#include "exprs/table_function/vexplode_file.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_document.h"
#include "util/jsonb_utils.h"

namespace doris {

class FileExprTest : public testing::Test {
protected:
    const DataTypePtr text = std::make_shared<DataTypeString>();
    const DataTypePtr binary = std::make_shared<DataTypeVarbinary>();
    const DataTypePtr bigint = std::make_shared<DataTypeInt64>();
    const DataTypePtr file_type = std::make_shared<DataTypeFile>();

    static Field string(const std::string& value) {
        return Field::create_field<TYPE_STRING>(value);
    }
    static Field integer(Int64 value) { return Field::create_field<TYPE_BIGINT>(value); }
    static Field structure(std::initializer_list<Field> fields) {
        return Field::create_field<TYPE_STRUCT>(Struct(fields));
    }
    static Field public_value(const std::string& uri, Field size = Field()) {
        return structure({string(uri), Field(), std::move(size), Field(), Field(), Field()});
    }
    static Field file_value(const std::string& uri = "s3://bucket/a%2Fb?versionId=AbC") {
        File value(6);
        value[0] = string(uri);
        return Field::create_field<TYPE_FILE>(std::move(value));
    }

    static ColumnWithTypeAndName column(const DataTypePtr& type,
                                        std::initializer_list<Field> values) {
        auto result = type->create_column();
        for (const auto& value : values) result->insert(value);
        return {std::move(result), type, "input"};
    }

    static Status cast(const ColumnWithTypeAndName& input, const DataTypePtr& target, bool strict,
                       ColumnPtr& output) {
        FunctionContext context;
        context.set_enable_strict_mode(strict);
        Block block {input, {nullptr, target, "output"}};
        auto wrapper = get_cast_wrapper(&context, input.type, target);
        RETURN_IF_ERROR(wrapper(&context, block, {0}, 1, input.column->size(), nullptr));
        output = block.get_by_position(1).column;
        return Status::OK();
    }

    static ColumnPtr selector(const std::string& name, size_t rows = 1) {
        auto nested = ColumnString::create();
        nested->insert_data(name.data(), name.size());
        return ColumnConst::create(std::move(nested), rows);
    }

    static Field json(const std::string& input) {
        JsonBinaryValue value;
        auto status = value.from_json_string(input);
        EXPECT_TRUE(status.ok()) << status;
        return Field::create_field<TYPE_JSONB>(JsonbField(value.value(), value.size()));
    }

    DataTypePtr public_struct() const {
        const auto& file = assert_cast<const DataTypeFile&>(*file_type);
        return std::make_shared<DataTypeStruct>(file.get_elements(), file.get_element_names());
    }

    static void check_constant_inline_row(const IColumn& output, size_t row, const Field& payload,
                                          bool parent_null) {
        EXPECT_EQ(output.is_null_at(row), parent_null || payload.is_null());
        if (!parent_null && !payload.is_null()) {
            EXPECT_EQ(output[row].get<TYPE_VARBINARY>().str(), payload.get<TYPE_VARBINARY>().str());
        }
    }

    void check_constant_metadata_projection(const ColumnPtr& file, size_t rows,
                                            const std::string& expected_uri) const {
        const auto nullable_file = make_nullable(file_type);
        ColumnsWithTypeAndName args {{file, nullable_file, "file"},
                                     {selector("uri", rows), text, "field"}};
        auto getter = SimpleFunctionFactory::instance().get_function("element_at", args,
                                                                     make_nullable(text));
        ASSERT_NE(getter, nullptr);
        Block projection {args[0], args[1], {nullptr, make_nullable(text), "uri"}};
        ASSERT_TRUE(getter->execute(nullptr, projection, {0, 1}, 2, rows).ok());
        const auto& uri = projection.get_by_position(2).column;
        ASSERT_TRUE(is_column_const(*uri));
        EXPECT_EQ((*uri)[rows - 1].get<TYPE_STRING>(), expected_uri);
    }

    void check_constant_inline_getter(const Field& payload, bool parent_null) const {
        auto value = file_value();
        value.get<TYPE_FILE>()[5] = payload;
        auto input =
                make_nullable(file_type)->create_column_const(4, parent_null ? Field() : value);
        Block block {{input, make_nullable(file_type), "file"},
                     {nullptr, make_nullable(binary), "output"}};
        auto function = SimpleFunctionFactory::instance().get_function(
                "fl_get_binary", {block.get_by_position(0)}, make_nullable(binary));
        ASSERT_NE(function, nullptr);
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 4).ok());
        const auto& output = block.get_by_position(1).column;
        ASSERT_EQ(output->size(), 4);
        ASSERT_TRUE(is_column_const(*output));
        for (size_t row = 0; row < 4; ++row) {
            check_constant_inline_row(*output, row, payload, parent_null);
        }
    }
};

TEST_F(FileExprTest, InlineGetterPreservesBinary) {
    const std::string bytes("\0\xff\x80", 3);
    auto value = file_value("urn:binary");
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    auto empty = file_value("urn:empty");
    empty.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView());
    const auto input =
            column(make_nullable(file_type), {value, empty, file_value("urn:missing"), Field()});
    auto function = SimpleFunctionFactory::instance().get_function("fl_get_binary", {input},
                                                                   make_nullable(binary));
    ASSERT_NE(function, nullptr);
    EXPECT_EQ(remove_nullable(function->get_return_type())->get_primitive_type(), TYPE_VARBINARY);
    Block block {input, {nullptr, make_nullable(binary), "output"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 4).ok());
    const auto& output = block.get_by_position(1).column;
    EXPECT_EQ((*output)[0].get<TYPE_VARBINARY>().str(), bytes);
    EXPECT_FALSE(output->is_null_at(1));
    EXPECT_EQ((*output)[1].get<TYPE_VARBINARY>().size(), 0);
    EXPECT_TRUE(output->is_null_at(2));
    EXPECT_TRUE(output->is_null_at(3));
}

TEST_F(FileExprTest, BinaryGetterPreservesParentNullsAndSharesPayload) {
    std::string bytes(1 << 20, '\xff');
    bytes[0] = '\0';
    auto value = file_value();
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    auto values = make_nullable(file_type)->create_column();
    values->insert(value);
    values->insert(value);
    values->insert(value);
    auto& source = assert_cast<ColumnNullable&>(*values);
    source.get_null_map_data()[0] = 1;
    ColumnWithTypeAndName input {std::move(values), make_nullable(file_type), "input"};
    const auto& inline_column = assert_cast<const ColumnNullable&>(
            assert_cast<const ColumnFile&>(source.get_nested_column()).get_column(5));
    auto function = SimpleFunctionFactory::instance().get_function("fl_get_binary", {input},
                                                                   make_nullable(binary));
    ASSERT_NE(function, nullptr);
    Block block {input, {nullptr, make_nullable(binary), "output"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 3).ok());
    const auto& output = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_TRUE(output.is_null_at(0));
    EXPECT_FALSE(output.is_null_at(1));
    EXPECT_FALSE(output.is_null_at(2));
    EXPECT_EQ(output[1].get<TYPE_VARBINARY>().str(), bytes);
    EXPECT_EQ(output.get_nested_column_ptr(), inline_column.get_nested_column_ptr());
    EXPECT_FALSE(inline_column.is_null_at(0));
    EXPECT_FALSE(source.is_null_at(1));
}

TEST_F(FileExprTest, BinaryGetterInsertsNullForMissingInline) {
    const std::string bytes("\0\xff\x80", 3);
    auto value = file_value();
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    auto empty = file_value();
    empty.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView());
    auto values = file_type->create_column();
    values->insert(value);
    values->insert(value);
    values->insert(empty);
    auto& inline_column = assert_cast<ColumnNullable&>(
            assert_cast<ColumnFile&>(*values).get_column(DataTypeFile::INLINE_FIELD_INDEX));
    inline_column.get_null_map_data()[1] = 1;
    ColumnWithTypeAndName input {std::move(values), file_type, "input"};
    auto function = SimpleFunctionFactory::instance().get_function("fl_get_binary", {input},
                                                                   make_nullable(binary));
    ASSERT_NE(function, nullptr);
    Block block {input, {nullptr, make_nullable(binary), "output"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 3).ok());
    const auto& output = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_EQ(output[0].get<TYPE_VARBINARY>().str(), bytes);
    EXPECT_TRUE(output.is_null_at(1));
    EXPECT_TRUE(output.get_nested_column()[1].get<TYPE_VARBINARY>().empty());
    EXPECT_FALSE(output.is_null_at(2));
    EXPECT_TRUE(output[2].get<TYPE_VARBINARY>().empty());
    EXPECT_EQ(inline_column.get_nested_column()[1].get<TYPE_VARBINARY>().str(), bytes);
}

TEST_F(FileExprTest, BinaryGetterHandlesEmptyColumns) {
    const auto input = column(file_type, {});
    auto function = SimpleFunctionFactory::instance().get_function("fl_get_binary", {input},
                                                                   make_nullable(binary));
    ASSERT_NE(function, nullptr);
    Block block {input, {nullptr, make_nullable(binary), "output"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 0).ok());
    EXPECT_EQ(block.get_by_position(1).column->size(), 0);
}

TEST_F(FileExprTest, CastRejectsScalarInputsAndFileToString) {
    for (const auto& input : {column(text, {string("s3://b/key")}), column(bigint, {integer(1)})}) {
        for (bool strict : {false, true}) {
            ColumnPtr output;
            EXPECT_FALSE(cast(input, make_nullable(file_type), strict, output).ok());
        }
    }
    for (bool strict : {false, true}) {
        ColumnPtr output;
        EXPECT_FALSE(
                cast(column(file_type, {file_value()}), make_nullable(text), strict, output).ok());
        const auto array_type = std::make_shared<DataTypeArray>(make_nullable(file_type));
        EXPECT_FALSE(
                cast(column(array_type, {Field::create_field<TYPE_ARRAY>(Array {file_value()})}),
                     make_nullable(text), strict, output)
                        .ok());
    }
}

TEST_F(FileExprTest, VariantCastsAreUnsupported) {
    for (const auto& variant_type :
         DataTypes {make_nullable(std::make_shared<DataTypeVariant>()),
                    make_nullable(std::make_shared<DataTypeVariantV2>())}) {
        const auto json_type = make_nullable(std::make_shared<DataTypeJsonb>());
        const std::string bytes("a\0\xff", 3);
        auto value = file_value("s3://b/variant");
        value.get<TYPE_FILE>()[1] = integer(0);
        value.get<TYPE_FILE>()[2] = integer(3);
        value.get<TYPE_FILE>()[3] = string("application/octet-stream");
        value.get<TYPE_FILE>()[4] = string("ETAG:opaque");
        value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
        const auto input = column(make_nullable(file_type), {value, Field()});
        ColumnPtr variant;
        const auto json_input = column(
                json_type,
                {json(R"({"uri":"s3://b/variant","offset":0,"size":3,"content_type":null,"checksum":null,"inline":null})")});
        ASSERT_TRUE(cast(json_input, variant_type, true, variant).ok());
        for (bool strict : {false, true}) {
            ColumnPtr output;
            EXPECT_FALSE(cast(input, variant_type, strict, output).ok());
            EXPECT_FALSE(cast({variant, variant_type, "variant"}, make_nullable(file_type), strict,
                              output)
                                 .ok());
        }
        EXPECT_EQ((*input.column)[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().str(), bytes);
    }
}

TEST_F(FileExprTest, VariantCastsRejectAllInputValues) {
    const auto json_type = make_nullable(std::make_shared<DataTypeJsonb>());
    const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    const auto input = column(
            json_type,
            {json(R"({"checksum":null,"content_type":null,"size":4,"offset":0,"uri":"s3://b/variant","inline":null})"),
             json("null"), Field(), json("[]"), json("1"), json(R"("s3://b/variant")"),
             json(R"({"offset":null,"size":null,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/variant","size":null,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/variant","offset":null,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/variant","offset":null,"size":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/variant","offset":null,"size":null,"content_type":null})"),
             json(R"({"uri":"s3://b/variant","offset":null,"size":null,"content_type":null,"checksum":null,"inline":"AA"})"),
             json(R"({"uri":"s3://b/variant","offset":null,"size":"4","content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/variant","offset":null,"size":null,"content_type":false,"checksum":null,"inline":null})"),
             json(R"({"uri":null,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":null})")});
    ColumnPtr variant;
    ASSERT_TRUE(cast(input, variant_type, true, variant).ok());
    ColumnWithTypeAndName source {variant, variant_type, "variant"};
    ColumnPtr output;
    EXPECT_FALSE(cast(source, make_nullable(file_type), false, output).ok());
    for (bool strict : {false, true}) {
        for (size_t row = 0; row < variant->size(); ++row) {
            EXPECT_FALSE(cast({variant->cut(row, 1), variant_type, "variant"},
                              make_nullable(file_type), strict, output)
                                 .ok());
        }
    }
}

TEST_F(FileExprTest, JsonCastsAreUnsupported) {
    const auto json_type = std::make_shared<DataTypeJsonb>();
    auto input = column(
            json_type,
            {json(R"({"uri":"s3://b/key","offset":null,"size":0,"content_type":null,"checksum":null,"inline":null})"),
             json("null"),
             json(R"({"uri":"s3://b/key","offset":null,"size":null,"content_type":null,"checksum":null,"inline":"AA"})"),
             json(R"({"uri":"s3://b/key","offset":1,"size":null,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/key","offset":null,"size":1.5,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/key","offset":null,"size":null,"content_type":null,"checksum":"MD5:ABCDEF0123456789ABCDEF0123456789","inline":null})")});
    ColumnPtr output;
    for (bool strict : {false, true}) {
        for (size_t row = 0; row < input.column->size(); ++row) {
            EXPECT_FALSE(cast({input.column->cut(row, 1), json_type, "input"},
                              make_nullable(file_type), strict, output)
                                 .ok());
        }
        EXPECT_FALSE(cast(column(file_type, {file_value()}), json_type, strict, output).ok());
    }
}

TEST_F(FileExprTest, ToJsonExposesExactlySixFields) {
    const std::string bytes("\0\xff", 2);
    auto value = file_value("s3://b/key");
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(
            StringView(bytes.data(), cast_set<uint32_t>(bytes.size())));
    const auto input = column(make_nullable(file_type), {value, Field()});
    ColumnPtr output;

    const auto expected =
            R"({"uri":"s3://b/key","offset":null,"size":null,"content_type":null,"checksum":null,"inline":"AP8="})";
    const auto json_type = make_nullable(std::make_shared<DataTypeJsonb>());
    RuntimeState runtime;
    auto context = FunctionContext::create_context(&runtime, json_type, {input.type});
    auto function = SimpleFunctionFactory::instance().get_function("to_json", {input}, json_type);
    ASSERT_NE(function, nullptr);
    Block block {input, {nullptr, json_type, "json"}};
    ASSERT_TRUE(function->execute(context.get(), block, {0}, 1, input.column->size()).ok());
    output = block.get_by_position(1).column;
    const auto& json_column = assert_cast<const ColumnNullable&>(*output).get_nested_column();
    const auto encoded = json_column.get_data_at(0);
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(encoded.data, encoded.size), expected);
    EXPECT_TRUE(output->is_null_at(1));
    // SQL output must preserve the source bytes.
    EXPECT_EQ((*input.column)[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().size(), bytes.size());
}

TEST_F(FileExprTest, FileToStructCastIsUnsupported) {
    const auto& file = assert_cast<const DataTypeFile&>(*file_type);
    ColumnPtr output;
    auto source_value = file_value("s3://b/key");
    source_value.get<TYPE_FILE>()[1] = integer(3);
    source_value.get<TYPE_FILE>()[2] = integer(12);
    const std::string bytes("\0\xff", 2);
    source_value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    const auto input = column(file_type, {source_value});
    auto six = std::make_shared<DataTypeStruct>(file.get_elements(), file.get_element_names());
    EXPECT_FALSE(cast(input, six, true, output).ok());
    EXPECT_FALSE(cast(input, six, false, output).ok());
    auto reordered = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(bigint), make_nullable(text), make_nullable(bigint),
                       make_nullable(text), make_nullable(text), make_nullable(binary)},
            Strings {"SIZE", "UrI", "offset", "content_type", "checksum", "inline"});
    EXPECT_FALSE(cast(input, reordered, true, output).ok());
    auto nonnullable = std::make_shared<DataTypeStruct>(
            DataTypes {text, make_nullable(bigint), make_nullable(bigint), make_nullable(text),
                       make_nullable(text), make_nullable(binary)},
            Strings {"uri", "offset", "size", "content_type", "checksum", "inline"});
    EXPECT_FALSE(cast(input, nonnullable, true, output).ok());
}

TEST_F(FileExprTest, CastRejectsStructInputsAndNestedFileTypes) {
    auto source_file = make_nullable(public_struct());
    auto target_file = make_nullable(file_type);
    auto good = public_value("s3://b/key");
    ColumnPtr output;
    EXPECT_FALSE(cast(column(source_file, {good}), target_file, true, output).ok());
    auto array_source = std::make_shared<DataTypeArray>(source_file);
    auto array_target = std::make_shared<DataTypeArray>(target_file);
    const auto valid_array = Field::create_field<TYPE_ARRAY>(Array {good, Field()});
    EXPECT_FALSE(cast(column(array_source, {valid_array}), array_target, false, output).ok());

    auto struct_source = std::make_shared<DataTypeStruct>(DataTypes {source_file}, Strings {"f"});
    auto struct_target = std::make_shared<DataTypeStruct>(DataTypes {target_file}, Strings {"f"});
    EXPECT_FALSE(
            cast(column(struct_source, {structure({good})}), struct_target, true, output).ok());

    auto map_source = std::make_shared<DataTypeMap>(make_nullable(bigint), source_file);
    auto map_target = std::make_shared<DataTypeMap>(make_nullable(bigint), target_file);
    auto map_value =
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {integer(1)}),
                                               Field::create_field<TYPE_ARRAY>(Array {good})});
    EXPECT_FALSE(cast(column(map_source, {map_value}), map_target, true, output).ok());
}

TEST_F(FileExprTest, GetterUsesConstantPublicNamesAndMergesNullsWithoutMutation) {
    auto value = file_value();
    value.get<TYPE_FILE>()[2] = integer(7);
    auto input = column(make_nullable(file_type), {value, Field(), file_value()});
    ColumnsWithTypeAndName args {input, {selector("SiZe", 3), text, "selector"}};
    auto function = SimpleFunctionFactory::instance().get_function("element_at", args,
                                                                   make_nullable(bigint));
    ASSERT_NE(function, nullptr);
    EXPECT_TRUE(function->get_return_type()->is_nullable());
    Block block {args[0], args[1], {nullptr, make_nullable(bigint), "output"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0, 1}, 2, 3).ok());
    auto output = block.get_by_position(2).column;
    ASSERT_EQ(output->size(), 3);
    EXPECT_EQ((*output)[0].get<TYPE_BIGINT>(), 7);
    EXPECT_TRUE(output->is_null_at(1));
    EXPECT_TRUE(output->is_null_at(2));
    auto changed = IColumn::mutate(output);
    changed->clear();
    EXPECT_EQ((*input.column)[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 7);
}

TEST_F(FileExprTest, GetterRejectsDynamicNumericNullAndUnknownSelectors) {
    FunctionArrayElement function;
    auto bad_selectors = std::vector<ColumnWithTypeAndName> {
            {selector("unknown"), text, "selector"},
            column(text, {string("uri")}),
            column(bigint, {integer(1)}),
            {make_nullable(text)->create_column_const_with_default_value(1), make_nullable(text),
             "null selector"}};
    for (const auto& index : bad_selectors) {
        ColumnsWithTypeAndName args {{nullptr, file_type, "file"}, index};
        EXPECT_THROW(function.get_return_type_impl(args), Exception);
    }
}

TEST_F(FileExprTest, JsonCastRejectsMissingDuplicateUnknownWrongTypeAndOutOfRangeFields) {
    const auto type = std::make_shared<DataTypeJsonb>();
    const auto input = column(
            type,
            {json(R"({"uri":"s3://b/a"})"),
             json(R"({"uri":"s3://b/a","offset":null,"size":null,"content_type":null})"),
             json(R"({"uri":"s3://b/a","uri":"s3://b/b","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"URI":"s3://b/a","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/a","offset":null,"size":9223372036854775808,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/a","offset":9223372036854775807,"size":1,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/a","offset":null,"size":null,"content_type":null,"checksum":"md5:0123456789abcdef0123456789abcdef","inline":null})"),
             json(R"({"uri":"s3://b/a","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null,"other":null})"),
             json(R"({"uri":1,"offset":null,"size":null,"content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/a","offset":null,"size":"12","content_type":null,"checksum":null,"inline":null})"),
             json(R"({"uri":"s3://b/a","offset":null,"size":null,"content_type":false,"checksum":null,"inline":null})"),
             json("[]"), json("1")});
    ColumnPtr output;
    EXPECT_FALSE(cast(input, make_nullable(file_type), false, output).ok());
    for (size_t row = 0; row < input.column->size(); ++row) {
        EXPECT_FALSE(cast({input.column->cut(row, 1), type, "input"}, make_nullable(file_type),
                          true, output)
                             .ok())
                << row;
    }
}

TEST_F(FileExprTest, GetterSupportsConstantFileAndConstantNullableSelector) {
    auto input = file_type->create_column_const(4, file_value("s3://b/key"));
    auto name = make_nullable(text)->create_column_const(4, string("URI"));
    ColumnsWithTypeAndName args {{input, file_type, "file"}, {name, make_nullable(text), "field"}};
    auto function =
            SimpleFunctionFactory::instance().get_function("element_at", args, make_nullable(text));
    ASSERT_NE(function, nullptr);
    Block block {args[0], args[1], {nullptr, make_nullable(text), "output"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0, 1}, 2, 4).ok());
    const auto output = block.get_by_position(2).column;
    ASSERT_EQ(output->size(), 4);
    for (size_t row = 0; row < 4; ++row) EXPECT_EQ((*output)[row].get<TYPE_STRING>(), "s3://b/key");
}

namespace {
void project_constant_fields_with_protected_payload() {
    constexpr size_t payload_size = 1 << 20;
    constexpr size_t rows = 4096;
    auto* bytes = static_cast<char*>(mmap(nullptr, payload_size, PROT_READ | PROT_WRITE,
                                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (bytes == MAP_FAILED) {
        std::_Exit(1);
    }
    std::memset(bytes, '\xff', payload_size);
    const auto type = std::make_shared<DataTypeFile>();
    auto column = type->create_column();
    column->insert(Field::create_field<TYPE_FILE>(
            File {Field::create_field<TYPE_STRING>("urn:constant"), Field(),
                  Field::create_field<TYPE_BIGINT>(7), Field(), Field(), Field()}));
    auto& inline_column =
            assert_cast<ColumnNullable&>(assert_cast<ColumnFile&>(*column).get_column(5));
    auto& payloads = assert_cast<ColumnVarbinary&>(inline_column.get_nested_column()).get_data();
    payloads[0] = StringView(bytes, static_cast<uint32_t>(payload_size));
    inline_column.get_null_map_data()[0] = 0;
    if (mprotect(bytes, payload_size, PROT_NONE) != 0) {
        std::_Exit(2);
    }
    const auto& schema = *type;
    const auto text = std::make_shared<DataTypeString>();
    const ColumnPtr source = std::move(column);
    FunctionArrayElement function;
    Columns struct_fields;
    for (size_t child = 0; child < DataTypeFile::FIELD_COUNT; ++child) {
        struct_fields.push_back(assert_cast<const ColumnFile&>(*source).get_column_ptr(child));
    }
    const auto struct_type =
            std::make_shared<DataTypeStruct>(schema.get_elements(), schema.get_element_names());
    const ColumnPtr struct_source = ColumnStruct::create(struct_fields);
    for (bool is_struct : {false, true}) {
        const ColumnPtr field_source = is_struct ? struct_source : source;
        const DataTypePtr field_type = is_struct ? DataTypePtr(struct_type) : DataTypePtr(type);
        for (int parent = 0; parent < 3; ++parent) {
            ColumnPtr input = field_source;
            DataTypePtr input_type = field_type;
            if (parent != 0) {
                input = ColumnNullable::create(input, ColumnUInt8::create(1, parent == 2));
                input_type = make_nullable(field_type);
            }
            input = ColumnConst::create(input, rows);
            for (size_t index : {size_t {0}, size_t {2}, size_t {4}}) {
                const auto& name = schema.get_element_names()[index];
                auto field_name = ColumnString::create();
                field_name->insert_data(name.data(), name.size());
                Block block {{input, input_type, "file"},
                             {ColumnConst::create(std::move(field_name), rows), text, "field"},
                             {nullptr, schema.get_element(index), "output"}};
                if (!function.execute_impl(nullptr, block, {0, 1}, 2, rows).ok()) {
                    std::_Exit(3);
                }
                const auto& output = block.get_by_position(2).column;
                if (output->size() != rows) {
                    std::_Exit(4);
                }
                for (size_t row = 0; row < rows; ++row) {
                    const bool expected_null = parent == 2 || index == 4;
                    if (output->is_null_at(row) != expected_null) {
                        std::_Exit(5);
                    }
                    if (!expected_null &&
                        ((index == 0 && output->get_data_at(row).to_string() != "urn:constant") ||
                         (index == 2 && (*output)[row].get<TYPE_BIGINT>() != 7))) {
                        std::_Exit(6);
                    }
                }
            }
        }
    }
    if (munmap(bytes, payload_size) != 0) {
        std::_Exit(7);
    }
    std::_Exit(0);
}
} // namespace

TEST(FileExprDeathTest, ConstantFieldAccessNeverReadsUnselectedBinary) {
    // Both FILE and STRUCT retain a protected 1 MiB binary field while extracting metadata.
    // Any read or copy of the unselected binary payload faults immediately.
    ASSERT_EXIT(project_constant_fields_with_protected_payload(), ::testing::ExitedWithCode(0), "");
}

TEST_F(FileExprTest, ConstantInlineGetterPreservesBinaryAndNulls) {
    const std::string bytes("\0\xff\x80", 3);
    for (const auto& payload : {Field(), Field::create_field<TYPE_VARBINARY>(StringView()),
                                Field::create_field<TYPE_VARBINARY>(StringView(bytes))}) {
        for (bool parent_null : {false, true}) {
            check_constant_inline_getter(payload, parent_null);
        }
    }
}

TEST_F(FileExprTest, BinaryGetterSharesStoredPayloadAndDetachesBeforeMutation) {
    const std::string bytes(128 * 1024, '\xff');
    auto value = file_value("urn:binary");
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    auto input = column(file_type, {value, value});
    const auto& source = assert_cast<const ColumnFile&>(*input.column);
    auto function = SimpleFunctionFactory::instance().get_function("fl_get_binary", {input},
                                                                   make_nullable(binary));
    ASSERT_NE(function, nullptr);
    Block block {input, {nullptr, make_nullable(binary), "binary"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 2).ok());
    const auto output = block.get_by_position(1).column;
    EXPECT_EQ(output.get(), source.get_column_ptr(DataTypeFile::INLINE_FIELD_INDEX).get());
    auto changed = IColumn::mutate(output);
    changed->clear();
    EXPECT_EQ(source.size(), 2);
    EXPECT_EQ(source.get_column(5)[1].get<TYPE_VARBINARY>().str(), bytes);
    block.clear();
    input.column = nullptr;
    EXPECT_EQ((*output)[0].get<TYPE_VARBINARY>().str(), bytes);
}

TEST_F(FileExprTest, ToJsonPreservesConstantFileAndNull) {
    const auto type = make_nullable(file_type);
    const auto json_type = make_nullable(std::make_shared<DataTypeJsonb>());
    auto value = file_value("urn:constant");
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView());
    for (bool is_null : {false, true}) {
        const ColumnWithTypeAndName input {type->create_column_const(3, is_null ? Field() : value),
                                           type, "input"};
        RuntimeState runtime;
        auto context = FunctionContext::create_context(&runtime, json_type, {type});
        auto function =
                SimpleFunctionFactory::instance().get_function("to_json", {input}, json_type);
        ASSERT_NE(function, nullptr);
        Block block {input, {nullptr, json_type, "json"}};
        ASSERT_TRUE(function->execute(context.get(), block, {0}, 1, 3).ok());
        const auto& output = block.get_by_position(1).column;
        ASSERT_EQ(output->size(), 3);
        for (size_t row = 0; row < 3; ++row) {
            EXPECT_EQ(output->is_null_at(row), is_null);
            if (!is_null) {
                const auto encoded = output->get_data_at(row);
                EXPECT_EQ(
                        JsonbToJson::jsonb_to_json_string(encoded.data, encoded.size),
                        R"({"uri":"urn:constant","offset":null,"size":null,"content_type":null,"checksum":null,"inline":""})");
            }
        }
    }
}

TEST_F(FileExprTest, ConstantContainersKeepSingleFileResultAndMetadataProjection) {
    constexpr size_t rows = 4096;
    const std::string bytes(1 << 20, '\xff');
    auto value = file_value("urn:constant-container");
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    const auto nullable_file = make_nullable(file_type);
    const auto array_type = std::make_shared<DataTypeArray>(nullable_file);
    const auto map_type = std::make_shared<DataTypeMap>(make_nullable(bigint), nullable_file);
    const auto struct_type =
            std::make_shared<DataTypeStruct>(DataTypes {nullable_file}, Strings {"asset"});
    const auto array = Field::create_field<TYPE_ARRAY>(Array {value});
    const auto map =
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {integer(1)}),
                                               Field::create_field<TYPE_ARRAY>(Array {value})});
    const std::vector<ColumnWithTypeAndName> inputs {
            {array_type->create_column_const(rows, array), array_type, "array"},
            {map_type->create_column_const(rows, map), map_type, "map"},
            {struct_type->create_column_const(rows, structure({value})), struct_type, "struct"}};
    for (const auto& input : inputs) {
        SCOPED_TRACE(input.name);
        auto key = input.type == struct_type ? selector("asset", rows)
                                             : bigint->create_column_const(rows, integer(1));
        const DataTypePtr key_type = input.type == struct_type ? text : bigint;
        ColumnsWithTypeAndName args {input, {key, key_type, "selector"}};
        auto function =
                SimpleFunctionFactory::instance().get_function("element_at", args, nullable_file);
        ASSERT_NE(function, nullptr);
        Block block {args[0], args[1], {nullptr, nullable_file, "file"}};
        ASSERT_TRUE(function->execute(nullptr, block, {0, 1}, 2, rows).ok());
        const auto& file = block.get_by_position(2).column;
        ASSERT_TRUE(is_column_const(*file));
        EXPECT_LT(file->allocated_bytes(), 4 * bytes.size());
        EXPECT_EQ(file->size(), rows);
        EXPECT_EQ((*file)[rows - 1].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().str(), bytes);
        check_constant_metadata_projection(file, rows, "urn:constant-container");
    }
}

TEST_F(FileExprTest, ConstantArrayAndMapStillAcceptDynamicSelectors) {
    constexpr size_t rows = 4;
    const auto nullable_file = make_nullable(file_type);
    const auto first = file_value("urn:first");
    const auto second = file_value("urn:second");
    const auto array_type = std::make_shared<DataTypeArray>(nullable_file);
    const auto map_type = std::make_shared<DataTypeMap>(make_nullable(bigint), nullable_file);
    const auto array = Field::create_field<TYPE_ARRAY>(Array {first, second});
    const auto map = Field::create_field<TYPE_MAP>(
            Map {Field::create_field<TYPE_ARRAY>(Array {integer(1), integer(2)}),
                 Field::create_field<TYPE_ARRAY>(Array {first, second})});
    const std::vector<ColumnWithTypeAndName> inputs {
            {array_type->create_column_const(rows, array), array_type, "array"},
            {map_type->create_column_const(rows, map), map_type, "map"}};
    for (const auto& input : inputs) {
        SCOPED_TRACE(input.name);
        ColumnsWithTypeAndName args {input, column(make_nullable(bigint),
                                                   {integer(2), integer(1), integer(3), Field()})};
        auto function =
                SimpleFunctionFactory::instance().get_function("element_at", args, nullable_file);
        ASSERT_NE(function, nullptr);
        Block block {args[0], args[1], {nullptr, nullable_file, "file"}};
        ASSERT_TRUE(function->execute(nullptr, block, {0, 1}, 2, rows).ok());
        const auto& output = block.get_by_position(2).column;
        EXPECT_FALSE(is_column_const(*output));
        EXPECT_EQ((*output)[0].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "urn:second");
        EXPECT_EQ((*output)[1].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "urn:first");
        EXPECT_TRUE(output->is_null_at(2));
        EXPECT_TRUE(output->is_null_at(3));
    }
}

TEST_F(FileExprTest, NestedFileToStructCastIsUnsupportedAndIdentityPreservesInline) {
    auto value = file_value("s3://b/key");
    const std::string bytes("a\0b", 3);
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    const auto input_type = std::make_shared<DataTypeArray>(make_nullable(file_type));
    const auto input =
            column(input_type, {Field::create_field<TYPE_ARRAY>(Array {value, Field()})});
    ColumnPtr output;
    ASSERT_TRUE(cast(input, input_type, true, output).ok());
    EXPECT_EQ((*output)[0].get<TYPE_ARRAY>()[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().size(),
              3);
    EXPECT_FALSE(cast(input, std::make_shared<DataTypeArray>(make_nullable(public_struct())), true,
                      output)
                         .ok());
}

TEST_F(FileExprTest, ExplodeFilePublicFieldsNullAndOuterRows) {
    auto value = file_value("s3://b/key");
    value.get<TYPE_FILE>()[2] = integer(17);
    const std::string bytes("\0\xff\x80", 3);
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    Block block {column(make_nullable(file_type), {value, Field(), file_value("s3://b/last")})};
    auto root = std::make_shared<VectorizedFnCall>();
    auto child = std::make_shared<VSlotRef>(0, 0, -1, make_nullable(file_type), "file");
    root->add_child(child);
    auto context = std::make_shared<VExprContext>(root);
    for (bool outer : {false, true}) {
        TFunction definition;
        definition.name.function_name = outer ? "explode_file_outer" : "explode_file";
        definition.binary_type = TFunctionBinaryType::BUILTIN;
        ObjectPool pool;
        TableFunction* function = nullptr;
        ASSERT_TRUE(TableFunctionFactory::get_fn(definition, &pool, &function, 0).ok());
        ASSERT_NE(dynamic_cast<VExplodeFileTableFunction*>(function), nullptr);
        EXPECT_EQ(function->is_outer(), outer);
        function->set_expr_context(context);
        function->set_nullable();
        ASSERT_TRUE(function->process_init(&block, nullptr).ok());
        auto output = make_nullable(public_struct())->create_column();
        function->process_row(0);
        EXPECT_FALSE(function->current_empty());
        EXPECT_EQ(function->get_value(output, 8), 1);
        EXPECT_TRUE(function->eos());
        EXPECT_EQ((*output)[0].get<TYPE_STRUCT>().size(), 6);
        EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[2].get<TYPE_BIGINT>(), 17);
        EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[5].get<TYPE_VARBINARY>().str(), bytes);
        function->process_row(1);
        EXPECT_TRUE(function->current_empty());
        EXPECT_EQ(function->get_value(output, 8), outer ? 1 : 0);
        if (outer) EXPECT_TRUE(output->is_null_at(1));
        function->process_row(2);
        EXPECT_EQ(function->get_value(output, 1), 1);
        EXPECT_EQ(output->size(), outer ? 3 : 2);
        EXPECT_EQ((*output)[output->size() - 1].get<TYPE_STRUCT>()[0].get<TYPE_STRING>(),
                  "s3://b/last");
        function->process_row(0);
        auto repeated = make_nullable(public_struct())->create_column();
        function->get_same_many_values(repeated, 3);
        ASSERT_EQ(repeated->size(), 3);
        EXPECT_EQ((*repeated)[2].get<TYPE_STRUCT>()[2].get<TYPE_BIGINT>(), 17);
        EXPECT_EQ((*repeated)[2].get<TYPE_STRUCT>()[5].get<TYPE_VARBINARY>().str(), bytes);
        function->process_close();
    }
}

TEST_F(FileExprTest, JsonCastsRejectNestedFileTypes) {
    const auto source = std::make_shared<DataTypeJsonb>();
    const auto arrays = column(
            source,
            {json(R"([{"uri":"s3://b/good","offset":null,"size":4,"content_type":null,"checksum":null,"inline":null},
                                                  {"uri":"s3://b/bad","offset":null,"size":-1,"content_type":null,"checksum":null,"inline":null},null])"),
             json("[]"), json("null")});
    const auto array_target =
            make_nullable(std::make_shared<DataTypeArray>(make_nullable(file_type)));
    const auto structs = column(
            source,
            {json(R"({"file":{"uri":"s3://b/bad","offset":null,"size":-1,"content_type":null,"checksum":null,"inline":null},"label":"kept"})"),
             json(R"({"file":{"uri":"s3://b/good","offset":null,"size":4,"content_type":null,"checksum":null,"inline":null},"label":"also"})")});
    const auto struct_target = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(file_type), make_nullable(text)}, Strings {"file", "label"}));
    for (bool strict : {false, true}) {
        SCOPED_TRACE(strict ? "strict" : "non-strict");
        ColumnPtr output;
        EXPECT_FALSE(cast(arrays, array_target, strict, output).ok());
        EXPECT_FALSE(cast(structs, struct_target, strict, output).ok());
        const auto files =
                column(array_target, {Field::create_field<TYPE_ARRAY>(Array {file_value()})});
        EXPECT_FALSE(cast(files, make_nullable(source), strict, output).ok());
    }
}

TEST_F(FileExprTest, JsonCastsKeepExistingNonFileBehavior) {
    const auto source = std::make_shared<DataTypeJsonb>();
    const auto target = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(text), make_nullable(bigint)}, Strings {"label", "number"}));
    const auto input = column(source, {json(R"({"label":"first","number":7})"),
                                       json(R"({"label":"second","number":8})")});
    ColumnPtr output;
    ASSERT_TRUE(cast(input, target, true, output).ok());
    ASSERT_EQ(output->size(), 2);
    EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[0].get<TYPE_STRING>(), "first");
    EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[1].get<TYPE_BIGINT>(), 7);
    EXPECT_EQ((*output)[1].get<TYPE_STRUCT>()[0].get<TYPE_STRING>(), "second");
    EXPECT_EQ((*output)[1].get<TYPE_STRUCT>()[1].get<TYPE_BIGINT>(), 8);
}

TEST_F(FileExprTest, ElementAtStillSupportsDynamicArraySelectors) {
    const auto array_type = std::make_shared<DataTypeArray>(make_nullable(bigint));
    auto array =
            column(array_type, {Field::create_field<TYPE_ARRAY>(Array {integer(10), integer(20)}),
                                Field::create_field<TYPE_ARRAY>(Array {integer(30), integer(40)})});
    auto indexes = column(bigint, {integer(2), integer(1)});
    auto function = SimpleFunctionFactory::instance().get_function("element_at", {array, indexes},
                                                                   make_nullable(bigint));
    ASSERT_NE(function, nullptr);
    Block block {array, indexes, {nullptr, make_nullable(bigint), "result"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0, 1}, 2, 2).ok());
    EXPECT_EQ((*block.get_by_position(2).column)[0].get<TYPE_BIGINT>(), 20);
    EXPECT_EQ((*block.get_by_position(2).column)[1].get<TYPE_BIGINT>(), 30);
}

TEST_F(FileExprTest, ReverseStructRejectsIntegerWideningAndNarrowing) {
    const auto& schema = assert_cast<const DataTypeFile&>(*file_type);
    auto value = file_value();
    value.get<TYPE_FILE>()[2] = integer(17);
    for (const auto& integer_type :
         DataTypes {std::make_shared<DataTypeInt128>(), std::make_shared<DataTypeInt32>()}) {
        auto types = schema.get_elements();
        types[2] = make_nullable(integer_type);
        auto target = std::make_shared<DataTypeStruct>(
                types, Strings {"uri", "offset", "size", "content_type", "checksum", "inline"});
        for (bool strict : {false, true}) {
            ColumnPtr output;
            EXPECT_FALSE(cast(column(file_type, {value}), target, strict, output).ok());
        }
    }
}

} // namespace doris
