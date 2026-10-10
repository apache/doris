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

#include <type_traits>

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_map.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/file_value.h"
#include "core/value/jsonb_value.h"
#include "exprs/function/array/function_array_element.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/mock_vexpr.h"
#include "exprs/table_function/table_function_factory.h"
#include "exprs/table_function/vexplode_file.h"
#include "exprs/vcast_expr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vfile_literal.h"
#include "exprs/vstruct_literal.h"
#include "runtime/descriptors.h"
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

    static Status try_cast(const ColumnWithTypeAndName& input, const DataTypePtr& target,
                           bool strict, ColumnPtr& output) {
        TryCastExpr expression;
        expression._data_type = target;
        expression._open_finished = true;
        expression._original_cast_return_is_nullable = true;
        expression._fn_context_index = 0;
        auto child = std::make_shared<MockVExpr>();
        child->_data_type = input.type;
        EXPECT_CALL(*child, expr_name()).WillRepeatedly(testing::ReturnRef(input.name));
        EXPECT_CALL(*child,
                    execute_column_impl(testing::_, testing::_, testing::_, testing::_, testing::_))
                .WillRepeatedly(testing::Invoke([](VExprContext*, const Block* block,
                                                   const Selector*, size_t, ColumnPtr& result) {
                    result = block->get_by_position(0).column;
                    return Status::OK();
                }));
        expression.add_child(child);
        auto context = std::make_shared<VExprContext>(child);
        context->_fn_contexts.push_back(
                FunctionContext::create_context(nullptr, target, {input.type, target}));
        context->fn_context(0)->set_enable_strict_mode(strict);
        expression._function = SimpleFunctionFactory::instance().get_function(
                "CAST", {input, {nullptr, target, "target"}}, target);
        EXPECT_NE(expression._function, nullptr);
        Block block {input};
        auto status = expression.execute_column(context.get(), &block, nullptr,
                                                input.column->size(), output);
        EXPECT_EQ(context->fn_context(0)->enable_strict_mode(), strict);
        return status;
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
};

TEST_F(FileExprTest, InlineGetterAndSixFieldStructRoundTripPreserveBinary) {
    const std::string bytes("\0\xff\x80", 3);
    auto value = file_value();
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    auto empty = file_value();
    empty.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView());
    const auto input = column(make_nullable(file_type), {value, empty, file_value(), Field()});
    const auto& schema = assert_cast<const DataTypeFile&>(*file_type);
    const auto structure_type = make_nullable(
            std::make_shared<DataTypeStruct>(schema.get_elements(), schema.get_element_names()));
    ColumnPtr projected;
    ASSERT_TRUE(cast(input, structure_type, true, projected).ok());
    ASSERT_EQ((*projected)[0].get<TYPE_STRUCT>().size(), 6);
    EXPECT_EQ((*projected)[0].get<TYPE_STRUCT>()[5].get<TYPE_VARBINARY>().str(), bytes);
    ColumnPtr restored;
    ASSERT_TRUE(
            cast({projected, structure_type, "struct"}, make_nullable(file_type), true, restored)
                    .ok());
    EXPECT_EQ((*restored)[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().str(), bytes);
    EXPECT_FALSE((*restored)[1].get<TYPE_FILE>()[5].is_null());
    EXPECT_EQ((*restored)[1].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().size(), 0);
    EXPECT_TRUE((*restored)[2].get<TYPE_FILE>()[5].is_null());
    EXPECT_TRUE(restored->is_null_at(3));
    for (const auto* name : {"element_at", "struct_element"}) {
        ColumnsWithTypeAndName args {input, {selector("InLiNe", 4), text, "field"}};
        auto function =
                SimpleFunctionFactory::instance().get_function(name, args, schema.get_element(5));
        ASSERT_NE(function, nullptr);
        EXPECT_EQ(remove_nullable(function->get_return_type())->get_primitive_type(),
                  TYPE_VARBINARY);
        Block block {args[0], args[1], {nullptr, schema.get_element(5), "output"}};
        ASSERT_TRUE(function->execute(nullptr, block, {0, 1}, 2, 4).ok());
        const auto& output = block.get_by_position(2).column;
        EXPECT_EQ((*output)[0].get<TYPE_VARBINARY>().str(), bytes);
        EXPECT_FALSE(output->is_null_at(1));
        EXPECT_EQ((*output)[1].get<TYPE_VARBINARY>().size(), 0);
        EXPECT_TRUE(output->is_null_at(2));
        EXPECT_TRUE(output->is_null_at(3));
    }
}

TEST_F(FileExprTest, StructCastMatchesReorderedPublicNamesAndStringFamily) {
    auto source_type = std::make_shared<DataTypeStruct>(
            DataTypes {bigint, std::make_shared<DataTypeString>(65533, TYPE_VARCHAR), bigint,
                       make_nullable(std::make_shared<DataTypeString>(1024, TYPE_CHAR)),
                       make_nullable(text), make_nullable(binary)},
            Strings {"SIZE", "UrI", "offset", "Content_Type", "checksum", "inline"});
    auto input = column(source_type, {structure({integer(12), string("s3://b/key"), integer(3),
                                                 string("text/plain"), Field(), Field()})});
    for (bool strict : {false, true}) {
        ColumnPtr output;
        ASSERT_TRUE(cast(input, make_nullable(file_type), strict, output).ok());
        ASSERT_FALSE(output->is_null_at(0));
        const auto value = (*output)[0];
        ASSERT_EQ(value.get_type(), TYPE_FILE);
        const auto& fields = value.get<TYPE_FILE>();
        ASSERT_EQ(fields.size(), 6);
        EXPECT_EQ(fields[0].get<TYPE_STRING>(), "s3://b/key");
        EXPECT_EQ(fields[1].get<TYPE_BIGINT>(), 3);
        EXPECT_EQ(fields[2].get<TYPE_BIGINT>(), 12);
        EXPECT_EQ(fields[3].get<TYPE_STRING>(), "text/plain");
        EXPECT_TRUE(fields[4].is_null());
        EXPECT_TRUE(fields[5].is_null());
    }
}

TEST_F(FileExprTest, StructCastValidationIsAtomicAndSkipsParentNull) {
    auto source_type = make_nullable(public_struct());
    auto input = column(
            source_type,
            {public_value("s3://b/good", integer(3)), public_value("s3://b/bad", integer(-1)),
             Field(), structure({Field(), Field(), integer(3), Field(), Field(), Field()})});
    ColumnPtr output;
    ASSERT_TRUE(cast(input, make_nullable(file_type), false, output).ok());
    ASSERT_EQ(output->size(), 4);
    EXPECT_FALSE(output->is_null_at(0));
    for (size_t row : {1, 2, 3}) EXPECT_TRUE(output->is_null_at(row));
    const auto& nested = assert_cast<const ColumnNullable&>(*output).get_nested_column();
    const auto invalid = nested[1];
    for (const auto& field : invalid.get<TYPE_FILE>()) EXPECT_TRUE(field.is_null());
    EXPECT_FALSE(cast(input, make_nullable(file_type), true, output).ok());
    // The all-NULL child placeholders behind a SQL NULL STRUCT are not FILE values.
    ASSERT_TRUE(cast(column(source_type, {Field()}), make_nullable(file_type), true, output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    for (const auto& value : {public_value("s3://b/key", integer(3)), Field()}) {
        SCOPED_TRACE(value.is_null() ? "constant NULL" : "constant non-NULL");
        ASSERT_TRUE(cast({source_type->create_column_const(4, value), source_type, "input"},
                         make_nullable(file_type), true, output)
                            .ok());
        ASSERT_EQ(output->size(), 4);
        for (size_t row = 0; row < 4; ++row) EXPECT_EQ(output->is_null_at(row), value.is_null());
        ASSERT_TRUE(try_cast({source_type->create_column_const(4, value), source_type, "input"},
                             make_nullable(file_type), true, output)
                            .ok());
        ASSERT_EQ(output->size(), 4);
        for (size_t row = 0; row < 4; ++row) EXPECT_EQ(output->is_null_at(row), value.is_null());
    }
}

TEST_F(FileExprTest, StructCastRejectsMissingExtraDuplicateAndWrongTypedChildren) {
    const Strings names {"uri", "offset", "size", "content_type", "checksum", "inline"};
    const DataTypes types {make_nullable(text), make_nullable(bigint), make_nullable(bigint),
                           make_nullable(text), make_nullable(text),   make_nullable(binary)};
    std::vector<std::pair<DataTypePtr, Field>> cases {
            {std::make_shared<DataTypeStruct>(DataTypes {text}, Strings {"uri"}),
             structure({string("s3://b/key")})}};
    cases.emplace_back(std::make_shared<DataTypeStruct>(DataTypes(types.begin(), types.begin() + 5),
                                                        Strings(names.begin(), names.begin() + 5)),
                       structure({string("s3://b/key"), Field(), Field(), Field(), Field()}));
    auto wrong_inline_types = types;
    wrong_inline_types[5] = make_nullable(text);
    cases.emplace_back(
            std::make_shared<DataTypeStruct>(wrong_inline_types, names),
            structure({string("s3://b/key"), Field(), Field(), Field(), Field(), string("AA==")}));
    auto extra_types = types;
    auto extra_names = names;
    extra_types.push_back(make_nullable(text));
    extra_names.push_back("extra");
    cases.emplace_back(std::make_shared<DataTypeStruct>(extra_types, extra_names),
                       structure({string("s3://b/key"), Field(), Field(), Field(), Field(), Field(),
                                  string("bytes")}));
    for (const auto& bad_names :
         {Strings {"uri", "offset", "size", "content_type", "other", "inline"},
          Strings {"uri", "offset", "size", "content_type", "uri", "inline"}}) {
        cases.emplace_back(std::make_shared<DataTypeStruct>(types, bad_names),
                           public_value("s3://b/key"));
    }
    const std::vector<std::pair<DataTypePtr, Field>> wrong_sizes {
            {text, string("12")},
            {std::make_shared<DataTypeInt16>(), Field::create_field<TYPE_SMALLINT>(12)},
            {std::make_shared<DataTypeInt32>(), Field::create_field<TYPE_INT>(12)},
            {std::make_shared<DataTypeInt128>(), Field::create_field<TYPE_LARGEINT>(12)}};
    for (const auto& [type, value] : wrong_sizes) {
        auto wrong_types = types;
        wrong_types[2] = make_nullable(type);
        cases.emplace_back(std::make_shared<DataTypeStruct>(wrong_types, names),
                           public_value("s3://b/key", value));
    }
    auto wrong_uri_types = types;
    wrong_uri_types[0] = make_nullable(bigint);
    cases.emplace_back(std::make_shared<DataTypeStruct>(wrong_uri_types, names),
                       structure({integer(1), Field(), Field(), Field(), Field(), Field()}));
    auto wrong_offset_types = types;
    wrong_offset_types[1] = make_nullable(std::make_shared<DataTypeInt32>());
    cases.emplace_back(std::make_shared<DataTypeStruct>(wrong_offset_types, names),
                       structure({string("s3://b/key"), Field::create_field<TYPE_INT>(0),
                                  integer(12), Field(), Field(), Field()}));
    for (const auto& [type, value] : cases) {
        SCOPED_TRACE(type->get_name());
        for (bool strict : {false, true}) {
            ColumnPtr output;
            EXPECT_FALSE(
                    cast(column(type, {value}), make_nullable(file_type), strict, output).ok());
        }
    }
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
    }
}

TEST_F(FileExprTest, StructCastAcceptsUntypedNullPublicChildren) {
    auto null_type = std::make_shared<DataTypeUInt8>();
    null_type->set_null_literal(true);
    auto source = std::make_shared<DataTypeStruct>(
            DataTypes {text, make_nullable(null_type), make_nullable(null_type),
                       make_nullable(null_type), make_nullable(null_type),
                       make_nullable(null_type)},
            Strings {"uri", "offset", "size", "content_type", "checksum", "inline"});
    ColumnPtr output;
    ASSERT_TRUE(cast(column(source, {public_value("urn:null-children")}), file_type, true, output)
                        .ok());
    const auto value = (*output)[0];
    EXPECT_EQ(value.get<TYPE_FILE>()[0].get<TYPE_STRING>(), "urn:null-children");
    for (size_t i = 1; i < 6; ++i) EXPECT_TRUE(value.get<TYPE_FILE>()[i].is_null());
}

TEST_F(FileExprTest, VariantRoundTripExposesSixFieldsAndPreservesBinaryAndNulls) {
    const auto variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
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
    ASSERT_TRUE(cast(input, variant_type, true, variant).ok());
    ASSERT_EQ(variant->size(), 2);
    EXPECT_TRUE(variant->is_null_at(1));
    ColumnPtr encoded;
    ASSERT_TRUE(cast({variant, variant_type, "variant"}, json_type, true, encoded).ok());
    const auto data =
            assert_cast<const ColumnNullable&>(*encoded).get_nested_column().get_data_at(0);
    const auto* root = JsonbDocument::createValue(data.data, data.size);
    ASSERT_NE(root, nullptr);
    ASSERT_TRUE(root->isObject());
    const auto* object = root->unpack<ObjectVal>();
    EXPECT_EQ(object->numElem(), 6);
    for (const auto* name : {"uri", "offset", "size", "content_type", "checksum", "inline"}) {
        EXPECT_NE(object->find(name), nullptr) << name;
    }
    ASSERT_TRUE(object->find("inline")->isString());
    EXPECT_EQ(std::string(object->find("inline")->unpack<JsonbStringVal>()->getBlob(),
                          object->find("inline")->unpack<JsonbStringVal>()->getBlobLen()),
              "YQD/");
    EXPECT_TRUE(encoded->is_null_at(1));
    ColumnPtr restored;
    ASSERT_TRUE(cast({variant, variant_type, "variant"}, make_nullable(file_type), true, restored)
                        .ok());
    const auto roundtrip = (*restored)[0];
    EXPECT_EQ(roundtrip.get<TYPE_FILE>()[0].get<TYPE_STRING>(), "s3://b/variant");
    EXPECT_EQ(roundtrip.get<TYPE_FILE>()[1].get<TYPE_BIGINT>(), 0);
    EXPECT_EQ(roundtrip.get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 3);
    EXPECT_EQ(roundtrip.get<TYPE_FILE>()[3].get<TYPE_STRING>(), "application/octet-stream");
    EXPECT_EQ(roundtrip.get<TYPE_FILE>()[4].get<TYPE_STRING>(), "ETAG:opaque");
    EXPECT_EQ(roundtrip.get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().str(), bytes);
    EXPECT_TRUE(restored->is_null_at(1));
    EXPECT_EQ((*input.column)[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().str(), bytes);
}

TEST_F(FileExprTest, VariantCastRequiresCompleteObjectAndValidPublicChildren) {
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
             json(R"({"uri":"relative/path","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null})")});
    ColumnPtr variant;
    ASSERT_TRUE(cast(input, variant_type, true, variant).ok());
    ColumnWithTypeAndName source {variant, variant_type, "variant"};
    ColumnPtr output;
    ASSERT_TRUE(cast(source, make_nullable(file_type), false, output).ok());
    ASSERT_EQ(output->size(), input.column->size());
    ASSERT_FALSE(output->is_null_at(0));
    const auto valid = (*output)[0];
    EXPECT_EQ(valid.get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 4);
    for (size_t row = 1; row < output->size(); ++row) EXPECT_TRUE(output->is_null_at(row)) << row;
    for (bool strict : {false, true}) {
        ASSERT_TRUE(try_cast(source, make_nullable(file_type), strict, output).ok());
        EXPECT_FALSE(output->is_null_at(0));
        for (size_t row = 1; row < output->size(); ++row)
            EXPECT_TRUE(output->is_null_at(row)) << row;
    }
    for (size_t row = 0; row < variant->size(); ++row) {
        const auto status = cast({variant->cut(row, 1), variant_type, "variant"},
                                 make_nullable(file_type), true, output);
        EXPECT_EQ(status.ok(), row < 3) << row << ": " << status;
    }
}

TEST_F(FileExprTest, JsonCastUsesPublicSchemaAndRejectsInvalidValuesAtomically) {
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
    ASSERT_TRUE(cast(input, make_nullable(file_type), false, output).ok());
    ASSERT_EQ(output->size(), 6);
    EXPECT_FALSE(output->is_null_at(0));
    const auto value = (*output)[0];
    EXPECT_EQ(value.get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 0);
    EXPECT_TRUE(value.get<TYPE_FILE>()[5].is_null());
    const auto& nested = assert_cast<const ColumnNullable&>(*output).get_nested_column();
    const auto invalid = nested[2];
    for (const auto& field : invalid.get<TYPE_FILE>()) EXPECT_TRUE(field.is_null());
    for (size_t row = 1; row < 6; ++row) EXPECT_TRUE(output->is_null_at(row));
    EXPECT_FALSE(cast(input, make_nullable(file_type), true, output).ok());
    ASSERT_TRUE(
            cast(column(json_type, {json("null")}), make_nullable(file_type), true, output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    ASSERT_TRUE(cast(column(make_nullable(json_type), {Field()}), make_nullable(file_type), true,
                     output)
                        .ok());
    EXPECT_TRUE(output->is_null_at(0));
}

TEST_F(FileExprTest, ReverseCastsExposeExactlySixFields) {
    const std::string bytes("\0\xff", 2);
    auto value = file_value("s3://b/key");
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(
            StringView(bytes.data(), cast_set<uint32_t>(bytes.size())));
    const auto input = column(make_nullable(file_type), {value, Field()});
    ColumnPtr output;
    ASSERT_TRUE(cast(input, make_nullable(public_struct()), true, output).ok());
    EXPECT_TRUE(output->is_null_at(1));
    const auto public_value = (*output)[0];
    ASSERT_EQ(public_value.get_type(), TYPE_STRUCT);
    EXPECT_EQ(public_value.get<TYPE_STRUCT>().size(), 6);
    EXPECT_EQ(public_value.get<TYPE_STRUCT>()[0].get<TYPE_STRING>(), "s3://b/key");

    const auto expected =
            R"({"uri":"s3://b/key","offset":null,"size":null,"content_type":null,"checksum":null,"inline":"AP8="})";
    ASSERT_TRUE(cast(input, make_nullable(std::make_shared<DataTypeJsonb>()), true, output).ok());
    const auto& json_column = assert_cast<const ColumnNullable&>(*output).get_nested_column();
    const auto encoded = json_column.get_data_at(0);
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(encoded.data, encoded.size), expected);
    EXPECT_TRUE(output->is_null_at(1));
    // SQL output must preserve the source bytes.
    EXPECT_EQ((*input.column)[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().size(), bytes.size());
}

TEST_F(FileExprTest, ReverseStructMatchesPublicNamesAndRequiresNullableChildren) {
    const auto& file = assert_cast<const DataTypeFile&>(*file_type);
    ColumnPtr output;
    auto source_value = file_value("s3://b/key");
    source_value.get<TYPE_FILE>()[1] = integer(3);
    source_value.get<TYPE_FILE>()[2] = integer(12);
    const std::string bytes("\0\xff", 2);
    source_value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    const auto input = column(file_type, {source_value});
    auto six = std::make_shared<DataTypeStruct>(file.get_elements(), file.get_element_names());
    EXPECT_TRUE(cast(input, six, true, output).ok());
    auto reordered = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(bigint), make_nullable(text), make_nullable(bigint),
                       make_nullable(text), make_nullable(text), make_nullable(binary)},
            Strings {"SIZE", "UrI", "offset", "content_type", "checksum", "inline"});
    ASSERT_TRUE(cast(input, reordered, true, output).ok());
    const auto value = (*output)[0];
    EXPECT_EQ(value.get<TYPE_STRUCT>()[0].get<TYPE_BIGINT>(), 12);
    EXPECT_EQ(value.get<TYPE_STRUCT>()[1].get<TYPE_STRING>(), "s3://b/key");
    EXPECT_EQ(value.get<TYPE_STRUCT>()[2].get<TYPE_BIGINT>(), 3);
    EXPECT_EQ(value.get<TYPE_STRUCT>()[5].get<TYPE_VARBINARY>().str(), bytes);
    auto nonnullable = std::make_shared<DataTypeStruct>(
            DataTypes {text, make_nullable(bigint), make_nullable(bigint), make_nullable(text),
                       make_nullable(text), make_nullable(binary)},
            Strings {"uri", "offset", "size", "content_type", "checksum", "inline"});
    EXPECT_FALSE(cast(input, nonnullable, true, output).ok());
}

TEST_F(FileExprTest, CastRecursesThroughArrayStructAndMapValues) {
    auto source_file = make_nullable(public_struct());
    auto target_file = make_nullable(file_type);
    auto good = public_value("s3://b/key");
    auto bad = public_value("relative/path");
    ColumnPtr output;
    auto array_source = std::make_shared<DataTypeArray>(source_file);
    auto array_target = std::make_shared<DataTypeArray>(target_file);
    auto array_value = Field::create_field<TYPE_ARRAY>(Array {good, bad, Field()});
    ASSERT_TRUE(cast(column(array_source, {array_value}), array_target, false, output).ok());
    const auto result_array = (*output)[0];
    ASSERT_EQ(result_array.get<TYPE_ARRAY>().size(), 3);
    EXPECT_EQ(result_array.get<TYPE_ARRAY>()[0].get_type(), TYPE_FILE);
    EXPECT_TRUE(result_array.get<TYPE_ARRAY>()[1].is_null());
    EXPECT_TRUE(result_array.get<TYPE_ARRAY>()[2].is_null());

    auto struct_source = std::make_shared<DataTypeStruct>(DataTypes {source_file}, Strings {"f"});
    auto struct_target = std::make_shared<DataTypeStruct>(DataTypes {target_file}, Strings {"f"});
    ASSERT_TRUE(cast(column(struct_source, {structure({good})}), struct_target, true, output).ok());
    EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[0].get_type(), TYPE_FILE);

    auto map_source = std::make_shared<DataTypeMap>(make_nullable(bigint), source_file);
    auto map_target = std::make_shared<DataTypeMap>(make_nullable(bigint), target_file);
    auto map_value =
            Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {integer(1)}),
                                               Field::create_field<TYPE_ARRAY>(Array {good})});
    ASSERT_TRUE(cast(column(map_source, {map_value}), map_target, true, output).ok());
    EXPECT_EQ((*output)[0].get<TYPE_MAP>()[1].get<TYPE_ARRAY>()[0].get_type(), TYPE_FILE);
}

TEST_F(FileExprTest, StatisticsCountsPayloadBytesAndPreservesParentNull) {
    const std::string bytes(2 * 1024 * 1024, '\xff');
    auto complete = file_value("urn:stats");
    auto& fields = complete.get<TYPE_FILE>();
    fields[1] = integer(0);
    fields[2] = integer(17);
    fields[3] = string("text/plain");
    fields[4] = string("ETAG:opaque");
    fields[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    auto empty_inline = file_value("urn:stats");
    empty_inline.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView());
    const auto input = column(make_nullable(file_type),
                              {complete, file_value("urn:stats"), empty_inline, Field()});
    const auto result_type = make_nullable(bigint);
    auto function = SimpleFunctionFactory::instance().get_function("__file_data_size", {input},
                                                                   result_type);
    ASSERT_NE(function, nullptr);
    Block block {input, {nullptr, result_type, "output"}};
    ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 4).ok());
    const auto& output = block.get_by_position(1).column;
    ASSERT_EQ(output->size(), 4);
    EXPECT_EQ((*output)[0].get<TYPE_BIGINT>(), 9 + 16 + 10 + 11 + bytes.size());
    EXPECT_EQ((*output)[1].get<TYPE_BIGINT>(), 9);
    EXPECT_EQ((*output)[2].get<TYPE_BIGINT>(), 9);
    EXPECT_TRUE(output->is_null_at(3));
    // Counting does not materialize, clear or change the opaque bytes.
    EXPECT_EQ((*input.column)[0].get<TYPE_FILE>()[5].get<TYPE_VARBINARY>().size(), bytes.size());
}

TEST_F(FileExprTest, StatisticsHandlesConstantAndNonNullableFile) {
    for (const bool nullable : {false, true}) {
        const auto input_type = nullable ? make_nullable(file_type) : file_type;
        const auto result_type = nullable ? make_nullable(bigint) : bigint;
        const ColumnWithTypeAndName input {input_type->create_column_const(7, file_value("urn:x")),
                                           input_type, "input"};
        auto function = SimpleFunctionFactory::instance().get_function("__file_data_size", {input},
                                                                       result_type);
        ASSERT_NE(function, nullptr);
        Block block {input, {nullptr, result_type, "output"}};
        ASSERT_TRUE(function->execute(nullptr, block, {0}, 1, 7).ok());
        const auto& output = block.get_by_position(1).column;
        ASSERT_EQ(output->size(), 7);
        for (size_t row = 0; row < output->size(); ++row) {
            EXPECT_EQ((*output)[row].get<TYPE_BIGINT>(), 5);
        }
    }
}

TEST_F(FileExprTest, GetterUsesConstantPublicNamesAndMergesNullsWithoutMutation) {
    auto value = file_value();
    value.get<TYPE_FILE>()[2] = integer(7);
    auto input = column(make_nullable(file_type), {value, Field(), file_value()});
    for (const auto& name : {"element_at", "struct_element"}) {
        SCOPED_TRACE(name);
        ColumnsWithTypeAndName args {input, {selector("SiZe", 3), text, "selector"}};
        auto function =
                SimpleFunctionFactory::instance().get_function(name, args, make_nullable(bigint));
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
}

TEST_F(FileExprTest, StructElementFactoryPreservesVariantOverloadsAndFileFallback) {
    auto expect_binding = [&](const DataTypePtr& input_type, const ColumnWithTypeAndName& index,
                              const DataTypePtr& return_type) {
        SCOPED_TRACE(input_type->get_name() + "/" + index.type->get_name());
        ColumnsWithTypeAndName args {{nullptr, input_type, "input"}, index};
        auto function =
                SimpleFunctionFactory::instance().get_function("struct_element", args, return_type);
        ASSERT_NE(function, nullptr);
        EXPECT_TRUE(function->get_return_type()->equals(*return_type));
    };
    const auto variant = std::make_shared<DataTypeVariantV2>();
    expect_binding(variant, {selector("key"), text, "selector"}, make_nullable(variant));
    expect_binding(variant, {bigint->create_column_const(1, integer(1)), bigint, "selector"},
                   make_nullable(variant));
    expect_binding(file_type, {selector("size"), text, "selector"}, make_nullable(bigint));
    expect_binding(public_struct(), {selector("size"), text, "selector"}, make_nullable(bigint));
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

TEST_F(FileExprTest, FileLiteralUsesIndependentNodeAndValidatedSixChildValue) {
    static_assert(!std::is_base_of_v<VStructLiteral, VFileLiteral>);
    EXPECT_EQ(TExprNodeType::FILE_LITERAL, 46);
    TExprNode node;
    node.__set_node_type(TExprNodeType::FILE_LITERAL);
    node.__set_type(file_type->to_thrift());
    node.__set_is_nullable(false);
    node.__set_num_children(6);
    VExprSPtr expression;
    ASSERT_TRUE(VExpr::create_expr(node, expression).ok());
    ASSERT_NE(dynamic_cast<VFileLiteral*>(expression.get()), nullptr);
    const auto& schema = assert_cast<const DataTypeFile&>(*file_type);
    for (size_t i = 0; i < 6; ++i) {
        expression->add_child(VLiteral::create_shared(schema.get_element(i),
                                                      i == 0 ? string("s3://b/key") : Field()));
    }
    auto context = std::make_shared<VExprContext>(expression);
    ASSERT_TRUE(expression->prepare(nullptr, RowDescriptor(), context.get()).ok());
    ColumnPtr output;
    ASSERT_TRUE(expression->execute_column_impl(context.get(), nullptr, nullptr, 3, output).ok());
    EXPECT_TRUE(is_column_const(*output));
    ASSERT_EQ(output->size(), 3);
    EXPECT_EQ((*output)[2].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "s3://b/key");
    VExprSPtr clone;
    ASSERT_TRUE(expression->deep_clone(&clone).ok());
    EXPECT_EQ(clone->node_type(), TExprNodeType::FILE_LITERAL);
    EXPECT_NE(dynamic_cast<VFileLiteral*>(clone.get()), nullptr);
    EXPECT_TRUE(expression->equals(*clone));
}

TEST_F(FileExprTest, FileLiteralDigestSupportsConditionCacheAndInlineIdentity) {
    const std::string bytes("a\0b", 3);
    const std::string large_bytes(4096, '\xff');
    const std::vector<Field> inline_values {
            Field(), Field::create_field<TYPE_VARBINARY>(StringView()),
            Field::create_field<TYPE_VARBINARY>(StringView(bytes)),
            Field::create_field<TYPE_VARBINARY>(StringView(large_bytes))};
    VExprSPtrs literals;
    for (const auto& inline_value : inline_values) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::FILE_LITERAL);
        node.__set_type(file_type->to_thrift());
        node.__set_is_nullable(false);
        node.__set_num_children(6);
        auto literal = VFileLiteral::create_shared(node);
        const auto& schema = assert_cast<const DataTypeFile&>(*file_type);
        for (size_t i = 0; i < 6; ++i) {
            literal->add_child(
                    VLiteral::create_shared(schema.get_element(i), i == 0   ? string("s3://b/key")
                                                                   : i == 5 ? inline_value
                                                                            : Field()));
        }
        auto context = std::make_shared<VExprContext>(literal);
        ASSERT_TRUE(literal->prepare(nullptr, RowDescriptor(), context.get()).ok());
        VExprSPtr clone;
        ASSERT_TRUE(literal->deep_clone(&clone).ok());
        EXPECT_TRUE(literal->equals(*clone));
        const VExprContext clone_context(clone);
        // ScanLocalState builds the condition-cache key through VExprContext::get_digest.
        for (uint64_t seed : {1, 12345}) {
            const auto digest = context->get_digest(seed);
            EXPECT_NE(digest, 0);
            EXPECT_EQ(digest, clone_context.get_digest(seed));
            for (const auto& previous : literals) {
                EXPECT_FALSE(literal->equals(*previous));
                EXPECT_NE(digest, previous->get_digest(seed));
            }
        }
        // Internal integrity hashing is supported independently of SQL hash capability.
        uint64_t hash = 1;
        uint64_t clone_hash = 1;
        literal->get_column_ptr()->update_xxHash_with_value(0, 1, hash, nullptr);
        assert_cast<const VFileLiteral&>(*clone).get_column_ptr()->update_xxHash_with_value(
                0, 1, clone_hash, nullptr);
        EXPECT_EQ(hash, clone_hash);
        for (const auto& previous : literals) {
            uint64_t previous_hash = 1;
            assert_cast<const VFileLiteral&>(*previous).get_column_ptr()->update_xxHash_with_value(
                    0, 1, previous_hash, nullptr);
            EXPECT_NE(hash, previous_hash);
        }
        literals.push_back(std::move(literal));
    }
}

TEST_F(FileExprTest, FileLiteralRejectsInvalidLocator) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FILE_LITERAL);
    node.__set_type(file_type->to_thrift());
    node.__set_is_nullable(false);
    node.__set_num_children(6);
    auto expression = VFileLiteral::create_shared(node);
    const auto& schema = assert_cast<const DataTypeFile&>(*file_type);
    for (size_t i = 0; i < 6; ++i) {
        expression->add_child(VLiteral::create_shared(schema.get_element(i),
                                                      i == 0 ? string("relative/path") : Field()));
    }
    auto context = std::make_shared<VExprContext>(expression);
    EXPECT_FALSE(expression->prepare(nullptr, RowDescriptor(), context.get()).ok());
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
    ASSERT_TRUE(cast(input, make_nullable(file_type), false, output).ok());
    for (size_t row = 0; row < input.column->size(); ++row)
        EXPECT_TRUE(output->is_null_at(row)) << row;
    for (size_t row = 0; row < input.column->size(); ++row) {
        EXPECT_FALSE(cast({input.column->cut(row, 1), type, "input"}, make_nullable(file_type),
                          true, output)
                             .ok())
                << row;
    }
}

TEST_F(FileExprTest, NestedCastsSkipDataBehindNullContainers) {
    const auto source_file = make_nullable(public_struct());
    const auto target_file = make_nullable(file_type);
    const auto bad = public_value("relative/path");
    const auto good = public_value("s3://b/good");
    const auto mask = [] {
        auto result = ColumnUInt8::create();
        result->insert_value(1);
        result->insert_value(0);
        return result;
    };
    const auto array_source = std::make_shared<DataTypeArray>(source_file);
    const auto array_target = std::make_shared<DataTypeArray>(target_file);
    const auto values =
            column(array_source, {Field::create_field<TYPE_ARRAY>(Array {bad, bad, bad}),
                                  Field::create_field<TYPE_ARRAY>(Array {good, good})});
    ColumnPtr output;
    ASSERT_TRUE(cast({ColumnNullable::create(values.column, mask()), make_nullable(array_source),
                      "input"},
                     make_nullable(array_target), true, output)
                        .ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_EQ((*output)[1].get<TYPE_ARRAY>()[1].get_type(), TYPE_FILE);

    const auto struct_source =
            std::make_shared<DataTypeStruct>(DataTypes {source_file}, Strings {"f"});
    const auto struct_target =
            std::make_shared<DataTypeStruct>(DataTypes {target_file}, Strings {"f"});
    const auto structs = column(struct_source, {structure({bad}), structure({good})});
    ASSERT_TRUE(cast({ColumnNullable::create(structs.column, mask()), make_nullable(struct_source),
                      "input"},
                     make_nullable(struct_target), true, output)
                        .ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_EQ((*output)[1].get<TYPE_STRUCT>()[0].get_type(), TYPE_FILE);

    const auto map_source = std::make_shared<DataTypeMap>(make_nullable(bigint), source_file);
    const auto map_target = std::make_shared<DataTypeMap>(make_nullable(bigint), target_file);
    const auto maps = column(
            map_source,
            {Field::create_field<TYPE_MAP>(Map {
                     Field::create_field<TYPE_ARRAY>(Array {integer(1), integer(2), integer(3)}),
                     Field::create_field<TYPE_ARRAY>(Array {bad, bad, bad})}),
             Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {integer(1)}),
                                                Field::create_field<TYPE_ARRAY>(Array {good})})});
    ASSERT_TRUE(
            cast({ColumnNullable::create(maps.column, mask()), make_nullable(map_source), "input"},
                 make_nullable(map_target), true, output)
                    .ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_EQ((*output)[1].get<TYPE_MAP>()[1].get<TYPE_ARRAY>()[0].get_type(), TYPE_FILE);
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

TEST_F(FileExprTest, ReverseCastsRecurseAndIdentityPreservesInline) {
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
    ASSERT_TRUE(cast(input, std::make_shared<DataTypeArray>(make_nullable(public_struct())), true,
                     output)
                        .ok());
    EXPECT_EQ((*output)[0].get<TYPE_ARRAY>()[0].get<TYPE_STRUCT>().size(), 6);
    EXPECT_TRUE((*output)[0].get<TYPE_ARRAY>()[1].is_null());
}

TEST_F(FileExprTest, ExplodeFilePublicFieldsNullAndOuterRows) {
    auto value = file_value("s3://b/key");
    value.get<TYPE_FILE>()[2] = integer(17);
    const std::string bytes("\0\xff\x80", 3);
    value.get<TYPE_FILE>()[5] = Field::create_field<TYPE_VARBINARY>(StringView(bytes));
    Block block {column(make_nullable(file_type), {value, Field(), file_value("s3://b/last")})};
    auto root = std::make_shared<MockVExpr>();
    auto child = std::make_shared<MockVExpr>();
    EXPECT_CALL(*child,
                execute_column_impl(testing::_, testing::_, testing::_, testing::_, testing::_))
            .WillRepeatedly(testing::Invoke([](VExprContext*, const Block* input, const Selector*,
                                               size_t, ColumnPtr& result) {
                result = input->get_by_position(0).column;
                return Status::OK();
            }));
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

TEST_F(FileExprTest, TryCastMakesInvalidFileNullInBothSessionModes) {
    const auto input_type = make_nullable(public_struct());
    const auto target = make_nullable(file_type);
    auto input = column(input_type, {public_value("s3://b/good", integer(4)),
                                     public_value("s3://b/bad", integer(-1)), Field()});
    for (bool strict : {false, true}) {
        ColumnPtr output;
        ASSERT_TRUE(try_cast(input, target, strict, output).ok());
        ASSERT_EQ(output->size(), 3);
        EXPECT_FALSE(output->is_null_at(0));
        EXPECT_EQ((*output)[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 4);
        EXPECT_TRUE(output->is_null_at(1));
        EXPECT_TRUE(output->is_null_at(2));
    }
}

TEST_F(FileExprTest, NestedTryCastUsesGenericFailureHandling) {
    for (bool from_json : {false, true}) {
        DataTypePtr leaf =
                from_json ? DataTypePtr(std::make_shared<DataTypeJsonb>()) : public_struct();
        leaf = make_nullable(leaf);
        const auto target_leaf = make_nullable(file_type);
        const auto good =
                from_json
                        ? json(R"({"uri":"s3://b/good","offset":null,"size":4,"content_type":null,"checksum":null,"inline":null})")
                        : public_value("s3://b/good", integer(4));
        const auto bad =
                from_json
                        ? json(R"({"uri":"s3://b/bad","offset":null,"size":-1,"content_type":null,"checksum":null,"inline":null})")
                        : public_value("s3://b/bad", integer(-1));
        const auto array_source = make_nullable(std::make_shared<DataTypeArray>(leaf));
        const auto array_target = make_nullable(std::make_shared<DataTypeArray>(target_leaf));
        const auto mixed_array = Field::create_field<TYPE_ARRAY>(Array {good, bad, Field()});
        const auto arrays = column(
                array_source, {mixed_array, Field::create_field<TYPE_ARRAY>(Array {}), Field()});
        const auto struct_source = make_nullable(std::make_shared<DataTypeStruct>(
                DataTypes {leaf, leaf, text}, Strings {"valid", "invalid", "label"}));
        const auto struct_target = make_nullable(std::make_shared<DataTypeStruct>(
                DataTypes {target_leaf, target_leaf, text}, Strings {"valid", "invalid", "label"}));
        const auto structs =
                column(struct_source, {structure({good, bad, string("kept")}), Field()});
        const auto map_source =
                make_nullable(std::make_shared<DataTypeMap>(make_nullable(bigint), leaf));
        const auto map_target =
                make_nullable(std::make_shared<DataTypeMap>(make_nullable(bigint), target_leaf));
        const auto keys =
                Field::create_field<TYPE_ARRAY>(Array {integer(1), integer(2), integer(3)});
        const auto maps = column(
                map_source,
                {Field::create_field<TYPE_MAP>(Map {keys, mixed_array}),
                 Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(Array {}),
                                                    Field::create_field<TYPE_ARRAY>(Array {})}),
                 Field()});
        for (bool strict : {false, true}) {
            ColumnPtr output;
            ASSERT_TRUE(try_cast(arrays, array_target, strict, output).ok());
            if (strict) {
                EXPECT_TRUE(output->is_null_at(0));
                EXPECT_TRUE((*output)[1].get<TYPE_ARRAY>().empty());
                EXPECT_TRUE(output->is_null_at(2));
            } else {
                ASSERT_FALSE(output->is_null_at(0));
                const auto array_result = (*output)[0];
                const auto& elements = array_result.get<TYPE_ARRAY>();
                ASSERT_EQ(elements.size(), 3);
                EXPECT_EQ(elements[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 4);
                EXPECT_TRUE(elements[1].is_null());
                EXPECT_TRUE(elements[2].is_null());
                EXPECT_TRUE((*output)[1].get<TYPE_ARRAY>().empty());
                EXPECT_TRUE(output->is_null_at(2));
            }

            ASSERT_TRUE(try_cast(structs, struct_target, strict, output).ok());
            if (strict) {
                EXPECT_TRUE(output->is_null_at(0));
                EXPECT_TRUE(output->is_null_at(1));
            } else {
                ASSERT_FALSE(output->is_null_at(0));
                const auto struct_result = (*output)[0];
                const auto& fields = struct_result.get<TYPE_STRUCT>();
                EXPECT_EQ(fields[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 4);
                EXPECT_TRUE(fields[1].is_null());
                EXPECT_EQ(fields[2].get<TYPE_STRING>(), "kept");
                EXPECT_TRUE(output->is_null_at(1));
            }

            ASSERT_TRUE(try_cast(maps, map_target, strict, output).ok());
            if (strict) {
                EXPECT_TRUE(output->is_null_at(0));
                EXPECT_TRUE((*output)[1].get<TYPE_MAP>()[0].get<TYPE_ARRAY>().empty());
                EXPECT_TRUE(output->is_null_at(2));
            } else {
                ASSERT_FALSE(output->is_null_at(0));
                const auto map_result = (*output)[0];
                const auto& map_fields = map_result.get<TYPE_MAP>();
                EXPECT_EQ(map_fields[0].get<TYPE_ARRAY>()[1].get<TYPE_BIGINT>(), 2);
                EXPECT_EQ(map_fields[1].get<TYPE_ARRAY>()[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(),
                          4);
                EXPECT_TRUE(map_fields[1].get<TYPE_ARRAY>()[1].is_null());
                EXPECT_TRUE(map_fields[1].get<TYPE_ARRAY>()[2].is_null());
                EXPECT_TRUE((*output)[1].get<TYPE_MAP>()[0].get<TYPE_ARRAY>().empty());
                EXPECT_TRUE(output->is_null_at(2));
            }

            const auto deep_source = make_nullable(std::make_shared<DataTypeStruct>(
                    DataTypes {array_source, text}, Strings {"files", "label"}));
            const auto deep_target = make_nullable(std::make_shared<DataTypeStruct>(
                    DataTypes {array_target, text}, Strings {"files", "label"}));
            const auto deep_input = column(deep_source, {structure({mixed_array, string("kept")})});
            ASSERT_TRUE(try_cast(deep_input, deep_target, strict, output).ok());
            if (strict) {
                EXPECT_TRUE(output->is_null_at(0));
            } else {
                EXPECT_FALSE(output->is_null_at(0));
                EXPECT_TRUE((*output)[0].get<TYPE_STRUCT>()[0].get<TYPE_ARRAY>()[1].is_null());
                EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[1].get<TYPE_STRING>(), "kept");
            }
        }
        ColumnPtr output;
        EXPECT_FALSE(cast(arrays, array_target, true, output).ok());
        EXPECT_FALSE(cast(structs, struct_target, true, output).ok());
        EXPECT_FALSE(cast(maps, map_target, true, output).ok());
    }
}

TEST_F(FileExprTest, JsonTryCastUsesGenericFailureHandling) {
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
        ASSERT_TRUE(try_cast(arrays, array_target, strict, output).ok());
        if (strict) {
            EXPECT_TRUE(output->is_null_at(0));
        } else {
            ASSERT_FALSE(output->is_null_at(0));
            EXPECT_EQ((*output)[0].get<TYPE_ARRAY>()[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 4);
            EXPECT_TRUE((*output)[0].get<TYPE_ARRAY>()[1].is_null());
            EXPECT_TRUE((*output)[0].get<TYPE_ARRAY>()[2].is_null());
        }
        EXPECT_TRUE((*output)[1].get<TYPE_ARRAY>().empty());
        EXPECT_TRUE(output->is_null_at(2));
        ASSERT_TRUE(try_cast(structs, struct_target, strict, output).ok());
        if (strict) {
            EXPECT_TRUE(output->is_null_at(0));
        } else {
            ASSERT_FALSE(output->is_null_at(0));
            EXPECT_TRUE((*output)[0].get<TYPE_STRUCT>()[0].is_null());
            EXPECT_EQ((*output)[0].get<TYPE_STRUCT>()[1].get<TYPE_STRING>(), "kept");
        }
        EXPECT_EQ((*output)[1].get<TYPE_STRUCT>()[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 4);
    }
}

TEST_F(FileExprTest, JsonContainerCastSkipsInvalidFilesBehindAncestorNull) {
    const auto source = std::make_shared<DataTypeJsonb>();
    const auto values = column(
            source,
            {json(R"([{"uri":"relative/path","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}])"),
             json(R"([{"uri":"s3://b/good","offset":null,"size":null,"content_type":null,"checksum":null,"inline":null}])")});
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(1);
    nulls->insert_value(0);
    const auto target = make_nullable(std::make_shared<DataTypeArray>(make_nullable(file_type)));
    ColumnPtr output;
    ASSERT_TRUE(cast({ColumnNullable::create(values.column, std::move(nulls)),
                      make_nullable(source), "input"},
                     target, true, output)
                        .ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_EQ((*output)[1].get<TYPE_ARRAY>()[0].get<TYPE_FILE>()[0].get<TYPE_STRING>(),
              "s3://b/good");
}

TEST_F(FileExprTest, JsonTryCastKeepsExistingNonFileFailureBehavior) {
    const auto source = std::make_shared<DataTypeJsonb>();
    const auto target = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(file_type), make_nullable(bigint)},
            Strings {"file", "number"}));
    const auto input = column(
            source,
            {json(R"({"file":{"uri":"s3://b/bad","offset":null,"size":-1,"content_type":null,"checksum":null,"inline":null},"number":7})"),
             json(R"({"file":{"uri":"s3://b/good","offset":null,"size":4,"content_type":null,"checksum":null,"inline":null},"number":{}})"),
             json(R"({"file":{"uri":"s3://b/good","offset":null,"size":4,"content_type":null,"checksum":null,"inline":null},"number":8})")});
    ColumnPtr output;
    ASSERT_TRUE(try_cast(input, target, true, output).ok());
    ASSERT_EQ(output->size(), 3);
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
    ASSERT_FALSE(output->is_null_at(2));
    EXPECT_EQ((*output)[2].get<TYPE_STRUCT>()[0].get<TYPE_FILE>()[2].get<TYPE_BIGINT>(), 4);
    EXPECT_EQ((*output)[2].get<TYPE_STRUCT>()[1].get<TYPE_BIGINT>(), 8);
}

TEST_F(FileExprTest, NestedTryCastKeepsExistingNonFileFailureBehavior) {
    const auto source_file = make_nullable(public_struct());
    const auto source = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {source_file, make_nullable(text)}, Strings {"file", "number"}));
    const auto target = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(file_type), make_nullable(bigint)},
            Strings {"file", "number"}));
    const auto input =
            column(source, {structure({public_value("s3://b/bad", integer(-1)), string("7")}),
                            structure({public_value("s3://b/good", integer(4)), string("bad")})});
    ColumnPtr output;
    ASSERT_TRUE(try_cast(input, target, true, output).ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(output->is_null_at(1));
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

TEST_F(FileExprTest, LiteralAcceptsUntypedNullChildrenAndCloneKeepsFileIdentity) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FILE_LITERAL);
    node.__set_type(file_type->to_thrift());
    node.__set_is_nullable(false);
    node.__set_num_children(6);
    auto expression = VFileLiteral::create_shared(node);
    expression->add_child(VLiteral::create_shared(make_nullable(text), string("s3://b/key")));
    auto null_type = std::make_shared<DataTypeUInt8>();
    null_type->set_null_literal(true);
    for (size_t i = 1; i < 6; ++i) {
        expression->add_child(VLiteral::create_shared(make_nullable(null_type), Field()));
    }
    auto context = std::make_shared<VExprContext>(expression);
    ASSERT_TRUE(expression->prepare(nullptr, RowDescriptor(), context.get()).ok());
    VExprSPtr clone;
    ASSERT_TRUE(expression->deep_clone(&clone).ok());
    ASSERT_NE(dynamic_cast<VFileLiteral*>(clone.get()), nullptr);
    auto clone_context = std::make_shared<VExprContext>(clone);
    ASSERT_TRUE(clone->prepare(nullptr, RowDescriptor(), clone_context.get()).ok());
    ColumnPtr output;
    ASSERT_TRUE(clone->execute_column_impl(clone_context.get(), nullptr, nullptr, 2, output).ok());
    EXPECT_EQ((*output)[1].get<TYPE_FILE>()[0].get<TYPE_STRING>(), "s3://b/key");
}

TEST_F(FileExprTest, StrictStructCastSupportsNonNullableFileChildren) {
    const auto source_file = public_struct();
    const auto good = public_value("s3://b/key");
    ColumnPtr output;
    ASSERT_TRUE(cast(column(source_file, {good}), file_type, true, output).ok());
    EXPECT_TRUE(file_type->check_column(*output).ok());
    const auto source = std::make_shared<DataTypeStruct>(DataTypes {source_file}, Strings {"f"});
    const auto target = std::make_shared<DataTypeStruct>(DataTypes {file_type}, Strings {"f"});
    ASSERT_TRUE(cast(column(source, {structure({good})}), target, true, output).ok());
    EXPECT_TRUE(target->check_column(*output).ok());
    ASSERT_TRUE(cast(column(make_nullable(source), {Field()}), make_nullable(target), true, output)
                        .ok());
    EXPECT_TRUE(output->is_null_at(0));
    EXPECT_TRUE(make_nullable(target)->check_column(*output).ok());
}

} // namespace doris
