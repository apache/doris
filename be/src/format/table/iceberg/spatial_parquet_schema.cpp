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

#include "format/table/iceberg/spatial_parquet_schema.h"

#include <parquet/arrow/schema.h>
#include <parquet/schema.h>

#include <boost/algorithm/string/predicate.hpp>

#include "format/table/iceberg/schema.h"

namespace doris {

namespace {

::parquet::schema::NodePtr clone_parquet_node(const ::parquet::schema::Node& node) {
    if (node.is_primitive()) {
        const auto& primitive = static_cast<const ::parquet::schema::PrimitiveNode&>(node);
        return ::parquet::schema::PrimitiveNode::Make(
                primitive.name(), primitive.repetition(), primitive.logical_type(),
                primitive.physical_type(), primitive.type_length(), primitive.field_id());
    }

    const auto& group = static_cast<const ::parquet::schema::GroupNode&>(node);
    ::parquet::schema::NodeVector fields;
    fields.reserve(group.field_count());
    for (int index = 0; index < group.field_count(); ++index) {
        fields.push_back(clone_parquet_node(*group.field(index)));
    }
    return ::parquet::schema::GroupNode::Make(
            group.name(), group.repetition(), fields,
            group.logical_type()->is_none() ? nullptr : group.logical_type(), group.field_id());
}

arrow::Status validate_spatial_crs_for_write(const std::string& crs) {
    // projjson:my_crs needs a my_crs definition in Parquet file metadata. The sink currently
    // carries only the schema, so writing this annotation would leave a dangling CRS reference.
    if (boost::istarts_with(crs, "projjson:")) {
        return arrow::Status::NotImplemented(
                "Referenced spatial CRS is not supported for Parquet writes without its file "
                "metadata definition: ",
                crs);
    }
    return arrow::Status::OK();
}

arrow::Result<::parquet::LogicalType::EdgeInterpolationAlgorithm> geography_algorithm(
        const std::string& algorithm) {
    using Algorithm = ::parquet::LogicalType::EdgeInterpolationAlgorithm;
    if (algorithm == "spherical") {
        return Algorithm::SPHERICAL;
    }
    if (algorithm == "vincenty") {
        return Algorithm::VINCENTY;
    }
    if (algorithm == "thomas") {
        return Algorithm::THOMAS;
    }
    if (algorithm == "andoyer") {
        return Algorithm::ANDOYER;
    }
    if (algorithm == "karney") {
        return Algorithm::KARNEY;
    }
    return arrow::Status::Invalid("Unsupported Iceberg geography edge algorithm: ", algorithm);
}

::parquet::schema::NodePtr copy_group_with_fields(const ::parquet::schema::Node& node,
                                                  ::parquet::schema::NodeVector fields) {
    return ::parquet::schema::GroupNode::Make(
            node.name(), node.repetition(), fields,
            node.logical_type()->is_none() ? nullptr : node.logical_type(), node.field_id());
}

arrow::Result<::parquet::schema::NodePtr> spatial_parquet_node(const ::parquet::schema::Node& node,
                                                               iceberg::Type* type) {
    switch (type->type_id()) {
    case iceberg::TypeID::GEOMETRY: {
        const auto& geometry = static_cast<const iceberg::GeometryType&>(*type);
        return ::parquet::schema::PrimitiveNode::Make(
                node.name(), node.repetition(), ::parquet::LogicalType::Geometry(geometry.crs()),
                ::parquet::Type::BYTE_ARRAY, -1, node.field_id());
    }
    case iceberg::TypeID::GEOGRAPHY: {
        const auto& geography = static_cast<const iceberg::GeographyType&>(*type);
        ARROW_ASSIGN_OR_RAISE(auto algorithm, geography_algorithm(geography.algorithm()));
        return ::parquet::schema::PrimitiveNode::Make(
                node.name(), node.repetition(),
                ::parquet::LogicalType::Geography(geography.crs(), algorithm),
                ::parquet::Type::BYTE_ARRAY, -1, node.field_id());
    }
    case iceberg::TypeID::STRUCT: {
        const auto& group = static_cast<const ::parquet::schema::GroupNode&>(node);
        const auto& fields = type->as_struct_type()->fields();
        ::parquet::schema::NodeVector children;
        for (int i = 0; i < group.field_count(); ++i) {
            ARROW_ASSIGN_OR_RAISE(auto child,
                                  spatial_parquet_node(*group.field(i), fields[i].field_type()));
            children.push_back(std::move(child));
        }
        return copy_group_with_fields(node, std::move(children));
    }
    case iceberg::TypeID::LIST: {
        // Arrow emits LIST -> repeated list -> element. Only element has an Iceberg type/id.
        const auto& list = static_cast<const ::parquet::schema::GroupNode&>(node);
        const auto& repeated = static_cast<const ::parquet::schema::GroupNode&>(*list.field(0));
        ARROW_ASSIGN_OR_RAISE(
                auto element,
                spatial_parquet_node(*repeated.field(0),
                                     type->as_list_type()->element_field().field_type()));
        return copy_group_with_fields(node, {copy_group_with_fields(repeated, {element})});
    }
    case iceberg::TypeID::MAP: {
        // MAP's key_value wrapper is physical, not a Struct in the Iceberg schema.
        const auto& map = static_cast<const ::parquet::schema::GroupNode&>(node);
        const auto& repeated = static_cast<const ::parquet::schema::GroupNode&>(*map.field(0));
        ARROW_ASSIGN_OR_RAISE(auto key,
                              spatial_parquet_node(*repeated.field(0),
                                                   type->as_map_type()->key_field().field_type()));
        ARROW_ASSIGN_OR_RAISE(
                auto value, spatial_parquet_node(*repeated.field(1),
                                                 type->as_map_type()->value_field().field_type()));
        return copy_group_with_fields(node, {copy_group_with_fields(repeated, {key, value})});
    }
    default:
        return clone_parquet_node(node);
    }
    // GroupNode::fields_ owns the child shared_ptrs; Arrow Result owns the returned node.
    // Temporary wrapper NodeVectors release only their copies (no owning parent pointer/cycle).
} // NOLINT(clang-analyzer-cplusplus.NewDeleteLeaks)

} // namespace

bool contains_spatial_type(iceberg::Type* type) {
    switch (type->type_id()) {
    case iceberg::TypeID::GEOMETRY:
    case iceberg::TypeID::GEOGRAPHY:
        return true;
    case iceberg::TypeID::STRUCT:
        for (const auto& field : type->as_struct_type()->fields()) {
            if (contains_spatial_type(field.field_type())) {
                return true;
            }
        }
        return false;
    case iceberg::TypeID::LIST:
        return contains_spatial_type(type->as_list_type()->element_field().field_type());
    case iceberg::TypeID::MAP:
        return contains_spatial_type(type->as_map_type()->key_field().field_type()) ||
               contains_spatial_type(type->as_map_type()->value_field().field_type());
    default:
        return false;
    }
}

arrow::Status validate_iceberg_spatial_crs_for_write(const iceberg::Type& type) {
    switch (type.type_id()) {
    case iceberg::TypeID::GEOMETRY:
        return validate_spatial_crs_for_write(
                static_cast<const iceberg::GeometryType&>(type).crs());
    case iceberg::TypeID::GEOGRAPHY:
        return validate_spatial_crs_for_write(
                static_cast<const iceberg::GeographyType&>(type).crs());
    case iceberg::TypeID::STRUCT:
        for (const auto& field : static_cast<const iceberg::StructType&>(type).fields()) {
            ARROW_RETURN_NOT_OK(validate_iceberg_spatial_crs_for_write(*field.field_type()));
        }
        return arrow::Status::OK();
    case iceberg::TypeID::LIST:
        return validate_iceberg_spatial_crs_for_write(
                *static_cast<const iceberg::ListType&>(type).element_field().field_type());
    case iceberg::TypeID::MAP: {
        const auto& map = static_cast<const iceberg::MapType&>(type);
        ARROW_RETURN_NOT_OK(validate_iceberg_spatial_crs_for_write(*map.key_field().field_type()));
        return validate_iceberg_spatial_crs_for_write(*map.value_field().field_type());
    }
    default:
        return arrow::Status::OK();
    }
}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> make_iceberg_parquet_schema(
        const std::shared_ptr<arrow::Schema>& arrow_schema, const iceberg::Schema& iceberg_schema,
        const std::shared_ptr<::parquet::WriterProperties>& writer_properties,
        const std::shared_ptr<::parquet::ArrowWriterProperties>& arrow_properties) {
    ARROW_RETURN_NOT_OK(validate_iceberg_spatial_crs_for_write(iceberg_schema.root_struct()));
    std::shared_ptr<::parquet::SchemaDescriptor> parquet_schema;
    ARROW_RETURN_NOT_OK(::parquet::arrow::ToParquetSchema(arrow_schema.get(), *writer_properties,
                                                          *arrow_properties, &parquet_schema));

    const auto* source_root = parquet_schema->group_node();
    ::parquet::schema::NodeVector fields;
    fields.reserve(source_root->field_count());
    for (int index = 0; index < source_root->field_count(); ++index) {
        const auto& source_field = source_root->field(index);
        const auto& iceberg_field = iceberg_schema.columns()[index];
        ARROW_ASSIGN_OR_RAISE(auto field,
                              spatial_parquet_node(*source_field, iceberg_field.field_type()));
        fields.push_back(std::move(field));
    }
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make(source_root->name(), source_root->repetition(),
                                               fields, nullptr, source_root->field_id()));
}

} // namespace doris
