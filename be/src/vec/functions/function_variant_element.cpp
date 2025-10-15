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

#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/json_functions.h"
#include "simdjson.h"
#include "util/defer_op.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {

class FunctionVariantElement : public IFunction {
public:
    static constexpr auto name = "element_at";
    static FunctionPtr create() { return std::make_shared<FunctionVariantElement>(); }

    // Get function name.
    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<vectorized::DataTypeVariant>(),
                std::make_shared<DataTypeString>()};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK_EQ(arguments[0]->get_primitive_type(), TYPE_VARIANT)
                << "First argument for function: " << name
                << " should be DataTypeVariant but it has type " << arguments[0]->get_name() << ".";
        DCHECK(is_string_type(arguments[1]->get_primitive_type()))
                << "Second argument for function: " << name << " should be String but it has type "
                << arguments[1]->get_name() << ".";
        auto arg_variant = remove_nullable(arguments[0]);
        const auto& data_type_object = assert_cast<const DataTypeVariant&>(*arg_variant);
        return make_nullable(
                std::make_shared<DataTypeVariant>(data_type_object.variant_max_subcolumns_count()));
    }

    // wrap variant column with nullable
    // 1. if variant is null root(empty or nothing as root), then nullable map is all null
    // 2. if variant is scalar variant, then use the root's nullable map
    // 3. if variant is hierarchical variant, then create a nullable map with all none null
    ColumnPtr wrap_variant_nullable(ColumnPtr col) const {
        const auto& var = assert_cast<const ColumnVariant&>(*col);
        if (var.is_null_root()) {
            return make_nullable(col, true);
        }
        if (var.is_scalar_variant() && var.get_root()->is_nullable()) {
            const auto* nullable = assert_cast<const ColumnNullable*>(var.get_root().get());
            return ColumnNullable::create(
                    col, nullable->get_null_map_column_ptr()->clone_resized(col->size()));
        }
        return make_nullable(col);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto* variant_col = check_and_get_column<ColumnVariant>(
                remove_nullable(block.get_by_position(arguments[0]).column).get());
        if (!variant_col) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({}, {})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }
        if (block.empty()) {
            block.replace_by_position(result, block.get_by_position(result).type->create_column());
            return Status::OK();
        }

        auto index_column = block.get_by_position(arguments[1]).column;
        ColumnPtr result_column;
        RETURN_IF_ERROR(get_element_column(*variant_col, index_column, &result_column));
        if (block.get_by_position(result).type->is_nullable()) {
            result_column = wrap_variant_nullable(result_column);
        }
        block.replace_by_position(result, result_column);
        return Status::OK();
    }

private:
    // Return sub-path by specified prefix.
    // For example, for prefix a.b:
    // a.b.c.d -> c.d, a.b.c -> c
    static std::string_view get_sub_path(const std::string_view& path,
                                         const std::string_view& prefix) {
        return path.substr(prefix.size() + 1);
    }
    static Status get_element_column(const ColumnVariant& src, const ColumnPtr& index_column,
                                     ColumnPtr* result) {
        std::string field_name = index_column->get_data_at(0).to_string();
        if (src.empty()) {
            *result = ColumnVariant::create(src.max_subcolumns_count());
            // src subcolumns empty but src row count may not be 0
            (*result)->assume_mutable()->insert_many_defaults(src.size());
            // ColumnVariant should be finalized before parsing, finalize maybe modify original column structure
            (*result)->assume_mutable()->finalize();
            return Status::OK();
        }
        if (src.is_scalar_variant() && is_string_type(src.get_root_type()->get_primitive_type())) {
            // use parser to extract from root
            auto type = std::make_shared<DataTypeString>();
            MutableColumnPtr result_column = type->create_column();
            const ColumnString& docs =
                    *check_and_get_column<ColumnString>(remove_nullable(src.get_root()).get());
            simdjson::ondemand::parser parser;
            std::vector<JsonPath> parsed_paths;
            if (field_name.empty() || field_name[0] != '$') {
                field_name = "$." + field_name;
            }
            JsonFunctions::parse_json_paths(field_name, &parsed_paths);
            ColumnString* col_str = assert_cast<ColumnString*>(result_column.get());
            for (size_t i = 0; i < docs.size(); ++i) {
                if (!extract_from_document(parser, docs.get_data_at(i), parsed_paths, col_str)) {
                    VLOG_DEBUG << "failed to parse " << docs.get_data_at(i) << ", field "
                               << field_name;
                    result_column->insert_default();
                }
            }
            *result = ColumnVariant::create(src.max_subcolumns_count(), type,
                                            std::move(result_column));
            (*result)->assume_mutable()->finalize();
            return Status::OK();
        } else {
            auto mutable_src = src.clone_finalized();
            auto* mutable_ptr = assert_cast<ColumnVariant*>(mutable_src.get());
            PathInData path(field_name);
            ColumnVariant::Subcolumns subcolumns = mutable_ptr->get_subcolumns();
            const auto* node = subcolumns.find_exact(path);
            MutableColumnPtr result_col = ColumnVariant::create(src.max_subcolumns_count());
            ColumnVariant::Subcolumns new_subcolumns;

            auto extract_from_sparse_column = [&](auto& container) {
                ColumnVariant::Subcolumn root {0, true, true};
                // no root, no sparse column
                const auto& sparse_data_map =
                        assert_cast<const ColumnMap&>(*mutable_ptr->get_sparse_column());
                const auto& src_sparse_data_offsets = sparse_data_map.get_offsets();
                const auto& src_sparse_data_paths =
                        assert_cast<const ColumnString&>(sparse_data_map.get_keys());
                const auto& src_sparse_data_values =
                        assert_cast<const ColumnString&>(sparse_data_map.get_values());
                auto& sparse_data_offsets =
                        assert_cast<ColumnMap&>(*container->get_sparse_column()->assume_mutable())
                                .get_offsets();
                auto [sparse_data_paths, sparse_data_values] =
                        container->get_sparse_data_paths_and_values();
                StringRef prefix_ref(path.get_path());
                std::string_view path_prefix(prefix_ref.data, prefix_ref.size);
                for (size_t i = 0; i != src_sparse_data_offsets.size(); ++i) {
                    size_t start = src_sparse_data_offsets[ssize_t(i) - 1];
                    size_t end = src_sparse_data_offsets[ssize_t(i)];
                    size_t lower_bound_index =
                            vectorized::ColumnVariant::find_path_lower_bound_in_sparse_data(
                                    prefix_ref, src_sparse_data_paths, start, end);
                    for (; lower_bound_index != end; ++lower_bound_index) {
                        auto path_ref = src_sparse_data_paths.get_data_at(lower_bound_index);
                        std::string_view path(path_ref.data, path_ref.size);
                        if (!path.starts_with(path_prefix)) {
                            break;
                        }
                        // Don't include path that is equal to the prefix.
                        if (path.size() != path_prefix.size()) {
                            auto sub_path = get_sub_path(path, path_prefix);
                            sparse_data_paths->insert_data(sub_path.data(), sub_path.size());
                            sparse_data_values->insert_from(src_sparse_data_values,
                                                            lower_bound_index);
                        } else {
                            // insert into root column, example:  access v['b'] and b is in sparse column
                            // data example:
                            // {"b" : 123}
                            // {"b" : {"c" : 456}}
                            // b maybe in sparse column, and b.c is in subolumn, put `b` into root column to distinguish
                            // from "" which is empty path and root
                            const auto& data = ColumnVariant::deserialize_from_sparse_column(
                                    &src_sparse_data_values, lower_bound_index);
                            root.insert(data.first, data.second);
                        }
                    }
                    if (root.size() == sparse_data_offsets.size()) {
                        root.insert_default();
                    }
                    sparse_data_offsets.push_back(sparse_data_paths->size());
                }
                container->get_subcolumns().create_root(root);
                container->set_num_rows(mutable_ptr->size());
            };

            if (node != nullptr) {
                std::vector<decltype(node)> nodes;
                PathsInData paths;
                ColumnVariant::Subcolumns::get_leaves_of_node(node, nodes, paths);
                for (const auto* n : nodes) {
                    PathInData new_path = n->path.copy_pop_front();
                    VLOG_DEBUG << "add node " << new_path.get_path()
                               << ", data size: " << n->data.size()
                               << ", finalized size: " << n->data.get_finalized_column().size()
                               << ", common type: " << n->data.get_least_common_type()->get_name();
                    // if new_path is empty, indicate it's the root column, but adding a root will return false when calling add
                    if (!new_subcolumns.add(new_path, n->data)) {
                        VLOG_DEBUG << "failed to add node " << new_path.get_path();
                    }
                }

                // handle the root node
                if (new_subcolumns.empty() && !nodes.empty()) {
                    CHECK_EQ(nodes.size(), 1);
                    new_subcolumns.create_root(ColumnVariant::Subcolumn {
                            nodes[0]->data.get_finalized_column_ptr()->assume_mutable(),
                            nodes[0]->data.get_least_common_type(), true, true});
                    auto container = ColumnVariant::create(src.max_subcolumns_count(),
                                                           std::move(new_subcolumns));
                    result_col->insert_range_from(*container, 0, container->size());
                } else {
                    auto container = ColumnVariant::create(src.max_subcolumns_count(),
                                                           std::move(new_subcolumns));
                    container->clear_sparse_column();
                    extract_from_sparse_column(container);
                    result_col->insert_range_from(*container, 0, container->size());
                }
            } else {
                auto container = ColumnVariant::create(src.max_subcolumns_count(),
                                                       std::move(new_subcolumns));
                extract_from_sparse_column(container);
                result_col->insert_range_from(*container, 0, container->size());
            }
            *result = result_col->get_ptr();
            // ColumnVariant should be finalized before parsing, finalize maybe modify original column structure
            (*result)->assume_mutable()->finalize();
            VLOG_DEBUG << "dump new object "
                       << static_cast<const ColumnVariant*>(result_col.get())->debug_string()
                       << ", path " << path.get_path();
            return Status::OK();
        }
    }

    static Status extract_from_document(simdjson::ondemand::parser& parser, const StringRef& doc,
                                        const std::vector<JsonPath>& paths, ColumnString* column) {
        try {
            simdjson::padded_string json_str {doc.data, doc.size};
            simdjson::ondemand::document doc = parser.iterate(json_str);
            simdjson::ondemand::object object = doc.get_object();
            simdjson::ondemand::value value;
            RETURN_IF_ERROR(JsonFunctions::extract_from_object(object, paths, &value));
            _write_data_to_column(value, column);
        } catch (simdjson::simdjson_error& e) {
            VLOG_DEBUG << "simdjson parse exception: " << e.what();
            return Status::DataQualityError("simdjson parse exception {}", e.what());
        }
        return Status::OK();
    }

    static void _write_data_to_column(simdjson::ondemand::value& value, ColumnString* column) {
        switch (value.type()) {
        case simdjson::ondemand::json_type::null: {
            column->insert_default();
            break;
        }
        case simdjson::ondemand::json_type::boolean: {
            if (value.get_bool()) {
                column->insert_data("1", 1);
            } else {
                column->insert_data("0", 1);
            }
            break;
        }
        default: {
            auto value_str = simdjson::to_json_string(value).value();
            column->insert_data(value_str.data(), value_str.length());
        }
        }
    }
};

void register_function_variant_element(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantElement>();
}

} // namespace doris::vectorized
