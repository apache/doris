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

#include "vec/json/parse2column.h"

#include <assert.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <simdjson/simdjson.h> // IWYU pragma: keep
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <stack>
#include <string>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/json/json_parser.h"
#include "vec/json/path_in_data.h"
#include "vec/json/simd_json_parser.h"

namespace doris::vectorized {

/** Pool for objects that cannot be used from different threads simultaneously.
  * Allows to create an object for each thread.
  * Pool has unbounded size and objects are not destroyed before destruction of pool.
  *
  * Use it in cases when thread local storage is not appropriate
  *  (when maximum number of simultaneously used objects is less
  *   than number of running/sleeping threads, that has ever used object,
  *   and creation/destruction of objects is expensive).
  */
template <typename T>
class SimpleObjectPool {
protected:
    /// Hold all available objects in stack.
    std::mutex mutex;
    std::stack<std::unique_ptr<T>> stack;
    /// Specialized deleter for std::unique_ptr.
    /// Returns underlying pointer back to stack thus reclaiming its ownership.
    struct Deleter {
        SimpleObjectPool<T>* parent;
        Deleter(SimpleObjectPool<T>* parent_ = nullptr) : parent {parent_} {} /// NOLINT
        void operator()(T* owning_ptr) const {
            std::lock_guard lock {parent->mutex};
            parent->stack.emplace(owning_ptr);
        }
    };

public:
    using Pointer = std::unique_ptr<T, Deleter>;
    /// Extracts and returns a pointer from the stack if it's not empty,
    ///  creates a new one by calling provided f() otherwise.
    template <typename Factory>
    Pointer get(Factory&& f) {
        std::unique_lock lock(mutex);
        if (stack.empty()) {
            return {f(), this};
        }
        auto object = stack.top().release();
        stack.pop();
        return std::unique_ptr<T, Deleter>(object, Deleter(this));
    }
    /// Like get(), but creates object using default constructor.
    Pointer getDefault() {
        return get([] { return new T; });
    }
};

SimpleObjectPool<JSONDataParser<SimdJSONParser>> parsers_pool;

using Node = typename ColumnObject::Subcolumns::Node;
/// Visitor that keeps @num_dimensions_to_keep dimensions in arrays
/// and replaces all scalars or nested arrays to @replacement at that level.
class FieldVisitorReplaceScalars : public StaticVisitor<Field> {
public:
    FieldVisitorReplaceScalars(const Field& replacement_, size_t num_dimensions_to_keep_)
            : replacement(replacement_), num_dimensions_to_keep(num_dimensions_to_keep_) {}
    template <typename T>
    Field operator()(const T& x) const {
        if constexpr (std::is_same_v<T, Array>) {
            if (num_dimensions_to_keep == 0) {
                return replacement;
            }
            const size_t size = x.size();
            Array res(size);
            for (size_t i = 0; i < size; ++i) {
                res[i] = apply_visitor(
                        FieldVisitorReplaceScalars(replacement, num_dimensions_to_keep - 1), x[i]);
            }
            return res;
        } else {
            return replacement;
        }
    }

private:
    const Field& replacement;
    size_t num_dimensions_to_keep;
};

/// Finds a subcolumn from the same Nested type as @entry and inserts
/// an array with default values with consistent sizes as in Nested type.
bool try_insert_default_from_nested(const std::shared_ptr<Node>& entry,
                                    const ColumnObject::Subcolumns& subcolumns) {
    if (!entry->path.has_nested_part()) {
        return false;
    }

    const Node* current_node = subcolumns.find_leaf(entry->path);
    const Node* leaf = nullptr;
    size_t num_skipped_nested = 0;

    while (current_node) {
        /// Try to find the first Nested up to the current node.
        const auto* node_nested = ColumnObject::Subcolumns::find_parent(
                current_node, [](const auto& candidate) { return candidate.is_nested(); });

        if (!node_nested) {
            break;
        }

        /// If there are no leaves, skip current node and find
        /// the next node up to the current.
        leaf = ColumnObject::Subcolumns::find_leaf(node_nested, [&](const auto& candidate) {
            return candidate.data.size() == entry->data.size() + 1;
        });

        if (leaf) {
            break;
        }

        current_node = node_nested->parent;
        ++num_skipped_nested;
    }

    if (!leaf) {
        return false;
    }

    auto last_field = leaf->data.get_last_field();
    if (last_field.is_null()) {
        return false;
    }

    const auto& least_common_type = entry->data.get_least_common_type();
    size_t num_dimensions = schema_util::get_number_of_dimensions(*least_common_type);
    assert(num_skipped_nested < num_dimensions);

    /// Replace scalars to default values with consistent array sizes.
    size_t num_dimensions_to_keep = num_dimensions - num_skipped_nested;
    auto default_scalar =
            num_skipped_nested
                    ? schema_util::create_empty_array_field(num_skipped_nested)
                    : schema_util::get_base_type_of_array(least_common_type)->get_default();

    auto default_field = apply_visitor(
            FieldVisitorReplaceScalars(default_scalar, num_dimensions_to_keep), last_field);
    entry->data.insert(std::move(default_field));

    return true;
}

template <typename ParserImpl>
void parse_json_to_variant(IColumn& column, const char* src, size_t length,
                           JSONDataParser<ParserImpl>* parser) {
    auto& column_object = assert_cast<ColumnObject&>(column);
    std::optional<ParseResult> result;
    /// Treat empty string as an empty object
    /// for better CAST from String to Object.
    if (length > 0) {
        result = parser->parse(src, length);
    } else {
        result = ParseResult {};
    }
    if (!result) {
        LOG(INFO) << "failed to parse " << std::string_view(src, length) << ", length= " << length;
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Failed to parse object {}",
                               std::string_view(src, length));
    }
    auto& [paths, values] = *result;
    assert(paths.size() == values.size());
    phmap::flat_hash_set<std::string> paths_set;
    size_t num_rows = column_object.size();
    for (size_t i = 0; i < paths.size(); ++i) {
        FieldInfo field_info;
        get_field_info(values[i], &field_info);
        // TODO support multi dimensions array
        if (!config::enable_parse_multi_dimession_array && field_info.num_dimensions >= 2) {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "Sorry multi dimensions array is not supported now, we are working on it");
        }
        if (is_nothing(field_info.scalar_type)) {
            continue;
        }
        if (!paths_set.insert(paths[i].get_path()).second) {
            // return Status::DataQualityError(
            //         fmt::format("Object has ambiguous path {}, {}", paths[i].get_path()));
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Object has ambiguous path {}",
                                   paths[i].get_path());
        }

        if (!column_object.has_subcolumn(paths[i])) {
            if (paths[i].has_nested_part()) {
                column_object.add_nested_subcolumn(paths[i], field_info, num_rows);
            } else {
                column_object.add_sub_column(paths[i], num_rows);
            }
        }
        auto* subcolumn = column_object.get_subcolumn(paths[i]);
        if (!subcolumn) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Failed to find sub column {}",
                                   paths[i].get_path());
        }
        assert(subcolumn->size() == num_rows);
        subcolumn->insert(std::move(values[i]), std::move(field_info));
    }
    // /// Insert default values to missed subcolumns.
    const auto& subcolumns = column_object.get_subcolumns();
    for (const auto& entry : subcolumns) {
        if (!paths_set.contains(entry->path.get_path())) {
            bool inserted = try_insert_default_from_nested(entry, subcolumns);
            if (!inserted) {
                entry->data.insertDefault();
            }
        }
    }
    column_object.incr_num_rows();
}

bool extract_key(MutableColumns& columns, StringRef json, const std::vector<StringRef>& keys,
                 const std::vector<ExtractType>& types, JSONDataParser<SimdJSONParser>* parser) {
    return parser->extract_key(columns, json, keys, types);
}

// exposed interfaces
void parse_json_to_variant(IColumn& column, const StringRef& json,
                           JSONDataParser<SimdJSONParser>* parser) {
    return parse_json_to_variant(column, json.data, json.size, parser);
}

void parse_json_to_variant(IColumn& column, const std::vector<StringRef>& jsons) {
    auto parser = parsers_pool.get([] { return new JSONDataParser<SimdJSONParser>(); });
    for (StringRef str : jsons) {
        parse_json_to_variant(column, str.data, str.size, parser.get());
    }
}

bool extract_key(MutableColumns& columns, const std::vector<StringRef>& jsons,
                 const std::vector<StringRef>& keys, const std::vector<ExtractType>& types) {
    auto parser = parsers_pool.get([] { return new JSONDataParser<SimdJSONParser>(); });
    for (StringRef json : jsons) {
        if (!extract_key(columns, json, keys, types, parser.get())) {
            return false;
        }
    }
    return true;
}

bool extract_key(MutableColumns& columns, const ColumnString& json_column,
                 const std::vector<StringRef>& keys, const std::vector<ExtractType>& types) {
    auto parser = parsers_pool.get([] { return new JSONDataParser<SimdJSONParser>(); });
    for (size_t x = 0; x < json_column.size(); ++x) {
        if (!extract_key(columns, json_column.get_data_at(x), keys, types, parser.get())) {
            return false;
        }
    }
    return true;
}

} // namespace doris::vectorized
