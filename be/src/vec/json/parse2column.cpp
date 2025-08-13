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

#include <fmt/format.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <simdjson/simdjson.h> // IWYU pragma: keep

#include <algorithm>
#include <cassert>
#include <cstddef>
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
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
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

SimpleObjectPool<JsonParser> parsers_pool;

using Node = typename ColumnVariant::Subcolumns::Node;
/// Visitor that keeps @num_dimensions_to_keep dimensions in arrays
/// and replaces all scalars or nested arrays to @replacement at that level.
class FieldVisitorReplaceScalars : public StaticVisitor<Field> {
public:
    FieldVisitorReplaceScalars(const Field& replacement_, size_t num_dimensions_to_keep_)
            : replacement(replacement_), num_dimensions_to_keep(num_dimensions_to_keep_) {}
    template <PrimitiveType T>
    Field operator()(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) const {
        if constexpr (T == TYPE_ARRAY) {
            if (num_dimensions_to_keep == 0) {
                return replacement;
            }
            const size_t size = x.size();
            Array res(size);
            for (size_t i = 0; i < size; ++i) {
                res[i] = apply_visitor(
                        FieldVisitorReplaceScalars(replacement, num_dimensions_to_keep - 1), x[i]);
            }
            return Field::create_field<TYPE_ARRAY>(res);
        } else {
            return replacement;
        }
    }

private:
    const Field& replacement;
    size_t num_dimensions_to_keep;
};

template <typename ParserImpl>
void parse_json_to_variant(IColumn& column, const char* src, size_t length,
                           JSONDataParser<ParserImpl>* parser, const ParseConfig& config) {
    auto& column_variant = assert_cast<ColumnVariant&>(column);
    std::optional<ParseResult> result;
    /// Treat empty string as an empty object
    /// for better CAST from String to Object.
    if (length > 0) {
        result = parser->parse(src, length, config);
    } else {
        result = ParseResult {};
    }
    if (!result) {
        VLOG_DEBUG << "failed to parse " << std::string_view(src, length) << ", length= " << length;
        if (config::variant_throw_exeception_on_invalid_json) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Failed to parse object {}",
                                   std::string_view(src, length));
        }
        // Treat as string
        PathInData root_path;
        Field field = Field::create_field<TYPE_STRING>(String(src, length));
        result = ParseResult {{root_path}, {field}};
    }
    auto& [paths, values] = *result;
    assert(paths.size() == values.size());
    size_t old_num_rows = column_variant.rows();
    if (config.enable_flatten_nested) {
        // here we should check the paths in variant and paths in result,
        // if two paths which same prefix have different structure, we should throw an exception
        std::vector<PathInData> check_paths;
        for (const auto& entry : column_variant.get_subcolumns()) {
            check_paths.push_back(entry->path);
        }
        check_paths.insert(check_paths.end(), paths.begin(), paths.end());
        THROW_IF_ERROR(vectorized::schema_util::check_variant_has_no_ambiguous_paths(check_paths));
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        FieldInfo field_info;
        schema_util::get_field_info(values[i], &field_info);
        if (field_info.scalar_type_id == PrimitiveType::INVALID_TYPE) {
            continue;
        }
        if (column_variant.get_subcolumn(paths[i], i) == nullptr) {
            if (paths[i].has_nested_part()) {
                column_variant.add_nested_subcolumn(paths[i], field_info, old_num_rows);
            } else {
                column_variant.add_sub_column(paths[i], old_num_rows);
            }
        }
        auto* subcolumn = column_variant.get_subcolumn(paths[i], i);
        if (!subcolumn) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Failed to find sub column {}",
                                   paths[i].get_path());
        }
        if (subcolumn->cur_num_of_defaults() > 0) {
            subcolumn->insert_many_defaults(subcolumn->cur_num_of_defaults());
            subcolumn->reset_current_num_of_defaults();
        }
        if (subcolumn->size() != old_num_rows) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "subcolumn {} size missmatched, may contains duplicated entry",
                                   paths[i].get_path());
        }
        subcolumn->insert(std::move(values[i]), std::move(field_info));
    }
    // /// Insert default values to missed subcolumns.
    const auto& subcolumns = column_variant.get_subcolumns();
    for (const auto& entry : subcolumns) {
        if (entry->data.size() == old_num_rows) {
            // Handle nested paths differently from simple paths
            if (entry->path.has_nested_part()) {
                // Try to insert default from nested, if failed, insert regular default
                bool success = UNLIKELY(column_variant.try_insert_default_from_nested(entry));
                if (!success) {
                    entry->data.insert_default();
                }
            } else {
                // For non-nested paths, increment default counter
                entry->data.increment_default_counter();
            }
        }
    }
    column_variant.incr_num_rows();
    auto sparse_column = column_variant.get_sparse_column();
    if (sparse_column->size() == old_num_rows) {
        sparse_column->assume_mutable()->insert_default();
    }
#ifndef NDEBUG
    column_variant.check_consistency();
#endif
}

// exposed interfaces
void parse_json_to_variant(IColumn& column, const StringRef& json, JsonParser* parser,
                           const ParseConfig& config) {
    return parse_json_to_variant(column, json.data, json.size, parser, config);
}

void parse_json_to_variant(IColumn& column, const ColumnString& raw_json_column,
                           const ParseConfig& config) {
    auto parser = parsers_pool.get([] { return new JsonParser(); });
    for (size_t i = 0; i < raw_json_column.size(); ++i) {
        StringRef raw_json = raw_json_column.get_data_at(i);
        parse_json_to_variant(column, raw_json.data, raw_json.size, parser.get(), config);
    }
    column.finalize();
}

} // namespace doris::vectorized
