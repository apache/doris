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

#include <cstdint>
#include <utility>
#include <vector>

#include "format/new_parquet/complex_column_reader.h"
#include "format/new_parquet/complex_column_reader_helpers.h"
#include "format/new_parquet/scalar_column_reader.h"

namespace doris::parquet {

Status ListColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet list read result pointer for column {}",
                                       _name);
    }
    if (_element_reader == nullptr) {
        return Status::InternalError("Parquet list element reader is not initialized for column {}",
                                     _name);
    }
    auto* array_column = array_column_from_output(column);
    DORIS_CHECK(array_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto nested_column = array_column->get_data_ptr()->assume_mutable();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    const int16_t element_slot_definition_level = _nullable_definition_level + 1;

    if (auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get())) {
        const int16_t element_max_definition_level =
                element_reader->descriptor()->max_definition_level();

        struct ScalarListSink {
            ListColumnReader* self = nullptr;
            ScalarColumnReader* element_reader = nullptr;
            MutableColumnPtr* nested_column = nullptr;
            RepeatedParentSinkState parent_state;
            int16_t element_max_definition_level = 0;

            Status start_batch(const NestedScalarBatch&) { return Status::OK(); }

            Status start_parent(const NestedScalarBatch& batch, int64_t level_idx) {
                const int16_t def_level = batch.def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    return parent_state.append_null_parent(self->_name, "LIST", self->_type);
                }
                parent_state.append_present_parent();
                if (def_level == self->_nullable_definition_level) {
                    return Status::OK();
                }
                return append_element(batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& batch, int64_t level_idx) {
                return append_element(batch, level_idx);
            }

            Status append_element(const NestedScalarBatch& batch, int64_t level_idx) {
                const int16_t def_level = batch.def_levels[level_idx];
                if (def_level == element_max_definition_level) {
                    RETURN_IF_ERROR(append_scalar_batch_value(*element_reader, batch, level_idx,
                                                              *nested_column));
                } else {
                    if (!element_reader->type()->is_nullable()) {
                        return Status::Corruption(
                                "Parquet LIST column {} contains null for non-nullable element",
                                self->_name);
                    }
                    (*nested_column)->insert_default();
                }
                return parent_state.add_entry(self->_name);
            }
        };

        ScalarListSink sink {this,
                             element_reader,
                             &nested_column,
                             {&entry_counts, &parent_nulls},
                             element_max_definition_level};
        RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                                 element_slot_definition_level, rows,
                                                 &_element_overflow, sink, rows_read));

        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    if (auto* struct_element_reader = dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        struct StructListSink {
            ListColumnReader* self = nullptr;
            StructColumnReader* element_reader = nullptr;
            MutableColumnPtr* nested_column = nullptr;
            RepeatedParentSinkState parent_state;

            Status start_batch(const NestedStructBatch&) { return Status::OK(); }

            Status start_parent(const NestedStructBatch& batch, int64_t level_idx) {
                const int16_t def_level = batch.child_batches[0].def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    RETURN_IF_ERROR(
                            parent_state.append_null_parent(self->_name, "LIST", self->_type));
                    RETURN_IF_ERROR(element_reader->skip_non_scalar_children(1));
                    return Status::OK();
                }
                parent_state.append_present_parent();
                if (def_level == self->_nullable_definition_level) {
                    RETURN_IF_ERROR(element_reader->skip_non_scalar_children(1));
                    return Status::OK();
                }
                return append_element(batch, level_idx);
            }

            Status append_repeated(const NestedStructBatch& batch, int64_t level_idx) {
                return append_element(batch, level_idx);
            }

            Status append_element(const NestedStructBatch& batch, int64_t level_idx) {
                RETURN_IF_ERROR(append_struct_batch_value(*element_reader, batch, level_idx,
                                                          *nested_column));
                return parent_state.add_entry(self->_name);
            }
        };

        StructListSink sink {
                this, struct_element_reader, &nested_column, {&entry_counts, &parent_nulls}};
        RETURN_IF_ERROR(assemble_repeated_struct_levels(
                *struct_element_reader, _repeated_repetition_level, element_slot_definition_level,
                rows, &_struct_element_overflow, sink, rows_read));

        array_column->get_data_ptr() = std::move(nested_column);
        append_offsets(array_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    auto* list_element_reader = dynamic_cast<ListColumnReader*>(_element_reader.get());
    if (list_element_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar, scalar-child STRUCT, or nested "
                "LIST elements for column {}",
                _name);
    }
    auto* scalar_nested_element_reader =
            dynamic_cast<ScalarColumnReader*>(list_element_reader->_element_reader.get());
    if (scalar_nested_element_reader == nullptr) {
        return Status::NotSupported(
                "Current parquet nested LIST reader only supports scalar nested elements for "
                "column "
                "{}",
                _name);
    }

    auto* inner_array_column = array_column_from_output(nested_column);
    DORIS_CHECK(inner_array_column != nullptr);
    auto* inner_null_map = null_map_from_nullable_output(nested_column);
    auto inner_nested_column = inner_array_column->get_data_ptr()->assume_mutable();
    std::vector<uint64_t> inner_entry_counts;
    NullMap inner_parent_nulls;
    const int16_t inner_element_slot_definition_level =
            list_element_reader->_nullable_definition_level + 1;
    const int16_t nested_element_max_definition_level =
            scalar_nested_element_reader->descriptor()->max_definition_level();

    struct NestedListSink {
        ListColumnReader* self = nullptr;
        ListColumnReader* element_reader = nullptr;
        ScalarColumnReader* scalar_element_reader = nullptr;
        MutableColumnPtr* inner_nested_column = nullptr;
        RepeatedParentSinkState parent_state;
        std::vector<uint64_t>* inner_entry_counts = nullptr;
        NullMap* inner_parent_nulls = nullptr;
        int16_t nested_element_max_definition_level = 0;

        Status start_batch(const NestedScalarBatch&) { return Status::OK(); }

        Status start_parent(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level < self->_nullable_definition_level) {
                return parent_state.append_null_parent(self->_name, "LIST", self->_type);
            }
            parent_state.append_present_parent();
            if (def_level == self->_nullable_definition_level) {
                return Status::OK();
            }
            return append_inner_list(batch, level_idx);
        }

        Status append_repeated(const NestedScalarBatch& batch, int64_t level_idx) {
            if (batch.rep_levels[level_idx] < element_reader->_repeated_repetition_level) {
                return append_inner_list(batch, level_idx);
            }
            if (inner_entry_counts->empty()) {
                return Status::Corruption("Invalid nested repeated LIST level for column {}",
                                          self->_name);
            }
            return append_scalar_element(batch, level_idx);
        }

        Status append_inner_list(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level < element_reader->_nullable_definition_level) {
                if (!element_reader->_type->is_nullable()) {
                    return Status::Corruption(
                            "Parquet LIST column {} contains null for non-nullable nested list",
                            self->_name);
                }
                RETURN_IF_ERROR(parent_state.add_entry(self->_name));
                inner_entry_counts->push_back(0);
                inner_parent_nulls->push_back(1);
                return Status::OK();
            }
            RETURN_IF_ERROR(parent_state.add_entry(self->_name));
            inner_entry_counts->push_back(0);
            inner_parent_nulls->push_back(0);
            if (def_level == element_reader->_nullable_definition_level) {
                return Status::OK();
            }
            return append_scalar_element(batch, level_idx);
        }

        Status append_scalar_element(const NestedScalarBatch& batch, int64_t level_idx) {
            const int16_t def_level = batch.def_levels[level_idx];
            if (def_level == nested_element_max_definition_level) {
                RETURN_IF_ERROR(append_scalar_batch_value(*scalar_element_reader, batch, level_idx,
                                                          *inner_nested_column));
            } else {
                if (!scalar_element_reader->type()->is_nullable()) {
                    return Status::Corruption(
                            "Parquet LIST column {} contains null for non-nullable nested element",
                            self->_name);
                }
                (*inner_nested_column)->insert_default();
            }
            ++inner_entry_counts->back();
            return Status::OK();
        }
    };

    NestedListSink sink {this,
                         list_element_reader,
                         scalar_nested_element_reader,
                         &inner_nested_column,
                         {&entry_counts, &parent_nulls},
                         &inner_entry_counts,
                         &inner_parent_nulls,
                         nested_element_max_definition_level};
    RETURN_IF_ERROR(assemble_repeated_levels(
            *scalar_nested_element_reader, _repeated_repetition_level,
            inner_element_slot_definition_level, rows, &_element_overflow, sink, rows_read));

    inner_array_column->get_data_ptr() = std::move(inner_nested_column);
    append_offsets(inner_array_column->get_offsets(), inner_entry_counts);
    append_parent_nulls(inner_null_map, inner_parent_nulls);
    append_offsets(array_column->get_offsets(), entry_counts);
    array_column->get_data_ptr() = std::move(nested_column);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

Status ListColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    struct SkipSink {
        Status start_batch(const NestedScalarBatch&) { return Status::OK(); }
        Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }
        Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }
    };
    int64_t rows_read = 0;
    if (auto* element_reader = dynamic_cast<ScalarColumnReader*>(_element_reader.get())) {
        SkipSink sink;
        RETURN_IF_ERROR(assemble_repeated_levels(*element_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_element_overflow, sink, &rows_read));
    } else if (auto* struct_element_reader =
                       dynamic_cast<StructColumnReader*>(_element_reader.get())) {
        struct StructSkipSink {
            Status start_batch(const NestedStructBatch&) { return Status::OK(); }
            Status start_parent(const NestedStructBatch&, int64_t) { return Status::OK(); }
            Status append_repeated(const NestedStructBatch&, int64_t) { return Status::OK(); }
        };
        StructSkipSink sink;
        RETURN_IF_ERROR(assemble_repeated_struct_levels(
                *struct_element_reader, _repeated_repetition_level, _nullable_definition_level + 1,
                rows, &_struct_element_overflow, sink, &rows_read));
    } else if (auto* list_element_reader = dynamic_cast<ListColumnReader*>(_element_reader.get())) {
        auto* scalar_nested_element_reader =
                dynamic_cast<ScalarColumnReader*>(list_element_reader->_element_reader.get());
        if (scalar_nested_element_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet nested LIST skip only supports scalar nested elements for "
                    "column {}",
                    _name);
        }
        SkipSink sink;
        RETURN_IF_ERROR(
                assemble_repeated_levels(*scalar_nested_element_reader, _repeated_repetition_level,
                                         list_element_reader->_nullable_definition_level + 1, rows,
                                         &_element_overflow, sink, &rows_read));
    } else {
        return Status::NotSupported(
                "Current parquet LIST reader only supports scalar, scalar-child STRUCT, or nested "
                "LIST elements for column {}",
                _name);
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet LIST column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

} // namespace doris::parquet
