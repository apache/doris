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

#include "format/new_parquet/column_reader/map_column_reader.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "format/new_parquet/column_reader/complex_column_reader_helpers.h"
#include "format/new_parquet/column_reader/list_column_reader.h"
#include "format/new_parquet/column_reader/scalar_column_reader.h"
#include "format/new_parquet/column_reader/struct_column_reader.h"

namespace doris::parquet {

Status MapColumnReader::read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) {
    if (column.get() == nullptr || rows_read == nullptr) {
        return Status::InvalidArgument("Invalid parquet map read result pointer for column {}",
                                       _name);
    }
    if (_key_reader == nullptr || _value_reader == nullptr) {
        return Status::InternalError("Parquet map child reader is not initialized for column {}",
                                     _name);
    }
    auto* key_reader = dynamic_cast<ScalarColumnReader*>(_key_reader.get());
    auto* value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get());
    auto* struct_value_reader = dynamic_cast<StructColumnReader*>(_value_reader.get());
    auto* list_value_reader = dynamic_cast<ListColumnReader*>(_value_reader.get());
    if (key_reader == nullptr || (value_reader == nullptr && struct_value_reader == nullptr &&
                                  list_value_reader == nullptr)) {
        return Status::NotSupported(
                "Current parquet MAP reader only supports scalar key with scalar, scalar-child "
                "STRUCT, or scalar LIST value for column {}",
                _name);
    }

    auto* map_column = map_column_from_output(column);
    DORIS_CHECK(map_column != nullptr);
    auto* parent_null_map = null_map_from_nullable_output(column);
    auto key_column = map_column->get_keys_ptr()->assume_mutable();
    auto value_column = map_column->get_values_ptr()->assume_mutable();
    std::vector<uint64_t> entry_counts;
    NullMap parent_nulls;
    const int16_t entry_definition_level = _nullable_definition_level + 1;
    const int16_t key_max_definition_level = key_reader->descriptor()->max_definition_level();

    if (value_reader != nullptr) {
        const int16_t value_max_definition_level =
                value_reader->descriptor()->max_definition_level();

        struct ScalarMapSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* key_reader = nullptr;
            ScalarColumnReader* value_reader = nullptr;
            MutableColumnPtr* key_column = nullptr;
            MutableColumnPtr* value_column = nullptr;
            RepeatedParentSinkState parent_state;
            int16_t key_max_definition_level = 0;
            int16_t value_max_definition_level = 0;

            Status read_value_batch(int64_t batch_rows, NestedScalarBatch* out_value_batch) {
                return read_nested_scalar_batch_from_overflow(
                        *value_reader, batch_rows, self->_nullable_definition_level + 1,
                        &self->_value_overflow, out_value_batch);
            }

            Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                            const NestedScalarBatch& candidate_value_batch) {
                return validate_nested_scalar_alignment(self->_name, key_batch,
                                                        candidate_value_batch, "value", "");
            }

            Status start_batch(const NestedScalarBatch& key_batch) {
                RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
                RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch& key_batch, int64_t level_idx) {
                const int16_t def_level = key_batch.def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    return parent_state.append_null_parent(self->_name, "MAP", self->_type);
                }
                parent_state.append_present_parent();
                if (def_level == self->_nullable_definition_level) {
                    return Status::OK();
                }
                return append_entry(key_batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& key_batch, int64_t level_idx) {
                return append_entry(key_batch, level_idx);
            }

            Status append_entry(const NestedScalarBatch& key_batch, int64_t level_idx) {
                RETURN_IF_ERROR(parent_state.require_parent(self->_name));
                if (key_batch.def_levels[level_idx] != key_max_definition_level) {
                    return Status::Corruption("Parquet MAP column {} contains null map key",
                                              self->_name);
                }
                RETURN_IF_ERROR(
                        append_scalar_batch_value(*key_reader, key_batch, level_idx, *key_column));
                RETURN_IF_ERROR(append_nullable_scalar_child(
                        self->_name, "MAP", "value", *value_reader, value_batch, level_idx,
                        value_max_definition_level, *value_column));
                return parent_state.add_entry(self->_name);
            }

            NestedScalarBatch value_batch;
        };

        ScalarMapSink sink;
        sink.self = this;
        sink.key_reader = key_reader;
        sink.value_reader = value_reader;
        sink.key_column = &key_column;
        sink.value_column = &value_column;
        sink.parent_state = {&entry_counts, &parent_nulls};
        sink.key_max_definition_level = key_max_definition_level;
        sink.value_max_definition_level = value_max_definition_level;
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 entry_definition_level, rows, &_key_overflow, sink,
                                                 rows_read));
        if (!_key_overflow.empty()) {
            move_nested_scalar_tail(
                    sink.value_batch,
                    sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                    &_value_overflow);
        }

        map_column->get_keys_ptr() = std::move(key_column);
        map_column->get_values_ptr() = std::move(value_column);
        append_offsets(map_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    if (list_value_reader != nullptr) {
        auto* scalar_list_value_reader =
                dynamic_cast<ScalarColumnReader*>(list_value_reader->element_reader());
        if (scalar_list_value_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet MAP LIST value reader only supports scalar list values for "
                    "column {}",
                    _name);
        }

        auto* list_value_column = array_column_from_output(value_column);
        DORIS_CHECK(list_value_column != nullptr);
        auto* list_value_null_map = null_map_from_nullable_output(value_column);
        auto list_nested_column = list_value_column->get_data_ptr()->assume_mutable();
        std::vector<uint64_t> list_entry_counts;
        NullMap list_parent_nulls;
        const int16_t list_element_slot_definition_level =
                list_value_reader->nullable_definition_level() + 1;
        const int16_t list_element_max_definition_level =
                scalar_list_value_reader->descriptor()->max_definition_level();

        struct ListValueMapSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* key_reader = nullptr;
            ListColumnReader* value_reader = nullptr;
            ScalarColumnReader* scalar_value_reader = nullptr;
            MutableColumnPtr* key_column = nullptr;
            MutableColumnPtr* list_nested_column = nullptr;
            RepeatedParentSinkState parent_state;
            RepeatedChildSinkState list_state;
            int16_t key_max_definition_level = 0;
            int16_t list_element_max_definition_level = 0;
            NestedScalarBatch key_batch;
            int64_t key_level_idx = 0;

            Status read_key_batch(int64_t batch_rows, NestedScalarBatch* out_key_batch) {
                return read_nested_scalar_batch_from_overflow(*key_reader, batch_rows,
                                                              self->_nullable_definition_level + 1,
                                                              &self->_key_overflow, out_key_batch);
            }

            Status validate_key_batch(const NestedScalarBatch& value_batch,
                                      const NestedScalarBatch& candidate_key_batch) {
                if (candidate_key_batch.records_read != value_batch.records_read) {
                    return Status::Corruption(
                            "Parquet MAP key/value rows are not aligned for column {}: key "
                            "rows={}, "
                            "value rows={}",
                            self->_name, candidate_key_batch.records_read,
                            value_batch.records_read);
                }
                return Status::OK();
            }

            Status start_batch(const NestedScalarBatch& value_batch) {
                RETURN_IF_ERROR(read_key_batch(value_batch.records_read, &key_batch));
                RETURN_IF_ERROR(validate_key_batch(value_batch, key_batch));
                key_level_idx = 0;
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch& value_batch, int64_t level_idx) {
                const int16_t def_level = value_batch.def_levels[level_idx];
                if (def_level < self->_nullable_definition_level) {
                    RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, false));
                    return parent_state.append_null_parent(self->_name, "MAP", self->_type);
                }
                parent_state.append_present_parent();
                if (def_level == self->_nullable_definition_level) {
                    RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, false));
                    return Status::OK();
                }
                return append_entry(value_batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& value_batch, int64_t level_idx) {
                if (value_batch.rep_levels[level_idx] < value_reader->repeated_repetition_level()) {
                    return append_entry(value_batch, level_idx);
                }
                return append_list_element(value_batch, level_idx);
            }

            Status append_entry(const NestedScalarBatch& value_batch, int64_t level_idx) {
                RETURN_IF_ERROR(consume_key_slot(value_batch, level_idx, true));
                RETURN_IF_ERROR(append_scalar_batch_value(*key_reader, key_batch, key_level_idx - 1,
                                                          *key_column));
                RETURN_IF_ERROR(parent_state.add_entry(self->_name));
                const int16_t def_level = value_batch.def_levels[level_idx];
                if (def_level < value_reader->nullable_definition_level()) {
                    if (!value_reader->type()->is_nullable()) {
                        return Status::Corruption(
                                "Parquet MAP column {} contains null for non-nullable LIST value",
                                self->_name);
                    }
                    return list_state.append_null_child(self->_name, "MAP", "LIST value",
                                                        value_reader->type());
                }
                list_state.append_present_child();
                if (def_level == value_reader->nullable_definition_level()) {
                    return Status::OK();
                }
                return append_list_element(value_batch, level_idx);
            }

            Status consume_key_slot(const NestedScalarBatch& value_batch, int64_t value_level_idx,
                                    bool require_defined_key) {
                if (key_level_idx >= key_batch.levels_written) {
                    return Status::Corruption(
                            "Parquet MAP key stream ended before value stream for column {}",
                            self->_name);
                }
                if (key_batch.rep_levels[key_level_idx] !=
                    value_batch.rep_levels[value_level_idx]) {
                    return Status::Corruption(
                            "Parquet MAP key/value repetition levels are not aligned for column {}",
                            self->_name);
                }
                if (require_defined_key &&
                    key_batch.def_levels[key_level_idx] != key_max_definition_level) {
                    return Status::Corruption("Parquet MAP column {} contains null map key",
                                              self->_name);
                }
                ++key_level_idx;
                return Status::OK();
            }

            Status append_list_element(const NestedScalarBatch& value_batch, int64_t level_idx) {
                RETURN_IF_ERROR(list_state.require_child(self->_name, "MAP LIST value"));
                RETURN_IF_ERROR(append_nullable_scalar_child(
                        self->_name, "MAP", "LIST value element", *scalar_value_reader, value_batch,
                        level_idx, list_element_max_definition_level, *list_nested_column));
                return list_state.add_entry(self->_name, "MAP LIST value");
            }
        };

        ListValueMapSink sink;
        sink.self = this;
        sink.key_reader = key_reader;
        sink.value_reader = list_value_reader;
        sink.scalar_value_reader = scalar_list_value_reader;
        sink.key_column = &key_column;
        sink.list_nested_column = &list_nested_column;
        sink.parent_state = {&entry_counts, &parent_nulls};
        sink.list_state = {&list_entry_counts, &list_parent_nulls};
        sink.key_max_definition_level = key_max_definition_level;
        sink.list_element_max_definition_level = list_element_max_definition_level;
        RETURN_IF_ERROR(assemble_repeated_levels(
                *scalar_list_value_reader, _repeated_repetition_level,
                list_element_slot_definition_level, rows, &_value_overflow, sink, rows_read));
        if (!_value_overflow.empty()) {
            move_nested_scalar_tail(sink.key_batch, sink.key_level_idx, &_key_overflow);
        }

        list_value_column->get_data_ptr() = std::move(list_nested_column);
        append_offsets(list_value_column->get_offsets(), list_entry_counts);
        append_parent_nulls(list_value_null_map, list_parent_nulls);
        map_column->get_keys_ptr() = std::move(key_column);
        map_column->get_values_ptr() = std::move(value_column);
        append_offsets(map_column->get_offsets(), entry_counts);
        append_parent_nulls(parent_null_map, parent_nulls);
        return Status::OK();
    }

    struct StructMapSink {
        MapColumnReader* self = nullptr;
        ScalarColumnReader* key_reader = nullptr;
        StructColumnReader* value_reader = nullptr;
        MutableColumnPtr* key_column = nullptr;
        MutableColumnPtr* value_column = nullptr;
        RepeatedParentSinkState parent_state;
        int16_t key_max_definition_level = 0;

        Status read_value_batch(int64_t batch_rows, NestedStructBatch* out_value_batch) {
            return read_nested_struct_batch_from_overflow(
                    *value_reader, batch_rows, self->_nullable_definition_level + 1,
                    &self->_struct_value_overflow, out_value_batch);
        }

        Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                        const NestedStructBatch& candidate_value_batch) {
            return validate_nested_struct_alignment(self->_name, key_batch, candidate_value_batch,
                                                    "");
        }

        Status start_batch(const NestedScalarBatch& key_batch) {
            RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
            RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
            return Status::OK();
        }

        Status start_parent(const NestedScalarBatch& key_batch, int64_t level_idx) {
            const int16_t def_level = key_batch.def_levels[level_idx];
            if (def_level < self->_nullable_definition_level) {
                RETURN_IF_ERROR(parent_state.append_null_parent(self->_name, "MAP", self->_type));
                RETURN_IF_ERROR(value_reader->skip_non_scalar_children(1));
                return Status::OK();
            }
            parent_state.append_present_parent();
            if (def_level == self->_nullable_definition_level) {
                RETURN_IF_ERROR(value_reader->skip_non_scalar_children(1));
                return Status::OK();
            }
            return append_entry(key_batch, level_idx);
        }

        Status append_repeated(const NestedScalarBatch& key_batch, int64_t level_idx) {
            return append_entry(key_batch, level_idx);
        }

        Status append_entry(const NestedScalarBatch& key_batch, int64_t level_idx) {
            RETURN_IF_ERROR(parent_state.require_parent(self->_name));
            if (key_batch.def_levels[level_idx] != key_max_definition_level) {
                return Status::Corruption("Parquet MAP column {} contains null map key",
                                          self->_name);
            }
            RETURN_IF_ERROR(
                    append_scalar_batch_value(*key_reader, key_batch, level_idx, *key_column));
            RETURN_IF_ERROR(append_struct_batch_value(*value_reader, value_batch, level_idx,
                                                      *value_column));
            return parent_state.add_entry(self->_name);
        }

        NestedStructBatch value_batch;
    };

    StructMapSink sink;
    sink.self = this;
    sink.key_reader = key_reader;
    sink.value_reader = struct_value_reader;
    sink.key_column = &key_column;
    sink.value_column = &value_column;
    sink.parent_state = {&entry_counts, &parent_nulls};
    sink.key_max_definition_level = key_max_definition_level;
    RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                             entry_definition_level, rows, &_key_overflow, sink,
                                             rows_read));
    if (!_key_overflow.empty()) {
        move_nested_struct_tail(
                sink.value_batch,
                sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                &_struct_value_overflow);
    }

    map_column->get_keys_ptr() = std::move(key_column);
    map_column->get_values_ptr() = std::move(value_column);
    append_offsets(map_column->get_offsets(), entry_counts);
    append_parent_nulls(parent_null_map, parent_nulls);
    return Status::OK();
}

Status MapColumnReader::skip(int64_t rows) {
    if (rows <= 0) {
        return Status::OK();
    }
    DORIS_CHECK(_key_reader != nullptr);
    DORIS_CHECK(_value_reader != nullptr);
    auto* key_reader = dynamic_cast<ScalarColumnReader*>(_key_reader.get());
    auto* value_reader = dynamic_cast<ScalarColumnReader*>(_value_reader.get());
    auto* struct_value_reader = dynamic_cast<StructColumnReader*>(_value_reader.get());
    auto* list_value_reader = dynamic_cast<ListColumnReader*>(_value_reader.get());
    if (key_reader == nullptr || (value_reader == nullptr && struct_value_reader == nullptr &&
                                  list_value_reader == nullptr)) {
        return Status::NotSupported(
                "Current parquet MAP reader only supports scalar key with scalar, scalar-child "
                "STRUCT, or scalar LIST value for column {}",
                _name);
    }

    int64_t rows_read = 0;
    if (value_reader != nullptr) {
        struct SkipSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* value_reader = nullptr;

            Status read_value_batch(int64_t batch_rows, NestedScalarBatch* out_value_batch) {
                return read_nested_scalar_batch_from_overflow(
                        *value_reader, batch_rows, self->_nullable_definition_level + 1,
                        &self->_value_overflow, out_value_batch);
            }

            Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                            const NestedScalarBatch& candidate_value_batch) {
                return validate_nested_scalar_alignment(
                        self->_name, key_batch, candidate_value_batch, "value", " while skipping");
            }

            Status start_batch(const NestedScalarBatch& key_batch) {
                RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
                RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            NestedScalarBatch value_batch;
        };
        SkipSink sink;
        sink.self = this;
        sink.value_reader = value_reader;
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_key_overflow, sink, &rows_read));
        if (!_key_overflow.empty()) {
            move_nested_scalar_tail(
                    sink.value_batch,
                    sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                    &_value_overflow);
        }
    } else if (struct_value_reader != nullptr) {
        struct StructSkipSink {
            MapColumnReader* self = nullptr;
            StructColumnReader* value_reader = nullptr;

            Status read_value_batch(int64_t batch_rows, NestedStructBatch* out_value_batch) {
                return read_nested_struct_batch_from_overflow(
                        *value_reader, batch_rows, self->_nullable_definition_level + 1,
                        &self->_struct_value_overflow, out_value_batch);
            }

            Status validate_value_alignment(const NestedScalarBatch& key_batch,
                                            const NestedStructBatch& candidate_value_batch) {
                return validate_nested_struct_alignment(self->_name, key_batch,
                                                        candidate_value_batch, " while skipping");
            }

            Status start_batch(const NestedScalarBatch& key_batch) {
                RETURN_IF_ERROR(read_value_batch(key_batch.records_read, &value_batch));
                RETURN_IF_ERROR(validate_value_alignment(key_batch, value_batch));
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            Status append_repeated(const NestedScalarBatch&, int64_t) { return Status::OK(); }

            NestedStructBatch value_batch;
        };
        StructSkipSink sink;
        sink.self = this;
        sink.value_reader = struct_value_reader;
        RETURN_IF_ERROR(assemble_repeated_levels(*key_reader, _repeated_repetition_level,
                                                 _nullable_definition_level + 1, rows,
                                                 &_key_overflow, sink, &rows_read));
        if (!_key_overflow.empty()) {
            move_nested_struct_tail(
                    sink.value_batch,
                    sink.value_batch.levels_written - _key_overflow.batch.levels_written,
                    &_struct_value_overflow);
        }
    } else {
        auto* scalar_list_value_reader =
                dynamic_cast<ScalarColumnReader*>(list_value_reader->element_reader());
        if (scalar_list_value_reader == nullptr) {
            return Status::NotSupported(
                    "Current parquet MAP LIST value skip only supports scalar list values for "
                    "column {}",
                    _name);
        }
        struct ListSkipSink {
            MapColumnReader* self = nullptr;
            ScalarColumnReader* key_reader = nullptr;
            ListColumnReader* value_reader = nullptr;
            NestedScalarBatch key_batch;
            int64_t key_level_idx = 0;

            Status read_key_batch(int64_t batch_rows, NestedScalarBatch* out_key_batch) {
                return read_nested_scalar_batch_from_overflow(*key_reader, batch_rows,
                                                              self->_nullable_definition_level + 1,
                                                              &self->_key_overflow, out_key_batch);
            }

            Status validate_key_batch(const NestedScalarBatch& value_batch,
                                      const NestedScalarBatch& candidate_key_batch) {
                if (candidate_key_batch.records_read != value_batch.records_read) {
                    return Status::Corruption(
                            "Parquet MAP key/value rows are not aligned for column {} while "
                            "skipping",
                            self->_name);
                }
                return Status::OK();
            }

            Status start_batch(const NestedScalarBatch& value_batch) {
                RETURN_IF_ERROR(read_key_batch(value_batch.records_read, &key_batch));
                RETURN_IF_ERROR(validate_key_batch(value_batch, key_batch));
                key_level_idx = 0;
                return Status::OK();
            }

            Status start_parent(const NestedScalarBatch& value_batch, int64_t level_idx) {
                return consume_key_slot(value_batch, level_idx);
            }

            Status append_repeated(const NestedScalarBatch& value_batch, int64_t level_idx) {
                if (value_batch.rep_levels[level_idx] < value_reader->repeated_repetition_level()) {
                    return consume_key_slot(value_batch, level_idx);
                }
                return Status::OK();
            }

            Status consume_key_slot(const NestedScalarBatch& value_batch, int64_t value_level_idx) {
                if (key_level_idx >= key_batch.levels_written) {
                    return Status::Corruption(
                            "Parquet MAP key stream ended before value stream for column {} while "
                            "skipping",
                            self->_name);
                }
                if (key_batch.rep_levels[key_level_idx] !=
                    value_batch.rep_levels[value_level_idx]) {
                    return Status::Corruption(
                            "Parquet MAP key/value repetition levels are not aligned for column {} "
                            "while skipping",
                            self->_name);
                }
                ++key_level_idx;
                return Status::OK();
            }
        };
        ListSkipSink sink;
        sink.self = this;
        sink.key_reader = key_reader;
        sink.value_reader = list_value_reader;
        RETURN_IF_ERROR(assemble_repeated_levels(*scalar_list_value_reader,
                                                 _repeated_repetition_level,
                                                 list_value_reader->nullable_definition_level() + 1,
                                                 rows, &_value_overflow, sink, &rows_read));
        if (!_value_overflow.empty()) {
            move_nested_scalar_tail(sink.key_batch, sink.key_level_idx, &_key_overflow);
        }
    }
    if (rows_read != rows) {
        return Status::Corruption("Failed to skip parquet MAP column {}: skipped {} of {} rows",
                                  _name, rows_read, rows);
    }
    return Status::OK();
}

} // namespace doris::parquet
