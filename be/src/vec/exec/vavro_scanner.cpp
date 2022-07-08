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

#include "vec/exec/vavro_scanner.h"

#include <fmt/format.h>

#include <algorithm>

#include "runtime/runtime_state.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

void deserializeColumnNoop(vectorized::IColumn*, SlotDescriptor*, avro::Decoder&, int) {}

VAvroScanner::VAvroScanner(RuntimeState* state, RuntimeProfile* profile,
                           const TBrokerScanRangeParams& params,
                           const std::vector<TBrokerRangeDesc>& ranges,
                           const std::vector<TNetworkAddress>& broker_addresses,
                           const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : AvroScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          _cur_vavro_reader(nullptr) {}

Status VAvroScanner::get_next(vectorized::Block* output_block, bool* eof) {
    SCOPED_TIMER(_read_timer);
    RETURN_IF_ERROR(_init_src_block());
    const int batch_size = _state->batch_size();

    auto columns = _src_block.mutate_columns();
    // Get one line
    while (columns[0]->size() < batch_size && !_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // if there isn't any more reader, break this
            if (_scanner_eof) {
                break;
            }
        }

        bool is_empty_row = false;
        RETURN_IF_ERROR(_cur_vavro_reader->read_avro_column(columns, _src_slot_descs, &is_empty_row,
                                                            &_cur_reader_eof));

        if (is_empty_row) {
            continue;
        }
    }
    COUNTER_UPDATE(_rows_read_counter, columns[0]->size());
    SCOPED_TIMER(_materialize_timer);
    return _fill_dest_block(output_block, eof);
}

Status VAvroScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(AvroScanner::open_file_reader());
    RETURN_IF_ERROR(open_vavro_reader());
    _next_range++;
    return Status::OK();
}

Status VAvroScanner::open_vavro_reader() {
    if (_cur_vavro_reader != nullptr) {
        _cur_vavro_reader.reset();
    }
    _cur_vavro_reader.reset(new VAvroReader(_state, _counter, _profile, _cur_file_reader, nullptr));
    RETURN_IF_ERROR(_cur_vavro_reader->init(_avro_schema_name));
    return Status::OK();
}

VAvroReader::VAvroReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                         FileReader* file_reader, LineReader* line_reader)
        : AvroReader(state, counter, profile, file_reader, line_reader) {}

VAvroReader::~VAvroReader() {}

VAvroReader::DeserializeColumnFn VAvroReader::createDeserializeColumnFn(avro::NodePtr root_node,
                                                                        SlotDescriptor* slot_desc) {
    // const char* str_value = nullptr;
    char tmp_buf[128] = {0};
    int32_t wbytes = 0;

    switch (root_node->type()) {
    case avro::AVRO_BYTES:
        [[fallthrough]];
    case avro::AVRO_STRING: {
        return [val_string = std::string(), this](vectorized::IColumn* column_ptr,
                                                  SlotDescriptor* slot_desc, avro::Decoder& decoder,
                                                  int nullcount) mutable {
            decoder.decodeString(val_string);
            if (val_string.empty()) {
                if (slot_desc->is_nullable()) {
                    //tuple->set_null(slot_desc->null_indicator_offset());
                    auto* nullable_column =
                            reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
                    nullable_column->insert_default();
                    nullcount++;
                } else {
                    throw avro::Exception("NULL data for non-nullable column" +
                                          slot_desc->col_name());
                }
            } else {
                //_fill_slot(tuple, slot_desc, tuple_pool, (uint8_t*)val_string.c_str(),
                //           (int32_t)strlen(val_string.c_str()));
                DCHECK(slot_desc->type().type == TYPE_VARCHAR);
                assert_cast<ColumnString*>(column_ptr)
                        ->insert_data(val_string.c_str(), (int32_t)strlen(val_string.c_str()));
            }
        };
    } break;
    case avro::AVRO_INT: {
        return [tmp_buf, wbytes, this](vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            int32_t val_int = decoder.decodeInt();
            wbytes = sprintf((char*)tmp_buf, "%d", val_int);
            //_fill_slot(tuple, slot_desc, tuple_pool, (char*)tmp_buf, wbytes);
            DCHECK(slot_desc->type().type == TYPE_VARCHAR);
            assert_cast<ColumnString*>(column_ptr)->insert_data((char*)tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_LONG: {
        return [tmp_buf, wbytes, this](vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            int64_t val_long = decoder.decodeLong();
            wbytes = sprintf((char*)tmp_buf, "%ld", val_long);
            //_fill_slot(tuple, slot_desc, tuple_pool, (char*)tmp_buf, wbytes);
            DCHECK(slot_desc->type().type == TYPE_VARCHAR);
            assert_cast<ColumnString*>(column_ptr)->insert_data((char*)tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_FLOAT: {
        return [tmp_buf, wbytes, this](vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            float val_float = decoder.decodeFloat();
            wbytes = sprintf((char*)tmp_buf, "%f", val_float);
            //_fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
            DCHECK(slot_desc->type().type == TYPE_VARCHAR);
            assert_cast<ColumnString*>(column_ptr)->insert_data((char*)tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_DOUBLE: {
        return [tmp_buf, wbytes, this](vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            double val_double = decoder.decodeDouble();
            wbytes = sprintf((char*)tmp_buf, "%lf", val_double);
            //_fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
            DCHECK(slot_desc->type().type == TYPE_VARCHAR);
            assert_cast<ColumnString*>(column_ptr)->insert_data((char*)tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_BOOL: {
        return [tmp_buf, wbytes, this](vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            bool val_bool = decoder.decodeBool();
            wbytes = sprintf((char*)tmp_buf, "%d", val_bool);
            //_fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
            DCHECK(slot_desc->type().type == TYPE_VARCHAR);
            assert_cast<ColumnString*>(column_ptr)->insert_data((char*)tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_ARRAY: {
        // should have array type info
        // TODO :
    } break;
    case avro::AVRO_UNION: {
        auto nullable_deserializer = [root_node, slot_desc, this](size_t non_null_union_index) {
            auto nested_deserialize =
                    createDeserializeColumnFn(root_node->leafAt(non_null_union_index), slot_desc);
            return [non_null_union_index, nested_deserialize, this](
                           vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                           avro::Decoder& decoder, int nullcount) mutable {
                size_t union_index = decoder.decodeUnionIndex();
                if (union_index == non_null_union_index) {
                    nested_deserialize(column_ptr, slot_desc, decoder, nullcount);
                } else {
                    column_ptr->insert_default();
                }
            };
        };
        if (root_node->leaves() == 2 && slot_desc->is_nullable()) {
            if (root_node->leafAt(0)->type() == avro::AVRO_NULL) return nullable_deserializer(1);
            if (root_node->leafAt(1)->type() == avro::AVRO_NULL) return nullable_deserializer(0);
        }
    } break;
    case avro::AVRO_NULL: {
        if (slot_desc->is_nullable()) {
            return [this](vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                          avro::Decoder& decoder, int nullcount) mutable {
                // tuple->set_null(slot_desc->null_indicator_offset());
                auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
                nullable_column->insert_default();
                nullcount++;
            };
        } else {
            throw avro::Exception("NULL data for non-nullable column" + slot_desc->col_name());
        }
    } break;
    case avro::AVRO_ENUM:
        [[fallthrough]];
    case avro::AVRO_FIXED:
        [[fallthrough]];
    case avro::AVRO_MAP:
        [[fallthrough]];
    case avro::AVRO_RECORD:
        [[fallthrough]];
    default: {
        throw avro::Exception("Not support " + root_node->type() + std::string(" type yet."));
    } break;
    }
    throw avro::Exception("Type " + slot_desc->type().type +
                          std::string(" is not compatible with Avro ") +
                          avro::ValidSchema(root_node).toJson(false));
}

VAvroReader::SkipColumnFn VAvroReader::createSkipColumnFn(avro::NodePtr root_node) {
    switch (root_node->type()) {
    case avro::AVRO_STRING:
        return [](avro::Decoder& decoder) { decoder.skipString(); };
    case avro::AVRO_BYTES:
        return [](avro::Decoder& decoder) { decoder.skipBytes(); };
    case avro::AVRO_INT:
        return [](avro::Decoder& decoder) { decoder.decodeInt(); };
    case avro::AVRO_LONG:
        return [](avro::Decoder& decoder) { decoder.decodeLong(); };
    case avro::AVRO_FLOAT:
        return [](avro::Decoder& decoder) { decoder.decodeFloat(); };
    case avro::AVRO_DOUBLE:
        return [](avro::Decoder& decoder) { decoder.decodeDouble(); };
    case avro::AVRO_BOOL:
        return [](avro::Decoder& decoder) { decoder.decodeBool(); };
    case avro::AVRO_ARRAY: {
        auto nested_skip_fn = createSkipFn(root_node->leafAt(0));
        return [nested_skip_fn](avro::Decoder& decoder) {
            for (size_t n = decoder.arrayStart(); n != 0; n = decoder.arrayNext()) {
                for (size_t i = 0; i < n; ++i) {
                    nested_skip_fn(decoder);
                }
            }
        };
    }
    case avro::AVRO_UNION: {
        std::vector<SkipColumnFn> union_skip_fns;
        for (size_t i = 0; i < root_node->leaves(); i++) {
            union_skip_fns.push_back(createSkipColumnFn(root_node->leafAt(i)));
        }
        return [union_skip_fns](avro::Decoder& decoder) {
            union_skip_fns[decoder.decodeUnionIndex()](decoder);
        };
    }
    case avro::AVRO_NULL:
        return [](avro::Decoder& decoder) { decoder.decodeNull(); };
    case avro::AVRO_ENUM:
        return [](avro::Decoder& decoder) { decoder.decodeEnum(); };
    case avro::AVRO_FIXED: {
        auto fixed_size = root_node->fixedSize();
        return [fixed_size](avro::Decoder& decoder) { decoder.skipFixed(fixed_size); };
    }
    case avro::AVRO_MAP: {
        auto value_skip_fn = createSkipColumnFn(root_node->leafAt(1));
        return [value_skip_fn](avro::Decoder& decoder) {
            for (size_t n = decoder.mapStart(); n != 0; n = decoder.mapNext()) {
                for (size_t i = 0; i < n; ++i) {
                    decoder.skipString();
                    value_skip_fn(decoder);
                }
            }
        };
    }
    case avro::AVRO_RECORD: {
        std::vector<SkipFn> field_skip_fns;
        for (size_t i = 0; i < root_node->leaves(); i++) {
            field_skip_fns.push_back(createSkipColumnFn(root_node->leafAt(i)));
        }
        return [field_skip_fns](avro::Decoder& decoder) {
            for (auto& skip_fn : field_skip_fns) {
                skip_fn(decoder);
            }
        };
    }
    default:
        throw avro::Exception("Unsupported Avro type");
    }
}

Status VAvroReader::_get_field_mapping_column(const std::vector<SlotDescriptor*>& slot_descs) {
    _field_mapping.resize(_schema.root()->leaves(), -1);
    for (size_t i = 0; i < _schema.root()->leaves(); ++i) {
        _skip_fns_column.push_back(createSkipColumnFn(_schema.root()->leafAt(i)));
        _deserialize_fns_column.push_back(&deserializeColumnNoop);
    }

    size_t field_index;
    for (size_t i = 0; i < slot_descs.size(); ++i) {
        if (!_schema.root()->nameIndex(slot_descs[i]->col_name(), field_index)) {
            return Status::DataQualityError("column " + slot_descs[i]->col_name() +
                                            " is not found in schema.");
        }
        auto field_schema = _schema.root()->leafAt(field_index);
        try {
            _deserialize_fns_column[field_index] =
                    createDeserializeColumnFn(field_schema, slot_descs[i]);
        } catch (...) {
            return Status::DataQualityError("column " + slot_descs[i]->col_name() +
                                            " failed to create deserialize function.");
        }
        _field_mapping[field_index] = i;
    }

    return Status::OK();
}

Status VAvroReader::deserialize_column(std::vector<MutableColumnPtr>& columns,
                                       const std::vector<SlotDescriptor*>& slot_descs,
                                       bool* is_empty_row, bool* eof) {
    size_t size = 0;
    Status st = _get_avro_doc(&size, eof);

    if (st.is_data_quality_error()) {
        return Status::DataQualityError("avro data quality bad.");
    }
    RETURN_IF_ERROR(st);
    if (size == 0 || *eof) {
        *is_empty_row = true;
        return Status::OK();
    }

    int nullcount = 0;
    // do deserialize
    for (size_t i = 0; i < _field_mapping.size(); i++) {
        int dest_index = _field_mapping[i];
        if (dest_index >= 0) {
            try {
                auto* column_ptr = columns[dest_index].get();
                if (slot_descs[dest_index]->is_nullable()) {
                    auto* nullable_column =
                            reinterpret_cast<vectorized::ColumnNullable*>(column_ptr);
                    nullable_column->get_null_map_data().push_back(0);
                    column_ptr = &nullable_column->get_nested_column();
                }

                _deserialize_fns_column[i](column_ptr, slot_descs[dest_index], *_decoder,
                                           nullcount);
            } catch (avro::Exception& e) {
                return Status::DataQualityError(
                        std::string(" Error occured in deserialize fns: ") + e.what() +
                        ". Please make sure the SCHEMA file and DATA SCHEMA are consistent");
            }
        } else {
            try {
                _skip_fns_column[i](*_decoder);
            } catch (avro::Exception& e) {
                return Status::DataQualityError(
                        std::string("Error occured in skip fns: ") + e.what() +
                        ". Please make sure the SCHEMA file and DATA SCHEMA are consistent");
            }
        }
    }

    if (nullcount == slot_descs.size()) {
        _counter->num_rows_filtered++;
        // valid = false;
    }
    return Status::OK();
}

Status VAvroReader::read_avro_column(std::vector<MutableColumnPtr>& columns,
                                     const std::vector<SlotDescriptor*>& slot_descs,
                                     bool* is_empty_row, bool* eof) {
    Status st = _get_field_mapping_column(slot_descs);
    RETURN_IF_ERROR(st);
    return VAvroReader::deserialize_column(columns, slot_descs, is_empty_row, eof);
}
} // namespace doris::vectorized