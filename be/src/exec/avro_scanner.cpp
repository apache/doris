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

#include <exec/avro_scanner.h>

#include <fstream>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "env/env.h"
#include "exprs/expr.h"
#include "gutil/strings/split.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace doris {

void deserializeNoop(MemPool*, Tuple*, SlotDescriptor*, avro::Decoder&, int) {}

AvroScanner::AvroScanner(RuntimeState* state, RuntimeProfile* profile,
                         const TBrokerScanRangeParams& params,
                         const std::vector<TBrokerRangeDesc>& ranges,
                         const std::vector<TNetworkAddress>& broker_addresses,
                         const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : BaseScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          _ranges(ranges),
          _broker_addresses(broker_addresses),
          _cur_file_reader(nullptr),
          _cur_avro_reader(nullptr),
          _next_range(0),
          _cur_reader_eof(false),
          _scanner_eof(false) {}

AvroScanner::~AvroScanner() {
    close();
}

Status AvroScanner::open() {
    Status st = BaseScanner::open();
    RETURN_IF_ERROR(st);
    return st;
}

Status AvroScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple) {
    SCOPED_TIMER(_read_timer);

    // read one line
    while (!_scanner_eof) {
        if (_cur_file_reader == nullptr || _cur_reader_eof) {
            RETURN_IF_ERROR(open_next_reader());
            // If there isn't any more reader, break this
            if (_scanner_eof) {
                break;
            }
        }

        bool is_empty_row = false;

        RETURN_IF_ERROR(_cur_avro_reader->read_avro_row(_src_tuple, _src_slot_descs, tuple_pool,
                                                        &is_empty_row, &_cur_reader_eof));
        if (is_empty_row) {
            continue;
        }
        COUNTER_UPDATE(_rows_read_counter, 1);
        SCOPED_TIMER(_materialize_timer);
        if (fill_dest_tuple(tuple, tuple_pool, fill_tuple)) {
            break; // break if true
        }
    }
    if (_scanner_eof) {
        *eof = true;
    } else {
        *eof = false;
    }
    return Status::OK();
}

void AvroScanner::close() {
    BaseScanner::close();
    if (_cur_avro_reader != nullptr) {
        delete _cur_avro_reader;
        _cur_avro_reader = nullptr;
    }
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
        } else {
            delete _cur_file_reader;
        }
        _cur_file_reader = nullptr;
    }
}

Status AvroScanner::open_file_reader() {
    if (_cur_file_reader != nullptr) {
        if (_stream_load_pipe != nullptr) {
            _stream_load_pipe.reset();
            _cur_file_reader = nullptr;
        } else {
            delete _cur_file_reader;
            _cur_file_reader = nullptr;
        }
    }

    const TBrokerRangeDesc& range = _ranges[_next_range];

    if (range.__isset.avro_schema_name) {
        _avro_schema_name = range.avro_schema_name;
    }

    switch (range.file_type) {
    case TFileType::FILE_STREAM: {
        _stream_load_pipe = _state->exec_env()->load_stream_mgr()->get(range.load_id);
        if (_stream_load_pipe == nullptr) {
            VLOG_NOTICE << "unknown stream load id: " << UniqueId(range.load_id);
            return Status::InternalError("unknown stream load id");
        }
        _cur_file_reader = _stream_load_pipe.get();
        break;
    }
    case TFileType::FILE_LOCAL:
    case TFileType::FILE_BROKER:
    case TFileType::FILE_S3:
    default: {
        std::stringstream ss;
        ss << "Only support stream type. Unsupport file type, type=" << range.file_type;
        return Status::InternalError(ss.str());
    }
    }
    _cur_reader_eof = false;
    return Status::OK();
}

Status AvroScanner::open_avro_reader() {
    if (_cur_avro_reader != nullptr) {
        delete _cur_avro_reader;
        _cur_avro_reader = nullptr;
    }

    _cur_avro_reader = new AvroReader(_state, _counter, _profile, _cur_file_reader, nullptr);
    RETURN_IF_ERROR(_cur_avro_reader->init(_avro_schema_name));
    return Status::OK();
}

Status AvroScanner::open_next_reader() {
    if (_next_range >= _ranges.size()) {
        _scanner_eof = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(open_file_reader());
    RETURN_IF_ERROR(open_avro_reader());
    _next_range++;

    return Status::OK();
}

////// class AvroReader
AvroReader::AvroReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                       FileReader* file_reader, LineReader* line_reader)
        : _next_line(0),
          _total_lines(0),
          _state(state),
          _counter(counter),
          _profile(profile),
          _file_reader(file_reader),
          _line_reader(line_reader),
          _closed(false),
          _decoder(avro::binaryDecoder()),
          _in(nullptr) {
    _bytes_read_counter = ADD_COUNTER(_profile, "BytesRead", TUnit::BYTES);
    _read_timer = ADD_TIMER(_profile, "ReadTime");
    _file_read_timer = ADD_TIMER(_profile, "FileReadTime");
}

AvroReader::~AvroReader() {
    _close();
}

Status AvroReader::init(std::string avro_schema_name) {
    std::string schema_path = config::custom_config_dir + std::string("/") + avro_schema_name;
    bool exist = FileUtils::check_exist(schema_path);
    if (!exist) {
        return Status::InternalError("there is no avro schema file at " +
                                     schema_path +
                                     ". Please put an schema file in json format.");
    }

    try {
        _schema = avro::compileJsonSchemaFromFile(schema_path.c_str());
    } catch (avro::Exception& e) {
        return Status::InternalError(std::string("getting schema from json failed.") + e.what());
    }

    if (_schema.root()->type() != avro::AVRO_RECORD) {
        return Status::DataQualityError("Root schema must be a record");
    }

    return Status::OK();
}

void AvroReader::_close() {
    if (_closed) {
        return;
    }
    _closed = true;
}

Status AvroReader::_get_avro_doc(size_t* size, bool* eof, MemPool* tuple_pool, Tuple* tuple,
                                 const std::vector<SlotDescriptor*>& slot_descs) {
    SCOPED_TIMER(_file_read_timer);

    if (_avro_str_ptr != nullptr) {
        _avro_str_ptr.reset();
    }
    int64_t length = 0;
    RETURN_IF_ERROR(_file_reader->read_one_message(&_avro_str_ptr, &length));

    *size = length;
    if (length == 0) {
        *eof = true;
    }

    _bytes_read_counter += *size;
    if (*eof) {
        return Status::OK();
    }

    try {
        auto schema_root = _schema.root();
        _in = avro::memoryInputStream(_avro_str_ptr.get(), *size);
        _decoder->init(*_in);
    } catch (avro::Exception& e) {
        return Status::DataQualityError(std::string("data quality is not good.") + e.what());
    }

    return Status::OK();
}

void AvroReader::_fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                            const uint8_t* value, int32_t len) {
    tuple->set_not_null(slot_desc->null_indicator_offset());
    void* slot = tuple->get_slot(slot_desc->tuple_offset());
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
    str_slot->ptr = reinterpret_cast<char*>(mem_pool->allocate(len));
    memcpy(str_slot->ptr, value, len);
    str_slot->len = len;
}

AvroReader::DeserializeFn AvroReader::createDeserializeFn(avro::NodePtr root_node,
                                                          SlotDescriptor* slot_desc) {
    uint8_t tmp_buf[128] = {0};
    int32_t wbytes = 0;
    switch (root_node->type()) {
    case avro::AVRO_BYTES:
        [[fallthrough]];
    case avro::AVRO_STRING: {
        return [val_string = std::string(), this](MemPool* tuple_pool, Tuple* tuple,
                                                  SlotDescriptor* slot_desc, avro::Decoder& decoder,
                                                  int nullcount) mutable {
            decoder.decodeString(val_string);
            if (val_string.empty()) {
                if (slot_desc->is_nullable()) {
                    tuple->set_null(slot_desc->null_indicator_offset());
                    nullcount++;
                } else {
                    throw avro::Exception("NULL data for non-nullable column" +
                                          slot_desc->col_name());
                }
            } else {
                _fill_slot(tuple, slot_desc, tuple_pool, (uint8_t*)val_string.c_str(),
                           (int32_t)strlen(val_string.c_str()));
            }
        };
    } break;
    case avro::AVRO_INT: {
        return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            int32_t val_int = decoder.decodeInt();
            wbytes = sprintf((char*)tmp_buf, "%d", val_int);
            _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_LONG: {
        return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            int64_t val_long = decoder.decodeLong();
            wbytes = sprintf((char*)tmp_buf, "%ld", val_long);
            _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_FLOAT: {
        return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            float val_float = decoder.decodeFloat();
            wbytes = sprintf((char*)tmp_buf, "%f", val_float);
            _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_DOUBLE: {
        return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            double val_double = decoder.decodeDouble();
            wbytes = sprintf((char*)tmp_buf, "%lf", val_double);
            _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_BOOL: {
        return [tmp_buf, wbytes, this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc,
                                       avro::Decoder& decoder, int nullcount) mutable {
            bool val_bool = decoder.decodeBool();
            wbytes = sprintf((char*)tmp_buf, "%d", val_bool);
            _fill_slot(tuple, slot_desc, tuple_pool, tmp_buf, wbytes);
        };
    } break;
    case avro::AVRO_ARRAY: {
        // should have array type info
        // TODO :
    } break;
    case avro::AVRO_UNION: {
        auto nullable_deserializer = [root_node, slot_desc, this](size_t non_null_union_index) {
            auto nested_deserialize =
                    createDeserializeFn(root_node->leafAt(non_null_union_index), slot_desc);
            return [non_null_union_index, nested_deserialize, this](
                           MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc,
                           avro::Decoder& decoder, int nullcount) mutable {
                size_t union_index = decoder.decodeUnionIndex();
                if (union_index == non_null_union_index) {
                    nested_deserialize(tuple_pool, tuple, slot_desc, decoder, nullcount);
                } else {
                    tuple->set_null(slot_desc->null_indicator_offset());
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
            return [this](MemPool* tuple_pool, Tuple* tuple, SlotDescriptor* slot_desc,
                          avro::Decoder& decoder, int nullcount) mutable {
                tuple->set_null(slot_desc->null_indicator_offset());
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

AvroReader::SkipFn AvroReader::createSkipFn(avro::NodePtr root_node) {
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
        std::vector<SkipFn> union_skip_fns;
        for (size_t i = 0; i < root_node->leaves(); i++) {
            union_skip_fns.push_back(createSkipFn(root_node->leafAt(i)));
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
        auto value_skip_fn = createSkipFn(root_node->leafAt(1));
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
            field_skip_fns.push_back(createSkipFn(root_node->leafAt(i)));
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

Status AvroReader::_get_field_mapping(const std::vector<SlotDescriptor*>& slot_descs) {
    _field_mapping.resize(_schema.root()->leaves(), -1);
    for (size_t i = 0; i < _schema.root()->leaves(); ++i) {
        _skip_fns.push_back(createSkipFn(_schema.root()->leafAt(i)));
        _deserialize_fns.push_back(&deserializeNoop);
    }

    size_t field_index;
    for (size_t i = 0; i < slot_descs.size(); ++i) {
        if (!_schema.root()->nameIndex(slot_descs[i]->col_name(), field_index)) {
            return Status::DataQualityError("column " + slot_descs[i]->col_name() +
                                            " is not found in schema.");
        }
        auto field_schema = _schema.root()->leafAt(field_index);
        try {
            _deserialize_fns[field_index] = createDeserializeFn(field_schema, slot_descs[i]);
        } catch (...) {
            return Status::DataQualityError("column " + slot_descs[i]->col_name() +
                                            " failed to create deserialize function.");
        }
        _field_mapping[field_index] = i;
    }

    return Status::OK();
}

Status AvroReader::deserialize_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                   MemPool* tuple_pool, bool* is_empty_row, bool* eof) {
    size_t size = 0;
    // get data, init _decoder
    Status st = _get_avro_doc(&size, eof, tuple_pool, tuple, slot_descs);
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
        if (_field_mapping[i] >= 0) {
            try {
                _deserialize_fns[i](tuple_pool, tuple, slot_descs[_field_mapping[i]], *_decoder,
                                    nullcount);
            } catch (avro::Exception& e) {
                return Status::DataQualityError(
                        std::string(" Error occured in deserialize fns: ") + e.what() +
                        ". Please make sure the SCHEMA file and DATA SCHEMA are consistent");
            }
        } else {
            try {
                _skip_fns[i](*_decoder);
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
Status AvroReader::read_avro_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                 MemPool* tuple_pool, bool* is_empty_row, bool* eof) {
    Status st = _get_field_mapping(slot_descs);
    RETURN_IF_ERROR(st);
    return AvroReader::deserialize_row(tuple, slot_descs, tuple_pool, is_empty_row, eof);
}

} // namespace doris
