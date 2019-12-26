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

#include "exec/orc_scanner.h"

#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "exprs/expr.h"
#include "exec/local_file_reader.h"
#include "exec/broker_reader.h"

namespace doris {

ORCFileStream::~ORCFileStream() {
    if (_file) {
        _file->close();
        delete _file;
        _file = nullptr;
    }
}

uint64_t ORCFileStream::getLength() const {
    return _file->size();
}

uint64_t ORCFileStream::getNaturalReadSize() const {
    return 128 * 1024;
}

void ORCFileStream::read(void *buf, uint64_t length, uint64_t offset) {
    if (!buf) {
        throw orc::ParseError("Buffer is null");
    }

    int64_t bytes_read = 0;
    int64_t reads = 0;
    while (bytes_read < length) {
        Status result = _file->readat(offset, length - bytes_read, &reads, buf);
        if (!result.ok()) {
            throw orc::ParseError("Bad read of " + _filename);
        }
        if (reads == 0) {
            break;
        }
        bytes_read += reads;// total read bytes
        offset += reads;
        buf = (char *) buf + reads;
    }
    if (length != bytes_read) {
        throw orc::ParseError("Short read of " + _filename
                              + ". expected :" + std::to_string(length) + ", actual : " + std::to_string(bytes_read));
    }
}

const std::string& ORCFileStream::getName() const {
    return _filename;
}

ORCScanner::ORCScanner(RuntimeState *state,
        RuntimeProfile *profile,
        const TBrokerScanRangeParams &params,
        const std::vector<TBrokerRangeDesc> &ranges,
        const std::vector<TNetworkAddress> &broker_addresses,
        ScannerCounter *counter) : BaseScanner(state, profile, params, counter),
            _ranges(ranges),
            _broker_addresses(broker_addresses),
            // _splittable(params.splittable),
            _next_range(0),
            _cur_file_eof(true),
            _scanner_eof(false),
            _total_groups(0),
            _current_group(0),
            _rows_of_group(0),
            _current_line_of_group(0) {}

ORCScanner::~ORCScanner() {
    close();
}

Status ORCScanner::open() {
    return BaseScanner::open();
}

Status ORCScanner::get_next(Tuple *tuple, MemPool *tuple_pool, bool *eof) {
    try {
        SCOPED_TIMER(_read_timer);
        // Get one line
        while (!_scanner_eof) {
            if (_cur_file_eof) {
                RETURN_IF_ERROR(open_next_reader());
                if (_scanner_eof) {
                    *eof = true;
                    return Status::OK();
                } else {
                    _cur_file_eof = false;
                }
            }
            if (_current_line_of_group >= _rows_of_group) { // read next stripe
                if (_current_group >= _total_groups) {
                    _cur_file_eof = true;
                    continue;
                }
                _rows_of_group = _reader->getStripe(_current_group)->getNumberOfRows();
                _batch = _row_reader->createRowBatch(_rows_of_group);
                _row_reader->next(*_batch.get());

                _current_line_of_group = 0;
                ++_current_group;
            }

            const std::vector<orc::ColumnVectorBatch *>& batch_vec = ((orc::StructVectorBatch *) _batch.get())->fields;
            for (int column_ipos = 0; column_ipos < _num_of_columns_from_file; ++column_ipos) {
                auto slot_desc = _src_slot_descs[column_ipos];
                int position_in_orc = _column_name_map_orc_index.find(slot_desc->col_name())->second;
                orc::ColumnVectorBatch *cvb = batch_vec[position_in_orc];

                if (cvb->hasNulls && !cvb->notNull[_current_line_of_group]) {
                    //all src tuple is nullable , judge null in dest_tuple
                    _src_tuple->set_null(slot_desc->null_indicator_offset());
                } else {
                    int32_t wbytes = 0;
                    uint8_t tmp_buf[128] = {0};
                    _src_tuple->set_not_null(slot_desc->null_indicator_offset());
                    void *slot = _src_tuple->get_slot(slot_desc->tuple_offset());
                    StringValue *str_slot = reinterpret_cast<StringValue *>(slot);

                    switch (_row_reader->getSelectedType().getSubtype(position_in_orc)->getKind()) {
                        case orc::BOOLEAN: {
                            int64_t value = ((orc::LongVectorBatch *) cvb)->data[_current_line_of_group];
                            if (value == 0) {
                                str_slot->ptr = reinterpret_cast<char *>(tuple_pool->allocate(5));
                                memcpy(str_slot->ptr, "false", 5);
                                str_slot->len = 5;
                            } else {
                                str_slot->ptr = reinterpret_cast<char *>(tuple_pool->allocate(4));
                                memcpy(str_slot->ptr, "true", 4);
                                str_slot->len = 4;
                            }
                            break;
                        }
                        case orc::INT:
                        case orc::SHORT:
                        case orc::LONG:
                        case orc::DATE: {
                            int64_t value = ((orc::LongVectorBatch *) cvb)->data[_current_line_of_group];
                            wbytes = sprintf((char *) tmp_buf, "%ld", value);
                            str_slot->ptr = reinterpret_cast<char *>(tuple_pool->allocate(wbytes));
                            memcpy(str_slot->ptr, tmp_buf, wbytes);
                            str_slot->len = wbytes;
                            break;
                        }
                        case orc::FLOAT:
                        case orc::DOUBLE: {
                            double value = ((orc::DoubleVectorBatch *) cvb)->data[_current_line_of_group];
                            wbytes = sprintf((char *) tmp_buf, "%f", value);
                            str_slot->ptr = reinterpret_cast<char *>(tuple_pool->allocate(wbytes));
                            memcpy(str_slot->ptr, tmp_buf, wbytes);
                            str_slot->len = wbytes;
                            break;
                        }
                        case orc::BINARY:
                        case orc::CHAR:
                        case orc::VARCHAR:
                        case orc::STRING: {
                            char *value = ((orc::StringVectorBatch *) cvb)->data[_current_line_of_group];
                            wbytes = ((orc::StringVectorBatch *) cvb)->length[_current_line_of_group];
                            str_slot->ptr = reinterpret_cast<char *>(tuple_pool->allocate(wbytes));
                            memcpy(str_slot->ptr, value, wbytes);
                            str_slot->len = wbytes;
                            break;
                        }
                            //TODO (lhy) : support more orc type
                        case orc::TIMESTAMP:
                        case orc::DECIMAL:
                        default: {
                            std::stringstream str_error;
                            str_error << "The field name(" << slot_desc->col_name() << ") type not support. ";
                            LOG(WARNING) << str_error.str();
                            return Status::InternalError(str_error.str());
                        }
                    }
                }
            }
            ++_current_line_of_group;

            // range of current file
            const TBrokerRangeDesc &range = _ranges.at(_next_range - 1);
            if (range.__isset.num_of_columns_from_file) {
                fill_slots_of_columns_from_path(range.num_of_columns_from_file, range.columns_from_path);
            }
            COUNTER_UPDATE(_rows_read_counter, 1);
            SCOPED_TIMER(_materialize_timer);
            if (fill_dest_tuple(Slice(), tuple, tuple_pool)) {
                break; // get one line, break from while
            } // else skip this line and continue get_next to return
        }
        return Status::OK();
    } catch (orc::ParseError &e) {
        std::stringstream str_error;
        str_error << "ParseError : " << e.what();
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    } catch (orc::InvalidArgument &e) {
        std::stringstream str_error;
        str_error << "ParseError : " << e.what();
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }
}

Status ORCScanner::open_next_reader() {
    while (true) {
        if (_next_range >= _ranges.size()) {
            _scanner_eof = true;
            return Status::OK();
        }
        const TBrokerRangeDesc &range = _ranges[_next_range++];
        std::unique_ptr<FileReader> file_reader;
        switch (range.file_type) {
            case TFileType::FILE_LOCAL: {
                file_reader.reset(new LocalFileReader(range.path, range.start_offset));
                break;
            }
            case TFileType::FILE_BROKER: {
                int64_t file_size = 0;
                // for compatibility
                if (range.__isset.file_size) { file_size = range.file_size; }
                file_reader.reset(new BrokerReader(_state->exec_env(), _broker_addresses, _params.properties,
                                                   range.path, range.start_offset, file_size));
                break;
            }
            default: {
                std::stringstream ss;
                ss << "Unknown file type, type=" << range.file_type;
                return Status::InternalError(ss.str());
            }
        }
        RETURN_IF_ERROR(file_reader->open());
        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }

        std::unique_ptr<orc::InputStream> inStream = std::unique_ptr<orc::InputStream>(
                new ORCFileStream(file_reader.release(), range.path));
        _reader = orc::createReader(std::move(inStream), _options);

        _total_groups = _reader->getNumberOfStripes();
        _current_group = 0;
        _rows_of_group = 0;
        _current_line_of_group = 0;

        _includes.clear();
        _num_of_columns_from_file =
                range.__isset.num_of_columns_from_file ? range.num_of_columns_from_file : _src_slot_descs.size();
        for (int i = 0; i < _num_of_columns_from_file; i++) {
            auto slot_desc = _src_slot_descs.at(i);
            _includes.push_back(slot_desc->col_name());
        }
        _rowReaderOptions.include(_includes);

        int orc_index = 0;
        for (int i = 0; i < _reader->getType().getSubtypeCount(); ++i) {
            if (std::find(_includes.begin(), _includes.end(), _reader->getType().getFieldName(i)) != _includes.end()) {
                _column_name_map_orc_index.emplace(_reader->getType().getFieldName(i), orc_index++);
            }
        }

        _row_reader = _reader->createRowReader(_rowReaderOptions);
        return Status::OK();
    }
}

void ORCScanner::close() {
    _batch = nullptr;
    _reader.reset(nullptr);
    _row_reader.reset(nullptr);
}

}
