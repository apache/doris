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

#include "io/file_factory.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"

// orc include file didn't expose orc::TimezoneError
// we have to declare it by hand, following is the source code in orc link
// https://github.com/apache/orc/blob/84353fbfc447b06e0924024a8e03c1aaebd3e7a5/c%2B%2B/src/Timezone.hh#L104-L109
namespace orc {

class TimezoneError : public std::runtime_error {
public:
    TimezoneError(const std::string& what);
    TimezoneError(const TimezoneError&);
    virtual ~TimezoneError() noexcept;
};

} // namespace orc

namespace doris {

class ORCFileStream : public orc::InputStream {
public:
    ORCFileStream(FileReader* file, std::string filename)
            : _file(file), _filename(std::move(filename)) {}

    ~ORCFileStream() override {
        if (_file != nullptr) {
            _file->close();
            delete _file;
            _file = nullptr;
        }
    }

    /**
     * Get the total length of the file in bytes.
     */
    uint64_t getLength() const override { return _file->size(); }

    /**
     * Get the natural size for reads.
     * @return the number of bytes that should be read at once
     */
    uint64_t getNaturalReadSize() const override { return 128 * 1024; }

    /**
     * Read length bytes from the file starting at offset into
     * the buffer starting at buf.
     * @param buf the starting position of a buffer.
     * @param length the number of bytes to read.
     * @param offset the position in the stream to read from.
     */
    void read(void* buf, uint64_t length, uint64_t offset) override {
        if (buf == nullptr) {
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
            bytes_read += reads; // total read bytes
            offset += reads;
            buf = (char*)buf + reads;
        }
        if (length != bytes_read) {
            throw orc::ParseError("Short read of " + _filename +
                                  ". expected :" + std::to_string(length) +
                                  ", actual : " + std::to_string(bytes_read));
        }
    }

    /**
     * Get the name of the stream for error messages.
     */
    const std::string& getName() const override { return _filename; }

private:
    FileReader* _file;
    std::string _filename;
};

ORCScanner::ORCScanner(RuntimeState* state, RuntimeProfile* profile,
                       const TBrokerScanRangeParams& params,
                       const std::vector<TBrokerRangeDesc>& ranges,
                       const std::vector<TNetworkAddress>& broker_addresses,
                       const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
        : BaseScanner(state, profile, params, ranges, broker_addresses, pre_filter_texprs, counter),
          // _splittable(params.splittable),
          _cur_file_eof(true),
          _total_groups(0),
          _current_group(0),
          _rows_of_group(0),
          _current_line_of_group(0) {}

ORCScanner::~ORCScanner() {
    close();
}

Status ORCScanner::open() {
    RETURN_IF_ERROR(BaseScanner::open());
    if (!_ranges.empty()) {
        std::list<std::string> include_cols;
        TBrokerRangeDesc range = _ranges[0];
        _num_of_columns_from_file = range.__isset.num_of_columns_from_file
                                            ? range.num_of_columns_from_file
                                            : _src_slot_descs.size();
        for (int i = 0; i < _num_of_columns_from_file; i++) {
            auto slot_desc = _src_slot_descs.at(i);
            include_cols.push_back(slot_desc->col_name());
        }
        _row_reader_options.include(include_cols);
    }

    return Status::OK();
}

Status ORCScanner::get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple) {
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

            const std::vector<orc::ColumnVectorBatch*>& batch_vec =
                    ((orc::StructVectorBatch*)_batch.get())->fields;
            for (int column_ipos = 0; column_ipos < _num_of_columns_from_file; ++column_ipos) {
                auto slot_desc = _src_slot_descs[column_ipos];
                orc::ColumnVectorBatch* cvb = batch_vec[_position_in_orc_original[column_ipos]];

                if (cvb->hasNulls && !cvb->notNull[_current_line_of_group]) {
                    if (!slot_desc->is_nullable()) {
                        std::stringstream str_error;
                        str_error << "The field name(" << slot_desc->col_name()
                                  << ") is not nullable ";
                        LOG(WARNING) << str_error.str();
                        return Status::InternalError(str_error.str());
                    }
                    _src_tuple->set_null(slot_desc->null_indicator_offset());
                } else {
                    int32_t wbytes = 0;
                    uint8_t tmp_buf[128] = {0};
                    if (slot_desc->is_nullable()) {
                        _src_tuple->set_not_null(slot_desc->null_indicator_offset());
                    }
                    void* slot = _src_tuple->get_slot(slot_desc->tuple_offset());
                    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);

                    switch (_row_reader->getSelectedType()
                                    .getSubtype(_position_in_orc_original[column_ipos])
                                    ->getKind()) {
                    case orc::BOOLEAN: {
                        int64_t value = ((orc::LongVectorBatch*)cvb)->data[_current_line_of_group];
                        if (value == 0) {
                            str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(5));
                            memcpy(str_slot->ptr, "false", 5);
                            str_slot->len = 5;
                        } else {
                            str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(4));
                            memcpy(str_slot->ptr, "true", 4);
                            str_slot->len = 4;
                        }
                        break;
                    }
                    case orc::BYTE:
                    case orc::INT:
                    case orc::SHORT:
                    case orc::LONG: {
                        int64_t value = ((orc::LongVectorBatch*)cvb)->data[_current_line_of_group];
                        wbytes = sprintf((char*)tmp_buf, "%" PRId64, value);
                        str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(wbytes));
                        memcpy(str_slot->ptr, tmp_buf, wbytes);
                        str_slot->len = wbytes;
                        break;
                    }
                    case orc::FLOAT:
                    case orc::DOUBLE: {
                        double value = ((orc::DoubleVectorBatch*)cvb)->data[_current_line_of_group];
                        wbytes = sprintf((char*)tmp_buf, "%.9f", value);
                        str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(wbytes));
                        memcpy(str_slot->ptr, tmp_buf, wbytes);
                        str_slot->len = wbytes;
                        break;
                    }
                    case orc::BINARY:
                    case orc::CHAR:
                    case orc::VARCHAR:
                    case orc::STRING: {
                        char* value = ((orc::StringVectorBatch*)cvb)->data[_current_line_of_group];
                        wbytes = ((orc::StringVectorBatch*)cvb)->length[_current_line_of_group];
                        str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(wbytes));
                        memcpy(str_slot->ptr, value, wbytes);
                        str_slot->len = wbytes;
                        break;
                    }
                    case orc::DECIMAL: {
                        int precision = ((orc::Decimal64VectorBatch*)cvb)->precision;
                        int scale = ((orc::Decimal64VectorBatch*)cvb)->scale;

                        //Decimal64VectorBatch handles decimal columns with precision no greater than 18.
                        //Decimal128VectorBatch handles the others.
                        std::string decimal_str;
                        if (precision <= 18) {
                            decimal_str = std::to_string(((orc::Decimal64VectorBatch*)cvb)
                                                                 ->values[_current_line_of_group]);
                        } else {
                            decimal_str = ((orc::Decimal128VectorBatch*)cvb)
                                                  ->values[_current_line_of_group]
                                                  .toString();
                        }

                        int negative = decimal_str[0] == '-' ? 1 : 0;
                        int decimal_scale_length = decimal_str.size() - negative;

                        std::string v;
                        if (decimal_scale_length <= scale) {
                            // decimal(5,2) : the integer of 0.01 is 1, so we should fill 0 befor integer
                            v = std::string(negative ? "-0." : "0.");
                            int fill_zero = scale - decimal_scale_length;
                            while (fill_zero--) {
                                v += "0";
                            }
                            if (negative) {
                                v += decimal_str.substr(1, decimal_str.length());
                            } else {
                                v += decimal_str;
                            }
                        } else {
                            //Orc api will fill in 0 at the end, so size must greater than scale
                            v = decimal_str.substr(0, decimal_str.size() - scale) + "." +
                                decimal_str.substr(decimal_str.size() - scale);
                        }

                        str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(v.size()));
                        memcpy(str_slot->ptr, v.c_str(), v.size());
                        str_slot->len = v.size();
                        break;
                    }
                    case orc::DATE: {
                        //Date columns record the number of days since the UNIX epoch (1/1/1970 in UTC).
                        int64_t timestamp =
                                ((orc::LongVectorBatch*)cvb)->data[_current_line_of_group] * 24 *
                                60 * 60;
                        DateTimeValue dtv;
                        if (!dtv.from_unixtime(timestamp, "UTC")) {
                            std::stringstream str_error;
                            str_error
                                    << "Parse timestamp (" + std::to_string(timestamp) + ") error";
                            LOG(WARNING) << str_error.str();
                            return Status::InternalError(str_error.str());
                        }
                        dtv.cast_to_date();
                        char* buf_end = dtv.to_string((char*)tmp_buf);
                        wbytes = buf_end - (char*)tmp_buf - 1;
                        str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(wbytes));
                        memcpy(str_slot->ptr, tmp_buf, wbytes);
                        str_slot->len = wbytes;
                        break;
                    }
                    case orc::TIMESTAMP: {
                        //The time zone of orc's timestamp is stored inside orc's stripe information,
                        //so the timestamp obtained here is an offset timestamp, so parse timestamp with UTC is actual datetime literal.
                        int64_t timestamp =
                                ((orc::TimestampVectorBatch*)cvb)->data[_current_line_of_group];
                        DateTimeValue dtv;
                        if (!dtv.from_unixtime(timestamp, "UTC")) {
                            std::stringstream str_error;
                            str_error
                                    << "Parse timestamp (" + std::to_string(timestamp) + ") error";
                            LOG(WARNING) << str_error.str();
                            return Status::InternalError(str_error.str());
                        }
                        char* buf_end = dtv.to_string((char*)tmp_buf);
                        wbytes = buf_end - (char*)tmp_buf - 1;
                        str_slot->ptr = reinterpret_cast<char*>(tuple_pool->allocate(wbytes));
                        memcpy(str_slot->ptr, tmp_buf, wbytes);
                        str_slot->len = wbytes;
                        break;
                    }
                    default: {
                        std::stringstream str_error;
                        str_error << "The field name(" << slot_desc->col_name()
                                  << ") type not support. ";
                        LOG(WARNING) << str_error.str();
                        return Status::InternalError(str_error.str());
                    }
                    }
                }
            }
            ++_current_line_of_group;

            // range of current file
            const TBrokerRangeDesc& range = _ranges.at(_next_range - 1);
            if (range.__isset.num_of_columns_from_file) {
                fill_slots_of_columns_from_path(range.num_of_columns_from_file,
                                                range.columns_from_path);
            }
            COUNTER_UPDATE(_rows_read_counter, 1);
            SCOPED_TIMER(_materialize_timer);
            RETURN_IF_ERROR(fill_dest_tuple(tuple, tuple_pool, fill_tuple));
            break;
        }
        if (_scanner_eof) {
            *eof = true;
        } else {
            *eof = false;
        }
        return Status::OK();
    } catch (orc::ParseError& e) {
        std::stringstream str_error;
        str_error << "ParseError : " << e.what();
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    } catch (orc::InvalidArgument& e) {
        std::stringstream str_error;
        str_error << "ParseError : " << e.what();
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    } catch (orc::TimezoneError& e) {
        std::stringstream str_error;
        str_error << "TimezoneError : " << e.what();
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
        const TBrokerRangeDesc& range = _ranges[_next_range++];
        std::unique_ptr<FileReader> file_reader;
        RETURN_IF_ERROR(FileFactory::create_file_reader(
                range.file_type, _state->exec_env(), _profile, _broker_addresses,
                _params.properties, range, range.start_offset, file_reader));
        RETURN_IF_ERROR(file_reader->open());

        if (file_reader->size() == 0) {
            file_reader->close();
            continue;
        }

        std::unique_ptr<orc::InputStream> inStream = std::unique_ptr<orc::InputStream>(
                new ORCFileStream(file_reader.release(), range.path));
        _reader = orc::createReader(std::move(inStream), _options);

        // Something the upstream system(eg, hive) may create empty orc file
        // which only has a header and footer, without schema.
        // And if we call `_reader->createRowReader()` with selected columns,
        // it will throw ParserError: Invalid column selected xx.
        // So here we first check its number of rows and skip these kind of files.
        if (_reader->getNumberOfRows() == 0) {
            continue;
        }

        _total_groups = _reader->getNumberOfStripes();
        _current_group = 0;
        _rows_of_group = 0;
        _current_line_of_group = 0;
        _row_reader = _reader->createRowReader(_row_reader_options);

        //include_colus is in loader columns order, and batch is in the orc order
        _position_in_orc_original.clear();
        _position_in_orc_original.resize(_num_of_columns_from_file);
        int orc_index = 0;
        auto include_cols = _row_reader_options.getIncludeNames();
        for (int i = 0; i < _row_reader->getSelectedType().getSubtypeCount(); ++i) {
            //include columns must in reader field, otherwise createRowReader will throw exception
            auto pos = std::find(include_cols.begin(), include_cols.end(),
                                 _row_reader->getSelectedType().getFieldName(i));
            _position_in_orc_original.at(std::distance(include_cols.begin(), pos)) = orc_index++;
        }
        return Status::OK();
    }
}

void ORCScanner::close() {
    BaseScanner::close();
    _batch = nullptr;
    _reader.reset(nullptr);
    _row_reader.reset(nullptr);
}

} // namespace doris
