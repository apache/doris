// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/olap_reader.h"

#include <sstream>

#include "runtime/datetime_value.h"
#include "util/palo_metrics.h"

using std::exception;
using std::string;
using std::stringstream;
using std::vector;
using std::map;
using std::nothrow;

namespace palo {

void OLAPReader::init_profile(RuntimeProfile* profile) {
    ADD_TIMER(profile, "GetTabletTime");
    ADD_TIMER(profile, "InitReaderTime");
    ADD_TIMER(profile, "ReadDataTime");
    ADD_TIMER(profile, "ShowHintsTime");
    ADD_COUNTER(profile, "RawRowsRead", TUnit::UNIT);
}

Status OLAPShowHints::show_hints(
        TShowHintsRequest& fetch_request,
        std::vector<std::vector<std::vector<std::string>>>* ranges, 
        RuntimeProfile* profile) {
    OLAP_LOG_DEBUG("Show hints:%s", apache::thrift::ThriftDebugString(fetch_request).c_str());
    {
        RuntimeProfile::Counter* show_hints_timer = profile->get_counter("ShowHintsTime");
        SCOPED_TIMER(show_hints_timer);
    
        OLAPStatus res = OLAP_SUCCESS;
        ranges->clear();
    
        SmartOLAPTable table = OLAPEngine::get_instance()->get_table(
                fetch_request.tablet_id, fetch_request.schema_hash);
        if (table.get() == NULL) {
            OLAP_LOG_WARNING("table does not exists. [tablet_id=%ld schema_hash=%d]",
                             fetch_request.tablet_id, fetch_request.schema_hash);
            return Status("table does not exists");
        }
    
        vector<string> start_key_strings;
        vector<string> end_key_strings;
        vector<vector<string>> range;
        if (fetch_request.start_key.size() == 0) {
            res = table->split_range(start_key_strings,
                                     end_key_strings,
                                     fetch_request.block_row_count,
                                     &range);
    
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to show hints by split range. [res=%d]", res);
                return Status("fail to show hints");
            }
    
            ranges->push_back(range);
        } else {
            for (int key_pair_index = 0;
                    key_pair_index < fetch_request.start_key.size(); ++key_pair_index) {
                start_key_strings.clear();
                end_key_strings.clear();
                range.clear();
    
                TFetchStartKey& start_key_field = fetch_request.start_key[key_pair_index];
                for (vector<string>::const_iterator it = start_key_field.key.begin();
                        it != start_key_field.key.end(); ++it) {
                    start_key_strings.push_back(*it);
                }
    
                TFetchEndKey& end_key_field = fetch_request.end_key[key_pair_index];
                for (vector<string>::const_iterator it = end_key_field.key.begin();
                        it != end_key_field.key.end(); ++it) {
                    end_key_strings.push_back(*it);
                }
    
                res = table->split_range(start_key_strings,
                                         end_key_strings,
                                         fetch_request.block_row_count,
                                         &range);
                if (res != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to show hints by split range. [res=%d]", res);
                    return Status("fail to show hints");
                }
    
                ranges->push_back(range);
            }
        }
    }

    return Status::OK;
}

OLAPReader *OLAPReader::create(const TupleDescriptor &tuple_desc, RuntimeState* runtime_state) {
    return new (std::nothrow) OLAPReader(tuple_desc, runtime_state);
}

Status OLAPReader::init(TFetchRequest& fetch_request, 
                        std::vector<ExprContext*> *conjunct_ctxs,
                        RuntimeProfile* profile) {
    OLAP_LOG_DEBUG("fetch request:%s", apache::thrift::ThriftDebugString(fetch_request).c_str());

    if (PaloMetrics::palo_fetch_count() != NULL) {
        PaloMetrics::palo_fetch_count()->increment(1);
    }

    OLAPStatus res = OLAP_SUCCESS;

    _get_tablet_timer = profile->get_counter("GetTabletTime");
    _init_reader_timer = profile->get_counter("InitReaderTime");

    _conjunct_ctxs = conjunct_ctxs;
    _aggregation = fetch_request.aggregation;

    {
        SCOPED_TIMER(_get_tablet_timer);
        _olap_table = OLAPEngine::get_instance()->get_table(
                fetch_request.tablet_id, fetch_request.schema_hash);
        if (_olap_table.get() == NULL) {
            OLAP_LOG_WARNING("table does not exists. [tablet_id=%ld schema_hash=%d]",
                             fetch_request.tablet_id, fetch_request.schema_hash);
            return Status("table does not exists");
        }
    }

    {
        AutoRWLock auto_lock(_olap_table->get_header_lock_ptr(), true);
        const FileVersionMessage* message = _olap_table->latest_version();
        if (message == NULL) {
            OLAP_LOG_WARNING("fail to get latest version. [tablet_id=%ld]",
                             fetch_request.tablet_id);
            return Status("fail to get latest version");
        }

        if (message->end_version() == fetch_request.version
                && message->version_hash() != fetch_request.version_hash) {
            OLAP_LOG_WARNING("fail to check latest version hash. "
                             "[res=%d tablet_id=%ld version_hash=%ld request_version_hash=%ld]",
                             res, fetch_request.tablet_id,
                             message->version_hash(), fetch_request.version_hash);
            return Status("fail to check version hash");
        }
    }

    {
        SCOPED_TIMER(_init_reader_timer);
        res = _init_params(fetch_request, profile);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init olap reader.[res=%d]", res);
            return Status("fail to init olap reader");
        }
    }

    _is_inited = true;
    _read_data_watch.reset();
    return Status::OK;
}

Status OLAPReader::close() {
    return Status::OK;
}

Status OLAPReader::next_tuple(Tuple* tuple, int64_t* raw_rows_read, bool* eof) {
    OLAPStatus res = OLAP_SUCCESS;
    res = _reader.next_row_with_aggregation(&_read_row_cursor, raw_rows_read, eof);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get new row.[res=%d]", res);
        return Status("fail to get new row");
    }

    if (!*eof) {
        res = _convert_row_to_tuple(tuple);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to convert row to tuple.[res=%d]", res);
            return Status("fail to convert row to tuple");
        }
    }

    return Status::OK;
}

OLAPStatus OLAPReader::_convert_row_to_tuple(Tuple* tuple) {
    RowCursor *row_cursor = NULL;
    if (_aggregation) {
        row_cursor = &_read_row_cursor;
    } else {
        _return_row_cursor.copy(_read_row_cursor);
        row_cursor = &_return_row_cursor;
    }

    int offset = 0;
    size_t slots_size = _query_slots.size();
    //offset += row_cursor->get_num_null_byte();
    for (int i = 0; i < slots_size; ++i)
    {
        offset += sizeof(char);
        if (true == row_cursor->is_null_converted(i)) {
            tuple->set_null(_query_slots[i]->null_indicator_offset());
            offset += _request_columns_size[i];
            if (TYPE_VARCHAR == _query_slots[i]->type().type || TYPE_HLL == _query_slots[i]->type().type) {
                offset += 2; //row_cursor has an uint16_t storage space for NULL varchar
            }
            continue;
        }

        switch (_query_slots[i]->type().type) {
        case TYPE_CHAR: {
            StringValue *slot = tuple->get_string_slot(_query_slots[i]->tuple_offset());
            slot->ptr = reinterpret_cast<char*>(
                    const_cast<char*>(row_cursor->get_buf() + offset));
            slot->len = strnlen(slot->ptr, _request_columns_size[i]);
            offset += _request_columns_size[i];
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_HLL: {
            StringValue *slot = tuple->get_string_slot(_query_slots[i]->tuple_offset());
            size_t size = *reinterpret_cast<uint16_t*>
                    (const_cast<char*>(row_cursor->get_buf() + offset));
            offset += 2;
            slot->ptr = reinterpret_cast<char*>
                    (const_cast<char*>(row_cursor->get_buf() + offset));
            slot->len = size;
            offset += _request_columns_size[i];
            break;
        }
        case TYPE_DECIMAL: {
            // DecimalValue *slot = tuple->get_decimal_slot(_tuple_desc.slots()[i]->tuple_offset());
            DecimalValue *slot = tuple->get_decimal_slot(_query_slots[i]->tuple_offset());

            // TODO(lingbin): should remove this assign, use set member function
            int64_t int_value = *reinterpret_cast<int64_t*>(
                    const_cast<char*>(row_cursor->get_buf() + offset));
            offset += sizeof(int64_t);
            int32_t frac_value = *reinterpret_cast<int32_t*>(
                    const_cast<char*>(row_cursor->get_buf() + offset));
            offset += sizeof(int32_t);
            *slot = DecimalValue(int_value, frac_value);
            break;
        }

        case TYPE_DATETIME: {
            DateTimeValue *slot = tuple->get_datetime_slot(
                    _query_slots[i]->tuple_offset());
            uint64_t value = *reinterpret_cast<uint64_t*>(
                    const_cast<char*>(row_cursor->get_buf() + offset));
            if (!slot->from_olap_datetime(value)) {
                tuple->set_null(_query_slots[i]->null_indicator_offset());
            }
            offset += 8;
            break;
        }
        case TYPE_DATE: {
            DateTimeValue *slot = tuple->get_datetime_slot(
                    _query_slots[i]->tuple_offset());
            uint64_t value = 0;
            value = *(unsigned char*)(row_cursor->get_buf() + offset + 2);
            value <<= 8;
            value |= *(unsigned char*)(row_cursor->get_buf() + offset + 1);
            value <<= 8;
            value |= *(unsigned char*)(row_cursor->get_buf() + offset);
            if (!slot->from_olap_date(value)) {
                tuple->set_null(_query_slots[i]->null_indicator_offset());
            }
            offset += 3;
            break;
        }
        default: {
            void *slot = tuple->get_slot(_query_slots[i]->tuple_offset());
            memory_copy(slot, row_cursor->get_buf() + offset, _request_columns_size[i]);
            offset += _request_columns_size[i];
            break;
        }
        }
    }

    return OLAP_SUCCESS;
}

OLAPStatus OLAPReader::_init_params(TFetchRequest& fetch_request, RuntimeProfile* profile) {
    OLAPStatus res = OLAP_SUCCESS;

    res = _init_return_columns(fetch_request);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init return columns.[res=%d]", res);
        return res;
    }

    ReaderParams reader_params;
    reader_params.olap_table = _olap_table;
    reader_params.reader_type = READER_FETCH;
    reader_params.aggregation = fetch_request.aggregation;
    reader_params.version = Version(0, fetch_request.version);
    reader_params.conditions = fetch_request.where;
    reader_params.range = fetch_request.range;
    reader_params.end_range = fetch_request.end_range;
    reader_params.start_key = fetch_request.start_key;
    reader_params.end_key = fetch_request.end_key;
    reader_params.conjunct_ctxs = _conjunct_ctxs;
    reader_params.profile = profile;
    reader_params.runtime_state = _runtime_state;

    if (_aggregation) {
        reader_params.return_columns = _return_columns;
    } else {
        for (size_t i = 0; i < _olap_table->num_key_fields(); ++i) {
            reader_params.return_columns.push_back(i);
        }
        for (size_t i = 0; i < fetch_request.field.size(); ++i) {
            int32_t index = _olap_table->get_field_index(fetch_request.field[i]);
            if (_olap_table->tablet_schema()[index].is_key) {
                continue;
            } else {
                reader_params.return_columns.push_back(index);
            }
        }
    }
    res = _read_row_cursor.init(_olap_table->tablet_schema(), reader_params.return_columns);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row cursor.[res=%d]", res);
        return res;
    }
    res = _reader.init(reader_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init reader.[res=%d]", res);
        return res;
    }

    for (int i = 0; i < _tuple_desc.slots().size(); ++i) {
        if (!_tuple_desc.slots()[i]->is_materialized()) {
            continue;
        }
        _query_slots.push_back(_tuple_desc.slots()[i]);
    }

    return res;
}

OLAPStatus OLAPReader::_init_return_columns(TFetchRequest& fetch_request) {
    for (int32_t i = 0, len = fetch_request.field.size(); i < len; i++) {
        int32_t index = _olap_table->get_field_index(fetch_request.field[i]);
        if (index < 0) {
            OLAP_LOG_WARNING("field name is invalied. [index=%d field='%s']",
                             index,
                             fetch_request.field[i].c_str());
            return OLAP_ERR_FETCH_GET_READER_PARAMS_ERR;
        }

        _return_columns.push_back(index);
        if (_olap_table->tablet_schema()[index].type == OLAP_FIELD_TYPE_VARCHAR || _olap_table->tablet_schema()[index].type == OLAP_FIELD_TYPE_HLL) {
            _request_columns_size.push_back(_olap_table->tablet_schema()[index].length - 
                    sizeof(VarCharField::LengthValueType));
        } else {
            _request_columns_size.push_back(_olap_table->tablet_schema()[index].length);
        }
    }

    _return_row_cursor.init(_olap_table->tablet_schema(), _return_columns);

    return OLAP_SUCCESS;
}

}  // namespace palo
