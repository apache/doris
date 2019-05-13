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

#include <cstring>
#include <string>

#include "gen_cpp/PaloInternalService_types.h"
#include "olap_scanner.h"
#include "olap_scan_node.h"
#include "olap_utils.h"
#include "olap/field.h"
#include "service/backend_options.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/mem_util.hpp"
#include "util/network_util.h"
#include "util/doris_metrics.h"

namespace doris {

static const std::string SCANNER_THREAD_TOTAL_WALLCLOCK_TIME =
    "ScannerThreadsTotalWallClockTime";
static const std::string MATERIALIZE_TUPLE_TIMER =
    "MaterializeTupleTime(*)";

OlapScanner::OlapScanner(
        RuntimeState* runtime_state,
        OlapScanNode* parent,
        bool aggregation,
        DorisScanRange* scan_range,
        const std::vector<OlapScanRange>& key_ranges)
            : _runtime_state(runtime_state),
            _parent(parent),
            _tuple_desc(parent->_tuple_desc),
            _profile(parent->runtime_profile()),
            _string_slots(parent->_string_slots),
            _is_open(false),
            _aggregation(aggregation),
            _tuple_idx(parent->_tuple_idx),
            _direct_conjunct_size(parent->_direct_conjunct_size) {
    _reader.reset(new Reader());
    DCHECK(_reader.get() != NULL);
    _ctor_status = _prepare(scan_range, key_ranges, parent->_olap_filter, parent->_is_null_vector);
    if (!_ctor_status.ok()) {
        LOG(WARNING) << "OlapScanner preapre failed, status:" << _ctor_status.get_error_msg();
    }
    _rows_read_counter = parent->rows_read_counter();
    _rows_pushed_cond_filtered_counter = parent->_rows_pushed_cond_filtered_counter;
}

OlapScanner::~OlapScanner() {
}

Status OlapScanner::_prepare(
        DorisScanRange* scan_range, const std::vector<OlapScanRange>& key_ranges,
        const std::vector<TCondition>& filters, const std::vector<TCondition>& is_nulls) {
    // Get olap table
    TTabletId tablet_id = scan_range->scan_range().tablet_id;
    SchemaHash schema_hash =
        strtoul(scan_range->scan_range().schema_hash.c_str(), nullptr, 10);
    _version =
        strtoul(scan_range->scan_range().version.c_str(), nullptr, 10);
    VersionHash version_hash =
        strtoul(scan_range->scan_range().version_hash.c_str(), nullptr, 10);
    {
        std::string err;
        _olap_table = OLAPEngine::get_instance()->get_table(tablet_id, schema_hash, true, &err);
        if (_olap_table.get() == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet: " << tablet_id << " with schema hash: " << schema_hash
               << ", reason: " << err;
            LOG(WARNING) << ss.str();
            return Status(ss.str());
        }
        {
            ReadLock rdlock(_olap_table->get_header_lock_ptr());
            const PDelta* delta = _olap_table->lastest_version();
            if (delta == NULL) {
                std::stringstream ss;
                ss << "fail to get latest version of tablet: " << tablet_id;
                OLAP_LOG_WARNING(ss.str().c_str());
                return Status(ss.str());
            }

            if (delta->end_version() == _version
                && delta->version_hash() != version_hash) {
                OLAP_LOG_WARNING("fail to check latest version hash. "
                                 "[tablet_id=%ld version_hash=%ld request_version_hash=%ld]",
                                 tablet_id, delta->version_hash(), version_hash);

                std::stringstream ss;
                ss << "fail to check version hash of tablet: " << tablet_id;
                return Status(ss.str());
            }
        }
    }

    // Initialize _params
    {
        RETURN_IF_ERROR(_init_params(key_ranges, filters, is_nulls));
    }

    return Status::OK;
}

Status OlapScanner::open() {
    RETURN_IF_ERROR(_ctor_status);
    SCOPED_TIMER(_parent->_reader_init_timer);

    if (_conjunct_ctxs.size() > _direct_conjunct_size) {
        _use_pushdown_conjuncts = true;
    }

    auto res = _reader->init(_params);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init reader.[res=%d]", res);
        std::stringstream ss;
        ss << "failed to initialize storage reader. tablet=" << _params.olap_table->full_name()
           << ", res=" << res << ", backend=" << BackendOptions::get_localhost();
        return Status(ss.str().c_str());
    }
    return Status::OK;
}

Status OlapScanner::_init_params(
        const std::vector<OlapScanRange>& key_ranges,
        const std::vector<TCondition>& filters,
        const std::vector<TCondition>& is_nulls) {
    RETURN_IF_ERROR(_init_return_columns());

    _params.olap_table = _olap_table;
    _params.reader_type = READER_QUERY;
    _params.aggregation = _aggregation;
    _params.version = Version(0, _version);

    // Condition
    for (auto& filter : filters) {
        _params.conditions.push_back(filter);
    }
    for (auto& is_null_str : is_nulls) {
        _params.conditions.push_back(is_null_str);
    }
    // Range
    for (auto& key_range : key_ranges) {
        if (key_range.begin_scan_range.size() == 1 &&
                key_range.begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _params.range = (key_range.begin_include ? "ge" : "gt");
        _params.end_range = (key_range.end_include ? "le" : "lt");

        _params.start_key.push_back(key_range.begin_scan_range);
        _params.end_key.push_back(key_range.end_scan_range);
    }

    // TODO(zc)
    _params.profile = _profile;
    _params.runtime_state = _runtime_state;

    if (_aggregation) {
        _params.return_columns = _return_columns;
    } else {
        for (size_t i = 0; i < _olap_table->num_key_fields(); ++i) {
            _params.return_columns.push_back(i);
        }
        for (auto index : _return_columns) {
            if (_olap_table->tablet_schema()[index].is_key) {
                continue;
            } else {
                _params.return_columns.push_back(index);
            }
        }
    }

    // use _params.return_columns, because reader use this to merge sort
    OLAPStatus res = _read_row_cursor.init(_olap_table->tablet_schema(), _params.return_columns);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row cursor.[res=%d]", res);
        return Status("failed to initialize storage read row cursor");
    }
    _read_row_cursor.allocate_memory_for_string_type(_olap_table->tablet_schema());
    for (auto cid : _return_columns) {
        _query_fields.push_back(_read_row_cursor.get_field_by_index(cid));
    }

    return Status::OK;
}

Status OlapScanner::_init_return_columns() {
    for (auto slot : _tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        int32_t index = _olap_table->get_field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "field name is invalied. field="  << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status(ss.str());
        }
        _return_columns.push_back(index);
        if (_olap_table->tablet_schema()[index].type == OLAP_FIELD_TYPE_VARCHAR ||
                _olap_table->tablet_schema()[index].type == OLAP_FIELD_TYPE_HLL) {
            _request_columns_size.push_back(
                _olap_table->tablet_schema()[index].length - sizeof(StringLengthType));
        } else {
            _request_columns_size.push_back(_olap_table->tablet_schema()[index].length);
        }
        _query_slots.push_back(slot);
    }
    if (_return_columns.empty()) {
        return Status("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK;
}

Status OlapScanner::get_batch(
        RuntimeState* state, RowBatch* batch, bool* eof) {
    // 2. Allocate Row's Tuple buf
    uint8_t *tuple_buf = batch->tuple_data_pool()->allocate(
        state->batch_size() * _tuple_desc->byte_size());
    bzero(tuple_buf, state->batch_size() * _tuple_desc->byte_size());
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf);

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    {
        SCOPED_TIMER(_parent->_scan_timer);
        while (true) {
            // Batch is full, break
            if (batch->is_full()) {
                _update_realtime_counter();
                break;
            }
            // Read one row from reader
            auto res = _reader->next_row_with_aggregation(&_read_row_cursor, eof);
            if (res != OLAP_SUCCESS) {
                return Status("Internal Error: read storage fail.");
            }
            // If we reach end of this scanner, break
            if (UNLIKELY(*eof)) {
                _update_realtime_counter();
                break;
            }

            _num_rows_read++;

            _convert_row_to_tuple(tuple);
            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "OlapScanner input row: " << Tuple::to_string(tuple, *_tuple_desc);
            }

            // 3.4 Set tuple to RowBatch(not commited)
            int row_idx = batch->add_row();
            TupleRow* row = batch->get_row(row_idx);
            row->set_tuple(_tuple_idx, tuple);

            do {
                // 3.5.1 Using direct conjuncts to filter data
                if (_eval_conjuncts_fn != nullptr) {
                    if (!_eval_conjuncts_fn(&_conjunct_ctxs[0], _direct_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                } else {
                    if (!ExecNode::eval_conjuncts(&_conjunct_ctxs[0], _direct_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                }

                // 3.5.2 Using pushdown conjuncts to filter data
                if (_use_pushdown_conjuncts) {
                    if (!ExecNode::eval_conjuncts(
                            &_conjunct_ctxs[_direct_conjunct_size],
                            _conjunct_ctxs.size() - _direct_conjunct_size, row)) {
                        // check pushdown conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        _num_rows_pushed_cond_filtered++;
                        break;
                    }
                }

                // Copy string slot
                for (auto desc : _string_slots) {
                    StringValue* slot = tuple->get_string_slot(desc->tuple_offset());
                    if (slot->len != 0) {
                        uint8_t* v = batch->tuple_data_pool()->allocate(slot->len);
                        memory_copy(v, slot->ptr, slot->len);
                        slot->ptr = reinterpret_cast<char*>(v);
                    }
                }
                if (VLOG_ROW_IS_ON) {
                    VLOG_ROW << "OlapScanner output row: " << Tuple::to_string(tuple, *_tuple_desc);
                }

                // check direct && pushdown conjuncts success then commit tuple
                batch->commit_last_row();
                char* new_tuple = reinterpret_cast<char*>(tuple);
                new_tuple += _tuple_desc->byte_size();
                tuple = reinterpret_cast<Tuple*>(new_tuple);

                // compute pushdown conjuncts filter rate
                if (_use_pushdown_conjuncts) {
                    // check this rate after 
                    if (_num_rows_read > 32768) {
                        int32_t pushdown_return_rate
                            = _num_rows_read * 100 / (_num_rows_read + _num_rows_pushed_cond_filtered);
                        if (pushdown_return_rate > config::doris_max_pushdown_conjuncts_return_rate) {
                            _use_pushdown_conjuncts = false;
                            VLOG(2) << "Stop Using PushDown Conjuncts. "
                                << "PushDownReturnRate: " << pushdown_return_rate << "%"
                                << " MaxPushDownReturnRate: "
                                << config::doris_max_pushdown_conjuncts_return_rate << "%";
                        }
                    }
                }
            } while (false);

            if (raw_rows_read() >= raw_rows_threshold) {
                break;
            }
        }
    }

    return Status::OK;
}

void OlapScanner::_convert_row_to_tuple(Tuple* tuple) {
    char* row = _read_row_cursor.get_buf();
    size_t slots_size = _query_slots.size();
    for (int i = 0; i < slots_size; ++i) {
        SlotDescriptor* slot_desc = _query_slots[i];
        const Field* field = _query_fields[i];
        if (field->is_null(row)) {
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        char* ptr = (char*)field->get_ptr(row);
        size_t len = field->size();
        switch (slot_desc->type().type) {
        case TYPE_CHAR: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            StringValue *slot = tuple->get_string_slot(slot_desc->tuple_offset());
            slot->ptr = slice->data;
            slot->len = strnlen(slot->ptr, slice->size);
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_HLL: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            StringValue *slot = tuple->get_string_slot(slot_desc->tuple_offset());
            slot->ptr = slice->data;
            slot->len = slice->size;
            break;
        }
        case TYPE_DECIMAL: {
            DecimalValue *slot = tuple->get_decimal_slot(slot_desc->tuple_offset());

            // TODO(lingbin): should remove this assign, use set member function
            int64_t int_value = *(int64_t*)(ptr);
            int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
            *slot = DecimalValue(int_value, frac_value);
            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value *slot = tuple->get_decimalv2_slot(slot_desc->tuple_offset());

            int64_t int_value = *(int64_t*)(ptr);
            int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
            if (!slot->from_olap_decimal(int_value, frac_value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        case TYPE_DATETIME: {
            DateTimeValue *slot = tuple->get_datetime_slot(slot_desc->tuple_offset());
            uint64_t value = *reinterpret_cast<uint64_t*>(ptr);
            if (!slot->from_olap_datetime(value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        case TYPE_DATE: {
            DateTimeValue *slot = tuple->get_datetime_slot(slot_desc->tuple_offset());
            uint64_t value = 0;
            value = *(unsigned char*)(ptr + 2);
            value <<= 8;
            value |= *(unsigned char*)(ptr + 1);
            value <<= 8;
            value |= *(unsigned char*)(ptr);
            if (!slot->from_olap_date(value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        default: {
            void *slot = tuple->get_slot(slot_desc->tuple_offset());
            memory_copy(slot, ptr, len);
            break;
        }
        }
    }
}

void OlapScanner::update_counter() {
    if (_has_update_counter) {
        return;
    }
    COUNTER_UPDATE(_rows_read_counter, _num_rows_read);
    COUNTER_UPDATE(_rows_pushed_cond_filtered_counter, _num_rows_pushed_cond_filtered);

    COUNTER_UPDATE(_parent->_io_timer, _reader->stats().io_ns);
    COUNTER_UPDATE(_parent->_read_compressed_counter, _reader->stats().compressed_bytes_read);
    COUNTER_UPDATE(_parent->_decompressor_timer, _reader->stats().decompress_ns);
    COUNTER_UPDATE(_parent->_read_uncompressed_counter, _reader->stats().uncompressed_bytes_read);
    COUNTER_UPDATE(_parent->bytes_read_counter(), _reader->stats().bytes_read);

    COUNTER_UPDATE(_parent->_block_load_timer, _reader->stats().block_load_ns);
    COUNTER_UPDATE(_parent->_block_load_counter, _reader->stats().blocks_load);
    COUNTER_UPDATE(_parent->_block_fetch_timer, _reader->stats().block_fetch_ns);
    COUNTER_UPDATE(_parent->_block_seek_timer, _reader->stats().block_seek_ns);
    COUNTER_UPDATE(_parent->_block_convert_timer, _reader->stats().block_convert_ns);

    COUNTER_UPDATE(_parent->_raw_rows_counter, _reader->stats().raw_rows_read);
    // COUNTER_UPDATE(_parent->_filtered_rows_counter, _reader->stats().num_rows_filtered);

    COUNTER_UPDATE(_parent->_vec_cond_timer, _reader->stats().vec_cond_ns);
    COUNTER_UPDATE(_parent->_rows_vec_cond_counter, _reader->stats().rows_vec_cond_filtered);

    COUNTER_UPDATE(_parent->_stats_filtered_counter, _reader->stats().rows_stats_filtered);
    COUNTER_UPDATE(_parent->_del_filtered_counter, _reader->stats().rows_del_filtered);

    COUNTER_UPDATE(_parent->_index_load_timer, _reader->stats().index_load_ns);

    DorisMetrics::query_scan_bytes.increment(_reader->stats().compressed_bytes_read);
    DorisMetrics::query_scan_rows.increment(_reader->stats().raw_rows_read);

    _has_update_counter = true;
}

void OlapScanner::_update_realtime_counter() {
    COUNTER_UPDATE(_parent->bytes_read_counter(), _reader->stats().bytes_read);
}

Status OlapScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK;
    }
    update_counter();
    _reader.reset();
    Expr::close(_conjunct_ctxs, state);
    _is_closed = true;
    return Status::OK;
}

} // namespace doris
