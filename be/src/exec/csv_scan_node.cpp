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

#include "csv_scan_node.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <string>
#include <vector>

#include "exec/text_converter.hpp"
#include "exprs/hll_hash_function.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/olap_common.h"
#include "olap/utils.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/file_utils.h"
#include "util/hash_util.hpp"
#include "util/runtime_profile.h"

namespace doris {

class StringRef {
public:
    StringRef(char const* const begin, int const size) : _begin(begin), _size(size) {}

    ~StringRef() {
        // No need to delete _begin, because it only record the index in a std::string.
        // The c-string will be released along with the std::string object.
    }

    int size() const { return _size; }
    int length() const { return _size; }

    char const* c_str() const { return _begin; }
    char const* begin() const { return _begin; }

    char const* end() const { return _begin + _size; }

private:
    char const* _begin;
    int _size;
};

void split_line(const std::string& str, char delimiter, std::vector<StringRef>& result) {
    enum State { IN_DELIM = 1, IN_TOKEN = 0 };

    // line-begin char and line-end char are considered to be 'delimeter'
    State state = IN_DELIM;
    char const* p_begin = str.c_str(); // Begin of either a token or a delimiter
    for (string::const_iterator it = str.begin(); it != str.end(); ++it) {
        State const new_state = (*it == delimiter ? IN_DELIM : IN_TOKEN);
        if (new_state != state) {
            if (new_state == IN_DELIM) {
                result.push_back(StringRef(p_begin, &*it - p_begin));
            }
            p_begin = &*it;
        } else if (new_state == IN_DELIM) {
            result.push_back(StringRef(&*p_begin, 0));
            p_begin = &*it;
        }

        state = new_state;
    }

    result.push_back(StringRef(p_begin, (&*str.end() - p_begin) - state));
}

CsvScanNode::CsvScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.csv_scan_node.tuple_id),
          _file_paths(tnode.csv_scan_node.file_paths),
          _column_separator(tnode.csv_scan_node.column_separator),
          _column_type_map(tnode.csv_scan_node.column_type_mapping),
          _column_function_map(tnode.csv_scan_node.column_function_mapping),
          _columns(tnode.csv_scan_node.columns),
          _unspecified_columns(tnode.csv_scan_node.unspecified_columns),
          _default_values(tnode.csv_scan_node.default_values),
          _is_init(false),
          _tuple_desc(nullptr),
          _slot_num(0),
          _tuple_pool(nullptr),
          _text_converter(nullptr),
          _tuple(nullptr),
          _runtime_state(nullptr),
          _split_check_timer(nullptr),
          _split_line_timer(nullptr),
          _hll_column_num(0) {
    // do nothing
    LOG(INFO) << "csv scan node: " << apache::thrift::ThriftDebugString(tnode).c_str();
}

CsvScanNode::~CsvScanNode() {
    // do nothing
}

Status CsvScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return ExecNode::init(tnode, state);
}

Status CsvScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "CsvScanNode::Prepare";

    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input runtime_state pointer is nullptr.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));

    // add timer
    _split_check_timer = ADD_TIMER(_runtime_profile, "split check timer");
    _split_line_timer = ADD_TIMER(_runtime_profile, "split line timer");

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (nullptr == _tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _slot_num = _tuple_desc->slots().size();
    const OlapTableDescriptor* csv_table =
            static_cast<const OlapTableDescriptor*>(_tuple_desc->table_desc());
    if (nullptr == csv_table) {
        return Status::InternalError("csv table pointer is nullptr.");
    }

    // <column_name, slot_descriptor>
    for (int i = 0; i < _slot_num; ++i) {
        SlotDescriptor* slot = _tuple_desc->slots()[i];
        const std::string& column_name = slot->col_name();

        if (slot->type().type == TYPE_HLL) {
            TMiniLoadEtlFunction& function = _column_function_map[column_name];
            if (check_hll_function(function) == false) {
                return Status::InternalError("Function name or param error.");
            }
            _hll_column_num++;
        }

        // NOTE: not all the columns in '_columns' is exist in table schema
        if (_columns.end() != std::find(_columns.begin(), _columns.end(), column_name)) {
            _column_slot_map[column_name] = slot;
        } else {
            _column_slot_map[column_name] = nullptr;
        }

        // add 'unspecified_columns' which have default values
        if (_unspecified_columns.end() !=
            std::find(_unspecified_columns.begin(), _unspecified_columns.end(), column_name)) {
            _column_slot_map[column_name] = slot;
        }
    }

    _column_type_vec.resize(_columns.size());
    for (int i = 0; i < _columns.size(); ++i) {
        const std::string& column_name = _columns[i];
        SlotDescriptor* slot = _column_slot_map[column_name];
        _column_slot_vec.push_back(slot);

        if (slot != nullptr) {
            _column_type_vec[i] = _column_type_map[column_name];
        }
    }
    for (int i = 0; i < _default_values.size(); ++i) {
        const std::string& column_name = _unspecified_columns[i];
        SlotDescriptor* slot = _column_slot_map[column_name];
        _unspecified_colomn_slot_vec.push_back(slot);
        _unspecified_colomn_type_vec.push_back(_column_type_map[column_name]);
    }

    // new one scanner
    _csv_scanner.reset(new (std::nothrow) CsvScanner(_file_paths));
    if (_csv_scanner.get() == nullptr) {
        return Status::InternalError("new a csv scanner failed.");
    }

    _tuple_pool.reset(new (std::nothrow) MemPool(state->instance_mem_tracker().get()));
    if (_tuple_pool.get() == nullptr) {
        return Status::InternalError("new a mem pool failed.");
    }

    _text_converter.reset(new (std::nothrow) TextConverter('\\'));
    if (_text_converter.get() == nullptr) {
        return Status::InternalError("new a text convertor failed.");
    }

    _is_init = true;
    return Status::OK();
}

Status CsvScanNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    VLOG_CRITICAL << "CsvScanNode::Open";

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    _runtime_state = state;

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(_csv_scanner->open());

    return Status::OK();
}

Status CsvScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    VLOG_CRITICAL << "CsvScanNode::GetNext";
    if (nullptr == state || nullptr == row_batch || nullptr == eos) {
        return Status::InternalError("input is nullptr pointer");
    }

    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }

    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    // create new tuple buffer for row_batch
    int tuple_buffer_size = row_batch->capacity() * _tuple_desc->byte_size();
    void* tuple_buffer = _tuple_pool->allocate(tuple_buffer_size);

    if (nullptr == tuple_buffer) {
        return Status::InternalError("Allocate memory failed.");
    }

    _tuple = reinterpret_cast<Tuple*>(tuple_buffer);
    memset(_tuple, 0, _tuple_desc->num_null_bytes());

    // Indicates whether there are more rows to process.
    bool csv_eos = false;

    // NOTE: not like Mysql, we need check correctness.
    while (!csv_eos) {
        RETURN_IF_CANCELLED(state);

        if (reached_limit() || row_batch->is_full()) {
            // hang on to last allocated chunk in pool, we'll keep writing into it in the
            // next get_next() call
            row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), !reached_limit());
            *eos = reached_limit();
            return Status::OK();
        }

        // read csv
        std::string line;
        RETURN_IF_ERROR(_csv_scanner->get_next_row(&line, &csv_eos));
        //VLOG_ROW << "line readed: [" << line << "]";
        if (line.empty()) {
            continue;
        }
        // split & check line & fill default value
        bool is_success = split_check_fill(line, state);
        ++_num_rows_load_total;
        if (!is_success) {
            ++_num_rows_load_filtered;
            continue;
        }

        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        // scan node is the first tuple of tuple row
        row->set_tuple(0, _tuple);

        {
            row_batch->commit_last_row();
            ++_num_rows_returned;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            char* new_tuple = reinterpret_cast<char*>(_tuple);
            new_tuple += _tuple_desc->byte_size();
            _tuple = reinterpret_cast<Tuple*>(new_tuple);
        }
    }
    state->update_num_rows_load_total(_num_rows_load_total);
    state->update_num_rows_load_filtered(_num_rows_load_filtered);
    VLOG_ROW << "normal_row_number: " << state->num_rows_load_success()
             << "; error_row_number: " << state->num_rows_load_filtered() << std::endl;

    row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), false);

    *eos = csv_eos;
    return Status::OK();
}

Status CsvScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    VLOG_CRITICAL << "CsvScanNode::Close";
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ExecNode::close(state));

    if (state->num_rows_load_success() == 0) {
        std::stringstream error_msg;
        error_msg << "Read zero normal line file. ";
        LOG(INFO) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }

    // only write summary line if there are error lines
    if (_num_rows_load_filtered > 0) {
        // Summary normal line and error line number info
        std::stringstream summary_msg;
        summary_msg << "error line: " << _num_rows_load_filtered
                    << "; normal line: " << state->num_rows_load_success();
        LOG(INFO) << summary_msg.str();
    }

    return Status::OK();
}

void CsvScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "csvScanNode(tupleid=" << _tuple_id;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status CsvScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

void CsvScanNode::fill_fix_length_string(const char* value, const int value_length, MemPool* pool,
                                         char** new_value_p, const int new_value_length) {
    if (new_value_length != 0 && value_length < new_value_length) {
        DCHECK(pool != nullptr);
        *new_value_p = reinterpret_cast<char*>(pool->allocate(new_value_length));

        // 'value' is guaranteed not to be nullptr
        memcpy(*new_value_p, value, value_length);
        for (int i = value_length; i < new_value_length; ++i) {
            (*new_value_p)[i] = '\0';
        }
        VLOG_ROW << "Fill fix length string. "
                 << "value: [" << std::string(value, value_length) << "]; "
                 << "value_length: " << value_length << "; "
                 << "*new_value_p: [" << *new_value_p << "]; "
                 << "new value length: " << new_value_length << std::endl;
    }
}

// Following format are included.
//      .123    1.23    123.   -1.23
// ATTN: The decimal point and (for negative numbers) the "-" sign are not counted.
//      like '.123', it will be regarded as '0.123', but it match decimal(3, 3)
bool CsvScanNode::check_decimal_input(const char* value, const int value_length,
                                      const int precision, const int scale,
                                      std::stringstream* error_msg) {
    if (value_length > (precision + 2)) {
        (*error_msg) << "the length of decimal value is overflow. "
                     << "precision in schema: (" << precision << ", " << scale << "); "
                     << "value: [" << std::string(value, value_length) << "]; "
                     << "str actual length: " << value_length << ";";
        return false;
    }

    // ignore leading spaces and trailing spaces
    int begin_index = 0;
    while (begin_index < value_length && std::isspace(value[begin_index])) {
        ++begin_index;
    }
    int end_index = value_length - 1;
    while (end_index >= begin_index && std::isspace(value[end_index])) {
        --end_index;
    }

    if (value[begin_index] == '+' || value[begin_index] == '-') {
        ++begin_index;
    }

    int point_index = -1;
    for (int i = begin_index; i <= end_index; ++i) {
        if (value[i] == '.') {
            point_index = i;
        }
    }

    int value_int_len = 0;
    int value_frac_len = 0;
    value_int_len = point_index - begin_index;
    value_frac_len = end_index - point_index;

    if (point_index == -1) {
        // an int value: like 123
        value_int_len = end_index - begin_index + 1;
        value_frac_len = 0;
    } else {
        value_int_len = point_index - begin_index;
        value_frac_len = end_index - point_index;
    }

    if (value_int_len > (precision - scale)) {
        (*error_msg) << "the int part length longer than schema precision [" << precision << "]. "
                     << "value [" << std::string(value, value_length) << "]. ";
        return false;
    } else if (value_frac_len > scale) {
        (*error_msg) << "the frac part length longer than schema scale [" << scale << "]. "
                     << "value [" << std::string(value, value_length) << "]. ";
        return false;
    }
    return true;
}

static bool is_null(const char* value, int value_length) {
    return value_length == 2 && value[0] == '\\' && value[1] == 'N';
}

// Writes a slot in _tuple from an value containing text data.
bool CsvScanNode::check_and_write_text_slot(const std::string& column_name,
                                            const TColumnType& column_type, const char* value,
                                            int value_length, const SlotDescriptor* slot,
                                            RuntimeState* state, std::stringstream* error_msg) {
    if (value_length == 0 && !slot->type().is_string_type()) {
        (*error_msg) << "the length of input should not be 0. "
                     << "column_name: " << column_name << "; "
                     << "type: " << slot->type() << "; "
                     << "input_str: [" << std::string(value, value_length) << "].";
        return false;
    }

    if (is_null(value, value_length)) {
        if (slot->is_nullable()) {
            _tuple->set_null(slot->null_indicator_offset());
            return true;
        } else {
            (*error_msg) << "value cannot be null. column name: " << column_name
                         << "; type: " << slot->type() << "; input_str: ["
                         << std::string(value, value_length) << "].";
            return false;
        }
    }

    char* value_to_convert = const_cast<char*>(value);
    int value_to_convert_length = value_length;

    // Fill all the spaces if it is 'TYPE_CHAR' type
    if (slot->type().is_string_type()) {
        int char_len = column_type.len;
        if (slot->type().type != TYPE_HLL && value_length > char_len) {
            (*error_msg) << "the length of input is too long than schema. "
                         << "column_name: " << column_name << "; "
                         << "input_str: [" << std::string(value, value_length) << "] "
                         << "type: " << slot->type() << "; "
                         << "schema length: " << char_len << "; "
                         << "actual length: " << value_length << "; ";
            return false;
        }
        if (slot->type().type == TYPE_CHAR && value_length < char_len) {
            fill_fix_length_string(value, value_length, _tuple_pool.get(), &value_to_convert,
                                   char_len);
            value_to_convert_length = char_len;
        }
    } else if (slot->type().is_decimal_type()) {
        int precision = column_type.precision;
        int scale = column_type.scale;
        bool is_success = check_decimal_input(value, value_length, precision, scale, error_msg);
        if (is_success == false) {
            return false;
        }
    }

    if (!_text_converter->write_slot(slot, _tuple, value_to_convert, value_to_convert_length, true,
                                     false, _tuple_pool.get())) {
        (*error_msg) << "convert csv string to " << slot->type() << " failed. "
                     << "column_name: " << column_name << "; "
                     << "input_str: [" << std::string(value, value_length) << "]; ";
        return false;
    }

    return true;
}

bool CsvScanNode::split_check_fill(const std::string& line, RuntimeState* state) {
    SCOPED_TIMER(_split_check_timer);

    std::stringstream error_msg;
    // std::vector<std::string> fields;
    std::vector<StringRef> fields;
    {
        SCOPED_TIMER(_split_line_timer);
        split_line(line, _column_separator[0], fields);
    }

    if (_hll_column_num == 0 && fields.size() < _columns.size()) {
        error_msg << "actual column number is less than schema column number. "
                  << "actual number: " << fields.size() << " ,"
                  << "schema number: " << _columns.size() << "; ";
        LOG(INFO) << error_msg.str();
        return false;
    } else if (_hll_column_num == 0 && fields.size() > _columns.size()) {
        error_msg << "actual column number is more than schema column number. "
                  << "actual number: " << fields.size() << " ,"
                  << "schema number: " << _columns.size() << "; ";
        LOG(INFO) << error_msg.str();
        return false;
    }

    for (int i = 0; i < _columns.size(); ++i) {
        const std::string& column_name = _columns[i];
        const SlotDescriptor* slot = _column_slot_vec[i];
        // ignore unspecified columns
        if (slot == nullptr) {
            continue;
        }

        if (!slot->is_materialized()) {
            continue;
        }

        if (slot->type().type == TYPE_HLL) {
            continue;
        }

        const TColumnType& column_type = _column_type_vec[i];
        bool flag = check_and_write_text_slot(column_name, column_type, fields[i].c_str(),
                                              fields[i].length(), slot, state, &error_msg);

        if (flag == false) {
            LOG(INFO) << error_msg.str();
            return false;
        }
    }

    for (int i = 0; i < _unspecified_columns.size(); ++i) {
        const std::string& column_name = _unspecified_columns[i];
        const SlotDescriptor* slot = _unspecified_colomn_slot_vec[i];
        if (slot == nullptr) {
            continue;
        }

        if (!slot->is_materialized()) {
            continue;
        }

        if (slot->type().type == TYPE_HLL) {
            continue;
        }

        const TColumnType& column_type = _unspecified_colomn_type_vec[i];
        bool flag = check_and_write_text_slot(column_name, column_type, _default_values[i].c_str(),
                                              _default_values[i].length(), slot, state, &error_msg);

        if (flag == false) {
            LOG(INFO) << error_msg.str();
            return false;
        }
    }

    for (auto iter = _column_function_map.begin(); iter != _column_function_map.end(); ++iter) {
        TMiniLoadEtlFunction& function = iter->second;
        const std::string& column_name = iter->first;
        const SlotDescriptor* slot = _column_slot_map[column_name];
        const TColumnType& column_type = _column_type_map[column_name];
        std::string column_string = "";
        const char* src = fields[function.param_column_index].c_str();
        int src_column_len = fields[function.param_column_index].length();
        hll_hash(src, src_column_len, &column_string);
        bool flag = check_and_write_text_slot(column_name, column_type, column_string.c_str(),
                                              column_string.length(), slot, state, &error_msg);
        if (flag == false) {
            LOG(INFO) << error_msg.str();
            return false;
        }
    }

    return true;
}

bool CsvScanNode::check_hll_function(TMiniLoadEtlFunction& function) {
    if (function.function_name.empty() || function.function_name != "hll_hash" ||
        function.param_column_index < 0) {
        return false;
    }
    return true;
}

void CsvScanNode::hll_hash(const char* src, int len, std::string* result) {
    std::string str(src, len);
    if (str != "\\N") {
        uint64_t hash = HashUtil::murmur_hash64A(src, len, HashUtil::MURMUR_SEED);
        char buf[10];
        // expliclit set
        buf[0] = HLL_DATA_EXPLICIT;
        buf[1] = 1;
        *((uint64_t*)(buf + 2)) = hash;
        *result = std::string(buf, sizeof(buf));
    } else {
        char buf[1];
        // empty set
        buf[0] = HLL_DATA_EMPTY;
        *result = std::string(buf, sizeof(buf));
    }
}

} // end namespace doris
