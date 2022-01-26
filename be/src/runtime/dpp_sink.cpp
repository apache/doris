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

#include "runtime/dpp_sink.h"

#include <memory>
#include <sstream>

#include "agent/cgroups_mgr.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "gen_cpp/Types_types.h"
#include "olap/field.h"
#include "runtime/descriptors.h"
#include "runtime/dpp_writer.h"
#include "runtime/exec_env.h"
#include "runtime/qsorter.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/countdown_latch.h"
#include "util/debug_util.h"
#include "util/priority_thread_pool.hpp"

namespace doris {

template <typename T>
static void update_min(SlotRef* ref, TupleRow* agg_row, TupleRow* row) {
    void* slot = ref->get_slot(agg_row);
    bool agg_row_null = ref->is_null_bit_set(agg_row);
    void* value = SlotRef::get_value(ref, row);
    if (!agg_row_null && value != nullptr) {
        T* t_slot = static_cast<T*>(slot);
        *t_slot = std::min(*t_slot, *static_cast<T*>(value));
    } else if (!agg_row_null && value == nullptr) {
        Tuple* agg_tuple = ref->get_tuple(agg_row);
        agg_tuple->set_null(ref->null_indicator_offset());
    }
}

template <typename T>
static void update_max(SlotRef* ref, TupleRow* agg_row, TupleRow* row) {
    void* slot = ref->get_slot(agg_row);
    bool agg_row_null = ref->is_null_bit_set(agg_row);
    void* value = SlotRef::get_value(ref, row);
    if (!agg_row_null && value != nullptr) {
        T* t_slot = static_cast<T*>(slot);
        *t_slot = std::max(*t_slot, *static_cast<T*>(value));
    } else if (agg_row_null && value != nullptr) {
        T* t_slot = static_cast<T*>(slot);
        *t_slot = *static_cast<T*>(value);
        Tuple* agg_tuple = ref->get_tuple(agg_row);
        agg_tuple->set_not_null(ref->null_indicator_offset());
    }
}

template <typename T>
static void update_sum(SlotRef* ref, TupleRow* agg_row, TupleRow* row) {
    void* slot = ref->get_slot(agg_row);
    bool agg_row_null = ref->is_null_bit_set(agg_row);
    void* value = SlotRef::get_value(ref, row);
    if (!agg_row_null && value != nullptr) {
        T* t_slot = static_cast<T*>(slot);
        *t_slot += *static_cast<T*>(value);
    } else if (agg_row_null && value != nullptr) {
        T* t_slot = static_cast<T*>(slot);
        *t_slot = *static_cast<T*>(value);
        Tuple* agg_tuple = ref->get_tuple(agg_row);
        agg_tuple->set_not_null(ref->null_indicator_offset());
    }
}

template <>
void update_sum<int128_t>(SlotRef* ref, TupleRow* agg_row, TupleRow* row) {
    void* slot = ref->get_slot(agg_row);
    bool agg_row_null = ref->is_null_bit_set(agg_row);
    void* value = SlotRef::get_value(ref, row);
    if (!agg_row_null && value != nullptr) {
        int128_t l_val, r_val;
        memcpy(&l_val, slot, sizeof(int128_t));
        memcpy(&r_val, value, sizeof(int128_t));
        l_val += r_val;
        memcpy(slot, &l_val, sizeof(int128_t));
    } else if (agg_row_null && value != nullptr) {
        memcpy(slot, value, sizeof(int128_t));
        Tuple* agg_tuple = ref->get_tuple(agg_row);
        agg_tuple->set_not_null(ref->null_indicator_offset());
    }
}

// Do nothing, just used to fill vector
static void fake_update(SlotRef* ref, TupleRow* agg_row, TupleRow* row) {
    // Assert maybe good!
}

class HllDppSinkMerge {
    // used in merge
    struct HllMergeValue {
        HllDataType type;
        std::set<uint64_t> hash_set;
        std::map<int, uint8_t> index_to_value;
    };

public:
    // init set
    void prepare(int count, MemPool* pool);

    // update set
    void update_hll_set(TupleRow* agg_row, TupleRow* row, ExprContext* ctx, int index);

    // merge all in vector which have same key
    void finalize_one_merge(TupleRow* agg_row, MemPool* pool, const RollupSchema& rollup_schema);

    // release set
    void close();

private:
    std::vector<HllMergeValue*> _hll_last_row;
};

// same tablet which (partition, rollup, bucket) all equals
// this is used by next steps
//  1. new one Translator
//  2. prepare
//  3. add batch until there is no data
//  4. process, this will process data added and send this to writer.
class Translator {
public:
    Translator(const TabletDesc& tablet_desc, const RowDescriptor& row_desc,
               const std::string& rollup_name, const RollupSchema& rollup_schema,
               ObjectPool* obj_pool);

    ~Translator();

    // Prepare this translator, includes
    // 1. new sorter for sort data
    // 2. new comparator for aggregate data with same key
    // 3. new writer for write data later
    // 4. create value updaters
    Status prepare(RuntimeState* state);

    Status close(RuntimeState* state);

    Status add_batch(RowBatch* batch);

    // NOTE: called when all data is added by 'add_batch'
    Status process(RuntimeState* state);

    const std::string& name() const { return _rollup_name; }

    RuntimeProfile* profile() { return _profile; }

    const std::string& output_path() const { return _output_path; }

private:
    void format_output_path(RuntimeState* state);
    // create profile information.
    Status create_profile(RuntimeState* state);
    // Create sorter for this translator,
    // sorter created is referenced by '_sorter' field,
    // and put to object pool of 'state', so no need to delete it
    Status create_sorter(RuntimeState* state);

    // Helper to create comparator used to aggregate same rows
    Status create_comparator(RuntimeState* state);

    // Create writer to write data
    // same with sorter, so don't worry about its lifecycle
    Status create_writer(RuntimeState* state);

    // Create value updaters
    Status create_value_updaters();

    // Following function is used to process batch

    // Check if this two rows are equal with each other
    // Compare these rows in keys slot
    bool eq_tuple_row(TupleRow* last, TupleRow* cur);

    // Deep copy 'row' to the '_batch_to_write',
    // call this function after make sure that '_batch_to_write' is not full
    void copy_row(TupleRow* row);

    // Update value of 'row' to 'agg_row'
    // call this after 'eq_tuple_row' return true.
    void update_row(TupleRow* agg_row, TupleRow* row);

    // process one row input
    Status process_one_row(TupleRow* row);

    // need this to construct RowBatch
    TabletDesc _tablet_desc;
    // need this to construct RowBatch
    const RowDescriptor& _row_desc;
    const std::string& _rollup_name;
    // Use this information to construct sorter and
    const RollupSchema& _rollup_schema;

    // All the object is stored in object_pool in runtime_state
    // '_sorter': used to sort data in this translator.
    Sorter* _sorter;
    // not owned
    ObjectPool* _obj_pool;

    // used to compare between two tuple_row after sorted.
    // Same rows will be aggregate to one row
    std::vector<ExprContext*> _last_row_expr_ctxs;
    std::vector<ExprContext*> _cur_row_expr_ctxs;

    // path to write
    std::string _output_path;

    // used by writer
    std::vector<ExprContext*> _output_row_expr_ctxs;

    // Used to write result to file
    DppWriter* _writer;

    // we use batch here to release memory as need
    // when one batch is send, memory must be free.
    // because input data maybe huge which can not
    // hold in memory.
    std::unique_ptr<RowBatch> _batch_to_write;

    typedef void (*update_func)(SlotRef*, TupleRow*, TupleRow*);
    std::vector<update_func> _value_updaters;

    // variable used to profile
    RuntimeProfile* _profile;
    RuntimeProfile::Counter* _add_batch_timer;
    RuntimeProfile::Counter* _sort_timer;
    RuntimeProfile::Counter* _agg_timer;
    RuntimeProfile::Counter* _writer_timer;
    HllDppSinkMerge _hll_merge;
};

Translator::Translator(const TabletDesc& tablet_desc, const RowDescriptor& row_desc,
                       const std::string& rollup_name, const RollupSchema& rollup_schema,
                       ObjectPool* obj_pool)
        : _tablet_desc(tablet_desc),
          _row_desc(row_desc),
          _rollup_name(rollup_name),
          _rollup_schema(rollup_schema),
          _sorter(nullptr),
          _obj_pool(obj_pool),
          _writer(nullptr),
          _profile(nullptr),
          _add_batch_timer(nullptr),
          _sort_timer(nullptr),
          _agg_timer(nullptr),
          _writer_timer(nullptr) {}

Translator::~Translator() {}

Status Translator::create_sorter(RuntimeState* state) {
    QSorter* sorter = _obj_pool->add(new QSorter(_row_desc, _rollup_schema.keys(), state));
    RETURN_IF_ERROR(sorter->prepare(state));
    _sorter = sorter;
    return Status::OK();
}

Status Translator::create_comparator(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_rollup_schema.keys(), state, &_last_row_expr_ctxs));
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_rollup_schema.keys(), state, &_cur_row_expr_ctxs));
    return Status::OK();
}

void Translator::format_output_path(RuntimeState* state) {
    std::stringstream ss;
    ss << state->load_dir() << "/" << state->import_label() << "." << _tablet_desc.partition_id
       << "." << _rollup_name << "." << _tablet_desc.bucket_id;
    _output_path = ss.str();
}

Status Translator::create_writer(RuntimeState* state) {
    // 1. create output expr
    std::vector<ExprContext*> ctxs;
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_rollup_schema.keys(), state, &ctxs));
    for (auto ctx : ctxs) {
        _output_row_expr_ctxs.push_back(ctx);
    }
    ctxs.clear();
    RETURN_IF_ERROR(Expr::clone_if_not_exists(_rollup_schema.values(), state, &ctxs));
    for (auto ctx : ctxs) {
        _output_row_expr_ctxs.push_back(ctx);
    }
    // 2. create file
    FileHandler* fh = _obj_pool->add(new FileHandler());
    if (fh->open_with_mode(_output_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRWXU | S_IRWXU) !=
        OLAP_SUCCESS) {
        std::stringstream ss;
        ss << "open file failed; [file=" << _output_path << "]";
        return Status::InternalError("open file failed.");
    }

    // 3. Create writer
    _writer = _obj_pool->add(new DppWriter(1, _output_row_expr_ctxs, fh));
    RETURN_IF_ERROR(_writer->open());

    return Status::OK();
}

Status Translator::create_value_updaters() {
    if (_rollup_schema.values().size() != _rollup_schema.value_ops().size()) {
        return Status::InternalError("size of values and value_ops are not equal.");
    }

    int num_values = _rollup_schema.values().size();

    std::string keys_type = _rollup_schema.keys_type();
    if ("DUP_KEYS" == keys_type) {
        return Status::OK();
    } else if ("UNIQUE_KEYS" == keys_type) {
        for (int i = 0; i < num_values; ++i) {
            _value_updaters.push_back(fake_update);
        }
        return Status::OK();
    }

    for (int i = 0; i < num_values; ++i) {
        switch (_rollup_schema.values()[i]->root()->type().type) {
        case TYPE_TINYINT: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<int8_t>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<int8_t>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<int8_t>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }
        case TYPE_SMALLINT: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<int16_t>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<int16_t>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<int16_t>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }
        case TYPE_INT: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<int32_t>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<int32_t>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<int32_t>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }
        case TYPE_BIGINT: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<int64_t>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<int64_t>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<int64_t>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }
        case TYPE_LARGEINT: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<__int128>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<__int128>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<__int128>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }
        case TYPE_FLOAT: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<float>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<float>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<float>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }
        case TYPE_DOUBLE: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<double>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<double>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<double>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }

        case TYPE_DECIMALV2: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<__int128>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<__int128>);
                break;
            case TAggregationType::SUM:
                _value_updaters.push_back(update_sum<__int128>);
                break;
            default:
                _value_updaters.push_back(fake_update);
            }
            break;
        }

        case TYPE_DATE:
        case TYPE_DATETIME: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
                _value_updaters.push_back(update_max<DateTimeValue>);
                break;
            case TAggregationType::MIN:
                _value_updaters.push_back(update_min<DateTimeValue>);
                break;
            case TAggregationType::SUM:
                return Status::InternalError("Unsupported sum operation on date/datetime column.");
            default:
                // replace
                _value_updaters.push_back(fake_update);
                break;
            }
            break;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::MAX:
            case TAggregationType::MIN:
            case TAggregationType::SUM:
                return Status::InternalError(
                        "Unsupported max/min/sum operation on char/varchar/string column.");
            default:
                // Only replace has meaning
                _value_updaters.push_back(fake_update);
                break;
            }
            break;
        }
        case TYPE_HLL: {
            switch (_rollup_schema.value_ops()[i]) {
            case TAggregationType::HLL_UNION:
                // only placeholder，merge in Translator::update_merge
                _value_updaters.push_back(fake_update);
                break;
            case TAggregationType::MAX:
            case TAggregationType::MIN:
            case TAggregationType::SUM:
                return Status::InternalError("Unsupported max/min/sum operation on hll column.");
            default:
                _value_updaters.push_back(fake_update);
                break;
            }
            break;
        }
        default: {
            std::stringstream ss;
            ss << "Unsupported column type(" << _rollup_schema.values()[i]->root()->type() << ")";
            // No operation, just push back a fake update
            return Status::InternalError(ss.str());
            break;
        }
        }
    }

    return Status::OK();
}

Status Translator::create_profile(RuntimeState* state) {
    // Add profile
    std::stringstream ss;
    ss << "Dpp translator(" << _tablet_desc.partition_id << "_" << _tablet_desc.bucket_id << "_"
       << _rollup_name << ")";
    _profile = state->obj_pool()->add(new RuntimeProfile(ss.str()));

    _add_batch_timer = ADD_TIMER(_profile, "add batch time");
    _sort_timer = ADD_TIMER(_profile, "sort time");
    _agg_timer = ADD_TIMER(_profile, "aggregate time");
    _writer_timer = ADD_TIMER(_profile, "write to file time");
    return Status::OK();
}

Status Translator::prepare(RuntimeState* state) {
    // create output path
    format_output_path(state);
    // create profile first
    RETURN_IF_ERROR(create_profile(state));
    // 1. Create sorter
    RETURN_IF_ERROR(create_sorter(state));

    // 2. Create comparator
    RETURN_IF_ERROR(create_comparator(state));

    // 3. Create writer
    RETURN_IF_ERROR(create_writer(state));

    // 4. new batch for writer
    _batch_to_write.reset(new RowBatch(_row_desc, state->batch_size()));
    if (_batch_to_write.get() == nullptr) {
        return Status::InternalError("No memory to allocate RowBatch.");
    }

    // 5. prepare value updater
    RETURN_IF_ERROR(create_value_updaters());

    int hll_column_count = 0;
    for (int i = 0; i < _rollup_schema.values().size(); ++i) {
        if (_rollup_schema.value_ops()[i] == TAggregationType::HLL_UNION) {
            hll_column_count++;
        }
    }
    _hll_merge.prepare(hll_column_count, ((QSorter*)_sorter)->get_mem_pool());
    return Status::OK();
}

Status Translator::add_batch(RowBatch* batch) {
    SCOPED_TIMER(_add_batch_timer);
    return _sorter->add_batch(batch);
}

bool Translator::eq_tuple_row(TupleRow* last, TupleRow* cur) {
    int num_exprs = _last_row_expr_ctxs.size();
    for (int i = 0; i < num_exprs; ++i) {
        void* last_value = _last_row_expr_ctxs[i]->get_value(last);
        void* cur_value = _cur_row_expr_ctxs[i]->get_value(cur);

        if (RawValue::compare(last_value, cur_value, _last_row_expr_ctxs[i]->root()->type()) != 0) {
            return false;
        }
    }

    return true;
}

void Translator::copy_row(TupleRow* row) {
    int row_idx = _batch_to_write->add_row();
    TupleRow* last_row = _batch_to_write->get_row(row_idx);
    row->deep_copy(last_row, _batch_to_write->row_desc().tuple_descriptors(),
                   _batch_to_write->tuple_data_pool(), false);
    // NOTE: Don't commit this row.
}

// merge value must be slot expr,
void Translator::update_row(TupleRow* agg_row, TupleRow* row) {
    int index = 0;
    for (int i = 0; i < _rollup_schema.values().size(); ++i) {
        ExprContext* ctx = _rollup_schema.values()[i];
        SlotRef* ref = (SlotRef*)(ctx->root());
        if (_rollup_schema.value_ops()[i] == TAggregationType::HLL_UNION) {
            _hll_merge.update_hll_set(agg_row, row, ctx, index);
            index++;
        } else {
            _value_updaters[i](ref, agg_row, row);
        }
    }
}

void HllDppSinkMerge::prepare(int count, MemPool* pool) {
    while (count--) {
        HllMergeValue* value = new HllMergeValue();
        value->type = HLL_DATA_EMPTY;
        _hll_last_row.push_back(value);
    }
}

void HllDppSinkMerge::update_hll_set(TupleRow* agg_row, TupleRow* row, ExprContext* ctx,
                                     int index) {
    HllMergeValue* value = _hll_last_row[index];
    StringValue* row_sv = static_cast<StringValue*>(SlotRef::get_value(ctx->root(), row));
    HllSetResolver row_resolver;
    HllSetResolver agg_row_resolver;
    row_resolver.init(row_sv->ptr, row_sv->len);
    row_resolver.parse();
    const int REGISTERS_SIZE = (int)std::pow(2, HLL_COLUMN_PRECISION);
    if (value->type == HLL_DATA_EMPTY) {
        value->type = HLL_DATA_EXPLICIT;
        StringValue* agg_row_sv =
                static_cast<StringValue*>(SlotRef::get_value(ctx->root(), agg_row));
        agg_row_resolver.init(agg_row_sv->ptr, agg_row_sv->len);
        agg_row_resolver.parse();
        if (agg_row_resolver.get_hll_data_type() == HLL_DATA_EXPLICIT) {
            value->hash_set.insert(agg_row_resolver.get_explicit_value(0));
        }
        if (row_resolver.get_hll_data_type() == HLL_DATA_EXPLICIT) {
            value->hash_set.insert(row_resolver.get_explicit_value(0));
        }
    } else if (value->type == HLL_DATA_EXPLICIT) {
        value->hash_set.insert(row_resolver.get_explicit_value(0));
        if (value->hash_set.size() > HLL_EXPLICIT_INT64_NUM) {
            value->type = HLL_DATA_SPARSE;
            for (std::set<uint64_t>::iterator iter = value->hash_set.begin();
                 iter != value->hash_set.end(); ++iter) {
                uint64_t hash = *iter;
                int idx = hash % REGISTERS_SIZE;
                uint8_t first_one_bit = __builtin_ctzl(hash >> HLL_COLUMN_PRECISION) + 1;
                if (value->index_to_value.find(idx) != value->index_to_value.end()) {
                    value->index_to_value[idx] = value->index_to_value[idx] < first_one_bit
                                                         ? first_one_bit
                                                         : value->index_to_value[idx];
                } else {
                    value->index_to_value[idx] = first_one_bit;
                }
            }
        }
    } else if (value->type == HLL_DATA_SPARSE) {
        uint64_t hash = row_resolver.get_explicit_value(0);
        int idx = hash % REGISTERS_SIZE;
        uint8_t first_one_bit = __builtin_ctzl(hash >> HLL_COLUMN_PRECISION) + 1;
        if (value->index_to_value.find(idx) != value->index_to_value.end()) {
            value->index_to_value[idx] = value->index_to_value[idx] < first_one_bit
                                                 ? first_one_bit
                                                 : value->index_to_value[idx];
        } else {
            value->index_to_value[idx] = first_one_bit;
        }
    } else {
    }
}

void HllDppSinkMerge::finalize_one_merge(TupleRow* agg_row, MemPool* pool,
                                         const RollupSchema& rollup_schema) {
    if (_hll_last_row.size() <= 0) {
        return;
    }
    const int REGISTERS_SIZE = (int)std::pow(2, HLL_COLUMN_PRECISION);
    int index = 0;
    for (int i = 0; i < rollup_schema.values().size(); ++i) {
        ExprContext* ctx = rollup_schema.values()[i];
        StringValue* agg_row_sv =
                static_cast<StringValue*>(SlotRef::get_value(ctx->root(), agg_row));
        if (rollup_schema.value_ops()[i] == TAggregationType::HLL_UNION) {
            HllMergeValue* value = _hll_last_row[index++];
            // explicit set
            if (value->type == HLL_DATA_EXPLICIT) {
                int set_len = 1 + 1 + value->hash_set.size() * 8;
                char* result = (char*)pool->allocate(set_len);
                memset(result, 0, set_len);
                HllSetHelper::set_explicit(result, value->hash_set, set_len);
                agg_row_sv->replace(result, set_len);
            } else if (value->type == HLL_DATA_SPARSE) {
                // full explicit set
                if (value->index_to_value.size() * (sizeof(HllSetResolver::SparseIndexType) +
                                                    sizeof(HllSetResolver::SparseValueType)) +
                            sizeof(HllSetResolver::SparseLengthValueType) >
                    REGISTERS_SIZE) {
                    int set_len = 1 + REGISTERS_SIZE;
                    char* result = (char*)pool->allocate(set_len);
                    memset(result, 0, set_len);
                    HllSetHelper::set_full(result, value->index_to_value, REGISTERS_SIZE, set_len);
                    agg_row_sv->replace(result, set_len);
                } else {
                    // sparse explicit set
                    int set_len = 1 + sizeof(HllSetResolver::SparseLengthValueType) +
                                  3 * value->index_to_value.size();
                    char* result = (char*)pool->allocate(set_len);
                    memset(result, 0, set_len);
                    HllSetHelper::set_sparse(result, value->index_to_value, set_len);
                    agg_row_sv->replace(result, set_len);
                }
            } else {
            }
        }
    }
    for (int i = 0; i < _hll_last_row.size(); i++) {
        HllMergeValue* value = _hll_last_row[i];
        value->hash_set.clear();
        value->index_to_value.clear();
        value->type = HLL_DATA_EMPTY;
    }
}

void HllDppSinkMerge::close() {
    for (int i = 0; i < _hll_last_row.size(); i++) {
        HllMergeValue* value = _hll_last_row[i];
        delete value;
    }
    _hll_last_row.clear();
}

// use batch to release data
Status Translator::process_one_row(TupleRow* row) {
    if (row == nullptr) {
        // Something strange happened
        std::stringstream ss;
        ss << "row is nullptr.";
        LOG(ERROR) << ss.str();
        return Status::InternalError(ss.str());
    }
    // first row
    // Just deep copy, and don't reuse its data.
    if (!_batch_to_write->in_flight()) {
        copy_row(row);
        return Status::OK();
    }

    int row_idx = _batch_to_write->add_row();
    TupleRow* last_row = _batch_to_write->get_row(row_idx);
    std::string keys_type = _rollup_schema.keys_type();
    if ("AGG_KEYS" == keys_type || "UNIQUE_KEYS" == keys_type) {
        if (eq_tuple_row(last_row, row)) {
            // Just merge to last row and return
            update_row(last_row, row);
            return Status::OK();
        }
    }
    _hll_merge.finalize_one_merge(last_row, ((QSorter*)_sorter)->get_mem_pool(), _rollup_schema);
    // Commit last row and check if batch is full
    _batch_to_write->commit_last_row();
    if (_batch_to_write->is_full()) {
        SCOPED_TIMER(_writer_timer);
        // output this batch
        RETURN_IF_ERROR(_writer->add_batch(_batch_to_write.get()));
        // reset batch to free memory
        _batch_to_write->reset();
    }

    // deep copy the new row
    copy_row(row);

    return Status::OK();
}

Status Translator::process(RuntimeState* state) {
    // 1. sort all the data in sorter.
    {
        SCOPED_TIMER(_sort_timer);
        RETURN_IF_ERROR(_sorter->input_done());
    }

    // 2. read data from sorter and aggregate them
    {
        SCOPED_TIMER(_agg_timer);
        bool eos = false;
        while (!eos) {
            RowBatch batch(_row_desc, state->batch_size());

            RETURN_IF_ERROR(_sorter->get_next(&batch, &eos));

            int num_rows = batch.num_rows();
            for (int i = 0; i < num_rows; ++i) {
                TupleRow* row = batch.get_row(i);
                RETURN_IF_ERROR(process_one_row(row));
            }
        }
        // merge last set in vector
        int row_idx = _batch_to_write->add_row();
        TupleRow* last_row = _batch_to_write->get_row(row_idx);
        _hll_merge.finalize_one_merge(last_row, ((QSorter*)_sorter)->get_mem_pool(),
                                      _rollup_schema);
    }

    // Send the last batch if there any
    if (_batch_to_write->in_flight()) {
        _batch_to_write->commit_last_row();
        RETURN_IF_ERROR(_writer->add_batch(_batch_to_write.get()));
    }

    {
        SCOPED_TIMER(_writer_timer);
        RETURN_IF_ERROR(_writer->close());
    }

    // Output last row
    return Status::OK();
}

Status Translator::close(RuntimeState* state) {
    if (_sorter != nullptr) {
        _sorter->close(state);
    }
    Expr::close(_last_row_expr_ctxs, state);
    Expr::close(_cur_row_expr_ctxs, state);
    Expr::close(_output_row_expr_ctxs, state);
    _batch_to_write.reset();
    _hll_merge.close();
    return Status::OK();
}

Status DppSink::init(RuntimeState* state) {
    _profile = state->obj_pool()->add(new RuntimeProfile("Dpp sink"));
    return Status::OK();
}

Status DppSink::get_or_create_translator(ObjectPool* obj_pool, RuntimeState* state,
                                         const TabletDesc& tablet_desc,
                                         std::vector<Translator*>** trans_vec) {
    auto iter = _translator_map.find(tablet_desc);
    if (iter != _translator_map.end()) {
        *trans_vec = &iter->second;
        return Status::OK();
    }
    // new one
    _translator_map.insert(std::make_pair(tablet_desc, std::vector<Translator*>()));
    *trans_vec = &_translator_map[tablet_desc];
    // create one translator for every rollup
    for (auto& it : _rollup_map) {
        // Translator* translator = state->obj_pool()->add(
        Translator* translator = obj_pool->add(
                new Translator(tablet_desc, _row_desc, it.first, *it.second, obj_pool));
        RETURN_IF_ERROR(translator->prepare(state));
        _profile->add_child(translator->profile(), true, nullptr);
        (*trans_vec)->push_back(translator);
    }
    _translator_count += (*trans_vec)->size();
    return Status::OK();
}

Status DppSink::add_batch(ObjectPool* obj_pool, RuntimeState* state, const TabletDesc& desc,
                          RowBatch* batch) {
    SCOPED_TIMER(_profile->total_time_counter());
    std::vector<Translator*>* trans_vec = nullptr;
    RETURN_IF_ERROR(get_or_create_translator(obj_pool, state, desc, &trans_vec));
    // add for every one
    for (auto& trans : *trans_vec) {
        RETURN_IF_ERROR(trans->add_batch(batch));
    }
    // add this batch to appoint translator
    return Status::OK();
}

void DppSink::process(RuntimeState* state, Translator* trans, CountDownLatch* latch) {
    // add dpp into cgroups
    CgroupsMgr::apply_system_cgroup();
    Status status = trans->process(state);
    if (!status.ok()) {
        state->set_process_status(status);
    }
    latch->count_down();
}

Status DppSink::finish(RuntimeState* state) {
    SCOPED_TIMER(_profile->total_time_counter());

    CountDownLatch latch(_translator_count);
    for (auto& iter : _translator_map) {
        for (auto& trans : iter.second) {
            state->exec_env()->etl_thread_pool()->offer(
                    std::bind<void>(&DppSink::process, this, state, trans, &latch));
        }
    }

    latch.wait();

    // Set output files in runtime state
    collect_output(&state->output_files());

    for (auto& iter : _translator_map) {
        for (auto& trans : iter.second) {
            trans->close(state);
        }
    }

    return state->query_status();
}

void DppSink::collect_output(std::vector<std::string>* files) {
    for (auto& iter : _translator_map) {
        for (auto& trans : iter.second) {
            files->push_back(trans->output_path());
        }
    }
}

} // namespace doris
