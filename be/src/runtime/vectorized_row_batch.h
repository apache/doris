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

#ifndef PALO_BE_SRC_RUNTIME_VECTORIZED_ROW_BATCH_H
#define PALO_BE_SRC_RUNTIME_VECTORIZED_ROW_BATCH_H

#include <cstddef>
#include <memory>
#include <vector>

#include "olap/field.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "runtime/row_batch_interface.hpp"
#include "runtime/row_batch.h"
#include "util/mem_util.hpp"

namespace palo {

class VectorizedRowBatch;

struct BackupInfo {
    BackupInfo() : selected_in_use(false), size(0), selected(NULL) {}

    bool selected_in_use;
    int size;
    int* selected;
};

class ColumnVector {
public:
    virtual ~ColumnVector() {
        //if (NULL == _is_null) {
            //delete _is_null;
        //}
    }

    inline bool is_repeating() {
        return _is_repeating;
    }

    void set_is_repeating(bool is_repeating) {
        _is_repeating = is_repeating;
    }

    void* col_data() {
        return _col_data;
    }
    void set_col_data(void* data) {
        _col_data = data;
    }

    void* col_string_data() {
        return _col_string_data;
    }
    void set_col_string_data(void* data) {
        _col_string_data = data;
    }

    int byte_size() {
        return _byte_size;
    }
    void set_byte_size(int byte_size) {
        _byte_size = byte_size;
    }
private:
    ColumnVector(int size) {
        _is_repeating = false;
        //_no_nulls = true;
        //_is_null = new bool[size];
        _col_data = NULL;
        _col_string_data = NULL;
        _byte_size = 0;
    }
    friend class VectorizedRowBatch;
    void* _col_data;
    void* _col_string_data;
    int _byte_size;
    bool _is_repeating;
    // this is no null in palo now
    //bool _no_nulls;
    //bool* _is_null;
};

class VectorizedRowBatch : public RowBatchInterface {
public:
    //VectorizedRowBatch(const TupleDescriptor& tuple_desc, int capacity);
    VectorizedRowBatch(const std::vector<FieldInfo>& schema, int capacity);
    virtual ~VectorizedRowBatch() { }

    MemPool* mem_pool() {
        return _mem_pool.get();
    }

    void add_column(int index, const TypeDescriptor& type) {
        if (-1 == index) {
            return;
        }

        DCHECK_EQ(index, _columns.size());
        boost::shared_ptr<ColumnVector> col_vec(new ColumnVector(_capacity));
        col_vec->set_col_data(_mem_pool->allocate(type.get_slot_size() * _capacity));
        _columns.push_back(col_vec);
    }

    ColumnVector* column(int column_index) {
        DCHECK_GE(column_index, 0);
        DCHECK_LT(column_index, _columns.size());
        return _columns[column_index].get();
    }

    int capacity() {
        return _capacity;
    }

    int size() {
        return _size;
    }

    void set_size(int size) {
        DCHECK_LE(size, _capacity);
        _size = size;
    }

    inline int num_rows() {
        return _size;
    }

    bool selected_in_use() const {
        return _selected_in_use;
    }

    void set_selected_in_use(bool selected_in_use) {
        _selected_in_use = selected_in_use;
    }

    int* selected() const {
        return _selected;
    }

    void set_selected(int* selected) {
        for (int i = 0; i < _capacity; ++i) {
            _selected[i] = selected[i];
        }
    }

    inline void backup() {
        _backup_info.size = _size;
        _backup_info.selected_in_use = _selected_in_use;
        if (_selected_in_use) {
            if (NULL == _backup_info.selected) {
                _backup_info.selected
                    = reinterpret_cast<int*>(_mem_pool->allocate(sizeof(int) * _capacity));
            }
            for (int i = 0; i < _capacity; ++i) {
                _backup_info.selected[i] = _selected[i];
            }
        }
        _has_backup = true;
    }

    inline void restore() {
        if (_has_backup) {
            _size = _backup_info.size;
            _selected_in_use = _backup_info.selected_in_use;
            if (_selected_in_use) {
                _selected = _backup_info.selected;
            }
            _row_iter = 0;
        }
    }

    //// reorganized memory layout from PAX storage
    //void reorganized_from_pax(const std::vector<FieldInfo>& schema);

    //// reorganized memory layout from DSM storage(Column Store)
    //void reorganized_from_dsm();

    inline bool is_iterator_end() {
        return _row_iter >= _size;
    }

    inline void reset_row_iterator() {
        _row_iter = 0;
    }

    inline void reset() {
        _size = 0;
        _selected_in_use = false;
        _row_iter = 0;
        _columns.erase(_columns.begin() + _num_cols, _columns.end());
        _mem_pool->clear();
        _selected = reinterpret_cast<int*>(_mem_pool->allocate(sizeof(int) * _capacity));
    }

    bool get_next_tuple(Tuple* tuple, const TupleDescriptor& tuple_desc);

    void to_row_batch(RowBatch* row_batch, const TupleDescriptor& tuple_desc);

private:
    //const TupleDescriptor& _tuple_desc;
    std::vector<FieldInfo> _schema;
    Tuple* _tuple;
    const int _capacity;
    const int _num_cols;
    int _size;
    int* _selected;
    bool _selected_in_use;
    int _row_iter;
    BackupInfo _backup_info;
    bool _has_backup;
    std::vector<boost::shared_ptr<ColumnVector> > _columns;

    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
};

}

#endif  // _PALO_BE_SRC_RUNTIME_VECTORIZED_ROW_BATCH_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
