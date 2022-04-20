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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exec/row-batch-list.h
// and modified by Doris

#ifndef DORIS_BE_SRC_QUERY_EXEC_ROW_BATCH_LIST_H
#define DORIS_BE_SRC_QUERY_EXEC_ROW_BATCH_LIST_H

#include <string>
#include <vector>

#include "common/logging.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"

namespace doris {

class TupleRow;
class RowDescriptor;
class MemPool;

// A simple list structure for RowBatches that provides an interface for
// iterating over the TupleRows.
class RowBatchList {
public:
    RowBatchList() : _total_num_rows(0) {}
    virtual ~RowBatchList() {}

    // A simple iterator used to scan over all the rows stored in the list.
    class TupleRowIterator {
    public:
        // Dummy constructor
        TupleRowIterator() : _list(nullptr), _row_idx(0) {}
        virtual ~TupleRowIterator() {}

        // Returns true if this iterator is at the end, i.e. get_row() cannot be called.
        bool at_end() { return _batch_it == _list->_row_batches.end(); }

        // Returns the current row. Callers must check the iterator is not at_end() before
        // calling get_row().
        TupleRow* get_row() {
            DCHECK(!at_end());
            return (*_batch_it)->get_row(_row_idx);
        }

        // Increments the iterator. No-op if the iterator is at the end.
        void next() {
            if (_batch_it == _list->_row_batches.end()) {
                return;
            }

            if (++_row_idx == (*_batch_it)->num_rows()) {
                ++_batch_it;
                _row_idx = 0;
            }
        }

    private:
        friend class RowBatchList;

        TupleRowIterator(RowBatchList* list)
                : _list(list), _batch_it(list->_row_batches.begin()), _row_idx(0) {}

        RowBatchList* _list;
        std::vector<RowBatch*>::iterator _batch_it;
        int64_t _row_idx;
    };

    // Add the 'row_batch' to the list. The RowBatch* and all of its resources are owned
    // by the caller.
    void add_row_batch(RowBatch* row_batch) {
        if (row_batch->num_rows() == 0) {
            return;
        }

        _row_batches.push_back(row_batch);
        _total_num_rows += row_batch->num_rows();
    }

    // Resets the list.
    void reset() {
        _row_batches.clear();
        _total_num_rows = 0;
    }

    // Outputs a debug string containing the contents of the list.
    std::string debug_string(const RowDescriptor& desc) {
        std::stringstream out;
        out << "RowBatchList(";
        out << "num_rows=" << _total_num_rows << "; ";
        RowBatchList::TupleRowIterator it = iterator();

        while (!it.at_end()) {
            out << " " << it.get_row()->to_string(desc);
            it.next();
        }

        out << " )";
        return out.str();
    }

    // Returns the total number of rows in all row batches.
    int64_t total_num_rows() { return _total_num_rows; }

    // Returns a new iterator over all the tuple rows.
    TupleRowIterator iterator() { return TupleRowIterator(this); }

private:
    friend class TupleRowIterator;

    std::vector<RowBatch*> _row_batches;

    // Total number of rows
    int64_t _total_num_rows;
};

} // namespace doris

#endif
