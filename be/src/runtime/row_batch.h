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

#ifndef DORIS_BE_RUNTIME_ROW_BATCH_H
#define DORIS_BE_RUNTIME_ROW_BATCH_H

#include <cstring>
#include <vector>

#include "common/logging.h"
#include "runtime/buffered_block_mgr2.h" // for BufferedBlockMgr2::Block
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/descriptors.h"
#include "runtime/disk_io_mgr.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch_interface.hpp"

namespace doris::vectorized {
class Block;
}

namespace doris {

class BufferedTupleStream2;
class Tuple;
class TupleRow;
class TupleDescriptor;
class PRowBatch;

// A RowBatch encapsulates a batch of rows, each composed of a number of tuples.
// The maximum number of rows is fixed at the time of construction, and the caller
// can add rows up to that capacity.
// The row batch reference a few different sources of memory.
//   1. TupleRow ptrs - this is always owned and managed by the row batch.
//   2. Tuple memory - this is allocated (or transferred to) the row batches tuple pool.
//   3. Auxiliary tuple memory (e.g. string data) - this can either be stored externally
//      (don't copy strings) or from the tuple pool (strings are copied).  If external,
//      the data is in an io buffer that may not be attached to this row batch.  The
//      creator of that row batch has to make sure that the io buffer is not recycled
//      until all batches that reference the memory have been consumed.
// In order to minimize memory allocations, RowBatches and PRowBatches that have been
// serialized and sent over the wire should be reused (this prevents _compression_scratch
// from being needlessly reallocated).
//
// Row batches and memory usage: We attempt to stream row batches through the plan
// tree without copying the data. This means that row batches are often not-compact
// and reference memory outside of the row batch. This results in most row batches
// having a very small memory footprint and in some row batches having a very large
// one (it contains all the memory that other row batches are referencing). An example
// is IoBuffers which are only attached to one row batch. Only when the row batch reaches
// a blocking operator or the root of the fragment is the row batch memory freed.
// This means that in some cases (e.g. very selective queries), we still need to
// pass the row batch through the exec nodes (even if they have no rows) to trigger
// memory deletion. at_capacity() encapsulates the check that we are not accumulating
// excessive memory.
//
// A row batch is considered at capacity if all the rows are full or it has accumulated
// auxiliary memory up to a soft cap. (See _at_capacity_mem_usage comment).
// TODO: stick _tuple_ptrs into a pool?
class RowBatch : public RowBatchInterface {
public:
    /// Flag indicating whether the resources attached to a RowBatch need to be flushed.
    /// Defined here as a convenience for other modules that need to communicate flushing
    /// modes.
    enum class FlushMode {
        FLUSH_RESOURCES,
        NO_FLUSH_RESOURCES,
    };

    // Create RowBatch for a maximum of 'capacity' rows of tuples specified
    // by 'row_desc'.
    RowBatch(const RowDescriptor& row_desc, int capacity, MemTracker* mem_tracker);

    // Populate a row batch from input_batch by copying input_batch's
    // tuple_data into the row batch's mempool and converting all offsets
    // in the data back into pointers.
    // TODO: figure out how to transfer the data from input_batch to this RowBatch
    // (so that we don't need to make yet another copy)
    RowBatch(const RowDescriptor& row_desc, const PRowBatch& input_batch, MemTracker* tracker);

    // Releases all resources accumulated at this row batch.  This includes
    //  - tuple_ptrs
    //  - tuple mem pool data
    //  - buffer handles from the io mgr
    virtual ~RowBatch();

    // used to c
    void clear();

    static const int INVALID_ROW_INDEX = -1;

    // Add n rows of tuple pointers after the last committed row and return its index.
    // The rows are uninitialized and each tuple of the row must be set.
    // Returns INVALID_ROW_INDEX if the row batch cannot fit n rows.
    // Two consecutive add_row() calls without a commit_last_row() between them
    // have the same effect as a single call.
    int add_rows(int n) {
        if (_num_rows + n > _capacity) {
            return INVALID_ROW_INDEX;
        }

        _has_in_flight_row = true;
        return _num_rows;
    }

    int add_row() { return add_rows(1); }

    void commit_rows(int n) {
        DCHECK_GE(n, 0);
        DCHECK_LE(_num_rows + n, _capacity);
        _num_rows += n;
        _has_in_flight_row = false;
    }

    void commit_last_row() { commit_rows(1); }

    bool in_flight() const { return _has_in_flight_row; }

    // Set function can be used to reduce the number of rows in the batch.  This is only
    // used in the limit case where more rows were added than necessary.
    void set_num_rows(int num_rows) {
        DCHECK_LE(num_rows, _num_rows);
        _num_rows = num_rows;
    }

    // Returns true if the row batch has filled all the rows or has accumulated
    // enough memory.
    bool at_capacity() const {
        return _num_rows == _capacity || _auxiliary_mem_usage >= AT_CAPACITY_MEM_USAGE ||
               num_tuple_streams() > 0 || _need_to_return;
    }

    // Returns true if the row batch has filled all the rows or has accumulated
    // enough memory. tuple_pool is an intermediate memory pool containing tuple data
    // that will eventually be attached to this row batch. We need to make sure
    // the tuple pool does not accumulate excessive memory.
    bool at_capacity(const MemPool* tuple_pool) const {
        DCHECK(tuple_pool != nullptr);
        return at_capacity() || tuple_pool->total_allocated_bytes() > AT_CAPACITY_MEM_USAGE;
    }

    // Returns true if row_batch has reached capacity.
    bool is_full() const { return _num_rows == _capacity; }

    // Returns true if uncommited rows has reached capacity.
    bool is_full_uncommited() { return _num_uncommitted_rows == _capacity; }

    // Returns true if the row batch has accumulated enough external memory (in MemPools
    // and io buffers).  This would be a trigger to compact the row batch or reclaim
    // the memory in some way.
    bool at_resource_limit() {
        return tuple_data_pool()->total_allocated_bytes() > MAX_MEM_POOL_SIZE;
    }

    // The total size of all data represented in this row batch (tuples and referenced
    // string data).
    size_t total_byte_size() const;

    TupleRow* get_row(int row_idx) const {
        DCHECK(_tuple_ptrs != nullptr);
        DCHECK_GE(row_idx, 0);
        //DCHECK_LT(row_idx, _num_rows + (_has_in_flight_row ? 1 : 0));
        return reinterpret_cast<TupleRow*>(_tuple_ptrs + row_idx * _num_tuples_per_row);
    }

    /// An iterator for going through a row batch, starting at 'row_idx'.
    /// If 'limit' is specified, it will iterate up to row number 'row_idx + limit'
    /// or the last row, whichever comes first. Otherwise, it will iterate till the last
    /// row in the batch. This is more efficient than using GetRow() as it avoids loading
    /// the row batch state and doing multiplication on each loop with GetRow().
    class Iterator {
    public:
        Iterator(RowBatch* parent, int row_idx, int limit = -1)
                : _num_tuples_per_row(parent->_num_tuples_per_row),
                  _row(parent->_tuple_ptrs + _num_tuples_per_row * row_idx),
                  _row_batch_end(parent->_tuple_ptrs +
                                 _num_tuples_per_row *
                                         (limit == -1 ? parent->_num_rows
                                                      : std::min<int>(row_idx + limit,
                                                                      parent->_num_rows))),
                  _parent(parent) {
            DCHECK_GE(row_idx, 0);
            DCHECK_GT(_num_tuples_per_row, 0);
            /// We allow empty row batches with _num_rows == capacity_ == 0.
            /// That's why we cannot call GetRow() above to initialize '_row'.
            DCHECK_LE(row_idx, parent->_capacity);
        }

        /// Return the current row pointed to by the row pointer.
        TupleRow* get() { return reinterpret_cast<TupleRow*>(_row); }

        /// Increment the row pointer and return the next row.
        TupleRow* next() {
            _row += _num_tuples_per_row;
            DCHECK_LE((_row - _parent->_tuple_ptrs) / _num_tuples_per_row, _parent->_capacity);
            return get();
        }

        /// Returns true if the iterator is beyond the last row for read iterators.
        /// Useful for read iterators to determine the limit. Write iterators should use
        /// RowBatch::AtCapacity() instead.
        bool at_end() const { return _row >= _row_batch_end; }

        /// Returns the row batch which this iterator is iterating through.
        RowBatch* parent() const { return _parent; }

    private:
        /// Number of tuples per row.
        const int _num_tuples_per_row;

        /// Pointer to the current row.
        Tuple** _row;

        /// Pointer to the row after the last row for read iterators.
        Tuple** const _row_batch_end;

        /// The row batch being iterated on.
        RowBatch* const _parent;
    };

    int num_tuples_per_row() const { return _num_tuples_per_row; }
    int row_byte_size() const { return _num_tuples_per_row * sizeof(Tuple*); }
    MemPool* tuple_data_pool() { return &_tuple_data_pool; }
    ObjectPool* agg_object_pool() { return &_agg_object_pool; }
    int num_io_buffers() const { return _io_buffers.size(); }
    int num_tuple_streams() const { return _tuple_streams.size(); }

    // increase # of uncommitted rows
    void increase_uncommitted_rows();

    // Resets the row batch, returning all resources it has accumulated.
    void reset();

    // Add io buffer to this row batch.
    void add_io_buffer(DiskIoMgr::BufferDescriptor* buffer);

    // Add tuple stream to this row batch. The row batch takes ownership of the stream
    // and will call Close() on the stream and delete it when freeing resources.
    void add_tuple_stream(BufferedTupleStream2* stream);

    /// Adds a buffer to this row batch. The buffer is deleted when freeing resources.
    /// The buffer's memory remains accounted against the original owner, even when the
    /// ownership of batches is transferred. If the original owner wants the memory to be
    /// released, it should call this with 'mode' FLUSH_RESOURCES (see MarkFlushResources()
    /// for further explanation).
    /// TODO: IMPALA-4179: after IMPALA-3200, simplify the ownership transfer model and
    /// make it consistent between buffers and I/O buffers.
    void add_buffer(BufferPool::ClientHandle* client, BufferPool::BufferHandle&& buffer,
                    FlushMode flush);

    // Adds a block to this row batch. The block must be pinned. The blocks must be
    // deleted when freeing resources.
    void add_block(BufferedBlockMgr2::Block* block);

    // Called to indicate this row batch must be returned up the operator tree.
    // This is used to control memory management for streaming rows.
    // TODO: consider using this mechanism instead of add_io_buffer/add_tuple_stream. This is
    // the property we need rather than meticulously passing resources up so the operator
    // tree.
    void mark_need_to_return() { _need_to_return = true; }

    bool need_to_return() const { return _need_to_return; }

    /// Used by an operator to indicate that it cannot produce more rows until the
    /// resources that it has attached to the row batch are freed or acquired by an
    /// ancestor operator. After this is called, the batch is at capacity and no more rows
    /// can be added. The "flush" mark is transferred by TransferResourceOwnership(). This
    /// ensures that batches are flushed by streaming operators all the way up the operator
    /// tree. Blocking operators can still accumulate batches with this flag.
    /// TODO: IMPALA-3200: blocking operators should acquire all memory resources including
    /// attached blocks/buffers, so that MarkFlushResources() can guarantee that the
    /// resources will not be accounted against the original operator (this is currently
    /// not true for Blocks, which can't be transferred).
    void mark_flush_resources() {
        DCHECK_LE(_num_rows, _capacity);
        _capacity = _num_rows;
        _flush = FlushMode::FLUSH_RESOURCES;
    }

    /// Called to indicate that some resources backing this batch were not attached and
    /// will be cleaned up after the next GetNext() call. This means that the batch must
    /// be returned up the operator tree. Blocking operators must deep-copy any rows from
    /// this batch or preceding batches.
    ///
    /// This is a stronger version of MarkFlushResources(), because blocking operators
    /// are not allowed to accumulate batches with the 'needs_deep_copy' flag.
    /// TODO: IMPALA-4179: always attach backing resources and remove this flag.
    void mark_needs_deep_copy() {
        mark_flush_resources(); // No more rows should be added to the batch.
        _needs_deep_copy = true;
    }

    bool needs_deep_copy() const { return _needs_deep_copy; }

    // Transfer ownership of resources to dest.  This includes tuple data in mem
    // pool and io buffers.
    // we firstly update dest resource, and then reset current resource
    void transfer_resource_ownership(RowBatch* dest);

    void copy_row(TupleRow* src, TupleRow* dest) {
        memcpy(dest, src, _num_tuples_per_row * sizeof(Tuple*));
    }

    // Copy 'num_rows' rows from 'src' to 'dest' within the batch. Useful for exec
    // nodes that skip an offset and copied more than necessary.
    void copy_rows(int dest, int src, int num_rows) {
        DCHECK_LE(dest, src);
        DCHECK_LE(src + num_rows, _capacity);
        memmove(_tuple_ptrs + _num_tuples_per_row * dest, _tuple_ptrs + _num_tuples_per_row * src,
                num_rows * _num_tuples_per_row * sizeof(Tuple*));
    }

    void clear_row(TupleRow* row) { memset(row, 0, _num_tuples_per_row * sizeof(Tuple*)); }

    // Acquires state from the 'src' row batch into this row batch. This includes all IO
    // buffers and tuple data.
    // This row batch must be empty and have the same row descriptor as the src batch.
    // This is used for scan nodes which produce RowBatches asynchronously.  Typically,
    // an ExecNode is handed a row batch to populate (pull model) but ScanNodes have
    // multiple threads which push row batches.
    // TODO: this is wasteful and makes a copy that's unnecessary.  Think about cleaning
    // this up.
    // TODO: rename this or unify with TransferResourceOwnership()
    void acquire_state(RowBatch* src);

    // Deep copy all rows this row batch into dst, using memory allocated from
    // dst's _tuple_data_pool. Only valid when dst is empty.
    // TODO: the current implementation of deep copy can produce an oversized
    // row batch if there are duplicate tuples in this row batch.
    void deep_copy_to(RowBatch* dst);

    // Create a serialized version of this row batch in output_batch, attaching all of the
    // data it references to output_batch.tuple_data. output_batch.tuple_data will be
    // snappy-compressed unless the compressed data is larger than the uncompressed
    // data. Use output_batch.is_compressed to determine whether tuple_data is compressed.
    // If an in-flight row is present in this row batch, it is ignored.
    // This function does not reset().
    // Returns the uncompressed serialized size (this will be the true size of output_batch
    // if tuple_data is actually uncompressed).
    Status serialize(PRowBatch* output_batch, size_t* uncompressed_size, size_t* compressed_size,
                     bool allow_transfer_large_data = false);

    // Utility function: returns total size of batch.
    static size_t get_batch_size(const PRowBatch& batch);

    vectorized::Block convert_to_vec_block() const;

    int num_rows() const { return _num_rows; }
    int capacity() const { return _capacity; }

    int num_buffers() const { return _buffers.size(); }

    const RowDescriptor& row_desc() const { return _row_desc; }

    // Max memory that this row batch can accumulate in _tuple_data_pool before it
    // is considered at capacity.
    /// This is a soft capacity: row batches may exceed the capacity, preferably only by a
    /// row's worth of data.
    static const int AT_CAPACITY_MEM_USAGE;

    // Max memory out of AT_CAPACITY_MEM_USAGE that should be used for fixed-length data,
    // in order to leave room for variable-length data.
    static const int FIXED_LEN_BUFFER_LIMIT;

    /// Allocates a buffer large enough for the fixed-length portion of 'capacity_' rows in
    /// this batch from 'tuple_data_pool_'. 'capacity_' is reduced if the allocation would
    /// exceed FIXED_LEN_BUFFER_LIMIT. Always returns enough space for at least one row.
    /// Returns Status::MemoryLimitExceeded("Memory limit exceeded") and sets 'buffer' to nullptr if a memory limit would
    /// have been exceeded. 'state' is used to log the error.
    /// On success, sets 'buffer_size' to the size in bytes and 'buffer' to the buffer.
    Status resize_and_allocate_tuple_buffer(RuntimeState* state, int64_t* buffer_size,
                                            uint8_t** buffer);

    void set_scanner_id(int id) { _scanner_id = id; }
    int scanner_id() const { return _scanner_id; }

    static const int MAX_MEM_POOL_SIZE = 32 * 1024 * 1024;
    std::string to_string();

private:
    MemTracker* _mem_tracker; // not owned

    // Close owned tuple streams and delete if needed.
    void close_tuple_streams();

    // All members need to be handled in RowBatch::swap()

    bool _has_in_flight_row;   // if true, last row hasn't been committed yet
    int _num_rows;             // # of committed rows
    int _num_uncommitted_rows; // # of uncommited rows in row batch mem pool
    int _capacity;             // maximum # of rows

    /// If FLUSH_RESOURCES, the resources attached to this batch should be freed or
    /// acquired by a new owner as soon as possible. See MarkFlushResources(). If
    /// FLUSH_RESOURCES, AtCapacity() is also true.
    FlushMode _flush;

    /// If true, this batch references unowned memory that will be cleaned up soon.
    /// See MarkNeedsDeepCopy(). If true, 'flush_' is FLUSH_RESOURCES and
    /// AtCapacity() is true.
    bool _needs_deep_copy;

    int _num_tuples_per_row;
    RowDescriptor _row_desc;

    // Array of pointers with _capacity * _num_tuples_per_row elements.
    // The memory ownership depends on whether legacy joins and aggs are enabled.
    //
    // Memory is malloc'd and owned by RowBatch:
    // If enable_partitioned_hash_join=true and enable_partitioned_aggregation=true
    // then the memory is owned by this RowBatch and is freed upon its destruction.
    // This mode is more performant especially with SubplanNodes in the ExecNode tree
    // because the tuple pointers are not transferred and do not have to be re-created
    // in every Reset().
    //
    // Memory is allocated from MemPool:
    // Otherwise, the memory is allocated from _tuple_data_pool. As a result, the
    // pointer memory is transferred just like tuple data, and must be re-created
    // in Reset(). This mode is required for the legacy join and agg which rely on
    // the tuple pointers being allocated from the _tuple_data_pool, so they can
    // acquire ownership of the tuple pointers.
    Tuple** _tuple_ptrs;
    int _tuple_ptrs_size;

    // Sum of all auxiliary bytes. This includes IoBuffers and memory from
    // TransferResourceOwnership().
    int64_t _auxiliary_mem_usage;

    // If true, this batch is considered at capacity. This is explicitly set by streaming
    // components that return rows via row batches.
    bool _need_to_return;

    // holding (some of the) data referenced by rows
    MemPool _tuple_data_pool;

    // holding some complex agg object data (bitmap, hll)
    ObjectPool _agg_object_pool;

    // IO buffers current owned by this row batch. Ownership of IO buffers transfer
    // between row batches. Any IO buffer will be owned by at most one row batch
    // (i.e. they are not ref counted) so most row batches don't own any.
    std::vector<DiskIoMgr::BufferDescriptor*> _io_buffers;

    struct BufferInfo {
        BufferPool::ClientHandle* client;
        BufferPool::BufferHandle buffer;
    };
    /// Pages attached to this row batch. See AddBuffer() for ownership semantics.
    std::vector<BufferInfo> _buffers;
    // Tuple streams currently owned by this row batch.
    std::vector<BufferedTupleStream2*> _tuple_streams;

    // Blocks attached to this row batch. The underlying memory and block manager client
    // are owned by the BufferedBlockMgr2.
    std::vector<BufferedBlockMgr2::Block*> _blocks;

    // String to write compressed tuple data to in serialize().
    // This is a string so we can swap() with the string in the PRowBatch we're serializing
    // to (we don't compress directly into the PRowBatch in case the compressed data is
    // longer than the uncompressed data). Swapping avoids copying data to the PRowBatch and
    // avoids excess memory allocations: since we reuse RowBatches and PRowBatchs, and
    // assuming all row batches are roughly the same size, all strings will eventually be
    // allocated to the right size.
    std::string _compression_scratch;

    int _scanner_id;
    bool _cleared = false;
};

/// Macros for iterating through '_row_batch', starting at '_start_row_idx'.
/// '_row_batch' is the row batch to iterate through.
/// '_start_row_idx' is the starting row index.
/// '_iter' is the iterator.
/// '_limit' is the max number of rows to iterate over.
#define FOREACH_ROW(_row_batch, _start_row_idx, _iter) \
    for (RowBatch::Iterator _iter(_row_batch, _start_row_idx); !_iter.at_end(); _iter.next())

#define FOREACH_ROW_LIMIT(_row_batch, _start_row_idx, _limit, _iter)                    \
    for (RowBatch::Iterator _iter(_row_batch, _start_row_idx, _limit); !_iter.at_end(); \
         _iter.next())

} // namespace doris

#endif
