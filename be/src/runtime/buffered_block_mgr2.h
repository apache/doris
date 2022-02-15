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

#ifndef DORIS_BE_SRC_RUNTIME_BUFFERED_BLOCK_MGR2_H
#define DORIS_BE_SRC_RUNTIME_BUFFERED_BLOCK_MGR2_H

#include <unordered_map>

#include "runtime/disk_io_mgr.h"
#include "runtime/tmp_file_mgr.h"

namespace doris {

class RuntimeState;

// The BufferedBlockMgr2 is used to allocate and manage blocks of data using a fixed memory
// budget. Available memory is split into a pool of fixed-size memory buffers. When a
// client allocates or requests a block, the block is assigned a buffer from this pool and
// is 'pinned' in memory. Clients can also unpin a block, allowing the manager to reassign
// its buffer to a different block.
//
// The BufferedBlockMgr2 typically allocates blocks in IO buffer size to get maximal IO
// efficiency when spilling. Clients can also request smaller buffers that cannot spill
// (note that it would be possible to spill small buffers, but we currently do not allow
// it). This is useful to present the same block API and mem tracking for clients (one can
// use the block mgr API to mem track non-spillable (smaller) buffers). Clients that do
// partitioning (e.g. PHJ and PAGG) will start with these smaller buffer sizes to reduce
// the minimum buffering requirements and grow to max sized buffers as the input grows.
// For simplicity, these small buffers are not recycled (there's also not really a need
// since they are allocated all at once on query startup). These buffers are not counted
// against the reservation.
//
// The BufferedBlockMgr2 reserves one buffer per disk ('_block_write_threshold') for
// itself. When the number of free buffers falls below 'block_write_threshold', unpinned
// blocks are flushed in Last-In-First-Out order. (It is assumed that unpinned blocks are
// re-read in FIFO order). The TmpFileMgr is used to obtain file handles to write to
// within the tmp directories configured for Impala.
//
// It is expected to have one BufferedBlockMgr2 per query. All allocations that can grow
// proportional to the input size and that might need to spill to disk should allocate
// from the same BufferedBlockMgr2.
//
// A client must pin a block in memory to read/write its contents and unpin it when it is
// no longer in active use. The BufferedBlockMgr2 guarantees that:
//  a) The memory buffer assigned to a block is not removed or released while it is pinned.
//  b) The contents of an unpinned block will be available on a subsequent call to pin.
//
// The Client supports the following operations:
//  get_new_block(): Returns a new pinned block.
//  Close(): Frees all memory and disk space. Called when a query is closed or cancelled.
//   Close() is idempotent.
//
// A Block supports the following operations:
//  pin(): Pins a block to a buffer in memory, and reads its contents from disk if
//   necessary. If there are no free buffers, waits for a buffer to become available.
//   Invoked before the contents of a block are read or written. The block
//   will be maintained in memory until unpin() is called.
//  unpin(): Invoked to indicate the block is not in active use. The block is added to a
//   list of unpinned blocks. Unpinned blocks are only written when the number of free
//   blocks falls below the 'block_write_threshold'.
//  del(): Invoked to deallocate a block. The buffer associated with the block is
//   immediately released and its on-disk location (if any) reused.
//
// The block manager is thread-safe with the following caveat: A single block cannot be
// used simultaneously by multiple clients in any capacity.
// However, the block manager client is not thread-safe. That is, the block manager
// allows multiple single-threaded block manager clients.
//
/// TODO: When a block is read from disk, data is copied from the IOMgr buffer to the
/// block manager's buffer. This should be avoided in the common case where these buffers
/// are of the same size.
/// TODO: See if the one big lock is a bottleneck. Break it up. This object is shared by
/// all operators within a query (across fragments), see IMPALA-1883.
/// TODO: No reason we can't spill the smaller buffers. Add it if we need to (it's likely
/// just removing dchecks).
/// TODO: The requirements on this object has grown organically. Consider a major
/// reworking.
class BufferedBlockMgr2 {
private:
    struct BufferDescriptor;

public:
    // A client of the BufferedBlockMgr2. There is a single BufferedBlockMgr2 per plan
    // fragment and all operators that need blocks from it should use a separate client.
    // Each client has the option to reserve a number of blocks that it can claim later.
    // The remaining memory that is not reserved by any clients is free for all and
    // available to all clients.
    // This is an opaque handle.
    // TODO: move the APIs to client we don't need to pass the BufferedBlockMgr2 around.
    // TODO: how can we ensure that each operator uses a separate client?
    class Client;

    // A fixed-size block of data that may be be persisted to disk. The state of the block
    // is maintained by the block manager and is described by 3 bools:
    // _is_pinned = True if the block is pinned. The block has a non-null _buffer_desc,
    //   _buffer_desc cannot be in the free buffer list and the block cannot be in
    //   _unused_blocks or _unpinned_blocks. Newly allocated blocks are pinned.
    // _in_write = True if a write has been issued but not completed for this block.
    //   The block cannot be in the _unpinned_blocks and must have a non-null _buffer_desc
    //   that's not in the free buffer list. It may be pinned or unpinned.
    // _is_deleted = True if del() has been called on a block. After this, no API call
    //   is valid on the block.
    //
    // pin() and unpin() can be invoked on a block any number of times before del().
    // When a pinned block is unpinned for the first time, it is added to the
    // _unpinned_blocks list and its buffer is removed from the free list.
    // If it is pinned or deleted at any time while it is on the unpinned list, it is
    // simply removed from that list. When it is dequeued from that list and enqueued
    // for writing, _in_write is set to true. The block may be pinned, unpinned or deleted
    // while _in_write is true. After the write has completed, the block's buffer will be
    // returned to the free buffer list if it is no longer pinned, and the block itself
    // will be put on the unused blocks list if del() was called.
    //
    // A block MUST have a non-null _buffer_desc if
    //  a) _is_pinned is true (i.e. the client is using it), or
    //  b) _in_write is true, (i.e. IO mgr is using it), or
    //  c) It is on the unpinned list (buffer has not been persisted.)
    //
    // In addition to the block manager API, Block exposes allocate(), return_allocation()
    // and bytes_remaining() to allocate and free memory within a block, and buffer() and
    // valid_data_len() to read/write the contents of a block. These are not thread-safe.
    class Block : public InternalQueue<Block>::Node {
    public:
        // A null dtor to pass codestyle check
        ~Block() {}

        // Pins a block in memory--assigns a free buffer to a block and reads it from disk if
        // necessary. If there are no free blocks and no unpinned blocks, '*pinned' is set to
        // false and the block is not pinned. If 'release_block' is non-nullptr, if there is
        // memory pressure, this block will be pinned using the buffer from 'release_block'.
        // If 'unpin' is true, 'release_block' will be unpinned (regardless of whether or not
        // the buffer was used for this block). If 'unpin' is false, 'release_block' is
        // deleted. 'release_block' must be pinned.
        Status pin(bool* pinned, Block* release_block = nullptr, bool unpin = true);

        // Unpins a block by adding it to the list of unpinned blocks maintained by the block
        // manager. An unpinned block must be flushed before its buffer is released or
        // assigned to a different block. Is non-blocking.
        Status unpin();

        // Delete a block. Its buffer is released and on-disk location can be over-written.
        // Non-blocking.
        void del();

        void add_row() { ++_num_rows; }
        int num_rows() const { return _num_rows; }

        // Allocates the specified number of bytes from this block.
        template <typename T>
        T* allocate(int size) {
            DCHECK_GE(bytes_remaining(), size);
            uint8_t* current_location = _buffer_desc->buffer + _valid_data_len;
            _valid_data_len += size;
            return reinterpret_cast<T*>(current_location);
        }

        // Return the number of remaining bytes that can be allocated in this block.
        int bytes_remaining() const {
            DCHECK(_buffer_desc != nullptr);
            return _buffer_desc->len - _valid_data_len;
        }

        // Return size bytes from the most recent allocation.
        void return_allocation(int size) {
            DCHECK_GE(_valid_data_len, size);
            _valid_data_len -= size;
        }

        // Pointer to start of the block data in memory. Only guaranteed to be valid if the
        // block is pinned.
        uint8_t* buffer() const {
            DCHECK(_buffer_desc != nullptr);
            return _buffer_desc->buffer;
        }

        // Return the number of bytes allocated in this block.
        int64_t valid_data_len() const { return _valid_data_len; }

        // Returns the length of the underlying buffer. Only callable if the block is
        // pinned.
        int64_t buffer_len() const {
            DCHECK(is_pinned());
            return _buffer_desc->len;
        }

        // Returns true if this block is the max block size. Only callable if the block
        // is pinned.
        bool is_max_size() const {
            DCHECK(is_pinned());
            return _buffer_desc->len == _block_mgr->max_block_size();
        }

        bool is_pinned() const { return _is_pinned; }

        // Path of temporary file backing the block. Intended for use in testing.
        // Returns empty string if no backing file allocated.
        std::string tmp_file_path() const;

        // Debug helper method to print the state of a block.
        std::string debug_string() const;

    private:
        friend class BufferedBlockMgr2;

        Block(BufferedBlockMgr2* block_mgr);

        // Initialize the state of a block and set the number of bytes allocated to 0.
        void init();

        // Debug helper method to validate the state of a block. _block_mgr lock must already
        // be taken.
        bool validate() const;

        // Pointer to the buffer associated with the block. nullptr if the block is not in
        // memory and cannot be changed while the block is pinned or being written.
        BufferDescriptor* _buffer_desc;

        // Parent block manager object. Responsible for maintaining the state of the block.
        BufferedBlockMgr2* _block_mgr;

        // The client that owns this block.
        Client* _client;

        // WriteRange object representing the on-disk location used to persist a block.
        // Is created the first time a block is persisted, and retained until the block
        // object is destroyed. The file location and offset in _write_range are valid
        // throughout the lifetime of this object, but the data and length in the
        // _write_range are only valid while the block is being written.
        // _write_range instance is owned by the block manager.
        DiskIoMgr::WriteRange* _write_range;

        // The file this block belongs to. The lifetime is the same as the file location
        // and offset in _write_range. The File is owned by BufferedBlockMgr2, not TmpFileMgr.
        TmpFileMgr::File* _tmp_file;

        // Length of valid (i.e. allocated) data within the block.
        int64_t _valid_data_len;

        // Number of rows in this block.
        int _num_rows;

        // Block state variables. The block's buffer can be freed only if _is_pinned and
        // _in_write are both false.
        // TODO: this might be better expressed as an enum.

        // _is_pinned is true while the block is pinned by a client.
        bool _is_pinned;

        // _in_write is set to true when the block is enqueued for writing via DiskIoMgr,
        // and set to false when the write is complete.
        bool _in_write;

        // True if the block is deleted by the client.
        bool _is_deleted;

        // Condition variable for when there is a specific client waiting for this block.
        // Only used if _client_local is true.
        // TODO: Currently we use _block_mgr->_lock for this condvar. There is no reason to
        // use that _lock that is already overloaded, see IMPALA-1883.
        std::condition_variable _write_complete_cv;

        // If true, this block is being written out so the underlying buffer can be
        // transferred to another block from the same client. We don't want this buffer
        // getting picked up by another client.
        bool _client_local;
    }; // class Block

    // Create a block manager with the specified mem_limit. If a block mgr with the
    // same query id has already been created, that block mgr is returned.
    // - mem_limit: maximum memory that will be used by the block mgr.
    // - buffer_size: maximum size of each buffer.
    static Status create(RuntimeState* state, const std::shared_ptr<MemTracker>& parent,
                         RuntimeProfile* profile, TmpFileMgr* tmp_file_mgr, int64_t mem_limit,
                         int64_t buffer_size, std::shared_ptr<BufferedBlockMgr2>* block_mgr);

    ~BufferedBlockMgr2();

    // Registers a client with num_reserved_buffers. The returned client is owned
    // by the BufferedBlockMgr2 and has the same lifetime as it.
    // We allow oversubscribing the reserved buffers. It is likely that the
    // num_reserved_buffers be very pessimistic for small queries and we don't want to
    // fail all of them with mem limit exceeded.
    // The min reserved buffers is often independent of data size and we still want
    // to run small queries with very small limits.
    // Buffers used by this client are reflected in tracker.
    // TODO: The fact that we allow oversubscription is problematic.
    // as the code expects the reservations to always be granted (currently not the case).
    Status register_client(int num_reserved_buffers, const std::shared_ptr<MemTracker>& tracker,
                           RuntimeState* state, Client** client);

    // Clears all reservations for this client.
    void clear_reservations(Client* client);

    // Tries to acquire a one-time reservation of num_buffers. The semantics are:
    //  - If this call fails, the next 'num_buffers' calls to pin()/get_new_block() might
    //    not have enough memory.
    //  - If this call succeeds, the next 'num_buffers' call to pin()/get_new_block() will
    //    be guaranteed to get the block. Once these blocks have been pinned, the
    //    reservation from this call has no more effect.
    // Blocks coming from the tmp reservation also count towards the regular reservation.
    // This is useful to pin() a number of blocks and guarantee all or nothing behavior.
    bool try_acquire_tmp_reservation(Client* client, int num_buffers);

    // Return a new pinned block. If there is no memory for this block, *block will be set
    // to nullptr.
    // If len > 0, get_new_block() will return a block with a buffer of size len. len
    // must be less than max_block_size and this block cannot be unpinned.
    // This function will try to allocate new memory for the block up to the limit.
    // Otherwise it will (conceptually) write out an unpinned block and use that memory.
    // The caller can pass a non-nullptr 'unpin_block' to transfer memory from 'unpin_block'
    // to the new block. If 'unpin_block' is non-nullptr, the new block can never fail to
    // get a buffer. The semantics of this are:
    //   - If 'unpin_block' is non-nullptr, it must be pinned.
    //   - If the call succeeds, 'unpin_block' is unpinned.
    //   - If there is no memory pressure, block will get a newly allocated buffer.
    //   - If there is memory pressure, block will get the buffer from 'unpin_block'.
    Status get_new_block(Client* client, Block* unpin_block, Block** block, int64_t len = -1);

    // Cancels the block mgr. All subsequent calls that return a Status fail with
    // Status::Cancelled("Cancelled"). Idempotent.
    void cancel();

    // Returns true if the block manager was cancelled.
    bool is_cancelled();

    // Dumps block mgr state. Grabs lock. If client is not nullptr, also dumps its state.
    std::string debug_string(Client* client = nullptr);

    // Consumes 'size' bytes from the buffered block mgr. This is used by callers that want
    // the memory to come from the block mgr pool (and therefore trigger spilling) but need
    // the allocation to be more flexible than blocks. Buffer space reserved with
    // try_acquire_tmp_reservation() may be used to fulfill the request if available. If the
    // request is unsuccessful, that temporary buffer space is not consumed.
    // Returns false if there was not enough memory.
    // TODO: this is added specifically to support the Buckets structure in the hash table
    // which does not map well to Blocks. Revisit this.
    bool consume_memory(Client* client, int64_t size);

    // All successful allocates bytes from consume_memory() must have a corresponding
    // release_memory() call.
    void release_memory(Client* client, int64_t size);

    // The number of buffers available for client. That is, if all other clients were
    // stopped, the number of buffers this client could get.
    int64_t available_buffers(Client* client) const;

    // Returns a MEM_LIMIT_EXCEEDED error which includes the minimum memory required by
    // this 'client' that acts on behalf of the node with id 'node_id'. 'node_id' is used
    // only for error reporting.
    Status mem_limit_too_low_error(Client* client, int node_id);

    // TODO: Remove these two. Not clear what the sorter really needs.
    // TODO: Those are dirty, dangerous reads to two lists whose all other accesses are
    // protected by the _lock. Using those two functions is looking for trouble.
    int available_allocated_buffers() const { return _all_io_buffers.size(); }
    int num_free_buffers() const { return _free_io_buffers.size(); }

    int num_pinned_buffers(Client* client) const;
    int num_reserved_buffers_remaining(Client* client) const;
    std::shared_ptr<MemTracker> get_tracker(Client* client) const;
    int64_t max_block_size() const {
        { return _max_block_size; }
    }
    int64_t bytes_allocated() const;
    RuntimeProfile* profile() {
        { return _profile.get(); }
    }
    int writes_issued() const {
        { return _writes_issued; }
    }

private:
    friend class Client;

    // Descriptor for a single memory buffer in the pool.
    struct BufferDescriptor : public InternalQueue<BufferDescriptor>::Node {
        // Start of the buffer.
        uint8_t* buffer;

        // Length of the buffer.
        int64_t len;

        // Block that this buffer is assigned to. May be nullptr.
        Block* block;

        // Iterator into _all_io_buffers for this buffer.
        std::list<BufferDescriptor*>::iterator all_buffers_it;

        BufferDescriptor(uint8_t* buf, int64_t len) : buffer(buf), len(len), block(nullptr) {}
    };

    BufferedBlockMgr2(RuntimeState* state, TmpFileMgr* tmp_file_mgr, int64_t block_size);

    // Initializes the block mgr. Idempotent and thread-safe.
    void init(DiskIoMgr* io_mgr, RuntimeProfile* profile,
              const std::shared_ptr<MemTracker>& parent_tracker, int64_t mem_limit);

    // Initializes _tmp_files. This is initialized the first time we need to write to disk.
    // Must be called with _lock taken.
    Status init_tmp_files();

    // pin_block(), unpin_block(), delete_block() perform the actual work of Block::pin(),
    // unpin() and del(). Must be called with the _lock taken.
    Status pin_block(Block* block, bool* pinned, Block* src, bool unpin);
    Status unpin_block(Block* block);
    void delete_block(Block* block);

    // If the 'block' is nullptr, checks if cancelled and returns. Otherwise, depending on
    // 'unpin' calls either  delete_block() or unpin_block(), which both first check for
    // cancellation. It should be called without the _lock acquired.
    Status delete_or_unpin_block(Block* block, bool unpin);

    // Transfers the buffer from 'src' to 'dst'. 'src' must be pinned.
    // If unpin == false, 'src' is simply deleted.
    // If unpin == true, 'src' is unpinned and it may block until the write of 'src' is
    // completed. In that case it will use the _lock for the condvar. Thus, the _lock
    // needs to not have been taken when this function is called.
    Status transfer_buffer(Block* dst, Block* src, bool unpin);

    // Returns the total number of unreserved buffers. This is the sum of unpinned,
    // free and buffers we can still allocate minus the total number of reserved buffers
    // that are not pinned.
    // Note this can be negative if the buffers are oversubscribed.
    // Must be called with _lock taken.
    int64_t remaining_unreserved_buffers() const;

    // Finds a buffer for a block and pins it. If the block's buffer has not been evicted,
    // it removes the block from the unpinned list and sets *in_mem = true.
    // If the block is not in memory, it will call find_buffer() that may block.
    // If we can't get a buffer (e.g. no more memory, nothing in the unpinned and free
    // lists) this function returns with the block unpinned.
    // Uses the _lock, the caller should not have already acquired the _lock.
    Status find_buffer_for_block(Block* block, bool* in_mem);

    // Returns a new buffer that can be used. *buffer is set to nullptr if there was no
    // memory.
    // Otherwise, this function gets a new buffer by:
    //   1. Allocating a new buffer if possible
    //   2. Using a buffer from the free list (which is populated by moving blocks from
    //      the unpinned list by writing them out).
    // Must be called with the _lock already taken. This function can block.
    Status find_buffer(std::unique_lock<std::mutex>& lock, BufferDescriptor** buffer);

    // Writes unpinned blocks via DiskIoMgr until one of the following is true:
    //   1. The number of outstanding writes >= (_block_write_threshold - num free buffers)
    //   2. There are no more unpinned blocks
    // Must be called with the _lock already taken. Is not blocking.
    Status write_unpinned_blocks();

    // Issues the write for this block to the DiskIoMgr.
    Status write_unpinned_block(Block* block);

    // Allocate block_size bytes in a temporary file. Try multiple disks if error occurs.
    // Returns an error only if no temporary files are usable.
    Status allocate_scratch_space(int64_t block_size, TmpFileMgr::File** tmp_file,
                                  int64_t* file_offset);

    // Callback used by DiskIoMgr to indicate a block write has completed.  write_status
    // is the status of the write. _is_cancelled is set to true if write_status is not
    // Status::OK() or a re-issue of the write fails. Returns the block's buffer to the
    // free buffers list if it is no longer pinned. Returns the block itself to the free
    // blocks list if it has been deleted.
    void write_complete(Block* block, const Status& write_status);

    // Returns a deleted block to the list of free blocks. Assumes the block's buffer has
    // already been returned to the free buffers list. Non-blocking.
    // Thread-safe and does not need the _lock acquired.
    void return_unused_block(Block* block);

    // Checks _unused_blocks for an unused block object, else allocates a new one.
    // Non-blocking and needs no _lock.
    Block* get_unused_block(Client* client);

    // Used to debug the state of the block manager. Lock must already be taken.
    bool validate() const;
    std::string debug_internal() const;

    // Add BE hostname and fragmentid for debug tuning
    Status add_exec_msg(const std::string& msg) const;

    // Size of the largest/default block in bytes.
    const int64_t _max_block_size;

    // Unpinned blocks are written when the number of free buffers is below this threshold.
    // Equal to the number of disks.
    const int _block_write_threshold;

    // If false, spilling is disabled. The client calls will fail if there is not enough
    // memory.
    const bool _enable_spill;

    const TUniqueId _query_id;

    ObjectPool _obj_pool;

    // Track buffers allocated by the block manager.
    std::shared_ptr<MemTracker> _mem_tracker;

    // The temporary file manager used to allocate temporary file space.
    TmpFileMgr* _tmp_file_mgr;

    // This lock protects the block and buffer lists below, except for _unused_blocks.
    // It also protects the various counters and changes to block state. Additionally, it is
    // used for the blocking condvars: _buffer_available_cv and block->_write_complete_cv.
    // TODO: We should break the protection of the various structures and usages to
    //       different spinlocks and a mutex to be used in the wait()s, see IMPALA-1883.
    std::mutex _lock;

    // If true, init() has been called.
    bool _initialized;

    // The total number of reserved buffers across all clients that are not pinned.
    int _unfullfilled_reserved_buffers;

    // The total number of pinned buffers across all clients.
    int _total_pinned_buffers;

    // Number of outstanding writes (Writes issued but not completed).
    // This does not include client-local writes.
    int _non_local_outstanding_writes;

    // Signal availability of free buffers.
    std::condition_variable _buffer_available_cv;

    // List of blocks _is_pinned = false AND are not on DiskIoMgr's write queue.
    // Blocks are added to and removed from the back of the list. (i.e. in LIFO order).
    // Blocks in this list must have _is_pinned = false, _in_write = false,
    // _is_deleted = false.
    InternalQueue<Block> _unpinned_blocks;

    // List of blocks that have been deleted and are no longer in use.
    // Can be reused in get_new_block(). Blocks in this list must be in the Init'ed state,
    // i.e. _buffer_desc = nullptr, _is_pinned = false, _in_write = false,
    // _is_deleted = false, valid_data_len = 0.
    InternalQueue<Block> _unused_blocks;

    // List of buffers that can be assigned to a block in pin() or get_new_block().
    // These buffers either have no block associated with them or are associated with an
    // an unpinned block that has been persisted. That is, either block = nullptr or
    // (!block->_is_pinned  && !block->_in_write  && !_unpinned_blocks.Contains(block)).
    // All of these buffers are io sized.
    InternalQueue<BufferDescriptor> _free_io_buffers;

    // All allocated io-sized buffers.
    std::list<BufferDescriptor*> _all_io_buffers;

    // Temporary physical file handle, (one per tmp device) to which blocks may be written.
    // Blocks are round-robined across these files.
    std::vector<std::unique_ptr<TmpFileMgr::File>> _tmp_files;

    // Index into _tmp_files denoting the file to which the next block to be persisted will
    // be written.
    int _next_block_index;

    // DiskIoMgr handles to read and write blocks.
    DiskIoMgr* _io_mgr;
    DiskIoMgr::RequestContext* _io_request_context;

    // If true, a disk write failed and all API calls return.
    // Status::Cancelled("Cancelled"). Set to true if there was an error writing a block, or if
    // write_complete() needed to reissue the write and that failed.
    bool _is_cancelled;

    // Counters and timers to track behavior.
    std::unique_ptr<RuntimeProfile> _profile;

    RuntimeProfile::Counter* _block_size_counter;

    // Total number of blocks created.
    RuntimeProfile::Counter* _created_block_counter;

    // Number of deleted blocks reused.
    RuntimeProfile::Counter* _recycled_blocks_counter;

    // Number of pin() calls that did not require a disk read.
    RuntimeProfile::Counter* _buffered_pin_counter;

    // Time taken for disk reads.
    RuntimeProfile::Counter* _disk_read_timer;

    // Time spent waiting for a free buffer.
    RuntimeProfile::Counter* _buffer_wait_timer;

    // Number of bytes written to disk (includes writes still queued in the IO manager).
    RuntimeProfile::Counter* _bytes_written_counter;

    // Number of writes outstanding (issued but not completed).
    RuntimeProfile::Counter* _outstanding_writes_counter;

    // Time spent in disk spill encryption and decryption.
    RuntimeProfile::Counter* _encryption_timer;

    // Time spent in disk spill integrity generation and checking.
    RuntimeProfile::Counter* _integrity_check_timer;

    // Number of writes issued.
    int _writes_issued;

    // Protects _s_query_to_block_mgrs.
    static SpinLock _s_block_mgrs_lock;

    // All per-query BufferedBlockMgr2 objects that are in use.  For memory management, this
    // map contains only weak ptrs. BufferedBlockMgr2s that are handed out are shared ptrs.
    // When all the shared ptrs are no longer referenced, the BufferedBlockMgr2
    // d'tor will be called at which point the weak ptr will be removed from the map.
    typedef std::unordered_map<TUniqueId, std::weak_ptr<BufferedBlockMgr2>> BlockMgrsMap;
    static BlockMgrsMap _s_query_to_block_mgrs;

    // Unowned.
    RuntimeState* _state;

}; // class BufferedBlockMgr2

} // end namespace doris

#endif // DORIS_BE_SRC_RUNTIME_BUFFERED_BLOCK_MGR2_H
