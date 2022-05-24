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

#include "runtime/buffered_block_mgr2.h"

#include "exec/exec_node.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/tmp_file_mgr.h"
#include "util/bit_util.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/doris_metrics.h"
#include "util/filesystem_util.h"
#include "util/pretty_printer.h"
#include "util/runtime_profile.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

using std::string;
using std::stringstream;
using std::vector;
using std::list;
using std::endl;

using std::bind;
using std::mem_fn;
using std::lock_guard;
using std::mutex;
using std::shared_ptr;
using std::unique_lock;

namespace doris {

BufferedBlockMgr2::BlockMgrsMap BufferedBlockMgr2::_s_query_to_block_mgrs;
SpinLock BufferedBlockMgr2::_s_block_mgrs_lock;

class BufferedBlockMgr2::Client {
public:
    Client(BufferedBlockMgr2* mgr, int num_reserved_buffers,
           const std::shared_ptr<MemTracker>& tracker, RuntimeState* state)
            : _mgr(mgr),
              _state(state),
              _tracker(tracker),
              _query_tracker(_mgr->_mem_tracker->parent()),
              _num_reserved_buffers(num_reserved_buffers),
              _num_tmp_reserved_buffers(0),
              _num_pinned_buffers(0) {
        DCHECK(tracker != nullptr);
    }

    // A null dtor to pass codestyle check
    ~Client() {}

    // Unowned.
    BufferedBlockMgr2* _mgr;

    // Unowned.
    RuntimeState* _state;

    // Tracker for this client. Unowned.
    // When the client gets a buffer, we update the consumption on this tracker. However,
    // we don't want to transfer the buffer from the block mgr to the client (i.e. release
    // from the block mgr), since the block mgr is where the block mem usage limit is
    // enforced. Even when we give a buffer to a client, the buffer is still owned and
    // counts against the block mgr tracker (i.e. there is a fixed pool of buffers
    // regardless of if they are in the block mgr or the clients).
    std::shared_ptr<MemTracker> _tracker;

    // This is the common ancestor between the block mgr tracker and the client tracker.
    // When memory is transferred to the client, we want it to stop at this tracker.
    std::shared_ptr<MemTracker> _query_tracker;

    // Number of buffers reserved by this client.
    int _num_reserved_buffers;

    // Number of buffers temporarily reserved.
    int _num_tmp_reserved_buffers;

    // Number of buffers pinned by this client.
    int _num_pinned_buffers;

    void pin_buffer(BufferDescriptor* buffer) {
        DCHECK(buffer != nullptr);
        if (buffer->len == _mgr->max_block_size()) {
            ++_num_pinned_buffers;
            _tracker->ConsumeLocal(buffer->len, _query_tracker.get());
            // _tracker->Consume(buffer->len);
        }
    }

    void unpin_buffer(BufferDescriptor* buffer) {
        DCHECK(buffer != nullptr);
        if (buffer->len == _mgr->max_block_size()) {
            DCHECK_GT(_num_pinned_buffers, 0);
            --_num_pinned_buffers;
            _tracker->ReleaseLocal(buffer->len, _query_tracker.get());
            // _tracker->Release(buffer->len);
        }
    }

    string debug_string() const {
        stringstream ss;
        ss << "Client " << this << endl
           << "  num_reserved_buffers=" << _num_reserved_buffers << endl
           << "  num_tmp_reserved_buffers=" << _num_tmp_reserved_buffers << endl
           << "  num_pinned_buffers=" << _num_pinned_buffers;
        return ss.str();
    }
};

// BufferedBlockMgr2::Block methods.
BufferedBlockMgr2::Block::Block(BufferedBlockMgr2* block_mgr)
        : _buffer_desc(nullptr),
          _block_mgr(block_mgr),
          _client(nullptr),
          _write_range(nullptr),
          _tmp_file(nullptr),
          _valid_data_len(0),
          _num_rows(0) {}

Status BufferedBlockMgr2::Block::pin(bool* pinned, Block* release_block, bool unpin) {
    return _block_mgr->pin_block(this, pinned, release_block, unpin);
}

Status BufferedBlockMgr2::Block::unpin() {
    return _block_mgr->unpin_block(this);
}

void BufferedBlockMgr2::Block::del() {
    _block_mgr->delete_block(this);
}

void BufferedBlockMgr2::Block::init() {
    // No locks are taken because the block is new or has previously been deleted.
    _is_pinned = false;
    _in_write = false;
    _is_deleted = false;
    _valid_data_len = 0;
    _client = nullptr;
    _num_rows = 0;
}

bool BufferedBlockMgr2::Block::validate() const {
    if (_is_deleted && (_is_pinned || (!_in_write && _buffer_desc != nullptr))) {
        LOG(ERROR) << "Deleted block in use - " << debug_string();
        return false;
    }

    if (_buffer_desc == nullptr && (_is_pinned || _in_write)) {
        LOG(ERROR) << "Block without buffer in use - " << debug_string();
        return false;
    }

    if (_buffer_desc == nullptr && _block_mgr->_unpinned_blocks.contains(this)) {
        LOG(ERROR) << "Unpersisted block without buffer - " << debug_string();
        return false;
    }

    if (_buffer_desc != nullptr && (_buffer_desc->block != this)) {
        LOG(ERROR) << "Block buffer inconsistency - " << debug_string();
        return false;
    }

    return true;
}

string BufferedBlockMgr2::Block::tmp_file_path() const {
    if (_tmp_file == nullptr) {
        return "";
    }
    return _tmp_file->path();
}

string BufferedBlockMgr2::Block::debug_string() const {
    stringstream ss;
    ss << "Block: " << this << endl
       << "  Buffer Desc: " << _buffer_desc << endl
       << "  Data Len: " << _valid_data_len << endl
       << "  Num Rows: " << _num_rows << endl;
    if (_is_pinned) {
        ss << "  Buffer Len: " << buffer_len() << endl;
    }
    ss << "  Deleted: " << _is_deleted << endl
       << "  Pinned: " << _is_pinned << endl
       << "  Write Issued: " << _in_write << endl
       << "  Client Local: " << _client_local;
    return ss.str();
}

BufferedBlockMgr2::BufferedBlockMgr2(RuntimeState* state, TmpFileMgr* tmp_file_mgr,
                                     int64_t block_size)
        : _max_block_size(block_size),
          // Keep two writes in flight per scratch disk so the disks can stay busy.
          _block_write_threshold(tmp_file_mgr->num_active_tmp_devices() * 2),
          _enable_spill(state->enable_spill()),
          _query_id(state->query_id()),
          _tmp_file_mgr(tmp_file_mgr),
          _initialized(false),
          _unfullfilled_reserved_buffers(0),
          _total_pinned_buffers(0),
          _non_local_outstanding_writes(0),
          _io_mgr(state->exec_env()->disk_io_mgr()),
          _is_cancelled(false),
          _writes_issued(0),
          _state(state) {}

Status BufferedBlockMgr2::create(RuntimeState* state, const std::shared_ptr<MemTracker>& parent,
                                 RuntimeProfile* profile, TmpFileMgr* tmp_file_mgr,
                                 int64_t mem_limit, int64_t block_size,
                                 std::shared_ptr<BufferedBlockMgr2>* block_mgr) {
    DCHECK(parent != nullptr);
    block_mgr->reset();
    {
        // we do not use global BlockMgrsMap for now, to avoid mem-exceeded different fragments
        // running on the same machine.
        // TODO(lingbin): open it later. note that open with query-mem-limit in RuntimeState
        // at the same time.

        // lock_guard<SpinLock> lock(_s_block_mgrs_lock);
        // BlockMgrsMap::iterator it = _s_query_to_block_mgrs.find(state->query_id());
        // if (it != _s_query_to_block_mgrs.end()){
        //     *block_mgr = it->second.lock();
        // }
        if (*block_mgr == nullptr) {
            // weak_ptr::lock returns nullptr if the weak_ptr is expired. This means
            // all shared_ptr references have gone to 0 and it is in the process of
            // being deleted. This can happen if the last shared reference is released
            // but before the weak ptr is removed from the map.
            block_mgr->reset(new BufferedBlockMgr2(state, tmp_file_mgr, block_size));
            // _s_query_to_block_mgrs[state->query_id()] = *block_mgr;
        }
    }
    (*block_mgr)->init(state->exec_env()->disk_io_mgr(), profile, parent, mem_limit);
    return Status::OK();
}

int64_t BufferedBlockMgr2::available_buffers(Client* client) const {
    int64_t unused_reserved = client->_num_reserved_buffers + client->_num_tmp_reserved_buffers -
                              client->_num_pinned_buffers;
    return std::max<int64_t>(0, remaining_unreserved_buffers()) +
           std::max<int64_t>(0, unused_reserved);
}

int64_t BufferedBlockMgr2::remaining_unreserved_buffers() const {
    int64_t num_buffers =
            _free_io_buffers.size() + _unpinned_blocks.size() + _non_local_outstanding_writes;
    num_buffers += _mem_tracker->SpareCapacity(MemLimit::HARD) / max_block_size();
    num_buffers -= _unfullfilled_reserved_buffers;
    return num_buffers;
}

Status BufferedBlockMgr2::register_client(int num_reserved_buffers,
                                          const std::shared_ptr<MemTracker>& tracker,
                                          RuntimeState* state, Client** client) {
    DCHECK_GE(num_reserved_buffers, 0);
    Client* a_client = new Client(this, num_reserved_buffers, tracker, state);
    lock_guard<mutex> lock(_lock);
    *client = _obj_pool.add(a_client);
    _unfullfilled_reserved_buffers += num_reserved_buffers;
    return Status::OK();
}

void BufferedBlockMgr2::clear_reservations(Client* client) {
    lock_guard<mutex> lock(_lock);
    // TODO: Can the modifications to the client's mem variables can be made w/o the lock?
    if (client->_num_pinned_buffers < client->_num_reserved_buffers) {
        _unfullfilled_reserved_buffers -=
                client->_num_reserved_buffers - client->_num_pinned_buffers;
    }
    client->_num_reserved_buffers = 0;

    _unfullfilled_reserved_buffers -= client->_num_tmp_reserved_buffers;
    client->_num_tmp_reserved_buffers = 0;
}

bool BufferedBlockMgr2::try_acquire_tmp_reservation(Client* client, int num_buffers) {
    lock_guard<mutex> lock(_lock);
    // TODO: Can the modifications to the client's mem variables can be made w/o the lock?
    DCHECK_EQ(client->_num_tmp_reserved_buffers, 0);
    if (client->_num_pinned_buffers < client->_num_reserved_buffers) {
        // If client has unused reserved buffers, we use those first.
        num_buffers -= client->_num_reserved_buffers - client->_num_pinned_buffers;
    }
    if (num_buffers < 0) {
        return true;
    }
    if (available_buffers(client) < num_buffers) {
        return false;
    }

    client->_num_tmp_reserved_buffers = num_buffers;
    _unfullfilled_reserved_buffers += num_buffers;
    return true;
}

bool BufferedBlockMgr2::consume_memory(Client* client, int64_t size) {
    // Later, we use this interface to manage the consumption of memory of hashtable instead of ReservationTracker.
    // So it is possible to allocate 0, which has no additional impact on the behavior of BufferedBlockMgr.
    // The process of memory allocation still by BufferPool, Because bufferpool has done a lot of optimization in memory allocation
    // which is better than using the new operator directly.
    if (size == 0) return true;
    // Workaround IMPALA-1619. Return immediately if the allocation size will cause
    // an arithmetic overflow.
    if (UNLIKELY(size >= (1LL << 31))) {
        LOG(WARNING) << "Trying to allocate memory >=2GB (" << size << ")B." << get_stack_trace();
        return false;
    }
    int buffers_needed = BitUtil::ceil(size, max_block_size());
    unique_lock<mutex> lock(_lock);
    if (size < max_block_size() && _mem_tracker->TryConsume(size)) {
        // For small allocations (less than a block size), just let the allocation through.
        client->_tracker->ConsumeLocal(size, client->_query_tracker.get());
        // client->_tracker->Consume(size);
        return true;
    }

    if (available_buffers(client) + client->_num_tmp_reserved_buffers < buffers_needed) {
        return false;
    }
    Status st = _mem_tracker->TryConsume(size);
    WARN_IF_ERROR(st, "consume failed");
    if (st) {
        // There was still unallocated memory, don't need to recycle allocated blocks.
        client->_tracker->ConsumeLocal(size, client->_query_tracker.get());
        // client->_tracker->Consume(size);
        return true;
    }

    // Bump up client->_num_tmp_reserved_buffers to satisfy this request. We don't want
    // another client to grab the buffer.
    int additional_tmp_reservations = 0;
    if (client->_num_tmp_reserved_buffers < buffers_needed) {
        additional_tmp_reservations = buffers_needed - client->_num_tmp_reserved_buffers;
        client->_num_tmp_reserved_buffers += additional_tmp_reservations;
        _unfullfilled_reserved_buffers += additional_tmp_reservations;
    }

    // Loop until we have freed enough memory.
    // We free all the memory at the end. We don't want another component to steal the
    // memory.
    int buffers_acquired = 0;
    do {
        BufferDescriptor* buffer_desc = nullptr;
        Status s = find_buffer(lock, &buffer_desc); // This waits on the lock.
        if (buffer_desc == nullptr) {
            break;
        }
        DCHECK(s.ok());
        _all_io_buffers.erase(buffer_desc->all_buffers_it);
        if (buffer_desc->block != nullptr) {
            buffer_desc->block->_buffer_desc = nullptr;
        }
        delete[] buffer_desc->buffer;
        ++buffers_acquired;
    } while (buffers_acquired != buffers_needed);

    Status status = Status::OK();
    if (buffers_acquired == buffers_needed) {
        status = write_unpinned_blocks();
    }
    // If we either couldn't acquire enough buffers or write_unpinned_blocks() failed, undo
    // the reservation.
    if (buffers_acquired != buffers_needed || !status.ok()) {
        if (!status.ok()) {
            VLOG_QUERY << "Query: " << _query_id << " write unpinned buffers failed.";
            client->_state->log_error(status);
        }
        client->_num_tmp_reserved_buffers -= additional_tmp_reservations;
        _unfullfilled_reserved_buffers -= additional_tmp_reservations;
        _mem_tracker->Release(buffers_acquired * max_block_size());
        return false;
    }

    client->_num_tmp_reserved_buffers -= buffers_acquired;
    _unfullfilled_reserved_buffers -= buffers_acquired;

    DCHECK_GE(buffers_acquired * max_block_size(), size);
    _mem_tracker->Release(buffers_acquired * max_block_size());
    st = _mem_tracker->TryConsume(size);
    WARN_IF_ERROR(st, "consume failed");
    if (!st) {
        return false;
    }
    client->_tracker->ConsumeLocal(size, client->_query_tracker.get());
    // client->_tracker->Consume(size);
    DCHECK(validate()) << endl << debug_internal();
    return true;
}

void BufferedBlockMgr2::release_memory(Client* client, int64_t size) {
    _mem_tracker->Release(size);
    client->_tracker->ReleaseLocal(size, client->_query_tracker.get());
}

void BufferedBlockMgr2::cancel() {
    {
        lock_guard<mutex> lock(_lock);
        if (_is_cancelled) {
            return;
        }
        _is_cancelled = true;
    }
    // Cancel the underlying io mgr to unblock any waiting threads.
    _io_mgr->cancel_context(_io_request_context);
}

bool BufferedBlockMgr2::is_cancelled() {
    lock_guard<mutex> lock(_lock);
    return _is_cancelled;
}

Status BufferedBlockMgr2::mem_limit_too_low_error(Client* client, int node_id) {
    VLOG_QUERY << "Query: " << _query_id << ". Node=" << node_id << " ran out of memory: " << endl
               << debug_internal() << endl
               << client->debug_string();

    // TODO: what to print here. We can't know the value of the entire query here.
    stringstream error_msg;
    error_msg << "The memory limit is set too low to initialize spilling operator (id=" << node_id
              << "). The minimum required memory to spill this operator is "
              << PrettyPrinter::print(client->_num_reserved_buffers * max_block_size(),
                                      TUnit::BYTES)
              << ".";
    return add_exec_msg(error_msg.str());
}

Status BufferedBlockMgr2::add_exec_msg(const std::string& msg) const {
    stringstream str;
    str << msg << " ";
    str << "Backend: " << BackendOptions::get_localhost() << ", ";
    str << "fragment: " << print_id(_state->fragment_instance_id()) << " ";
    return Status::MemoryLimitExceeded(str.str());
}

Status BufferedBlockMgr2::get_new_block(Client* client, Block* unpin_block, Block** block,
                                        int64_t len) {
    DCHECK_LE(len, _max_block_size) << "Cannot request block bigger than max_len";
    DCHECK_NE(len, 0) << "Cannot request block of zero size";
    *block = nullptr;
    Block* new_block = nullptr;

    {
        lock_guard<mutex> lock(_lock);
        if (_is_cancelled) {
            return Status::Cancelled("Cancelled");
        }
        new_block = get_unused_block(client);
        DCHECK(new_block->validate()) << endl << new_block->debug_string();
        DCHECK_EQ(new_block->_client, client);

        if (len > 0 && len < _max_block_size) {
            DCHECK(unpin_block == nullptr);
            Status st = client->_tracker->TryConsume(len);
            WARN_IF_ERROR(st, "get_new_block failed");
            if (st) {
                // TODO: Have a cache of unused blocks of size 'len' (0, _max_block_size)
                uint8_t* buffer = new uint8_t[len];
                // Descriptors for non-I/O sized buffers are deleted when the block is deleted.
                new_block->_buffer_desc = new BufferDescriptor(buffer, len);
                new_block->_buffer_desc->block = new_block;
                new_block->_is_pinned = true;
                client->pin_buffer(new_block->_buffer_desc);
                ++_total_pinned_buffers;
                *block = new_block;
            } else {
                new_block->_is_deleted = true;
                return_unused_block(new_block);
            }
            return Status::OK();
        }
    }

    bool in_mem = true;
    RETURN_IF_ERROR(find_buffer_for_block(new_block, &in_mem));
    DCHECK(!in_mem) << "A new block cannot start in mem.";
    DCHECK(!new_block->is_pinned() || new_block->_buffer_desc != nullptr)
            << new_block->debug_string();

    if (!new_block->is_pinned()) {
        if (unpin_block == nullptr) {
            // We couldn't get a new block and no unpin block was provided. Can't return
            // a block.
            new_block->_is_deleted = true;
            return_unused_block(new_block);
            new_block = nullptr;
        } else {
            // We need to transfer the buffer from unpin_block to new_block.
            RETURN_IF_ERROR(transfer_buffer(new_block, unpin_block, true));
        }
    } else if (unpin_block != nullptr) {
        // Got a new block without needing to transfer. Just unpin this block.
        RETURN_IF_ERROR(unpin_block->unpin());
    }

    DCHECK(new_block == nullptr || new_block->is_pinned());
    *block = new_block;
    return Status::OK();
}

Status BufferedBlockMgr2::transfer_buffer(Block* dst, Block* src, bool unpin) {
    Status status = Status::OK();
    DCHECK(dst != nullptr);
    DCHECK(src != nullptr);

    // First write out the src block.
    DCHECK(src->_is_pinned);
    DCHECK(!dst->_is_pinned);
    DCHECK(dst->_buffer_desc == nullptr);
    DCHECK_EQ(src->_buffer_desc->len, _max_block_size);
    src->_is_pinned = false;

    if (unpin) {
        unique_lock<mutex> lock(_lock);
        src->_client_local = true;
        status = write_unpinned_block(src);
        if (!status.ok()) {
            // The transfer failed, return the buffer to src.
            src->_is_pinned = true;
            return status;
        }
        // Wait for the write to complete.
        while (src->_in_write && !_is_cancelled) {
            src->_write_complete_cv.wait(lock);
        }
        if (_is_cancelled) {
            // We can't be sure the write succeeded, so return the buffer to src.
            src->_is_pinned = true;
            return Status::Cancelled("Cancelled");
        }
        DCHECK(!src->_in_write);
    }
    // Assign the buffer to the new block.
    dst->_buffer_desc = src->_buffer_desc;
    dst->_buffer_desc->block = dst;
    src->_buffer_desc = nullptr;
    dst->_is_pinned = true;
    if (!unpin) {
        src->_is_deleted = true;
        return_unused_block(src);
    }
    return Status::OK();
}

BufferedBlockMgr2::~BufferedBlockMgr2() {
    {
        lock_guard<SpinLock> lock(_s_block_mgrs_lock);
        BlockMgrsMap::iterator it = _s_query_to_block_mgrs.find(_query_id);
        // IMPALA-2286: Another fragment may have called create() for this _query_id and
        // saw that this BufferedBlockMgr2 is being destructed.  That fragement will
        // overwrite the map entry for _query_id, pointing it to a different
        // BufferedBlockMgr2 object.  We should let that object's destructor remove the
        // entry.  On the other hand, if the second BufferedBlockMgr2 is destructed before
        // this thread acquires the lock, then we'll remove the entry (because we can't
        // distinguish between the two expired pointers), and when the other
        // ~BufferedBlockMgr2() call occurs, it won't find an entry for this _query_id.
        if (it != _s_query_to_block_mgrs.end()) {
            std::shared_ptr<BufferedBlockMgr2> mgr = it->second.lock();
            if (mgr.get() == nullptr) {
                // The BufferBlockMgr object referenced by this entry is being deconstructed.
                _s_query_to_block_mgrs.erase(it);
            } else {
                // The map references another (still valid) BufferedBlockMgr2.
                DCHECK_NE(mgr.get(), this);
            }
        }
    }

    if (_io_request_context != nullptr) {
        _io_mgr->unregister_context(_io_request_context);
    }

    // If there are any outstanding writes and we are here it means that when the
    // write_complete() callback gets executed it is going to access invalid memory.
    // See IMPALA-1890.
    DCHECK_EQ(_non_local_outstanding_writes, 0) << endl << debug_internal();
    // Delete tmp files.
    for (auto& file : _tmp_files) {
        file->remove();
    }
    _tmp_files.clear();

    // Free memory resources.
    for (BufferDescriptor* buffer : _all_io_buffers) {
        _mem_tracker->Release(buffer->len);
        delete[] buffer->buffer;
    }
    DCHECK_EQ(_mem_tracker->consumption(), 0);
    _mem_tracker.reset();
}

int64_t BufferedBlockMgr2::bytes_allocated() const {
    return _mem_tracker->consumption();
}

int BufferedBlockMgr2::num_pinned_buffers(Client* client) const {
    return client->_num_pinned_buffers;
}

int BufferedBlockMgr2::num_reserved_buffers_remaining(Client* client) const {
    return std::max(client->_num_reserved_buffers - client->_num_pinned_buffers, 0);
}

std::shared_ptr<MemTracker> BufferedBlockMgr2::get_tracker(Client* client) const {
    return client->_tracker;
}

// TODO: It would be good if we had a sync primitive that supports is_mine() calls, see
//       IMPALA-1884.
Status BufferedBlockMgr2::delete_or_unpin_block(Block* block, bool unpin) {
    if (block == nullptr) {
        return is_cancelled() ? Status::Cancelled("Cancelled") : Status::OK();
    }
    if (unpin) {
        return block->unpin();
    } else {
        block->del();
        return is_cancelled() ? Status::Cancelled("Cancelled") : Status::OK();
    }
}

Status BufferedBlockMgr2::pin_block(Block* block, bool* pinned, Block* release_block, bool unpin) {
    DCHECK(block != nullptr);
    DCHECK(!block->_is_deleted);
    *pinned = false;
    if (block->_is_pinned) {
        *pinned = true;
        return delete_or_unpin_block(release_block, unpin);
    }

    bool in_mem = false;
    RETURN_IF_ERROR(find_buffer_for_block(block, &in_mem));
    *pinned = block->_is_pinned;

    // Block was not evicted or had no data, nothing left to do.
    if (in_mem || block->_valid_data_len == 0) {
        return delete_or_unpin_block(release_block, unpin);
    }

    if (!block->_is_pinned) {
        if (release_block == nullptr) {
            return Status::OK();
        }

        if (block->_buffer_desc != nullptr) {
            {
                lock_guard<mutex> lock(_lock);
                if (_free_io_buffers.contains(block->_buffer_desc)) {
                    DCHECK(!block->_is_pinned && !block->_in_write &&
                           !_unpinned_blocks.contains(block))
                            << endl
                            << block->debug_string();
                    _free_io_buffers.remove(block->_buffer_desc);
                } else if (_unpinned_blocks.contains(block)) {
                    _unpinned_blocks.remove(block);
                } else {
                    DCHECK(block->_in_write);
                }
                block->_is_pinned = true;
                *pinned = true;
                block->_client->pin_buffer(block->_buffer_desc);
                ++_total_pinned_buffers;
                RETURN_IF_ERROR(write_unpinned_blocks());
            }
            return delete_or_unpin_block(release_block, unpin);
        }

        RETURN_IF_ERROR(transfer_buffer(block, release_block, unpin));
        DCHECK(!release_block->_is_pinned);
        release_block = nullptr; // Handled by transfer.
        DCHECK(block->_is_pinned);
        *pinned = true;
    }

    // Read the block from disk if it was not in memory.
    DCHECK(block->_write_range != nullptr) << block->debug_string() << endl << release_block;
    SCOPED_TIMER(_disk_read_timer);
    // Create a ScanRange to perform the read.
    DiskIoMgr::ScanRange* scan_range = _obj_pool.add(new DiskIoMgr::ScanRange());
    scan_range->reset(nullptr, block->_write_range->file(), block->_write_range->len(),
                      block->_write_range->offset(), block->_write_range->disk_id(), false, block,
                      DiskIoMgr::ScanRange::NEVER_CACHE);
    vector<DiskIoMgr::ScanRange*> ranges(1, scan_range);
    RETURN_IF_ERROR(_io_mgr->add_scan_ranges(_io_request_context, ranges, true));

    // Read from the io mgr buffer into the block's assigned buffer.
    int64_t offset = 0;
    bool buffer_eosr = false;
    do {
        DiskIoMgr::BufferDescriptor* io_mgr_buffer;
        RETURN_IF_ERROR(scan_range->get_next(&io_mgr_buffer));
        memcpy(block->buffer() + offset, io_mgr_buffer->buffer(), io_mgr_buffer->len());
        offset += io_mgr_buffer->len();
        buffer_eosr = io_mgr_buffer->eosr();
        io_mgr_buffer->return_buffer();
    } while (!buffer_eosr);
    DCHECK_EQ(offset, block->_write_range->len());

    return delete_or_unpin_block(release_block, unpin);
}

Status BufferedBlockMgr2::unpin_block(Block* block) {
    DCHECK(!block->_is_deleted) << "Unpin for deleted block.";

    lock_guard<mutex> unpinned_lock(_lock);
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }
    DCHECK(block->validate()) << endl << block->debug_string();
    if (!block->_is_pinned) {
        return Status::OK();
    }
    DCHECK_EQ(block->_buffer_desc->len, _max_block_size) << "Can only unpin io blocks.";
    DCHECK(validate()) << endl << debug_internal();
    // Add 'block' to the list of unpinned blocks and set _is_pinned to false.
    // Cache its position in the list for later removal.
    block->_is_pinned = false;
    DCHECK(!_unpinned_blocks.contains(block)) << " Unpin for block in unpinned list";
    if (!block->_in_write) {
        _unpinned_blocks.enqueue(block);
    }
    block->_client->unpin_buffer(block->_buffer_desc);
    if (block->_client->_num_pinned_buffers < block->_client->_num_reserved_buffers) {
        ++_unfullfilled_reserved_buffers;
    }
    --_total_pinned_buffers;
    RETURN_IF_ERROR(write_unpinned_blocks());
    DCHECK(validate()) << endl << debug_internal();
    DCHECK(block->validate()) << endl << block->debug_string();
    return Status::OK();
}

Status BufferedBlockMgr2::write_unpinned_blocks() {
    if (!_enable_spill) {
        return Status::OK();
    }

    // Assumes block manager lock is already taken.
    while (_non_local_outstanding_writes + _free_io_buffers.size() < _block_write_threshold &&
           !_unpinned_blocks.empty()) {
        // Pop a block from the back of the list (LIFO).
        Block* write_block = _unpinned_blocks.pop_back();
        write_block->_client_local = false;
        RETURN_IF_ERROR(write_unpinned_block(write_block));
        ++_non_local_outstanding_writes;
    }
    DCHECK(validate()) << endl << debug_internal();
    return Status::OK();
}

Status BufferedBlockMgr2::write_unpinned_block(Block* block) {
    // Assumes block manager lock is already taken.
    DCHECK(!block->_is_pinned) << block->debug_string();
    DCHECK(!block->_in_write) << block->debug_string();
    DCHECK_EQ(block->_buffer_desc->len, _max_block_size);

    if (block->_write_range == nullptr) {
        if (_tmp_files.empty()) {
            RETURN_IF_ERROR(init_tmp_files());
        }

        // First time the block is being persisted - need to allocate tmp file space.
        TmpFileMgr::File* tmp_file;
        int64_t file_offset;
        RETURN_IF_ERROR(allocate_scratch_space(_max_block_size, &tmp_file, &file_offset));
        int disk_id = tmp_file->disk_id();
        if (disk_id < 0) {
            // Assign a valid disk id to the write range if the tmp file was not assigned one.
            static unsigned int next_disk_id = 0;
            disk_id = ++next_disk_id;
        }
        disk_id %= _io_mgr->num_local_disks();
        DiskIoMgr::WriteRange::WriteDoneCallback callback = bind(
                mem_fn(&BufferedBlockMgr2::write_complete), this, block, std::placeholders::_1);
        block->_write_range = _obj_pool.add(
                new DiskIoMgr::WriteRange(tmp_file->path(), file_offset, disk_id, callback));
        block->_tmp_file = tmp_file;
    }

    uint8_t* outbuf = nullptr;
    outbuf = block->buffer();

    block->_write_range->set_data(outbuf, block->_valid_data_len);

    // Issue write through DiskIoMgr.
    RETURN_IF_ERROR(_io_mgr->add_write_range(_io_request_context, block->_write_range));
    block->_in_write = true;
    DCHECK(block->validate()) << endl << block->debug_string();
    _outstanding_writes_counter->update(1);
    _bytes_written_counter->update(block->_valid_data_len);
    ++_writes_issued;
    if (_writes_issued == 1) {
    }
    return Status::OK();
}

Status BufferedBlockMgr2::allocate_scratch_space(int64_t block_size, TmpFileMgr::File** tmp_file,
                                                 int64_t* file_offset) {
    // Assumes block manager lock is already taken.
    vector<std::string> errs;
    // Find the next physical file in round-robin order and create a write range for it.
    for (int attempt = 0; attempt < _tmp_files.size(); ++attempt) {
        *tmp_file = _tmp_files[_next_block_index].get();
        _next_block_index = (_next_block_index + 1) % _tmp_files.size();
        if ((*tmp_file)->is_blacklisted()) {
            continue;
        }
        Status status = (*tmp_file)->allocate_space(_max_block_size, file_offset);
        if (status.ok()) {
            return Status::OK();
        }
        // Log error and try other files if there was a problem. Problematic files will be
        // blacklisted so we will not repeatedly log the same error.
        LOG(WARNING) << "Error while allocating temporary file range: " << status.get_error_msg()
                     << ". Will try another temporary file.";
        errs.emplace_back(status.message().data, status.message().size);
    }
    Status err_status = Status::InternalError(
            "No usable temporary files: space could not be allocated on any temporary device.");
    for (int i = 0; i < errs.size(); ++i) {
        err_status = err_status.clone_and_append(errs[i]);
    }
    return err_status;
}

void BufferedBlockMgr2::write_complete(Block* block, const Status& write_status) {
    Status status = Status::OK();
    lock_guard<mutex> lock(_lock);
    _outstanding_writes_counter->update(-1);
    DCHECK(validate()) << endl << debug_internal();
    DCHECK(_is_cancelled || block->_in_write) << "write_complete() for block not in write." << endl
                                              << block->debug_string();
    if (!block->_client_local) {
        DCHECK_GT(_non_local_outstanding_writes, 0) << block->debug_string();
        --_non_local_outstanding_writes;
    }
    block->_in_write = false;

    // Explicitly release our temporarily allocated buffer here so that it doesn't
    // hang around needlessly.

    // return_unused_block() will clear the block, so save the client pointer.
    // We have to be careful while touching the state because it may have been cleaned up by
    // another thread.
    RuntimeState* state = block->_client->_state;
    // If the block was re-pinned when it was in the IOMgr queue, don't free it.
    if (block->_is_pinned) {
        // The number of outstanding writes has decreased but the number of free buffers
        // hasn't.
        DCHECK(!block->_client_local)
                << "Client should be waiting. No one should have pinned this block.";
        if (write_status.ok() && !_is_cancelled && !state->is_cancelled()) {
            status = write_unpinned_blocks();
        }
    } else if (block->_client_local) {
        DCHECK(!block->_is_deleted)
                << "Client should be waiting. No one should have deleted this block.";
        block->_write_complete_cv.notify_one();
    } else {
        DCHECK_EQ(block->_buffer_desc->len, _max_block_size)
                << "Only io sized buffers should spill";
        _free_io_buffers.enqueue(block->_buffer_desc);
        // Finish the delete_block() work.
        if (block->_is_deleted) {
            block->_buffer_desc->block = nullptr;
            block->_buffer_desc = nullptr;
            return_unused_block(block);
        }
        // Multiple threads may be waiting for the same block in find_buffer().  Wake them
        // all up.  One thread will get this block, and the others will re-evaluate whether
        // they should continue waiting and if another write needs to be initiated.
        _buffer_available_cv.notify_all();
    }
    DCHECK(validate()) << endl << debug_internal();

    if (!write_status.ok() || !status.ok() || _is_cancelled) {
        VLOG_FILE << "Query: " << _query_id
                  << ". Write did not complete successfully: "
                     "write_status="
                  << write_status.get_error_msg() << ", status=" << status.get_error_msg()
                  << ". _is_cancelled=" << _is_cancelled;

        // If the instance is already cancelled, don't confuse things with these errors.
        if (!write_status.is_cancelled() && !state->is_cancelled()) {
            if (!write_status.ok()) {
                // Report but do not attempt to recover from write error.
                DCHECK(block->_tmp_file != nullptr);
                block->_tmp_file->report_io_error(write_status.get_error_msg());
                VLOG_QUERY << "Query: " << _query_id << " write complete callback with error.";
                state->log_error(write_status.get_error_msg());
            }
            if (!status.ok()) {
                VLOG_QUERY << "Query: " << _query_id << " error while writing unpinned blocks.";
                state->log_error(status.get_error_msg());
            }
        }
        // Set cancelled and wake up waiting threads if an error occurred.  Note that in
        // the case of _client_local, that thread was woken up above.
        _is_cancelled = true;
        _buffer_available_cv.notify_all();
    }
}

void BufferedBlockMgr2::delete_block(Block* block) {
    DCHECK(!block->_is_deleted);

    lock_guard<mutex> lock(_lock);
    DCHECK(block->validate()) << endl << debug_internal();
    block->_is_deleted = true;

    if (block->_is_pinned) {
        if (block->is_max_size()) {
            --_total_pinned_buffers;
        }
        block->_client->unpin_buffer(block->_buffer_desc);
        // Only block is io size we need change _unfullfilled_reserved_buffers
        if (block->is_max_size() &&
            block->_client->_num_pinned_buffers < block->_client->_num_reserved_buffers) {
            ++_unfullfilled_reserved_buffers;
        }
        block->_is_pinned = false;
    } else if (_unpinned_blocks.contains(block)) {
        // Remove block from unpinned list.
        _unpinned_blocks.remove(block);
    }

    if (block->_in_write) {
        DCHECK(block->_buffer_desc != nullptr && block->_buffer_desc->len == _max_block_size)
                << "Should never be writing a small buffer";
        // If a write is still pending, return. Cleanup will be done in write_complete().
        DCHECK(block->validate()) << endl << block->debug_string();
        return;
    }

    if (block->_buffer_desc != nullptr) {
        if (block->_buffer_desc->len != _max_block_size) {
            // Just delete the block for now.
            delete[] block->_buffer_desc->buffer;
            block->_client->_tracker->Release(block->_buffer_desc->len);
            delete block->_buffer_desc;
            block->_buffer_desc = nullptr;
        } else {
            if (!_free_io_buffers.contains(block->_buffer_desc)) {
                _free_io_buffers.enqueue(block->_buffer_desc);
                _buffer_available_cv.notify_one();
            }
            block->_buffer_desc->block = nullptr;
            block->_buffer_desc = nullptr;
        }
    }
    return_unused_block(block);
    DCHECK(block->validate()) << endl << block->debug_string();
    DCHECK(validate()) << endl << debug_internal();
}

void BufferedBlockMgr2::return_unused_block(Block* block) {
    DCHECK(block->_is_deleted) << block->debug_string();
    DCHECK(!block->_is_pinned) << block->debug_string();
    ;
    DCHECK(block->_buffer_desc == nullptr);
    block->init();
    _unused_blocks.enqueue(block);
}

Status BufferedBlockMgr2::find_buffer_for_block(Block* block, bool* in_mem) {
    DCHECK(block != nullptr);
    Client* client = block->_client;
    DCHECK(client != nullptr);
    DCHECK(!block->_is_pinned && !block->_is_deleted) << "Pinned or deleted block " << endl
                                                      << block->debug_string();
    *in_mem = false;

    unique_lock<mutex> l(_lock);
    if (_is_cancelled) {
        return Status::Cancelled("Cancelled");
    }

    // First check if there is enough reserved memory to satisfy this request.
    bool is_reserved_request = false;
    if (client->_num_pinned_buffers < client->_num_reserved_buffers) {
        is_reserved_request = true;
    } else if (client->_num_tmp_reserved_buffers > 0) {
        is_reserved_request = true;
        --client->_num_tmp_reserved_buffers;
    }

    DCHECK(validate()) << endl << debug_internal();
    if (is_reserved_request) {
        --_unfullfilled_reserved_buffers;
    }

    if (!is_reserved_request && remaining_unreserved_buffers() < 1) {
        // The client already has its quota and there are no unreserved blocks left.
        // Note that even if this passes, it is still possible for the path below to
        // see OOM because another query consumed memory from the process tracker. This
        // only happens if the buffer has not already been allocated by the block mgr.
        // This check should ensure that the memory cannot be consumed by another client
        // of the block mgr.
        return Status::OK();
    }

    if (block->_buffer_desc != nullptr) {
        // The block is in memory. It may be in 3 states:
        //  1. In the unpinned list. The buffer will not be in the free list.
        //  2. _in_write == true. The buffer will not be in the free list.
        //  3. The buffer is free, but hasn't yet been reassigned to a different block.
        DCHECK_EQ(block->_buffer_desc->len, max_block_size()) << "Non-I/O blocks are always pinned";
        DCHECK(_unpinned_blocks.contains(block) || block->_in_write ||
               _free_io_buffers.contains(block->_buffer_desc));
        if (_unpinned_blocks.contains(block)) {
            _unpinned_blocks.remove(block);
            DCHECK(!_free_io_buffers.contains(block->_buffer_desc));
        } else if (block->_in_write) {
            DCHECK(block->_in_write && !_free_io_buffers.contains(block->_buffer_desc));
        } else {
            _free_io_buffers.remove(block->_buffer_desc);
        }
        _buffered_pin_counter->update(1);
        *in_mem = true;
    } else {
        BufferDescriptor* buffer_desc = nullptr;
        RETURN_IF_ERROR(find_buffer(l, &buffer_desc));

        if (buffer_desc == nullptr) {
            // There are no free buffers or blocks we can evict. We need to fail this request.
            // If this is an optional request, return OK. If it is required, return OOM.
            if (!is_reserved_request) {
                return Status::OK();
            }

            if (VLOG_QUERY_IS_ON) {
                stringstream ss;
                ss << "Query id=" << _query_id << " was unable to get minimum required buffers."
                   << endl
                   << debug_internal() << endl
                   << client->debug_string();
                VLOG_QUERY << ss.str();
            }
            return add_exec_msg(
                    "Query did not have enough memory to get the minimum required "
                    "buffers in the block manager.");
        }

        DCHECK(buffer_desc != nullptr);
        DCHECK_EQ(buffer_desc->len, max_block_size()) << "Non-I/O buffer";
        if (buffer_desc->block != nullptr) {
            // This buffer was assigned to a block but now we are reusing it. Reset the
            // previous block->buffer link.
            DCHECK(buffer_desc->block->validate()) << endl << buffer_desc->block->debug_string();
            buffer_desc->block->_buffer_desc = nullptr;
        }
        buffer_desc->block = block;
        block->_buffer_desc = buffer_desc;
    }
    DCHECK(block->_buffer_desc != nullptr);
    DCHECK(block->_buffer_desc->len < max_block_size() || !block->_is_pinned)
            << "Trying to pin already pinned block. " << block->_buffer_desc->len << " "
            << block->_is_pinned;
    block->_is_pinned = true;
    client->pin_buffer(block->_buffer_desc);
    ++_total_pinned_buffers;

    DCHECK(block->validate()) << endl << block->debug_string();
    // The number of free buffers has decreased. Write unpinned blocks if the number
    // of free buffers below the threshold is reached.
    RETURN_IF_ERROR(write_unpinned_blocks());
    DCHECK(validate()) << endl << debug_internal();
    return Status::OK();
}

// We need to find a new buffer. We prefer getting this buffer in this order:
//  1. Allocate a new block if the number of free blocks is less than the write
//     threshold, until we run out of memory.
//  2. Pick a buffer from the free list.
//  3. Wait and evict an unpinned buffer.
Status BufferedBlockMgr2::find_buffer(unique_lock<mutex>& lock, BufferDescriptor** buffer_desc) {
    *buffer_desc = nullptr;

    // First, try to allocate a new buffer.
    if (_free_io_buffers.size() < _block_write_threshold &&
        _mem_tracker->TryConsume(_max_block_size)) {
        uint8_t* new_buffer = new uint8_t[_max_block_size];
        *buffer_desc = _obj_pool.add(new BufferDescriptor(new_buffer, _max_block_size));
        (*buffer_desc)->all_buffers_it =
                _all_io_buffers.insert(_all_io_buffers.end(), *buffer_desc);
        return Status::OK();
    }

    // Second, try to pick a buffer from the free list.
    if (_free_io_buffers.empty()) {
        // There are no free buffers. If spills are disabled or there no unpinned blocks we
        // can write, return. We can't get a buffer.
        if (!_enable_spill) {
            return add_exec_msg(
                    "Spilling has been disabled for plans,"
                    "current memory usage has reached the bottleneck. "
                    "You can avoid the behavior via increasing the mem limit "
                    "by session variable exec_mem_limit or enable_spilling.");
        }

        // Third, this block needs to use a buffer that was unpinned from another block.
        // Get a free buffer from the front of the queue and assign it to the block.
        do {
            if (_unpinned_blocks.empty() && _non_local_outstanding_writes == 0) {
                return Status::OK();
            }
            SCOPED_TIMER(_buffer_wait_timer);
            // Try to evict unpinned blocks before waiting.
            RETURN_IF_ERROR(write_unpinned_blocks());
            DCHECK_GT(_non_local_outstanding_writes, 0) << endl << debug_internal();
            _buffer_available_cv.wait(lock);
            if (_is_cancelled) {
                return Status::Cancelled("Cancelled");
            }
        } while (_free_io_buffers.empty());
    }
    *buffer_desc = _free_io_buffers.dequeue();
    return Status::OK();
}

BufferedBlockMgr2::Block* BufferedBlockMgr2::get_unused_block(Client* client) {
    DCHECK(client != nullptr);
    Block* new_block = nullptr;
    if (_unused_blocks.empty()) {
        new_block = _obj_pool.add(new Block(this));
        new_block->init();
        _created_block_counter->update(1);
    } else {
        new_block = _unused_blocks.dequeue();
        _recycled_blocks_counter->update(1);
    }
    DCHECK(new_block != nullptr);
    new_block->_client = client;
    return new_block;
}

bool BufferedBlockMgr2::validate() const {
    int num_free_io_buffers = 0;

    if (_total_pinned_buffers < 0) {
        LOG(ERROR) << "_total_pinned_buffers < 0: " << _total_pinned_buffers;
        return false;
    }

    for (BufferDescriptor* buffer : _all_io_buffers) {
        bool is_free = _free_io_buffers.contains(buffer);
        num_free_io_buffers += is_free;

        if (*buffer->all_buffers_it != buffer) {
            LOG(ERROR) << "All buffers list is corrupt. Buffer iterator is not valid.";
            return false;
        }

        if (buffer->block == nullptr && !is_free) {
            LOG(ERROR) << "Buffer with no block not in free list." << endl << debug_internal();
            return false;
        }

        if (buffer->len != _max_block_size) {
            LOG(ERROR) << "Non-io sized buffers should not end up on free list.";
            return false;
        }

        if (buffer->block != nullptr) {
            if (buffer->block->_buffer_desc != buffer) {
                LOG(ERROR) << "buffer<->block pointers inconsistent. Buffer: " << buffer << endl
                           << buffer->block->debug_string();
                return false;
            }

            if (!buffer->block->validate()) {
                LOG(ERROR) << "buffer->block inconsistent." << endl
                           << buffer->block->debug_string();
                return false;
            }

            if (is_free && (buffer->block->_is_pinned || buffer->block->_in_write ||
                            _unpinned_blocks.contains(buffer->block))) {
                LOG(ERROR) << "Block with buffer in free list and"
                           << " _is_pinned = " << buffer->block->_is_pinned
                           << " _in_write = " << buffer->block->_in_write
                           << " _Unpinned_blocks.contains = "
                           << _unpinned_blocks.contains(buffer->block) << endl
                           << buffer->block->debug_string();
                return false;
            }
        }
    }

    if (_free_io_buffers.size() != num_free_io_buffers) {
        LOG(ERROR) << "_free_buffer_list inconsistency."
                   << " num_free_io_buffers = " << num_free_io_buffers
                   << " _free_io_buffers.size() = " << _free_io_buffers.size() << endl
                   << debug_internal();
        return false;
    }

    Block* block = _unpinned_blocks.head();
    while (block != nullptr) {
        if (!block->validate()) {
            LOG(ERROR) << "Block inconsistent in unpinned list." << endl << block->debug_string();
            return false;
        }

        if (block->_in_write || _free_io_buffers.contains(block->_buffer_desc)) {
            LOG(ERROR) << "Block in unpinned list with"
                       << " _in_write = " << block->_in_write << " _free_io_buffers.contains = "
                       << _free_io_buffers.contains(block->_buffer_desc) << endl
                       << block->debug_string();
            return false;
        }
        block = block->next();
    }

    // Check if we're writing blocks when the number of free buffers falls below
    // threshold. We don't write blocks after cancellation.
    if (!_is_cancelled && !_unpinned_blocks.empty() && _enable_spill &&
        (_free_io_buffers.size() + _non_local_outstanding_writes < _block_write_threshold)) {
        // TODO: this isn't correct when write_unpinned_blocks() fails during the call to
        // write_unpinned_block() so just log the condition but don't return false. Figure
        // out a way to re-enable this change?
        LOG(ERROR) << "Missed writing unpinned blocks";
    }
    return true;
}

string BufferedBlockMgr2::debug_string(Client* client) {
    stringstream ss;
    unique_lock<mutex> l(_lock);
    ss << debug_internal();
    if (client != nullptr) {
        ss << endl << client->debug_string();
    }
    return ss.str();
}

string BufferedBlockMgr2::debug_internal() const {
    stringstream ss;
    ss << "Buffered block mgr" << endl
       << "  Num writes outstanding: " << _outstanding_writes_counter->value() << endl
       << "  Num free io buffers: " << _free_io_buffers.size() << endl
       << "  Num unpinned blocks: " << _unpinned_blocks.size() << endl
       << "  Num available buffers: " << remaining_unreserved_buffers() << endl
       << "  Total pinned buffers: " << _total_pinned_buffers << endl
       << "  Unfullfilled reserved buffers: " << _unfullfilled_reserved_buffers << endl
       << "  Remaining memory: " << _mem_tracker->SpareCapacity(MemLimit::HARD)
       << " (#blocks=" << (_mem_tracker->SpareCapacity(MemLimit::HARD) / _max_block_size) << ")"
       << endl
       << "  Block write threshold: " << _block_write_threshold;
    return ss.str();
}

void BufferedBlockMgr2::init(DiskIoMgr* io_mgr, RuntimeProfile* parent_profile,
                             const std::shared_ptr<MemTracker>& parent_tracker, int64_t mem_limit) {
    unique_lock<mutex> l(_lock);
    if (_initialized) {
        return;
    }

    io_mgr->register_context(&_io_request_context);

    _profile.reset(new RuntimeProfile("BlockMgr"));
    parent_profile->add_child(_profile.get(), true, nullptr);

    _block_size_counter = ADD_COUNTER(_profile.get(), "MaxBlockSize", TUnit::BYTES);
    _block_size_counter->set(_max_block_size);
    _created_block_counter = ADD_COUNTER(_profile.get(), "BlocksCreated", TUnit::UNIT);
    _recycled_blocks_counter = ADD_COUNTER(_profile.get(), "BlocksRecycled", TUnit::UNIT);
    _bytes_written_counter = ADD_COUNTER(_profile.get(), "BytesWritten", TUnit::BYTES);
    _outstanding_writes_counter =
            ADD_COUNTER(_profile.get(), "BlockWritesOutstanding", TUnit::UNIT);
    _buffered_pin_counter = ADD_COUNTER(_profile.get(), "BufferedPins", TUnit::UNIT);
    _disk_read_timer = ADD_TIMER(_profile.get(), "TotalReadBlockTime");
    _buffer_wait_timer = ADD_TIMER(_profile.get(), "TotalBufferWaitTime");
    _encryption_timer = ADD_TIMER(_profile.get(), "TotalEncryptionTime");
    _integrity_check_timer = ADD_TIMER(_profile.get(), "TotalIntegrityCheckTime");

    // Create a new mem_tracker and allocate buffers.
    _mem_tracker = MemTracker::CreateTracker(mem_limit, "BufferedBlockMgr2", parent_tracker);

    _initialized = true;
}

Status BufferedBlockMgr2::init_tmp_files() {
    DCHECK(_tmp_files.empty());
    DCHECK(_tmp_file_mgr != nullptr);

    vector<TmpFileMgr::DeviceId> tmp_devices = _tmp_file_mgr->active_tmp_devices();
    // Initialize the tmp files and the initial file to use.
    _tmp_files.reserve(tmp_devices.size());
    for (int i = 0; i < tmp_devices.size(); ++i) {
        TmpFileMgr::File* tmp_file;
        TmpFileMgr::DeviceId tmp_device_id = tmp_devices[i];
        // It is possible for a device to be blacklisted after it was returned
        // by active_tmp_devices() - handle this gracefully.
        Status status = _tmp_file_mgr->get_file(tmp_device_id, _query_id, &tmp_file);
        if (status.ok()) {
            _tmp_files.emplace_back(tmp_file);
        }
    }
    if (_tmp_files.empty()) {
        return Status::InternalError(
                "No spilling directories configured. Cannot spill. Set --scratch_dirs"
                " or see log for previous errors that prevented use of provided directories");
    }
    _next_block_index = rand() % _tmp_files.size();
    return Status::OK();
}

} // namespace doris
