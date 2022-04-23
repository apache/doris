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

#ifndef DORIS_BE_RUNTIME_BUFFER_POOL_INTERNAL_H
#define DORIS_BE_RUNTIME_BUFFER_POOL_INTERNAL_H

#include <memory>
#include <mutex>
#include <sstream>

#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/bufferpool/buffer_pool_counters.h"
#include "runtime/bufferpool/reservation_tracker.h"

// Ensure that DCheckConsistency() function calls get removed in release builds.
#ifndef NDEBUG
#define DCHECK_CONSISTENCY() DCheckConsistency()
#else
#define DCHECK_CONSISTENCY()
#endif

namespace doris {

/// The internal representation of a page, which can be pinned or unpinned. See the
/// class comment for explanation of the different page states.
class BufferPool::Page : public InternalList<Page>::Node {
public:
    Page(Client* client, int64_t len)
            : client(client), len(len), pin_count(0), pin_in_flight(false) {}

    std::string DebugString();

    // Helper for BufferPool::DebugString().
    static bool DebugStringCallback(std::stringstream* ss, BufferPool::Page* page);

    /// The client that the page belongs to.
    Client* const client;

    /// The length of the page in bytes.
    const int64_t len;

    /// The pin count of the page. Only accessed in contexts that are passed the associated
    /// PageHandle, so it cannot be accessed by multiple threads concurrently.
    int pin_count;

    /// True if the read I/O to pin the page was started but not completed. Only accessed
    /// in contexts that are passed the associated PageHandle, so it cannot be accessed
    /// by multiple threads concurrently.
    bool pin_in_flight;

    /// Non-null if there is a write in flight, the page is clean, or the page is evicted.
    //std::unique_ptr<TmpFileMgr::WriteHandle> write_handle;

    /// This lock must be held when accessing 'buffer' if the page is unpinned and not
    /// evicted (i.e. it is safe to access 'buffer' if the page is pinned or evicted).
    SpinLock buffer_lock;

    /// Buffer with the page's contents. Closed only iff page is evicted. Open otherwise.
    BufferHandle buffer;
};

/// Wrapper around InternalList<Page> that tracks the # of bytes in the list.
class BufferPool::PageList {
public:
    PageList() : bytes_(0) {}
    ~PageList() {
        // Clients always empty out their list before destruction.
        DCHECK(list_.empty());
        DCHECK_EQ(0, bytes_);
    }

    void enqueue(Page* page) {
        list_.enqueue(page);
        bytes_ += page->len;
    }

    bool remove(Page* page) {
        if (list_.remove(page)) {
            bytes_ -= page->len;
            return true;
        }
        return false;
    }

    Page* dequeue() {
        Page* page = list_.dequeue();
        if (page != nullptr) {
            bytes_ -= page->len;
        }
        return page;
    }

    Page* pop_back() {
        Page* page = list_.pop_back();
        if (page != nullptr) {
            bytes_ -= page->len;
        }
        return page;
    }

    void iterate(std::function<bool(Page*)> fn) { list_.iterate(fn); }
    bool contains(Page* page) { return list_.contains(page); }
    Page* tail() { return list_.tail(); }
    bool empty() const { return list_.empty(); }
    int size() const { return list_.size(); }
    int64_t bytes() const { return bytes_; }

    void DCheckConsistency() {
        DCHECK_GE(bytes_, 0);
        DCHECK_EQ(list_.empty(), bytes_ == 0);
    }

private:
    InternalList<Page> list_;
    int64_t bytes_;
};

/// The internal state for the client.
class BufferPool::Client {
public:
    Client(BufferPool* pool, //TmpFileMgr::FileGroup* file_group,
           const std::string& name, ReservationTracker* parent_reservation,
           const std::shared_ptr<MemTracker>& mem_tracker, int64_t reservation_limit,
           RuntimeProfile* profile);

    ~Client() {
        DCHECK_EQ(0, num_pages_);
        DCHECK_EQ(0, buffers_allocated_bytes_);
    }

    /// Release reservation for this client.
    void Close() { reservation_.Close(); }

    /// Create a pinned page using 'buffer', which was allocated using AllocateBuffer().
    /// No client or page locks should be held by the caller.
    Page* CreatePinnedPage(BufferHandle&& buffer);

    /// Reset 'handle', clean up references to handle->page and release any resources
    /// associated with handle->page. If the page is pinned, 'out_buffer' can be passed in
    /// and the page's buffer will be returned.
    /// Neither the client's lock nor handle->page_->buffer_lock should be held by the
    /// caller.
    void DestroyPageInternal(PageHandle* handle, BufferHandle* out_buffer = nullptr);

    /// Updates client state to reflect that 'page' is now a dirty unpinned page. May
    /// initiate writes for this or other dirty unpinned pages.
    /// Neither the client's lock nor page->buffer_lock should be held by the caller.
    void MoveToDirtyUnpinned(Page* page);

    /// Move an unpinned page to the pinned state, moving between data structures and
    /// reading from disk if necessary. Ensures the page has a buffer. If the data is
    /// already in memory, ensures the data is in the page's buffer. If the data is on
    /// disk, starts an async read of the data and sets 'pin_in_flight' on the page to
    /// true. Neither the client's lock nor page->buffer_lock should be held by the caller.
    Status StartMoveToPinned(ClientHandle* client, Page* page) WARN_UNUSED_RESULT;

    /// Moves a page that has a pin in flight back to the evicted state, undoing
    /// StartMoveToPinned(). Neither the client's lock nor page->buffer_lock should be held
    /// by the caller.
    //void UndoMoveEvictedToPinned(Page* page);

    /// Finish the work of bring the data of an evicted page to memory if
    /// page->pin_in_flight was set to true by StartMoveToPinned().
    //Status FinishMoveEvictedToPinned(Page* page) WARN_UNUSED_RESULT;

    /// Must be called once before allocating a buffer of 'len' via the AllocateBuffer()
    /// API to deduct from the client's reservation and update internal accounting. Cleans
    /// dirty pages if needed to satisfy the buffer pool's internal invariants. No page or
    /// client locks should be held by the caller.
    Status PrepareToAllocateBuffer(int64_t len) WARN_UNUSED_RESULT;

    /// Implementation of ClientHandle::DecreaseReservationTo().
    Status DecreaseReservationTo(int64_t target_bytes) WARN_UNUSED_RESULT;

    /// Called after a buffer of 'len' is freed via the FreeBuffer() API to update
    /// internal accounting and release the buffer to the client's reservation. No page or
    /// client locks should be held by the caller.
    void FreedBuffer(int64_t len) {
        std::lock_guard<std::mutex> cl(lock_);
        reservation_.ReleaseTo(len);
        buffers_allocated_bytes_ -= len;
        DCHECK_CONSISTENCY();
    }

    /// Wait for the in-flight write for 'page' to complete.
    /// 'lock_' must be held by the caller via 'client_lock'. page->buffer_lock should
    /// not be held.
    //void WaitForWrite(std::unique_lock<std::mutex>* client_lock, Page* page);

    /// Test helper: wait for all in-flight writes to complete.
    /// 'lock_' must not be held by the caller.
    //void WaitForAllWrites();

    /// Asserts that 'client_lock' is holding 'lock_'.
    void DCheckHoldsLock(const std::unique_lock<std::mutex>& client_lock) {
        DCHECK(client_lock.mutex() == &lock_ && client_lock.owns_lock());
    }

    ReservationTracker* reservation() { return &reservation_; }
    const BufferPoolClientCounters& counters() const { return counters_; }
    //bool spilling_enabled() const { return file_group_ != nullptr; }
    void set_debug_write_delay_ms(int val) { debug_write_delay_ms_ = val; }
    bool has_unpinned_pages() const {
        // Safe to read without lock since other threads should not be calling BufferPool
        // functions that create, destroy or unpin pages.
        return pinned_pages_.size() < num_pages_;
    }

    std::string DebugString();

private:
    // Check consistency of client, DCHECK if inconsistent. 'lock_' must be held.
    void DCheckConsistency() {
        DCHECK_GE(buffers_allocated_bytes_, 0);
        pinned_pages_.DCheckConsistency();
        dirty_unpinned_pages_.DCheckConsistency();
        in_flight_write_pages_.DCheckConsistency();
        DCHECK_LE(
                pinned_pages_.size() + dirty_unpinned_pages_.size() + in_flight_write_pages_.size(),
                num_pages_);
        // Check that we flushed enough pages to disk given our eviction policy.
        DCHECK_GE(reservation_.GetReservation(), buffers_allocated_bytes_ + pinned_pages_.bytes() +
                                                         dirty_unpinned_pages_.bytes() +
                                                         in_flight_write_pages_.bytes());
    }

    /// Must be called once before allocating or reclaiming a buffer of 'len'. Ensures that
    /// enough dirty pages are flushed to disk to satisfy the buffer pool's internal
    /// invariants after the allocation. 'lock_' should be held by the caller via
    /// 'client_lock'
    Status CleanPages(std::unique_lock<std::mutex>* client_lock, int64_t len);

    /// Initiates asynchronous writes of dirty unpinned pages to disk. Ensures that at
    /// least 'min_bytes_to_write' bytes of writes will be written asynchronously. May
    /// start writes more aggressively so that I/O and compute can be overlapped. If
    /// any errors are encountered, 'write_status_' is set. 'write_status_' must therefore
    /// be checked before reading back any pages. 'lock_' must be held by the caller.
    //void WriteDirtyPagesAsync(int64_t min_bytes_to_write = 0);

    /// Called when a write for 'page' completes.
    //void WriteCompleteCallback(Page* page, const Status& write_status);

    /// Move an evicted page to the pinned state by allocating a new buffer, starting an
    /// async read from disk and moving the page to 'pinned_pages_'. client->impl must be
    /// locked by the caller via 'client_lock' and handle->page must be unlocked.
    /// 'client_lock' is released then reacquired.
    //Status StartMoveEvictedToPinned(
    //    std::unique_lock<std::mutex>* client_lock, ClientHandle* client, Page* page);

    /// The buffer pool that owns the client.
    BufferPool* const pool_;

    /// The file group that should be used for allocating scratch space. If nullptr, spilling
    /// is disabled.
    //TmpFileMgr::FileGroup* const file_group_;

    /// A name identifying the client.
    const std::string name_;

    /// The reservation tracker for the client. All pages pinned by the client count as
    /// usage against 'reservation_'.
    ReservationTracker reservation_;

    /// The RuntimeProfile counters for this client, owned by the client's RuntimeProfile.
    /// All non-nullptr.
    BufferPoolClientCounters counters_;

    /// Debug option to delay write completion.
    int debug_write_delay_ms_;

    /// Lock to protect the below member variables;
    std::mutex lock_;

    /// All non-OK statuses returned by write operations are merged into this status.
    /// All operations that depend on pages being written to disk successfully (e.g.
    /// reading pages back from disk) must check 'write_status_' before proceeding, so
    /// that write errors that occurred asynchronously are correctly propagated. The
    /// write error is global to the client so can be propagated to any Status-returning
    /// operation for the client (even for operations on different Pages or Buffers).
    /// Write errors are not recoverable so it is best to propagate them as quickly
    /// as possible, instead of waiting to propagate them in a specific way.
    Status write_status_;

    /// Total number of pages for this client. Used for debugging and enforcing that all
    /// pages are destroyed before the client.
    int64_t num_pages_;

    /// Total bytes of buffers in BufferHandles returned to clients (i.e. obtained from
    /// AllocateBuffer() or ExtractBuffer()).
    int64_t buffers_allocated_bytes_;

    /// All pinned pages for this client.
    PageList pinned_pages_;

    /// Dirty unpinned pages for this client for which writes are not in flight. Page
    /// writes are started in LIFO order, because operators typically have sequential access
    /// patterns where the most recently evicted page will be last to be read.
    PageList dirty_unpinned_pages_;

    /// Dirty unpinned pages for this client for which writes are in flight.
    PageList in_flight_write_pages_;
};
} // namespace doris

#endif
