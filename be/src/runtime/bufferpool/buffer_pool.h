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

#ifndef DORIS_BE_RUNTIME_BUFFER_POOL_H
#define DORIS_BE_RUNTIME_BUFFER_POOL_H

#include <stdint.h>

#include <string>
#include <vector>

#include "common/compiler_util.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "gutil/dynamic_annotations.h"
#include "gutil/macros.h"
//#include "runtime/tmp_file_mgr.h"
#include "util/aligned_new.h"
#include "util/internal_queue.h"
#include "util/mem_range.h"
#include "util/spinlock.h"

namespace doris {

class ReservationTracker;
class RuntimeProfile;
class SystemAllocator;
class MemTracker;

/// A buffer pool that manages memory buffers for all queries in an Impala daemon.
/// The buffer pool enforces buffer reservations, limits, and implements policies
/// for moving spilled memory from in-memory buffers to disk. It also enables reuse of
/// buffers between queries, to avoid frequent allocations.
///
/// The buffer pool can be used for allocating any large buffers (above a configurable
/// minimum length), whether or not the buffers will be spilled. Smaller allocations
/// are not serviced directly by the buffer pool: clients of the buffer pool must
/// subdivide buffers if they wish to use smaller allocations.
///
/// All buffer pool operations are in the context of a registered buffer pool client.
/// A buffer pool client should be created for every allocator of buffers at the level
/// of granularity required for reporting and enforcement of reservations, e.g. an
/// operator. The client tracks buffer reservations via its ReservationTracker and also
/// includes info that is helpful for debugging (e.g. the operator that is associated
/// with the buffer). Unless otherwise noted, it is not safe to invoke concurrent buffer
/// pool operations for the same client.
///
/// Pages, Buffers and Pinning
/// ==========================
/// * A page is a logical block of memory that can reside in memory or on disk.
/// * A buffer is a physical block of memory that can hold a page in memory.
/// * A page handle is used by buffer pool clients to identify and access a page and
///   the corresponding buffer. Clients do not interact with pages directly.
/// * A buffer handle is used by buffer pool clients to identify and access a buffer.
/// * A page is pinned if it has pin count > 0. A pinned page stays mapped to the same
///   buffer.
/// * An unpinned page can be written out to disk by the buffer pool so that the buffer
///   can be used for another purpose.
///
/// Buffer/Page Sizes
/// =================
/// The buffer pool has a minimum buffer size, which must be a power-of-two. Page and
/// buffer sizes must be an exact power-of-two multiple of the minimum buffer size.
///
/// Reservations
/// ============
/// Before allocating buffers or pinning pages, a client must reserve memory through its
/// ReservationTracker. Reservation of n bytes give a client the right to allocate
/// buffers or pin pages summing up to n bytes. Reservations are both necessary and
/// sufficient for a client to allocate buffers or pin pages: the operations succeed
/// unless a "system error" such as a disk write error is encountered that prevents
/// unpinned pages from being written to disk.
///
/// More memory may be reserved than is used, e.g. if a client is not using its full
/// reservation. In such cases, the buffer pool can use the free buffers in any way,
/// e.g. for keeping unpinned pages in memory, so long as it is able to fulfill the
/// reservations when needed, e.g. by flushing unpinned pages to disk.
///
/// Page/Buffer Handles
/// ===================
/// The buffer pool exposes PageHandles and BufferHandles, which are owned by clients of
/// the buffer pool, and act as a proxy for the internal data structure representing the
/// page or buffer in the buffer pool. Handles are "open" if they are associated with a
/// page or buffer. An open PageHandle is obtained by creating a page. PageHandles are
/// closed by calling BufferPool::DestroyPage(). An open BufferHandle is obtained by
/// allocating a buffer or extracting a BufferHandle from a PageHandle. The buffer of a
/// pinned page can also be accessed through the PageHandle. The handle destructors check
/// for resource leaks, e.g. an open handle that would result in a buffer leak.
///
/// Pin Counting of Page Handles:
/// ----------------------------------
/// Page handles are scoped to a client. The invariants are as follows:
/// * A page can only be accessed through an open handle.
/// * A page is destroyed once the handle is destroyed via DestroyPage().
/// * A page's buffer can only be accessed through a pinned handle.
/// * Pin() can be called on an open handle, incrementing the handle's pin count.
/// * Unpin() can be called on a pinned handle, but not an unpinned handle.
/// * Pin() always increases usage of reservations, and Unpin() always decreases usage,
///   i.e. the handle consumes <pin count> * <page size> bytes of reservation.
///
/// Example Usage: Buffers
/// ==================================
/// The simplest use case is to allocate a memory buffer.
/// * The new buffer is created with AllocateBuffer().
/// * The client reads and writes to the buffer as it sees fit.
/// * If the client is done with the buffer's contents it can call FreeBuffer() to
///   destroy the handle and free the buffer, or use TransferBuffer() to transfer
///   the buffer to a different client.
///
/// Example Usage: Spillable Pages
/// ==============================
/// * In order to spill pages to disk, the Client must be registered with a FileGroup,
///   which is used to allocate scratch space on disk.
/// * A spilling operator creates a new page with CreatePage().
/// * The client reads and writes to the page's buffer as it sees fit.
/// * If the operator encounters memory pressure, it can decrease reservation usage by
///   calling Unpin() on the page. The page may then be written to disk and its buffer
///   repurposed internally by BufferPool.
/// * Once the operator needs the page's contents again and has sufficient unused
///   reservation, it can call Pin(), which brings the page's contents back into memory,
///   perhaps in a different buffer. Therefore the operator must fix up any pointers into
///   the previous buffer. Pin() executes asynchronously - the caller only blocks waiting
///   for read I/O if it calls GetBuffer() or ExtractBuffer() while the read is in
///   flight.
/// * If the operator is done with the page, it can call DestroyPage() to destroy the
///   handle and release resources, or call ExtractBuffer() to extract the buffer.
///
/// Synchronization
/// ===============
/// The data structures in the buffer pool itself are thread-safe. Client-owned data
/// structures - Client, PageHandle and BufferHandle - are not protected from concurrent
/// accesses. Clients must ensure that they do not invoke concurrent operations with the
/// same Client, PageHandle or BufferHandle.
class BufferPool : public CacheLineAligned {
public:
    struct BufferAllocator;
    class BufferHandle;
    class ClientHandle;
    class PageHandle;
    class SubReservation;
    /// Constructs a new buffer pool.
    /// 'min_buffer_len': the minimum buffer length for the pool. Must be a power of two.
    /// 'buffer_bytes_limit': the maximum physical memory in bytes that can be used by the
    ///     buffer pool. If 'buffer_bytes_limit' is not a multiple of 'min_buffer_len', the
    ///     remainder will not be usable.
    /// 'clean_page_bytes_limit': the maximum bytes of clean pages that will be retained by the
    ///     buffer pool.
    BufferPool(int64_t min_buffer_len, int64_t buffer_bytes_limit, int64_t clean_page_bytes_limit);
    ~BufferPool();

    /// Register a client. Returns an error status and does not register the client if the
    /// arguments are invalid. 'name' is an arbitrary name used to identify the client in
    /// any errors messages or logging. If 'file_group' is non-nullptr, it is used to allocate
    /// scratch space to write unpinned pages to disk. If it is nullptr, unpinning of pages is
    /// not allowed for this client. Counters for this client are added to the (non-nullptr)
    /// 'profile'. 'client' is the client to register. 'client' must not already be
    /// registered.
    ///
    /// The client's reservation is created as a child of 'parent_reservation' with limit
    /// 'reservation_limit' and associated with MemTracker 'mem_tracker'. The initial
    /// reservation is 0 bytes.
    Status RegisterClient(const std::string& name, ReservationTracker* parent_reservation,
                          const std::shared_ptr<MemTracker>& mem_tracker, int64_t reservation_limit,
                          RuntimeProfile* profile, ClientHandle* client) WARN_UNUSED_RESULT;

    /// Deregister 'client' if it is registered. All pages must be destroyed and buffers
    /// must be freed for the client before calling this. Releases any reservation that
    /// belongs to the client. Idempotent.
    void DeregisterClient(ClientHandle* client);

    /// Create a new page of 'len' bytes with pin count 1. 'len' must be a page length
    /// supported by BufferPool (see BufferPool class comment). The client must have
    /// sufficient unused reservation to pin the new page (otherwise it will DCHECK).
    /// CreatePage() only fails when a system error prevents the buffer pool from fulfilling
    /// the reservation.
    /// On success, the handle is mapped to the new page and 'buffer', if non-nullptr, is set
    /// to the page's buffer.
    Status CreatePage(ClientHandle* client, int64_t len, PageHandle* handle,
                      const BufferHandle** buffer = nullptr) WARN_UNUSED_RESULT;

    /// Increment the pin count of 'handle'. After Pin() the underlying page will
    /// be mapped to a buffer, which will be accessible through 'handle'. If the data
    /// was evicted from memory, it will be read back into memory asynchronously.
    /// Attempting to access the buffer with ExtractBuffer() or handle.GetBuffer() will
    /// block until the data is in memory. The caller is responsible for ensuring it has
    /// enough unused reservation before calling Pin() (otherwise it will DCHECK). Pin()
    /// only fails when a system error prevents the buffer pool from fulfilling the
    /// reservation or if an I/O error is encountered reading back data from disk.
    /// 'handle' must be open.
    Status Pin(ClientHandle* client, PageHandle* handle) WARN_UNUSED_RESULT;

    /// Decrement the pin count of 'handle'. Decrease client's reservation usage. If the
    /// handle's pin count becomes zero, it is no longer valid for the underlying page's
    /// buffer to be accessed via 'handle'. If the page's total pin count across all
    /// handles that reference it goes to zero, the page's data may be written to disk and
    /// the buffer reclaimed. 'handle' must be open and have a pin count > 0.
    ///
    /// It is an error to reduce the pin count to 0 if 'client' does not have an associated
    /// FileGroup.
    void Unpin(ClientHandle* client, PageHandle* handle);

    /// Destroy the page referenced by 'handle' (if 'handle' is open). Any buffers or disk
    /// storage backing the page are freed. Idempotent. If the page is pinned, the
    /// reservation usage is decreased accordingly.
    void DestroyPage(ClientHandle* client, PageHandle* handle);

    /// Extracts buffer from a pinned page. After this returns, the page referenced by
    /// 'page_handle' will be destroyed and 'buffer_handle' will reference the buffer from
    /// 'page_handle'. This may decrease reservation usage of 'client' if the page was
    /// pinned multiple times via 'page_handle'. May return an error if 'page_handle' was
    /// unpinned earlier with no subsequent GetBuffer() call and a read error is
    /// encountered while bringing the page back into memory.
    Status ExtractBuffer(ClientHandle* client, PageHandle* page_handle,
                         BufferHandle* buffer_handle) WARN_UNUSED_RESULT;

    /// Allocates a new buffer of 'len' bytes. Uses reservation from 'client'. The caller
    /// is responsible for ensuring it has enough unused reservation before calling
    /// AllocateBuffer() (otherwise it will DCHECK). AllocateBuffer() only fails when
    /// a system error prevents the buffer pool from fulfilling the reservation.
    Status AllocateBuffer(ClientHandle* client, int64_t len,
                          BufferHandle* handle) WARN_UNUSED_RESULT;

    /// If 'handle' is open, close 'handle', free the buffer and decrease the reservation
    /// usage from 'client'. Idempotent. Safe to call concurrently with any other
    /// operations for 'client'.
    void FreeBuffer(ClientHandle* client, BufferHandle* handle);

    /// Transfer ownership of buffer from 'src_client' to 'dst_client' and move the
    /// handle from 'src' to 'dst'. Increases reservation usage in 'dst_client' and
    /// decreases reservation usage in 'src_client'. 'src' must be open and 'dst' must be
    /// closed before calling. 'src'/'dst' and 'src_client'/'dst_client' must be different.
    /// After a successful call, 'src' is closed and 'dst' is open. Safe to call
    /// concurrently with any other operations for 'src_client'.
    Status TransferBuffer(ClientHandle* src_client, BufferHandle* src, ClientHandle* dst_client,
                          BufferHandle* dst) WARN_UNUSED_RESULT;

    /// Try to release at least 'bytes_to_free' bytes of memory to the system allocator.
    /// TODO: once IMPALA-4834 is done and all large allocations are served from the buffer
    /// pool, this may not be necessary.
    void ReleaseMemory(int64_t bytes_to_free);

    /// Called periodically by a maintenance thread to release unused memory back to the
    /// system allocator.
    void Maintenance();

    /// Print a debug string with the state of the buffer pool.
    std::string DebugString();

    int64_t min_buffer_len() const { return min_buffer_len_; }
    int64_t GetSystemBytesLimit() const;
    int64_t GetSystemBytesAllocated() const;

    /// Return the limit on bytes of clean pages in the pool.
    int64_t GetCleanPageBytesLimit() const;

    /// Return the total number of clean pages in the pool.
    int64_t GetNumCleanPages() const;

    /// Return the total bytes of clean pages in the pool.
    int64_t GetCleanPageBytes() const;

    /// Return the total number of free buffers in the pool.
    int64_t GetNumFreeBuffers() const;

    /// Return the total bytes of free buffers in the pool.
    int64_t GetFreeBufferBytes() const;

    /// Generous upper bounds on page and buffer size and the number of different
    /// power-of-two buffer sizes.
    static constexpr int LOG_MAX_BUFFER_BYTES = 48;
    static constexpr int64_t MAX_BUFFER_BYTES = 1L << LOG_MAX_BUFFER_BYTES;

protected:
    friend class BufferPoolTest;
    /// Test helper: get a reference to the allocator.
    BufferAllocator* allocator() { return allocator_.get(); }

private:
    DISALLOW_COPY_AND_ASSIGN(BufferPool);
    class Client;
    class FreeBufferArena;
    class PageList;
    class Page;

    /// Allocator for allocating and freeing all buffer memory and managing lists of free
    /// buffers and clean pages.
    std::unique_ptr<BufferAllocator> allocator_;

    /// The minimum length of a buffer in bytes. All buffers and pages are a power-of-two
    /// multiple of this length. This is always a power of two.
    const int64_t min_buffer_len_;
};

/// External representation of a client of the BufferPool. Clients are used for
/// reservation accounting, and will be used in the future for tracking per-client
/// buffer pool counters. This class is the external handle for a client so
/// each Client instance is owned by the BufferPool's client, rather than the BufferPool.
/// Each Client should only be used by a single thread at a time: concurrently calling
/// Client methods or BufferPool methods with the Client as an argument is not supported.
class BufferPool::ClientHandle {
public:
    ClientHandle() : impl_(nullptr) {}
    /// Client must be deregistered.
    ~ClientHandle() { DCHECK(!is_registered()); }

    /// Request to increase reservation for this client by 'bytes' by calling
    /// ReservationTracker::IncreaseReservation(). Returns true if the reservation was
    /// successfully increased.
    bool IncreaseReservation(int64_t bytes) WARN_UNUSED_RESULT;

    /// Tries to ensure that 'bytes' of unused reservation is available for this client
    /// to use by calling ReservationTracker::IncreaseReservationToFit(). Returns true
    /// if successful, after which 'bytes' can be used.
    bool IncreaseReservationToFit(int64_t bytes) WARN_UNUSED_RESULT;

    /// Try to decrease this client's reservation down to a minimum of 'target_bytes' by
    /// releasing unused reservation to ancestor ReservationTrackers, all the way up to
    /// the root of the ReservationTracker tree. May block waiting for unpinned pages to
    /// be flushed. This client's reservation must be at least 'target_bytes' before
    /// calling this method. May fail if decreasing the reservation requires flushing
    /// unpinned pages to disk and a write to disk fails.
    Status DecreaseReservationTo(int64_t target_bytes) WARN_UNUSED_RESULT;

    /// Move some of this client's reservation to the SubReservation. 'bytes' of unused
    /// reservation must be available in this tracker.
    void SaveReservation(SubReservation* dst, int64_t bytes);

    /// Move some of src's reservation to this client. 'bytes' of unused reservation must be
    /// available in 'src'.
    void RestoreReservation(SubReservation* src, int64_t bytes);

    /// Accessors for this client's reservation corresponding to the identically-named
    /// methods in ReservationTracker.
    int64_t GetReservation() const;
    int64_t GetUsedReservation() const;
    int64_t GetUnusedReservation() const;

    /// Try to transfer 'bytes' of reservation from 'src' to this client using
    /// ReservationTracker::TransferReservationTo().
    bool TransferReservationFrom(ReservationTracker* src, int64_t bytes);

    /// Transfer 'bytes' of reservation from this client to 'dst' using
    /// ReservationTracker::TransferReservationTo().
    bool TransferReservationTo(ReservationTracker* dst, int64_t bytes);

    /// Call SetDebugDenyIncreaseReservation() on this client's ReservationTracker.
    void SetDebugDenyIncreaseReservation(double probability);

    bool is_registered() const { return impl_ != nullptr; }

    /// Return true if there are any unpinned pages for this client.
    bool has_unpinned_pages() const;

    std::string DebugString() const;

private:
    friend class BufferPool;
    friend class BufferPoolTest;
    friend class SubReservation;
    DISALLOW_COPY_AND_ASSIGN(ClientHandle);

    /// Internal state for the client. nullptr means the client isn't registered.
    /// Owned by BufferPool.
    Client* impl_;
};

/// Helper class that allows dividing up a client's reservation into separate buckets.
class BufferPool::SubReservation {
public:
    SubReservation(ClientHandle* client);
    ~SubReservation();

    /// Returns the amount of reservation stored in this sub-reservation.
    int64_t GetReservation() const;

    /// Releases the sub-reservation to the client's tracker. Must be called before
    /// destruction.
    void Close();

    bool is_closed() const { return tracker_ == nullptr; }

private:
    friend class BufferPool::ClientHandle;
    DISALLOW_COPY_AND_ASSIGN(SubReservation);

    /// Child of the client's tracker used to track the sub-reservation. Usage is not
    /// tracked against this tracker - instead the reservation is always transferred back
    /// to the client's tracker before use.
    std::unique_ptr<ReservationTracker> tracker_;
};

/// A handle to a buffer allocated from the buffer pool. Each BufferHandle should only
/// be used by a single thread at a time: concurrently calling BufferHandle methods or
/// BufferPool methods with the BufferHandle as an argument is not supported.
class BufferPool::BufferHandle {
public:
    BufferHandle() { Reset(); }
    ~BufferHandle() { DCHECK(!is_open()); }

    /// Allow move construction of handles to support std::move(). Inline to make moving
    /// efficient.
    BufferHandle(BufferHandle&& src);

    /// Allow move assignment of handles to support STL classes like std::vector.
    /// Destination must be uninitialized. Inline to make moving efficient.
    BufferHandle& operator=(BufferHandle&& src);

    bool is_open() const { return data_ != nullptr; }
    int64_t len() const {
        DCHECK(is_open());
        return len_;
    }
    /// Get a pointer to the start of the buffer.
    uint8_t* data() const {
        DCHECK(is_open());
        return data_;
    }

    MemRange mem_range() const { return MemRange(data(), len()); }

    std::string DebugString() const;

    /// Poison the memory associated with this handle. If ASAN is not enabled, this is a
    /// no-op.
    void Poison() { ASAN_POISON_MEMORY_REGION(data(), len()); }

    /// Unpoison the memory associated with this handle. If ASAN is not enabled, this is a
    /// no-op.
    void Unpoison() { ASAN_UNPOISON_MEMORY_REGION(data(), len()); }

private:
    DISALLOW_COPY_AND_ASSIGN(BufferHandle);
    friend class BufferPool;
    friend class SystemAllocator;

    /// Internal helper to set the handle to an opened state.
    void Open(uint8_t* data, int64_t len, int home_core);

    /// Internal helper to reset the handle to an unopened state. Inlined to make moving
    /// efficient.
    void Reset();

    /// The client the buffer handle belongs to, used to validate that the correct client
    /// is provided in BufferPool method calls. Set to nullptr if the buffer is in a free list.
    const ClientHandle* client_;

    /// Pointer to the start of the buffer. Non-nullptr if open, nullptr if closed.
    uint8_t* data_;

    /// Length of the buffer in bytes.
    int64_t len_;

    /// The CPU core that the buffer was allocated from - used to determine which arena
    /// it will be added to.
    int home_core_;
};

/// The handle for a page used by clients of the BufferPool. Each PageHandle should
/// only be used by a single thread at a time: concurrently calling PageHandle methods
/// or BufferPool methods with the PageHandle as an argument is not supported.
class BufferPool::PageHandle {
public:
    PageHandle();
    ~PageHandle() { DCHECK(!is_open()); }

    // Allow move construction of page handles, to support std::move().
    PageHandle(PageHandle&& src);

    // Allow move assignment of page handles, to support STL classes like std::vector.
    // Destination must be closed.
    PageHandle& operator=(PageHandle&& src);

    bool is_open() const { return page_ != nullptr; }
    bool is_pinned() const { return pin_count() > 0; }
    int pin_count() const;
    int64_t len() const;

    /// Get a reference to the page's buffer handle. Only valid to call if the page is
    /// pinned. If the page was previously unpinned and the read I/O for the data is still
    /// in flight, this can block waiting. Returns an error if an error was encountered
    /// reading the data back, which can only happen if Unpin() was called on the page
    /// since the last call to GetBuffer(). Only const accessors of the returned handle can
    /// be used: it is invalid to call FreeBuffer() or TransferBuffer() on it or to
    /// otherwise modify the handle.
    Status GetBuffer(const BufferHandle** buffer_handle) const WARN_UNUSED_RESULT;

    std::string DebugString() const;

private:
    DISALLOW_COPY_AND_ASSIGN(PageHandle);
    friend class BufferPool;
    friend class BufferPoolTest;
    friend class Page;

    /// Internal helper to open the handle for the given page.
    void Open(Page* page, ClientHandle* client);

    /// Internal helper to reset the handle to an unopened state.
    void Reset();

    /// The internal page structure. nullptr if the handle is not open.
    Page* page_;

    /// The client the page handle belongs to.
    ClientHandle* client_;
};

inline BufferPool::BufferHandle::BufferHandle(BufferHandle&& src) {
    Reset();
    *this = std::move(src);
}

inline BufferPool::BufferHandle& BufferPool::BufferHandle::operator=(BufferHandle&& src) {
    DCHECK(!is_open());
    // Copy over all members then close src.
    client_ = src.client_;
    data_ = src.data_;
    len_ = src.len_;
    home_core_ = src.home_core_;
    src.Reset();
    return *this;
}

inline void BufferPool::BufferHandle::Reset() {
    client_ = nullptr;
    data_ = nullptr;
    len_ = -1;
    home_core_ = -1;
}
} // namespace doris

#endif
