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

#include "runtime/bufferpool/suballocator.h"

#include <new>

#include "runtime/bufferpool/reservation_tracker.h"
#include "util/bit_util.h"

#include "common/names.h"
#include "gutil/strings/substitute.h"

namespace doris {

constexpr int Suballocator::LOG_MAX_ALLOCATION_BYTES;
constexpr int64_t Suballocator::MAX_ALLOCATION_BYTES;
constexpr int Suballocator::LOG_MIN_ALLOCATION_BYTES;
constexpr int64_t Suballocator::MIN_ALLOCATION_BYTES;
//const int Suballocator::NUM_FREE_LISTS;

Suballocator::Suballocator(
    BufferPool* pool, BufferPool::ClientHandle* client, int64_t min_buffer_len)
  : pool_(pool), client_(client), min_buffer_len_(min_buffer_len), allocated_(0) {}

Suballocator::~Suballocator() {
  // All allocations should be free and buffers deallocated.
  DCHECK_EQ(allocated_, 0);
  for (int i = 0; i < NUM_FREE_LISTS; ++i) {
    DCHECK(free_lists_[i] == nullptr);
  }
}

Status Suballocator::Allocate(int64_t bytes, unique_ptr<Suballocation>* result) {
  DCHECK_GE(bytes, 0);
  if (UNLIKELY(bytes > MAX_ALLOCATION_BYTES)) {
    std::stringstream err_stream;
    err_stream << "Requested memory allocation of "  << bytes
               << " bytes, larger than max " << "supported of " << MAX_ALLOCATION_BYTES
               << " bytes";
    return Status::InternalError(err_stream.str());
  }
  unique_ptr<Suballocation> free_node;
  bytes = max(bytes, MIN_ALLOCATION_BYTES);
  const int target_list_idx = ComputeListIndex(bytes);
  for (int i = target_list_idx; i < NUM_FREE_LISTS; ++i) {
    free_node = PopFreeListHead(i);
    if (free_node != nullptr) break;
  }

  if (free_node == nullptr) {
    // Unable to find free allocation, need to get more memory from buffer pool.
    RETURN_IF_ERROR(AllocateBuffer(bytes, &free_node));
    if (free_node == nullptr) {
      *result = nullptr;
      return Status::OK();
    }
  }

  // Free node may be larger than required.
  const int free_list_idx = ComputeListIndex(free_node->len_);
  if (free_list_idx != target_list_idx) {
    RETURN_IF_ERROR(SplitToSize(move(free_node), bytes, &free_node));
    DCHECK(free_node != nullptr);
  }

  free_node->in_use_ = true;
  allocated_ += free_node->len_;
  *result = move(free_node);
  return Status::OK();
}

int Suballocator::ComputeListIndex(int64_t bytes) const {
  return BitUtil::Log2CeilingNonZero64(bytes) - LOG_MIN_ALLOCATION_BYTES;
}

Status Suballocator::AllocateBuffer(int64_t bytes, unique_ptr<Suballocation>* result) {
  DCHECK_LE(bytes, MAX_ALLOCATION_BYTES);
  const int64_t buffer_len = max(min_buffer_len_, BitUtil::RoundUpToPowerOfTwo(bytes));
  if (!client_->IncreaseReservationToFit(buffer_len)) {
    *result = nullptr;
    return Status::OK();
  }

  unique_ptr<Suballocation> free_node;
  RETURN_IF_ERROR(Suballocation::Create(&free_node));
  RETURN_IF_ERROR(pool_->AllocateBuffer(client_, buffer_len, &free_node->buffer_));

  free_node->data_ = free_node->buffer_.data();
  free_node->len_ = buffer_len;
  *result = move(free_node);
  return Status::OK();
}

Status Suballocator::SplitToSize(unique_ptr<Suballocation> free_node,
    int64_t target_bytes, unique_ptr<Suballocation>* result) {
  DCHECK(!free_node->in_use_);
  DCHECK_GT(free_node->len_, target_bytes);

  const int free_list_idx = ComputeListIndex(free_node->len_);
  const int target_list_idx = ComputeListIndex(target_bytes);

  // Preallocate nodes to avoid handling allocation failures during splitting.
  // Need two nodes per level for the left and right children.
  const int num_nodes = (free_list_idx - target_list_idx) * 2;
  constexpr int MAX_NUM_NODES = NUM_FREE_LISTS * 2;
  unique_ptr<Suballocation> nodes[MAX_NUM_NODES];
  for (int i = 0; i < num_nodes; ++i) {
    if (!Suballocation::Create(&nodes[i]).ok()) {
      // Add the free node to the free list to restore the allocator to an internally
      // consistent state.
      AddToFreeList(move(free_node));
      return Status::InternalError("Failed to allocate list node in Suballocator");
    }
  }

  // Iteratively split from the current size down to the target size. We will return
  // the leftmost descendant node.
  int next_node = 0;
  for (int i = free_list_idx; i > target_list_idx; --i) {
    DCHECK_EQ(free_node->len_, 1LL << (i + LOG_MIN_ALLOCATION_BYTES));
    unique_ptr<Suballocation> left_child = move(nodes[next_node++]);
    unique_ptr<Suballocation> right_child = move(nodes[next_node++]);
    DCHECK_LE(next_node, num_nodes);

    const int64_t child_len = free_node->len_ / 2;
    left_child->data_ = free_node->data_;
    right_child->data_ = free_node->data_ + child_len;
    left_child->len_ = right_child->len_ = child_len;
    left_child->buddy_ = right_child.get();
    right_child->buddy_ = left_child.get();
    free_node->in_use_ = true;
    left_child->parent_ = move(free_node);

    AddToFreeList(move(right_child));
    free_node = move(left_child);
  }
  *result = move(free_node);
  return Status::OK();
}

void Suballocator::Free(unique_ptr<Suballocation> allocation) {
  if (allocation == nullptr) return;

  DCHECK(allocation->in_use_);
  allocation->in_use_ = false;
  allocated_ -= allocation->len_;

  // Iteratively coalesce buddies until the buddy is in use or we get to the root.
  // This ensures that all buddies in the free lists are coalesced. I.e. we do not
  // have two buddies in the same free list.
  unique_ptr<Suballocation> curr_allocation = move(allocation);
  while (curr_allocation->buddy_ != nullptr) {
    if (curr_allocation->buddy_->in_use_) {
      // If the buddy is not free we can't coalesce, just add it to free list.
      AddToFreeList(move(curr_allocation));
      return;
    }
    unique_ptr<Suballocation> buddy = RemoveFromFreeList(curr_allocation->buddy_);
    curr_allocation = CoalesceBuddies(move(curr_allocation), move(buddy));
  }

  // Reached root, which is an entire free buffer. We are not using it, so free up memory.
  DCHECK(curr_allocation->buffer_.is_open());
  pool_->FreeBuffer(client_, &curr_allocation->buffer_);
  curr_allocation.reset();
}

void Suballocator::AddToFreeList(unique_ptr<Suballocation> node) {
  DCHECK(!node->in_use_);
  int list_idx = ComputeListIndex(node->len_);
  if (free_lists_[list_idx] != nullptr) {
    free_lists_[list_idx]->prev_free_ = node.get();
  }
  node->next_free_ = move(free_lists_[list_idx]);
  DCHECK(node->prev_free_ == nullptr);
  free_lists_[list_idx] = move(node);
}

unique_ptr<Suballocation> Suballocator::RemoveFromFreeList(Suballocation* node) {
  DCHECK(node != nullptr);
  const int list_idx = ComputeListIndex(node->len_);

  if (node->next_free_ != nullptr) {
    node->next_free_->prev_free_ = node->prev_free_;
  }

  unique_ptr<Suballocation>* ptr_from_prev = node->prev_free_ == nullptr ?
      &free_lists_[list_idx] :
      &node->prev_free_->next_free_;
  node->prev_free_ = nullptr;
  unique_ptr<Suballocation> result = move(*ptr_from_prev);
  *ptr_from_prev = move(node->next_free_);
  return result;
}

unique_ptr<Suballocation> Suballocator::PopFreeListHead(int list_idx) {
  if (free_lists_[list_idx] == nullptr) return nullptr;
  unique_ptr<Suballocation> result = move(free_lists_[list_idx]);
  DCHECK(result->prev_free_ == nullptr);
  if (result->next_free_ != nullptr) {
    result->next_free_->prev_free_ = nullptr;
  }
  free_lists_[list_idx] = move(result->next_free_);
  return result;
}

unique_ptr<Suballocation> Suballocator::CoalesceBuddies(
    unique_ptr<Suballocation> b1, unique_ptr<Suballocation> b2) {
  DCHECK(!b1->in_use_);
  DCHECK(!b2->in_use_);
  DCHECK_EQ(b1->buddy_, b2.get());
  DCHECK_EQ(b2->buddy_, b1.get());
  // Only the left child's parent should be present.
  DCHECK((b1->parent_ != nullptr) ^ (b2->parent_ != nullptr));
  unique_ptr<Suballocation> parent =
      b1->parent_ != nullptr ? move(b1->parent_) : move(b2->parent_);
  parent->in_use_ = false;
  return parent;
}

Status Suballocation::Create(unique_ptr<Suballocation>* new_suballocation) {
  // Allocate from system allocator for simplicity. We don't expect this to be
  // performance critical or to be used for small allocations where CPU/memory
  // overhead of these allocations might be a consideration.
  new_suballocation->reset(new (nothrow) Suballocation());
  if (*new_suballocation == nullptr) {
    return Status::MemoryAllocFailed("allocate memory failed");
  }
  return Status::OK();
}
}
