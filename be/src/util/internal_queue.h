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

#ifndef DORIS_BE_SRC_UTIL_INTERNAL_QUEUE_H
#define DORIS_BE_SRC_UTIL_INTERNAL_QUEUE_H

#include <boost/function.hpp>
#include <boost/thread/locks.hpp>

#include "util/fake_lock.h"
#include "util/spinlock.h"

namespace doris {

/// FIFO queue implemented as a doubly-linked lists with internal pointers. This is in
/// contrast to the STL list which allocates a wrapper Node object around the data. Since
/// it's an internal queue, the list pointers are maintained in the Nodes which is memory
/// owned by the user. The nodes cannot be deallocated while the queue has elements.
/// The internal structure is a doubly-linked list.
///  NULL <-- N1 <--> N2 <--> N3 --> NULL
///          (head)          (tail)
///
/// InternalQueue<T> instantiates a thread-safe queue where the queue is protected by an
/// internal Spinlock. InternalList<T> instantiates a list with no thread safety.
///
/// To use these data structures, the element to be added to the queue or list must
/// subclass ::Node.
///
/// TODO: this is an ideal candidate to be made lock free.

/// T must be a subclass of InternalQueueBase::Node.
template <typename LockType, typename T>
class InternalQueueBase {
public:
    struct Node {
    public:
        Node() : parent_queue(NULL), next_node(NULL), prev_node(NULL) {}
        virtual ~Node() {}

        /// Returns true if the node is in a queue.
        bool in_queue() const { return parent_queue != NULL; }

        /// Returns the Next/Prev node or NULL if this is the end/front.
        T* next() const {
            boost::lock_guard<LockType> lock(parent_queue->lock_);
            return reinterpret_cast<T*>(next_node);
        }
        T* prev() const {
            boost::lock_guard<LockType> lock(parent_queue->lock_);
            return reinterpret_cast<T*>(prev_node);
        }

    private:
        friend class InternalQueueBase<LockType, T>;

        /// Pointer to the queue this Node is on. NULL if not on any queue.
        InternalQueueBase<LockType, T>* parent_queue;
        Node* next_node;
        Node* prev_node;
    };

    InternalQueueBase() : head_(NULL), tail_(NULL), size_(0) {}

    /// Returns the element at the head of the list without dequeuing or NULL
    /// if the queue is empty. This is O(1).
    T* head() const {
        boost::lock_guard<LockType> lock(lock_);
        if (empty()) return NULL;
        return reinterpret_cast<T*>(head_);
    }

    /// Returns the element at the end of the list without dequeuing or NULL
    /// if the queue is empty. This is O(1).
    T* tail() {
        boost::lock_guard<LockType> lock(lock_);
        if (empty()) return NULL;
        return reinterpret_cast<T*>(tail_);
    }

    /// Enqueue node onto the queue's tail. This is O(1).
    void enqueue(T* n) {
        Node* node = (Node*)n;
        DCHECK(node->next_node == NULL);
        DCHECK(node->prev_node == NULL);
        DCHECK(node->parent_queue == NULL);
        node->parent_queue = this;
        {
            boost::lock_guard<LockType> lock(lock_);
            if (tail_ != NULL) tail_->next_node = node;
            node->prev_node = tail_;
            tail_ = node;
            if (head_ == NULL) head_ = node;
            ++size_;
        }
    }

    /// Dequeues an element from the queue's head. Returns NULL if the queue
    /// is empty. This is O(1).
    T* dequeue() {
        Node* result = NULL;
        {
            boost::lock_guard<LockType> lock(lock_);
            if (empty()) return NULL;
            --size_;
            result = head_;
            head_ = head_->next_node;
            if (head_ == NULL) {
                tail_ = NULL;
            } else {
                head_->prev_node = NULL;
            }
        }
        DCHECK(result != NULL);
        result->next_node = result->prev_node = NULL;
        result->parent_queue = NULL;
        return reinterpret_cast<T*>(result);
    }

    /// Dequeues an element from the queue's tail. Returns NULL if the queue
    /// is empty. This is O(1).
    T* pop_back() {
        Node* result = NULL;
        {
            boost::lock_guard<LockType> lock(lock_);
            if (empty()) return NULL;
            --size_;
            result = tail_;
            tail_ = tail_->prev_node;
            if (tail_ == NULL) {
                head_ = NULL;
            } else {
                tail_->next_node = NULL;
            }
        }
        DCHECK(result != NULL);
        result->next_node = result->prev_node = NULL;
        result->parent_queue = NULL;
        return reinterpret_cast<T*>(result);
    }

    /// Removes 'node' from the queue. This is O(1). No-op if node is
    /// not on the list. Returns true if removed
    bool remove(T* n) {
        Node* node = (Node*)n;
        if (node->parent_queue != this) return false;
        {
            boost::lock_guard<LockType> lock(lock_);
            if (node->next_node == NULL && node->prev_node == NULL) {
                // Removing only node
                DCHECK(node == head_);
                DCHECK(tail_ == node);
                head_ = tail_ = NULL;
                --size_;
                node->parent_queue = NULL;
                return true;
            }

            if (head_ == node) {
                DCHECK(node->prev_node == NULL);
                head_ = node->next_node;
            } else {
                DCHECK(node->prev_node != NULL);
                node->prev_node->next_node = node->next_node;
            }

            if (node == tail_) {
                DCHECK(node->next_node == NULL);
                tail_ = node->prev_node;
            } else if (node->next_node != NULL) {
                node->next_node->prev_node = node->prev_node;
            }
            --size_;
        }
        node->next_node = node->prev_node = NULL;
        node->parent_queue = NULL;
        return true;
    }

    /// Clears all elements in the list.
    void clear() {
        boost::lock_guard<LockType> lock(lock_);
        Node* cur = head_;
        while (cur != NULL) {
            Node* tmp = cur;
            cur = cur->next_node;
            tmp->prev_node = tmp->next_node = NULL;
            tmp->parent_queue = NULL;
        }
        size_ = 0;
        head_ = tail_ = NULL;
    }

    int size() const { return size_; }
    bool empty() const { return head_ == NULL; }

    /// Returns if the target is on the queue. This is O(1) and does not acquire any locks.
    bool contains(const T* target) const { return target->parent_queue == this; }

    /// Validates the internal structure of the list
    bool validate() {
        int num_elements_found = 0;
        boost::lock_guard<LockType> lock(lock_);
        if (head_ == NULL) {
            if (tail_ != NULL) return false;
            if (size() != 0) return false;
            return true;
        }

        if (head_->prev_node != NULL) return false;
        Node* current = head_;
        while (current != NULL) {
            if (current->parent_queue != this) return false;
            ++num_elements_found;
            Node* next_node = current->next_node;
            if (next_node == NULL) {
                if (current != tail_) return false;
            } else {
                if (next_node->prev_node != current) return false;
            }
            current = next_node;
        }
        if (num_elements_found != size()) return false;
        return true;
    }

    // Iterate over elements of queue, calling 'fn' for each element. If 'fn' returns
    // false, terminate iteration. It is invalid to call other InternalQueue methods
    // from 'fn'.
    void iterate(boost::function<bool(T*)> fn) {
        boost::lock_guard<LockType> lock(lock_);
        for (Node* current = head_; current != NULL; current = current->next_node) {
            if (!fn(reinterpret_cast<T*>(current))) return;
        }
    }

    /// Prints the queue ptrs to a string.
    std::string DebugString() {
        std::stringstream ss;
        ss << "(";
        {
            boost::lock_guard<LockType> lock(lock_);
            Node* curr = head_;
            while (curr != NULL) {
                ss << (void*)curr;
                curr = curr->next_node;
            }
        }
        ss << ")";
        return ss.str();
    }

private:
    friend struct Node;
    mutable LockType lock_;
    Node *head_, *tail_;
    int size_;
};

// The default LockType is SpinLock.
template <typename T>
class InternalQueue : public InternalQueueBase<SpinLock, T> {};

// InternalList is a non-threadsafe implementation.
template <typename T>
class InternalList : public InternalQueueBase<FakeLock, T> {};
} // namespace doris
#endif
