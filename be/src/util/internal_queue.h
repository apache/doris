// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_UTIL_INTERNAL_QUEUE_H
#define BDG_PALO_BE_SRC_UTIL_INTERNAL_QUEUE_H

#include <boost/thread/locks.hpp>

#include "common/atomic.h"
#include "util/spinlock.h"

namespace palo {

// Thread safe fifo-queue. This is an internal queue, meaning the links to nodes
// are maintained in the object itself. This is in contrast to the stl list which
// allocates a wrapper Node object around the data. Since it's an internal queue,
// the list pointers are maintained in the Nodes which is memory owned by the user.
// The nodes cannot be deallocated while the queue has elements.
// To use: subclass InternalQueue::Node.
// The internal structure is a doubly-linked list.
//  NULL <-- N1 <--> N2 <--> N3 --> NULL
//          (head)          (tail)
// TODO: this is an ideal candidate to be made lock free.

// T must be a subclass of InternalQueue::Node
template<typename T>
class InternalQueue {
public:
    class Node {
    public:
        Node() : _parent_queue(NULL), _next(NULL), _prev(NULL) {}
        virtual ~Node() {}

        // Returns the Next/Prev node or NULL if this is the end/front.
        T* next() const {
            boost::lock_guard<SpinLock> lock(_parent_queue->_lock);
            return reinterpret_cast<T*>(_next);
        }
        T* prev() const {
            boost::lock_guard<SpinLock> lock(_parent_queue->_lock);
            return reinterpret_cast<T*>(_prev);
        }

    private:
        friend class InternalQueue;

        // Pointer to the queue this Node is on. NULL if not on any queue.
        InternalQueue* _parent_queue;
        Node* _next;
        Node* _prev;
    };

    InternalQueue() : _head(NULL), _tail(NULL), _size(0) {}

    ~InternalQueue() {
        // do nothing
    }

    // Returns the element at the head of the list without dequeuing or NULL
    // if the queue is empty. This is O(1).
    T* head() const {
        boost::lock_guard<SpinLock> lock(_lock);
        if (empty()) {
            return NULL;
        }
        return reinterpret_cast<T*>(_head);
    }

    // Returns the element at the end of the list without dequeuing or NULL
    // if the queue is empty. This is O(1).
    T* tail() {
        boost::lock_guard<SpinLock> lock(_lock);
        if (empty()) {
            return NULL;
        }
        return reinterpret_cast<T*>(_tail);
    }

    // Enqueue node onto the queue's tail. This is O(1).
    void enqueue(T* n) {
        Node* node = (Node*)n;
        DCHECK(node->_next == NULL);
        DCHECK(node->_prev == NULL);
        DCHECK(node->_parent_queue == NULL);
        node->_parent_queue = this;
        {
            boost::lock_guard<SpinLock> lock(_lock);
            if (_tail != NULL) {
                _tail->_next = node;
            }
            node->_prev = _tail;
            _tail = node;
            if (_head == NULL) {
                _head = node;
            }
            ++_size;
        }
    }

    // Dequeues an element from the queue's head. Returns NULL if the queue
    // is empty. This is O(1).
    T* dequeue() {
        Node* result = NULL;
        {
            boost::lock_guard<SpinLock> lock(_lock);
            if (empty()) {
                return NULL;
            }
            --_size;
            result = _head;
            _head = _head->_next;
            if (_head == NULL) {
                _tail = NULL;
            } else {
                _head->_prev = NULL;
            }
        }
        DCHECK(result != NULL);
        result->_next = result->_prev = NULL;
        result->_parent_queue = NULL;
        return reinterpret_cast<T*>(result);
    }

    // Dequeues an element from the queue's tail. Returns NULL if the queue
    // is empty. This is O(1).
    T* pop_back() {
        Node* result = NULL;
        {
            boost::lock_guard<SpinLock> lock(_lock);
            if (empty()) {
                return NULL;
            }
            --_size;
            result = _tail;
            _tail = _tail->_prev;
            if (_tail == NULL) {
                _head = NULL;
            } else {
                _tail->_next = NULL;
            }
        }
        DCHECK(result != NULL);
        result->_next = result->_prev = NULL;
        result->_parent_queue = NULL;
        return reinterpret_cast<T*>(result);
    }

    // Removes 'node' from the queue. This is O(1). No-op if node is
    // not on the list.
    void remove(T* n) {
        Node* node = (Node*)n;
        if (node->_parent_queue == NULL) {
            return;
        }
        DCHECK(node->_parent_queue == this);
        {
            boost::lock_guard<SpinLock> lock(_lock);
            if (node->_next == NULL && node->_prev == NULL) {
                // Removing only node
                DCHECK(node == _head);
                DCHECK(_tail == node);
                _head = _tail = NULL;
                --_size;
                node->_parent_queue = NULL;
                return;
            }

            if (_head == node) {
                DCHECK(node->_prev == NULL);
                _head = node->_next;
            } else {
                DCHECK(node->_prev != NULL);
                node->_prev->_next = node->_next;
            }

            if (node == _tail) {
                DCHECK(node->_next == NULL);
                _tail = node->_prev;
            } else if (node->_next != NULL) {
                node->_next->_prev = node->_prev;
            }
            --_size;
        }
        node->_next = node->_prev = NULL;
        node->_parent_queue = NULL;
    }

    // Clears all elements in the list.
    void clear() {
        boost::lock_guard<SpinLock> lock(_lock);
        Node* cur = _head;
        while (cur != NULL) {
            Node* tmp = cur;
            cur = cur->_next;
            tmp->_prev = tmp->_next = NULL;
            tmp->_parent_queue = NULL;
        }
        _size = 0;
        _head = _tail = NULL;
    }

    int size() const {
        return _size;
    }
    bool empty() const {
        return _head == NULL;
    }

    // Returns if the target is on the queue. This is O(1) and intended to
    // be used for debugging.
    bool contains(const T* target) const {
        return target->_parent_queue == this;
    }

    // Validates the internal structure of the list
    bool validate() {
        int num_elements_found = 0;
        boost::lock_guard<SpinLock> lock(_lock);
        if (_head == NULL) {
            if (_tail != NULL) return false;
            if (size() != 0) return false;
            return true;
        }

        if (_head->_prev != NULL) return false;
        Node* current = _head;
        while (current != NULL) {
            if (current->_parent_queue != this) return false;
            ++num_elements_found;
            Node* next = current->_next;
            if (next == NULL) {
                if (current != _tail) return false;
            } else {
                if (next->_prev != current) return false;
            }
            current = next;
        }
        if (num_elements_found != size()) return false;
        return true;
    }

    // Prints the queue ptrs to a string.
    std::string debug_string() {
        std::stringstream ss;
        ss << "(";
        {
            boost::lock_guard<SpinLock> lock(_lock);
            Node* curr = _head;
            while (curr != NULL) {
                ss << (void*)curr;
                curr = curr->_next;
            }
        }
        ss << ")";
        return ss.str();
    }

private:
    friend struct Node;
    mutable SpinLock _lock;
    Node* _head;
    Node* _tail;
    int _size;
};

} // end namespace palo

#endif // BDG_PALO_BE_SRC_UTIL_INTERNAL_QUEUE_H

