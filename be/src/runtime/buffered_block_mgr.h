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

#ifndef INF_DORIS_QE_SRC_BE_SRC_RUNTIME_BUFFERED_BLOCK_MGR_H
#define INF_DORIS_QE_SRC_BE_SRC_RUNTIME_BUFFERED_BLOCK_MGR_H

#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <common/status.h>
#include <common/object_pool.h>
#include "runtime/mem_pool.h"
#include "common/logging.h"
#include <list>

namespace doris {

class RuntimeState;

class BufferedBlockMgr {
private:
    struct BufferDescriptor;

public:
    class Block { //}: public InternalQueue<Block>::Node {
    public:
        ~Block() {}
        void add_row() {
            ++_num_rows;
        }
        int num_rows() const {
            return _num_rows;
        }

        // Allocates the specified number of bytes from this block.
        template <typename T> T* allocate(int size) {
            DCHECK_GE(bytes_remaining(), size);
            uint8_t* current_location = _buffer_desc->buffer + _valid_data_len;
            _valid_data_len += size;
            return reinterpret_cast<T*>(current_location);
        }

        // Return the number of remaining bytes that can be allocated in this block.
        int bytes_remaining() const {
            DCHECK(_buffer_desc != NULL);
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
            DCHECK(_buffer_desc != NULL);
            return _buffer_desc->buffer;
        }

        // Return the number of bytes allocated in this block.
        int64_t valid_data_len() const {
            return _valid_data_len;
        }

        // Returns the length of the underlying buffer. Only callable if the block is
        // pinned.
        int64_t buffer_len() const {
            return _buffer_desc->len;
        }

        // Returns true if this block is the max block size. Only callable if the block
        // is pinned.
        bool is_max_size() const {
            return _buffer_desc->len == _block_mgr->max_block_size();
        }

        Status delete_block() {
            return Status::OK();
        }

        // Debug helper method to print the state of a block.
        std::string debug_string() const {
            return "";
        }

    private:
        friend class BufferedBlockMgr;

        //Block(BufferedBlockMgr* block_mgr);
        Block();

        void init();

        // Pointer to the buffer associated with the block. NULL if the block is not in
        // memory and cannot be changed while the block is pinned or being written.
        BufferDescriptor* _buffer_desc;

        // Parent block manager object. Responsible for maintaining the state of the block.
        BufferedBlockMgr* _block_mgr;

        // Length of valid (i.e. allocated) data within the block.
        int64_t _valid_data_len;

        // Number of rows in this block.
        int _num_rows;
    }; // class Block

    static Status create(RuntimeState* state,
                         int64_t block_size, boost::shared_ptr<BufferedBlockMgr>* block_mgr);

    ~BufferedBlockMgr() {};

    Status get_new_block(Block** block, int64_t len);

    Status get_new_block(Block** block);
    int64_t max_block_size() const {
        return _max_block_size;
    }

private:

    // Descriptor for a single memory buffer in the pool.
    struct BufferDescriptor { //}: public InternalQueue<BufferDescriptor>::Node {
        // Start of the buffer
        uint8_t* buffer;

        // Length of the buffer
        int64_t len;

        // Block that this buffer is assigned to. May be NULL.
        Block* block;

        // Iterator into all_io_buffers_ for this buffer.
        std::list<BufferDescriptor*>::iterator all_buffers_it;

        BufferDescriptor(uint8_t* buf, int64_t len)
            : buffer(buf), len(len), block(NULL) {
        }
    }; //block

private:
    BufferedBlockMgr(RuntimeState* state, int64_t block_size);
    void init(RuntimeState* state);
    // Size of the largest/default block in bytes.
    const int64_t _max_block_size;
    ObjectPool _obj_pool;
    //MemPool* _tuple_pool;
    boost::scoped_ptr<MemPool> _tuple_pool;
    RuntimeState* _state;

}; // class BufferedBlockMgr

} // namespace doris.

#endif
