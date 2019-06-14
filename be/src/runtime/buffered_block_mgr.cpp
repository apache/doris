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

#include "runtime/buffered_block_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"
#include "runtime/mem_tracker.h"

namespace doris {

//BufferedBlockMgr::BlockMgrsMap BufferedBlockMgr::query_to_block_mgrs_;
//SpinLock BufferedBlockMgr::static_block_mgrs_lock_;

// BufferedBlockMgr::Block methods.
BufferedBlockMgr::Block::Block()
    : _buffer_desc(NULL),
      // _block_mgr(block_mgr),
      _valid_data_len(0),
      _num_rows(0)
{
}

Status BufferedBlockMgr::create(RuntimeState* state,
        int64_t block_size, boost::shared_ptr<BufferedBlockMgr>* block_mgr) {
    block_mgr->reset(new BufferedBlockMgr(state, block_size));
    (*block_mgr)->init(state);
    return Status::OK();
}

void BufferedBlockMgr::init(RuntimeState* state) {
    _state = state;
    _tuple_pool.reset(new MemPool(state->instance_mem_tracker()));
}

void BufferedBlockMgr::Block::init() {
    _valid_data_len = 0;
    _num_rows = 0;
}

BufferedBlockMgr::BufferedBlockMgr(RuntimeState* state, int64_t block_size)
    : _max_block_size(block_size)
{
}

Status BufferedBlockMgr::get_new_block(Block** block, int64_t len) {
    DCHECK_LE(len, _max_block_size) << "Cannot request blocks bigger than max_len";

    *block = NULL;
    Block* new_block = _obj_pool.add(new Block());
    if (UNLIKELY(new_block == NULL)) {
        return Status::InternalError("Allocate memory failed.");
    }

    uint8_t* buffer = _tuple_pool->allocate(len);
    if (UNLIKELY(buffer == NULL)) {
        return Status::InternalError("Allocate memory failed.");
    }

    //new_block->set_buffer_desc(_obj_pool.Add(new BufferDescriptor(buffer, len));
    new_block->_buffer_desc = _obj_pool.add(new BufferDescriptor(buffer, len));
    new_block->_buffer_desc->block = new_block;
    *block = new_block;

    if (UNLIKELY(_state->instance_mem_tracker()->any_limit_exceeded())) {
        return Status::MemoryLimitExceeded("Memory limit exceeded");
    }

    return Status::OK();
}

//Status BufferedBlockMgr::Block::delete() {
  //// TODO: delete block or not,we should delete the new BLOCK
  //return Status::OK();
//}

Status BufferedBlockMgr::get_new_block(Block** block) {
    return get_new_block(block, _max_block_size);
}

} // namespace doris
