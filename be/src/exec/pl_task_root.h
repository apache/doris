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

#pragma once

#include "exec/exec_node.h"

namespace doris {

// Pull load task root
class PlTaskRoot : public ExecNode {
public:
    PlTaskRoot(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual ~PlTaskRoot();

    virtual Status init(const TPlanNode& tnode, RuntimeState* state = nullptr);
    virtual Status prepare(RuntimeState* state);
    virtual Status open(RuntimeState* state);
    virtual Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos);
    virtual Status close(RuntimeState* state);

    // the number of senders needs to be set after the c'tor, because it's not
    // recorded in TPlanNode, and before calling prepare()
    void set_num_senders(int num_senders) { _num_senders = num_senders; }

protected:
    virtual void debug_string(int indentation_level, std::stringstream* out) const;

private:
    int _num_senders; // needed for _stream_recvr construction
};

} // namespace doris
