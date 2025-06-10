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

#include <stdint.h>

#include "common/status.h"
#include "operator.h"
#include "pipeline/dependency.h"

namespace doris {
#include "common/compile_check_begin.h"
class RuntimeState;

namespace pipeline {

// The LocalMergeSortSourceOperatorX is an operator that performs merge sort locally.
// If there is an instance, it will only operate in one instance (referred to as the main source below),
// while the other instances will directly return EOS (referred to as other sources below).
// The LocalMergeSortSourceOperatorX can be seen as a specialized version of the SortSourceOperatorX.

/*                                                                                                                                             
          |                        |                        |               
          |                        |                        |               
          |merge and output        |  eos                   | eos           
          |                        |                        |               
          |                        |                        |               
   +----------------+     +----------------+       +----------------+       
   |local merge sort|     |local merge sort|       |local merge sort|       
   +---------|------+     +----------------+       +----------------+       
 main_source |               other_source               other_source        
 task_id = 0 |               task_id > 0                task_id > 0         
             |                                                              
             |                                                              
             ---------------------+----------------------+                  
             |                    |                      |                  
             |                    |                      |                  
             |                    |                      |                  
       +-----------+        +-----------+          +-----------+            
       | sort sink |        | sort sink |          | sort sink |            
       +-----------+        +-----------+          +-----------+                                                                                                                                                           
*/

class LocalMergeSortSourceOperatorX;

class LocalMergeSortLocalState final : public PipelineXLocalState<SortSharedState> {
public:
    using Base = PipelineXLocalState<SortSharedState>;
    ENABLE_FACTORY_CREATOR(LocalMergeSortLocalState);
    LocalMergeSortLocalState(RuntimeState* state, OperatorXBase* parent);
    ~LocalMergeSortLocalState() override = default;
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    std::vector<Dependency*> dependencies() const override;

private:
    Status build_merger(RuntimeState* state);

    friend class LocalMergeSortSourceOperatorX;
    int _task_idx;
    std::unique_ptr<vectorized::VSortedRunMerger> _merger = nullptr;
};

class LocalMergeSortSourceOperatorX final : public OperatorX<LocalMergeSortLocalState> {
public:
    using Base = OperatorX<LocalMergeSortLocalState>;
    LocalMergeSortSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                  const DescriptorTbl& descs);

#ifdef BE_TEST
    LocalMergeSortSourceOperatorX() : _merge_by_exchange(false), _offset(0) {}
#endif

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;

    bool is_source() const override { return true; }

private:
    void init_dependencies_and_sorter();

    Status other_source_get_block(RuntimeState* state, vectorized::Block* block, bool* eos);
    Status main_source_get_block(RuntimeState* state, vectorized::Block* block, bool* eos);

    friend class PipelineFragmentContext;
    friend class LocalMergeSortLocalState;

    const bool _merge_by_exchange;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;
    vectorized::VSortExecExprs _vsort_exec_exprs;
    const int64_t _offset;

    std::vector<DependencySPtr> _other_source_deps;
    // The sorters of all instances are used in the main source.
    std::vector<std::shared_ptr<vectorized::Sorter>> _sorters;
};

} // namespace pipeline
#include "common/compile_check_end.h"
} // namespace doris
