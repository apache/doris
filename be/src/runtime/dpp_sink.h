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

#ifndef DORIS_BE_RUNTIME_DPP_SINK_H
#define DORIS_BE_RUNTIME_DPP_SINK_H

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/dpp_sink_internal.h"
namespace doris {

class RuntimeState;
class RowDescriptor;
class RowBatch;
class DppWriter;
class Translator;
class RuntimeProfile;
class CountDownLatch;

// This class swallow data which is split by partition and rollup.
// Sort input data and then aggregate data contains same key,
// then write new data into dpp writer for next push operation.
class DppSink {
public:
    DppSink(const RowDescriptor& row_desc, const std::map<std::string, RollupSchema*>& rollup_map)
            : _row_desc(row_desc),
              _rollup_map(rollup_map),
              _profile(nullptr),
              _translator_count(0) {}

    ~DppSink() {}

    Status init(RuntimeState* state);

    Status add_batch(ObjectPool* obj_pool, RuntimeState* state, const TabletDesc& desc,
                     RowBatch* batch);

    // called when all data is pushed by 'add_batch'
    // this function will sort, aggregate, write data one by one
    Status finish(RuntimeState* state);

    RuntimeProfile* profile() { return _profile; }

    void collect_output(std::vector<std::string>* files);

private:
    Status get_or_create_translator(ObjectPool* obj_pool, RuntimeState* state,
                                    const TabletDesc& tablet_desc,
                                    std::vector<Translator*>** trans_vec);
    void process(RuntimeState* state, Translator* trans, CountDownLatch* latch);

    // description of batch added
    const RowDescriptor& _row_desc;
    // map from 'rollup name' to 'rollup schema'
    const std::map<std::string, RollupSchema*>& _rollup_map;
    RuntimeProfile* _profile;

    // This map from batch id to Translator
    std::unordered_map<TabletDesc, std::vector<Translator*>> _translator_map;
    int _translator_count;
};

} // namespace doris

#endif
