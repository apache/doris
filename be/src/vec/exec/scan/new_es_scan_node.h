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

#include <gen_cpp/PlanNodes_types.h>

#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/global_types.h"
#include "common/status.h"
#include "util/runtime_profile.h"
#include "vec/exec/scan/vscan_node.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TScanRangeParams;
class TupleDescriptor;

namespace vectorized {
class VScanner;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class NewEsScanNode : public VScanNode {
public:
    friend class NewEsScanner;

public:
    NewEsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~NewEsScanNode() override = default;

    std::string get_name() override;
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    void set_scan_ranges(RuntimeState* state,
                         const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    Status _init_profile() override;
    Status _process_conjuncts() override;
    Status _init_scanners(std::list<VScannerSPtr>* scanners) override;

private:
    TupleId _tuple_id;
    TupleDescriptor* _tuple_desc;

    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _fields_context;
    std::map<std::string, std::string> _docvalue_context;

    std::vector<std::unique_ptr<TEsScanRange>> _scan_ranges;
    std::vector<std::string> _column_names;

    // Profile
    std::unique_ptr<RuntimeProfile> _es_profile;
    // FIXME: non-static data member '_rows_read_counter' of 'NewEsScanNode' shadows member inherited from type 'VScanNode'
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
    RuntimeProfile::Counter* _rows_read_counter;
#ifdef __clang__
#pragma clang diagnostic pop
#endif
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;
};
} // namespace doris::vectorized
