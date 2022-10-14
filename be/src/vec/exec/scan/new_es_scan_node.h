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

#include "exec/es/es_predicate.h"
#include "vec/exec/scan/new_es_scanner.h"
#include "vec/exec/scan/vscan_node.h"

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
    void set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

protected:
    Status _init_profile() override;
    Status _process_conjuncts() override;
    Status _init_scanners(std::list<VScanner*>* scanners) override;

private:
    Status build_conjuncts_list();

private:
    TupleId _tuple_id;
    TupleDescriptor* _tuple_desc;

    std::map<std::string, std::string> _properties;
    std::map<std::string, std::string> _fields_context;
    std::map<std::string, std::string> _docvalue_context;

    std::vector<std::unique_ptr<TEsScanRange>> _scan_ranges;
    std::vector<std::string> _column_names;

    std::vector<EsPredicate*> _predicates;
    std::vector<int> _predicate_to_conjunct;
    std::vector<int> _conjunct_to_predicate;

    // Profile
    std::unique_ptr<RuntimeProfile> _es_profile;
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;
};
} // namespace doris::vectorized
