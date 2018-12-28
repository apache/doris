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

#ifndef DORIS_BE_RUNTIME_EXEC_NODE_RESOURCE_CONSUMPATION_H
#define DORIS_BE_RUNTIME_EXEC_NODE_RESOURCE_CONSUMPATION_H

#include "util/runtime_profile.h"
#include "util/string_util.h"

namespace doris {

class ExecNodeResourceConsumpation {
public:

    ExecNodeResourceConsumpation() {
        init();
    }

    struct Consumpation {
        
        Consumpation() : cpu(0), io_by_byte(0) {
        }

        void plus(const Consumpation& other) {
            cpu += other.cpu;
            io_by_byte += other.io_by_byte;
        }

        int64_t cpu;
        int64_t io_by_byte;
    };

    bool get_consumpation(RuntimeProfile* profile, Consumpation* consumpation, const std::string& name) {
        ConsumpationFunc get_consumpation_func = function_map[name];
        if (get_consumpation_func != nullptr) {        
            get_consumpation_func(profile, consumpation);
            return true;
        } 
        return false;
    }

    int get_exec_node_enum_type(const std::string& type) {
        return type_name_map[type]; 
    }

private:

    void init() {
        function_map["OLAP_SCAN_NODE"] = get_olap_scan_consumpation;
        function_map["HASH_JOIN_NODE"] = get_hash_join_consumpation;
        function_map["AGGREGATION_NODE"] = get_hash_agg_consumpation;
        function_map["SORT_NODE"] = get_sort_consumpation;
        function_map["ANALYTIC_EVAL_NODE"] = get_windows_consumpation;
        function_map["UNION_NODE"] = get_union_consumpation;
        function_map["EXCHANGE_NODE"] = get_exchange_consumpation;

        type_name_map["OLAP_SCAN_NODE"] = TPlanNodeType::OLAP_SCAN_NODE;
        type_name_map["HASH_JOIN_NODE"] = TPlanNodeType::HASH_JOIN_NODE;
        type_name_map["AGGREGATION_NODE"] = TPlanNodeType::AGGREGATION_NODE;
        type_name_map["SORT_NODE"] = TPlanNodeType::SORT_NODE;
        type_name_map["ANALYTIC_EVAL_NODE"] = TPlanNodeType::ANALYTIC_EVAL_NODE;
        type_name_map["UNION_NODE"] = TPlanNodeType::UNION_NODE;
        type_name_map["EXCHANGE_NODE"] = TPlanNodeType::EXCHANGE_NODE;
    }

    static void get_olap_scan_consumpation(RuntimeProfile* profile, Consumpation* consumpation) {
       RuntimeProfile::Counter* read_compressed_counter = profile->get_counter("CompressedBytesRead");
       consumpation->cpu = 0;
       consumpation->io_by_byte = read_compressed_counter->value();
    }

    static void get_hash_join_consumpation(RuntimeProfile* profile, Consumpation* consumpation) {
        RuntimeProfile::Counter* probe_counter = profile->get_counter("ProbeRows");
        RuntimeProfile::Counter* build_counter = profile->get_counter("BuildRows");
        consumpation->cpu = probe_counter->value() + build_counter->value();
        consumpation->io_by_byte = 0;
    }

    static void get_hash_agg_consumpation(RuntimeProfile* profile, Consumpation* consumpation) {
        RuntimeProfile::Counter* build_counter = profile->get_counter("BuildRows");
        consumpation->cpu = build_counter->value();
        consumpation->io_by_byte = 0;
    }

    static void get_sort_consumpation(RuntimeProfile* profile, Consumpation* consumpation) {
        RuntimeProfile::Counter* sort_counter = profile->get_counter("SortRows");
        consumpation->cpu = sort_counter->value();
        consumpation->io_by_byte = 0;
    }

    static void get_windows_consumpation(RuntimeProfile* profile, Consumpation* consumpation) {
        RuntimeProfile::Counter* process_counter = profile->get_counter("ProcessRows");
        consumpation->cpu = process_counter->value();
        consumpation->io_by_byte = 0;
    }

    static void get_union_consumpation(RuntimeProfile* profile, Consumpation* consumpation) {
        RuntimeProfile::Counter* materialize_counter = profile->get_counter("MaterializeRows");
        consumpation->cpu = materialize_counter->value();
        consumpation->io_by_byte = 0;
    }

    static void get_exchange_consumpation(RuntimeProfile* profile, Consumpation* consumpation) {
        RuntimeProfile::Counter* merge_counter = profile->get_counter("MergeRows");
        // exchange merge sort
        if (merge_counter != nullptr) {
            consumpation->cpu = merge_counter->value();
            consumpation->io_by_byte = 0;
        }
    }

    typedef std::function<void(RuntimeProfile*, Consumpation*)> ConsumpationFunc;
    std::map<std::string, ConsumpationFunc> function_map;
    std::map<std::string, int> type_name_map;
};

}

#endif
