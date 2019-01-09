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

#ifndef DORIS_BE_RUNTIME_EXEC_NODE_CONSUMPTION_PROVIDER_H
#define DORIS_BE_RUNTIME_EXEC_NODE_CONSUMPTION_PROVIDER_H

#include "util/runtime_profile.h"
#include "util/string_util.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

// Generate ExecNode resource Consumption with RuntimeProfile. CPU 
// consumption is measured by the number of rows processed and IO 
// consumption is measured by the size of the scans.
class ExecNodeConsumptionProvider {
public:

    ExecNodeConsumptionProvider() {
        init();
    }

    class Consumption {
    public:
        Consumption() : cpu(0), io(0) {
        }

        void add(const Consumption& other) {
            cpu.add(other.cpu);
            io.add(other.io);
        }

        void serialize(PQueryConsumption* consumption) {
            DCHECK(consumption != nullptr);
            consumption->set_cpu(cpu.load());
            consumption->set_io(io.load());
        }

        void deserialize(const PQueryConsumption& consumption) {
            cpu.store(consumption.cpu());
            io.store(consumption.io());
        }

        int64_t get_cpu() {
            return cpu.load();
        }

        int64_t get_io() {
            return io.load();
        }

        void set(int64_t cpu, int64_t io) {
            this->cpu.store(cpu);
            this->io.store(io);
        }

        Consumption& operator=(const Consumption& other) {
            if (this != &other) {
                set(other.cpu, other.io); 
            }
            return *this;
        }
    private:
        AtomicInt64 cpu;
        AtomicInt64 io;
    };

    Consumption get_consumption(RuntimeProfile* profile) {
        Consumption total_consumption;
        std::vector<RuntimeProfile*> all_profiles;
        profile->get_all_children(&all_profiles);
        for (auto profile : all_profiles) {
            // ExecNode's RuntimeProfile name is "$ExecNode_type_name (id=?)" 
            std::vector<std::string> elements;
            boost::split(elements, profile->name(), boost::is_any_of(" "), boost::token_compress_off);
            Consumption consumption;
            bool has = get_consumption(profile, &consumption, elements[0]);
            if (elements.size() == 2 && has) {
                total_consumption.add(consumption);
            }
        }
        return total_consumption;
    }

private:

    void init() {
        functions["OLAP_SCAN_NODE"] = get_olap_scan_consumption;
        functions["HASH_JOIN_NODE"] = get_hash_join_consumption;
        functions["AGGREGATION_NODE"] = get_hash_agg_consumption;
        functions["SORT_NODE"] = get_sort_consumption;
        functions["ANALYTIC_EVAL_NODE"] = get_windows_consumption;
        functions["UNION_NODE"] = get_union_consumption;
        functions["EXCHANGE_NODE"] = get_exchange_consumption;
    }

    bool get_consumption(RuntimeProfile* profile, Consumption* consumption, const std::string& name) {
        ConsumptionFunc get_consumption_func = functions[name];
        if (get_consumption_func != nullptr) {
            get_consumption_func(profile, consumption);
            return true;
        }
        return false;
    }

    static void get_olap_scan_consumption(RuntimeProfile* profile, Consumption* consumption) {
       RuntimeProfile::Counter* read_compressed_counter = profile->get_counter("CompressedBytesRead");
       consumption->set(0, read_compressed_counter->value());
    }

    static void get_hash_join_consumption(RuntimeProfile* profile, Consumption* consumption) {
        RuntimeProfile::Counter* probe_counter = profile->get_counter("ProbeRows");
        RuntimeProfile::Counter* build_counter = profile->get_counter("BuildRows");
        consumption->set(probe_counter->value() + build_counter->value(), 0);
    }

    static void get_hash_agg_consumption(RuntimeProfile* profile, Consumption* consumption) {
        RuntimeProfile::Counter* build_counter = profile->get_counter("BuildRows");
        consumption->set(build_counter->value(), 0);
    }

    static void get_sort_consumption(RuntimeProfile* profile, Consumption* consumption) {
        RuntimeProfile::Counter* sort_counter = profile->get_counter("SortRows");
        consumption->set(sort_counter->value(), 0);
    }

    static void get_windows_consumption(RuntimeProfile* profile, Consumption* consumption) {
        RuntimeProfile::Counter* process_counter = profile->get_counter("ProcessRows");
        consumption->set(process_counter->value(), 0);
    }

    static void get_union_consumption(RuntimeProfile* profile, Consumption* consumption) {
        RuntimeProfile::Counter* materialize_counter = profile->get_counter("MaterializeRows");
        consumption->set(materialize_counter->value(), 0);
    }

    static void get_exchange_consumption(RuntimeProfile* profile, Consumption* consumption) {
        RuntimeProfile::Counter* merge_counter = profile->get_counter("MergeRows");
        // exchange merge sort
        if (merge_counter != nullptr) {
            consumption->set(merge_counter->value(), 0);
        }
    }

    typedef std::function<void(RuntimeProfile*, Consumption*)> ConsumptionFunc;
    // ExecNode type name to function
    std::map<std::string, ConsumptionFunc> functions;
};

}

#endif
