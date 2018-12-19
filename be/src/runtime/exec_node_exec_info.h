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

#ifndef DORIS_BE_SRC_QUERY_RUNTIME_CURRENT_EXEC_QUERY_INFO_H
#define DORIS_BE_SRC_QUERY_RUNTIME_CURRENT_EXEC_QUERY_INFO_H

#include "common/atomic.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

// Privide query's execution informations for current query execution, 
// to help locate which query causes cluster overload. Now the data size 
// read by ScanNode, as a measure of IO and row number of data processed 
// by ExecNode as a measure of CPU. 
// TODO ch, for more detail accurate cpu consumption, the times of comparing
// in the calculation of all ExecNode can be actted as a measure of CPU.
class ExecNodeExecInfo {
public:
    ExecNodeExecInfo() {
    }

    ExecNodeExecInfo(int id, TPlanNodeType::type type) 
           : id(id), type(type) {
    }

    void set_cpu_consumpation(long cpu_consumpation) {
        this->cpu.store(cpu_consumpation); 
    }

    void add_cpu_consumpation(long added) {
        this->cpu.add(added);
    }

    int64_t get_cpu_consumpation() {
        return this->cpu.load();
    }

    // only for scannode
    void set_io_by_byte(long raw_data) {
        this->io.store(raw_data);
    }

    int64_t get_io_by_byte() {
       return io.load();
    }

    int get_id() {
        return id;
    }

    TPlanNodeType::type get_type() {
        return type;
    }    
private:
    // ExecNode Id
    int id;
    // ExecNode type
    TPlanNodeType::type type;
    // Row number of data processed by ExecNode, as a measure of CPU.
    AtomicInt64 cpu;
    // The data size readed by ScanNode in byte units, as a measure of IO.
    AtomicInt64 io;
};

}
#endif
