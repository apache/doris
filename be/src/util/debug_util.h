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

#ifndef DORIS_BE_SRC_COMMON_UTIL_DEBUG_UTIL_H
#define DORIS_BE_SRC_COMMON_UTIL_DEBUG_UTIL_H

#include <ostream>
#include <string>
#include <boost/cstdint.hpp>

#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/RuntimeProfile_types.h"
#include "gen_cpp/AgentService_types.h"

namespace doris {

class RowDescriptor;
class TupleDescriptor;
class Tuple;
class TupleRow;
class RowBatch;
class PUniqueId;

std::string print_tuple(const Tuple* t, const TupleDescriptor& d);
std::string print_row(TupleRow* row, const RowDescriptor& d);
std::string print_batch(RowBatch* batch);
std::string print_id(const TUniqueId& id);
std::string print_id(const PUniqueId& id);
std::string print_plan_node_type(const TPlanNodeType::type& type);
std::string print_tstmt_type(const TStmtType::type& type);
std::string print_query_state(const QueryState::type& type);
std::string PrintTUnit(const TUnit::type& type);
std::string PrintTMetricKind(const TMetricKind::type& type);

// Parse 's' into a TUniqueId object.  The format of s needs to be the output format
// from PrintId.  (<hi_part>:<low_part>)
// Returns true if parse succeeded.
bool parse_id(const std::string& s, TUniqueId* id);

// Returns a string "<product version number> (build <build hash>)"
// If compact == false, this string is appended: "\nBuilt on <build time>"
// This is used to set gflags build version
std::string get_build_version(bool compact);

// Returns "<program short name> version <GetBuildVersion(compact)>"
std::string get_version_string(bool compact);

// Returns the stack trace as a string from the current location.
// Note: there is a libc bug that causes this not to work on 64 bit machines
// for recursive calls.
std::string get_stack_trace();

std::string hexdump(const char* buf, int len);

}

#endif
