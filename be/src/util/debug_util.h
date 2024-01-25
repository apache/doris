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

#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

#include <string>

namespace doris {

std::string print_plan_node_type(const TPlanNodeType::type& type);
std::string print_tstmt_type(const TStmtType::type& type);
std::string print_query_state(const QueryState::type& type);
std::string PrintTUnit(const TUnit::type& type);
std::string PrintTMetricKind(const TMetricKind::type& type);
std::string PrintThriftNetworkAddress(const TNetworkAddress&);
std::string PrintFrontendInfo(const TFrontendInfo& fe_info);
std::string PrintFrontendInfos(const std::vector<TFrontendInfo>& fe_infos);

// A desirable scenario would be to call this function WHENEVER whenever we need to print instance information.
// By using a fixed format, we would be able to identify all the paths in which this instance is executed.
// InstanceId|FragmentIdx|QueryId
std::string PrintInstanceStandardInfo(const TUniqueId& qid, const TUniqueId& iid);

// Returns a string "<product version number> (build <build hash>)"
// If compact == false, this string is appended: "\nBuilt on <build time>"
// This is used to set gflags build version
std::string get_build_version(bool compact);

// Returns a string "<product version number> (<short build hash>)"
std::string get_short_version();

// Returns "<program short name> version <GetBuildVersion(compact)>"
std::string get_version_string(bool compact);

std::string hexdump(const char* buf, int len);

} // namespace doris
