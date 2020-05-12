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

#include "kudu/util/trace_metrics.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <map>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <glog/stl_logging.h>

#include "kudu/util/debug/leakcheck_disabler.h"

using std::string;

namespace kudu {

// Make glog's STL-compatible operators visible inside this namespace.
using ::operator<<;

namespace {

static simple_spinlock g_intern_map_lock;
typedef std::map<string, const char*> InternMap;
static InternMap* g_intern_map;

} // anonymous namespace

const char* TraceMetrics::InternName(const string& name) {
  DCHECK(std::all_of(name.begin(), name.end(), [] (char c) { return isprint(c); } ))
      << "not printable: " << name;

  debug::ScopedLeakCheckDisabler no_leakcheck;
  std::lock_guard<simple_spinlock> l(g_intern_map_lock);
  if (g_intern_map == nullptr) {
    g_intern_map = new InternMap();
  }

  InternMap::iterator it = g_intern_map->find(name);
  if (it != g_intern_map->end()) {
    return it->second;
  }

  const char* dup = strdup(name.c_str());
  (*g_intern_map)[name] = dup;

  // We don't expect this map to grow large.
  DCHECK_LT(g_intern_map->size(), 100) <<
      "Too many interned strings: " << *g_intern_map;

  return dup;
}

} // namespace kudu
