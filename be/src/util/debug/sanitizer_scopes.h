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
//
// Wrappers around the annotations from gutil/dynamic_annotations.h,
// provided as C++-style scope guards.

#pragma once

#include "gutil/dynamic_annotations.h"
#include "gutil/macros.h"

namespace doris {
namespace debug {

// Scope guard which instructs TSAN to ignore all reads and writes
// on the current thread as long as it is alive. These may be safely
// nested.
class ScopedTSANIgnoreReadsAndWrites {
 public:
  ScopedTSANIgnoreReadsAndWrites() {
    ANNOTATE_IGNORE_READS_AND_WRITES_BEGIN();
  }
  ~ScopedTSANIgnoreReadsAndWrites() {
    ANNOTATE_IGNORE_READS_AND_WRITES_END();
  }
 private:
  DISALLOW_COPY_AND_ASSIGN(ScopedTSANIgnoreReadsAndWrites);
};

} // namespace debug
} // namespace doris

