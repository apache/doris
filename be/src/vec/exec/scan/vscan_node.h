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

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <parallel_hashmap/phmap.h>
#include <stdint.h>

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/olap_common.h"
#include "exprs/function_filter.h"
#include "runtime/define_primitive_type.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/exec/runtime_filter_consumer.h"
#include "vec/exec/scan/scanner_context.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/runtime/shared_scanner_controller.h"

namespace doris {
class BitmapFilterFuncBase;
class BloomFilterFuncBase;
class DescriptorTbl;
class FunctionContext;
class HybridSetBase;
class IRuntimeFilter;
class SlotDescriptor;
class TScanRangeParams;
class TupleDescriptor;

namespace vectorized {
class Block;
class VExpr;
class VExprContext;
class VInPredicate;
class VectorizedFnCall;
} // namespace vectorized
struct StringRef;
} // namespace doris

namespace doris::pipeline {
class ScanOperator;
}

namespace doris::vectorized {

class VScanner;
class VSlotRef;

// We want to close scanner automatically, so using a delegate class
// and call close method in the delegate class's dctor.
class ScannerDelegate {
public:
    VScannerSPtr _scanner;
    ScannerDelegate(VScannerSPtr& scanner_ptr) : _scanner(scanner_ptr) {}
    ~ScannerDelegate() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_scanner->runtime_state()->query_mem_tracker());
        Status st = _scanner->close(_scanner->runtime_state());
        if (!st.ok()) {
            LOG(WARNING) << "close scanner failed, st = " << st;
        }
        _scanner.reset();
    }
    ScannerDelegate(ScannerDelegate&&) = delete;
};

} // namespace doris::vectorized
