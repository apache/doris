// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "common/daemon.h"

#include <gperftools/malloc_extension.h>
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/network_util.h"
#include "util/thrift_util.h"
#include "runtime/lib_cache.h"
#include "exprs/operators.h"
#include "exprs/is_null_predicate.h"
#include "exprs/like_predicate.h"
#include "exprs/compound_predicate.h"
#include "exprs/new_in_predicate.h"
#include "exprs/string_functions.h"
#include "exprs/cast_functions.h"
#include "exprs/math_functions.h"
#include "exprs/encryption_functions.h"
#include "exprs/timestamp_functions.h"
#include "exprs/decimal_operators.h"
#include "exprs/utility_functions.h"
#include "exprs/json_functions.h"
#include "exprs/hll_hash_function.h"

namespace palo {

void* tcmalloc_gc_thread(void* dummy) {
    while (1) {
        sleep(10);
        size_t used_size = 0;
        size_t free_size = 0;

#ifndef ADDRESS_SANITIZER
        MallocExtension::instance()->GetNumericProperty("generic.current_allocated_bytes", &used_size);
        MallocExtension::instance()->GetNumericProperty("tcmalloc.pageheap_free_bytes", &free_size);
#endif
        size_t alloc_size = used_size + free_size;

        if (alloc_size > config::tc_use_memory_min) {
            size_t max_free_size = alloc_size * config::tc_free_memory_rate / 100;

#ifndef ADDRESS_SANITIZER
            if (free_size > max_free_size) {
                MallocExtension::instance()->ReleaseToSystem(free_size - max_free_size);
            }
#endif
        }
    }

    return NULL;
}

void init_daemon(int argc, char** argv) {
    // google::SetVersionString(get_build_version(false));
    // google::ParseCommandLineFlags(&argc, &argv, true);
    init_glog("be", true);

    LOG(INFO) << get_version_string(false);

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    LibCache::init();
    Operators::init();
    IsNullPredicate::init();
    LikePredicate::init();
    StringFunctions::init();
    CastFunctions::init();
    InPredicate::init();
    MathFunctions::init();
    EncryptionFunctions::init();
    TimestampFunctions::init();
    DecimalOperators::init();
    UtilityFunctions::init();
    CompoundPredicate::init();
    JsonFunctions::init();
    HllHashFunctions::init();

    pthread_t id;
    pthread_create(&id, NULL, tcmalloc_gc_thread, NULL);

    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();
}

}
