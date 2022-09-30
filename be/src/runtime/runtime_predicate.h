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

#include <atomic>
#include <string>
#include <mutex>
#include <shared_mutex>

#include "common/config.h"
#include "common/object_pool.h"
#include "gen_cpp/PaloInternalService_types.h" // for TQueryOptions
#include "gen_cpp/Types_types.h"               // for TUniqueId
#include "runtime/datetime_value.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "util/threadpool.h"
#include "vec/core/types.h"
#include "vec/core/field.h"

#include "exec/olap_common.h"
#include "olap/tablet_schema.h"
#include "olap/column_predicate.h"
#include "olap/predicate_creator.h"


namespace doris {

namespace vectorized {

class RuntimePredicate {
public:
    RuntimePredicate() { }

    void set_tablet_schema(TabletSchemaSPtr tablet_schema) {
        std::unique_lock<std::shared_mutex> wlock(_rwlock);
        _tablet_schema = tablet_schema;
    }

    std::shared_ptr<ColumnPredicate> get_predictate() {
        std::shared_lock<std::shared_mutex> rlock(_rwlock);
        return _predictate;
    }

    Status update(std::vector<Field>& values, const String& col_name,
                  const TypeIndex type, bool is_reverse);

private:
    mutable std::shared_mutex _rwlock;
    std::vector<vectorized::Field> _orderby_extrems;
    std::shared_ptr<ColumnPredicate> _predictate {nullptr};
    TabletSchemaSPtr _tablet_schema;
    std::unique_ptr<MemPool> _predicate_mem_pool;
};

} // namespace vectorized
} // namespace doris
