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

#include <parquet/statistics.h>

#include "vec/core/types.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris::vectorized::parquet_utils {

void merge_stats(const std::shared_ptr<::parquet::Statistics>& left,
                 const std::shared_ptr<::parquet::Statistics>& right);

} // namespace doris::vectorized::parquet_utils
