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

#include <cctz/time_zone.h>

#include <memory>

#include "exprs/vexpr_fwd.h"
#include "format_v2/file_reader.h"

namespace orc {
class SearchArgumentBuilder;
class Type;
} // namespace orc

namespace doris::format::orc {

// Lower already-localized Doris file filters to ORC SearchArgument predicates.
// TableColumnMapper owns table-schema -> file-local localization; this module
// owns the ORC-specific type-id/literal lowering needed by the ORC C++ library.
bool build_orc_search_argument(const format::FileScanRequest& request, const ::orc::Type& root_type,
                               const cctz::time_zone& timezone, const VExprSPtr& expr,
                               std::unique_ptr<::orc::SearchArgumentBuilder>& builder);

} // namespace doris::format::orc
