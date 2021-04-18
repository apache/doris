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

#include "util/arrow/utils.h"

#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include "gutil/strings/substitute.h"

namespace doris {

using strings::Substitute;

Status to_status(const arrow::Status& status) {
    if (status.ok()) {
        return Status::OK();
    } else {
        // TODO(zc): convert arrow status to doris status
        return Status::InvalidArgument(status.ToString());
    }
}

Status arrow_pretty_print(const arrow::RecordBatch& rb, std::ostream* os) {
    return to_status(arrow::PrettyPrint(rb, 0, os));
}

Status arrow_pretty_print(const arrow::Array& arr, std::ostream* os) {
    arrow::PrettyPrintOptions opts(4);
    return to_status(arrow::PrettyPrint(arr, opts, os));
}

} // namespace doris
