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
#include <arrow/status.h>

#include "gutil/strings/substitute.h"

namespace doris {

Status to_doris_status(const arrow::Status& status) {
    if (status.ok()) {
        return Status::OK();
    } else {
        return Status::InternalError(status.ToString());
    }
}

arrow::Status to_arrow_status(const Status& status) {
    if (LIKELY(status.ok())) {
        return arrow::Status::OK();
    } else {
        LOG(WARNING) << status.to_string();
        // The length of exception msg returned to the ADBC Client cannot larger than 8192,
        // otherwise ADBC Client will receive:
        // `INTERNAL: http2 exception Header size exceeded max allowed size (8192)`.
        return arrow::Status::Invalid(status.to_string_no_stack());
    }
}

Status arrow_pretty_print(const arrow::RecordBatch& rb, std::ostream* os) {
    return to_doris_status(arrow::PrettyPrint(rb, 0, os));
}

Status arrow_pretty_print(const arrow::Array& arr, std::ostream* os) {
    arrow::PrettyPrintOptions opts(4);
    return to_doris_status(arrow::PrettyPrint(arr, opts, os));
}

} // namespace doris
