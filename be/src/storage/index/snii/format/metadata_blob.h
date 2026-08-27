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

#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {

// Appends a raw metadata frame or a zstd carrier for it. The carrier payload is
// varint64 raw-frame length followed by zstd(raw frame).
Status encode_metadata_blob(Slice raw_frame, SectionType raw_type, SectionType compressed_type,
                            ByteSink* out);

// Returns a raw metadata frame from a raw frame or zstd carrier. A raw frame
// remains a view into stored_frame; a materialized carrier is a view into scratch.
Status materialize_metadata_blob(Slice stored_frame, SectionType raw_type,
                                 SectionType compressed_type, std::vector<uint8_t>* scratch,
                                 Slice* raw_frame);

} // namespace doris::snii::format
