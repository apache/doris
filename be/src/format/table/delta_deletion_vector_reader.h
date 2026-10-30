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

#include <cstddef>
#include <cstdint>
#include <string>

#include "common/status.h"
#include "format/table/deletion_vector.h"

namespace doris {

Status validate_delta_deletion_vector_read_range(int64_t offset, int64_t size, size_t& bytes_read);

Status decode_delta_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                           int64_t expected_cardinality,
                                           DeletionVector* deletion_vector);

Status decode_delta_inline_deletion_vector(const std::string& encoded, int64_t size_in_bytes,
                                           int64_t expected_cardinality,
                                           DeletionVector* deletion_vector);

Status resolve_delta_deletion_vector_path(const std::string& storage_type,
                                          const std::string& path_or_inline_dv,
                                          const std::string& table_path, std::string* path);

} // namespace doris
