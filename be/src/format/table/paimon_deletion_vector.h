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
#include <string>
#include <vector>

#include "common/status.h"
#include "format/table/deletion_vector.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

class DeletionVectorReader;
struct DeleteFileDesc;

std::string build_paimon_deletion_vector_cache_key(const TPaimonDeletionFileDesc& deletion_file);
Status validate_paimon_deletion_vector_descriptor(const TPaimonDeletionFileDesc& deletion_file);
// The reader is open and the Paimon descriptor range has been validated.
Status read_paimon_deletion_vector_buffer(DeletionVectorReader& reader, const DeleteFileDesc& desc,
                                          std::vector<char>& buffer);
Status decode_paimon_deletion_vector_buffer(const char* buf, size_t buffer_size,
                                            DeletionVector* deletion_vector);

} // namespace doris
