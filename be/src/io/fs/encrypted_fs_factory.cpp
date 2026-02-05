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

#include "io/fs/encrypted_fs_factory.h"

#include <gen_cpp/olap_file.pb.h>

#include "common/exception.h"
#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris::io {

FileSystemSPtr make_file_system(const FileSystemSPtr& inner, EncryptionAlgorithmPB algorithm) {
    if (algorithm == EncryptionAlgorithmPB::PLAINTEXT) {
        return inner;
    }
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Current version does not support TDE");
}

} // namespace doris::io
