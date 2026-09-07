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

#include <gen_cpp/Status_types.h>

#include "cpp/obj-client/obj_storage_client.h"

namespace doris::io {

// Supplies explicit unsupported implementations for tests that only exercise a subset of the
// object storage interface. Production providers must implement every method themselves.
class ObjStorageClientTestStub : public ObjStorageClient {
public:
    ObjStorageCapabilities capabilities() const override { return {.max_delete_batch = 1}; }

    ObjStorageResponse get_lifecycle(const std::string&, int64_t*) override {
        return not_supported();
    }

    ObjStorageResponse check_versioning(const std::string&) override { return not_supported(); }

    ObjStorageResponse abort_multipart_upload(const ObjStoragePath&, const std::string&) override {
        return not_supported();
    }

private:
    static ObjStorageResponse not_supported() {
        return {
                .status = {TStatusCode::NOT_IMPLEMENTED_ERROR,
                           "operation is not supported by the object storage test stub"},
                .http_code = 0,
        };
    }
};

} // namespace doris::io
