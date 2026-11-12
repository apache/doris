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

#include "bos_obj_storage_client.h"

#include "cpp/obj-storage/obj_storage_config.h"

namespace doris {

ObjStorageClientBuildResult BosObjStorageClient::create(const BosConfig& config,
                                                        const ObjStorageClientRuntime& runtime) {
    return create_s3_client<BosObjStorageClient>(config, runtime);
}

} // namespace doris
