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

#include "util/hdfs_util.h"

#include <util/string_util.h>

#include "common/config.h"
#include "common/logging.h"

namespace doris {

HDFSHandle& HDFSHandle::instance() {
    static HDFSHandle hdfs_handle;
    return hdfs_handle;
}

hdfsFS HDFSHandle::create_hdfs_fs(HDFSCommonBuilder& hdfs_builder) {
    if (hdfs_builder.is_need_kinit()) {
        Status status = hdfs_builder.run_kinit();
        if (!status.ok()) {
            LOG(WARNING) << status.get_error_msg();
            return nullptr;
        }
    }
    hdfsFS hdfs_fs = hdfsBuilderConnect(hdfs_builder.get());
    if (hdfs_fs == nullptr) {
        LOG(WARNING) << "connect to hdfs failed."
                     << ", error: " << hdfsGetLastError();
        return nullptr;
    }
    return hdfs_fs;
}

} // namespace doris