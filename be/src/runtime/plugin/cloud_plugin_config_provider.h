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

#include <memory>
#include <string>

#include "common/status.h"
#include "runtime/plugin/s3_plugin_downloader.h"

namespace doris {

/**
 * CloudPluginConfigProvider retrieves S3 config from cloud mode.
 */
class CloudPluginConfigProvider {
public:
    // Get S3 configuration from cloud mode
    static Status get_cloud_s3_config(std::unique_ptr<S3PluginDownloader::S3Config>* s3_config);

    // Get cloud instance ID
    static Status get_cloud_instance_id(std::string* instance_id);

private:
    // Get default storage vault info using CloudMetaMgr
    static Status _get_default_storage_vault_info(S3PluginDownloader::S3Config* s3_config);
};

} // namespace doris