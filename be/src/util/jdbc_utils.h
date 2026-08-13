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

#include <string>

#include "common/status.h"

namespace doris {

/**
 * Utility functions for JDBC driver management.
 */
class JdbcUtils {
public:
    /**
     * Resolve a JDBC driver URL to an absolute file:// URL.
     *
     * FE sends just the JAR filename (e.g. "mysql-connector-java-8.0.25.jar").
     * This method resolves it to a full file:// URL by searching in the
     * configured jdbc_drivers_dir (or the default DORIS_HOME/plugins/jdbc_drivers).
     *
     * If the URL already contains ":/", it is assumed to be a full URL and
     * returned as-is.
     *
     * @param url         The driver URL from FE (may be just a filename)
     * @param result_url  Output: the resolved file:// URL
     * @return Status::OK on success, or InternalError if the file is not found
     */
    static Status resolve_driver_url(const std::string& url, std::string* result_url);
};

} // namespace doris
