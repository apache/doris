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

#ifndef DORIS_BE_SRC_UTIL_ZIP_UTIL_H
#define DORIS_BE_SRC_UTIL_ZIP_UTIL_H

#include <string>

#include "common/status.h"

namespace doris {

/**
 * extract .zip file to $(target path)/$(target directory)
 * 
 * @param zip_path .zip file path
 * @param target_path target path
 * @param target_directory target directory name and prefix without '/' or '\\', like 'test' not '/test' 
 */
Status zip_extract(const std::string& zip_path, const std::string& target_path, const std::string& target_directory);

}


#endif //CORE_ZIP_UTIL_H
