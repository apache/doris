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
#include "minizip/unzip.h"

namespace doris {

/**
 * zip file extract 
 * extract .zip file to $(target path)/$(target directory)
 * 
 *  usage:
 *      zipfile = ZipFile("/home/test/test.zip");
 *      zipfile.open();
 *      zipfile.extract("/home/test", "target_directory");
 *      zipfile.close();
 *      
 *  /home/test/test.zip content:
 *  --one/
 *  ----test.txt
 *  --two/
 *  
 *  The extract result:
 *  /home/test/target_directory
 *  --one/
 *  ----test.txt
 *  --two/
 */
class ZipFile {
public:
    ZipFile(const std::string& zip_path)
            : _zip_path(zip_path), _zip_file(nullptr), _open_current_file(false) {}

    ~ZipFile() { WARN_IF_ERROR(close(), "failed to close zip file: " + _zip_path); }

    Status extract(const std::string& target_path, const std::string& target_directory);

private:
    Status close();

    Status extract_file(const std::string& target_path);

private:
    std::string _zip_path;

    unzFile _zip_file;

    bool _open_current_file;

    std::vector<std::string> _clean_paths;
};
} // namespace doris

#endif //CORE_ZIP_UTIL_H
