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
//
// Utility methods for dealing with file paths.
#pragma once

#include <string>
#include <vector>

namespace doris {
namespace path_util {

// NOTE: The methods here are only related to path processing, do not involve
// any file and IO operations.

// Join two path segments with the appropriate path separator, if necessary.
std::string join_path_segments(const std::string& a, const std::string& b);

// Return the enclosing directory of path.
// This is like dirname(3) but for C++ strings.
// The following list of examples shows the strings returned by dirname() and basename():
//   path         dirname    basename
//   "/usr/lib"    "/usr"    "lib"
//   "/usr/"       "/"       "usr"
//   "usr"         "."       "usr"
//   "/"           "/"       "/"
//   "."           "."       "."
//   ".."          "."       ".."
std::string dir_name(const std::string& path);

// Return the terminal component of a path.
// This is like basename(3) but for C++ strings.
std::string base_name(const std::string& path);

// It is used to replace std::filesystem::path::extension().
// If the filename contains a dot but does not consist solely of one or to two dots,
// returns the substring of file_name starting at the rightmost dot and ending at
// the path's end. Otherwise, returns an empty string.
// The dot is included in the return value so that it is possible to distinguish
// between no extension and an empty extension.
// NOTE: path can be either one file's full path or only file name
std::string file_extension(const std::string& path);

} // namespace path_util
} // namespace doris
