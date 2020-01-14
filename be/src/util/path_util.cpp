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

#include "util/path_util.h"

#include <cstring>
#include <memory>
// Use the POSIX version of dirname(3). See `man 3 dirname`
#include <libgen.h>

#include "common/logging.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"

using std::string;
using std::vector;
using strings::SkipEmpty;
using strings::Split;

namespace doris {
namespace path_util {

const string kTmpInfix = ".doristmp";

string join_path_segments(const string& a, const string& b) {
    DCHECK(!a.empty()) << "empty first component: " << a;
    DCHECK(!b.empty() && b[0] != '/')
            << "second path component must be non-empty and relative: " << b;
    if (a.back() == '/') {
        return a + b;
    } else {
        return a + "/" + b;
    }
}

vector<string> join_path_segments_v(const vector<string>& v, const string& s) {
    vector<string> out;
    for (const string& path : v) {
        out.emplace_back(join_path_segments(path, s));
    }
    return out;
}

vector<string> split_path(const string& path) {
    if (path.empty()) {
        return {};
    }
    vector<string> segments;
    if (path[0] == '/') {
        segments.emplace_back("/");
    }
    vector<StringPiece> pieces = Split(path, "/", SkipEmpty());
    for (const StringPiece& piece : pieces) {
        segments.emplace_back(piece.data(), piece.size());
    }
    return segments;
}

string dir_name(const string& path) {
    std::unique_ptr<char[]> path_copy(strdup(path.c_str()));
    return dirname(path_copy.get());
}

string base_name(const string& path) {
    std::unique_ptr<char[]> path_copy(strdup(path.c_str()));
    return basename(path_copy.get());
}

} // namespace path_util
} // namespace doris
