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

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace doris::test {

// A file of frozen digests in the source tree, one "<name> <part> <count> <digest>" line each; a
// line starting with '#' is a comment. A test checks every digest it computes against the file.
// Lines are only ever added: ANALYSIS_GOLDEN_REGEN=<commit> appends the digests the file does not
// have yet, under a comment naming the commit of the code that produced them, and never rewrites
// one, so a changed output always fails. ANALYSIS_GOLDEN_WRITE=path also
// writes every computed line to path, to compare two builds byte for byte.
class GoldenFile {
public:
    // The path is relative to the source root, where unit tests run.
    explicit GoldenFile(std::string path);

    // Checks one computed digest against its frozen line. `context` describes what was computed,
    // for the failure message.
    void check(const std::string& name, const std::string& part, uint64_t count, uint64_t digest,
               const std::string& context);

    // Ends a run. When the run computed every line `owns` selects, a frozen line it selects that no
    // check named fails, because lines are never removed. The digests the file lacks are appended
    // with ANALYSIS_GOLDEN_REGEN when the run was complete, and fail otherwise.
    void finish(const std::function<bool(const std::string& name)>& owns, bool complete);

private:
    struct Frozen {
        uint64_t count = 0;
        uint64_t digest = 0;
    };

    const std::string _path;
    std::map<std::string, Frozen> _frozen;
    std::set<std::string> _checked;
    std::vector<std::string> _computed;
    std::vector<std::string> _missing;
};

} // namespace doris::test
