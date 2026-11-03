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

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/analysis/golden/golden_configs.h"
#include "storage/index/inverted/analysis/golden/golden_corpus.h"
#include "testutil/scoped_temp_dir.h"

namespace doris {
class IndexPolicyMgr;
} // namespace doris

namespace doris::segment_v2::inverted_index::golden {

// What the golden configurations need around them: a dictionary directory laid out as in the BE
// package, with the repository's dictionaries and a fixed kuromoji dictionary; kuromoji enabled;
// and the policies of the configurations registered in a policy manager of its own. Everything
// is restored when the environment is destroyed. Unit tests run from the source root.
class GoldenEnvironment {
public:
    GoldenEnvironment();
    ~GoldenEnvironment();

    GoldenEnvironment(const GoldenEnvironment&) = delete;
    GoldenEnvironment& operator=(const GoldenEnvironment&) = delete;

    Status init(const std::vector<GoldenConfig>& configs);

    CorpusSources corpus_sources() const;

private:
    const std::filesystem::path _source_root;
    doris::test::ScopedTempDirectory _dict_dir {"analysis_golden_dict"};
    const std::string _saved_dict_path;
    const bool _saved_enable_kuromoji;
    IndexPolicyMgr* const _saved_policy_mgr;
    std::unique_ptr<IndexPolicyMgr> _policy_mgr;
};

// Writes the fixed kuromoji dictionary of the golden tests to `dir`: a few dozen words with IPADIC
// features, the IPADIC character categories, and unknown words for every category. The real
// dictionary is not in the repository and unit test builds do not generate it.
Status write_golden_kuromoji_dictionary(const std::string& dir);

} // namespace doris::segment_v2::inverted_index::golden
