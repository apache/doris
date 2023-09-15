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
#include "common/status.h"

namespace doris {

class BuildHelper {
public:
    static BuildHelper* init_instance();
    ~BuildHelper();
    // Return global instance.
    static BuildHelper* instance() { return _s_instance; }

    void initial_build_env();
    void open(const std::string& meta_file, const std::string& build_dir,
              const std::string& data_path, const std::string& file_type, 
              const bool& enable_post_compaction);
    Status build();

private:
    static BuildHelper* _s_instance;
    std::string _meta_file;
    std::string _build_dir;
    std::string _data_path;
    std::string _file_type;
    bool _enable_post_compaction;
};

} // namespace doris
