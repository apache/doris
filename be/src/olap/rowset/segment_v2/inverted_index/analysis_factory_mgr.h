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

#include "common/exception.h"
#include "olap/rowset/segment_v2/inverted_index/abstract_analysis_factory.h"
#include "olap/rowset/segment_v2/inverted_index/setting.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class AnalysisFactoryMgr {
public:
    using FactoryCreator = std::function<AbstractAnalysisFactoryPtr()>;

    AnalysisFactoryMgr(const AnalysisFactoryMgr&) = delete;
    AnalysisFactoryMgr& operator=(const AnalysisFactoryMgr&) = delete;

    static AnalysisFactoryMgr& instance() {
        static AnalysisFactoryMgr instance;
        return instance;
    }

    void initialise();
    void registerFactory(const std::string& name, FactoryCreator creator);

    template <typename FactoryType>
    std::shared_ptr<FactoryType> create(const std::string& name, const Settings& params);

private:
    AnalysisFactoryMgr() = default;
    ~AnalysisFactoryMgr() = default;

    std::map<std::string, FactoryCreator> registry_;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index