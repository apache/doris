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

#include "AnalyzeContext.h"
#include "CLucene/_ApiHeader.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class ISegmenter {
public:
    virtual ~ISegmenter() {}

    // Read the next possible token from the analyzer
    // param context Segmentation algorithm context
    virtual void analyze(AnalyzeContext& context) = 0;

    // Reset the sub-analyzer state
    virtual void reset() = 0;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
