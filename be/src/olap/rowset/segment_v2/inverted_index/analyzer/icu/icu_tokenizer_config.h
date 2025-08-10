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

#include "icu_common.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class ICUTokenizerConfig {
public:
    ICUTokenizerConfig() = default;
    virtual ~ICUTokenizerConfig() = default;

    virtual void initialize(const std::string& dictPath) = 0;
    virtual icu::BreakIterator* get_break_iterator(int32_t script) = 0;
    virtual bool combine_cj() = 0;

    static const int32_t EMOJI_SEQUENCE_STATUS = 299;
};
using ICUTokenizerConfigPtr = std::shared_ptr<ICUTokenizerConfig>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2