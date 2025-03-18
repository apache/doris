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

#include <memory>

#include "CLucene.h"
#include "CLucene/debug/error.h"
#include "unicode/brkiter.h"
#include "unicode/rbbi.h"
#include "unicode/ubrk.h"
#include "unicode/uchar.h"
#include "unicode/uniset.h"
#include "unicode/unistr.h"
#include "unicode/uscript.h"
#include "unicode/utext.h"
#include "unicode/utf8.h"

namespace doris::segment_v2 {

using BreakIteratorPtr = std::unique_ptr<icu::BreakIterator>;

struct UTextDeleter {
    void operator()(UText* utext) const {
        if (utext != nullptr) {
            utext_close(utext);
        }
    }
};

using UTextPtr = std::unique_ptr<UText, UTextDeleter>;

} // namespace doris::segment_v2