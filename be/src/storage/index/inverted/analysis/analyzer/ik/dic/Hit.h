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

#include <CLucene.h>

#include <memory>

namespace doris::segment_v2 {

class DictSegment;

class Hit {
private:
    static const int UNMATCH = 0x00000000;
    static const int MATCH = 0x00000001;
    static const int PREFIX = 0x00000010;

    int hitState_ {UNMATCH};
    DictSegment* matchedDictSegment_;
    size_t byteBegin_ {0};
    size_t byteEnd_ {0};
    size_t charBegin_ {0};
    size_t charEnd_ {0};

public:
    Hit() = default;

    bool isMatch() const { return (hitState_ & MATCH) > 0; }
    void setMatch() { hitState_ |= MATCH; }

    bool isPrefix() const { return (hitState_ & PREFIX) > 0; }
    void setPrefix() { hitState_ |= PREFIX; }

    bool isUnmatch() const { return hitState_ == UNMATCH; }
    void setUnmatch() { hitState_ = UNMATCH; }

    DictSegment* getMatchedDictSegment() const { return matchedDictSegment_; }
    void setMatchedDictSegment(DictSegment* segment) { matchedDictSegment_ = segment; }

    size_t getByteBegin() const { return byteBegin_; }
    void setByteBegin(size_t pos) { byteBegin_ = pos; }

    size_t getByteEnd() const { return byteEnd_; }
    void setByteEnd(size_t pos) { byteEnd_ = pos; }

    size_t getCharBegin() const { return charBegin_; }
    void setCharBegin(size_t pos) { charBegin_ = pos; }

    size_t getCharEnd() const { return charEnd_; }
    void setCharEnd(size_t pos) { charEnd_ = pos; }
};

} // namespace doris::segment_v2
