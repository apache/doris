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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rune.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

class SmartGetWord;

class SmartForest {
public:
    using ParamType = std::vector<std::string>;

    // Corresponding Java constants
    static constexpr int MAX_SIZE = 65536; // 0x10000, supports all characters in BMP plane

    // Node status: corresponding to status values in Java
    enum Status : uint8_t {
        CONTINUE = 1,      // Continue matching (1 in Java)
        WORD_CONTINUE = 2, // Word can continue (2 in Java)
        WORD_END = 3       // Definite word end (3 in Java)
    };

public:
    std::unordered_map<UChar32, std::unique_ptr<SmartForest>> branches; // On-demand branch mapping
    SmartForest* branch = nullptr;                                      // Temporary lookup result

public:
    SmartForest() : rate_(0.9), c_(0), status_(CONTINUE) {}

    explicit SmartForest(double rate) : rate_(rate), c_(0), status_(CONTINUE) {}

    explicit SmartForest(UChar32 c) : rate_(0.9), c_(c), status_(CONTINUE) {}

    SmartForest(UChar32 c, uint8_t status) : rate_(0.9), c_(c), status_(status) {}

    SmartForest(UChar32 c, uint8_t status, ParamType param)
            : rate_(0.9), c_(c), status_(status), param_(std::move(param)) {}

    SmartForest* add(std::unique_ptr<SmartForest> branch_node);

    void add(const std::string& keyWord, const ParamType& param);

    void add(const std::vector<Rune>& runes, const ParamType& param);

    SmartForest* get(UChar32 c) { return getBranch(c); }

    SmartForest* getBranch(UChar32 c);

    SmartForest* getBranch(const std::string& keyWord);

    SmartForest* getBranch(const std::vector<Rune>& runes);

    std::unique_ptr<SmartGetWord> getWord(const std::string& str);

    std::unique_ptr<SmartGetWord> getWord(const std::vector<Rune>& runes);

    const std::unordered_map<UChar32, std::unique_ptr<SmartForest>>& getBranches() const {
        return branches;
    }

    void remove(const std::string& word);

    void clear();

    UChar32 getC() const { return c_; }
    void setC(UChar32 c) { c_ = c; }

    uint8_t getStatus() const { return status_; }
    void setStatus(uint8_t status) { status_ = status; }

    const ParamType& getParam() const { return param_; }
    void setParam(const ParamType& param) { param_ = param; }

    double getRate() const { return rate_; }
    void setRate(double rate) { rate_ = rate; }

    bool operator<(const SmartForest& other) const { return c_ < other.c_; }

    struct Compare {
        bool operator()(const std::unique_ptr<SmartForest>& a,
                        const std::unique_ptr<SmartForest>& b) const {
            return a->c_ < b->c_;
        }
    };

    struct CompareChar {
        bool operator()(const std::unique_ptr<SmartForest>& a, UChar32 c) const {
            return a->c_ < c;
        }

        bool operator()(UChar32 c, const std::unique_ptr<SmartForest>& a) const {
            return c < a->c_;
        }
    };

    void print(int depth = 0) const;

private:
    double rate_;
    UChar32 c_;
    uint8_t status_;
    ParamType param_;

    int getIndex(UChar32 c);

    bool contains(UChar32 c);

    int compareTo(UChar32 c) const;

    bool equals(UChar32 c) const;

    static std::vector<Rune> utf8_to_runes(const std::string& utf8_str);
};

using PolyphoneForest = SmartForest;

} // namespace doris::segment_v2::inverted_index
