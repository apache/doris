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

#include <iostream>
#include <string>

namespace doris::segment_v2 {

class TermItem {
public:
    // Constructor matching Java version
    TermItem(const std::string& term, int start_offset, int end_offset, int position)
            : term(term), start_offset(start_offset), end_offset(end_offset), position(position) {}

    // Default constructor
    TermItem() : start_offset(0), end_offset(0), position(0) {}

    // Copy constructor
    TermItem(const TermItem& other) = default;

    // Assignment operator
    TermItem& operator=(const TermItem& other) = default;

    // Destructor
    ~TermItem() = default;

    // toString equivalent - returns the term
    std::string toString() const { return term; }

    // Comparison operators for sorting (implementing Comparable<TermItem>)
    // Match Java's compareTo logic: sort by position first, then by term length when positions are equal (shorter first)
    bool operator<(const TermItem& other) const {
        if (this->position != other.position) {
            return this->position < other.position;
        }
        // When positions are equal, sort lexicographically (matching Java's string comparison logic)
        return this->term < other.term;
    }

    bool operator>(const TermItem& other) const { return this->position > other.position; }

    bool operator==(const TermItem& other) const { return this->position == other.position; }

    bool operator!=(const TermItem& other) const { return !(*this == other); }

    bool operator<=(const TermItem& other) const { return (*this < other) || (*this == other); }

    bool operator>=(const TermItem& other) const { return (*this > other) || (*this == other); }

    // Public member variables (matching Java public fields)
    std::string term;
    int start_offset;
    int end_offset;
    int position;
};

} // namespace doris::segment_v2