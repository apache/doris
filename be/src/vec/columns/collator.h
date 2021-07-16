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

#include <boost/noncopyable.hpp>
#include <string>
#include <vector>

struct UCollator;

class Collator : private boost::noncopyable {
public:
    explicit Collator(const std::string& locale_);
    ~Collator();

    int compare(const char* str1, size_t length1, const char* str2, size_t length2) const;

    const std::string& get_locale() const;

    static std::vector<std::string> get_available_collations();

private:
    std::string locale;
    UCollator* collator;
};
