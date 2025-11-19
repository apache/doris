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

#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter.h"
#include "olap/rowset/segment_v2/inverted_index/char_filter/char_filter_factory.h"

namespace doris::segment_v2::inverted_index {

class EmptyCharFilter : public DorisCharFilter {
public:
    EmptyCharFilter(ReaderPtr in) : DorisCharFilter(std::move(in)) {}
    ~EmptyCharFilter() override = default;

    void initialize() override {}

    void init(const void* _value, int32_t _length, bool copyData) override {
        _reader->init(_value, _length, copyData);
    }

    int32_t read(const void** start, int32_t min, int32_t max) override {
        return _reader->read(start, min, max);
    }

    int32_t readCopy(void* start, int32_t off, int32_t len) override {
        return _reader->readCopy(start, off, len);
    }

    size_t size() override { return _reader->size(); }
};

class EmptyCharFilterFactory : public CharFilterFactory {
public:
    EmptyCharFilterFactory() = default;
    ~EmptyCharFilterFactory() override = default;

    void initialize(const Settings& settings) override {}

    ReaderPtr create(const ReaderPtr& reader) override {
        auto filter = std::make_shared<EmptyCharFilter>(reader);
        filter->initialize();
        return filter;
    }
};

} // namespace doris::segment_v2::inverted_index