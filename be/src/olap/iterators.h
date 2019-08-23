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

#include "common/status.h"

namespace doris {

class RowCursor;
class RowBlockV2;
class Schema;
class Conditions;

struct StorageReadOptions {
    // lower_bound defines the smallest key at which iterator will
    // return data.
    // If lower_bound is null, won't return
    std::shared_ptr<RowCursor> lower_bound;

    // If include_lower_bound is true, data equal with lower_bound will
    // be read
    bool include_lower_bound = false;

    // upper_bound defines the extend upto which the iterator can return
    // data.
    std::shared_ptr<RowCursor> upper_bound;

    // If include_upper_bound is true, data equal with upper_bound will
    // be read
    bool include_upper_bound = false;

    // reader's column predicates
    // used by zone map/bloom filter/secondary index to prune data
    std::shared_ptr<Conditions> conditions;
};

// Used to read data in RowBlockV2 one by one
class RowwiseIterator {
public:
    RowwiseIterator() { }
    virtual ~RowwiseIterator() { }

    // Initialize this iterator and make it ready to read with
    // input options.
    // Input options may contain scan range in which this scan.
    // Return Status::OK() if init successfully,
    // Return other error otherwise
    virtual Status init(const StorageReadOptions& opts) = 0;

    // If there is any valid data, this function will load data
    // into input batch with Status::OK() returned
    // If there is no data to read, will return Status::EndOfFile.
    // If other error happens, other error code will be returned.
    virtual Status next_batch(RowBlockV2* block) = 0;

    // return schema for this Iterator
    virtual const Schema& schema() const = 0;
};

}
