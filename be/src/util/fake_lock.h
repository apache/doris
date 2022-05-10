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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/fake-lock.h
// and modified by Doris

#pragma once

#include "gutil/macros.h"

namespace doris {

// Implementation of Boost's lockable interface that does nothing. Used to replace an
// actual lock implementation in template classes in if no thread safety is needed.
class FakeLock {
public:
    FakeLock() {}
    void lock() {}
    void unlock() {}
    bool try_lock() { return true; }

private:
    DISALLOW_COPY_AND_ASSIGN(FakeLock);
};

} // namespace doris
