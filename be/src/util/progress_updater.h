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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/progress-updater.h
// and modified by Doris

#ifndef DORIS_BE_SRC_COMMON_UTIL_PROGRESS_UPDATER_H
#define DORIS_BE_SRC_COMMON_UTIL_PROGRESS_UPDATER_H

#include <string>

namespace doris {

// Utility class to update progress.  This is split out so a different
// logging level can be set for these updates (GLOG_module)
// This class is thread safe.
// Example usage:
//   ProgressUpdater updater("Task", 100, 10);  // 100 items, print every 10%
//   updater.Update(15);  // 15 done, prints 15%
//   updater.Update(3);   // 18 done, doesn't print
//   update.Update(5);    // 23 done, prints 23%
class ProgressUpdater {
public:
    // label - label that is printed with each update.
    // max - maximum number of work items
    // update_period - how often the progress is spewed expressed as a percentage
    ProgressUpdater(const std::string& label, int64_t max, int update_period);

    ProgressUpdater();

    // 'delta' more of the work has been complete.  Will potentially output to
    // VLOG_PROGRESS
    void update(int64_t delta);

    // Returns if all tasks are done.
    bool done() const { return _num_complete >= _total; }

    int64_t total() const { return _total; }
    int64_t num_complete() const { return _num_complete; }

private:
    std::string _label;
    int64_t _total;
    int _update_period;
    int64_t _num_complete;
    int _last_output_percentage;
};

} // namespace doris

#endif
