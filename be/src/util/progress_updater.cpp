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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/progress-updater.cpp
// and modified by Doris

#include "util/progress_updater.h"

#include "common/logging.h"

namespace doris {

ProgressUpdater::ProgressUpdater(const std::string& label, int64_t total, int period)
        : _label(label),
          _total(total),
          _update_period(period),
          _num_complete(0),
          _last_output_percentage(0) {}

ProgressUpdater::ProgressUpdater()
        : _total(0), _update_period(0), _num_complete(0), _last_output_percentage(0) {}

void ProgressUpdater::update(int64_t delta) {
    DCHECK_GE(delta, 0);

    if (delta == 0) {
        return;
    }

    __sync_fetch_and_add(&_num_complete, delta);

    // Cache some shared variables to avoid locking.  It's possible the progress
    // update is out of order (e.g. prints 1 out of 10 after 2 out of 10)
    double old_percentage = _last_output_percentage;
    int64_t num_complete = _num_complete;

    if (num_complete >= _total) {
        // Always print the final 100% complete
        VLOG_DEBUG << _label << " 100\% Complete (" << num_complete << " out of " << _total << ")";
        return;
    }

    // Convert to percentage as int
    int new_percentage = (static_cast<double>(num_complete) / _total) * 100;

    if (new_percentage - old_percentage > _update_period) {
        // Only update shared variable if this guy was the latest.
        __sync_val_compare_and_swap(&_last_output_percentage, old_percentage, new_percentage);
        VLOG_DEBUG << _label << ": " << new_percentage << "\% Complete (" << num_complete
                   << " out of " << _total << ")";
    }
}
} // namespace doris
