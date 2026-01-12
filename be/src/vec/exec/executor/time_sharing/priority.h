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
#include <atomic>
#include <sstream>
#include <string>

namespace doris {
namespace vectorized {

/**
 * Task (and split) priority is composed of a level and a within-level
 * priority. Level decides which queue the split is placed in, while
 * within-level priority decides which split is executed next in that level.
 * <p>
 * Tasks move from a lower to higher level as they exceed level thresholds
 * of total scheduled time accrued to a task.
 * <p>
 * The priority within a level increases with the scheduled time accumulated
 * in that level. This is necessary to achieve fairness when tasks acquire
 * scheduled time at varying rates.
 * <p>
 * However, this priority is <b>not</b> equal to the task total accrued
 * scheduled time. When a task graduates to a higher level, the level
 * priority is set to the minimum current priority in the new level. This
 * allows us to maintain instantaneous fairness in terms of scheduled time.
 */
class Priority {
public:
    Priority(int level, int64_t level_priority) : _level(level), _level_priority(level_priority) {}

    int level() const { return _level; }
    int64_t level_priority() const { return _level_priority; }

    bool operator<(const Priority& other) const {
        if (_level != other._level) return _level < other._level;
        return _level_priority < other._level_priority;
    }

    std::string to_string() const {
        std::ostringstream os;
        os << "Priority(level=" << _level << ", priority=" << _level_priority << ")";
        return os.str();
    }

private:
    int _level;
    int64_t _level_priority;
};

} // namespace vectorized
} // namespace doris
