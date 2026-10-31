# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

class SumState:
    def __init__(self):
        self.total = 0
        self.count = 0


class StateObjectUDAF:
    def __init__(self):
        self.state = SumState()

    def accumulate(self, value):
        if value is not None:
            self.state.total += value
            self.state.count += 1

    def merge(self, other_state):
        if type(other_state) is not SumState:
            raise TypeError(f"unexpected state type {type(other_state)}")
        self.state.total += other_state.total
        self.state.count += other_state.count

    def finish(self):
        return self.state.total * 1000 + self.state.count

    @property
    def aggregate_state(self):
        return self.state
