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

class SumAgg:
    def __init__(self):
        from shared_udaf_dependency import State

        self.state = State()

    def init(self):
        from shared_udaf_dependency import State

        self.state = State()

    @property
    def aggregate_state(self):
        return self.state

    def accumulate(self, value):
        from shared_udaf_dependency import add_value

        if value is not None:
            self.state.value = add_value(self.state.value, value)

    def merge(self, other_state):
        from shared_udaf_dependency import merge_value

        if other_state is not None:
            self.state.value = merge_value(self.state.value, other_state.value)

    def finish(self):
        from shared_udaf_dependency import OFFSET

        return self.state.value + OFFSET
