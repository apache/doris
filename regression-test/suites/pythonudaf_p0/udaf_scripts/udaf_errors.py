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

"""Module-based UDAF error cases for regression tests."""


class ModuleFinishErrorUDAF:
    """Raise a stable error from finish() to verify propagation."""

    def __init__(self):
        self.count = 0

    @property
    def aggregate_state(self):
        return self.count

    def accumulate(self, value):
        if value is not None:
            self.count += 1

    def merge(self, other_state):
        self.count += other_state

    def reset(self):
        self.count = 0

    def finish(self):
        raise TypeError("module_udaf_error_42")
