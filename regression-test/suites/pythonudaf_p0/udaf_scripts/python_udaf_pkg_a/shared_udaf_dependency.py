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

# Load the namespace package so State.__reduce__ can lazily import its child.
import mypkg

OFFSET = 10
MODULE_ID = "A"


def add_value(total, value):
    return total + value


def merge_value(total, other_value):
    return total + other_value


class State:
    def __init__(self):
        self.value = 0

    def __reduce__(self):
        from mypkg.lazy_state import restore_state

        return restore_state, (self.value, MODULE_ID)
