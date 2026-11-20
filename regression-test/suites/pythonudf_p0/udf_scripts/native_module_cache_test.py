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


import builtins


# Keep the counter outside this module so an unintended reimport is observable.
builtins._doris_native_module_loads = getattr(builtins, "_doris_native_module_loads", 0) + 1


def evaluate(value):
    if value is None:
        return None
    # This dependency must remain importable after the loader returns.
    from native_module_cache_helper import add_offset
    return builtins._doris_native_module_loads * 1000 + add_offset(value)
