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

import importlib
import uuid.sub.deep

from uuid import helper
from . import helper as relative_helper


def evaluate(a):
    if a is None:
        return None
    from uuid.helper import OFFSET
    from uuid.sub.deep import DEEP_OFFSET

    if relative_helper is not helper or importlib.import_module("uuid.helper") is not helper:
        raise RuntimeError("uuid.helper of the UDF package was loaded more than once")
    return a + OFFSET + DEEP_OFFSET + uuid.sub.deep.DEEP_OFFSET
