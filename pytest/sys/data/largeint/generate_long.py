#!/bin/env python
# -*- coding: utf-8 -*-
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

"""
generate largeint case test data
Date: 2015/05/27 11:56:23
"""

import sys
import random

num = 300000000
palo_file = open("long_10G.txt", "w")
max_value = 2 ** 63 - 1
min_value = - (2 ** 63) + 1

for i in range(num):
    k = random.randint(min_value, max_value)
    v = random.randint(min_value, max_value)
    palo_file.write("%d\t%d\n" % (k, v))

