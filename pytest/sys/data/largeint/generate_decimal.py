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
generate decimal case test data
Date: 2015/06/05 17:11:3333
"""

import sys
import random

precision = 20
scale = 8

decimal_file = open("decimal.txt", "w")

min_integer = 0
max_integer = 0
for i in range(precision - scale):
    max_integer = max_integer + (9 * (10 ** i))

min_fraction = 0
max_fraction = 0
for i in range(scale):
    max_fraction = max_fraction + (9 * (10 ** i))

for i in range(1200):
    integer = random.randint(min_integer, max_integer) 
    fraction = random.randint(min_fraction, max_fraction)
    zero = ""
    for i in range(random.randint(0, scale - len(str(fraction)))):
        zero = "%s0" % zero

    decimal_file.write("%d.%s%d\n" % (integer, zero, fraction))
   
