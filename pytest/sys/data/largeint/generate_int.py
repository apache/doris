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
generate int128 case test data
Date: 2015/05/27 11:56:23
"""

import sys
import random

if len(sys.argv) < 2:
    num = 1000
else:
    num = int(sys.argv[1])

palo_file = open("default_value.expected", "w")

for i in range(num):
    palo_file.write("%d\t0\t%d\n" % (i, (2 ** 127 - 1)))

