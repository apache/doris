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

palo_file = open("largeint_1G.txt", "w")
#sql_file = open("int128.sql", "w")
max_value = 2 ** 127 + 1
min_value = - (2 ** 127) - 1

spcial_value_list = [2 ** 63, -(2 ** 63), 2 ** 63 - 1, -(2 ** 63 - 1), \
        2 ** 64, -(2 ** 64), 2 ** 64 - 1, -(2 ** 64 - 1), \
        2 ** 65, -(2 ** 65), 2 ** 65 - 1, -(2 ** 65 - 1), \
        (2 ** 66) + 1, -((2 ** 65) + 1), (2 ** 66 - 1) + 1, -((2 ** 65 - 1) + 1), \
        2 ** 127, -(2 ** 127), 2 ** 127 - 1, -(2 ** 127 - 1), -(2 ** 127 + 1), \
        (2 ** 63 - 1) << 63, -((2 ** 63 - 1) << 63), \
        (2 ** 63 - 1) << 64, -((2 ** 63 - 1) << 64), \
        (2 ** 64 - 1) << 63, -((2 ** 64 - 1) << 63), \
        (1 << 126) + 1, -((1 << 126) + 1), \
        (1 << 126) + (1 << 64), -((1 << 126) + (1 << 64)), \
        (1 << 126) + (1 << 63), -((1 << 126) + (1 << 63)), \
        (1 << 126) + (2 ** 64 - 1), -((1 << 126) + (2 ** 64 - 1)), \
        -1, 0, 1]
i = 0
for v in spcial_value_list:
    i -= 1
    palo_file.write("%d\t%d\t%d\n" % (i, v, v))
    #sql_file.write('INSERT INTO `test`.`test_int128` (`k1`, `k2`, `v`) VALUES (%d, "%d", "%d");\n' \
    #        %(i, v, v))
    i -= 1
    k = random.randint(min_value, max_value)
    palo_file.write("%d\t%d\t%d\n" % (i, k, v))
    #sql_file.write('INSERT INTO `test`.`test_int128` (`k1`, `k2`, `v`) VALUES (%d, "%d", "%d");\n' \
    #        %(i, k, v))
    i -= 1
    k = random.randint(min_value, max_value)
    palo_file.write("%d\t%d\t%d\n" % (i, v, k))
    #sql_file.write('INSERT INTO `test`.`test_int128` (`k1`, `k2`, `v`) VALUES (%d, "%d", "%d");\n' \
    #        %(i, v, k))



for i in range(num):
    k = random.randint(min_value, max_value)
    v = random.randint(min_value, max_value)
    palo_file.write("%d\t%d\t%d\n" % (i, k, v))
    #sql_file.write('INSERT INTO `test`.`test_int128` (`k1`, `k2`, `v`) VALUES (%d, "%d", "%d");\n' \
    #        %(i, k, v))

