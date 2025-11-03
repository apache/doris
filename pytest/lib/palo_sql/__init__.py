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

###########################################################################
#   @file palo_client.py
#   @date 2023-02-17 17:52:55
#   @brief sql
#
###########################################################################
"""import"""
from .palo_patition import Partition
from .palo_patition import PartitionInfo
from .palo_dynamic_partition_info import DynamicPartitionInfo
from .palo_distribution import DistributionInfo
from .palo_hadoop import HadoopInfo
from .palo_loadinfo import LoadDataInfo
from .palo_routine_load import RoutineLoadProperty

