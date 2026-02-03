#!/usr/bin/env python
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
Python UDAF implementations for testing.
"""


class SumInt:
    """Aggregate function that sums integer values."""
    
    def __init__(self):
        self.sum = 0
    
    @property
    def aggregate_state(self):
        return self.sum
    
    def accumulate(self, value):
        if value is not None:
            self.sum += value
    
    def merge(self, other_state):
        self.sum += other_state
    
    def finish(self):
        return self.sum


class AvgDouble:
    """Aggregate function that calculates average of double values."""
    
    def __init__(self):
        self.count = 0
        self.sum = 0.0
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum)
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum += value
    
    def merge(self, other_state):
        other_count, other_sum = other_state
        self.count += other_count
        self.sum += other_sum
    
    def finish(self):
        if self.count == 0:
            return None
        return self.sum / self.count
