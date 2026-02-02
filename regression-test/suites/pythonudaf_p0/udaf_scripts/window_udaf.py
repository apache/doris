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
Window Function UDAFs for Apache Doris
Advanced window analytics including moving averages, volatility, and percentiles
"""

import math


class MovingAvgUDAF:
    """Simple Moving Average (SMA) UDAF"""
    
    def __init__(self):
        self.values = []
    
    @property
    def aggregate_state(self):
        return self.values
    
    def accumulate(self, value):
        if value is not None:
            self.values.append(value)
    
    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)
    
    def finish(self):
        if not self.values:
            return None
        return sum(self.values) / len(self.values)


class WindowStdDevUDAF:
    """Standard Deviation for window volatility analysis"""
    
    def __init__(self):
        self.values = []
    
    @property
    def aggregate_state(self):
        return self.values
    
    def accumulate(self, value):
        if value is not None:
            self.values.append(value)
    
    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)
    
    def finish(self):
        if not self.values or len(self.values) < 2:
            return None
        mean = sum(self.values) / len(self.values)
        variance = sum((x - mean) ** 2 for x in self.values) / len(self.values)
        return math.sqrt(variance)


class LastValueUDAF:
    """Last value in window (for delta calculations)"""
    
    def __init__(self):
        self.last = None
    
    @property
    def aggregate_state(self):
        return self.last
    
    def accumulate(self, value):
        if value is not None:
            self.last = value
    
    def merge(self, other_state):
        if other_state is not None:
            self.last = other_state
    
    def finish(self):
        return self.last


class WindowMinUDAF:
    """Minimum value in window"""
    
    def __init__(self):
        self.min_val = None
    
    @property
    def aggregate_state(self):
        return self.min_val
    
    def accumulate(self, value):
        if value is not None:
            if self.min_val is None or value < self.min_val:
                self.min_val = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.min_val is None or other_state < self.min_val:
                self.min_val = other_state
    
    def finish(self):
        return self.min_val


class WindowMaxUDAF:
    """Maximum value in window"""
    
    def __init__(self):
        self.max_val = None
    
    @property
    def aggregate_state(self):
        return self.max_val
    
    def accumulate(self, value):
        if value is not None:
            if self.max_val is None or value > self.max_val:
                self.max_val = value
    
    def merge(self, other_state):
        if other_state is not None:
            if self.max_val is None or other_state > self.max_val:
                self.max_val = other_state
    
    def finish(self):
        return self.max_val


class Percentile50UDAF:
    """50th percentile (median) calculation in window"""
    
    def __init__(self):
        self.values = []
    
    @property
    def aggregate_state(self):
        return self.values
    
    def accumulate(self, value):
        if value is not None:
            self.values.append(value)
    
    def merge(self, other_state):
        if other_state:
            self.values.extend(other_state)
    
    def finish(self):
        if not self.values:
            return None
        sorted_vals = sorted(self.values)
        n = len(sorted_vals)
        if n % 2 == 0:
            return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2.0
        else:
            return sorted_vals[n//2]
