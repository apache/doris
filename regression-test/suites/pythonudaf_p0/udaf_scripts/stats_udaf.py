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
Statistical Aggregate Functions for Doris Python UDAF
Includes: Variance, StdDev, Median, CollectList, Range, GeometricMean, WeightedAvg
"""

import math


# ========================================
# Variance UDAF
# ========================================
class VarianceUDAF:
    """Calculate population variance"""
    
    def __init__(self):
        self.count = 0
        self.sum_val = 0.0
        self.sum_sq = 0.0
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum_val, self.sum_sq)
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum_val += value
            self.sum_sq += value * value
    
    def merge(self, other_state):
        other_count, other_sum, other_sum_sq = other_state
        self.count += other_count
        self.sum_val += other_sum
        self.sum_sq += other_sum_sq
    
    def finish(self):
        if self.count == 0:
            return None
        mean = self.sum_val / self.count
        variance = (self.sum_sq / self.count) - (mean * mean)
        return variance


# ========================================
# Standard Deviation UDAF
# ========================================
class StdDevUDAF:
    """Calculate population standard deviation"""
    
    def __init__(self):
        self.count = 0
        self.sum_val = 0.0
        self.sum_sq = 0.0
    
    @property
    def aggregate_state(self):
        return (self.count, self.sum_val, self.sum_sq)
    
    def accumulate(self, value):
        if value is not None:
            self.count += 1
            self.sum_val += value
            self.sum_sq += value * value
    
    def merge(self, other_state):
        other_count, other_sum, other_sum_sq = other_state
        self.count += other_count
        self.sum_val += other_sum
        self.sum_sq += other_sum_sq
    
    def finish(self):
        if self.count == 0:
            return None
        mean = self.sum_val / self.count
        variance = (self.sum_sq / self.count) - (mean * mean)
        return math.sqrt(max(0, variance))


# ========================================
# Median UDAF
# ========================================
class MedianUDAF:
    """Calculate median value"""
    
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


# ========================================
# Collect List UDAF
# ========================================
class CollectListUDAF:
    """Collect values into a sorted, comma-separated string"""
    
    def __init__(self):
        self.items = []
    
    @property
    def aggregate_state(self):
        return self.items
    
    def accumulate(self, value):
        if value is not None:
            self.items.append(value)
    
    def merge(self, other_state):
        if other_state:
            self.items.extend(other_state)
    
    def finish(self):
        if not self.items:
            return None
        return ','.join(sorted(self.items))


# ========================================
# Range (Max - Min) UDAF
# ========================================
class RangeUDAF:
    """Calculate range (max - min)"""
    
    def __init__(self):
        self.min_val = None
        self.max_val = None
    
    @property
    def aggregate_state(self):
        return (self.min_val, self.max_val)
    
    def accumulate(self, value):
        if value is not None:
            if self.min_val is None or value < self.min_val:
                self.min_val = value
            if self.max_val is None or value > self.max_val:
                self.max_val = value
    
    def merge(self, other_state):
        other_min, other_max = other_state
        if other_min is not None:
            if self.min_val is None or other_min < self.min_val:
                self.min_val = other_min
        if other_max is not None:
            if self.max_val is None or other_max > self.max_val:
                self.max_val = other_max
    
    def finish(self):
        if self.min_val is None or self.max_val is None:
            return None
        return self.max_val - self.min_val


# ========================================
# Geometric Mean UDAF
# ========================================
class GeometricMeanUDAF:
    """Calculate geometric mean using log transformation"""
    
    def __init__(self):
        self.log_sum = 0.0
        self.count = 0
    
    @property
    def aggregate_state(self):
        return (self.log_sum, self.count)
    
    def accumulate(self, value):
        if value is not None and value > 0:
            self.log_sum += math.log(value)
            self.count += 1
    
    def merge(self, other_state):
        other_log_sum, other_count = other_state
        self.log_sum += other_log_sum
        self.count += other_count
    
    def finish(self):
        if self.count == 0:
            return None
        return math.exp(self.log_sum / self.count)


# ========================================
# Weighted Average UDAF
# ========================================
class WeightedAvgUDAF:
    """Calculate weighted average"""
    
    def __init__(self):
        self.weighted_sum = 0.0
        self.weight_sum = 0
    
    @property
    def aggregate_state(self):
        return (self.weighted_sum, self.weight_sum)
    
    def accumulate(self, value, weight):
        if value is not None and weight is not None and weight > 0:
            self.weighted_sum += value * weight
            self.weight_sum += weight
    
    def merge(self, other_state):
        other_weighted_sum, other_weight_sum = other_state
        self.weighted_sum += other_weighted_sum
        self.weight_sum += other_weight_sum
    
    def finish(self):
        if self.weight_sum == 0:
            return None
        return self.weighted_sum / self.weight_sum
