// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp doris
namespace java org.apache.doris.thrift

// Metric and counter data types.
enum TUnit {
  // A dimensionless numerical quantity
  UNIT = 0,
  // Rate of a dimensionless numerical quantity
  UNIT_PER_SECOND = 1,
  CPU_TICKS = 2,
  BYTES = 3,
  BYTES_PER_SECOND = 4,
  TIME_NS = 5,
  DOUBLE_VALUE = 6,
  // No units at all, may not be a numerical quantity
  // It is used as a label now, so do not treat it as 
  // a real counter.
  NONE = 7,
  TIME_MS = 8,
  TIME_S = 9
}

// The kind of value that a metric represents.
enum TMetricKind {
  // May go up or down over time
  GAUGE = 0,
  // A strictly increasing value
  COUNTER = 1,
  // Fixed; will never change
  PROPERTY = 2,
  STATS = 3,
  SET = 4,
  HISTOGRAM = 5
}
