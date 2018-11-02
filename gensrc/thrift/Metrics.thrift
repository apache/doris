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
  UNIT,
  // Rate of a dimensionless numerical quantity
  UNIT_PER_SECOND,
  CPU_TICKS,
  BYTES
  BYTES_PER_SECOND,
  TIME_NS,
  DOUBLE_VALUE,
  // No units at all, may not be a numerical quantity
  NONE,
  TIME_MS,
  TIME_S
}

// The kind of value that a metric represents.
enum TMetricKind {
  // May go up or down over time
  GAUGE,
  // A strictly increasing value
  COUNTER,
  // Fixed; will never change
  PROPERTY,
  STATS,
  SET,
  HISTOGRAM
}
