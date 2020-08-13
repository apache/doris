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

// Converts a human-readable bytes value like '1.23B' or '985.32M' to a number
// of bytes. The suffix must be present: '1.23' is not valid but '1.23B' is.
// Returns -1 if there's some failure.
function toNumBytes(humanReadableBytes) {
  len = humanReadableBytes.length;
  if (len <= 1) {
    return -1;
  }
  unit = humanReadableBytes[len - 1];
  val = parseFloat(humanReadableBytes.substring(0, len - 1));
  if (isNaN(val)) {
    return -1;
  }
  // Fallthrough intended throughout.
  switch (unit) {
    case 'Y': val *= 1024.0; // Enough bytes to handle any double.
    case 'Z': val *= 1024.0;
    case 'E': val *= 1024.0;
    case 'P': val *= 1024.0;
    case 'T': val *= 1024.0;
    case 'G': val *= 1024.0;
    case 'M': val *= 1024.0;
    case 'K': val *= 1024.0;
    case 'B': break;
    default:
      return -1;
  }
  return val;
}

// A comparison function for human-readable byte strings.
function bytesSorter(left, right) {
  if (right.length == 0 && left.length == 0) {
    return 0;
  }
  if (left.length == 0) {
    return -1;
  }
  if (right.length == 0) {
    return 1;
  }
  left_bytes = toNumBytes(left.trim());
  right_bytes = toNumBytes(right.trim());
  if (left_bytes < right_bytes) {
    return -1;
  }
  if (left_bytes > right_bytes) {
    return 1;
  }
  return 0;
}

// Converts numeric strings to numbers and then compares them.
function compareNumericStrings(left, right) {
  left_num = parseInt(left, 10);
  right_num = parseInt(right, 10);
  if (left_num < right_num) {
    return -1;
  }
  if (left_num > right_num) {
    return 1;
  }
  return 0;
}

// A comparison function for human-readable time strings.
//
// The human-readable time format should look like this:
//   "2019-09-06 19:56:46 CST"
//
// Note: the time zones will be ignored since the masters
// should not be deployed across time zones. In addition,
// we compare the time strings unit by unit since it's
// really hard to convert them to timestamps.
function timesSorter(left, right) {
  // "2019-09-06 19:56:46".
  var expect_min_length = 19;
  if (left.length < expect_min_length && right.length < expect_min_length) {
    return 0;
  }
  if (left.length < expect_min_length) {
    return -1;
  }
  if (right.length < expect_min_length) {
    return 1;
  }

  // Year.
  var ret = compareNumericStrings(left.substr(0, 4), right.substr(0, 4));
  if (ret != 0) {
    return ret;
  }
  // Month.
  ret = compareNumericStrings(left.substr(5, 2), right.substr(5, 2));
  if (ret != 0) {
    return ret;
  }
  // Day.
  ret = compareNumericStrings(left.substr(8, 2), right.substr(8, 2));
  if (ret != 0) {
    return ret;
  }
  // Hour.
  ret = compareNumericStrings(left.substr(11, 2), right.substr(11, 2));
  if (ret != 0) {
    return ret;
  }
  // Minute.
  ret = compareNumericStrings(left.substr(14, 2), right.substr(14, 2));
  if (ret != 0) {
    return ret;
  }
  // Second.
  ret = compareNumericStrings(left.substr(17, 2), right.substr(17, 2));
  if (ret != 0) {
    return ret;
  }

  return 0;
}

// A comparison function for strings.
function stringsSorter(left, right) {
  if (left < right) {
    return -1;
  }
  if (left > right) {
    return 1;
  }
  return 0;
}
