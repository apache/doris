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

#include "util/bitmap.h"

#include <sstream>

using namespace palo;

std::string Bitmap::DebugString(bool print_bits) const {
  int64_t words = BitUtil::round_up(num_bits_, 64) / 64;
  std::stringstream ss;
  ss << "Size (" << num_bits_ << ") words (" << words << ") ";
  if (print_bits) {
    for (int i = 0; i < num_bits(); ++i) {
      if (Get(i)) {
        ss << "1";
      } else {
        ss << "0";
      }
    }
  } else {
    for (auto v : buffer_) {
      ss << v << ".";
    }
  }
  ss << std::endl;
  return ss.str();
}

