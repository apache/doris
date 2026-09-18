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

#pragma once

#include <string>

#include "common/status.h"
#include "core/field.h"
#include "storage/key_coder.h"
#include "storage/olap_common.h"

// Query-value encoding shared by every BKD reader, CLucene-backed or SNII-native.
//
// It lives in its own header because BOTH readers must encode a query value with
// the key coder of the INDEX's own field type (INV-1). An index encoded with one
// coder and probed with another is self-consistent -- every byte round-trips --
// but compares in the wrong order, and no round-trip test can see it. One
// definition is the only way the two readers cannot drift.
//
// The +/- infinity sentinels are deliberately NOT here: they are an artifact of
// the CLucene visitor, whose bounds are always closed and whose strictness lives
// in matches(). The SNII-native reader carries strictness on the interval itself
// and leaves an open side unbounded, so it never needs them.
namespace doris {

Status encode_bkd_field_ascending(FieldType ft, const Field& field, const KeyCoder* coder,
                                  std::string* out);

} // namespace doris
