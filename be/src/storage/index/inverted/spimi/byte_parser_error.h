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

// SPIMI byte-parser hard-fail helper. The pure-SPIMI readers
// (`term_dict_reader.cpp`, `term_docs_reader.cpp`, `prox_reader.cpp`,
// `segment_infos_reader.cpp`, `segment_reader.cpp`, `pfor_encoder.cpp`)
// throw `doris::Exception` (NOT CLucene `CLuceneError`) on
// corrupt/malformed input. This decouples the byte parsers from
// CLucene — they live in `spimi/` but are pure Doris code.
//
// The CLucene boundary classes (`clucene_*.cpp`) catch `doris::Exception`
// at every overridden virtual entry point and rethrow as `CLuceneError`
// so the CLucene query engine sees its expected exception type.
//
// Usage:
//   SPIMI_THROW_CORRUPT("SPIMI .tis read past end");
//   SPIMI_THROW_CORRUPT_FMT("SPIMI .frq doc_freq out of range: {}", value);

#include "common/exception.h"
#include "common/status.h"

#define SPIMI_THROW_CORRUPT(msg)                                                                  \
    do {                                                                                          \
        throw ::doris::Exception(                                                                 \
                ::doris::Status::Error<::doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>( \
                        (msg)));                                                                  \
    } while (false)

#define SPIMI_THROW_CORRUPT_FMT(fmt_str, ...)                                                     \
    do {                                                                                          \
        throw ::doris::Exception(                                                                 \
                ::doris::Status::Error<::doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>( \
                        (fmt_str), ##__VA_ARGS__));                                               \
    } while (false)
