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

#include <cstdint>
#include <string>
#include <string_view>

#include "common/status.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h"

// Parsers for the mecab-ipadic source files (assumed already transcoded to UTF-8).
// Pure text->struct transforms feeding KuromojiDictionaryBuilder. Build-time only.
namespace doris::segment_v2::kuromoji {

// Maps an IPADIC char.def category name (e.g. "KANJI") to our canonical
// CharCategory ordinal. Returns CAT_CLASS_COUNT for an unknown name.
uint8_t ipadic_category_ordinal(std::string_view name);

// Parse one IPADIC lexicon CSV row: surface,left,right,cost,<feature columns...>.
// `feature` keeps columns 5.. verbatim (already comma-separated).
Status parse_lexicon_line(std::string_view line, std::string* surface, BuilderWord* out);

// Parse the whole matrix.def: header "<forward_size> <backward_size>" then
// "<forward_id> <backward_id> <cost>" lines.
Status parse_matrix_def(std::string_view content, MatrixInput* out);

// Parse the whole char.def: category definitions (NAME INVOKE GROUP LENGTH) and
// code-point mappings (0xXXXX[..0xYYYY] CATEGORY [extra...]).
Status parse_char_def(std::string_view content, CharDefInput* out);

// Parse the whole unk.def: CATEGORY,left,right,cost,<feature columns...> rows.
Status parse_unk_def(std::string_view content, UnkDictInput* out);

} // namespace doris::segment_v2::kuromoji
