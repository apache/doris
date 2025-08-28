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

#include "SurrogatePairSegmenter.h"

namespace doris::segment_v2 {

void SurrogatePairSegmenter::analyze(AnalyzeContext& context) {
    const auto& current_char_type = context.getCurrentCharType();

    if (current_char_type == CharacterUtil::CHAR_SURROGATE) {
        Lexeme newLexeme(context.getBufferOffset(), context.getCurrentCharOffset(),
                         context.getCurrentCharLen(), Lexeme::Type::CNChar, context.getCursor(),
                         context.getCursor());
        context.addLexeme(newLexeme);
    }

    context.unlockBuffer(SEGMENTER_TYPE);
}

void SurrogatePairSegmenter::reset() {}

} // namespace doris::segment_v2