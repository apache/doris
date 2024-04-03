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

#include "prefix_query.h"

namespace doris::segment_v2 {

void PrefixQuery::get_prefix_terms(IndexReader* reader, const std::wstring& field_name,
                                   const std::string& prefix,
                                   std::vector<CL_NS(index)::Term*>& prefix_terms,
                                   int32_t max_expansions) {
    std::wstring ws_prefix = StringUtil::string_to_wstring(prefix);

    Term* prefix_term = _CLNEW Term(field_name.c_str(), ws_prefix.c_str());
    TermEnum* enumerator = reader->terms(prefix_term);

    int32_t count = 0;
    Term* lastTerm = nullptr;
    try {
        const TCHAR* prefixText = prefix_term->text();
        const TCHAR* prefixField = prefix_term->field();
        const TCHAR* tmp = nullptr;
        size_t i = 0;
        size_t prefixLen = prefix_term->textLength();
        do {
            lastTerm = enumerator->term();
            if (lastTerm != nullptr && lastTerm->field() == prefixField) {
                size_t termLen = lastTerm->textLength();
                if (prefixLen > termLen) {
                    break;
                }

                tmp = lastTerm->text();

                for (i = prefixLen - 1; i != -1; --i) {
                    if (tmp[i] != prefixText[i]) {
                        tmp = nullptr;
                        break;
                    }
                }
                if (tmp == nullptr) {
                    break;
                }

                if (max_expansions > 0 && count >= max_expansions) {
                    break;
                }

                Term* t = _CLNEW Term(field_name.c_str(), tmp);
                prefix_terms.push_back(t);
                count++;
            } else {
                break;
            }
            _CLDECDELETE(lastTerm);
        } while (enumerator->next());
    }
    _CLFINALLY({
        enumerator->close();
        _CLDELETE(enumerator);
        _CLDECDELETE(lastTerm);
        _CLDECDELETE(prefix_term);
    });
}

} // namespace doris::segment_v2