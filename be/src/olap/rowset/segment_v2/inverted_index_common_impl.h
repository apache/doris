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

#include <CLucene.h>

#include "common/logging.h"
#include "olap/rowset/segment_v2/inverted_index_common.h"

namespace doris::segment_v2 {

template <typename... Args>
TermPtr make_term_ptr(Args&&... args) {
    return TermPtr(new lucene::index::Term(std::forward<Args>(args)...));
}

template <typename... Args>
TermDocsPtr make_term_doc_ptr(lucene::index::IndexReader* reader, Args&&... args) {
    return TermDocsPtr(reader->termDocs(std::forward<Args>(args)...));
}

template <typename... Args>
TermPositionsPtr make_term_positions_ptr(lucene::index::IndexReader* reader, Args&&... args) {
    return TermPositionsPtr(reader->termPositions(std::forward<Args>(args)...));
}

template <typename PtrType>
    requires HasClose<PtrType>
void finally_close(PtrType& resource, ErrorContext& error_context) {
    if (resource) {
        try {
            resource->close();
        } catch (CLuceneError& err) {
            error_context.eptr = std::current_exception();
            error_context.err_msg.append("Error occurred while closing resource: ");
            error_context.err_msg.append(err.what());
            LOG(ERROR) << error_context.err_msg;
        } catch (...) {
            error_context.eptr = std::current_exception();
            error_context.err_msg.append("Error occurred while closing resource");
            LOG(ERROR) << error_context.err_msg;
        }
    }
}

} // namespace doris::segment_v2
