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

#include <exception>
#include <memory>
#include <string>

namespace lucene {
namespace store {
class Directory;
} // namespace store

namespace index {
class Term;
class TermDocs;
class TermPositions;
class IndexReader;
} // namespace index
} // namespace lucene

class CLuceneError;

namespace doris::segment_v2 {

struct DirectoryDeleter {
    void operator()(lucene::store::Directory* p) const;
};

struct TermDeleter {
    void operator()(lucene::index::Term* p) const;
};
using TermPtr = std::unique_ptr<lucene::index::Term, TermDeleter>;

template <typename... Args>
TermPtr make_term_ptr(Args&&... args);

struct CLuceneDeleter {
    void operator()(lucene::index::TermDocs* p) const;
};
using TermDocsPtr = std::unique_ptr<lucene::index::TermDocs, CLuceneDeleter>;
using TermPositionsPtr = std::unique_ptr<lucene::index::TermPositions, CLuceneDeleter>;

template <typename... Args>
TermDocsPtr make_term_doc_ptr(lucene::index::IndexReader* reader, Args&&... args);

template <typename... Args>
TermPositionsPtr make_term_positions_ptr(lucene::index::IndexReader* reader, Args&&... args);

struct ErrorContext {
    std::string err_msg;
    std::exception_ptr eptr;
};

template <typename T>
concept HasClose = requires(T t) {
    { t->close() };
};

template <typename PtrType>
    requires HasClose<PtrType>
void finally_close(PtrType& resource, ErrorContext& error_context);

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-macros"
#endif

#define FINALLY_CLOSE(resource)                                                     \
    {                                                                               \
        static_assert(sizeof(error_context) > 0,                                    \
                      "error_context must be defined before using FINALLY macro!"); \
        finally_close(resource, error_context);                                     \
    }

// Return ERROR after finally
#define FINALLY(finally_block)                                                                    \
    {                                                                                             \
        static_assert(sizeof(error_context) > 0,                                                  \
                      "error_context must be defined before using FINALLY macro!");               \
        finally_block;                                                                            \
        if (error_context.eptr) {                                                                 \
            return Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(error_context.err_msg); \
        }                                                                                         \
    }

// Re-throw the exception after finally
#define FINALLY_EXCEPTION(finally_block)                                            \
    {                                                                               \
        static_assert(sizeof(error_context) > 0,                                    \
                      "error_context must be defined before using FINALLY macro!"); \
        finally_block;                                                              \
        if (error_context.eptr) {                                                   \
            std::rethrow_exception(error_context.eptr);                             \
        }                                                                           \
    }

#if defined(__clang__)
#pragma clang diagnostic pop
#endif

} // namespace doris::segment_v2

#include "inverted_index_common_impl.h"