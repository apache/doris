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

#include <CLucene.h> // IWYU pragma: keep

#include <memory>

#include "common/logging.h"

namespace lucene::store {
class Directory;
} // namespace lucene::store

namespace doris::segment_v2 {

struct DirectoryDeleter {
    void operator()(lucene::store::Directory* ptr) const { _CLDECDELETE(ptr); }
};

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