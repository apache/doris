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

#ifndef DORIS_BE_SRC_COMMON_UTIL_HTTP_PARSER_H
#define DORIS_BE_SRC_COMMON_UTIL_HTTP_PARSER_H

#include <stdint.h>
#include <stddef.h>
#include <ostream>

namespace doris {

struct HttpChunkParseCtx {
    int state;     // Parse state
    size_t size;   // Chunk size
    size_t length; // minimal length need to read
    HttpChunkParseCtx() : state(0), size(0), length(0) {}
};

std::ostream& operator<<(std::ostream& os, const HttpChunkParseCtx& ctx);

class HttpParser {
public:
    enum ParseState {
        PARSE_OK,    // reach trunk data, you can read data
        PARSE_AGAIN, // continue call this function
        PARSE_DONE,  // trunk is over
        PARSE_ERROR, // parse failed.
    };

    // Used to parse http Chunked Transfer Coding(rfc2616 3.6.1)
    // Params:
    //  buf             network buffer's pointer, changed when receive data
    //                  when this function return, *buf point to position where
    //                  next call will begin processing
    //  buf_len         length of receive http data
    //  ctx             parse context, used to save parse state and data size
    //
    // Returns:
    //  PARSE_OK        return this means we reach chunk-data, *buf point to begin of data
    //                  size of chunk-data is saved in ctx->size. Caller need subtract size
    //                  consumed from ctx->size before next call of this function.
    //  PARSE_AGAIN     return this means that caller need to call this function with new data
    //                  from network
    //  PARSE_DONE      All of chunks readed
    //  PARSE_ERROR     Error happened
    static ParseState http_parse_chunked(const uint8_t** buf, const int64_t buf_len,
                                         HttpChunkParseCtx* ctx);
};

} // namespace doris

#endif
