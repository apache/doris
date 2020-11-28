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

#include "http/http_parser.h"

namespace doris {

std::ostream& operator<<(std::ostream& os, const HttpChunkParseCtx& ctx) {
    os << "HttpChunkParseCtx("
       << "state=" << ctx.state << "size=" << ctx.size << "length=" << ctx.length << ")";
    return os;
}

#define CR ((uint8_t)'\r')
#define LF ((uint8_t)'\n')

HttpParser::ParseState HttpParser::http_parse_chunked(const uint8_t** buf, const int64_t buf_len,
                                                      HttpChunkParseCtx* ctx) {
    enum ChunkParseState {
        SW_CHUNK_START = 0,
        SW_CHUNK_SIZE,
        SW_CHUNK_EXTENSION,
        SW_CHUNK_EXTENSION_ALMOST_DONE,
        SW_CHUNK_DATA,
        SW_AFTER_DATA,
        SW_AFTER_DATA_ALMOST_DONE,
        SW_LAST_CHUNK_EXTENSION,
        SW_LAST_CHUNK_EXTENSION_ALMOST_DONE,
        SW_TRAILER,
        SW_TRAILER_ALMOST_DONE,
        SW_TRAILER_HEADER,
        SW_TRAILER_HEADER_ALMOST_DONE
    } state;

    // from last state
    state = (ChunkParseState)ctx->state;
    ParseState rc = PARSE_AGAIN;
    if (state == SW_CHUNK_DATA && ctx->size == 0) {
        state = SW_AFTER_DATA;
    }

    const uint8_t* pos = *buf;
    const uint8_t* end = *buf + buf_len;
    for (; pos < end; pos++) {
        uint8_t ch = *pos;
        uint8_t c = ch;
        bool break_loop = false;
        switch (state) {
        case SW_CHUNK_START:
            if (ch >= '0' && ch <= '9') {
                state = SW_CHUNK_SIZE;
                ctx->size = ch - '0';
                break;
            }
            c = ch | 0x20;
            if (c >= 'a' && c <= 'f') {
                state = SW_CHUNK_SIZE;
                ctx->size = c - 'a' + 10;
                break;
            }
            // Invalid state
            return PARSE_ERROR;
        case SW_CHUNK_SIZE:
            if (ch >= '0' && ch <= '9') {
                ctx->size = ctx->size * 16 + (ch - '0');
                break;
            }
            c = ch | 0x20;
            if (c >= 'a' && c <= 'f') {
                ctx->size = ctx->size * 16 + (c - 'a' + 10);
                break;
            }
            // handle last check
            if (ctx->size == 0) {
                switch (ch) {
                case CR:
                    state = SW_LAST_CHUNK_EXTENSION_ALMOST_DONE;
                    break;
                case LF:
                    state = SW_TRAILER;
                    break;
                case ';':
                case ' ':
                case '\t':
                    state = SW_LAST_CHUNK_EXTENSION;
                    break;
                default:
                    // Invalid state
                    return PARSE_ERROR;
                }
                break;
            }
            // Check if size over
            switch (ch) {
            case CR:
                state = SW_CHUNK_EXTENSION_ALMOST_DONE;
                break;
            case LF:
                state = SW_CHUNK_DATA;
                break;
            case ';':
            case ' ':
            case '\t':
                state = SW_CHUNK_EXTENSION;
                break;
            default:
                return PARSE_ERROR;
            }
            break;
        case SW_CHUNK_EXTENSION:
            switch (ch) {
            case CR:
                state = SW_CHUNK_EXTENSION_ALMOST_DONE;
                break;
            case LF:
                state = SW_CHUNK_DATA;
                break;
            default:
                // just swallow this character
                break;
            }
            break;
        case SW_CHUNK_EXTENSION_ALMOST_DONE:
            if (ch == LF) {
                state = SW_CHUNK_DATA;
                break;
            }
            return PARSE_ERROR;
        case SW_CHUNK_DATA:
            rc = PARSE_OK;
            break_loop = true;
            break;
        case SW_AFTER_DATA:
            switch (ch) {
            case CR:
                state = SW_AFTER_DATA_ALMOST_DONE;
                break;
            case LF:
                // Next chunk
                state = SW_CHUNK_START;
                break;
            default:
                // just swallow
                break;
            }
            break;
        case SW_AFTER_DATA_ALMOST_DONE:
            if (ch == LF) {
                state = SW_CHUNK_START;
                break;
            }
            return PARSE_ERROR;
        case SW_LAST_CHUNK_EXTENSION:
            switch (ch) {
            case CR:
                state = SW_LAST_CHUNK_EXTENSION_ALMOST_DONE;
                break;
            case LF:
                state = SW_TRAILER;
                break;
            default:
                // Just swallow
                break;
            }
            break;
        case SW_LAST_CHUNK_EXTENSION_ALMOST_DONE:
            if (ch == LF) {
                state = SW_TRAILER;
                break;
            }
            return PARSE_ERROR;
        case SW_TRAILER:
            switch (ch) {
            case CR:
                state = SW_TRAILER_ALMOST_DONE;
                break;
            case LF:
                // Over
                ctx->state = 0;
                *buf = pos + 1;
                return PARSE_DONE;
            default:
                state = SW_TRAILER_HEADER;
            }
            break;
        case SW_TRAILER_ALMOST_DONE:
            if (ch == LF) {
                ctx->state = 0;
                *buf = pos + 1;
                return PARSE_DONE;
            }
            return PARSE_ERROR;
        case SW_TRAILER_HEADER:
            switch (ch) {
            case CR:
                state = SW_TRAILER_HEADER_ALMOST_DONE;
                break;
            case LF:
                state = SW_TRAILER;
                break;
            default:
                break;
            }
            break;
        case SW_TRAILER_HEADER_ALMOST_DONE:
            if (ch == LF) {
                state = SW_TRAILER;
                break;
            }
            return PARSE_ERROR;
        }
        if (break_loop) {
            break;
        }
    }

    ctx->state = state;
    *buf = pos;

    // Set length have
    switch (state) {
    case SW_CHUNK_START:
        ctx->length = 3 /* "0" LF LF */;
        break;
    case SW_CHUNK_SIZE:
        ctx->length = 1                            /* LF */
                      + (ctx->size ? ctx->size + 4 /* LF "0" LF LF */
                                   : 1 /* LF */);
        break;
    case SW_CHUNK_EXTENSION:
    case SW_CHUNK_EXTENSION_ALMOST_DONE:
        ctx->length = 1 /* LF */ + ctx->size + 4 /* LF "0" LF LF */;
        break;
    case SW_CHUNK_DATA:
        ctx->length = ctx->size + 4 /* LF "0" LF LF */;
        break;
    case SW_AFTER_DATA:
    case SW_AFTER_DATA_ALMOST_DONE:
        ctx->length = 4 /* LF "0" LF LF */;
        break;
    case SW_LAST_CHUNK_EXTENSION:
    case SW_LAST_CHUNK_EXTENSION_ALMOST_DONE:
        ctx->length = 2 /* LF LF */;
        break;
    case SW_TRAILER:
    case SW_TRAILER_ALMOST_DONE:
        ctx->length = 1 /* LF */;
        break;
    case SW_TRAILER_HEADER:
    case SW_TRAILER_HEADER_ALMOST_DONE:
        ctx->length = 2 /* LF LF */;
        break;
    }

    return rc;
}

} // namespace doris
