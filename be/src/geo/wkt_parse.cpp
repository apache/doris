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

#include "geo/wkt_parse.h"

#include <utility>

#include "geo/wkt_lex.l.h"
#include "geo/wkt_parse_ctx.h"
#include "geo/wkt_parse_type.h" // IWYU pragma: keep
#include "geo/wkt_yacc.y.hpp"

extern int wkt_lex_init_extra(WktParseContext*, yyscan_t*);

namespace doris {
#include "common/compile_check_avoid_begin.h"

GeoParseStatus WktParse::parse_wkt(const char* str, size_t len, std::unique_ptr<GeoShape>& shape) {
    WktParseContext ctx;

    if (wkt_parse(str, len, ctx) == GEO_PARSE_OK) {
        shape = std::move(ctx.shape);
    } else if (ctx.parse_status == GEO_PARSE_OK) {
        /// For Syntax errors(e.g., `POIN(1 2)`, `POINT(1, 2)`)
        /// wkt_parse won't set parse_status, so we need to set it here.
        /// For semantic errors (e.g., invalid coordinates)
        /// wkt_parse will be set to GEO_PARSE_COORD_INVALID
        ctx.parse_status = GEO_PARSE_WKT_SYNTAX_ERROR;
    }
    return ctx.parse_status;
}

int WktParse::wkt_parse(const char* str, size_t len, WktParseContext& ctx) {
    wkt_lex_init_extra(&ctx, &ctx.scaninfo);
    YY_BUFFER_STATE buffer = wkt__scan_bytes(str, len, ctx.scaninfo);

    wkt_::parser parser(&ctx, ctx.scaninfo);
    int res = parser.parse();
    wkt__delete_buffer(buffer, ctx.scaninfo);
    wkt_lex_destroy(ctx.scaninfo);
    return res;
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
