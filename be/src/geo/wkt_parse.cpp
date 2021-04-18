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

#include "geo/geo_types.h"
#include "geo/wkt_parse_ctx.h"
#include "geo/wkt_parse_type.h"
#include "geo/wkt_yacc.y.hpp"
#define YYSTYPE WKT_STYPE
#define YY_EXTRA_TYPE WktParseContext*
#include "geo/wkt_lex.l.h"

namespace doris {

GeoParseStatus WktParse::parse_wkt(const char* str, size_t len, GeoShape** shape) {
    WktParseContext ctx;
    // initialize lexer
    wkt_lex_init_extra(&ctx, &ctx.scaninfo);
    wkt__scan_bytes(str, len, ctx.scaninfo);

    // parse
    auto res = wkt_parse(&ctx);
    wkt_lex_destroy(ctx.scaninfo);
    if (res == 0) {
        *shape = ctx.shape;
    } else {
        if (ctx.parse_status == GEO_PARSE_OK) {
            ctx.parse_status = GEO_PARSE_WKT_SYNTAX_ERROR;
        }
    }
    return ctx.parse_status;
}

} // namespace doris
