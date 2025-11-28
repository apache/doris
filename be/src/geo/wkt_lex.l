/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

%{

#include "common/logging.h"

namespace doris {
class GeoShape;
}

#include <stdlib.h>
/* wkt_parse_ctx and wkt_parse_type must be ahead of wkt_yacc */
#include "geo/wkt_parse_ctx.h"
#include "geo/wkt_parse_type.h"
#include "geo/wkt_yacc.y.hpp"
#define YY_EXTRA_TYPE  WktParseContext*

#undef YY_DECL
#define YY_DECL wkt_::parser::symbol_type wkt_lex(WktParseContext* ctx, yyscan_t yyscanner)

%}

%option case-insensitive
%option reentrant
%option never-interactive
%option prefix="wkt_"
%option noyywrap
%option nounput
%option noinput

%%

-?(([0-9]+\.?)|([0-9]*\.?[0-9]+)([eE][-+]?[0-9]+)?) {
    return wkt_::parser::make_NUMERIC(atof(yytext));
}

MULTIPOLYGON { return wkt_::parser::make_KW_MULTI_POLYGON(); }
MULTILINESTRING { return wkt_::parser::make_KW_MULTI_LINESTRING(); }
MULTIPOINT { return wkt_::parser::make_KW_MULTI_POINT(); }
POLYGON { return wkt_::parser::make_KW_POLYGON(); }
LINESTRING { return wkt_::parser::make_KW_LINESTRING(); }
POINT { 
    return wkt_::parser::make_KW_POINT(); }

\( |
\) |
,  |
\; { return yytext[0]; }

[ \t\n\r]+ { /* ignore whitespace */ }

. { yyterminate(); }

%%
