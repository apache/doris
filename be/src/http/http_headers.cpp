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

#include "http/http_headers.h"

namespace doris {

const char* HttpHeaders::ACCEPT = "Accept";
const char* HttpHeaders::ACCEPT_CHARSET = "Accept-Charset";
const char* HttpHeaders::ACCEPT_ENCODING = "Accept-Encoding";
const char* HttpHeaders::ACCEPT_LANGUAGE = "Accept-Language";
const char* HttpHeaders::ACCEPT_RANGES = "Accept-Ranges";
const char* HttpHeaders::ACCEPT_PATCH = "Accept-Patch";
const char* HttpHeaders::ACCESS_CONTROL_ALLOW_CREDENTIALS = "Access-Control-Allow-Credentials";
const char* HttpHeaders::ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
const char* HttpHeaders::ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
const char* HttpHeaders::ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
const char* HttpHeaders::ACCESS_CONTROL_EXPOSE_HEADERS = "Access-Control-Expose-Headers";
const char* HttpHeaders::ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";
const char* HttpHeaders::ACCESS_CONTROL_REQUEST_HEADERS = "Access-Control-Request-Headers";
const char* HttpHeaders::ACCESS_CONTROL_REQUEST_METHOD = "Access-Control-Request-Method";
const char* HttpHeaders::AGE = "Age";
const char* HttpHeaders::ALLOW = "Allow";
const char* HttpHeaders::AUTHORIZATION = "Authorization";
const char* HttpHeaders::CACHE_CONTROL = "Cache-Control";
const char* HttpHeaders::CONNECTION = "Connection";
const char* HttpHeaders::CONTENT_BASE = "Content-Base";
const char* HttpHeaders::CONTENT_ENCODING = "Content-Encoding";
const char* HttpHeaders::CONTENT_LANGUAGE = "Content-Language";
const char* HttpHeaders::CONTENT_LENGTH = "Content-Length";
const char* HttpHeaders::CONTENT_LOCATION = "Content-Location";
const char* HttpHeaders::CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding";
const char* HttpHeaders::CONTENT_MD5 = "Content-MD5";
const char* HttpHeaders::CONTENT_RANGE = "Content-Range";
const char* HttpHeaders::CONTENT_TYPE = "Content-Type";
const char* HttpHeaders::COOKIE = "Cookie";
const char* HttpHeaders::DATE = "Date";
const char* HttpHeaders::ETAG = "ETag";
const char* HttpHeaders::EXPECT = "Expect";
const char* HttpHeaders::EXPIRES = "Expires";
const char* HttpHeaders::FROM = "From";
const char* HttpHeaders::HOST = "Host";
const char* HttpHeaders::IF_MATCH = "If-Match";
const char* HttpHeaders::IF_MODIFIED_SINCE = "If-Modified-Since";
const char* HttpHeaders::IF_NONE_MATCH = "If-None-Match";
const char* HttpHeaders::IF_RANGE = "If-Range";
const char* HttpHeaders::IF_UNMODIFIED_SINCE = "If-Unmodified-Since";
const char* HttpHeaders::LAST_MODIFIED = "Last-Modified";
const char* HttpHeaders::LOCATION = "Location";
const char* HttpHeaders::MAX_FORWARDS = "Max-Forwards";
const char* HttpHeaders::ORIGIN = "Origin";
const char* HttpHeaders::PRAGMA = "Pragma";
const char* HttpHeaders::PROXY_AUTHENTICATE = "Proxy-Authenticate";
const char* HttpHeaders::PROXY_AUTHORIZATION = "Proxy-Authorization";
const char* HttpHeaders::RANGE = "Range";
const char* HttpHeaders::REFERER = "Referer";
const char* HttpHeaders::RETRY_AFTER = "Retry-After";
const char* HttpHeaders::SEC_WEBSOCKET_KEY1 = "Sec-WebSocket-Key1";
const char* HttpHeaders::SEC_WEBSOCKET_KEY2 = "Sec-WebSocket-Key2";
const char* HttpHeaders::SEC_WEBSOCKET_LOCATION = "Sec-WebSocket-Location";
const char* HttpHeaders::SEC_WEBSOCKET_ORIGIN = "Sec-WebSocket-Origin";
const char* HttpHeaders::SEC_WEBSOCKET_PROTOCOL = "Sec-WebSocket-Protocol";
const char* HttpHeaders::SEC_WEBSOCKET_VERSION = "Sec-WebSocket-Version";
const char* HttpHeaders::SEC_WEBSOCKET_KEY = "Sec-WebSocket-Key";
const char* HttpHeaders::SEC_WEBSOCKET_ACCEPT = "Sec-WebSocket-Accept";
const char* HttpHeaders::SERVER = "Server";
const char* HttpHeaders::SET_COOKIE = "Set-Cookie";
const char* HttpHeaders::SET_COOKIE2 = "Set-Cookie2";
const char* HttpHeaders::TE = "TE";
const char* HttpHeaders::TRAILER = "Trailer";
const char* HttpHeaders::TRANSFER_ENCODING = "Transfer-Encoding";
const char* HttpHeaders::UPGRADE = "Upgrade";
const char* HttpHeaders::USER_AGENT = "User-Agent";
const char* HttpHeaders::VARY = "Vary";
const char* HttpHeaders::VIA = "Via";
const char* HttpHeaders::WARNING = "Warning";
const char* HttpHeaders::WEBSOCKET_LOCATION = "WebSocket-Location";
const char* HttpHeaders::WEBSOCKET_ORIGIN = "WebSocket-Origin";
const char* HttpHeaders::WEBSOCKET_PROTOCOL = "WebSocket-Protocol";
const char* HttpHeaders::WWW_AUTHENTICATE = "WWW-Authenticate";

const std::string HttpHeaders::JSON_TYPE = "application/json";
const std::string HttpHeaders::AUTH_TOKEN = "Auth-Token";

} // namespace doris
