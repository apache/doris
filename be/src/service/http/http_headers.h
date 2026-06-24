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

#include <string>

namespace doris {

class HttpHeaders {
public:
    static const char* ACCEPT;
    static const char* ACCEPT_CHARSET;
    static const char* ACCEPT_ENCODING;
    static const char* ACCEPT_LANGUAGE;
    static const char* ACCEPT_RANGES;
    static const char* ACCEPT_PATCH;
    static const char* ACCESS_CONTROL_ALLOW_CREDENTIALS;
    static const char* ACCESS_CONTROL_ALLOW_HEADERS;
    static const char* ACCESS_CONTROL_ALLOW_METHODS;
    static const char* ACCESS_CONTROL_ALLOW_ORIGIN;
    static const char* ACCESS_CONTROL_EXPOSE_HEADERS;
    static const char* ACCESS_CONTROL_MAX_AGE;
    static const char* ACCESS_CONTROL_REQUEST_HEADERS;
    static const char* ACCESS_CONTROL_REQUEST_METHOD;
    static const char* AGE;
    static const char* ALLOW;
    static const char* AUTHORIZATION;
    static const char* CACHE_CONTROL;
    static const char* CONNECTION;
    static const char* CONTENT_BASE;
    static const char* CONTENT_ENCODING;
    static const char* CONTENT_LANGUAGE;
    static const char* CONTENT_LENGTH;
    static const char* CONTENT_LOCATION;
    static const char* CONTENT_TRANSFER_ENCODING;
    static const char* CONTENT_MD5;
    static const char* CONTENT_RANGE;
    static const char* CONTENT_TYPE;
    static const char* COOKIE;
    static const char* DATE;
    static const char* ETAG;
    static const char* EXPECT;
    static const char* EXPIRES;
    static const char* FROM;
    static const char* HOST;
    static const char* IF_MATCH;
    static const char* IF_MODIFIED_SINCE;
    static const char* IF_NONE_MATCH;
    static const char* IF_RANGE;
    static const char* IF_UNMODIFIED_SINCE;
    static const char* LAST_MODIFIED;
    static const char* LOCATION;
    static const char* MAX_FORWARDS;
    static const char* ORIGIN;
    static const char* PRAGMA;
    static const char* PROXY_AUTHENTICATE;
    static const char* PROXY_AUTHORIZATION;
    static const char* RANGE;
    static const char* REFERER;
    static const char* RETRY_AFTER;
    static const char* SEC_WEBSOCKET_KEY1;
    static const char* SEC_WEBSOCKET_KEY2;
    static const char* SEC_WEBSOCKET_LOCATION;
    static const char* SEC_WEBSOCKET_ORIGIN;
    static const char* SEC_WEBSOCKET_PROTOCOL;
    static const char* SEC_WEBSOCKET_VERSION;
    static const char* SEC_WEBSOCKET_KEY;
    static const char* SEC_WEBSOCKET_ACCEPT;
    static const char* SERVER;
    static const char* SET_COOKIE;
    static const char* SET_COOKIE2;
    static const char* TE;
    static const char* TRAILER;
    static const char* TRANSFER_ENCODING;
    static const char* UPGRADE;
    static const char* USER_AGENT;
    static const char* VARY;
    static const char* VIA;
    static const char* WARNING;
    static const char* WEBSOCKET_LOCATION;
    static const char* WEBSOCKET_ORIGIN;
    static const char* WEBSOCKET_PROTOCOL;
    static const char* WWW_AUTHENTICATE;

    static const std::string JSON_TYPE;
    static const std::string AUTH_TOKEN;
};

} // namespace doris
