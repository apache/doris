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
    /**
    * Normalizes HTTP URI by removing duplicate slashes while preserving the protocol part.
    * 
    * This function removes consecutive forward slashes from URIs while keeping the protocol 
    * section (http:// or https://) intact. It processes everything after the protocol to 
    * ensure clean URI formatting.
    *
    * @param uri The input URI string to be normalized
    * @return A normalized URI string with duplicate slashes removed, or the original 
    *         string if it's empty
    *
    * @example
    *   normalize_http_uri("https://example.com//path//to///file") 
    *   returns "https://example.com/path/to/file"
    *
    *   normalize_http_uri("http://host.com///bucket//prefix/") 
    *   returns "http://host.com/bucket/prefix/"
    *
    *   normalize_http_uri("endpoint.com//bucket///prefix") 
    *   returns "endpoint.com/bucket/prefix"
    *
    *   normalize_http_uri("https://account.blob.core.windows.net////container") 
    *   returns "https://account.blob.core.windows.net/container"
    */
    std::string normalize_http_uri(const std::string& uri);
} // namespace doris
