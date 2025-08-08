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

// Most code of this file is copied from rocksdb SyncPoint.
// https://github.com/facebook/rocksdb

#include <string>

namespace doris {

std::string normalize_http_uri(const std::string& uri) {
    if (uri.empty()) {
        return uri;
    }

    // Find the end of protocol part (http:// or https://)
    // Example: in "https://example.com", protocol_end will be 8 (position after "://")
    size_t protocol_end = uri.find("://");
    if (protocol_end == std::string::npos) {
        protocol_end = 0; // No protocol found, start from beginning
    } else {
        protocol_end += 3; // Skip past "://"
    }

    // Keep protocol part (e.g., "https://")
    std::string result = uri.substr(0, protocol_end);

    // Process the rest of URI to remove duplicate slashes
    // Example: "//path//to///file" becomes "/path/to/file"
    for (size_t i = protocol_end; i < uri.length(); i++) {
        char current = uri[i];

        // Add current character if it's not a slash, or if it's the first slash in sequence
        // This prevents consecutive slashes like "//" or "///" from being added
        if (current != '/' || result.empty() || result.back() != '/') {
            result += current;
        }
    }
    return result;
}
} // namespace doris
