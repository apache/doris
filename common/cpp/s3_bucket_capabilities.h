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

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <regex>
#include <string>
#include <string_view>

namespace doris {

enum class S3BucketType { GENERAL_PURPOSE, DIRECTORY };
enum class S3EndpointMode { EXPLICIT_OVERRIDE, AWS_SDK_RULES };
enum class S3ChecksumPolicy { CONTENT_MD5, CRC32C };

struct S3BucketCapabilities {
    S3BucketType bucket_type = S3BucketType::GENERAL_PURPOSE;
    S3EndpointMode endpoint_mode = S3EndpointMode::EXPLICIT_OVERRIDE;
    S3ChecksumPolicy checksum_policy = S3ChecksumPolicy::CONTENT_MD5;
    bool official_aws_service = false;
    bool require_https = false;
    bool require_virtual_addressing = false;
    bool supports_start_after = true;
    bool list_is_lexicographic = true;
    bool supports_versioning = true;
    bool supports_presign = true;

    bool is_directory_bucket() const { return bucket_type == S3BucketType::DIRECTORY; }
};

inline std::string s3_endpoint_host(std::string_view endpoint) {
    const auto scheme = endpoint.find("://");
    if (scheme != std::string_view::npos) {
        endpoint.remove_prefix(scheme + 3);
    }
    endpoint = endpoint.substr(0, endpoint.find_first_of("/?#"));
    if (const auto port = endpoint.find(':'); port != std::string_view::npos) {
        endpoint = endpoint.substr(0, port);
    }
    std::string host(endpoint);
    std::transform(host.begin(), host.end(), host.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    while (!host.empty() && host.back() == '.') {
        host.pop_back();
    }
    return host;
}

inline bool is_aws_s3_endpoint(std::string_view endpoint) {
    if (endpoint.empty()) {
        // An omitted endpoint means that the AWS SDK resolves the public AWS S3 endpoint
        // from the configured region.
        return true;
    }
    const std::string host = s3_endpoint_host(endpoint);
    if (host.empty()) {
        return false;
    }
    const bool aws_dns = host.ends_with(".amazonaws.com") ||
                         host.ends_with(".amazonaws.com.cn") || host.ends_with(".api.aws");
    if (!aws_dns) {
        return false;
    }
    return host.starts_with("s3.") || host.starts_with("s3-") ||
           host.starts_with("s3express-") || host.starts_with("s3express-control.") ||
           host.find(".s3.") != std::string::npos ||
           host.find(".s3-") != std::string::npos ||
           host.find(".s3express-") != std::string::npos;
}

inline bool is_s3express_control_endpoint(std::string_view endpoint) {
    const std::string host = s3_endpoint_host(endpoint);
    return host.starts_with("s3express-control.") ||
           host.find(".s3express-control.") != std::string::npos;
}

inline bool is_s3express_zonal_endpoint(std::string_view endpoint) {
    if (is_s3express_control_endpoint(endpoint)) {
        return false;
    }
    const std::string host = s3_endpoint_host(endpoint);
    return host.starts_with("s3express-") || host.find(".s3express-") != std::string::npos;
}

inline bool is_aws_explicit_s3_endpoint(std::string_view endpoint) {
    const std::string host = s3_endpoint_host(endpoint);
    return host.find("fips") != std::string::npos || host.find("dualstack") != std::string::npos ||
           host.find("accelerate") != std::string::npos ||
           host.find("vpce") != std::string::npos ||
           is_s3express_control_endpoint(endpoint);
}

inline std::string s3_directory_bucket_zone_id(std::string_view bucket) {
    constexpr std::string_view suffix = "--x-s3";
    if (!bucket.ends_with(suffix)) {
        return {};
    }
    bucket.remove_suffix(suffix.size());
    const auto separator = bucket.rfind("--");
    return separator == std::string_view::npos ? "" : std::string(bucket.substr(separator + 2));
}

inline std::string s3express_endpoint_zone_id(std::string_view endpoint) {
    if (is_s3express_control_endpoint(endpoint)) {
        return {};
    }
    const std::string host = s3_endpoint_host(endpoint);
    const auto marker = host.find("s3express-");
    if (marker == std::string::npos) {
        return {};
    }
    const auto begin = marker + std::string_view("s3express-").size();
    const auto end = host.find('.', begin);
    return host.substr(begin, end == std::string::npos ? end : end - begin);
}

inline std::string s3_endpoint_host_label(std::string_view host, std::size_t begin) {
    const auto end = host.find('.', begin);
    return std::string(host.substr(begin, end == std::string_view::npos ? end : end - begin));
}

inline std::string aws_s3_endpoint_region(std::string_view endpoint) {
    const std::string host = s3_endpoint_host(endpoint);
    if (host.empty() || host == "s3.amazonaws.com") {
        return {};
    }

    const auto control = host.find("s3express-control.");
    if (control != std::string::npos) {
        const auto begin = control + std::string_view("s3express-control.").size();
        return s3_endpoint_host_label(host, begin);
    }
    const auto express = host.find("s3express-");
    if (express != std::string::npos) {
        const auto dot = host.find('.', express);
        return dot == std::string::npos ? "" : s3_endpoint_host_label(host, dot + 1);
    }
    const auto standard = host.find("s3.");
    if (standard != std::string::npos) {
        auto begin = standard + std::string_view("s3.").size();
        if (host.substr(begin).starts_with("dualstack.")) {
            begin += std::string_view("dualstack.").size();
        }
        const auto value = s3_endpoint_host_label(host, begin);
        return value == "amazonaws" ? "" : value;
    }
    if (host.starts_with("s3-")) {
        const auto begin = std::string_view("s3-").size();
        return s3_endpoint_host_label(host, begin);
    }
    return {};
}

inline bool is_s3_directory_bucket_name(std::string_view bucket) {
    static const std::regex pattern(
            "^[a-z0-9]([a-z0-9-]*[a-z0-9])?--[a-z0-9]+-az[0-9]+--x-s3$");
    return bucket.size() >= 3 && bucket.size() <= 63 &&
           std::regex_match(bucket.begin(), bucket.end(), pattern);
}

inline S3BucketCapabilities resolve_s3_bucket_capabilities(std::string_view bucket,
                                                            std::string_view endpoint) {
    S3BucketCapabilities capabilities;
    capabilities.official_aws_service = is_aws_s3_endpoint(endpoint);
    if (!is_s3_directory_bucket_name(bucket) || !capabilities.official_aws_service) {
        return capabilities;
    }

    capabilities.bucket_type = S3BucketType::DIRECTORY;
    capabilities.endpoint_mode = is_aws_explicit_s3_endpoint(endpoint)
                                         ? S3EndpointMode::EXPLICIT_OVERRIDE
                                         : S3EndpointMode::AWS_SDK_RULES;
    capabilities.checksum_policy = S3ChecksumPolicy::CRC32C;
    capabilities.require_https = true;
    capabilities.require_virtual_addressing = true;
    capabilities.supports_start_after = false;
    capabilities.list_is_lexicographic = false;
    capabilities.supports_versioning = false;
    capabilities.supports_presign = false;
    return capabilities;
}

} // namespace doris
