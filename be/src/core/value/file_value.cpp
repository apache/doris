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

#include "core/value/file_value.h"

#include <algorithm>
#include <array>
#include <limits>
#include <string_view>

#include "core/field.h"

namespace doris {
namespace {
// Keep helper names separate from other anonymous namespaces in Core unity builds.
namespace file_value_detail {

constexpr std::array<std::string_view, 6> FIELD_NAMES = {"uri",          "offset",   "size",
                                                         "content_type", "checksum", "inline"};
constexpr size_t MAX_METADATA_BYTES = 1024;

bool ascii_digit(char ch) {
    return ch >= '0' && ch <= '9';
}

bool mime_token_char(char ch) {
    // RFC 2045 section 5.1: ASCII minus SPACE, CTLs and tspecials.
    return ch > 0x20 && ch < 0x7f &&
           std::string_view("()<>@,;:\\\"/[]?=").find(ch) == std::string_view::npos;
}

bool consume_mime_token(std::string_view& text) {
    size_t length = 0;
    while (length < text.size() && mime_token_char(text[length])) {
        ++length;
    }
    text.remove_prefix(length);
    return length != 0;
}

bool consume_mime_fold(std::string_view& text) {
    // RFC 822 section 3.1.1: a fold is CRLF followed by SP or HTAB.
    if (text.size() < 3 || text[0] != '\r' || text[1] != '\n' ||
        (text[2] != ' ' && text[2] != '\t')) {
        return false;
    }
    text.remove_prefix(3);
    return true;
}

bool consume_mime_delimited(std::string_view& text, bool comment) {
    // RFC 822 section 3.3: qtext/ctext and quoted-pair use all ASCII, not
    // HTTP qdtext. Count nested comments iteratively; never rewrite the value.
    text.remove_prefix(1); // Opening '(' or DQUOTE.
    size_t depth = 1;
    while (!text.empty()) {
        const auto ch = static_cast<unsigned char>(text.front());
        if (ch >= 0x80) {
            return false;
        }
        if (ch == '\r') {
            if (!consume_mime_fold(text)) {
                return false;
            }
            continue;
        }
        text.remove_prefix(1);
        if (ch == '\\') {
            if (text.empty() || static_cast<unsigned char>(text.front()) >= 0x80) {
                return false;
            }
            if (text.starts_with("\r\n")) {
                // RFC 822 sections 3.4.3 and 3.4.5 require quoted CRLFs
                // in comments and quoted strings to obey the folding rules.
                if (!consume_mime_fold(text)) {
                    return false;
                }
            } else {
                text.remove_prefix(1); // quoted-pair = backslash CHAR, including CTLs.
            }
        } else if (ch == (comment ? ')' : '"')) {
            if (--depth == 0) {
                return true;
            }
        } else if (comment && ch == '(') {
            ++depth;
        }
    }
    return false;
}

bool consume_mime_cfws(std::string_view& text) {
    // Comments and linear whitespace may surround each lexical token, but
    // cannot join two separate token fragments (e.g. te(comment)xt/plain).
    while (!text.empty()) {
        const char ch = text.front();
        if (ch == ' ' || ch == '\t') {
            text.remove_prefix(1);
        } else if (ch == '\r') {
            if (!consume_mime_fold(text)) {
                return false;
            }
        } else if (ch == '(') {
            if (!consume_mime_delimited(text, true)) {
                return false;
            }
        } else {
            break;
        }
    }
    return true;
}

bool consume_mime_separator(std::string_view& text, char separator) {
    if (!consume_mime_cfws(text) || text.empty() || text.front() != separator) {
        return false;
    }
    text.remove_prefix(1);
    return consume_mime_cfws(text);
}

bool mime(std::string_view text) {
    // RFC 2045 section 5.1 with RFC 822 lexical rules. Validate syntax only:
    // unknown types/parameters are allowed and all original bytes are preserved.
    if (!consume_mime_cfws(text) || !consume_mime_token(text) ||
        !consume_mime_separator(text, '/') || !consume_mime_token(text) ||
        !consume_mime_cfws(text)) {
        return false;
    }
    while (!text.empty()) {
        if (!consume_mime_separator(text, ';') || !consume_mime_token(text) ||
            !consume_mime_separator(text, '=') || text.empty()) {
            return false;
        }
        if (text.front() == '"') {
            if (!consume_mime_delimited(text, false)) {
                return false;
            }
        } else if (!consume_mime_token(text)) {
            return false;
        }
        if (!consume_mime_cfws(text)) {
            return false;
        }
    }
    return true;
}

bool equals_known_algorithm(std::string_view text, std::string_view canonical) {
    return text.size() == canonical.size() &&
           std::equal(text.begin(), text.end(), canonical.begin(), [](char ch, char expected) {
               return (ch >= 'a' && ch <= 'z' ? ch - 'a' + 'A' : ch) == expected;
           });
}

bool checksum(std::string_view text) {
    const auto colon = text.find(':');
    if (colon == std::string_view::npos || colon == 0 || colon + 1 == text.size()) {
        return false;
    }
    const auto algorithm = text.substr(0, colon);
    const auto digest = text.substr(colon + 1);
    struct KnownAlgorithm {
        std::string_view name;
        size_t hex_length;
    };
    constexpr std::array algorithms = {KnownAlgorithm {"ETAG", 0}, KnownAlgorithm {"MD5", 32},
                                       KnownAlgorithm {"CRC32", 8}, KnownAlgorithm {"CRC32C", 8},
                                       KnownAlgorithm {"SHA-256", 64}};
    for (const auto& known : algorithms) {
        if (equals_known_algorithm(algorithm, known.name)) {
            // Case-insensitive recognition prevents misspellings of known names
            // from escaping validation as unknown algorithms. Never normalize.
            if (algorithm != known.name) {
                return false;
            }
            if (known.hex_length == 0) {
                return true; // ETAG is opaque, including quotes and multipart suffixes.
            }
            return digest.size() == known.hex_length &&
                   std::all_of(digest.begin(), digest.end(),
                               [](char ch) { return ascii_digit(ch) || (ch >= 'a' && ch <= 'f'); });
        }
    }
    return true; // Both parts of an unknown algorithm are nonempty opaque bytes.
}

} // namespace file_value_detail
} // namespace

bool is_valid_file_content_type(std::string_view content_type) {
    return content_type.size() <= file_value_detail::MAX_METADATA_BYTES &&
           file_value_detail::mime(content_type);
}

std::string_view infer_file_content_type_from_name(std::string_view name) {
    // Shared by TO_FILE and list_file. Keep inference independent of URI rewriting.
    static constexpr std::pair<std::string_view, std::string_view> TYPES[] = {
            {".csv", "text/csv"},
            {".tsv", "text/tab-separated-values"},
            {".json", "application/json"},
            {".jsonl", "application/x-ndjson"},
            {".parquet", "application/x-parquet"},
            {".orc", "application/x-orc"},
            {".avro", "application/avro"},
            {".txt", "text/plain"},
            {".log", "text/plain"},
            {".tbl", "text/plain"},
            {".xml", "application/xml"},
            {".html", "text/html"},
            {".htm", "text/html"},
            {".pdf", "application/pdf"},
            {".jpg", "image/jpeg"},
            {".jpeg", "image/jpeg"},
            {".png", "image/png"},
            {".gif", "image/gif"},
            {".bmp", "image/bmp"},
            {".svg", "image/svg+xml"},
            {".webp", "image/webp"},
            {".mp3", "audio/mpeg"},
            {".wav", "audio/wav"},
            {".mp4", "video/mp4"},
            {".avi", "video/x-msvideo"},
            {".gz", "application/gzip"},
            {".bz2", "application/x-bzip2"},
            {".zst", "application/zstd"},
            {".lz4", "application/x-lz4"},
            {".snappy", "application/x-snappy"},
            {".zip", "application/zip"},
            {".tar", "application/x-tar"},
    };
    const auto basename = name.substr(name.find_last_of('/') + 1);
    const auto dot = basename.find_last_of('.');
    if (dot != std::string_view::npos) {
        std::string extension(basename.substr(dot));
        for (auto& ch : extension) {
            if (ch >= 'A' && ch <= 'Z') {
                ch += 'a' - 'A';
            }
        }
        for (const auto& [suffix, type] : TYPES) {
            if (extension == suffix) {
                return type;
            }
        }
    }
    return "application/octet-stream";
}

std::optional<std::string> file_checksum_from_etag(std::optional<std::string_view> etag) {
    if (!etag) {
        return std::nullopt;
    }
    if (etag->size() >= 2 && etag->front() == '"' && etag->back() == '"') {
        etag->remove_prefix(1);
        etag->remove_suffix(1);
    }
    if (etag->empty()) {
        return std::nullopt;
    }
    return "ETAG:" + std::string(*etag);
}

Status validate_file(const File& value) {
    // Callers establish the canonical field count and types when constructing or decoding FILE.
    DCHECK_EQ(value.size(), file_value_detail::FIELD_NAMES.size());
    const auto text = [&value](size_t field) -> std::optional<std::string_view> {
        return value[field].is_null() ? std::nullopt : std::optional(value[field].as_string_view());
    };
    const auto number = [&value](size_t field) -> std::optional<int64_t> {
        return value[field].is_null() ? std::nullopt
                                      : std::optional(value[field].get<TYPE_BIGINT>());
    };
    return validate_file(text(0), number(1), number(2), text(3), text(4), value[5].is_null());
}

Status validate_file(std::optional<std::string_view> uri_value, std::optional<int64_t> offset,
                     std::optional<int64_t> size, std::optional<std::string_view> content_type,
                     std::optional<std::string_view> checksum_value, bool inline_is_null) {
    using namespace file_value_detail;
    if (!uri_value && inline_is_null) {
        return Status::InvalidArgument("FILE uri must not be NULL when inline is NULL");
    }
    if (size && *size < 0) {
        return Status::InvalidArgument("FILE size must be nonnegative");
    }
    if (offset) {
        if (*offset < 0) {
            return Status::InvalidArgument("FILE offset must be nonnegative");
        }
        if (!size) {
            return Status::InvalidArgument("FILE offset requires size");
        }
        if (*offset > std::numeric_limits<Int64>::max() - *size) {
            return Status::InvalidArgument("FILE offset plus size exceeds BIGINT");
        }
    }
    if (content_type && !is_valid_file_content_type(*content_type)) {
        return Status::InvalidArgument(
                "FILE content_type must be a nonempty valid media type within 1024 bytes");
    }
    if (checksum_value &&
        (checksum_value->size() > MAX_METADATA_BYTES || !checksum(*checksum_value))) {
        return Status::InvalidArgument(
                "FILE checksum must be algorithm:digest within 1024 bytes; known algorithms "
                "require canonical names and prescribed lowercase hex digests (ETAG is "
                "opaque)");
    }
    return Status::OK();
}

} // namespace doris
