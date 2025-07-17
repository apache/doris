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

#include <butil/containers/linked_list.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <google/protobuf/service.h>

#include <cstdint>
#include <ostream>
#include <string_view>
#include <type_traits>

namespace doris::cloud {

bool init_glog(const char* basename);
uint64_t get_log_id(google::protobuf::RpcController* controller);

/// Wrap a glog stream and tag on the log. usage:
///   LOG_INFO("here is an info for a {} query", query_type).tag("query_id", queryId);
///   LOG_INFO("msg") << "query_id" << query_id;
///   LOG_INFO("msg {}", msg) << "query_id" << query_id;
#define LOG_INFO(...) ::doris::cloud::TaggableLogger(LOG(INFO), ##__VA_ARGS__)
#define LOG_WARNING(...) ::doris::cloud::TaggableLogger(LOG(WARNING), ##__VA_ARGS__)
#define LOG_ERROR(...) ::doris::cloud::TaggableLogger(LOG(ERROR), ##__VA_ARGS__)
#define LOG_FATAL(...) ::doris::cloud::TaggableLogger(LOG(FATAL), ##__VA_ARGS__)

// If you want to use AnnotateTag but the parameters do not match AnnotateTagAllowType, causing the compile fail
// you need to add the supported types using the following syntax:
// such as you want add a `int64_t txn_id; AnnotateTag tag_txn_id("txn_id", txn_id);`
// 1. add type to AnnotateTagAllowType: `std::is_same_v<int64_t, T>` connect with `||` with other is_same_v
// 2. --if is a rvalue (see tag_log_id) add origin type to `std::variant<..., uint64_t> data;`
//    +-if is a lvalue (see txn_id) add pointer type to `std::variant<..., int64_t*> data;`
//    +-if is a more complex type (class A,B...) maybe you need rewrite `AnnotateTagValue::to_string()`
//      +- you need prove a lambda to AnnotateTagValueHelper{[](const A&)...} make sure return std::string
template <typename T>
concept AnnotateTagAllowType = requires {
    std::is_same_v<uint64_t, T> || std::is_same_v<int64_t, T> || std::is_same_v<std::string, T>;
};

class AnnotateTagValue {
public:
    template <AnnotateTagAllowType T>
    explicit AnnotateTagValue(T&& val) {
        // rvalue store origin value, lvalue store pointer
        if constexpr (std::is_rvalue_reference_v<decltype(val)>) {
            data_ = std::move(val);
        } else {
            data_ = &val;
        }
    };

    std::string to_string() const;

private:
    std::variant<uint64_t, int64_t*, std::string*> data_;
};

class AnnotateTag final : public butil::LinkNode<AnnotateTag> {
public:
    template <AnnotateTagAllowType T>
    explicit AnnotateTag(std::string_view key, T&& value)
            : key_(key), value_(std::forward<T>(value)) {
        append_to_tag_list();
    }
    ~AnnotateTag();

    static void format_tag_list(std::ostream& stream);

    static void* operator new(size_t) = delete;
    static void* operator new[](size_t) = delete;

private:
    void append_to_tag_list();
    std::string_view key_;
    AnnotateTagValue value_;
};

class TaggableLogger {
public:
    template <typename... Args>
    TaggableLogger(std::ostream& stream, std::string_view fmt, Args&&... args) : stream_(stream) {
        if constexpr (sizeof...(args) == 0) {
            stream_ << fmt;
        } else {
            stream_ << fmt::format(fmt, std::forward<Args>(args)...);
        }
    };

    ~TaggableLogger() { AnnotateTag::format_tag_list(stream_); };

    template <typename V>
    TaggableLogger& tag(std::string_view key, const V& value) {
        stream_ << ' ' << key << '=';
        if constexpr (std::is_convertible_v<V, std::string_view>) {
            stream_ << '"' << value << '"';
        } else {
            stream_ << value;
        }
        return *this;
    }

    template <typename V>
    std::ostream& operator<<(const V& value) {
        if constexpr (std::is_convertible_v<V, std::string_view>) {
            stream_ << '"' << value << '"';
        } else {
            stream_ << value;
        }
        return stream_;
    }

private:
    std::ostream& stream_;
};

} // namespace doris::cloud

// To keep it simple and practical, we don't actually need so many VLOG levels.
// Using `VLOG(${number})` is confusing and hard to desid in most cases, all we
// need is a complementary debug level to glog's default 4 levels of logging.
// "One VLOG level to rule them all!"
#define DEBUG 5
// VLOG_DEBUG is alias of VLOG(DEBUG) I.O.W VLOG(5)
#define VLOG_DEBUG VLOG(DEBUG)
