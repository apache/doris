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

// clang-format off
#include "util.h"

#include <bthread/butex.h>
#include <butil/iobuf.h>
#include <google/protobuf/util/json_util.h>

// FIXME: we should not rely other modules that may rely on this common module
#include "common/logging.h"
#include "meta-service/keys.h"
#include "meta-service/codec.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

#include <iomanip>
#include <sstream>
#include <unordered_map>
#include <variant>
// clang-format on

namespace doris::cloud {

/**
 * This is a naïve implementation of hex, DONOT use it on retical path.
 */
std::string hex(std::string_view str) {
    std::stringstream ss;
    for (auto& i : str) {
        ss << std::hex << std::setw(2) << std::setfill('0') << ((int16_t)i & 0xff);
    }
    return ss.str();
}

/**
 * This is a naïve implementation of unhex.
 */
std::string unhex(std::string_view hex_str) {
    // clang-format off
    const static std::unordered_map<char, char> table = {
            {'0', 0},  {'1', 1},  {'2', 2},  {'3', 3},  {'4', 4}, 
            {'5', 5},  {'6', 6},  {'7', 7},  {'8', 8},  {'9', 9}, 
            {'a', 10}, {'b', 11}, {'c', 12}, {'d', 13}, {'e', 14}, {'f', 15},
            {'A', 10}, {'B', 11}, {'C', 12}, {'D', 13}, {'E', 14}, {'F', 15}};
    [[maybe_unused]] static int8_t lut[std::max({'9', 'f', 'F'}) + 1];
    lut[(int)'0'] = 0; lut[(int)'1'] = 1; lut[(int)'2'] = 2; lut[(int)'3'] = 3; lut[(int)'4'] = 4; lut[(int)'5'] = 5; lut[(int)'6'] = 6; lut[(int)'7'] = 7; lut[(int)'8'] = 8; lut[(int)'9'] = 9;
    lut[(int)'a'] = 10; lut[(int)'b'] = 11; lut[(int)'c'] = 12; lut[(int)'d'] = 13; lut[(int)'e'] = 14; lut[(int)'f'] = 15;
    lut[(int)'A'] = 10; lut[(int)'B'] = 11; lut[(int)'C'] = 12; lut[(int)'D'] = 13; lut[(int)'E'] = 14; lut[(int)'F'] = 15;
    // clang-format on
    size_t len = hex_str.length();
    len &= ~0x01UL;
    std::string buf(len >> 1, '\0');
    for (size_t i = 0; i < len; ++i) {
        const auto it = table.find(hex_str[i]);
        if (it == table.end()) break;
        buf[i >> 1] |= i & 0x1 ? (it->second & 0x0f) : (it->second & 0x0f) << 4;
    }
    return buf;
}

static std::string explain_fields(std::string_view text, const std::vector<std::string>& fields,
                                  const std::vector<int>& pos, bool unicode = false) {
    if (fields.size() != pos.size() || fields.size() == 0 || pos.size() == 0) {
        return std::string(text.data(), text.size());
    }
    size_t last_hyphen_pos = pos.back() + 1;
    std::stringstream ss;
    std::string blank_line(last_hyphen_pos + 1, ' ');

    // clang-format off
    static const std::string hyphen("\xe2\x94\x80"); // ─ e2 94 80
    static const std::string bar   ("\xe2\x94\x82"); // │ e2 94 82
    static const std::string angle ("\xe2\x94\x8c"); // ┌ e2 94 8c
    static const std::string arrow ("\xe2\x96\xbc"); // ▼ e2 96 bc
    // clang-format on

    // Each line with hyphens
    for (size_t i = 0; i < fields.size(); ++i) {
        std::string line = blank_line;
        line[pos[i]] = '/';
        int nbar = i;
        for (size_t j = 0; j < i; ++j) {
            line[pos[j]] = '|';
        }
        int nhyphen = 0;
        for (size_t j = pos[i] + 1; j <= last_hyphen_pos; ++j) {
            line[j] = '-';
            ++nhyphen;
        }

        if (unicode) {
            int i = line.size();
            line.resize(line.size() + 2 * (1 /*angle*/ + nbar + nhyphen), ' ');
            int j = line.size();
            while (--i >= 0) {
                if (line[i] == '-') {
                    line[--j] = hyphen[2];
                    line[--j] = hyphen[1];
                    line[--j] = hyphen[0];
                } else if (line[i] == '|') {
                    line[--j] = bar[2];
                    line[--j] = bar[1];
                    line[--j] = bar[0];
                } else if (line[i] == '/') {
                    line[--j] = angle[2];
                    line[--j] = angle[1];
                    line[--j] = angle[0];
                } else {
                    --j;
                    continue;
                }
                line[i] = i != j ? ' ' : line[i]; // Replace if needed
            }
        }

        ss << line << " " << i << ". " << fields[i] << "\n";
    }

    // Mark position indicator
    std::string line = blank_line;
    for (size_t i = 0; i < fields.size(); ++i) {
        line[pos[i]] = '|';
    }

    if (unicode) {
        int i = line.size();
        line.resize(line.size() + 2 * fields.size(), ' ');
        int j = line.size();
        while (--i >= 0) {
            if (line[i] != '|') {
                --j;
                continue;
            }
            line[--j] = bar[2];
            line[--j] = bar[1];
            line[--j] = bar[0];
            line[i] = i != j ? ' ' : line[i]; // Replace if needed
        }
    }

    ss << line << "\n";

    line = blank_line;
    for (size_t i = 0; i < fields.size(); ++i) {
        line[pos[i]] = 'v';
    }

    if (unicode) {
        int i = line.size();
        line.resize(line.size() + 2 * fields.size(), ' ');
        int j = line.size();
        while (--i >= 0) {
            if (line[i] != 'v') {
                --j;
                continue;
            }
            line[--j] = arrow[2];
            line[--j] = arrow[1];
            line[--j] = arrow[0];
            line[i] = i != j ? ' ' : line[i]; // Replace if needed
        }
    }

    ss << line << "\n";

    // Original text to explain
    ss << text << "\n";

    return ss.str();
}

std::string prettify_key(std::string_view key_hex, bool unicode) {
    // Decoded result container
    //                                    val                  tag  pos
    //                     .---------------^----------------.  .^.  .^.
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> fields;
    std::string unhex_key = unhex(key_hex);
    int key_space = unhex_key[0];
    std::string_view key_copy = unhex_key;
    key_copy.remove_prefix(1); // Remove the first key space byte
    int ret = decode_key(&key_copy, &fields);
    if (ret != 0) return "";

    std::vector<std::string> fields_str;
    std::vector<int> fields_pos;
    fields_str.reserve(fields.size() + 1);
    fields_pos.reserve(fields.size() + 1);
    // Key space byte
    fields_str.push_back("key space: " + std::to_string(key_space));
    fields_pos.push_back(0);

    for (auto& i : fields) {
        fields_str.emplace_back(std::get<1>(i) == EncodingTag::BYTES_TAG
                                        ? std::get<std::string>(std::get<0>(i))
                                        : std::to_string(std::get<int64_t>(std::get<0>(i))));
        fields_pos.push_back((std::get<2>(i) + 1) * 2);
    }

    return explain_fields(key_hex, fields_str, fields_pos, unicode);
}

std::string proto_to_json(const ::google::protobuf::Message& msg, bool add_whitespace) {
    std::string json;
    google::protobuf::util::JsonPrintOptions opts;
    opts.add_whitespace = add_whitespace;
    opts.preserve_proto_field_names = true;
    google::protobuf::util::MessageToJsonString(msg, &json, opts);
    return json;
}

std::vector<std::string_view> split_string(const std::string_view& str, int n) {
    std::vector<std::string_view> substrings;

    for (size_t i = 0; i < str.size(); i += n) {
        substrings.push_back(str.substr(i, n));
    }

    return substrings;
}

bool ValueBuf::to_pb(google::protobuf::Message* pb) const {
    butil::IOBuf merge;
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, v] = it->next();
            merge.append_user_data((void*)v.data(), v.size(), +[](void*) {});
        }
    }
    butil::IOBufAsZeroCopyInputStream merge_stream(merge);
    return pb->ParseFromZeroCopyStream(&merge_stream);
}

void ValueBuf::remove(Transaction* txn) const {
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            txn->remove(it->next().first);
        }
    }
}

TxnErrorCode ValueBuf::get(Transaction* txn, std::string_view key, bool snapshot) {
    iters.clear();
    ver = -1;

    std::string begin_key {key};
    std::string end_key {key};
    encode_int64(INT64_MAX, &end_key);
    std::unique_ptr<RangeGetIterator> it;
    TxnErrorCode err = txn->get(begin_key, end_key, &it, snapshot);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    if (!it->has_next()) {
        return TxnErrorCode::TXN_KEY_NOT_FOUND;
    }
    // Extract version
    auto [k, _] = it->next();
    if (k.size() == key.size()) { // Old version KV
        DCHECK(k == key) << hex(k) << ' ' << hex(key);
        DCHECK_EQ(it->size(), 1) << hex(k) << ' ' << hex(key);
        ver = 0;
    } else {
        k.remove_prefix(key.size());
        int64_t suffix;
        if (decode_int64(&k, &suffix) != 0) [[unlikely]] {
            LOG_WARNING("failed to decode key").tag("key", hex(k));
            return TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
        }
        ver = suffix >> 56 & 0xff;
    }
    bool more = it->more();
    if (!more) {
        iters.push_back(std::move(it));
        return TxnErrorCode::TXN_OK;
    }
    begin_key = it->next_begin_key();
    iters.push_back(std::move(it));
    do {
        err = txn->get(begin_key, end_key, &it, snapshot);
        if (err != TxnErrorCode::TXN_OK) {
            return err;
        }
        more = it->more();
        if (more) {
            begin_key = it->next_begin_key();
        }
        iters.push_back(std::move(it));
    } while (more);
    return TxnErrorCode::TXN_OK;
}

TxnErrorCode get(Transaction* txn, std::string_view key, ValueBuf* val, bool snapshot) {
    return val->get(txn, key, snapshot);
}

TxnErrorCode key_exists(Transaction* txn, std::string_view key, bool snapshot) {
    std::string end_key {key};
    encode_int64(INT64_MAX, &end_key);
    std::unique_ptr<RangeGetIterator> it;
    TxnErrorCode err = txn->get(key, end_key, &it, snapshot, 1);
    if (err != TxnErrorCode::TXN_OK) {
        return err;
    }
    return it->has_next() ? TxnErrorCode::TXN_OK : TxnErrorCode::TXN_KEY_NOT_FOUND;
}

void put(Transaction* txn, std::string_view key, const google::protobuf::Message& pb, uint8_t ver,
         size_t split_size) {
    std::string value;
    bool ret = pb.SerializeToString(&value); // Always success
    DCHECK(ret) << hex(key) << ' ' << pb.ShortDebugString();
    put(txn, key, value, ver, split_size);
}

void put(Transaction* txn, std::string_view key, std::string_view value, uint8_t ver,
         size_t split_size) {
    auto split_vec = split_string(value, split_size);
    int64_t suffix_base = ver;
    suffix_base <<= 56;
    for (size_t i = 0; i < split_vec.size(); ++i) {
        std::string k(key);
        encode_int64(suffix_base + i, &k);
        txn->put(k, split_vec[i]);
    }
}

} // namespace doris::cloud
