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

#include <parallel_hashmap/phmap.h>
#include <parallel_hashmap/phmap_dump.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace {

using phmap::priv::ctrl_t;
using phmap::priv::Group;
using phmap::priv::GroupPortableImpl;
using phmap::priv::h2_t;
using phmap::priv::kDeleted;
using phmap::priv::kEmpty;
using phmap::priv::kSentinel;

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename Mask>
std::vector<uint32_t> positions(Mask mask) {
    std::vector<uint32_t> result;
    for (uint32_t position : mask) {
        result.push_back(position);
    }
    require(static_cast<bool>(mask) == !result.empty(), "mask bool");
    if (!result.empty()) {
        require(mask.TrailingZeros() == result.front(), "trailing zeros");
    }
    return result;
}

template <typename Impl>
void check_group(const ctrl_t* bytes, h2_t hash) {
    constexpr int width = Impl::kWidth;
    Impl group(bytes);
    std::vector<uint32_t> matches;
    std::vector<uint32_t> empty;
    std::vector<uint32_t> available;
    for (int i = 0; i < width; ++i) {
        if (bytes[i] == static_cast<ctrl_t>(hash)) {
            matches.push_back(i);
        }
        if (bytes[i] == kEmpty) {
            empty.push_back(i);
        }
        if (bytes[i] == kEmpty || bytes[i] == kDeleted) {
            available.push_back(i);
        }
    }
    auto actual = positions(group.Match(hash));
    if constexpr (std::is_same_v<Impl, GroupPortableImpl>) {
        // The scalar subtraction can borrow into an adjacent byte: 0x12,0x13
        // can both match 0x12. Only full slots may be false positives.
        require(matches.empty() == actual.empty(), "portable match emptiness");
        for (auto expected : matches) {
            require(std::find(actual.begin(), actual.end(), expected) != actual.end(),
                    "portable missed an exact match");
        }
        for (auto position : actual) {
            require(position < width && bytes[position] >= 0, "portable matched special slot");
        }
    } else {
        require(actual == matches, "SIMD match differs from scalar oracle");
    }
    require(positions(group.MatchEmpty()) == empty, "empty mask");
    require(positions(group.MatchEmptyOrDeleted()) == available, "empty/deleted mask");
    if (!actual.empty()) {
        require(group.Match(hash).LeadingZeros() == width - 1 - actual.back(), "leading zeros");
    }
    uint32_t leading = 0;
    while (leading < width && (bytes[leading] == kEmpty || bytes[leading] == kDeleted)) {
        ++leading;
    }
    require(group.CountLeadingEmptyOrDeleted() == leading, "leading empty/deleted count");

    std::array<ctrl_t, width + 16> converted;
    converted.fill(42);
    group.ConvertSpecialToEmptyAndFullToDeleted(converted.data() + 7);
    for (int i = 0; i < width; ++i) {
        require(converted[i + 7] == (bytes[i] < 0 ? kEmpty : kDeleted), "special conversion");
    }
    for (int i = 0; i < 7; ++i) {
        require(converted[i] == 42, "conversion underrun");
    }
    for (size_t i = width + 7; i < converted.size(); ++i) {
        require(converted[i] == 42, "conversion overrun");
    }
}

void check_groups() {
    std::mt19937 random(81384);
    std::array<ctrl_t, 32> bytes;
    constexpr std::array<ctrl_t, 3> special_values {kEmpty, kDeleted, kSentinel};
    for (int iteration = 0; iteration < 100000; ++iteration) {
        bytes.fill(kEmpty);
        auto* data = bytes.data() + (iteration & 7);
        for (int i = 0; i < Group::kWidth; ++i) {
            uint32_t value = random() % 131;
            data[i] = value < 128 ? static_cast<ctrl_t>(value) : special_values[value - 128];
        }
        h2_t hash = random() % 128;
        check_group<Group>(data, hash);
        check_group<GroupPortableImpl>(data, hash);
    }
    for (int offset = 0; offset < 8; ++offset) {
        for (auto special : {kEmpty, kDeleted, kSentinel}) {
            bytes.fill(special);
            check_group<Group>(bytes.data() + offset, 0);
            check_group<GroupPortableImpl>(bytes.data() + offset, 0);
        }
        for (int lane = 0; lane < Group::kWidth; ++lane) {
            for (int value = 0; value < 128; ++value) {
                bytes.fill(kEmpty);
                bytes[offset + lane] = static_cast<ctrl_t>(value);
                check_group<Group>(bytes.data() + offset, value);
            }
        }
    }
    bytes.fill(kEmpty);
    bytes[2] = 0x12;
    bytes[3] = 0x13;
    check_group<Group>(bytes.data(), 0x12);
    require(positions(GroupPortableImpl(bytes.data()).Match(0x12)) == std::vector<uint32_t>({2, 3}),
            "adjacent-H2 scalar baseline");
}

template <typename Map>
void check_map() {
    Map map;
    std::unordered_map<uint64_t, uint64_t> reference;
    std::mt19937_64 random(81384);
    map.reserve(65536);
    for (int i = 0; i < 200000; ++i) {
        uint64_t key = random() % 60000;
        uint64_t value = random();
        switch (i % 4) {
        case 0:
        case 1:
            map[key] = value;
            reference[key] = value;
            break;
        case 2:
            require(map.erase(key) == reference.erase(key), "erase result");
            break;
        default: {
            auto found = map.find(key);
            auto expected = reference.find(key);
            require((found == map.end()) == (expected == reference.end()), "find result");
            if (found != map.end()) {
                require(found->second == expected->second, "find value");
            }
        }
        }
        if (i % 16384 == 0) {
            map.rehash(map.size() * 2);
        }
    }
    require(map.size() == reference.size(), "map size");
    for (const auto& [key, value] : reference) {
        auto found = map.find(key);
        require(found != map.end() && found->second == value, "map final content");
    }
}

struct Archive {
    std::iostream& stream;
    void saveBinary(const void* data, size_t bytes) {
        stream.write(static_cast<const char*>(data), bytes);
        require(static_cast<bool>(stream), "dump write");
    }
    void loadBinary(void* data, size_t bytes) {
        stream.read(static_cast<char*>(data), bytes);
        require(static_cast<bool>(stream), "dump read");
    }
};

using DumpMap = phmap::flat_hash_map<uint64_t, uint64_t>;

DumpMap dump_fixture() {
    DumpMap map;
    for (uint64_t i = 0; i < 10000; ++i) {
        map[i * 7 + 3] = i * 11 + 5;
    }
    for (uint64_t i = 0; i < 10000; i += 3) {
        map.erase(i * 7 + 3);
    }
    return map;
}

void check_loaded(const DumpMap& map) {
    auto expected = dump_fixture();
    require(map.size() == expected.size(), "dump/load size");
    for (const auto& [key, value] : expected) {
        auto found = map.find(key);
        require(found != map.end() && found->second == value, "dump/load content");
    }
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc == 3) {
            const bool dumping = std::string(argv[1]) == "--dump";
            auto mode = std::ios::in | std::ios::out | std::ios::binary;
            if (dumping) {
                mode |= std::ios::trunc;
            }
            std::fstream stream(argv[2], mode);
            require(static_cast<bool>(stream), "open archive");
            Archive archive {stream};
            if (dumping) {
                auto map = dump_fixture();
                require(map.phmap_dump(archive), "dump");
            } else {
                require(std::string(argv[1]) == "--load", "unknown archive operation");
                DumpMap map;
                require(map.phmap_load(archive), "load");
                check_loaded(map);
            }
        } else {
            require(argc == 1, "unexpected arguments");
            check_groups();
            check_map<phmap::flat_hash_map<uint64_t, uint64_t>>();
            check_map<phmap::parallel_flat_hash_map<uint64_t, uint64_t>>();
            std::stringstream stream;
            Archive archive {stream};
            auto source = dump_fixture();
            require(source.phmap_dump(archive), "roundtrip dump");
            DumpMap loaded;
            require(loaded.phmap_load(archive), "roundtrip load");
            check_loaded(loaded);
        }
        std::cout << "PASS group_width=" << Group::kWidth
                  << " portable=" << std::is_same_v<Group, GroupPortableImpl> << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL: " << error.what() << '\n';
        return 1;
    }
}
