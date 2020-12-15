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

#include <algorithm>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

#include "gen_cpp/olap_common.pb.h"
#include "topn_counter.h"
#include "slice.h"

namespace doris {

void TopNCounter::add_item(const std::string& item, uint64_t incrementCount) {
    auto iter = _counter_map->find(item);
    if (iter != _counter_map->end()) {
        iter->second.add_count(incrementCount);
    } else {
        _counter_map->insert(std::make_pair(item, Counter(item, incrementCount)));
    }
    _ordered = false;
}

void TopNCounter::serialize(std::string* buffer) {
    sort_retain(_capacity);
    PTopNCounter topn_counter;
    topn_counter.set_top_num(_top_num);
    topn_counter.set_space_expand_rate(_space_expand_rate);
    for(std::vector<Counter>::const_iterator it = _counter_vec->begin(); it != _counter_vec->end(); ++it)
    {
        PCounter* counter = topn_counter.add_counter();
        counter->set_item(it->get_item());
        counter->set_count(it->get_count());
    }
    topn_counter.SerializeToString(buffer);
}

bool TopNCounter::deserialize(const doris::Slice &src) {
    PTopNCounter topn_counter;
    if (!topn_counter.ParseFromArray(src.data, src.size)) {
        LOG(WARNING) << "topn counter deserialize failed";
        return false;
    }

    _space_expand_rate = topn_counter.space_expand_rate();
    set_top_num(topn_counter.top_num());
    for (int i = 0; i < topn_counter.counter_size(); ++i) {
        const PCounter& counter = topn_counter.counter(i);
        _counter_map->insert(std::make_pair(counter.item(), Counter(counter.item(), counter.count())));
        _counter_vec->emplace_back(counter.item(), counter.count());
    }
    _ordered = true;
    return true;
}

void TopNCounter::sort_retain(uint32_t capacity) {
    _counter_vec->clear();
    sort_retain(capacity, _counter_vec);
    _ordered = true;
}

void TopNCounter::sort_retain(uint32_t capacity, std::vector<Counter>* sort_vec) {
    for(std::unordered_map<std::string, Counter>::const_iterator it = _counter_map->begin(); it != _counter_map->end(); ++it) {
        sort_vec->emplace_back(it->second.get_item(), it->second.get_count());
    }

    std::sort(sort_vec->begin(), sort_vec->end(), TopNComparator());
    if (sort_vec->size() > capacity) {
        for (uint32_t i = 0, n = sort_vec->size() - capacity; i < n; ++i) {
            auto &counter = sort_vec->back();
            _counter_map->erase(counter.get_item());
            sort_vec->pop_back();
        }
    }
}

// Based on the  parallel version of the Space Saving algorithm as described in:
// A parallel space saving algorithm for frequent items and the Hurwitz zeta distribution by Massimo Cafaro, et al.
void TopNCounter::merge(doris::TopNCounter &&other) {
    if (other._counter_map->size() == 0) {
        return;
    }

    _space_expand_rate = other._space_expand_rate;
    set_top_num(other._top_num);
    bool this_full = _counter_map->size() >= _capacity;
    bool another_full = other._counter_map->size() >= other._capacity;

    uint64_t m1 = this_full ? _counter_vec->back().get_count() : 0;
    uint64_t m2 = another_full ? other._counter_vec->back().get_count() : 0;

    if (another_full == true) {
        for (auto &entry : *(this->_counter_map)) {
            entry.second.add_count(m2);
        }
    }

    for (auto &other_entry : *(other._counter_map)) {
        auto itr = this->_counter_map->find(other_entry.first);
        if (itr != _counter_map->end()) {
            itr->second.add_count(other_entry.second.get_count() - m2);
        } else {
            this->_counter_map->insert(std::make_pair(other_entry.first,
                    Counter(other_entry.first,other_entry.second.get_count() + m1)));
        }
    }
    _ordered = false;
    sort_retain(_capacity);
}

void TopNCounter::finalize(std::string& finalize_str) {
    if (!_ordered) {
        sort_retain(_top_num);
    }
    // use json format print
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    uint32_t k = 0;
    writer.StartObject();
    for (std::vector<Counter>::const_iterator it = _counter_vec->begin(); it != _counter_vec->end() && k < _top_num; ++it, ++k) {
        writer.Key(it->get_item().data());
        writer.Uint64(it->get_count());
    }
    writer.EndObject();
    finalize_str = buffer.GetString();
}

}
