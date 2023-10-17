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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/runtime-profile.cc
// and modified by Doris

#include "util/runtime_profile.h"

#include <gen_cpp/RuntimeProfile_types.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>
#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <iomanip>
#include <iostream>

#include "common/object_pool.h"
#include "util/container_util.hpp"

namespace doris {

// Thread counters name
static const std::string THREAD_TOTAL_TIME = "TotalWallClockTime";
static const std::string THREAD_VOLUNTARY_CONTEXT_SWITCHES = "VoluntaryContextSwitches";
static const std::string THREAD_INVOLUNTARY_CONTEXT_SWITCHES = "InvoluntaryContextSwitches";

static const std::string SPAN_ATTRIBUTE_KEY_SEPARATOR = "-";

// The root counter name for all top level counters.
static const std::string ROOT_COUNTER;

RuntimeProfile::RuntimeProfile(const std::string& name, bool is_averaged_profile)
        : _pool(new ObjectPool()),
          _name(name),
          _metadata(-1),
          _timestamp(-1),
          _is_averaged_profile(is_averaged_profile),
          _counter_total_time(TUnit::TIME_NS, 0, 1),
          _local_time_percent(0) {
    _counter_map["TotalTime"] = &_counter_total_time;
}

RuntimeProfile::~RuntimeProfile() = default;

void RuntimeProfile::merge(RuntimeProfile* other) {
    DCHECK(other != nullptr);

    // Merge this level
    {
        CounterMap::iterator dst_iter;
        CounterMap::const_iterator src_iter;
        std::lock_guard<std::mutex> l(_counter_map_lock);
        std::lock_guard<std::mutex> m(other->_counter_map_lock);

        for (src_iter = other->_counter_map.begin(); src_iter != other->_counter_map.end();
             ++src_iter) {
            dst_iter = _counter_map.find(src_iter->first);

            if (dst_iter == _counter_map.end()) {
                _counter_map[src_iter->first] = _pool->add(
                        new Counter(src_iter->second->type(), src_iter->second->value()));
            } else {
                DCHECK(dst_iter->second->type() == src_iter->second->type());

                if (dst_iter->second->type() == TUnit::DOUBLE_VALUE) {
                    double new_val =
                            dst_iter->second->double_value() + src_iter->second->double_value();
                    dst_iter->second->set(new_val);
                } else {
                    dst_iter->second->update(src_iter->second->value());
                }
            }
        }

        ChildCounterMap::const_iterator child_counter_src_itr;

        for (child_counter_src_itr = other->_child_counter_map.begin();
             child_counter_src_itr != other->_child_counter_map.end(); ++child_counter_src_itr) {
            std::set<std::string>* child_counters = find_or_insert(
                    &_child_counter_map, child_counter_src_itr->first, std::set<std::string>());
            child_counters->insert(child_counter_src_itr->second.begin(),
                                   child_counter_src_itr->second.end());
        }
    }

    {
        std::lock_guard<std::mutex> l(_children_lock);
        std::lock_guard<std::mutex> m(other->_children_lock);

        // Recursively merge children with matching names
        for (int i = 0; i < other->_children.size(); ++i) {
            RuntimeProfile* other_child = other->_children[i].first;
            ChildMap::iterator j = _child_map.find(other_child->_name);
            RuntimeProfile* child = nullptr;

            if (j != _child_map.end()) {
                child = j->second;
            } else {
                child = _pool->add(new RuntimeProfile(other_child->_name));
                child->_local_time_percent = other_child->_local_time_percent;
                child->_metadata = other_child->_metadata;
                child->_timestamp = other_child->_timestamp;
                bool indent_other_child = other->_children[i].second;
                _child_map[child->_name] = child;
                _children.push_back(std::make_pair(child, indent_other_child));
            }

            child->merge(other_child);
        }
    }
}

void RuntimeProfile::update(const TRuntimeProfileTree& thrift_profile) {
    int idx = 0;
    update(thrift_profile.nodes, &idx);
    DCHECK_EQ(idx, thrift_profile.nodes.size());
}

void RuntimeProfile::update(const std::vector<TRuntimeProfileNode>& nodes, int* idx) {
    DCHECK_LT(*idx, nodes.size());
    const TRuntimeProfileNode& node = nodes[*idx];
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        // update this level
        std::map<std::string, Counter*>::iterator dst_iter;

        for (int i = 0; i < node.counters.size(); ++i) {
            const TCounter& tcounter = node.counters[i];
            CounterMap::iterator j = _counter_map.find(tcounter.name);

            if (j == _counter_map.end()) {
                _counter_map[tcounter.name] =
                        _pool->add(new Counter(tcounter.type, tcounter.value));
            } else {
                if (j->second->type() != tcounter.type) {
                    LOG(ERROR) << "Cannot update counters with the same name (" << j->first
                               << ") but different types.";
                } else {
                    j->second->set(tcounter.value);
                }
            }
        }

        ChildCounterMap::const_iterator child_counter_src_itr;

        for (child_counter_src_itr = node.child_counters_map.begin();
             child_counter_src_itr != node.child_counters_map.end(); ++child_counter_src_itr) {
            std::set<std::string>* child_counters = find_or_insert(
                    &_child_counter_map, child_counter_src_itr->first, std::set<std::string>());
            child_counters->insert(child_counter_src_itr->second.begin(),
                                   child_counter_src_itr->second.end());
        }
    }

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        const InfoStrings& info_strings = node.info_strings;
        for (const std::string& key : node.info_strings_display_order) {
            // Look for existing info strings and update in place. If there
            // are new strings, add them to the end of the display order.
            // TODO: Is nodes.info_strings always a superset of
            // _info_strings? If so, can just copy the display order.
            InfoStrings::const_iterator it = info_strings.find(key);
            DCHECK(it != info_strings.end());
            InfoStrings::iterator existing = _info_strings.find(key);

            if (existing == _info_strings.end()) {
                _info_strings.insert(std::make_pair(key, it->second));
                _info_strings_display_order.push_back(key);
            } else {
                _info_strings[key] = it->second;
            }
        }
    }

    ++*idx;
    {
        std::lock_guard<std::mutex> l(_children_lock);

        // update children with matching names; create new ones if they don't match
        for (int i = 0; i < node.num_children; ++i) {
            const TRuntimeProfileNode& tchild = nodes[*idx];
            ChildMap::iterator j = _child_map.find(tchild.name);
            RuntimeProfile* child = nullptr;

            if (j != _child_map.end()) {
                child = j->second;
            } else {
                child = _pool->add(new RuntimeProfile(tchild.name));
                child->_metadata = tchild.metadata;
                child->_timestamp = tchild.timestamp;
                _child_map[tchild.name] = child;
                _children.push_back(std::make_pair(child, tchild.indent));
            }

            child->update(nodes, idx);
        }
    }
}

void RuntimeProfile::divide(int n) {
    DCHECK_GT(n, 0);
    std::map<std::string, Counter*>::iterator iter;
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);

        for (iter = _counter_map.begin(); iter != _counter_map.end(); ++iter) {
            if (iter->second->type() == TUnit::DOUBLE_VALUE) {
                iter->second->set(iter->second->double_value() / n);
            } else {
                int64_t value = iter->second->_value.load();
                value = value / n;
                iter->second->_value.store(value);
            }
        }
    }
    {
        std::lock_guard<std::mutex> l(_children_lock);

        for (ChildMap::iterator i = _child_map.begin(); i != _child_map.end(); ++i) {
            i->second->divide(n);
        }
    }
}

void RuntimeProfile::clear_children() {
    std::lock_guard<std::mutex> l(_children_lock);
    _children.clear();
}

void RuntimeProfile::compute_time_in_profile() {
    compute_time_in_profile(total_time_counter()->value());
}

void RuntimeProfile::compute_time_in_profile(int64_t total) {
    if (total == 0) {
        return;
    }

    // Add all the total times in all the children
    int64_t total_child_time = 0;
    std::lock_guard<std::mutex> l(_children_lock);

    for (int i = 0; i < _children.size(); ++i) {
        total_child_time += _children[i].first->total_time_counter()->value();
    }

    int64_t local_time = total_time_counter()->value() - total_child_time;
    // Counters have some margin, set to 0 if it was negative.
    local_time = std::max<int64_t>(0L, local_time);
    _local_time_percent = static_cast<double>(local_time) / total;
    _local_time_percent = std::min(1.0, _local_time_percent) * 100;

    // Recurse on children
    for (int i = 0; i < _children.size(); ++i) {
        _children[i].first->compute_time_in_profile(total);
    }
}

RuntimeProfile* RuntimeProfile::create_child(const std::string& name, bool indent, bool prepend) {
    std::lock_guard<std::mutex> l(_children_lock);
    DCHECK(_child_map.find(name) == _child_map.end());
    RuntimeProfile* child = _pool->add(new RuntimeProfile(name));
    if (this->is_set_metadata()) {
        child->set_metadata(this->metadata());
    }
    if (this->is_set_sink()) {
        child->set_is_sink(this->is_sink());
    }
    if (_children.empty()) {
        add_child_unlock(child, indent, nullptr);
    } else {
        ChildVector::iterator pos = prepend ? _children.begin() : _children.end();
        add_child_unlock(child, indent, (*pos).first);
    }
    return child;
}

void RuntimeProfile::insert_child_head(doris::RuntimeProfile* child, bool indent) {
    std::lock_guard<std::mutex> l(_children_lock);
    DCHECK(child != nullptr);
    _child_map[child->_name] = child;

    auto it = _children.begin();
    _children.insert(it, std::make_pair(child, indent));
}

void RuntimeProfile::add_child_unlock(RuntimeProfile* child, bool indent, RuntimeProfile* loc) {
    DCHECK(child != nullptr);
    _child_map[child->_name] = child;

    if (loc == nullptr) {
        _children.push_back(std::make_pair(child, indent));
    } else {
        for (ChildVector::iterator it = _children.begin(); it != _children.end(); ++it) {
            if (it->first == loc) {
                _children.insert(it, std::make_pair(child, indent));
                return;
            }
        }
        DCHECK(false) << "Invalid loc";
    }
}

void RuntimeProfile::add_child(RuntimeProfile* child, bool indent, RuntimeProfile* loc) {
    std::lock_guard<std::mutex> l(_children_lock);
    add_child_unlock(child, indent, loc);
}

void RuntimeProfile::get_children(std::vector<RuntimeProfile*>* children) {
    children->clear();
    std::lock_guard<std::mutex> l(_children_lock);

    for (ChildMap::iterator i = _child_map.begin(); i != _child_map.end(); ++i) {
        children->push_back(i->second);
    }
}

void RuntimeProfile::get_all_children(std::vector<RuntimeProfile*>* children) {
    std::lock_guard<std::mutex> l(_children_lock);

    for (ChildMap::iterator i = _child_map.begin(); i != _child_map.end(); ++i) {
        children->push_back(i->second);
        i->second->get_all_children(children);
    }
}

void RuntimeProfile::add_info_string(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> l(_info_strings_lock);
    InfoStrings::iterator it = _info_strings.find(key);

    if (it == _info_strings.end()) {
        _info_strings.insert(std::make_pair(key, value));
        _info_strings_display_order.push_back(key);
    } else {
        it->second = value;
    }
}

const std::string* RuntimeProfile::get_info_string(const std::string& key) {
    std::lock_guard<std::mutex> l(_info_strings_lock);
    InfoStrings::const_iterator it = _info_strings.find(key);

    if (it == _info_strings.end()) {
        return nullptr;
    }

    return &it->second;
}

#define ADD_COUNTER_IMPL(NAME, T)                                                                  \
    RuntimeProfile::T* RuntimeProfile::NAME(const std::string& name, TUnit::type unit,             \
                                            const std::string& parent_counter_name) {              \
        DCHECK_EQ(_is_averaged_profile, false);                                                    \
        std::lock_guard<std::mutex> l(_counter_map_lock);                                          \
        if (_counter_map.find(name) != _counter_map.end()) {                                       \
            return reinterpret_cast<T*>(_counter_map[name]);                                       \
        }                                                                                          \
        DCHECK(parent_counter_name == ROOT_COUNTER ||                                              \
               _counter_map.find(parent_counter_name) != _counter_map.end());                      \
        T* counter = _pool->add(new T(unit));                                                      \
        _counter_map[name] = counter;                                                              \
        std::set<std::string>* child_counters =                                                    \
                find_or_insert(&_child_counter_map, parent_counter_name, std::set<std::string>()); \
        child_counters->insert(name);                                                              \
        return counter;                                                                            \
    }

//ADD_COUNTER_IMPL(AddCounter, Counter);
ADD_COUNTER_IMPL(AddHighWaterMarkCounter, HighWaterMarkCounter);
//ADD_COUNTER_IMPL(AddConcurrentTimerCounter, ConcurrentTimerCounter);

std::shared_ptr<RuntimeProfile::HighWaterMarkCounter> RuntimeProfile::AddSharedHighWaterMarkCounter(
        const std::string& name, TUnit::type unit, const std::string& parent_counter_name) {
    DCHECK_EQ(_is_averaged_profile, false);
    std::lock_guard<std::mutex> l(_counter_map_lock);
    if (_shared_counter_pool.find(name) != _shared_counter_pool.end()) {
        return _shared_counter_pool[name];
    }
    DCHECK(parent_counter_name == ROOT_COUNTER ||
           _counter_map.find(parent_counter_name) != _counter_map.end());
    std::shared_ptr<HighWaterMarkCounter> counter = std::make_shared<HighWaterMarkCounter>(unit);
    _shared_counter_pool[name] = counter;

    DCHECK(_counter_map.find(name) == _counter_map.end())
            << "already has a raw counter named " << name;

    // it's OK to insert shared counter to _counter_map, cuz _counter_map is not the owner of counters
    _counter_map[name] = counter.get();
    std::set<std::string>* child_counters =
            find_or_insert(&_child_counter_map, parent_counter_name, std::set<std::string>());
    child_counters->insert(name);
    return counter;
}

RuntimeProfile::Counter* RuntimeProfile::add_counter(const std::string& name, TUnit::type type,
                                                     const std::string& parent_counter_name,
                                                     int64_t level) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    // TODO(yingchun): Can we ensure that 'name' is not exist in '_counter_map'? Use CHECK instead?
    if (_counter_map.find(name) != _counter_map.end()) {
        // TODO: should we make sure that we don't return existing derived counters?
        return _counter_map[name];
    }

    DCHECK(parent_counter_name == ROOT_COUNTER ||
           _counter_map.find(parent_counter_name) != _counter_map.end());
    Counter* counter = _pool->add(new Counter(type, 0, level));
    _counter_map[name] = counter;
    std::set<std::string>* child_counters =
            find_or_insert(&_child_counter_map, parent_counter_name, std::set<std::string>());
    child_counters->insert(name);
    return counter;
}

RuntimeProfile::DerivedCounter* RuntimeProfile::add_derived_counter(
        const std::string& name, TUnit::type type, const DerivedCounterFunction& counter_fn,
        const std::string& parent_counter_name) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    if (_counter_map.find(name) != _counter_map.end()) {
        return nullptr;
    }

    DerivedCounter* counter = _pool->add(new DerivedCounter(type, counter_fn));
    _counter_map[name] = counter;
    std::set<std::string>* child_counters =
            find_or_insert(&_child_counter_map, parent_counter_name, std::set<std::string>());
    child_counters->insert(name);
    return counter;
}

RuntimeProfile::Counter* RuntimeProfile::get_counter(const std::string& name) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    if (_counter_map.find(name) != _counter_map.end()) {
        return _counter_map[name];
    }

    return nullptr;
}

void RuntimeProfile::get_counters(const std::string& name, std::vector<Counter*>* counters) {
    Counter* c = get_counter(name);

    if (c != nullptr) {
        counters->push_back(c);
    }

    std::lock_guard<std::mutex> l(_children_lock);

    for (int i = 0; i < _children.size(); ++i) {
        _children[i].first->get_counters(name, counters);
    }
}

// Print the profile:
//  1. Profile Name
//  2. Info Strings
//  3. Counters
//  4. Children
void RuntimeProfile::pretty_print(std::ostream* s, const std::string& prefix) const {
    std::ostream& stream = *s;

    // create copy of _counter_map and _child_counter_map so we don't need to hold lock
    // while we call value() on the counters
    CounterMap counter_map;
    ChildCounterMap child_counter_map;
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        counter_map = _counter_map;
        child_counter_map = _child_counter_map;
    }

    std::map<std::string, Counter*>::const_iterator total_time = counter_map.find("TotalTime");
    DCHECK(total_time != counter_map.end());

    stream.flags(std::ios::fixed);
    stream << prefix << _name << ":";

    if (total_time->second->value() != 0) {
        stream << "(Active: "
               << PrettyPrinter::print(total_time->second->value(), total_time->second->type())
               << ", non-child: " << std::setprecision(2) << _local_time_percent << "%)";
    }

    stream << std::endl;

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        for (const std::string& key : _info_strings_display_order) {
            stream << prefix << "   - " << key << ": " << _info_strings.find(key)->second
                   << std::endl;
        }
    }

    {
        // Print all the event timers as the following:
        // <EventKey> Timeline: 2s719ms
        //     - Event 1: 6.522us (6.522us)
        //     - Event 2: 2s288ms (2s288ms)
        //     - Event 3: 2s410ms (121.138ms)
        // The times in parentheses are the time elapsed since the last event.
        std::lock_guard<std::mutex> l(_event_sequences_lock);
        for (const EventSequenceMap::value_type& event_sequence : _event_sequence_map) {
            stream << prefix << "  " << event_sequence.first << ": "
                   << PrettyPrinter::print(event_sequence.second->elapsed_time(), TUnit::TIME_NS)
                   << std::endl;

            int64_t last = 0L;
            for (const EventSequence::Event& event : event_sequence.second->events()) {
                stream << prefix << "     - " << event.first << ": "
                       << PrettyPrinter::print(event.second, TUnit::TIME_NS) << " ("
                       << PrettyPrinter::print(event.second - last, TUnit::TIME_NS) << ")"
                       << std::endl;
                last = event.second;
            }
        }
    }

    RuntimeProfile::print_child_counters(prefix, ROOT_COUNTER, counter_map, child_counter_map, s);

    // create copy of _children so we don't need to hold lock while we call
    // pretty_print() on the children
    ChildVector children;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        children = _children;
    }

    for (int i = 0; i < children.size(); ++i) {
        RuntimeProfile* profile = children[i].first;
        bool indent = children[i].second;
        profile->pretty_print(s, prefix + (indent ? "  " : ""));
    }
}

void RuntimeProfile::add_to_span(OpentelemetrySpan span) {
    if (!span || !span->IsRecording() || _added_to_span) {
        return;
    }
    _added_to_span = true;

    CounterMap counter_map;
    ChildCounterMap child_counter_map;
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        counter_map = _counter_map;
        child_counter_map = _child_counter_map;
    }

    auto total_time = counter_map.find("TotalTime");
    DCHECK(total_time != counter_map.end());

    // profile name like "VDataBufferSender  (dst_fragment_instance_id=-2608c96868f3b77d--713968f450bfbe0d):"
    // to "VDataBufferSender"
    auto i = _name.find_first_of("(: ");
    auto short_name = _name.substr(0, i);
    span->SetAttribute(short_name + SPAN_ATTRIBUTE_KEY_SEPARATOR + "TotalTime",
                       print_counter(total_time->second));

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        for (const std::string& key : _info_strings_display_order) {
            // nlohmann json will core dump when serializing 'KeyRanges', here temporarily skip it.
            if (key.compare("KeyRanges") == 0) {
                continue;
            }
            span->SetAttribute(short_name + SPAN_ATTRIBUTE_KEY_SEPARATOR + key,
                               _info_strings.find(key)->second);
        }
    }

    RuntimeProfile::add_child_counters_to_span(span, short_name, ROOT_COUNTER, counter_map,
                                               child_counter_map);

    ChildVector children;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        children = _children;
    }

    for (auto& [profile, flag] : children) {
        profile->add_to_span(span);
    }
}

void RuntimeProfile::add_child_counters_to_span(OpentelemetrySpan span,
                                                const std::string& profile_name,
                                                const std::string& counter_name,
                                                const CounterMap& counter_map,
                                                const ChildCounterMap& child_counter_map) {
    ChildCounterMap::const_iterator itr = child_counter_map.find(counter_name);

    if (itr != child_counter_map.end()) {
        const std::set<std::string>& child_counters = itr->second;
        for (const std::string& child_counter : child_counters) {
            CounterMap::const_iterator iter = counter_map.find(child_counter);
            DCHECK(iter != counter_map.end());
            span->SetAttribute(profile_name + SPAN_ATTRIBUTE_KEY_SEPARATOR + iter->first,
                               print_counter(iter->second));
            RuntimeProfile::add_child_counters_to_span(span, profile_name, child_counter,
                                                       counter_map, child_counter_map);
        }
    }
}

void RuntimeProfile::to_thrift(TRuntimeProfileTree* tree) {
    tree->nodes.clear();
    to_thrift(&tree->nodes);
}

void RuntimeProfile::sub_projection() {
    static constexpr auto projection_name = "ProjectionTime";
    for (auto& [child, _t] : _children) {
        if (child->_counter_map.contains(projection_name)) {
            _counter_total_time.sub_value(child->_counter_map[projection_name]->value());
        }
    }
}

void RuntimeProfile::to_thrift(std::vector<TRuntimeProfileNode>* nodes) {
    nodes->reserve(nodes->size() + _children.size());

    int index = nodes->size();
    nodes->push_back(TRuntimeProfileNode());
    TRuntimeProfileNode& node = (*nodes)[index];
    node.name = _name;
    node.metadata = _metadata;
    node.timestamp = _timestamp;
    node.indent = true;
    if (this->is_set_sink()) {
        node.__set_is_sink(this->is_sink());
    }
    sub_projection();
    CounterMap counter_map;
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        counter_map = _counter_map;
        node.child_counters_map = _child_counter_map;
    }

    for (std::map<std::string, Counter*>::const_iterator iter = counter_map.begin();
         iter != counter_map.end(); ++iter) {
        TCounter counter;
        counter.name = iter->first;
        counter.value = iter->second->value();
        counter.type = iter->second->type();
        counter.__set_level(iter->second->level());
        node.counters.push_back(counter);
    }

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        node.info_strings = _info_strings;
        node.info_strings_display_order = _info_strings_display_order;
    }

    ChildVector children;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        children = _children;
    }
    node.num_children = children.size();

    for (int i = 0; i < children.size(); ++i) {
        int child_idx = nodes->size();
        children[i].first->to_thrift(nodes);
        // fix up indentation flag
        (*nodes)[child_idx].indent = children[i].second;
    }
}

int64_t RuntimeProfile::units_per_second(const RuntimeProfile::Counter* total_counter,
                                         const RuntimeProfile::Counter* timer) {
    DCHECK(total_counter->type() == TUnit::BYTES || total_counter->type() == TUnit::UNIT);
    DCHECK(timer->type() == TUnit::TIME_NS);

    if (timer->value() == 0) {
        return 0;
    }

    double secs = static_cast<double>(timer->value()) / 1000.0 / 1000.0 / 1000.0;
    return total_counter->value() / secs;
}

int64_t RuntimeProfile::counter_sum(const std::vector<Counter*>* counters) {
    int64_t value = 0;

    for (int i = 0; i < counters->size(); ++i) {
        value += (*counters)[i]->value();
    }

    return value;
}

RuntimeProfile::Counter* RuntimeProfile::add_rate_counter(const std::string& name,
                                                          Counter* src_counter) {
    TUnit::type dst_type;

    switch (src_counter->type()) {
    case TUnit::BYTES:
        dst_type = TUnit::BYTES_PER_SECOND;
        break;

    case TUnit::UNIT:
        dst_type = TUnit::UNIT_PER_SECOND;
        break;

    default:
        DCHECK(false) << "Unsupported src counter type: " << src_counter->type();
        return nullptr;
    }

    Counter* dst_counter = add_counter(name, dst_type);
    return dst_counter;
}

RuntimeProfile::Counter* RuntimeProfile::add_rate_counter(const std::string& name, SampleFn fn,
                                                          TUnit::type dst_type) {
    return add_counter(name, dst_type);
}

RuntimeProfile::Counter* RuntimeProfile::add_sampling_counter(const std::string& name,
                                                              Counter* src_counter) {
    DCHECK(src_counter->type() == TUnit::UNIT);
    return add_counter(name, TUnit::DOUBLE_VALUE);
}

RuntimeProfile::Counter* RuntimeProfile::add_sampling_counter(const std::string& name,
                                                              SampleFn sample_fn) {
    return add_counter(name, TUnit::DOUBLE_VALUE);
}

RuntimeProfile::EventSequence* RuntimeProfile::add_event_sequence(const std::string& name) {
    std::lock_guard<std::mutex> l(_event_sequences_lock);
    EventSequenceMap::iterator timer_it = _event_sequence_map.find(name);

    if (timer_it != _event_sequence_map.end()) {
        return timer_it->second;
    }

    EventSequence* timer = _pool->add(new EventSequence());
    _event_sequence_map[name] = timer;
    return timer;
}

void RuntimeProfile::print_child_counters(const std::string& prefix,
                                          const std::string& counter_name,
                                          const CounterMap& counter_map,
                                          const ChildCounterMap& child_counter_map,
                                          std::ostream* s) {
    std::ostream& stream = *s;
    ChildCounterMap::const_iterator itr = child_counter_map.find(counter_name);

    if (itr != child_counter_map.end()) {
        const std::set<std::string>& child_counters = itr->second;
        for (const std::string& child_counter : child_counters) {
            CounterMap::const_iterator iter = counter_map.find(child_counter);
            DCHECK(iter != counter_map.end());
            stream << prefix << "   - " << iter->first << ": "
                   << PrettyPrinter::print(iter->second->value(), iter->second->type())
                   << std::endl;
            RuntimeProfile::print_child_counters(prefix + "  ", child_counter, counter_map,
                                                 child_counter_map, s);
        }
    }
}

} // namespace doris
