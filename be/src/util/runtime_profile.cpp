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
#include <rapidjson/encodings.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>

#include "common/logging.h"
#include "common/object_pool.h"
#include "util/runtime_profile_counter_tree_node.h"
#ifdef BE_TEST
#include "common/status.h" // For ErrorCode
#endif

namespace doris {
#include "common/compile_check_begin.h"
// Thread counters name
static const std::string THREAD_VOLUNTARY_CONTEXT_SWITCHES = "VoluntaryContextSwitches";
static const std::string THREAD_INVOLUNTARY_CONTEXT_SWITCHES = "InvoluntaryContextSwitches";

const std::string RuntimeProfile::ROOT_COUNTER;

std::unique_ptr<RuntimeProfile> RuntimeProfile::from_thrift(const TRuntimeProfileTree& node) {
    if (node.nodes.empty()) {
        return std::make_unique<RuntimeProfile>("");
    }
    TRuntimeProfileNode root_node = node.nodes.front();
    std::unique_ptr<RuntimeProfile> res = std::make_unique<RuntimeProfile>(root_node.name);
    res->update(node);
    return res;
}

std::unique_ptr<RuntimeProfile> RuntimeProfile::from_proto(const PRuntimeProfileTree& tree) {
    if (tree.nodes().empty()) {
        return std::make_unique<RuntimeProfile>("");
    }

    const PRuntimeProfileNode& root_node = tree.nodes(0);
    std::unique_ptr<RuntimeProfile> res = std::make_unique<RuntimeProfile>(root_node.name());
    res->update(tree);
    return res;
}

RuntimeProfile::RuntimeProfile(std::string name, bool is_averaged_profile, int64_t metadata, int64_t timestamp)
    : _pool(new ObjectPool()),
      _name(std::move(name)),
      _metadata(metadata),
      _timestamp(timestamp),
      _is_averaged_profile(is_averaged_profile),
      _counter_total_time(TUnit::TIME_NS, 0, 3),
      _local_time_percent(0) {
    // TotalTime counter has level3 to disable it from plan profile, because
    // it contains its child running time, we use exec time instead.
    _counter_map["TotalTime"] = &_counter_total_time;
}

RuntimeProfile::~RuntimeProfile() = default;

bool RuntimeProfile::Counter::operator==(const Counter& other) const {
    return _value.load(std::memory_order_relaxed) == other._value.load(std::memory_order_relaxed) &&
           _type == other._type && _level == other._level;
}

void RuntimeProfile::merge(const RuntimeProfile* other) {
    DCHECK(other != nullptr);

    // Merge this level
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        std::lock_guard<std::mutex> m(other->_counter_map_lock);

        for (const auto& [other_name, other_counter] : other->_counter_map) {
            if (auto iter = _counter_map.find(other_name); iter != _counter_map.end()) {
                auto* this_counter = iter->second;
                CHECK_EQ(this_counter->type(), other_counter->type()) << "Cannot merge counter with the different types";
                if (this_counter->type() == TUnit::DOUBLE_VALUE) {
                    this_counter->set(this_counter->double_value() + other_counter->double_value());
                } else {
                    this_counter->update(other_counter->value());
                }
            } else {
                _counter_map[other_name] = _pool->add(other_counter->clone());
            }
        }

        for (const auto & [parent_name, children_names] : other->_child_counter_map) {
            _child_counter_map[parent_name].insert(children_names.begin(), children_names.end());
        }
    }

    // Recursively merge children with matching names
    {
        std::lock_guard<std::mutex> l(_children_lock);
        std::lock_guard<std::mutex> m(other->_children_lock);

        for (const auto & [other_child, indent] : other->_children) {
            RuntimeProfile* child = nullptr;
            if (auto iter = _child_map.find(other_child->_name); iter != _child_map.end()) {
                child = iter->second;
            } else {
                child = _pool->add(new RuntimeProfile(other_child->_name, 
                                        other_child->_metadata, other_child->_timestamp));
                child->_local_time_percent = other_child->_local_time_percent;
                _child_map[child->_name] = child;
                _children.emplace_back(child, indent);
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

void RuntimeProfile::update(const PRuntimeProfileTree& proto_profile) {
    int idx = 0;
    update(proto_profile.nodes(), &idx);
    DCHECK_EQ(idx, proto_profile.nodes_size());
}

void RuntimeProfile::update(const std::vector<TRuntimeProfileNode>& nodes, int* idx) {
    DCHECK_LT(*idx, nodes.size());
    const TRuntimeProfileNode& node = nodes[*idx];

    // update this level
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);

        for (const auto& tcounter : node.counters) {
            const std::string& name = tcounter.name;
            if (auto iter = _counter_map.find(name); iter != _counter_map.end()) {
                CHECK_EQ(iter->second->type(), tcounter.type)
                        << "Cannot update counters with the same name (" << name
                        << ") but different types.";
                        iter->second->set(tcounter.value);
            } else {
                _counter_map[name] =
                        _pool->add(new Counter(tcounter.type, tcounter.value));
            }
        }

        for (const auto& [parent_name, children_names] : node.child_counters_map) {
            _child_counter_map[parent_name].insert(children_names.begin(), children_names.end());
        }
    }

    // Look for existing info strings and update in place. If there are new strings, add them to the end of the display order.
    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        const InfoStrings& info_strings = node.info_strings;

        for (const std::string& key : node.info_strings_display_order) {
            // TODO: Is nodes.info_strings always a superset of
            // _info_strings? If so, can just copy the display order.
            auto it = info_strings.find(key);
            DCHECK(it != info_strings.end());
            if (auto existing = _info_strings.find(key); existing != _info_strings.end()) {
                _info_strings[key] = it->second;
            } else {
                _info_strings.insert(std::make_pair(key, it->second));
                _info_strings_display_order.push_back(key);
            }
        }
    }

    // update children with matching names; create new ones if they don't match
    ++*idx;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        for (int i = 0; i < node.num_children; ++i) {
            const auto& tchild = nodes[*idx];
            RuntimeProfile* child = nullptr;

            if (const auto& iter =  _child_map.find(tchild.name); iter != _child_map.end()) {
                child = iter->second;
            } else {
                child = _pool->add(new RuntimeProfile(tchild.name, tchild.metadata, tchild.timestamp));
                _child_map[tchild.name] = child;
                _children.emplace_back(child, tchild.indent);
            }

            child->update(nodes, idx);
        }
    }
}

void RuntimeProfile::update(const google::protobuf::RepeatedPtrField<PRuntimeProfileNode>& nodes,
                            int* idx) {
    DCHECK_LT(*idx, nodes.size());
    const PRuntimeProfileNode& node = nodes.Get(*idx);

    // update this level
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);

        for (const auto& pcounter : node.counters()) {
            const std::string& name = pcounter.name();

            if (auto iter = _counter_map.find(name); iter != _counter_map.find(name)) {
                if (unit_to_proto(iter->second->type()) != pcounter.type()) {
                    CHECK_EQ(unit_to_proto(iter->second->type()), pcounter.type())
                        << "Cannot update counters with the same name ("
                        << name << ") but different types.";
                } else {
                    iter->second->set(pcounter.value());
                }
            } else {
                _counter_map[name] =
                _pool->add(new Counter(unit_to_thrift(pcounter.type()), pcounter.value()));
            }
        }

        for (const auto& [parent_name, children_names] : node.child_counters_map()) {
            for (const auto& child_name : children_names.child_counters()) {
                _child_counter_map[parent_name].insert(child_name);
            }
        }
    }

    // Look for existing info strings and update in place. If there are new strings, add them to the end of the display order.
    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        const auto& info_map = node.info_strings();

        for (const std::string& key : node.info_strings_display_order()) {
            auto it = info_map.find(key);
            DCHECK(it != info_map.end());
            if (auto existing = _info_strings.find(key); existing != _info_strings.end()) {
                _info_strings[key] = it->second;
            } else {
                _info_strings.insert(std::make_pair(key, it->second));
                _info_strings_display_order.push_back(key);
            }
        }
    }

    // update children with matching names; create new ones if they don't match
    ++*idx;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        for (int i = 0; i < node.num_children(); ++i) {
            const PRuntimeProfileNode& pchild = nodes.Get(*idx);
            RuntimeProfile* child = nullptr;

            if (const auto& iter =  _child_map.find(pchild.name()); iter != _child_map.end()) {
                child = iter->second;
            } else {
                child = _pool->add(new RuntimeProfile(pchild.name(), pchild.metadata(), pchild.timestamp()));
                _child_map[pchild.name()] = child;
                _children.emplace_back(child, pchild.indent());
            }

            child->update(nodes, idx);
        }
    }
}

void RuntimeProfile::divide(int n) {
    DCHECK_GT(n, 0);

    // divide this
    {
        std::lock_guard<std::mutex> l(_counter_map_lock);

        for (auto & [_, counter] : _counter_map) {
            if (counter->type() == TUnit::DOUBLE_VALUE) {
                counter->set(counter->double_value() / n);
            } else {
                auto value = counter->_value.load();
                value = value / n;
                counter->_value.store(value);
            }
        }
    }

    // divide children
    {
        std::lock_guard<std::mutex> l(_children_lock);

        for (auto & [_, child_profile] : _child_map) {
            child_profile->divide(n);
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

    for (auto & [child_profile, _] : _children) {
        total_child_time += child_profile->total_time_counter()->value();
    }

    auto local_time = total_time_counter()->value() - total_child_time;
    // Counters have some margin, set to 0 if it was negative.
    local_time = std::max<int64_t>(0L, local_time);
    _local_time_percent = static_cast<double>(local_time) / static_cast<double>(total);
    _local_time_percent = std::min(1.0, _local_time_percent) * 100;

    // Recurse on children
    for (auto & [child_profile, _] : _children) {
        child_profile->compute_time_in_profile(total);
    }
}

RuntimeProfile* RuntimeProfile::create_child(const std::string& name, bool indent, bool prepend) {
    std::lock_guard<std::mutex> l(_children_lock);
    DCHECK(!_child_map.contains(name)) << ", name: " << name;
    RuntimeProfile* child = _pool->add(new RuntimeProfile(name));
    if (is_set_metadata()) {
        child->set_metadata(this->metadata());
    }

    if (_children.empty()) {
        add_child_unlock(child, indent, nullptr);
    } else {
        auto* pos = prepend ? _children.begin()->first : nullptr;
        add_child_unlock(child, indent, pos);
    }
    return child;
}

void RuntimeProfile::add_child_unlock(RuntimeProfile* child, bool indent, RuntimeProfile* loc) {
    DCHECK(child != nullptr);
    _child_map[child->_name] = child;

    if (loc == nullptr) {
        _children.emplace_back(child, indent);
    } else {
        for (auto it = _children.begin(); it != _children.end(); ++it) {
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

std::optional<RuntimeProfile*> RuntimeProfile::get_child(const std::string& name) {
    std::lock_guard<std::mutex> l(_children_lock);
    if (auto it = _child_map.find(name); it != _child_map.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<RuntimeProfile*> RuntimeProfile::get_children() const {
    std::lock_guard<std::mutex> l(_children_lock);
    std::vector<RuntimeProfile*> children;
    children.reserve(_child_map.size());

    for (const auto& [_, child_profile] : _child_map) {
        children.push_back(child_profile);
    }
    return children;
}

std::vector<RuntimeProfile*> RuntimeProfile::get_all_children() {
    std::lock_guard<std::mutex> l(_children_lock);
    std::vector<RuntimeProfile*> children;

    for (auto& [_, child_profile] : _child_map) {
        children.push_back(child_profile);
        auto child_children = child_profile->get_all_children();
        children.insert(children.end(), child_children.begin(), child_children.end());
    }
    return children;
}

void RuntimeProfile::add_info_string(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> l(_info_strings_lock);
    if (auto it = _info_strings.find(key); it != _info_strings.end()) {
        it->second = value;
    } else {
        _info_strings.insert(std::make_pair(key, value));
        _info_strings_display_order.push_back(key);
    }
}

std::optional<std::string> RuntimeProfile::get_info_string(const std::string& key) {
    std::lock_guard<std::mutex> l(_info_strings_lock);
    if (auto it = _info_strings.find(key); it != _info_strings.end()) {
        return it->second;
    }

    return std::nullopt;
}

RuntimeProfile::HighWaterMarkCounter* RuntimeProfile::AddHighWaterMarkCounter(
        const std::string& name, TUnit::type unit, const std::string& parent_counter_name,
        int64_t level) {
    DCHECK_EQ(_is_averaged_profile, false);
    std::lock_guard<std::mutex> l(_counter_map_lock);
    if (_counter_map.contains(name)) {
        return reinterpret_cast<RuntimeProfile::HighWaterMarkCounter*>(_counter_map[name]);
    }
    DCHECK(parent_counter_name == ROOT_COUNTER ||
           _counter_map.find(parent_counter_name) != _counter_map.end());
    RuntimeProfile::HighWaterMarkCounter* counter =
            _pool->add(new RuntimeProfile::HighWaterMarkCounter(unit, level, parent_counter_name));
    _counter_map[name] = counter;
    _child_counter_map[parent_counter_name].insert(name);
    return counter;
}

RuntimeProfile::Counter* RuntimeProfile::add_counter(const std::string& name, TUnit::type type,
                                                     const std::string& parent_counter_name,
                                                     int64_t level) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    if (_counter_map.contains(name)) {
        return _counter_map[name];
    }

    // Parent counter must already exist.
    DCHECK(parent_counter_name == ROOT_COUNTER ||
           _counter_map.find(parent_counter_name) != _counter_map.end());

    Counter* counter = _pool->add(new Counter(type, 0, level));
    _counter_map[name] = counter;
    _child_counter_map[parent_counter_name].insert(name);
    return counter;
}

RuntimeProfile::NonZeroCounter* RuntimeProfile::add_nonzero_counter(
        const std::string& name, TUnit::type type, const std::string& parent_counter_name,
        int64_t level) {
    std::lock_guard<std::mutex> l(_counter_map_lock);
    if (_counter_map.find(name) != _counter_map.end()) {
        DCHECK(dynamic_cast<NonZeroCounter*>(_counter_map[name]));
        return static_cast<NonZeroCounter*>(_counter_map[name]);
    }

    DCHECK(parent_counter_name == ROOT_COUNTER ||
           _counter_map.find(parent_counter_name) != _counter_map.end());
    NonZeroCounter* counter = _pool->add(new NonZeroCounter(type, level, parent_counter_name));
    _counter_map[name] = counter;
    _child_counter_map[parent_counter_name].insert(name);
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
    _child_counter_map[parent_counter_name].insert(name);
    return counter;
}

void RuntimeProfile::add_description(const std::string& name, const std::string& description,
                                     std::string parent_counter_name) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    if (_counter_map.find(name) != _counter_map.end()) {
        Counter* counter = _counter_map[name];
        if (dynamic_cast<DescriptionEntry*>(counter) != nullptr) {
            // Do replace instead of update to avoid data race.
            _counter_map.erase(name);
        } else {
            DCHECK(false) << "Counter type mismatch, name: " << name
                          << ", type: " << counter->type() << ", description: " << description;
        }
    }

    // Parent counter must already exist.
    DCHECK(parent_counter_name == ROOT_COUNTER ||
           _counter_map.find(parent_counter_name) != _counter_map.end());
    DescriptionEntry* counter = _pool->add(new DescriptionEntry(name, description));
    _counter_map[name] = counter;
    _child_counter_map[parent_counter_name].insert(name);
}

RuntimeProfile::ConditionCounter* RuntimeProfile::add_conditition_counter(
        const std::string& name, TUnit::type type, const ConditionCounterFunction& counter_fn,
        const std::string& parent_counter_name, int64_t level) {
    std::lock_guard<std::mutex> l(_counter_map_lock);

    if (_counter_map.find(name) != _counter_map.end()) {
        RuntimeProfile::ConditionCounter* contition_counter =
                dynamic_cast<ConditionCounter*>(_counter_map[name]);
        if (contition_counter == nullptr) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "Failed to add a conditition counter that is duplicate and of a "
                                   "different type for {}.",
                                   name);
        }
        return contition_counter;
    }

    ConditionCounter* counter = _pool->add(new ConditionCounter(type, counter_fn, level));
    _counter_map[name] = counter;
    _child_counter_map[parent_counter_name].insert(name);
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
void RuntimeProfile::pretty_print(std::ostream* s, const std::string& prefix,
                                  int64_t profile_level) const {
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

void RuntimeProfile::to_thrift(TRuntimeProfileTree* tree, int64_t profile_level) {
    tree->nodes.clear();
    to_thrift(&tree->nodes, profile_level);
}

void RuntimeProfile::to_thrift(std::vector<TRuntimeProfileNode>* nodes, int64_t profile_level) {
    size_t index = nodes->size();
    nodes->push_back(TRuntimeProfileNode());
    TRuntimeProfileNode& node = (*nodes)[index];
    node.name = _name;
    node.metadata = _metadata;
    node.timestamp = _timestamp;
    node.indent = true;

    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        RuntimeProfileCounterTreeNode conter_tree = RuntimeProfileCounterTreeNode::from_map(
                _counter_map, _child_counter_map, ROOT_COUNTER);
        conter_tree = RuntimeProfileCounterTreeNode::prune_the_tree(conter_tree, profile_level);
        conter_tree.to_thrift(node.counters, node.child_counters_map);
    }

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        node.info_strings = _info_strings;
        node.info_strings_display_order = _info_strings_display_order;
    }

    ChildVector children;
    {
        // _children may be modified during to_thrift(),
        // so we have to lock and copy _children to avoid race condition
        std::lock_guard<std::mutex> l(_children_lock);
        children = _children;
    }
    node.num_children = cast_set<int32_t>(children.size());
    nodes->reserve(nodes->size() + children.size());

    for (int i = 0; i < children.size(); ++i) {
        size_t child_idx = nodes->size();
        children[i].first->to_thrift(nodes, profile_level);
        // fix up indentation flag
        (*nodes)[child_idx].indent = children[i].second;
    }
}

void RuntimeProfile::to_proto(PRuntimeProfileTree* tree, int64_t profile_level) {
    tree->clear_nodes();
    to_proto(tree->mutable_nodes(), profile_level);
}

void RuntimeProfile::to_proto(google::protobuf::RepeatedPtrField<PRuntimeProfileNode>* nodes,
                              int64_t profile_level) {
    PRuntimeProfileNode* node = nodes->Add(); // allocate new node
    node->set_name(_name);
    node->set_metadata(_metadata);
    node->set_timestamp(_timestamp);
    node->set_indent(true);

    {
        std::lock_guard<std::mutex> l(_counter_map_lock);
        RuntimeProfileCounterTreeNode counter_tree = RuntimeProfileCounterTreeNode::from_map(
                _counter_map, _child_counter_map, ROOT_COUNTER);
        counter_tree = RuntimeProfileCounterTreeNode::prune_the_tree(counter_tree, profile_level);
        counter_tree.to_proto(node->mutable_counters(), node->mutable_child_counters_map());
    }

    {
        std::lock_guard<std::mutex> l(_info_strings_lock);
        auto* info_map = node->mutable_info_strings();
        for (const auto& kv : _info_strings) {
            (*info_map)[kv.first] = kv.second;
        }
        for (const auto& key : _info_strings_display_order) {
            node->add_info_strings_display_order(key);
        }
    }

    ChildVector children;
    {
        std::lock_guard<std::mutex> l(_children_lock);
        children = _children;
    }

    node->set_num_children(cast_set<int32_t>(children.size()));

    for (const auto& child : children) {
        int child_index = cast_set<int>(nodes->size()); // capture index for indent correction
        child.first->to_proto(nodes, profile_level);
        (*nodes)[child_index].set_indent(child.second);
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
    return int64_t(static_cast<double>(total_counter->value()) / secs);
}

int64_t RuntimeProfile::counter_sum(const std::vector<Counter*>* counters) {
    int64_t value = 0;

    for (int i = 0; i < counters->size(); ++i) {
        value += (*counters)[i]->value();
    }

    return value;
}

void RuntimeProfile::print_child_counters(const std::string& prefix,
                                          const std::string& counter_name,
                                          const CounterMap& counter_map,
                                          const ChildCounterMap& child_counter_map,
                                          std::ostream* s) {
    auto itr = child_counter_map.find(counter_name);

    if (itr != child_counter_map.end()) {
        const std::set<std::string>& child_counters = itr->second;
        for (const std::string& child_counter : child_counters) {
            auto iter = counter_map.find(child_counter);
            DCHECK(iter != counter_map.end());
            iter->second->pretty_print(s, prefix, iter->first);
            RuntimeProfile::print_child_counters(prefix + "  ", child_counter, counter_map,
                                                 child_counter_map, s);
        }
    }
}

} // namespace doris
