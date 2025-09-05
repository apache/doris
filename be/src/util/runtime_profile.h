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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/runtime-profile.h
// and modified by Doris

#pragma once

#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/runtime_profile.pb.h>
#include <glog/logging.h>
#include <stdint.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "util/binary_cast.hpp"
#include "util/pretty_printer.h"
#include "util/stopwatch.hpp"

namespace doris {
#include "common/compile_check_begin.h"
class TRuntimeProfileNode;
class TRuntimeProfileTree;
class RuntimeProfileCounterTreeNode;

// Some macro magic to generate unique ids using __COUNTER__
#define CONCAT_IMPL(x, y) x##y
#define MACRO_CONCAT(x, y) CONCAT_IMPL(x, y)

#define ADD_LABEL_COUNTER(profile, name) (profile)->add_counter(name, TUnit::NONE)
#define ADD_LABEL_COUNTER_WITH_LEVEL(profile, name, level) \
    (profile)->add_counter_with_level(name, TUnit::NONE, level)
#define ADD_COUNTER(profile, name, type) (profile)->add_counter(name, type)
#define ADD_COUNTER_WITH_LEVEL(profile, name, type, level) \
    (profile)->add_counter_with_level(name, type, level)
#define ADD_TIMER(profile, name) (profile)->add_counter(name, TUnit::TIME_NS)
#define ADD_TIMER_WITH_LEVEL(profile, name, level) \
    (profile)->add_counter_with_level(name, TUnit::TIME_NS, level)
#define ADD_CHILD_COUNTER(profile, name, type, parent) (profile)->add_counter(name, type, parent)
#define ADD_CHILD_COUNTER_WITH_LEVEL(profile, name, type, parent, level) \
    (profile)->add_counter(name, type, parent, level)
#define ADD_CHILD_TIMER(profile, name, parent) (profile)->add_counter(name, TUnit::TIME_NS, parent)
#define ADD_CHILD_TIMER_WITH_LEVEL(profile, name, parent, level) \
    (profile)->add_counter(name, TUnit::TIME_NS, parent, level)
#define SCOPED_TIMER(c) ScopedTimer<MonotonicStopWatch> MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c)
#define SCOPED_TIMER_ATOMIC(c) \
    ScopedTimer<MonotonicStopWatch, std::atomic_bool> MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c)
#define SCOPED_CPU_TIMER(c) \
    ScopedTimer<ThreadCpuStopWatch> MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c)
#define CANCEL_SAFE_SCOPED_TIMER(c, is_cancelled) \
    ScopedTimer<MonotonicStopWatch> MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c, is_cancelled)
#define SCOPED_RAW_TIMER(c)                                                                  \
    doris::ScopedRawTimer<doris::MonotonicStopWatch, int64_t> MACRO_CONCAT(SCOPED_RAW_TIMER, \
                                                                           __COUNTER__)(c)
#define SCOPED_ATOMIC_TIMER(c)                                                                 \
    ScopedRawTimer<MonotonicStopWatch, std::atomic<int64_t>> MACRO_CONCAT(SCOPED_ATOMIC_TIMER, \
                                                                          __COUNTER__)(c)
#define COUNTER_UPDATE(c, v) (c)->update(v)
#define COUNTER_SET(c, v) (c)->set(v)

class ObjectPool;

// Runtime profile is a group of profiling counters.  It supports adding named counters
// and being able to serialize and deserialize them.
// The profiles support a tree structure to form a hierarchy of counters.
// Runtime profiles supports measuring wall clock rate based counters.  There is a
// single thread per process that will convert an amount (i.e. bytes) counter to a
// corresponding rate based counter.  This thread wakes up at fixed intervals and updates
// all of the rate counters.
// Thread-safe.
class RuntimeProfile {
public:
    static std::unique_ptr<RuntimeProfile> from_thrift(const TRuntimeProfileTree& node);

    static std::unique_ptr<RuntimeProfile> from_proto(const PRuntimeProfileTree& tree);

    static PProfileUnit unit_to_proto(const TUnit::type& type) {
        switch (type) {
        case TUnit::UNIT: {
            return PProfileUnit::UNIT;
        }
        case TUnit::UNIT_PER_SECOND: {
            return PProfileUnit::UNIT_PER_SECOND;
        }
        case TUnit::CPU_TICKS: {
            return PProfileUnit::CPU_TICKS;
        }
        case TUnit::BYTES: {
            return PProfileUnit::BYTES;
        }
        case TUnit::BYTES_PER_SECOND: {
            return PProfileUnit::BYTES_PER_SECOND;
        }
        case TUnit::TIME_NS: {
            return PProfileUnit::TIME_NS;
        }
        case TUnit::DOUBLE_VALUE: {
            return PProfileUnit::DOUBLE_VALUE;
        }
        case TUnit::NONE: {
            return PProfileUnit::NONE;
        }
        case TUnit::TIME_MS: {
            return PProfileUnit::TIME_MS;
        }
        case TUnit::TIME_S: {
            return PProfileUnit::TIME_S;
        }
        default: {
            DCHECK(false);
            return PProfileUnit::NONE;
        }
        }
    }

    static TUnit::type unit_to_thrift(const PProfileUnit& unit) {
        switch (unit) {
        case PProfileUnit::UNIT: {
            return TUnit::UNIT;
        }
        case PProfileUnit::UNIT_PER_SECOND: {
            return TUnit::UNIT_PER_SECOND;
        }
        case PProfileUnit::CPU_TICKS: {
            return TUnit::CPU_TICKS;
        }
        case PProfileUnit::BYTES: {
            return TUnit::BYTES;
        }
        case PProfileUnit::BYTES_PER_SECOND: {
            return TUnit::BYTES_PER_SECOND;
        }
        case PProfileUnit::TIME_NS: {
            return TUnit::TIME_NS;
        }
        case PProfileUnit::DOUBLE_VALUE: {
            return TUnit::DOUBLE_VALUE;
        }
        case PProfileUnit::NONE: {
            return TUnit::NONE;
        }
        case PProfileUnit::TIME_MS: {
            return TUnit::TIME_MS;
        }
        case PProfileUnit::TIME_S: {
            return TUnit::TIME_S;
        }
        default: {
            DCHECK(false);
            return TUnit::NONE;
        }
        }
    }

    // The root counter name for all top level counters.
    static const std::string ROOT_COUNTER;
    class Counter {
    public:
        Counter(TUnit::type type, int64_t value = 0, int64_t level = 3)
                : _value(value), _type(type), _level(level) {}
        virtual ~Counter() = default;

        virtual Counter* clone() const { return new Counter(type(), value(), _level); }

        virtual void update(int64_t delta) { _value.fetch_add(delta, std::memory_order_relaxed); }

        void bit_or(int64_t delta) { _value.fetch_or(delta, std::memory_order_relaxed); }

        virtual void set(int64_t value) { _value.store(value, std::memory_order_relaxed); }

        virtual void set(double value) {
            DCHECK_EQ(sizeof(value), sizeof(int64_t));
            _value.store(binary_cast<double, int64_t>(value), std::memory_order_relaxed);
        }

        virtual int64_t value() const { return _value.load(std::memory_order_relaxed); }

        virtual double double_value() const {
            return binary_cast<int64_t, double>(_value.load(std::memory_order_relaxed));
        }

        virtual TCounter to_thrift(const std::string& name) const {
            TCounter counter;
            counter.name = name;
            counter.value = this->value();
            counter.type = this->type();
            counter.__set_level(this->_level);
            return counter;
        }

        virtual PProfileCounter to_proto(const std::string& name) const {
            PProfileCounter counter;
            counter.set_name(name);
            counter.set_value(this->value());
            counter.set_type(unit_to_proto(this->type()));
            counter.set_level(this->value());
            return counter;
        }

        virtual void pretty_print(std::ostream* s, const std::string& prefix,
                                  const std::string& name) const {
            std::ostream& stream = *s;
            stream << prefix << "   - " << name << ": "
                   << PrettyPrinter::print(_value.load(std::memory_order_relaxed), type())
                   << std::endl;
        }

        TUnit::type type() const { return _type; }

        virtual int64_t level() const { return _level; }

        void set_level(int64_t level) { _level = level; }

        bool operator==(const Counter& other) const;

    private:
        friend class RuntimeProfile;
        friend class RuntimeProfileCounterTreeNode;

        std::atomic<int64_t> _value;
        TUnit::type _type;
        int64_t _level;
    };

    /// A counter that keeps track of the highest value seen (reporting that
    /// as value()) and the current value.
    class HighWaterMarkCounter : public Counter {
    public:
        HighWaterMarkCounter(TUnit::type unit, int64_t level, const std::string& parent_name,
                             int64_t value = 0, int64_t current_value = 0)
                : Counter(unit, value, level),
                  current_value_(current_value),
                  _parent_name(parent_name) {}

        virtual Counter* clone() const override {
            return new HighWaterMarkCounter(type(), level(), parent_name(), value(),
                                            current_value());
        }

        void add(int64_t delta) {
            current_value_.fetch_add(delta, std::memory_order_relaxed);
            if (delta > 0) {
                UpdateMax(current_value_);
            }
        }
        virtual void update(int64_t delta) override { add(delta); }

        TCounter to_thrift(const std::string& name) const override {
            TCounter counter;
            counter.name = std::move(name);
            counter.value = current_value();
            counter.type = type();
            counter.__set_level(level());
            return counter;
        }

        PProfileCounter to_proto(const std::string& name) const override {
            PProfileCounter counter;
            counter.set_name(name);
            counter.set_value(current_value());
            counter.set_type(unit_to_proto(this->type()));
            counter.set_level(this->value());
            return counter;
        }

        TCounter to_thrift_peak(std::string name) {
            TCounter counter;
            counter.name = std::move(name);
            counter.value = value();
            counter.type = type();
            counter.__set_level(level());
            return counter;
        }

        PProfileCounter to_proto_peak(const std::string& name) const {
            return Counter::to_proto(name);
        }

        virtual void pretty_print(std::ostream* s, const std::string& prefix,
                                  const std::string& name) const override {
            std::ostream& stream = *s;
            stream << prefix << "   - " << name
                   << " Current: " << PrettyPrinter::print(current_value(), type()) << " (Peak: "
                   << PrettyPrinter::print(_value.load(std::memory_order_relaxed), type()) << ")"
                   << std::endl;
        }

        /// Tries to increase the current value by delta. If current_value() + delta
        /// exceeds max, return false and current_value is not changed.
        bool try_add(int64_t delta, int64_t max) {
            while (true) {
                int64_t old_val = current_value_.load(std::memory_order_relaxed);
                int64_t new_val = old_val + delta;
                if (UNLIKELY(new_val > max)) return false;
                if (LIKELY(current_value_.compare_exchange_weak(old_val, new_val,
                                                                std::memory_order_relaxed))) {
                    UpdateMax(new_val);
                    return true;
                }
            }
        }

        void set(int64_t v) override {
            current_value_.store(v, std::memory_order_relaxed);
            UpdateMax(v);
        }

        int64_t current_value() const { return current_value_.load(std::memory_order_relaxed); }

        std::string parent_name() const { return _parent_name; }

    private:
        /// Set '_value' to 'v' if 'v' is larger than '_value'. The entire operation is
        /// atomic.
        void UpdateMax(int64_t v) {
            while (true) {
                int64_t old_max = _value.load(std::memory_order_relaxed);
                int64_t new_max = std::max(old_max, v);
                if (new_max == old_max) {
                    break; // Avoid atomic update.
                }
                if (LIKELY(_value.compare_exchange_weak(old_max, new_max,
                                                        std::memory_order_relaxed))) {
                    break;
                }
            }
        }

        /// The current value of the counter. _value in the super class represents
        /// the high water mark.
        std::atomic<int64_t> current_value_;

        const std::string _parent_name;
    };

    using DerivedCounterFunction = std::function<int64_t()>;

    // A DerivedCounter also has a name and type, but the value is computed.
    // Do not call Set() and Update().
    class DerivedCounter : public Counter {
    public:
        DerivedCounter(TUnit::type type, const DerivedCounterFunction& counter_fn,
                       int64_t value = 0, int64_t level = 1)
                : Counter(type, value, level), _counter_fn(counter_fn) {}

        virtual Counter* clone() const override {
            return new DerivedCounter(type(), _counter_fn, value(), level());
        }

        int64_t value() const override { return _counter_fn(); }

    private:
        DerivedCounterFunction _counter_fn;
    };

    using ConditionCounterFunction = std::function<bool(int64_t, int64_t)>;

    // ConditionCounter is a specialized counter that only updates its value when a specific condition is met.
    // It uses a condition function (condition_func) to determine when the counter's value should be updated.
    // This type of counter is particularly useful for tracking maximum values, minimum values, or other metrics
    // that should only be updated when they meet certain criteria.
    // For example, it can be used to record the maximum value of a specific metric during query execution,
    // or to update the counter only when a new value exceeds some threshold.
    class ConditionCounter : public Counter {
    public:
        ConditionCounter(TUnit::type type, const ConditionCounterFunction& condition_func,
                         int64_t level = 2, int64_t condition = 0, int64_t value = 0)
                : Counter(type, value, level),
                  _condition(condition),
                  _value(value),
                  _condition_func(condition_func) {}

        Counter* clone() const override {
            std::lock_guard<std::mutex> l(_mutex);
            return new ConditionCounter(type(), _condition_func, _condition, value(), level());
        }

        int64_t value() const override {
            std::lock_guard<std::mutex> l(_mutex);
            return _value;
        }

        void conditional_update(int64_t c, int64_t v) {
            std::lock_guard<std::mutex> l(_mutex);
            if (_condition_func(_condition, c)) {
                _value = v;
                _condition = c;
            }
        }

    private:
        mutable std::mutex _mutex;
        int64_t _condition;
        int64_t _value;
        ConditionCounterFunction _condition_func;
    };

    // NonZeroCounter will not be converted to Thrift if the value is 0.
    class NonZeroCounter : public Counter {
    public:
        NonZeroCounter(TUnit::type type, int64_t level, const std::string& parent_name,
                       int64_t value = 0)
                : Counter(type, value, level), _parent_name(parent_name) {}

        virtual Counter* clone() const override {
            return new NonZeroCounter(type(), level(), parent_name(), value());
        }

        std::string parent_name() const { return _parent_name; }

    private:
        const std::string _parent_name;
    };

    class DescriptionEntry : public Counter {
    public:
        DescriptionEntry(const std::string& name, const std::string& description)
                : Counter(TUnit::NONE, 0, 2), _description(description), _name(name) {}

        virtual Counter* clone() const override {
            return new DescriptionEntry(_name, _description);
        }

        void set(int64_t value) override {
            // Do nothing
        }
        void set(double value) override {
            // Do nothing
        }
        void update(int64_t delta) override {
            // Do nothing
        }

        TCounter to_thrift(const std::string& name) const override {
            TCounter counter;
            counter.name = name;
            counter.__set_level(2);
            counter.__set_description(_description);
            return counter;
        }

        PProfileCounter to_proto(const std::string& name) const override {
            PProfileCounter counter;
            counter.set_name(name);
            counter.set_level(2);
            counter.set_description(_description);
            return counter;
        }

    private:
        const std::string _description;
        const std::string _name;
    };

    // Create a runtime profile object with 'name'.
    RuntimeProfile(const std::string& name, bool is_averaged_profile = false);

    ~RuntimeProfile();

    // Adds a child profile.  This is thread safe.
    // 'indent' indicates whether the child will be printed w/ extra indentation
    // relative to the parent.
    // If location is non-null, child will be inserted after location.  Location must
    // already be added to the profile.
    void add_child(RuntimeProfile* child, bool indent, RuntimeProfile* location = nullptr);

    void add_child_unlock(RuntimeProfile* child, bool indent, RuntimeProfile* loc);

    /// Creates a new child profile with the given 'name'. A child profile with that name
    /// must not already exist. If 'prepend' is true, prepended before other child profiles,
    /// otherwise appended after other child profiles.
    RuntimeProfile* create_child(const std::string& name, bool indent = true, bool prepend = false);

    // Merges the src profile into this one, combining counters that have an identical
    // path. Info strings from profiles are not merged. 'src' would be a const if it
    // weren't for locking.
    // Calling this concurrently on two RuntimeProfiles in reverse order results in
    // undefined behavior.
    void merge(const RuntimeProfile* src);

    // Updates this profile w/ the thrift profile: behaves like Merge(), except
    // that existing counters are updated rather than added up.
    // Info strings matched up by key and are updated or added, depending on whether
    // the key has already been registered.
    void update(const TRuntimeProfileTree& thrift_profile);

    //Similar to `void update(const TRuntimeProfileTree& thrift_profile)`
    void update(const PRuntimeProfileTree& proto_profile);

    // Add a counter with 'name'/'type'.  Returns a counter object that the caller can
    // update.  The counter is owned by the RuntimeProfile object.
    // If parent_counter_name is a non-empty string, the counter is added as a child of
    // parent_counter_name.
    // If the counter already exists, the existing counter object is returned.
    Counter* add_counter(const std::string& name, TUnit::type type,
                         const std::string& parent_counter_name, int64_t level = 2);

    Counter* add_counter(const std::string& name, TUnit::type type) {
        return add_counter(name, type, RuntimeProfile::ROOT_COUNTER);
    }

    Counter* add_counter_with_level(const std::string& name, TUnit::type type, int64_t level) {
        return add_counter(name, type, RuntimeProfile::ROOT_COUNTER, level);
    }

    NonZeroCounter* add_nonzero_counter(
            const std::string& name, TUnit::type type,
            const std::string& parent_counter_name = RuntimeProfile::ROOT_COUNTER,
            int64_t level = 2);

    // Add a description entry under target counter.
    void add_description(const std::string& name, const std::string& description,
                         std::string parent_counter_name);
    // Add a derived counter with 'name'/'type'. The counter is owned by the
    // RuntimeProfile object.
    // If parent_counter_name is a non-empty string, the counter is added as a child of
    // parent_counter_name.
    // Returns nullptr if the counter already exists.
    DerivedCounter* add_derived_counter(const std::string& name, TUnit::type type,
                                        const DerivedCounterFunction& counter_fn,
                                        const std::string& parent_counter_name);

    ConditionCounter* add_conditition_counter(const std::string& name, TUnit::type type,
                                              const ConditionCounterFunction& counter_fn,
                                              const std::string& parent_counter_name,
                                              int64_t level = 2);

    // Gets the counter object with 'name'.  Returns nullptr if there is no counter with
    // that name.
    Counter* get_counter(const std::string& name);

    // Adds all counters with 'name' that are registered either in this or
    // in any of the child profiles to 'counters'.
    void get_counters(const std::string& name, std::vector<Counter*>* counters);

    // Helper to append to the "ExecOption" info string.
    void append_exec_option(const std::string& option) { add_info_string("ExecOption", option); }

    // Adds a string to the runtime profile.  If a value already exists for 'key',
    // the value will be updated.
    void add_info_string(const std::string& key, const std::string& value);

    // Returns a pointer to the info string value for 'key'.  Returns nullptr if
    // the key does not exist.
    const std::string* get_info_string(const std::string& key);

    // Returns the counter for the total elapsed time.
    Counter* total_time_counter() { return &_counter_total_time; }

    // Prints the counters in a name: value format.
    // Does not hold locks when it makes any function calls.
    void pretty_print(std::ostream* s, const std::string& prefix = "",
                      int64_t profile_level = 2) const;
    std::string pretty_print() const {
        std::stringstream ss;
        pretty_print(&ss);
        return ss.str();
    };

    // Serializes profile to thrift.
    // Does not hold locks when it makes any function calls.
    void to_thrift(TRuntimeProfileTree* tree, int64_t profile_level = 2);
    void to_thrift(std::vector<TRuntimeProfileNode>* nodes, int64_t profile_level = 2);

    // Similar to `to_thrift`.
    void to_proto(PRuntimeProfileTree* tree, int64_t profile_level = 2);
    void to_proto(google::protobuf::RepeatedPtrField<PRuntimeProfileNode>* nodes,
                  int64_t profile_level = 2);

    // Divides all counters by n
    void divide(int n);

    RuntimeProfile* get_child(std::string name);

    void get_children(std::vector<RuntimeProfile*>* children);

    // Gets all profiles in tree, including this one.
    void get_all_children(std::vector<RuntimeProfile*>* children);

    // Returns the number of counters in this profile
    int num_counters() const { return cast_set<int>(_counter_map.size()); }

    // Returns name of this profile
    const std::string& name() const { return _name; }

    // *only call this on top-level profiles*
    // (because it doesn't re-file child profiles)
    void set_name(const std::string& name) { _name = name; }

    int64_t metadata() const { return _metadata; }
    void set_metadata(int64_t md) {
        _is_set_metadata = true;
        _metadata = md;
    }

    bool is_set_metadata() const { return _is_set_metadata; }

    time_t timestamp() const { return _timestamp; }
    void set_timestamp(time_t ss) { _timestamp = ss; }

    // Derived counter function: return measured throughput as input_value/second.
    static int64_t units_per_second(const Counter* total_counter, const Counter* timer);

    // Derived counter function: return aggregated value
    static int64_t counter_sum(const std::vector<Counter*>* counters);

    /// Adds a high water mark counter to the runtime profile. Otherwise, same behavior
    /// as AddCounter().
    HighWaterMarkCounter* AddHighWaterMarkCounter(
            const std::string& name, TUnit::type unit,
            const std::string& parent_counter_name = RuntimeProfile::ROOT_COUNTER,
            int64_t profile_level = 2);

    // Recursively compute the fraction of the 'total_time' spent in this profile and
    // its children.
    // This function updates _local_time_percent for each profile.
    void compute_time_in_profile();

    void clear_children();

private:
    // RuntimeProfileCounterTreeNode needs to access the counter map and child counter map
    friend class RuntimeProfileCounterTreeNode;
    // Pool for allocated counters. Usually owned by the creator of this
    // object, but occasionally allocated in the constructor.
    std::unique_ptr<ObjectPool> _pool;

    // Pool for allocated counters. These counters are shared with some other objects.
    std::map<std::string, std::shared_ptr<HighWaterMarkCounter>> _shared_counter_pool;

    // Name for this runtime profile.
    std::string _name;

    // user-supplied, uninterpreted metadata.
    int64_t _metadata;
    bool _is_set_metadata = false;

    bool _is_sink = false;
    bool _is_set_sink = false;

    // The timestamp when the profile was modified, make sure the update is up to date.
    time_t _timestamp;

    /// True if this profile is an average derived from other profiles.
    /// All counters in this profile must be of unit AveragedCounter.
    bool _is_averaged_profile;

    // Map from counter names to counters.  The profile owns the memory for the
    // counters.
    using CounterMap = std::map<std::string, Counter*>;
    CounterMap _counter_map;

    // Map from parent counter name to a set of child counter name.
    // All top level counters are the child of RuntimeProfile::ROOT_COUNTER (root).
    using ChildCounterMap = std::map<std::string, std::set<std::string>>;
    ChildCounterMap _child_counter_map;

    // protects _counter_map, _counter_child_map and _bucketing_counters
    mutable std::mutex _counter_map_lock;

    // Child profiles.  Does not own memory.
    // We record children in both a map (to facilitate updates) and a vector
    // (to print things in the order they were registered)
    using ChildMap = std::map<std::string, RuntimeProfile*>;
    ChildMap _child_map;
    // vector of (profile, indentation flag)
    using ChildVector = std::vector<std::pair<RuntimeProfile*, bool>>;
    ChildVector _children;
    mutable std::mutex _children_lock; // protects _child_map and _children

    using InfoStrings = std::map<std::string, std::string>;
    InfoStrings _info_strings;

    // Keeps track of the order in which InfoStrings are displayed when printed
    using InfoStringsDisplayOrder = std::vector<std::string>;
    InfoStringsDisplayOrder _info_strings_display_order;

    // Protects _info_strings and _info_strings_display_order
    mutable std::mutex _info_strings_lock;

    Counter _counter_total_time;
    // Time spent in just in this profile (i.e. not the children) as a fraction
    // of the total time in the entire profile tree.
    double _local_time_percent;

    // update a subtree of profiles from nodes, rooted at *idx.
    // On return, *idx points to the node immediately following this subtree.
    void update(const std::vector<TRuntimeProfileNode>& nodes, int* idx);

    // Similar to `void update(const std::vector<TRuntimeProfileNode>& nodes, int* idx)`
    void update(const google::protobuf::RepeatedPtrField<PRuntimeProfileNode>& nodes, int* idx);

    // Helper function to compute compute the fraction of the total time spent in
    // this profile and its children.
    // Called recursively.
    void compute_time_in_profile(int64_t total_time);

    // Print the child counters of the given counter name
    static void print_child_counters(const std::string& prefix, const std::string& counter_name,
                                     const CounterMap& counter_map,
                                     const ChildCounterMap& child_counter_map, std::ostream* s);
};

// Utility class to update the counter at object construction and destruction.
// When the object is constructed, decrement the counter by val.
// When the object goes out of scope, increment the counter by val.
class ScopedCounter {
public:
    ScopedCounter(RuntimeProfile::Counter* counter, int64_t val) : _val(val), _counter(counter) {
        if (counter == nullptr) {
            return;
        }

        _counter->update(-1L * _val);
    }

    // Increment the counter when object is destroyed
    ~ScopedCounter() {
        if (_counter != nullptr) {
            _counter->update(_val);
        }
    }

    // Disable copy constructor and assignment
    ScopedCounter(const ScopedCounter& counter) = delete;
    ScopedCounter& operator=(const ScopedCounter& counter) = delete;

private:
    int64_t _val;
    RuntimeProfile::Counter* _counter = nullptr;
};

// Utility class to update time elapsed when the object goes out of scope.
// 'T' must implement the stopWatch "interface" (start,stop,elapsed_time) but
// we use templates not to pay for virtual function overhead.
template <class T, typename Bool = bool>
class ScopedTimer {
public:
    ScopedTimer(RuntimeProfile::Counter* counter, const Bool* is_cancelled = nullptr)
            : _counter(counter), _is_cancelled(is_cancelled) {
        if (counter == nullptr) {
            return;
        }
        DCHECK_EQ(counter->type(), TUnit::TIME_NS);
        _sw.start();
    }

    void stop() { _sw.stop(); }

    void start() { _sw.start(); }

    bool is_cancelled() { return _is_cancelled != nullptr && *_is_cancelled; }

    void UpdateCounter() {
        if (_counter != nullptr && !is_cancelled()) {
            _counter->update(_sw.elapsed_time());
        }
    }

    // Update counter when object is destroyed
    ~ScopedTimer() {
        if (_counter == nullptr) {
            return;
        }
        _sw.stop();
        UpdateCounter();
    }

    // Disable copy constructor and assignment
    ScopedTimer(const ScopedTimer& timer) = delete;
    ScopedTimer& operator=(const ScopedTimer& timer) = delete;

private:
    T _sw;
    RuntimeProfile::Counter* _counter = nullptr;
    const Bool* _is_cancelled = nullptr;
};

// Utility class to update time elapsed when the object goes out of scope.
// 'T' must implement the stopWatch "interface" (start,stop,elapsed_time) but
// we use templates not to pay for virtual function overhead.
template <class T, class C>
class ScopedRawTimer {
public:
    ScopedRawTimer(C* counter) : _counter(counter) { _sw.start(); }
    // Update counter when object is destroyed
    ~ScopedRawTimer() { *_counter += _sw.elapsed_time(); }

    // Disable copy constructor and assignment
    ScopedRawTimer(const ScopedRawTimer& timer) = delete;
    ScopedRawTimer& operator=(const ScopedRawTimer& timer) = delete;

private:
    T _sw;
    C* _counter = nullptr;
};
#include "common/compile_check_end.h"
} // namespace doris
