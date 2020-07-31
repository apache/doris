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

#ifndef DORIS_BE_SRC_COMMON_UTIL_RUNTIME_PROFILE_H
#define DORIS_BE_SRC_COMMON_UTIL_RUNTIME_PROFILE_H

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <iostream>
#include <sys/time.h>
#include <sys/resource.h>

#include "common/logging.h"
#include "common/object_pool.h"
#include "util/stopwatch.hpp"
#include "gen_cpp/RuntimeProfile_types.h"

namespace doris {

// Define macros for updating counters.  The macros make it very easy to disable
// all counters at compile time.  Set this to 0 to remove counters.  This is useful
// to do to make sure the counters aren't affecting the system.
#define ENABLE_COUNTERS 1

// Some macro magic to generate unique ids using __COUNTER__
#define CONCAT_IMPL(x, y) x##y
#define MACRO_CONCAT(x, y) CONCAT_IMPL(x, y)

#if ENABLE_COUNTERS
#define ADD_COUNTER(profile, name, type) (profile)->add_counter(name, type)
#define ADD_TIMER(profile, name) (profile)->add_counter(name, TUnit::TIME_NS)
#define ADD_CHILD_TIMER(profile, name, parent) \
      (profile)->add_counter(name, TUnit::TIME_NS, parent)
#define SCOPED_TIMER(c) \
      ScopedTimer<MonotonicStopWatch> MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c)
#define CANCEL_SAFE_SCOPED_TIMER(c, is_cancelled) \
      ScopedTimer<MonotonicStopWatch> MACRO_CONCAT(SCOPED_TIMER, __COUNTER__)(c, is_cancelled)
#define SCOPED_RAW_TIMER(c) \
      ScopedRawTimer<MonotonicStopWatch> MACRO_CONCAT(SCOPED_RAW_TIMER, __COUNTER__)(c)
#define COUNTER_UPDATE(c, v) (c)->update(v)
#define COUNTER_SET(c, v) (c)->set(v)
#define ADD_THREAD_COUNTERS(profile, prefix) (profile)->add_thread_counters(prefix)
#define SCOPED_THREAD_COUNTER_MEASUREMENT(c) \
    /*ThreadCounterMeasurement                                        \
      MACRO_CONCAT(SCOPED_THREAD_COUNTER_MEASUREMENT, __COUNTER__)(c)*/
#else
#define ADD_COUNTER(profile, name, type) NULL
#define ADD_TIMER(profile, name) NULL
#define SCOPED_TIMER(c)
#define SCOPED_RAW_TIMER(c)
#define COUNTER_UPDATE(c, v)
#define COUNTER_SET(c, v)
#define ADD_THREADCOUNTERS(profile, prefix) NULL
#define SCOPED_THREAD_COUNTER_MEASUREMENT(c)
#endif

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
    class Counter {
    public:
        Counter(TUnit::type type, int64_t value = 0) :
                _value(value),
                _type(type) {
        }
        virtual ~Counter() { }

        virtual void update(int64_t delta) {
            //__sync_fetch_and_add(&_value, delta);
            _value.add(delta);
        }

        // Use this to update if the counter is a bitmap
        void bit_or(int64_t delta) {
            int64_t old;
            do {
                old = _value.load();
                if (LIKELY((old | delta) == old)) return; // Bits already set, avoid atomic.
            } while (UNLIKELY(!_value.compare_and_swap(old, old | delta)));
        }

        virtual void set(int64_t value) {
            _value.store(value);
        }

        virtual void set(double value) {
            DCHECK_EQ(sizeof(value), sizeof(int64_t));
            _value.store(*reinterpret_cast<int64_t*>(&value));
        }

        virtual int64_t value() const {
            return _value.load();
        }

        virtual double double_value() const {
            int64_t v = _value.load();
            return *reinterpret_cast<const double*>(&v);
        }

        TUnit::type type() const {
            return _type;
        }

    private:
        friend class RuntimeProfile;

        AtomicInt64 _value;
        TUnit::type _type;
    };

    class AveragedCounter;
    class ConcurrentTimerCounter;
    class DerivedCounter;
    class EventSequence;
    class HighWaterMarkCounter;
    class SummaryStatsCounter;
    class ThreadCounters;
    class TimeSeriesCounter;

    /// A counter that keeps track of the highest value seen (reporting that
    /// as value()) and the current value.
    class HighWaterMarkCounter : public Counter {
    public:
        HighWaterMarkCounter(TUnit::type unit) : Counter(unit) {}

        virtual void add(int64_t delta) {
            int64_t new_val = current_value_.add(delta);
            UpdateMax(new_val);
        }

        /// Tries to increase the current value by delta. If current_value() + delta
        /// exceeds max, return false and current_value is not changed.
        bool try_add(int64_t delta, int64_t max) {
            while (true) {
                int64_t old_val = current_value_.load();
                int64_t new_val = old_val + delta;
                if (UNLIKELY(new_val > max)) return false;
                if (LIKELY(current_value_.compare_and_swap(old_val, new_val))) {
                    UpdateMax(new_val);
                    return true;
                }
            }
        }

        virtual void set(int64_t v) {
            current_value_.store(v);
            UpdateMax(v);
        }

        int64_t current_value() const { return current_value_.load(); }

    private:
        /// Set '_value' to 'v' if 'v' is larger than '_value'. The entire operation is
        /// atomic.
        void UpdateMax(int64_t v) {
            while (true) {
                int64_t old_max = _value.load();
                int64_t new_max = std::max(old_max, v);
                if (new_max == old_max) break; // Avoid atomic update.
                if (LIKELY(_value.compare_and_swap(old_max, new_max))) break;
            }
        }

        /// The current value of the counter. _value in the super class represents
        /// the high water mark.
        AtomicInt64 current_value_;
    };

    typedef boost::function<int64_t ()> DerivedCounterFunction;

    // A DerivedCounter also has a name and type, but the value is computed.
    // Do not call Set() and Update().
    class DerivedCounter : public Counter {
    public:
        DerivedCounter(TUnit::type type, const DerivedCounterFunction& counter_fn) :
                Counter(type, 0),
                _counter_fn(counter_fn) {}

        virtual int64_t value() const {
            return _counter_fn();
        }

    private:
        DerivedCounterFunction _counter_fn;
    };

    // A set of counters that measure thread info, such as total time, user time, sys time.
    class ThreadCounters {
    private:
        friend class ThreadCounterMeasurement;
        friend class RuntimeProfile;

        Counter* _total_time; // total wall clock time
        Counter* _user_time;  // user CPU time
        Counter* _sys_time;   // system CPU time

        // The number of times a context switch resulted due to a process voluntarily giving
        // up the processor before its time slice was completed.
        Counter* _voluntary_context_switches;

        // The number of times a context switch resulted due to a higher priority process
        // becoming runnable or because the current process exceeded its time slice.
        Counter* _involuntary_context_switches;
    };

    // An EventSequence captures a sequence of events (each added by
    // calling MarkEvent). Each event has a text label, and a time
    // (measured relative to the moment start() was called as t=0). It is
    // useful for tracking the evolution of some serial process, such as
    // the query lifecycle.
    // Not thread-safe.
    class EventSequence {
    public:
        EventSequence() { }

        // starts the timer without resetting it.
        void start() {
            _sw.start();
        }

        // stops (or effectively pauses) the timer.
        void stop() {
            _sw.stop();
        }

        // Stores an event in sequence with the given label and the
        // current time (relative to the first time start() was called) as
        // the timestamp.
        void mark_event(const std::string& label) {
            _events.push_back(make_pair(label, _sw.elapsed_time()));
        }

        int64_t elapsed_time() {
            return _sw.elapsed_time();
        }

        // An Event is a <label, timestamp> pair
        typedef std::pair<std::string, int64_t> Event;

        // An EventList is a sequence of Events, in increasing timestamp order
        typedef std::vector<Event> EventList;

        const EventList& events() const {
            return _events;
        }

    private:
        // Stored in increasing time order
        EventList _events;

        // Timer which allows events to be timestamped when they are recorded.
        MonotonicStopWatch _sw;
    };

    // Create a runtime profile object with 'name'.
    RuntimeProfile(const std::string& name, bool is_averaged_profile = false);

    ~RuntimeProfile();

    // Adds a child profile.  This is thread safe.
    // 'indent' indicates whether the child will be printed w/ extra indentation
    // relative to the parent.
    // If location is non-null, child will be inserted after location.  Location must
    // already be added to the profile.
    void add_child(RuntimeProfile* child, bool indent, RuntimeProfile* location);

    void add_child_unlock(RuntimeProfile* child, bool indent, RuntimeProfile* loc);
    
    /// Creates a new child profile with the given 'name'. A child profile with that name
    /// must not already exist. If 'prepend' is true, prepended before other child profiles,
    /// otherwise appended after other child profiles.
    RuntimeProfile* create_child(
        const std::string& name, bool indent = true, bool prepend = false);

    // Sorts all children according to a custom comparator. Does not
    // invalidate pointers to profiles.
    template <class Compare>
    void sort_childer(const Compare& cmp) {
        boost::lock_guard<boost::mutex> l(_children_lock);
        std::sort(_children.begin(), _children.end(), cmp);
    }

    // Merges the src profile into this one, combining counters that have an identical
    // path. Info strings from profiles are not merged. 'src' would be a const if it
    // weren't for locking.
    // Calling this concurrently on two RuntimeProfiles in reverse order results in
    // undefined behavior.
    void merge(RuntimeProfile* src);

    // Updates this profile w/ the thrift profile: behaves like Merge(), except
    // that existing counters are updated rather than added up.
    // Info strings matched up by key and are updated or added, depending on whether
    // the key has already been registered.
    void update(const TRuntimeProfileTree& thrift_profile);

    // Add a counter with 'name'/'type'.  Returns a counter object that the caller can
    // update.  The counter is owned by the RuntimeProfile object.
    // If parent_counter_name is a non-empty string, the counter is added as a child of
    // parent_counter_name.
    // If the counter already exists, the existing counter object is returned.
    Counter* add_counter(const std::string& name, TUnit::type type,
                        const std::string& parent_counter_name);
    Counter* add_counter(const std::string& name, TUnit::type type) {
        return add_counter(name, type, "");
    }

    // Add a derived counter with 'name'/'type'. The counter is owned by the
    // RuntimeProfile object.
    // If parent_counter_name is a non-empty string, the counter is added as a child of
    // parent_counter_name.
    // Returns NULL if the counter already exists.
    DerivedCounter* add_derived_counter(const std::string& name, TUnit::type type,
                                      const DerivedCounterFunction& counter_fn,
                                      const std::string& parent_counter_name);

    // Add a set of thread counters prefixed with 'prefix'. Returns a ThreadCounters object
    // that the caller can update.  The counter is owned by the RuntimeProfile object.
    ThreadCounters* add_thread_counters(const std::string& prefix);

    // Gets the counter object with 'name'.  Returns NULL if there is no counter with
    // that name.
    Counter* get_counter(const std::string& name);

    // Adds all counters with 'name' that are registered either in this or
    // in any of the child profiles to 'counters'.
    void get_counters(const std::string& name, std::vector<Counter*>* counters);

    // Helper to append to the "ExecOption" info string.
    void append_exec_option(const std::string& option) {
        add_info_string("ExecOption", option);
    }

    // Adds a string to the runtime profile.  If a value already exists for 'key',
    // the value will be updated.
    void add_info_string(const std::string& key, const std::string& value);

    // Creates and returns a new EventSequence (owned by the runtime
    // profile) - unless a timer with the same 'key' already exists, in
    // which case it is returned.
    // TODO: EventSequences are not merged by Merge()
    EventSequence* add_event_sequence(const std::string& key);

    // Returns a pointer to the info string value for 'key'.  Returns NULL if
    // the key does not exist.
    const std::string* get_info_string(const std::string& key);

    // Returns the counter for the total elapsed time.
    Counter* total_time_counter() {
        return &_counter_total_time;
    }

    // Prints the counters in a name: value format.
    // Does not hold locks when it makes any function calls.
    void pretty_print(std::ostream* s, const std::string& prefix = "") const;

    // Serializes profile to thrift.
    // Does not hold locks when it makes any function calls.
    void to_thrift(TRuntimeProfileTree* tree);
    void to_thrift(std::vector<TRuntimeProfileNode>* nodes);

    // Divides all counters by n
    void divide(int n);

    void get_children(std::vector<RuntimeProfile*>* children);

    // Gets all profiles in tree, including this one.
    void get_all_children(std::vector<RuntimeProfile*>* children);

    // Returns the number of counters in this profile
    int num_counters() const {
        return _counter_map.size();
    }

    // Returns name of this profile
    const std::string& name() const {
        return _name;
    }

    // *only call this on top-level profiles*
    // (because it doesn't re-file child profiles)
    void set_name(const std::string& name) {
        _name = name;
    }

    int64_t metadata() const {
        return _metadata;
    }
    void set_metadata(int64_t md) {
        _metadata = md;
    }

    // Derived counter function: return measured throughput as input_value/second.
    static int64_t units_per_second(
        const Counter* total_counter, const Counter* timer);

    // Derived counter function: return aggregated value
    static int64_t counter_sum(const std::vector<Counter*>* counters);

    // Function that returns a counter metric.
    // Note: this function should not block (or take a long time).
    typedef boost::function<int64_t ()> SampleFn;

    // Add a rate counter to the current profile based on src_counter with name.
    // The rate counter is updated periodically based on the src counter.
    // The rate counter has units in src_counter unit per second.
    Counter* add_rate_counter(const std::string& name, Counter* src_counter);

    // Same as 'add_rate_counter' above except values are taken by calling fn.
    // The resulting counter will be of 'type'.
    Counter* add_rate_counter(const std::string& name, SampleFn fn, TUnit::type type);

    // Add a sampling counter to the current profile based on src_counter with name.
    // The sampling counter is updated periodically based on the src counter by averaging
    // the samples taken from the src counter.
    // The sampling counter has the same unit as src_counter unit.
    Counter* add_sampling_counter(const std::string& name, Counter* src_counter);

    // Same as 'add_sampling_counter' above except the samples are taken by calling fn.
    Counter* add_sampling_counter(const std::string& name, SampleFn fn);

    // Add a bucket of counters to store the sampled value of src_counter.
    // The src_counter is sampled periodically and the buckets are updated.
    void add_bucketing_counters(const std::string& name,
                              const std::string& parent_counter_name, Counter* src_counter,
                              int max_buckets, std::vector<Counter*>* buckets);

    /// Adds a high water mark counter to the runtime profile. Otherwise, same behavior
    /// as AddCounter().
    HighWaterMarkCounter* AddHighWaterMarkCounter(const std::string& name,
            TUnit::type unit, const std::string& parent_counter_name = "");

    // Only for create MemTracker(using profile's counter to calc consumption)
    std::shared_ptr<HighWaterMarkCounter> AddSharedHighWaterMarkCounter(
        const std::string& name, TUnit::type unit,
        const std::string& parent_counter_name = "");

    // stops updating the value of 'rate_counter'. Rate counters are updated
    // periodically so should be removed as soon as the underlying counter is
    // no longer going to change.
    void stop_rate_counters_updates(Counter* rate_counter);

    // stops updating the value of 'sampling_counter'. Sampling counters are updated
    // periodically so should be removed as soon as the underlying counter is
    // no longer going to change.
    void stop_sampling_counters_updates(Counter* sampling_counter);

    // stops updating the bucket counter.
    // If convert is true, convert the buckets from count to percentage.
    // Sampling counters are updated periodically so should be removed as soon as the
    // underlying counter is no longer going to change.
    void stop_bucketing_counters_updates(std::vector<Counter*>* buckets, bool convert);

    // Recursively compute the fraction of the 'total_time' spent in this profile and
    // its children.
    // This function updates _local_time_percent for each profile.
    void compute_time_in_profile();

private:
    // Pool for allocated counters. Usually owned by the creator of this
    // object, but occasionally allocated in the constructor.
    std::unique_ptr<ObjectPool> _pool;

    // Pool for allocated counters. These counters are shared with some other objects.
    std::map<std::string, std::shared_ptr<HighWaterMarkCounter>> _shared_counter_pool;

    // True if we have to delete the _pool on destruction.
    bool _own_pool;

    // Name for this runtime profile.
    std::string _name;

    // user-supplied, uninterpreted metadata.
    int64_t _metadata;

    /// True if this profile is an average derived from other profiles.
    /// All counters in this profile must be of unit AveragedCounter.
    bool _is_averaged_profile;

    // Map from counter names to counters.  The profile owns the memory for the
    // counters.
    typedef std::map<std::string, Counter*> CounterMap;
    CounterMap _counter_map;

    // Map from parent counter name to a set of child counter name.
    // All top level counters are the child of "" (root).
    typedef std::map<std::string, std::set<std::string> > ChildCounterMap;
    ChildCounterMap _child_counter_map;

    // A set of bucket counters registered in this runtime profile.
    std::set<std::vector<Counter*>* > _bucketing_counters;

    // protects _counter_map, _counter_child_map and _bucketing_counters
    mutable boost::mutex _counter_map_lock;

    // Child profiles.  Does not own memory.
    // We record children in both a map (to facilitate updates) and a vector
    // (to print things in the order they were registered)
    typedef std::map<std::string, RuntimeProfile*> ChildMap;
    ChildMap _child_map;
    // vector of (profile, indentation flag)
    typedef std::vector<std::pair<RuntimeProfile*, bool> > ChildVector;
    ChildVector _children;
    mutable boost::mutex _children_lock;  // protects _child_map and _children

    typedef std::map<std::string, std::string> InfoStrings;
    InfoStrings _info_strings;

    // Keeps track of the order in which InfoStrings are displayed when printed
    typedef std::vector<std::string> InfoStringsDisplayOrder;
    InfoStringsDisplayOrder _info_strings_display_order;

    // Protects _info_strings and _info_strings_display_order
    mutable boost::mutex _info_strings_lock;

    typedef std::map<std::string, EventSequence*> EventSequenceMap;
    EventSequenceMap _event_sequence_map;
    mutable boost::mutex _event_sequences_lock;

    Counter _counter_total_time;
    // Time spent in just in this profile (i.e. not the children) as a fraction
    // of the total time in the entire profile tree.
    double _local_time_percent;

    enum PeriodicCounterType {
        RATE_COUNTER = 0,
        SAMPLING_COUNTER,
    };

    struct RateCounterInfo {
        Counter* src_counter;
        SampleFn sample_fn;
        int64_t elapsed_ms;
    };

    struct SamplingCounterInfo {
        Counter* src_counter; // the counter to be sampled
        SampleFn sample_fn;
        int64_t total_sampled_value; // sum of all sampled values;
        int64_t num_sampled; // number of samples taken
    };

    struct BucketCountersInfo {
        Counter* src_counter; // the counter to be sampled
        int64_t num_sampled; // number of samples taken
        // TODO: customize bucketing
    };

    // This is a static singleton object that is used to update all rate counters and
    // sampling counters.
    struct PeriodicCounterUpdateState {
        PeriodicCounterUpdateState();

        // Tears down the update thread.
        ~PeriodicCounterUpdateState();

        // Lock protecting state below
        boost::mutex lock;

        // If true, tear down the update thread.
        volatile bool _done;

        // Thread performing asynchronous updates.
        boost::scoped_ptr<boost::thread> update_thread;

        // A map of the dst (rate) counter to the src counter and elapsed time.
        typedef std::map<Counter*, RateCounterInfo> RateCounterMap;
        RateCounterMap rate_counters;

        // A map of the dst (averages over samples) counter to the src counter (to be sampled)
        // and number of samples taken.
        typedef std::map<Counter*, SamplingCounterInfo> SamplingCounterMap;
        SamplingCounterMap sampling_counters;

        // Map from a bucket of counters to the src counter
        typedef std::map<std::vector<Counter*>*, BucketCountersInfo> BucketCountersMap;
        BucketCountersMap bucketing_counters;
    };

    // Singleton object that keeps track of all rate counters and the thread
    // for updating them.
    static PeriodicCounterUpdateState _s_periodic_counter_update_state;

    // update a subtree of profiles from nodes, rooted at *idx.
    // On return, *idx points to the node immediately following this subtree.
    void update(const std::vector<TRuntimeProfileNode>& nodes, int* idx);

    // Helper function to compute compute the fraction of the total time spent in
    // this profile and its children.
    // Called recusively.
    void compute_time_in_profile(int64_t total_time);

    // Registers a periodic counter to be updated by the update thread.
    // Either sample_fn or dst_counter must be non-NULL.  When the periodic counter
    // is updated, it either gets the value from the dst_counter or calls the sample
    // function to get the value.
    // dst_counter/sample fn is assumed to be compatible types with src_counter.
    static void register_periodic_counter(Counter* src_counter, SampleFn sample_fn,
                                        Counter* dst_counter, PeriodicCounterType type);

    // Loop for periodic counter update thread.  This thread wakes up once in a while
    // and updates all the added rate counters and sampling counters.
    static void periodic_counter_update_loop();

    // Print the child counters of the given counter name
    static void print_child_counters(const std::string& prefix,
                                   const std::string& counter_name, const CounterMap& counter_map,
                                   const ChildCounterMap& child_counter_map, std::ostream* s);
};

// Utility class to update the counter at object construction and destruction.
// When the object is constructed, decrement the counter by val.
// When the object goes out of scope, increment the counter by val.
class ScopedCounter {
public:
    ScopedCounter(RuntimeProfile::Counter* counter, int64_t val) :
        _val(val),
        _counter(counter) {
        if (counter == NULL) {
            return;
        }

        _counter->update(-1L * _val);
    }

    // Increment the counter when object is destroyed
    ~ScopedCounter() {
        if (_counter != NULL) {
            _counter->update(_val);
        }
    }

private:
    // Disable copy constructor and assignment
    ScopedCounter(const ScopedCounter& counter);
    ScopedCounter& operator=(const ScopedCounter& counter);

    int64_t _val;
    RuntimeProfile::Counter* _counter;
};

// Utility class to update time elapsed when the object goes out of scope.
// 'T' must implement the stopWatch "interface" (start,stop,elapsed_time) but
// we use templates not to pay for virtual function overhead.
template<class T>
class ScopedTimer {
public:
    ScopedTimer(RuntimeProfile::Counter* counter, const bool* is_cancelled = nullptr) :
        _counter(counter), _is_cancelled(is_cancelled) {
        if (counter == NULL) {
            return;
        }
        DCHECK(counter->type() == TUnit::TIME_NS);
        _sw.start();
    }

    void stop() {
        _sw.stop();
    }

    void start() {
        _sw.start();
    }

    bool is_cancelled() {
        return _is_cancelled != nullptr && *_is_cancelled;
    }

    void UpdateCounter() {
        if (_counter != NULL && !is_cancelled()) {
            _counter->update(_sw.elapsed_time());
        }
    }

    // Update counter when object is destroyed
    ~ScopedTimer() {
        _sw.stop();
        UpdateCounter();
    }

private:
    // Disable copy constructor and assignment
    ScopedTimer(const ScopedTimer& timer);
    ScopedTimer& operator=(const ScopedTimer& timer);

    T _sw;
    RuntimeProfile::Counter* _counter;
    const bool* _is_cancelled;
};

// Utility class to update time elapsed when the object goes out of scope.
// 'T' must implement the stopWatch "interface" (start,stop,elapsed_time) but
// we use templates not to pay for virtual function overhead.
template<class T>
class ScopedRawTimer {
public:
    ScopedRawTimer(int64_t* counter) : _counter(counter) {
        _sw.start();
    }
    // Update counter when object is destroyed
    ~ScopedRawTimer() {
        *_counter += _sw.elapsed_time();
    }
private:
    // Disable copy constructor and assignment
    ScopedRawTimer(const ScopedRawTimer& timer);
    ScopedRawTimer& operator=(const ScopedRawTimer& timer);

    T _sw;
    int64_t* _counter;
};

}

#endif
