// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <memory>
#include <type_traits>
#include <vector>

#include <boost/dynamic_bitset.hpp>

namespace diskann
{
// A map whose key is a natural number (from 0 onwards) and maps to a value.
// Made as both memory and performance efficient map for scenario such as
// DiskANN location-to-tag map. There, the pool of numbers is consecutive from
// zero to some max value, and it's expected that most if not all keys from 0
// up to some current maximum will be present in the map. The memory usage of
// the map is determined by the largest inserted key since it uses vector as a
// backing store and bitset for presence indication.
//
// Thread-safety: this class is not thread-safe in general.
// Exception: multiple read-only operations are safe on the object only if
// there are no writers to it in parallel.
template <typename Key, typename Value> class natural_number_map
{
  public:
    static_assert(std::is_trivial<Key>::value, "Key must be a trivial type");

    // Represents a reference to a element in the map. Used while iterating
    // over map entries.
    struct position
    {
        size_t _key;
        // The number of keys that were enumerated when iterating through the
        // map so far. Used to early-terminate enumeration when ithere are no
        // more entries in the map.
        size_t _keys_already_enumerated;

        // Returns whether it's valid to access the element at this position in
        // the map.
        bool is_valid() const;
    };

    natural_number_map();

    void reserve(size_t count);
    size_t size() const;

    void set(Key key, Value value);
    void erase(Key key);

    bool contains(Key key) const;
    bool try_get(Key key, Value &value) const;

    // Returns the value at the specified position. Prerequisite: position is
    // valid.
    Value get(const position &pos) const;

    // Finds the first element in the map, if any. Invalidated by changes in the
    // map.
    position find_first() const;

    // Finds the next element in the map after the specified position.
    // Invalidated by changes in the map.
    position find_next(const position &after_position) const;

    void clear();

  private:
    // Number of entries in the map. Not the same as size() of the
    // _values_vector below.
    size_t _size;

    // Array of values. The key is the index of the value.
    std::vector<Value> _values_vector;

    // Values that are in the set have the corresponding bit index set
    // to 1.
    //
    // Use a pointer here to allow for forward declaration of dynamic_bitset
    // in public headers to avoid making boost a dependency for clients
    // of DiskANN.
    std::unique_ptr<boost::dynamic_bitset<>> _values_bitset;
};
} // namespace diskann
