// Copyright 2012 Google Inc. All Rights Reserved.
//
// This file declares INTERNAL parts of the Split API that are inline/templated
// or otherwise need to be available at compile time. The main two abstractions
// defined in here are
//
//   - SplitIterator<>
//   - Splitter<>
//
// Everything else is plumbing for those two.
//
// DO NOT INCLUDE THIS FILE DIRECTLY. Use this file by including
// strings/split.h.
//
// IWYU pragma: private, include "strings/split.h"

#pragma once

#include <iterator>
using std::back_insert_iterator;
using std::iterator_traits;
#include <map>
using std::map;
using std::multimap;
#include <vector>
using std::vector;

#include "gutil/port.h" // for LANG_CXX11
#include "gutil/strings/stringpiece.h"

#ifdef LANG_CXX11
// This must be included after "base/port.h", which defines LANG_CXX11.
#include <initializer_list>
#endif // LANG_CXX11

namespace strings {

namespace internal {

// The default Predicate object, which doesn't filter out anything.
struct NoFilter {
    bool operator()(StringPiece /* ignored */) { return true; }
};

// This class splits a string using the given delimiter, returning the split
// substrings via an iterator interface. An optional Predicate functor may be
// supplied, which will be used to filter the split strings: strings for which
// the predicate returns false will be skipped. A Predicate object is any
// functor that takes a StringPiece and returns bool. By default, the NoFilter
// Predicate is used, which does not filter out anything.
//
// This class is NOT part of the public splitting API.
//
// Usage:
//
//   using strings::delimiter::Literal;
//   Literal d(",");
//   for (SplitIterator<Literal> it("a,b,c", d), end(d); it != end; ++it) {
//     StringPiece substring = *it;
//     DoWork(substring);
//   }
//
// The explicit single-argument constructor is used to create an "end" iterator.
// The two-argument constructor is used to split the given text using the given
// delimiter.
template <typename Delimiter, typename Predicate = NoFilter>
class SplitIterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = StringPiece;
    using difference_type = ptrdiff_t;
    using pointer = StringPiece*;
    using reference = StringPiece&;

    // Two constructors for "end" iterators.
    explicit SplitIterator(Delimiter d) : delimiter_(std::move(d)), predicate_(), is_end_(true) {}
    SplitIterator(Delimiter d, Predicate p)
            : delimiter_(std::move(d)), predicate_(std::move(p)), is_end_(true) {}
    // Two constructors taking the text to iterator.
    SplitIterator(StringPiece text, Delimiter d)
            : text_(std::move(text)), delimiter_(std::move(d)), predicate_(), is_end_(false) {
        ++(*this);
    }
    SplitIterator(StringPiece text, Delimiter d, Predicate p)
            : text_(std::move(text)),
              delimiter_(std::move(d)),
              predicate_(std::move(p)),
              is_end_(false) {
        ++(*this);
    }

    StringPiece operator*() { return curr_piece_; }
    StringPiece* operator->() { return &curr_piece_; }

    SplitIterator& operator++() {
        do {
            if (text_.end() == curr_piece_.end()) {
                // Already consumed all of text_, so we're done.
                is_end_ = true;
                return *this;
            }
            StringPiece found_delimiter = delimiter_.Find(text_);
            assert(found_delimiter.data() != NULL);
            assert(text_.begin() <= found_delimiter.begin());
            assert(found_delimiter.end() <= text_.end());
            // found_delimiter is allowed to be empty.
            // Sets curr_piece_ to all text up to but excluding the delimiter itself.
            // Sets text_ to remaining data after the delimiter.
            curr_piece_.set(text_.begin(), found_delimiter.begin() - text_.begin());
            text_.remove_prefix(found_delimiter.end() - text_.begin());
        } while (!predicate_(curr_piece_));
        return *this;
    }

    SplitIterator operator++(int /* postincrement */) {
        SplitIterator old(*this);
        ++(*this);
        return old;
    }

    bool operator==(const SplitIterator& other) const {
        // Two "end" iterators are always equal. If the two iterators being compared
        // aren't both end iterators, then we fallback to comparing their fields.
        // Importantly, the text being split must be equal and the current piece
        // within the text being split must also be equal. The delimiter_ and
        // predicate_ fields need not be checked here because they're template
        // parameters that are already part of the SplitIterator's type.
        return (is_end_ && other.is_end_) ||
               (is_end_ == other.is_end_ && text_ == other.text_ &&
                text_.data() == other.text_.data() && curr_piece_ == other.curr_piece_ &&
                curr_piece_.data() == other.curr_piece_.data());
    }

    bool operator!=(const SplitIterator& other) const { return !(*this == other); }

private:
    // The text being split. Modified as delimited pieces are consumed.
    StringPiece text_;
    Delimiter delimiter_;
    Predicate predicate_;
    bool is_end_;
    // Holds the currently split piece of text. Will always refer to string data
    // within text_. This value is returned when the iterator is dereferenced.
    StringPiece curr_piece_;
};

// Declares a functor that can convert a StringPiece to another type. This works
// for any type that has a constructor (explicit or not) taking a single
// StringPiece argument. A specialization exists for converting to string
// because the underlying data needs to be copied. In theory, these
// specializations could be extended to work with other types (e.g., int32), but
// then a solution for error reporting would need to be devised.
template <typename To>
struct StringPieceTo {
    To operator()(StringPiece from) const { return To(from); }
};

// Specialization for converting to string.
template <>
struct StringPieceTo<string> {
    string operator()(StringPiece from) const { return from.ToString(); }
};

// Specialization for converting to *const* string.
template <>
struct StringPieceTo<const string> {
    string operator()(StringPiece from) const { return from.ToString(); }
};

#ifdef LANG_CXX11
// IsNotInitializerList<T>::type exists iff T is not an initializer_list. More
// details below in Splitter<> where this is used.
template <typename T>
struct IsNotInitializerList {
    typedef void type;
};
template <typename T>
struct IsNotInitializerList<std::initializer_list<T>> {};
#endif // LANG_CXX11

// This class implements the behavior of the split API by giving callers access
// to the underlying split substrings in various convenient ways, such as
// through iterators or implicit conversion functions. Do not construct this
// class directly, rather use the Split() function instead.
//
// Output containers can be collections of either StringPiece or string objects.
// StringPiece is more efficient because the underlying data will not need to be
// copied; the returned StringPieces will all refer to the data within the
// original input string. If a collection of string objects is used, then each
// substring will be copied.
//
// An optional Predicate functor may be supplied. This predicate will be used to
// filter the split strings: only strings for which the predicate returns true
// will be kept. A Predicate object is any unary functor that takes a
// StringPiece and returns bool. By default, the NoFilter predicate is used,
// which does not filter out anything.
template <typename Delimiter, typename Predicate = NoFilter>
class Splitter {
public:
    typedef internal::SplitIterator<Delimiter, Predicate> Iterator;

    Splitter(StringPiece text, Delimiter d) : begin_(text, d), end_(d) {}

    Splitter(StringPiece text, Delimiter d, Predicate p) : begin_(text, d, p), end_(d, p) {}

    // Range functions that iterate the split substrings as StringPiece objects.
    // These methods enable a Splitter to be used in a range-based for loop in
    // C++11, for example:
    //
    //   for (StringPiece sp : my_splitter) {
    //     DoWork(sp);
    //   }
    const Iterator& begin() const { return begin_; }
    const Iterator& end() const { return end_; }

#ifdef LANG_CXX11
// Support for default template arguments for function templates was added in
// C++11, but it is not allowed if compiled in C++98 compatibility mode. Since
// this code is under a LANG_CXX11 guard, we can safely ignore the
// -Wc++98-compat flag and use default template arguments on the implicit
// conversion operator below.
//
// This use of default template arguments on a function template was approved
// by tgs and sanjay on behalf of the c-style-arbiters in email thread
//
// All compiler flags are first saved with a diagnostic push and restored with a
// diagnostic pop below.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic ignored "-Wc++98-compat"

    // Uses SFINAE to restrict conversion to container-like types (by testing for
    // the presence of a const_iterator member type) and also to disable
    // conversion to an initializer_list (which also has a const_iterator).
    // Otherwise, code compiled in C++11 will get an error due to ambiguous
    // conversion paths (in C++11 vector<T>::operator= is overloaded to take
    // either a vector<T> or an initializer_list<T>).
    //
    // This trick was taken from util/gtl/container_literal.h
    template <typename Container,
              typename IsNotInitializerListChecker = typename IsNotInitializerList<Container>::type,
              typename ContainerChecker = typename Container::const_iterator>
    operator Container() {
        return SelectContainer<Container, is_map<Container>::value>()(this);
    }

// Restores diagnostic settings, i.e., removes the "ignore" on -Wpragmas and
// -Wc++98-compat.
#pragma GCC diagnostic pop

#else
    // Not under LANG_CXX11
    template <typename Container>
    operator Container() {
        return SelectContainer<Container, is_map<Container>::value>()(this);
    }
#endif // LANG_CXX11

    template <typename First, typename Second>
    operator std::pair<First, Second>() {
        return ToPair<First, Second>();
    }

private:
    // is_map<T>::value is true iff there exists a type T::mapped_type. This is
    // used to dispatch to one of the SelectContainer<> functors (below) from the
    // implicit conversion operator (above).
    template <typename T>
    struct is_map {
        template <typename U>
        static base::big_ test(typename U::mapped_type*);
        template <typename>
        static base::small_ test(...);
        static const bool value = (sizeof(test<T>(0)) == sizeof(base::big_));
    };

    // Base template handles splitting to non-map containers
    template <typename Container, bool>
    struct SelectContainer {
        Container operator()(Splitter* splitter) const {
            return splitter->template ToContainer<Container>();
        }
    };

    // Partial template specialization for splitting to map-like containers.
    template <typename Container>
    struct SelectContainer<Container, true> {
        Container operator()(Splitter* splitter) const {
            return splitter->template ToMap<Container>();
        }
    };

    // Inserts split results into the container. To do this the results are first
    // stored in a vector<StringPiece>. This is where the input text is actually
    // "parsed". This vector is then used to possibly reserve space in the output
    // container, and the StringPieces in "v" are converted as necessary to the
    // output container's value type.
    //
    // The reason to use an intermediate vector of StringPiece is so we can learn
    // the needed capacity of the output container. This is needed when the output
    // container is a vector<string> in which case resizes can be expensive due to
    // copying of the ::string objects.
    //
    // At some point in the future we might add a C++11 move constructor to
    // ::string, in which case the vector resizes are much less expensive and the
    // use of this intermediate vector "v" can be removed.
    template <typename Container>
    Container ToContainer() {
        vector<StringPiece> v;
        for (Iterator it = begin(); it != end_; ++it) {
            v.push_back(*it);
        }
        typedef typename Container::value_type ToType;
        internal::StringPieceTo<ToType> converter;
        Container c;
        ReserveCapacity(&c, v.size());
        std::insert_iterator<Container> inserter(c, c.begin());
        for (const auto& sp : v) {
            *inserter++ = converter(sp);
        }
        return c;
    }

    // The algorithm is to insert a new pair into the map for each even-numbered
    // item, with the even-numbered item as the key with a default-constructed
    // value. Each odd-numbered item will then be assigned to the last pair's
    // value.
    template <typename Map>
    Map ToMap() {
        typedef typename Map::key_type Key;
        typedef typename Map::mapped_type Data;
        Map m;
        StringPieceTo<Key> key_converter;
        StringPieceTo<Data> val_converter;
        typename Map::iterator curr_pair;
        bool is_even = true;
        for (Iterator it = begin(); it != end_; ++it) {
            if (is_even) {
                curr_pair = InsertInMap(std::make_pair(key_converter(*it), Data()), &m);
            } else {
                curr_pair->second = val_converter(*it);
            }
            is_even = !is_even;
        }
        return m;
    }

    // Returns a pair with its .first and .second members set to the first two
    // strings returned by the begin() iterator. Either/both of .first and .second
    // will be empty strings if the iterator doesn't have a corresponding value.
    template <typename First, typename Second>
    std::pair<First, Second> ToPair() {
        StringPieceTo<First> first_converter;
        StringPieceTo<Second> second_converter;
        StringPiece first, second;
        Iterator it = begin();
        if (it != end()) {
            first = *it;
            if (++it != end()) {
                second = *it;
            }
        }
        return std::make_pair(first_converter(first), second_converter(second));
    }

    // Overloaded InsertInMap() function. The first overload is the commonly used
    // one for most map-like objects. The second overload is a special case for
    // multimap, because multimap's insert() member function directly returns an
    // iterator, rather than a pair<iterator, bool> like map's.
    template <typename Map>
    typename Map::iterator InsertInMap(const typename Map::value_type& value, Map* map) {
        return map->insert(value).first;
    }

    // InsertInMap overload for multimap.
    template <typename K, typename T, typename C, typename A>
    typename std::multimap<K, T, C, A>::iterator InsertInMap(
            const typename std::multimap<K, T, C, A>::value_type& value,
            typename std::multimap<K, T, C, A>* map) {
        return map->insert(value);
    }

    // Reserves the given amount of capacity in a vector<string>
    template <typename A>
    void ReserveCapacity(vector<string, A>* v, size_t size) {
        v->reserve(size);
    }
    void ReserveCapacity(...) {}

    const Iterator begin_;
    const Iterator end_;
};

} // namespace internal

} // namespace strings
