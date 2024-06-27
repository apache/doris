// Copyright 2001, Google Inc.  All rights reserved.
// Maintainer: mec@google.com (Michael Chastain)
//
// A StringPiece points to part or all of a string, Cord, double-quoted string
// literal, or other string-like object.  A StringPiece does *not* own the
// string to which it points.  A StringPiece is not null-terminated.
//
// You can use StringPiece as a function or method parameter.  A StringPiece
// parameter can receive a double-quoted string literal argument, a "const
// char*" argument, a string argument, or a StringPiece argument with no data
// copying.  Systematic use of StringPiece for arguments reduces data
// copies and strlen() calls.
//
// You may pass a StringPiece argument by value or const reference.
// Passing by value generates slightly smaller code.
//   void MyFunction(const StringPiece& arg);
//   // Slightly better, but same lifetime requirements as const-ref parameter:
//   void MyFunction(StringPiece arg);
//
// StringPiece is also suitable for local variables if you know that
// the lifetime of the underlying object is longer than the lifetime
// of your StringPiece variable.
//
// Beware of binding a StringPiece to a temporary:
//   StringPiece sp = obj.MethodReturningString();  // BAD: lifetime problem
//
// This code is okay:
//   string str = obj.MethodReturningString();  // str owns its contents
//   StringPiece sp(str);  // GOOD, although you may not need sp at all
//
// StringPiece is sometimes a poor choice for a return value and usually a poor
// choice for a data member.  If you do use a StringPiece this way, it is your
// responsibility to ensure that the object pointed to by the StringPiece
// outlives the StringPiece.
//
// A StringPiece may represent just part of a string; thus the name "Piece".
// For example, when splitting a string, vector<StringPiece> is a natural data
// type for the output.  For another example, a Cord is a non-contiguous,
// potentially very long string-like object.  The Cord class has an interface
// that iteratively provides StringPiece objects that point to the
// successive pieces of a Cord object.
//
// A StringPiece is not null-terminated.  If you write code that scans a
// StringPiece, you must check its length before reading any characters.
// Common idioms that work on null-terminated strings do not work on
// StringPiece objects.
//
// There are several ways to create a null StringPiece:
//   StringPiece()
//   StringPiece(NULL)
//   StringPiece(NULL, 0)
// For all of the above, sp.data() == NULL, sp.length() == 0,
// and sp.empty() == true.  Also, if you create a StringPiece with
// a non-NULL pointer then sp.data() != non-NULL.  Once created,
// sp.data() will stay either NULL or not-NULL, except if you call
// sp.clear() or sp.set().
//
// Thus, you can use StringPiece(NULL) to signal an out-of-band value
// that is different from other StringPiece values.  This is similar
// to the way that const char* p1 = NULL; is different from
// const char* p2 = "";.
//
// There are many ways to create an empty StringPiece:
//   StringPiece()
//   StringPiece(NULL)
//   StringPiece(NULL, 0)
//   StringPiece("")
//   StringPiece("", 0)
//   StringPiece("abcdef", 0)
//   StringPiece("abcdef"+6, 0)
// For all of the above, sp.length() will be 0 and sp.empty() will be true.
// For some empty StringPiece values, sp.data() will be NULL.
// For some empty StringPiece values, sp.data() will not be NULL.
//
// Be careful not to confuse: null StringPiece and empty StringPiece.
// The set of empty StringPieces properly includes the set of null StringPieces.
// That is, every null StringPiece is an empty StringPiece,
// but some non-null StringPieces are empty Stringpieces too.
//
// All empty StringPiece values compare equal to each other.
// Even a null StringPieces compares equal to a non-null empty StringPiece:
//  StringPiece() == StringPiece("", 0)
//  StringPiece(NULL) == StringPiece("abc", 0)
//  StringPiece(NULL, 0) == StringPiece("abcdef"+6, 0)
//
// Look carefully at this example:
//   StringPiece("") == NULL
// True or false?  TRUE, because StringPiece::operator== converts
// the right-hand side from NULL to StringPiece(NULL),
// and then compares two zero-length spans of characters.
// However, we are working to make this example produce a compile error.
//
// Suppose you want to write:
//   bool TestWhat?(StringPiece sp) { return sp == NULL; }  // BAD
// Do not do that.  Write one of these instead:
//   bool TestNull(StringPiece sp) { return sp.data() == NULL; }
//   bool TestEmpty(StringPiece sp) { return sp.empty(); }
// The intent of TestWhat? is unclear.  Did you mean TestNull or TestEmpty?
// Right now, TestWhat? behaves likes TestEmpty.
// We are working to make TestWhat? produce a compile error.
// TestNull is good to test for an out-of-band signal.
// TestEmpty is good to test for an empty StringPiece.
//
// Caveats (again):
// (1) The lifetime of the pointed-to string (or piece of a string)
//     must be longer than the lifetime of the StringPiece.
// (2) There may or may not be a '\0' character after the end of
//     StringPiece data.
// (3) A null StringPiece is empty.
//     An empty StringPiece may or may not be a null StringPiece.

#pragma once

#include <assert.h>
#include <stddef.h>
#include <string.h>

#include <cstddef>
#include <iosfwd>
#include <iterator>
#include <limits> // IWYU pragma: keep
#include <string>
#include <string_view>

class StringPiece {
private:
    const char* ptr_ = nullptr;
    int length_;

public:
    // We provide non-explicit singleton constructors so users can pass
    // in a "const char*" or a "string" wherever a "StringPiece" is
    // expected.
    //
    // Style guide exception granted:
    // http://goto/style-guide-exception-20978288
    StringPiece() : ptr_(NULL), length_(0) {}
    StringPiece(const char* str) // NOLINT(runtime/explicit)
            : ptr_(str), length_(0) {
        if (str != NULL) {
            size_t length = strlen(str);
            assert(length <= static_cast<size_t>(std::numeric_limits<int>::max()));
            length_ = static_cast<int>(length);
        }
    }
    StringPiece(const std::string& str) // NOLINT(runtime/explicit)
            : ptr_(str.data()), length_(0) {
        size_t length = str.size();
        assert(length <= static_cast<size_t>(std::numeric_limits<int>::max()));
        length_ = static_cast<int>(length);
    }
    StringPiece(const char* offset, int len) : ptr_(offset), length_(len) { assert(len >= 0); }

    // Substring of another StringPiece.
    // pos must be non-negative and <= x.length().
    StringPiece(StringPiece x, int pos);
    // Substring of another StringPiece.
    // pos must be non-negative and <= x.length().
    // len must be non-negative and will be pinned to at most x.length() - pos.
    StringPiece(StringPiece x, int pos, int len);

    // data() may return a pointer to a buffer with embedded NULs, and the
    // returned buffer may or may not be null terminated.  Therefore it is
    // typically a mistake to pass data() to a routine that expects a NUL
    // terminated string.
    const char* data() const { return ptr_; }
    int size() const { return length_; }
    int length() const { return length_; }
    bool empty() const { return length_ == 0; }

    void clear() {
        ptr_ = NULL;
        length_ = 0;
    }

    void set(const char* data, int len) {
        assert(len >= 0);
        ptr_ = data;
        length_ = len;
    }

    void set(const char* str) {
        ptr_ = str;
        if (str != NULL)
            length_ = static_cast<int>(strlen(str));
        else
            length_ = 0;
    }
    void set(const void* data, int len) {
        ptr_ = reinterpret_cast<const char*>(data);
        length_ = len;
    }

    char operator[](int i) const {
        assert(0 <= i);
        assert(i < length_);
        return ptr_[i];
    }

    void remove_prefix(int n) {
        assert(length_ >= n);
        ptr_ += n;
        length_ -= n;
    }

    void remove_suffix(int n) {
        assert(length_ >= n);
        length_ -= n;
    }

    // returns {-1, 0, 1}
    int compare(StringPiece x) const {
        const int min_size = length_ < x.length_ ? length_ : x.length_;
        int r = memcmp(ptr_, x.ptr_, min_size);
        if (r < 0) return -1;
        if (r > 0) return 1;
        if (length_ < x.length_) return -1;
        if (length_ > x.length_) return 1;
        return 0;
    }

    std::string as_string() const { return ToString(); }
    // We also define ToString() here, since many other string-like
    // interfaces name the routine that converts to a C++ string
    // "ToString", and it's confusing to have the method that does that
    // for a StringPiece be called "as_string()".  We also leave the
    // "as_string()" method defined here for existing code.
    std::string ToString() const {
        if (ptr_ == NULL) return std::string();
        return std::string(data(), size());
    }

    void CopyToString(std::string* target) const;
    void AppendToString(std::string* target) const;

    bool starts_with(StringPiece x) const {
        return (length_ >= x.length_) && (memcmp(ptr_, x.ptr_, x.length_) == 0);
    }

    bool ends_with(StringPiece x) const {
        return ((length_ >= x.length_) &&
                (memcmp(ptr_ + (length_ - x.length_), x.ptr_, x.length_) == 0));
    }

    // standard STL container boilerplate
    typedef char value_type;
    typedef const char* pointer;
    typedef const char& reference;
    typedef const char& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;
    static const size_type npos;
    typedef const char* const_iterator;
    typedef const char* iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
    typedef std::reverse_iterator<iterator> reverse_iterator;
    iterator begin() const { return ptr_; }
    iterator end() const { return ptr_ + length_; }
    const_reverse_iterator rbegin() const { return const_reverse_iterator(ptr_ + length_); }
    const_reverse_iterator rend() const { return const_reverse_iterator(ptr_); }
    // STLS says return size_type, but Google says return int
    int max_size() const { return length_; }
    int capacity() const { return length_; }

    // cpplint.py emits a false positive [build/include_what_you_use]
    int copy(char* buf, size_type n, size_type pos = 0) const; // NOLINT

    bool contains(StringPiece s) const;

    int find(StringPiece s, size_type pos = 0) const;
    int find(char c, size_type pos = 0) const;
    int rfind(StringPiece s, size_type pos = npos) const;
    int rfind(char c, size_type pos = npos) const;

    int find_first_of(StringPiece s, size_type pos = 0) const;
    int find_first_of(char c, size_type pos = 0) const { return find(c, pos); }
    int find_first_not_of(StringPiece s, size_type pos = 0) const;
    int find_first_not_of(char c, size_type pos = 0) const;
    int find_last_of(StringPiece s, size_type pos = npos) const;
    int find_last_of(char c, size_type pos = npos) const { return rfind(c, pos); }
    int find_last_not_of(StringPiece s, size_type pos = npos) const;
    int find_last_not_of(char c, size_type pos = npos) const;

    StringPiece substr(size_type pos, size_type n = npos) const;
};

// This large function is defined inline so that in a fairly common case where
// one of the arguments is a literal, the compiler can elide a lot of the
// following comparisons.
inline bool operator==(StringPiece x, StringPiece y) {
    int len = x.size();
    if (len != y.size()) {
        return false;
    }

    return x.data() == y.data() || len <= 0 || 0 == memcmp(x.data(), y.data(), len);
}

inline bool operator!=(StringPiece x, StringPiece y) {
    return !(x == y);
}

inline bool operator<(StringPiece x, StringPiece y) {
    const int min_size = x.size() < y.size() ? x.size() : y.size();
    const int r = memcmp(x.data(), y.data(), min_size);
    return (r < 0) || (r == 0 && x.size() < y.size());
}

inline bool operator>(StringPiece x, StringPiece y) {
    return y < x;
}

inline bool operator<=(StringPiece x, StringPiece y) {
    return !(x > y);
}

inline bool operator>=(StringPiece x, StringPiece y) {
    return !(x < y);
}

// allow StringPiece to be logged
extern std::ostream& operator<<(std::ostream& o, StringPiece piece);
