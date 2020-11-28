//
// Copyright 1999-2006 and onwards Google, Inc.
//
// Useful string functions and so forth.  This is a grab-bag file.
//
// You might also want to look at memutil.h, which holds mem*()
// equivalents of a lot of the str*() functions in string.h,
// eg memstr, mempbrk, etc.
//
// These functions work fine for UTF-8 strings as long as you can
// consider them to be just byte strings.  For example, due to the
// design of UTF-8 you do not need to worry about accidental matches,
// as long as all your inputs are valid UTF-8 (use \uHHHH, not \xHH or \oOOO).
//
// Caveats:
// * all the lengths in these routines refer to byte counts,
//   not character counts.
// * case-insensitivity in these routines assumes that all the letters
//   in question are in the range A-Z or a-z.
//
// If you need Unicode specific processing (for example being aware of
// Unicode character boundaries, or knowledge of Unicode casing rules,
// or various forms of equivalence and normalization), take a look at
// files in i18n/utf8.

#ifndef STRINGS_UTIL_H_
#define STRINGS_UTIL_H_

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifndef _MSC_VER
#include <strings.h> // for strcasecmp, but msvc does not have this header
#endif

#include <functional>
using std::binary_function;
using std::less;
#include <string>
using std::string;
#include <vector>
using std::vector;

#include "gutil/integral_types.h"
#include "gutil/port.h"
#include "gutil/strings/stringpiece.h"

// Newer functions.

namespace strings {

// Finds the next end-of-line sequence.
// An end-of-line sequence is one of:
//   \n    common on unix, including mac os x
//   \r    common on macos 9 and before
//   \r\n  common on windows
//
// Returns a StringPiece that contains the end-of-line sequence (a pointer into
// the input, 1 or 2 characters long).
//
// If the input does not contain an end-of-line sequence, returns an empty
// StringPiece located at the end of the input:
//    StringPiece(sp.data() + sp.length(), 0).

StringPiece FindEol(StringPiece sp);

} // namespace strings

// Older functions.

// Duplicates a non-null, non-empty char* string. Returns a pointer to the new
// string, or NULL if the input is null or empty.
inline char* strdup_nonempty(const char* src) {
    if (src && src[0]) return strdup(src);
    return NULL;
}

// Finds the first occurrence of a character in at most a given number of bytes
// of a char* string. Returns a pointer to the first occurrence, or NULL if no
// occurrence found in the first sz bytes.
// Never searches past the first null character in the string; therefore, only
// suitable for null-terminated strings.
// WARNING: Removes const-ness of string argument!
inline char* strnchr(const char* buf, char c, int sz) {
    const char* end = buf + sz;
    while (buf != end && *buf) {
        if (*buf == c) return const_cast<char*>(buf);
        ++buf;
    }
    return NULL;
}

// Finds the first occurrence of the null-terminated needle in at most the first
// haystack_len bytes of haystack. Returns NULL if needle is not found. Returns
// haystack if needle is empty.
// WARNING: Removes const-ness of string argument!
char* strnstr(const char* haystack, const char* needle, size_t haystack_len);

// Matches a prefix (which must be a char* literal!) against the beginning of
// str. Returns a pointer past the prefix, or NULL if the prefix wasn't matched.
// (Like the standard strcasecmp(), but for efficiency doesn't call strlen() on
// prefix, and returns a pointer rather than an int.)
//
// The ""'s catch people who don't pass in a literal for "prefix"
#ifndef strprefix
#define strprefix(str, prefix) \
    (strncmp(str, prefix, sizeof("" prefix "") - 1) == 0 ? str + sizeof(prefix) - 1 : NULL)
#endif

// Same as strprefix() (immediately above), but matches a case-insensitive
// prefix.
#ifndef strcaseprefix
#define strcaseprefix(str, prefix) \
    (strncasecmp(str, prefix, sizeof("" prefix "") - 1) == 0 ? str + sizeof(prefix) - 1 : NULL)
#endif

// Matches a prefix (up to the first needle_size bytes of needle) in the first
// haystack_size byte of haystack. Returns a pointer past the prefix, or NULL if
// the prefix wasn't matched. (Unlike strprefix(), prefix doesn't need to be a
// char* literal. Like the standard strncmp(), but also takes a haystack_size,
// and returns a pointer rather than an int.)
//
// Always returns either NULL or haystack + needle_size.
//
// Some windows header sometimes #defines strnprefix to something we
// don't want.
#ifdef strnprefix
#undef strnprefix
#endif
const char* strnprefix(const char* haystack, int haystack_size, const char* needle,
                       int needle_size);

// Matches a case-insensitive prefix (up to the first needle_size bytes of
// needle) in the first haystack_size byte of haystack. Returns a pointer past
// the prefix, or NULL if the prefix wasn't matched.
//
// Always returns either NULL or haystack + needle_size.
const char* strncaseprefix(const char* haystack, int haystack_size, const char* needle,
                           int needle_size);

// Matches a prefix; returns a pointer past the prefix, or NULL if not found.
// (Like strprefix() and strcaseprefix() but not restricted to searching for
// char* literals). Templated so searching a const char* returns a const char*,
// and searching a non-const char* returns a non-const char*.
template <class CharStar>
inline CharStar var_strprefix(CharStar str, const char* prefix) {
    const int len = strlen(prefix);
    return strncmp(str, prefix, len) == 0 ? str + len : NULL;
}

// Same as var_strprefix() (immediately above), but matches a case-insensitive
// prefix.
template <class CharStar>
inline CharStar var_strcaseprefix(CharStar str, const char* prefix) {
    const int len = strlen(prefix);
    return strncasecmp(str, prefix, len) == 0 ? str + len : NULL;
}

// Returns input, or "(null)" if NULL. (Useful for logging.)
inline const char* GetPrintableString(const char* const in) {
    return NULL == in ? "(null)" : in;
}

// Returns whether str begins with prefix.
inline bool HasPrefixString(const StringPiece& str, const StringPiece& prefix) {
    return str.starts_with(prefix);
}

// Returns whether str ends with suffix.
inline bool HasSuffixString(const StringPiece& str, const StringPiece& suffix) {
    return str.ends_with(suffix);
}

// Returns true if the string passed in matches the pattern. The pattern
// string can contain wildcards like * and ?
// The backslash character (\) is an escape character for * and ?
// We limit the patterns to having a max of 16 * or ? characters.
// ? matches 0 or 1 character, while * matches 0 or more characters.
bool MatchPattern(const StringPiece& string, const StringPiece& pattern);

// Returns where suffix begins in str, or NULL if str doesn't end with suffix.
inline char* strsuffix(char* str, const char* suffix) {
    const int lenstr = strlen(str);
    const int lensuffix = strlen(suffix);
    char* strbeginningoftheend = str + lenstr - lensuffix;

    if (lenstr >= lensuffix && 0 == strcmp(strbeginningoftheend, suffix)) {
        return (strbeginningoftheend);
    } else {
        return (NULL);
    }
}
inline const char* strsuffix(const char* str, const char* suffix) {
    return const_cast<const char*>(strsuffix(const_cast<char*>(str), suffix));
}

// Same as strsuffix() (immediately above), but matches a case-insensitive
// suffix.
char* strcasesuffix(char* str, const char* suffix);
inline const char* strcasesuffix(const char* str, const char* suffix) {
    return const_cast<const char*>(strcasesuffix(const_cast<char*>(str), suffix));
}

const char* strnsuffix(const char* haystack, int haystack_size, const char* needle,
                       int needle_size);
const char* strncasesuffix(const char* haystack, int haystack_size, const char* needle,
                           int needle_size);

// Returns the number of times a character occurs in a string for a null
// terminated string.
inline ptrdiff_t strcount(const char* buf, char c) {
    if (buf == NULL) return 0;
    ptrdiff_t num = 0;
    for (const char* bp = buf; *bp != '\0'; bp++) {
        if (*bp == c) num++;
    }
    return num;
}
// Returns the number of times a character occurs in a string for a string
// defined by a pointer to the first character and a pointer just past the last
// character.
inline ptrdiff_t strcount(const char* buf_begin, const char* buf_end, char c) {
    if (buf_begin == NULL) return 0;
    if (buf_end <= buf_begin) return 0;
    ptrdiff_t num = 0;
    for (const char* bp = buf_begin; bp != buf_end; bp++) {
        if (*bp == c) num++;
    }
    return num;
}
// Returns the number of times a character occurs in a string for a string
// defined by a pointer to the first char and a length:
inline ptrdiff_t strcount(const char* buf, size_t len, char c) {
    return strcount(buf, buf + len, c);
}
// Returns the number of times a character occurs in a string for a C++ string:
inline ptrdiff_t strcount(const string& buf, char c) {
    return strcount(buf.c_str(), buf.size(), c);
}

// Returns a pointer to the nth occurrence of a character in a null-terminated
// string.
// WARNING: Removes const-ness of string argument!
char* strchrnth(const char* str, const char& c, int n);

// Returns a pointer to the nth occurrence of a character in a null-terminated
// string, or the last occurrence if occurs fewer than n times.
// WARNING: Removes const-ness of string argument!
char* AdjustedLastPos(const char* str, char separator, int n);

// STL-compatible function objects for char* string keys:

// Compares two char* strings for equality. (Works with NULL, which compares
// equal only to another NULL). Useful in hash tables:
//    hash_map<const char*, Value, hash<const char*>, streq> ht;
struct streq : public binary_function<const char*, const char*, bool> {
    bool operator()(const char* s1, const char* s2) const {
        return ((s1 == 0 && s2 == 0) || (s1 && s2 && *s1 == *s2 && strcmp(s1, s2) == 0));
    }
};

// Compares two char* strings. (Works with NULL, which compares greater than any
// non-NULL). Useful in maps:
//    map<const char*, Value, strlt> m;
struct strlt : public binary_function<const char*, const char*, bool> {
    bool operator()(const char* s1, const char* s2) const {
        return (s1 != s2) && (s2 == 0 || (s1 != 0 && strcmp(s1, s2) < 0));
    }
};

// Returns whether str has only Ascii characters (as defined by ascii_isascii()
// in strings/ascii_ctype.h).
bool IsAscii(const char* str, int len);
inline bool IsAscii(const StringPiece& str) {
    return IsAscii(str.data(), str.size());
}

// Returns the smallest lexicographically larger string of equal or smaller
// length. Returns an empty string if there is no such successor (if the input
// is empty or consists entirely of 0xff bytes).
// Useful for calculating the smallest lexicographically larger string
// that will not be prefixed by the input string.
//
// Examples:
// "a" -> "b", "aaa" -> "aab", "aa\xff" -> "ab", "\xff" -> "", "" -> ""
string PrefixSuccessor(const StringPiece& prefix);

// Returns the immediate lexicographically-following string. This is useful to
// turn an inclusive range into something that can be used with Bigtable's
// SetLimitRow():
//
//     // Inclusive range [min_element, max_element].
//     string min_element = ...;
//     string max_element = ...;
//
//     // Equivalent range [range_start, range_end).
//     string range_start = min_element;
//     string range_end = ImmediateSuccessor(max_element);
//
// WARNING: Returns the input string with a '\0' appended; if you call c_str()
// on the result, it will compare equal to s.
//
// WARNING: Transforms "" -> "\0"; this doesn't account for Bigtable's special
// treatment of "" as infinity.
string ImmediateSuccessor(const StringPiece& s);

// Fills in *separator with a short string less than limit but greater than or
// equal to start. If limit is greater than start, *separator is the common
// prefix of start and limit, followed by the successor to the next character in
// start. Examples:
// FindShortestSeparator("foobar", "foxhunt", &sep) => sep == "fop"
// FindShortestSeparator("abracadabra", "bacradabra", &sep) => sep == "b"
// If limit is less than or equal to start, fills in *separator with start.
void FindShortestSeparator(const StringPiece& start, const StringPiece& limit, string* separator);

// Copies at most n-1 bytes from src to dest, and returns dest. If n >=1, null
// terminates dest; otherwise, returns dest unchanged. Unlike strncpy(), only
// puts one null character at the end of dest.
inline char* safestrncpy(char* dest, const char* src, size_t n) {
    if (n < 1) return dest;

    // Avoid using non-ANSI memccpy(), which is also deprecated in MSVC
    for (size_t i = 0; i < n; ++i) {
        if ((dest[i] = src[i]) == '\0') return dest;
    }

    dest[n - 1] = '\0';
    return dest;
}

namespace strings {

// BSD-style safe and consistent string copy functions.
// Copies |src| to |dst|, where |dst_size| is the total allocated size of |dst|.
// Copies at most |dst_size|-1 characters, and always NULL terminates |dst|, as
// long as |dst_size| is not 0.  Returns the length of |src| in characters.
// If the return value is >= dst_size, then the output was truncated.
// NOTE: All sizes are in number of characters, NOT in bytes.
size_t strlcpy(char* dst, const char* src, size_t dst_size);

} // namespace strings

// Replaces the first occurrence (if replace_all is false) or all occurrences
// (if replace_all is true) of oldsub in s with newsub. In the second version,
// *res must be distinct from all the other arguments.
string StringReplace(const StringPiece& s, const StringPiece& oldsub, const StringPiece& newsub,
                     bool replace_all);
void StringReplace(const StringPiece& s, const StringPiece& oldsub, const StringPiece& newsub,
                   bool replace_all, string* res);

// Replaces all occurrences of substring in s with replacement. Returns the
// number of instances replaced. s must be distinct from the other arguments.
//
// Less flexible, but faster, than RE::GlobalReplace().
int GlobalReplaceSubstring(const StringPiece& substring, const StringPiece& replacement, string* s);

// Removes v[i] for every element i in indices. Does *not* preserve the order of
// v. indices must be sorted in strict increasing order (no duplicates). Runs in
// O(indices.size()).
void RemoveStrings(vector<string>* v, const vector<int>& indices);

// Case-insensitive strstr(); use system strcasestr() instead.
// WARNING: Removes const-ness of string argument!
char* gstrcasestr(const char* haystack, const char* needle);

// Finds (case insensitively) the first occurrence of (null terminated) needle
// in at most the first len bytes of haystack. Returns a pointer into haystack,
// or NULL if needle wasn't found.
// WARNING: Removes const-ness of haystack!
const char* gstrncasestr(const char* haystack, const char* needle, size_t len);
char* gstrncasestr(char* haystack, const char* needle, size_t len);

// Finds (case insensitively), in str (which is a list of tokens separated by
// non_alpha), a token prefix and a token suffix. Returns a pointer into str of
// the position of prefix, or NULL if not found.
// WARNING: Removes const-ness of string argument!
char* gstrncasestr_split(const char* str, const char* prefix, char non_alpha, const char* suffix,
                         size_t n);

// Finds (case insensitively) needle in haystack, paying attention only to
// alphanumerics in either string. Returns a pointer into haystack, or NULL if
// not found.
// Example: strcasestr_alnum("This is a longer test string", "IS-A-LONGER")
// returns a pointer to "is a longer".
// WARNING: Removes const-ness of string argument!
char* strcasestr_alnum(const char* haystack, const char* needle);

// Returns the number times substring appears in text.
// Note: Runs in O(text.length() * substring.length()). Do *not* use on long
// strings.
int CountSubstring(StringPiece text, StringPiece substring);

// Finds, in haystack (which is a list of tokens separated by delim), an token
// equal to needle. Returns a pointer into haystack, or NULL if not found (or
// either needle or haystack is empty).
const char* strstr_delimited(const char* haystack, const char* needle, char delim);

// Gets the next token from string *stringp, where tokens are strings separated
// by characters from delim.
char* gstrsep(char** stringp, const char* delim);

// Appends StringPiece(data, len) to *s.
void FastStringAppend(string* s, const char* data, int len);

// Returns a duplicate of the_string, with memory allocated by new[].
char* strdup_with_new(const char* the_string);

// Returns a duplicate of up to the first max_length bytes of the_string, with
// memory allocated by new[].
char* strndup_with_new(const char* the_string, int max_length);

// Finds, in the_string, the first "word" (consecutive !ascii_isspace()
// characters). Returns pointer to the beginning of the word, and sets *end_ptr
// to the character after the word (which may be space or '\0'); returns NULL
// (and *end_ptr is undefined) if no next word found.
// end_ptr must not be NULL.
const char* ScanForFirstWord(const char* the_string, const char** end_ptr);
inline char* ScanForFirstWord(char* the_string, char** end_ptr) {
    // implicit_cast<> would be more appropriate for casting to const,
    // but we save the inclusion of "base/casts.h" here by using const_cast<>.
    return const_cast<char*>(ScanForFirstWord(const_cast<const char*>(the_string),
                                              const_cast<const char**>(end_ptr)));
}

// For the following functions, an "identifier" is a letter or underscore,
// followed by letters, underscores, or digits.

// Returns a pointer past the end of the "identifier" (see above) beginning at
// str, or NULL if str doesn't start with an identifier.
const char* AdvanceIdentifier(const char* str);
inline char* AdvanceIdentifier(char* str) {
    // implicit_cast<> would be more appropriate for casting to const,
    // but we save the inclusion of "base/casts.h" here by using const_cast<>.
    return const_cast<char*>(AdvanceIdentifier(const_cast<const char*>(str)));
}

// Returns whether str is an "identifier" (see above).
bool IsIdentifier(const char* str);

// Finds the first tag and value in a string of tag/value pairs.
//
// The first pair begins after the first occurrence of attribute_separator (or
// string_terminal, if not '\0'); tag_value_separator separates the tag and
// value; and the value ends before the following occurrence of
// attribute_separator (or string_terminal, if not '\0').
//
// Returns true (and populates tag, tag_len, value, and value_len) if a
// tag/value pair is founds; returns false otherwise.
bool FindTagValuePair(const char* in_str, char tag_value_separator, char attribute_separator,
                      char string_terminal, char** tag, int* tag_len, char** value, int* value_len);

// Inserts separator after every interval characters in *s (but never appends to
// the end of the original *s).
void UniformInsertString(string* s, int interval, const char* separator);

// Inserts separator into s at each specified index. indices must be sorted in
// ascending order.
void InsertString(string* s, const vector<uint32>& indices, char const* separator);

// Finds the nth occurrence of c in n; returns the index in s of that
// occurrence, or string::npos if fewer than n occurrences.
int FindNth(StringPiece s, char c, int n);

// Finds the nth-to-last occurrence of c in s; returns the index in s of that
// occurrence, or string::npos if fewer than n occurrences.
int ReverseFindNth(StringPiece s, char c, int n);

// Returns whether s contains only whitespace characters (including the case
// where s is empty).
bool OnlyWhitespace(const StringPiece& s);

// Formats a string in the same fashion as snprintf(), but returns either the
// number of characters written, or zero if not enough space was available.
// (snprintf() returns the number of characters that would have been written if
// enough space had been available.)
//
// A drop-in replacement for the safe_snprintf() macro.
int SafeSnprintf(char* str, size_t size, const char* format, ...) PRINTF_ATTRIBUTE(3, 4);

// Reads a line (terminated by delim) from file into *str. Reads delim from
// file, but doesn't copy it into *str. Returns true if read a delim-terminated
// line, or false on end-of-file or error.
bool GetlineFromStdioFile(FILE* file, string* str, char delim);

#endif // STRINGS_UTIL_H_
