// Copyright 2011 Google Inc. All Rights Reserved.
// Refactored from contributions of various authors in strings/strutil.h
//
// This file contains functions that remove a defined part from the string,
// i.e., strip the string.

#ifndef STRINGS_STRIP_H_
#define STRINGS_STRIP_H_

#include <stddef.h>

#include <string>
using std::string;

#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/stringpiece.h"

// Given a string and a putative prefix, returns the string minus the
// prefix string if the prefix matches, otherwise the original
// string.
string StripPrefixString(StringPiece str, const StringPiece& prefix);

// Like StripPrefixString, but return true if the prefix was
// successfully matched.  Write the output to *result.
// It is safe for result to point back to the input string.
bool TryStripPrefixString(StringPiece str, const StringPiece& prefix, string* result);

// Given a string and a putative suffix, returns the string minus the
// suffix string if the suffix matches, otherwise the original
// string.
string StripSuffixString(StringPiece str, const StringPiece& suffix);

// Like StripSuffixString, but return true if the suffix was
// successfully matched.  Write the output to *result.
// It is safe for result to point back to the input string.
bool TryStripSuffixString(StringPiece str, const StringPiece& suffix, string* result);

// ----------------------------------------------------------------------
// StripString
//    Replaces any occurrence of the character 'remove' (or the characters
//    in 'remove') with the character 'replacewith'.
//    Good for keeping html characters or protocol characters (\t) out
//    of places where they might cause a problem.
// ----------------------------------------------------------------------
inline void StripString(char* str, char remove, char replacewith) {
    for (; *str; str++) {
        if (*str == remove) *str = replacewith;
    }
}

void StripString(char* str, StringPiece remove, char replacewith);
void StripString(char* str, int len, StringPiece remove, char replacewith);
void StripString(string* s, StringPiece remove, char replacewith);

// ----------------------------------------------------------------------
// StripDupCharacters
//    Replaces any repeated occurrence of the character 'dup_char'
//    with single occurrence.  e.g.,
//       StripDupCharacters("a//b/c//d", '/', 0) => "a/b/c/d"
//    Return the number of characters removed
// ----------------------------------------------------------------------
int StripDupCharacters(string* s, char dup_char, int start_pos);

// ----------------------------------------------------------------------
// StripWhiteSpace
//    "Removes" whitespace from both sides of string.  Pass in a pointer to an
//    array of characters, and its length.  The function changes the pointer
//    and length to refer to a substring that does not contain leading or
//    trailing spaces; it does not modify the string itself.  If the caller is
//    using NUL-terminated strings, it is the caller's responsibility to insert
//    the NUL character at the end of the substring."
//
//    Note: to be completely type safe, this function should be
//    parameterized as a template: template<typename anyChar> void
//    StripWhiteSpace(anyChar** str, int* len), where the expectation
//    is that anyChar could be char, const char, w_char, const w_char,
//    unicode_char, or any other character type we want.  However, we
//    just provided a version for char and const char.  C++ is
//    inconvenient, but correct, here.  Ask Amit is you want to know
//    the type safety details.
// ----------------------------------------------------------------------
void StripWhiteSpace(const char** str, int* len);

//------------------------------------------------------------------------
// StripTrailingWhitespace()
//   Removes whitespace at the end of the string *s.
//------------------------------------------------------------------------
void StripTrailingWhitespace(string* s);

//------------------------------------------------------------------------
// StripTrailingNewline(string*)
//   Strips the very last trailing newline or CR+newline from its
//   input, if one exists.  Useful for dealing with MapReduce's text
//   input mode, which appends '\n' to each map input.  Returns true
//   if a newline was stripped.
//------------------------------------------------------------------------
bool StripTrailingNewline(string* s);

inline void StripWhiteSpace(char** str, int* len) {
    // The "real" type for StripWhiteSpace is ForAll char types C, take
    // (C, int) as input and return (C, int) as output.  We're using the
    // cast here to assert that we can take a char*, even though the
    // function thinks it's assigning to const char*.
    StripWhiteSpace(const_cast<const char**>(str), len);
}

inline void StripWhiteSpace(StringPiece* str) {
    const char* data = str->data();
    int len = str->size();
    StripWhiteSpace(&data, &len);
    str->set(data, len);
}

void StripWhiteSpace(string* str);

namespace strings {

template <typename Collection>
inline void StripWhiteSpaceInCollection(Collection* collection) {
    for (typename Collection::iterator it = collection->begin(); it != collection->end(); ++it)
        StripWhiteSpace(&(*it));
}

} // namespace strings

// ----------------------------------------------------------------------
// StripLeadingWhiteSpace
//    "Removes" whitespace from beginning of string. Returns ptr to first
//    non-whitespace character if one is present, NULL otherwise. Assumes
//    "line" is null-terminated.
// ----------------------------------------------------------------------

inline const char* StripLeadingWhiteSpace(const char* line) {
    // skip leading whitespace
    while (ascii_isspace(*line)) ++line;

    if ('\0' == *line) // end of line, no non-whitespace
        return NULL;

    return line;
}

// StripLeadingWhiteSpace for non-const strings.
inline char* StripLeadingWhiteSpace(char* line) {
    return const_cast<char*>(StripLeadingWhiteSpace(const_cast<const char*>(line)));
}

void StripLeadingWhiteSpace(string* str);

// Remove leading, trailing, and duplicate internal whitespace.
void RemoveExtraWhitespace(string* s);

// ----------------------------------------------------------------------
// SkipLeadingWhiteSpace
//    Returns str advanced past white space characters, if any.
//    Never returns NULL.  "str" must be terminated by a null character.
// ----------------------------------------------------------------------
inline const char* SkipLeadingWhiteSpace(const char* str) {
    while (ascii_isspace(*str)) ++str;
    return str;
}

inline char* SkipLeadingWhiteSpace(char* str) {
    while (ascii_isspace(*str)) ++str;
    return str;
}

// ----------------------------------------------------------------------
// StripCurlyBraces
//    Strips everything enclosed in pairs of curly braces and the curly
//    braces. Doesn't touch open braces. It doesn't handle nested curly
//    braces. This is used for removing things like {:stopword} from
//    queries.
// StripBrackets does the same, but allows the caller to specify different
//    left and right bracket characters, such as '(' and ')'.
// ----------------------------------------------------------------------

void StripCurlyBraces(string* s);
void StripBrackets(char left, char right, string* s);

// ----------------------------------------------------------------------
// StripMarkupTags
//    Strips everything enclosed in pairs of angle brackets and the angle
//    brackets.
//    This is used for stripping strings of markup; e.g. going from
//    "the quick <b>brown</b> fox" to "the quick brown fox."
//    If you want to skip entire sections of markup (e.g. the word "brown"
//    too in that example), see webutil/pageutil/pageutil.h .
//    This function was designed for stripping the bold tags (inserted by the
//    docservers) from the titles of news stories being returned by RSS.
//    This implementation DOES NOT cover all cases in html documents
//    like tags that contain quoted angle-brackets, or HTML comment.
//    For example <IMG SRC = "foo.gif" ALT = "A > B">
//    or <!-- <A comment> -->
//    See "perldoc -q html"
// ----------------------------------------------------------------------

void StripMarkupTags(string* s);
string OutputWithMarkupTagsStripped(const string& s);

// ----------------------------------------------------------------------
// TrimStringLeft
//    Removes any occurrences of the characters in 'remove' from the start
//    of the string.  Returns the number of chars trimmed.
// ----------------------------------------------------------------------
int TrimStringLeft(string* s, const StringPiece& remove);

// ----------------------------------------------------------------------
// TrimStringRight
//    Removes any occurrences of the characters in 'remove' from the end
//    of the string.  Returns the number of chars trimmed.
// ----------------------------------------------------------------------
int TrimStringRight(string* s, const StringPiece& remove);

// ----------------------------------------------------------------------
// TrimString
//    Removes any occurrences of the characters in 'remove' from either
//    end of the string.
// ----------------------------------------------------------------------
inline int TrimString(string* s, const StringPiece& remove) {
    return TrimStringRight(s, remove) + TrimStringLeft(s, remove);
}

// ----------------------------------------------------------------------
// TrimRunsInString
//    Removes leading and trailing runs, and collapses middle
//    runs of a set of characters into a single character (the
//    first one specified in 'remove').  Useful for collapsing
//    runs of repeated delimiters, whitespace, etc.  E.g.,
//    TrimRunsInString(&s, " :,()") removes leading and trailing
//    delimiter chars and collapses and converts internal runs
//    of delimiters to single ' ' characters, so, for example,
//    "  a:(b):c  " -> "a b c"
//    "first,last::(area)phone, ::zip" -> "first last area phone zip"
// ----------------------------------------------------------------------
void TrimRunsInString(string* s, StringPiece remove);

// ----------------------------------------------------------------------
// RemoveNullsInString
//    Removes any internal \0 characters from the string.
// ----------------------------------------------------------------------
void RemoveNullsInString(string* s);

// ----------------------------------------------------------------------
// strrm()
// memrm()
//    Remove all occurrences of a given character from a string.
//    Returns the new length.
// ----------------------------------------------------------------------

int strrm(char* str, char c);
int memrm(char* str, int strlen, char c);

// ----------------------------------------------------------------------
// strrmm()
//    Remove all occurrences of a given set of characters from a string.
//    Returns the new length.
// ----------------------------------------------------------------------
int strrmm(char* str, const char* chars);
int strrmm(string* str, const string& chars);

#endif // STRINGS_STRIP_H_
