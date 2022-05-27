//
// Copyright (C) 1999-2005 Google, Inc.
//

// TODO(user): visit each const_cast.  Some of them are no longer necessary
// because last Single Unix Spec and grte v2 are more const-y.

#include "gutil/strings/util.h"

#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h> // for FastTimeToBuffer()

#include <algorithm>
using std::copy;
using std::max;
using std::min;
using std::reverse;
using std::sort;
using std::swap;
#include <string>
using std::string;
#include <vector>
using std::vector;

#include <common/logging.h>

#include "gutil/stl_util.h" // for string_as_array, STLAppendToString
#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/utf/utf.h"

#ifdef OS_WINDOWS
#ifdef min // windows.h defines this to something silly
#undef min
#endif
#endif

// Use this instead of gmtime_r if you want to build for Windows.
// Windows doesn't have a 'gmtime_r', but it has the similar 'gmtime_s'.
// TODO(user): Probably belongs in //base:time_support.{cc|h}.
static struct tm* PortableSafeGmtime(const time_t* timep, struct tm* result) {
#ifdef OS_WINDOWS
    return gmtime_s(result, timep) == 0 ? result : NULL;
#else
    return gmtime_r(timep, result);
#endif // OS_WINDOWS
}

char* strnstr(const char* haystack, const char* needle, size_t haystack_len) {
    if (*needle == '\0') {
        return const_cast<char*>(haystack);
    }
    size_t needle_len = strlen(needle);
    char* where;
    while ((where = strnchr(haystack, *needle, haystack_len)) != nullptr) {
        if (where - haystack + needle_len > haystack_len) {
            return nullptr;
        }
        if (strncmp(where, needle, needle_len) == 0) {
            return where;
        }
        haystack_len -= where + 1 - haystack;
        haystack = where + 1;
    }
    return nullptr;
}

const char* strnprefix(const char* haystack, int haystack_size, const char* needle,
                       int needle_size) {
    if (needle_size > haystack_size) {
        return nullptr;
    } else {
        if (strncmp(haystack, needle, needle_size) == 0) {
            return haystack + needle_size;
        } else {
            return nullptr;
        }
    }
}

const char* strncaseprefix(const char* haystack, int haystack_size, const char* needle,
                           int needle_size) {
    if (needle_size > haystack_size) {
        return nullptr;
    } else {
        if (strncasecmp(haystack, needle, needle_size) == 0) {
            return haystack + needle_size;
        } else {
            return nullptr;
        }
    }
}

char* strcasesuffix(char* str, const char* suffix) {
    const int lenstr = strlen(str);
    const int lensuffix = strlen(suffix);
    char* strbeginningoftheend = str + lenstr - lensuffix;

    if (lenstr >= lensuffix && 0 == strcasecmp(strbeginningoftheend, suffix)) {
        return (strbeginningoftheend);
    } else {
        return (nullptr);
    }
}

const char* strnsuffix(const char* haystack, int haystack_size, const char* needle,
                       int needle_size) {
    if (needle_size > haystack_size) {
        return nullptr;
    } else {
        const char* start = haystack + haystack_size - needle_size;
        if (strncmp(start, needle, needle_size) == 0) {
            return start;
        } else {
            return nullptr;
        }
    }
}

const char* strncasesuffix(const char* haystack, int haystack_size, const char* needle,
                           int needle_size) {
    if (needle_size > haystack_size) {
        return nullptr;
    } else {
        const char* start = haystack + haystack_size - needle_size;
        if (strncasecmp(start, needle, needle_size) == 0) {
            return start;
        } else {
            return nullptr;
        }
    }
}

char* strchrnth(const char* str, const char& c, int n) {
    if (str == nullptr) return nullptr;
    if (n <= 0) return const_cast<char*>(str);
    const char* sp;
    int k = 0;
    for (sp = str; *sp != '\0'; sp++) {
        if (*sp == c) {
            ++k;
            if (k >= n) break;
        }
    }
    return (k < n) ? nullptr : const_cast<char*>(sp);
}

char* AdjustedLastPos(const char* str, char separator, int n) {
    if (str == nullptr) return nullptr;
    const char* pos = nullptr;
    if (n > 0) pos = strchrnth(str, separator, n);

    // if n <= 0 or separator appears fewer than n times, get the last occurrence
    if (pos == nullptr) pos = strrchr(str, separator);
    return const_cast<char*>(pos);
}

// ----------------------------------------------------------------------
// Misc. routines
// ----------------------------------------------------------------------

bool IsAscii(const char* str, int len) {
    const char* end = str + len;
    while (str < end) {
        if (!ascii_isascii(*str++)) {
            return false;
        }
    }
    return true;
}

// ----------------------------------------------------------------------
// StringReplace()
//    Give me a string and two patterns "old" and "new", and I replace
//    the first instance of "old" in the string with "new", if it
//    exists.  If "replace_all" is true then call this repeatedly until it
//    fails.  RETURN a new string, regardless of whether the replacement
//    happened or not.
// ----------------------------------------------------------------------

string StringReplace(const StringPiece& s, const StringPiece& oldsub, const StringPiece& newsub,
                     bool replace_all) {
    string ret;
    StringReplace(s, oldsub, newsub, replace_all, &ret);
    return ret;
}

// ----------------------------------------------------------------------
// StringReplace()
//    Replace the "old" pattern with the "new" pattern in a string,
//    and append the result to "res".  If replace_all is false,
//    it only replaces the first instance of "old."
// ----------------------------------------------------------------------

void StringReplace(const StringPiece& s, const StringPiece& oldsub, const StringPiece& newsub,
                   bool replace_all, string* res) {
    if (oldsub.empty()) {
        res->append(s.data(), s.length()); // If empty, append the given string.
        return;
    }

    StringPiece::size_type start_pos = 0;
    StringPiece::size_type pos;
    do {
        pos = s.find(oldsub, start_pos);
        if (pos == StringPiece::npos) {
            break;
        }
        res->append(s.data() + start_pos, pos - start_pos);
        res->append(newsub.data(), newsub.length());
        // Start searching again after the "old".
        start_pos = pos + oldsub.length();
    } while (replace_all);
    res->append(s.data() + start_pos, s.length() - start_pos);
}

// ----------------------------------------------------------------------
// GlobalReplaceSubstring()
//    Replaces all instances of a substring in a string.  Does nothing
//    if 'substring' is empty.  Returns the number of replacements.
//
//    NOTE: The string pieces must not overlap s.
// ----------------------------------------------------------------------

int GlobalReplaceSubstring(const StringPiece& substring, const StringPiece& replacement,
                           string* s) {
    CHECK(s != nullptr);
    if (s->empty() || substring.empty()) return 0;
    string tmp;
    int num_replacements = 0;
    size_t pos = 0;
    for (size_t match_pos = s->find(substring.data(), pos, substring.length());
         match_pos != string::npos; pos = match_pos + substring.length(),
                match_pos = s->find(substring.data(), pos, substring.length())) {
        ++num_replacements;
        // Append the original content before the match.
        tmp.append(*s, pos, match_pos - pos);
        // Append the replacement for the match.
        tmp.append(replacement.begin(), replacement.end());
    }
    // Append the content after the last match. If no replacements were made, the
    // original string is left untouched.
    if (num_replacements > 0) {
        tmp.append(*s, pos, s->length() - pos);
        s->swap(tmp);
    }
    return num_replacements;
}

//---------------------------------------------------------------------------
// RemoveStrings()
//   Remove the strings from v given by the (sorted least -> greatest)
//   numbers in indices.
//   Order of v is *not* preserved.
//---------------------------------------------------------------------------
void RemoveStrings(vector<string>* v, const vector<int>& indices) {
    assert(v);
    assert(indices.size() <= v->size());
    // go from largest index to smallest so that smaller indices aren't
    // invalidated
    for (int lcv = indices.size() - 1; lcv >= 0; --lcv) {
#ifndef NDEBUG
        // verify that indices is sorted least->greatest
        if (indices.size() >= 2 && lcv > 0)
            // use LT and not LE because we should never see repeat indices
            CHECK_LT(indices[lcv - 1], indices[lcv]);
#endif
        assert(indices[lcv] >= 0);
        assert(indices[lcv] < v->size());
        swap((*v)[indices[lcv]], v->back());
        v->pop_back();
    }
}

// ----------------------------------------------------------------------
// gstrcasestr is a case-insensitive strstr. Eventually we should just
// use the GNU libc version of strcasestr, but it isn't compiled into
// RedHat Linux by default in version 6.1.
//
// This function uses ascii_tolower() instead of tolower(), for speed.
// ----------------------------------------------------------------------

char* gstrcasestr(const char* haystack, const char* needle) {
    char c, sc;
    size_t len;

    if ((c = *needle++) != 0) {
        c = ascii_tolower(c);
        len = strlen(needle);
        do {
            do {
                if ((sc = *haystack++) == 0) return nullptr;
            } while (ascii_tolower(sc) != c);
        } while (strncasecmp(haystack, needle, len) != 0);
        haystack--;
    }
    // This is a const violation but strstr() also returns a char*.
    return const_cast<char*>(haystack);
}

// ----------------------------------------------------------------------
// gstrncasestr is a case-insensitive strnstr.
//    Finds the occurence of the (null-terminated) needle in the
//    haystack, where no more than len bytes of haystack is searched.
//    Characters that appear after a '\0' in the haystack are not searched.
//
// This function uses ascii_tolower() instead of tolower(), for speed.
// ----------------------------------------------------------------------
const char* gstrncasestr(const char* haystack, const char* needle, size_t len) {
    char c, sc;

    if ((c = *needle++) != 0) {
        c = ascii_tolower(c);
        size_t needle_len = strlen(needle);
        do {
            do {
                if (len-- <= needle_len || 0 == (sc = *haystack++)) return nullptr;
            } while (ascii_tolower(sc) != c);
        } while (strncasecmp(haystack, needle, needle_len) != 0);
        haystack--;
    }
    return haystack;
}

// ----------------------------------------------------------------------
// gstrncasestr is a case-insensitive strnstr.
//    Finds the occurence of the (null-terminated) needle in the
//    haystack, where no more than len bytes of haystack is searched.
//    Characters that appear after a '\0' in the haystack are not searched.
//
//    This function uses ascii_tolower() instead of tolower(), for speed.
// ----------------------------------------------------------------------
char* gstrncasestr(char* haystack, const char* needle, size_t len) {
    return const_cast<char*>(gstrncasestr(static_cast<const char*>(haystack), needle, len));
}
// ----------------------------------------------------------------------
// gstrncasestr_split performs a case insensitive search
// on (prefix, non_alpha, suffix).
// ----------------------------------------------------------------------
char* gstrncasestr_split(const char* str, const char* prefix, char non_alpha, const char* suffix,
                         size_t n) {
    int prelen = prefix == nullptr ? 0 : strlen(prefix);
    int suflen = suffix == nullptr ? 0 : strlen(suffix);

    // adjust the string and its length to avoid unnessary searching.
    // an added benefit is to avoid unnecessary range checks in the if
    // statement in the inner loop.
    if (suflen + prelen >= n) return nullptr;
    str += prelen;
    n -= prelen;
    n -= suflen;

    const char* where = nullptr;

    // for every occurance of non_alpha in the string ...
    while ((where = static_cast<const char*>(memchr(str, non_alpha, n))) != nullptr) {
        // ... test whether it is followed by suffix and preceded by prefix
        if ((!suflen || strncasecmp(where + 1, suffix, suflen) == 0) &&
            (!prelen || strncasecmp(where - prelen, prefix, prelen) == 0)) {
            return const_cast<char*>(where - prelen);
        }
        // if not, advance the pointer, and adjust the length according
        n -= (where + 1) - str;
        str = where + 1;
    }

    return nullptr;
}

// ----------------------------------------------------------------------
// strcasestr_alnum is like a case-insensitive strstr, except that it
// ignores non-alphanumeric characters in both strings for the sake of
// comparison.
//
// This function uses ascii_isalnum() instead of isalnum() and
// ascii_tolower() instead of tolower(), for speed.
//
// E.g. strcasestr_alnum("i use google all the time", " !!Google!! ")
// returns pointer to "google all the time"
// ----------------------------------------------------------------------
char* strcasestr_alnum(const char* haystack, const char* needle) {
    const char* haystack_ptr;
    const char* needle_ptr;

    // Skip non-alnums at beginning
    while (!ascii_isalnum(*needle))
        if (*needle++ == '\0') return const_cast<char*>(haystack);
    needle_ptr = needle;

    // Skip non-alnums at beginning
    while (!ascii_isalnum(*haystack))
        if (*haystack++ == '\0') return nullptr;
    haystack_ptr = haystack;

    while (*needle_ptr != '\0') {
        // Non-alnums - advance
        while (!ascii_isalnum(*needle_ptr))
            if (*needle_ptr++ == '\0') return const_cast<char*>(haystack);

        while (!ascii_isalnum(*haystack_ptr))
            if (*haystack_ptr++ == '\0') return nullptr;

        if (ascii_tolower(*needle_ptr) == ascii_tolower(*haystack_ptr)) {
            // Case-insensitive match - advance
            needle_ptr++;
            haystack_ptr++;
        } else {
            // No match - rollback to next start point in haystack
            haystack++;
            while (!ascii_isalnum(*haystack))
                if (*haystack++ == '\0') return nullptr;
            haystack_ptr = haystack;
            needle_ptr = needle;
        }
    }
    return const_cast<char*>(haystack);
}

// ----------------------------------------------------------------------
// CountSubstring()
//    Return the number times a "substring" appears in the "text"
//    NOTE: this function's complexity is O(|text| * |substring|)
//          It is meant for short "text" (such as to ensure the
//          printf format string has the right number of arguments).
//          DO NOT pass in long "text".
// ----------------------------------------------------------------------
int CountSubstring(StringPiece text, StringPiece substring) {
    CHECK_GT(substring.length(), 0);

    int count = 0;
    StringPiece::size_type curr = 0;
    while (StringPiece::npos != (curr = text.find(substring, curr))) {
        ++count;
        ++curr;
    }
    return count;
}

// ----------------------------------------------------------------------
// strstr_delimited()
//    Just like strstr(), except it ensures that the needle appears as
//    a complete item (or consecutive series of items) in a delimited
//    list.
//
//    Like strstr(), returns haystack if needle is empty, or NULL if
//    either needle/haystack is NULL.
// ----------------------------------------------------------------------
const char* strstr_delimited(const char* haystack, const char* needle, char delim) {
    if (!needle || !haystack) return nullptr;
    if (*needle == '\0') return haystack;

    int needle_len = strlen(needle);

    while (true) {
        // Skip any leading delimiters.
        while (*haystack == delim) ++haystack;

        // Walk down the haystack, matching every character in the needle.
        const char* this_match = haystack;
        int i = 0;
        for (; i < needle_len; i++) {
            if (*haystack != needle[i]) {
                // We ran out of haystack or found a non-matching character.
                break;
            }
            ++haystack;
        }

        // If we matched the whole needle, ensure that it's properly delimited.
        if (i == needle_len && (*haystack == '\0' || *haystack == delim)) {
            return this_match;
        }

        // No match. Consume non-delimiter characters until we run out of them.
        while (*haystack != delim) {
            if (*haystack == '\0') return nullptr;
            ++haystack;
        }
    }
    LOG(FATAL) << "Unreachable statement";
    return nullptr;
}

// ----------------------------------------------------------------------
// Older versions of libc have a buggy strsep.
// ----------------------------------------------------------------------

char* gstrsep(char** stringp, const char* delim) {
    char* s;
    const char* spanp;
    int c, sc;
    char* tok;

    if ((s = *stringp) == nullptr) return nullptr;

    tok = s;
    while (true) {
        c = *s++;
        spanp = delim;
        do {
            if ((sc = *spanp++) == c) {
                if (c == 0)
                    s = nullptr;
                else
                    s[-1] = 0;
                *stringp = s;
                return tok;
            }
        } while (sc != 0);
    }

    return nullptr; /* should not happen */
}

void FastStringAppend(string* s, const char* data, int len) {
    STLAppendToString(s, data, len);
}

// TODO(user): add a microbenchmark and revisit
// the optimizations done here.
//
// Several converters use this table to reduce
// division and modulo operations.
extern const char two_ASCII_digits[100][2];

const char two_ASCII_digits[100][2] = {
        {'0', '0'}, {'0', '1'}, {'0', '2'}, {'0', '3'}, {'0', '4'}, {'0', '5'}, {'0', '6'},
        {'0', '7'}, {'0', '8'}, {'0', '9'}, {'1', '0'}, {'1', '1'}, {'1', '2'}, {'1', '3'},
        {'1', '4'}, {'1', '5'}, {'1', '6'}, {'1', '7'}, {'1', '8'}, {'1', '9'}, {'2', '0'},
        {'2', '1'}, {'2', '2'}, {'2', '3'}, {'2', '4'}, {'2', '5'}, {'2', '6'}, {'2', '7'},
        {'2', '8'}, {'2', '9'}, {'3', '0'}, {'3', '1'}, {'3', '2'}, {'3', '3'}, {'3', '4'},
        {'3', '5'}, {'3', '6'}, {'3', '7'}, {'3', '8'}, {'3', '9'}, {'4', '0'}, {'4', '1'},
        {'4', '2'}, {'4', '3'}, {'4', '4'}, {'4', '5'}, {'4', '6'}, {'4', '7'}, {'4', '8'},
        {'4', '9'}, {'5', '0'}, {'5', '1'}, {'5', '2'}, {'5', '3'}, {'5', '4'}, {'5', '5'},
        {'5', '6'}, {'5', '7'}, {'5', '8'}, {'5', '9'}, {'6', '0'}, {'6', '1'}, {'6', '2'},
        {'6', '3'}, {'6', '4'}, {'6', '5'}, {'6', '6'}, {'6', '7'}, {'6', '8'}, {'6', '9'},
        {'7', '0'}, {'7', '1'}, {'7', '2'}, {'7', '3'}, {'7', '4'}, {'7', '5'}, {'7', '6'},
        {'7', '7'}, {'7', '8'}, {'7', '9'}, {'8', '0'}, {'8', '1'}, {'8', '2'}, {'8', '3'},
        {'8', '4'}, {'8', '5'}, {'8', '6'}, {'8', '7'}, {'8', '8'}, {'8', '9'}, {'9', '0'},
        {'9', '1'}, {'9', '2'}, {'9', '3'}, {'9', '4'}, {'9', '5'}, {'9', '6'}, {'9', '7'},
        {'9', '8'}, {'9', '9'}};

static inline void PutTwoDigits(int i, char* p) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, 100);
    p[0] = two_ASCII_digits[i][0];
    p[1] = two_ASCII_digits[i][1];
}

char* FastTimeToBuffer(time_t s, char* buffer) {
    if (s == 0) {
        time(&s);
    }

    struct tm tm;
    if (PortableSafeGmtime(&s, &tm) == nullptr) {
        // Error message must fit in 30-char buffer.
        memcpy(buffer, "Invalid:", sizeof("Invalid:"));
        FastInt64ToBufferLeft(s, buffer + strlen(buffer));
        return buffer;
    }

    // strftime format: "%a, %d %b %Y %H:%M:%S GMT",
    // but strftime does locale stuff which we do not want
    // plus strftime takes > 10x the time of hard code

    const char* weekday_name = "Xxx";
    switch (tm.tm_wday) {
    default: {
        DLOG(FATAL) << "tm.tm_wday: " << tm.tm_wday;
    } break;
    case 0:
        weekday_name = "Sun";
        break;
    case 1:
        weekday_name = "Mon";
        break;
    case 2:
        weekday_name = "Tue";
        break;
    case 3:
        weekday_name = "Wed";
        break;
    case 4:
        weekday_name = "Thu";
        break;
    case 5:
        weekday_name = "Fri";
        break;
    case 6:
        weekday_name = "Sat";
        break;
    }

    const char* month_name = "Xxx";
    switch (tm.tm_mon) {
    default: {
        DLOG(FATAL) << "tm.tm_mon: " << tm.tm_mon;
    } break;
    case 0:
        month_name = "Jan";
        break;
    case 1:
        month_name = "Feb";
        break;
    case 2:
        month_name = "Mar";
        break;
    case 3:
        month_name = "Apr";
        break;
    case 4:
        month_name = "May";
        break;
    case 5:
        month_name = "Jun";
        break;
    case 6:
        month_name = "Jul";
        break;
    case 7:
        month_name = "Aug";
        break;
    case 8:
        month_name = "Sep";
        break;
    case 9:
        month_name = "Oct";
        break;
    case 10:
        month_name = "Nov";
        break;
    case 11:
        month_name = "Dec";
        break;
    }

    // Write out the buffer.

    memcpy(buffer + 0, weekday_name, 3);
    buffer[3] = ',';
    buffer[4] = ' ';

    PutTwoDigits(tm.tm_mday, buffer + 5);
    buffer[7] = ' ';

    memcpy(buffer + 8, month_name, 3);
    buffer[11] = ' ';

    int32 year = tm.tm_year + 1900;
    PutTwoDigits(year / 100, buffer + 12);
    PutTwoDigits(year % 100, buffer + 14);
    buffer[16] = ' ';

    PutTwoDigits(tm.tm_hour, buffer + 17);
    buffer[19] = ':';

    PutTwoDigits(tm.tm_min, buffer + 20);
    buffer[22] = ':';

    PutTwoDigits(tm.tm_sec, buffer + 23);

    // includes ending NUL
    memcpy(buffer + 25, " GMT", 5);

    return buffer;
}

// ----------------------------------------------------------------------
// strdup_with_new()
// strndup_with_new()
//
//    strdup_with_new() is the same as strdup() except that the memory
//    is allocated by new[] and hence an exception will be generated
//    if out of memory.
//
//    strndup_with_new() is the same as strdup_with_new() except that it will
//    copy up to the specified number of characters.  This function
//    is useful when we want to copy a substring out of a string
//    and didn't want to (or cannot) modify the string
// ----------------------------------------------------------------------
char* strdup_with_new(const char* the_string) {
    if (the_string == nullptr)
        return nullptr;
    else
        return strndup_with_new(the_string, strlen(the_string));
}

char* strndup_with_new(const char* the_string, int max_length) {
    if (the_string == nullptr) return nullptr;
    size_t str_len = strlen(the_string);
    if (str_len > max_length) {
        str_len = max_length;
    }
    auto result = new char[str_len + 1];
    result[max_length] = '\0'; // truncated string might not have \0 at end
    memcpy(result, the_string, str_len);
    return result;
}

// ----------------------------------------------------------------------
// ScanForFirstWord()
//    This function finds the first word in the string "the_string" given.
//    A word is defined by consecutive !ascii_isspace() characters.
//    If no valid words are found,
//        return NULL and *end_ptr will contain junk
//    else
//        return the beginning of the first word and
//        *end_ptr will store the address of the first invalid character
//        (ascii_isspace() or '\0').
//
//    Precondition: (end_ptr != NULL)
// ----------------------------------------------------------------------
const char* ScanForFirstWord(const char* the_string, const char** end_ptr) {
    CHECK(end_ptr != nullptr) << ": precondition violated";

    if (the_string == nullptr) // empty string
        return nullptr;

    const char* curr = the_string;
    while ((*curr != '\0') && ascii_isspace(*curr)) // skip initial spaces
        ++curr;

    if (*curr == '\0') // no valid word found
        return nullptr;

    // else has a valid word
    const char* first_word = curr;

    // now locate the end of the word
    while ((*curr != '\0') && !ascii_isspace(*curr)) ++curr;

    *end_ptr = curr;
    return first_word;
}

// ----------------------------------------------------------------------
// AdvanceIdentifier()
//    This function returns a pointer past the end of the longest C-style
//    identifier that is a prefix of str or NULL if str does not start with
//    one.  A C-style identifier begins with an ASCII letter or underscore
//    and continues with ASCII letters, digits, or underscores.
// ----------------------------------------------------------------------
const char* AdvanceIdentifier(const char* str) {
    // Not using isalpha and isalnum so as not to rely on the locale.
    // We could have used ascii_isalpha and ascii_isalnum.
    char ch = *str++;
    if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_')) return nullptr;
    while (true) {
        ch = *str;
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
              ch == '_'))
            return str;
        str++;
    }
}

// ----------------------------------------------------------------------
// IsIdentifier()
//    This function returns true if str is a C-style identifier.
//    A C-style identifier begins with an ASCII letter or underscore
//    and continues with ASCII letters, digits, or underscores.
// ----------------------------------------------------------------------
bool IsIdentifier(const char* str) {
    const char* end = AdvanceIdentifier(str);
    return end && *end == '\0';
}

static bool IsWildcard(Rune character) {
    return character == '*' || character == '?';
}

// Move the strings pointers to the point where they start to differ.
template <typename CHAR, typename NEXT>
static void EatSameChars(const CHAR** pattern, const CHAR* pattern_end, const CHAR** string,
                         const CHAR* string_end, NEXT next) {
    const CHAR* escape = nullptr;
    while (*pattern != pattern_end && *string != string_end) {
        if (!escape && IsWildcard(**pattern)) {
            // We don't want to match wildcard here, except if it's escaped.
            return;
        }

        // Check if the escapement char is found. If so, skip it and move to the
        // next character.
        if (!escape && **pattern == '\\') {
            escape = *pattern;
            next(pattern, pattern_end);
            continue;
        }

        // Check if the chars match, if so, increment the ptrs.
        const CHAR* pattern_next = *pattern;
        const CHAR* string_next = *string;
        Rune pattern_char = next(&pattern_next, pattern_end);
        if (pattern_char == next(&string_next, string_end) && pattern_char != Runeerror &&
            pattern_char <= Runemax) {
            *pattern = pattern_next;
            *string = string_next;
        } else {
            // Uh ho, it did not match, we are done. If the last char was an
            // escapement, that means that it was an error to advance the ptr here,
            // let's put it back where it was. This also mean that the MatchPattern
            // function will return false because if we can't match an escape char
            // here, then no one will.
            if (escape) {
                *pattern = escape;
            }
            return;
        }

        escape = nullptr;
    }
}

template <typename CHAR, typename NEXT>
static void EatWildcard(const CHAR** pattern, const CHAR* end, NEXT next) {
    while (*pattern != end) {
        if (!IsWildcard(**pattern)) return;
        next(pattern, end);
    }
}

template <typename CHAR, typename NEXT>
static bool MatchPatternT(const CHAR* eval, const CHAR* eval_end, const CHAR* pattern,
                          const CHAR* pattern_end, int depth, NEXT next) {
    const int kMaxDepth = 16;
    if (depth > kMaxDepth) return false;

    // Eat all the matching chars.
    EatSameChars(&pattern, pattern_end, &eval, eval_end, next);

    // If the string is empty, then the pattern must be empty too, or contains
    // only wildcards.
    if (eval == eval_end) {
        EatWildcard(&pattern, pattern_end, next);
        return pattern == pattern_end;
    }

    // Pattern is empty but not string, this is not a match.
    if (pattern == pattern_end) return false;

    // If this is a question mark, then we need to compare the rest with
    // the current string or the string with one character eaten.
    const CHAR* next_pattern = pattern;
    next(&next_pattern, pattern_end);
    if (pattern[0] == '?') {
        if (MatchPatternT(eval, eval_end, next_pattern, pattern_end, depth + 1, next)) return true;
        const CHAR* next_eval = eval;
        next(&next_eval, eval_end);
        if (MatchPatternT(next_eval, eval_end, next_pattern, pattern_end, depth + 1, next))
            return true;
    }

    // This is a *, try to match all the possible substrings with the remainder
    // of the pattern.
    if (pattern[0] == '*') {
        // Collapse duplicate wild cards (********** into *) so that the
        // method does not recurse unnecessarily. http://crbug.com/52839
        EatWildcard(&next_pattern, pattern_end, next);

        while (eval != eval_end) {
            if (MatchPatternT(eval, eval_end, next_pattern, pattern_end, depth + 1, next))
                return true;
            eval++;
        }

        // We reached the end of the string, let see if the pattern contains only
        // wildcards.
        if (eval == eval_end) {
            EatWildcard(&pattern, pattern_end, next);
            if (pattern != pattern_end) return false;
            return true;
        }
    }

    return false;
}

struct NextCharUTF8 {
    Rune operator()(const char** p, const char* end) {
        Rune c;
        int offset = charntorune(&c, *p, static_cast<int>(end - *p));
        *p += offset;
        return c;
    }
};

bool MatchPattern(const StringPiece& eval, const StringPiece& pattern) {
    return MatchPatternT(eval.data(), eval.data() + eval.size(), pattern.data(),
                         pattern.data() + pattern.size(), 0, NextCharUTF8());
}

// ----------------------------------------------------------------------
// FindTagValuePair
//    Given a string of the form
//    <something><attr_sep><tag><tag_value_sep><value><attr_sep>...<string_term>
//    where the part before the first attr_sep is optional,
//    this function extracts the first tag and value, if any.
//    The function returns true if successful, in which case "tag" and "value"
//    are set to point to the beginning of the tag and the value, respectively,
//    and "tag_len" and "value_len" are set to the respective lengths.
// ----------------------------------------------------------------------

bool FindTagValuePair(const char* arg_str, char tag_value_separator, char attribute_separator,
                      char string_terminal, char** tag, int* tag_len, char** value,
                      int* value_len) {
    char* in_str = const_cast<char*>(arg_str); // For msvc8.
    if (in_str == nullptr) return false;
    char tv_sep_or_term[3] = {tag_value_separator, string_terminal, '\0'};
    char attr_sep_or_term[3] = {attribute_separator, string_terminal, '\0'};

    // Look for beginning of tag
    *tag = strpbrk(in_str, attr_sep_or_term);
    // If string_terminal is '\0', strpbrk won't find it but return null.
    if (*tag == nullptr || **tag == string_terminal)
        *tag = in_str;
    else
        (*tag)++; // Move past separator
    // Now look for value...
    char* tv_sep_pos = strpbrk(*tag, tv_sep_or_term);
    if (tv_sep_pos == nullptr || *tv_sep_pos == string_terminal) return false;
    // ...and end of value
    char* attr_sep_pos = strpbrk(tv_sep_pos, attr_sep_or_term);

    *tag_len = tv_sep_pos - *tag;
    *value = tv_sep_pos + 1;
    if (attr_sep_pos != nullptr)
        *value_len = attr_sep_pos - *value;
    else
        *value_len = strlen(*value);
    return true;
}

void UniformInsertString(string* s, int interval, const char* separator) {
    const size_t separator_len = strlen(separator);

    if (interval < 1 ||     // invalid interval
        s->empty() ||       // nothing to do
        separator_len == 0) // invalid separator
        return;

    int num_inserts = (s->size() - 1) / interval; // -1 to avoid appending at end
    if (num_inserts == 0)                         // nothing to do
        return;

    string tmp;
    tmp.reserve(s->size() + num_inserts * separator_len + 1);

    for (int i = 0; i < num_inserts; ++i) {
        // append this interval
        tmp.append(*s, i * interval, interval);
        // append a separator
        tmp.append(separator, separator_len);
    }

    // append the tail
    const size_t tail_pos = num_inserts * interval;
    tmp.append(*s, tail_pos, s->size() - tail_pos);

    s->swap(tmp);
}

void InsertString(string* const s, const vector<uint32>& indices, char const* const separator) {
    const unsigned num_indices(indices.size());
    if (num_indices == 0) {
        return; // nothing to do...
    }

    const unsigned separator_len(strlen(separator));
    if (separator_len == 0) {
        return; // still nothing to do...
    }

    string tmp;
    const unsigned s_len(s->size());
    tmp.reserve(s_len + separator_len * num_indices);

    vector<uint32>::const_iterator const ind_end(indices.end());
    auto ind_pos(indices.begin());

    uint32 last_pos(0);
    while (ind_pos != ind_end) {
        const uint32 pos(*ind_pos);
        DCHECK_GE(pos, last_pos);
        DCHECK_LE(pos, s_len);

        tmp.append(s->substr(last_pos, pos - last_pos));
        tmp.append(separator);

        last_pos = pos;
        ++ind_pos;
    }
    tmp.append(s->substr(last_pos));

    s->swap(tmp);
}

//------------------------------------------------------------------------
// FindNth()
//  return index of nth occurrence of c in the string,
//  or string::npos if n > number of occurrences of c.
//  (returns string::npos = -1 if n <= 0)
//------------------------------------------------------------------------
int FindNth(StringPiece s, char c, int n) {
    size_t pos = string::npos;

    for (int i = 0; i < n; ++i) {
        pos = s.find_first_of(c, pos + 1);
        if (pos == StringPiece::npos) {
            break;
        }
    }
    return pos;
}

//------------------------------------------------------------------------
// ReverseFindNth()
//  return index of nth-to-last occurrence of c in the string,
//  or string::npos if n > number of occurrences of c.
//  (returns string::npos if n <= 0)
//------------------------------------------------------------------------
int ReverseFindNth(StringPiece s, char c, int n) {
    if (n <= 0) {
        return static_cast<int>(StringPiece::npos);
    }

    size_t pos = s.size();

    for (int i = 0; i < n; ++i) {
        // If pos == 0, we return StringPiece::npos right away. Otherwise,
        // the following find_last_of call would take (pos - 1) as string::npos,
        // which means it would again search the entire input string.
        if (pos == 0) {
            return static_cast<int>(StringPiece::npos);
        }
        pos = s.find_last_of(c, pos - 1);
        if (pos == string::npos) {
            break;
        }
    }
    return pos;
}

namespace strings {

// FindEol()
// Returns the location of the next end-of-line sequence.

StringPiece FindEol(StringPiece s) {
    for (size_t i = 0; i < s.length(); ++i) {
        if (s[i] == '\n') {
            return StringPiece(s.data() + i, 1);
        }
        if (s[i] == '\r') {
            if (i + 1 < s.length() && s[i + 1] == '\n') {
                return StringPiece(s.data() + i, 2);
            } else {
                return StringPiece(s.data() + i, 1);
            }
        }
    }
    return StringPiece(s.data() + s.length(), 0);
}

} // namespace strings

//------------------------------------------------------------------------
// OnlyWhitespace()
//  return true if string s contains only whitespace characters
//------------------------------------------------------------------------
bool OnlyWhitespace(const StringPiece& s) {
    for (const auto& c : s) {
        if (!ascii_isspace(c)) return false;
    }
    return true;
}

string PrefixSuccessor(const StringPiece& prefix) {
    // We can increment the last character in the string and be done
    // unless that character is 255, in which case we have to erase the
    // last character and increment the previous character, unless that
    // is 255, etc. If the string is empty or consists entirely of
    // 255's, we just return the empty string.
    bool done = false;
    string limit(prefix.data(), prefix.size());
    int index = limit.length() - 1;
    while (!done && index >= 0) {
        if (limit[index] == 255) {
            limit.erase(index);
            index--;
        } else {
            limit[index]++;
            done = true;
        }
    }
    if (!done) {
        return "";
    } else {
        return limit;
    }
}

string ImmediateSuccessor(const StringPiece& s) {
    // Return the input string, with an additional NUL byte appended.
    string out;
    out.reserve(s.size() + 1);
    out.append(s.data(), s.size());
    out.push_back('\0');
    return out;
}

void FindShortestSeparator(const StringPiece& start, const StringPiece& limit, string* separator) {
    // Find length of common prefix
    size_t min_length = min(start.size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) && (start[diff_index] == limit[diff_index])) {
        diff_index++;
    }

    if (diff_index >= min_length) {
        // Handle the case where either string is a prefix of the other
        // string, or both strings are identical.
        start.CopyToString(separator);
        return;
    }

    if (diff_index + 1 == start.size()) {
        // If the first difference is in the last character, do not bother
        // incrementing that character since the separator will be no
        // shorter than "start".
        start.CopyToString(separator);
        return;
    }

    if (start[diff_index] == 0xff) {
        // Avoid overflow when incrementing start[diff_index]
        start.CopyToString(separator);
        return;
    }

    separator->assign(start.data(), diff_index);
    separator->push_back(start[diff_index] + 1);
    if (*separator >= limit) {
        // Never pick a separator that causes confusion with "limit"
        start.CopyToString(separator);
    }
}

int SafeSnprintf(char* str, size_t size, const char* format, ...) {
    va_list printargs;
    va_start(printargs, format);
    int ncw = vsnprintf(str, size, format, printargs);
    va_end(printargs);
    return (ncw < size && ncw >= 0) ? ncw : 0;
}

bool GetlineFromStdioFile(FILE* file, string* str, char delim) {
    str->erase();
    while (true) {
        if (feof(file) || ferror(file)) {
            return false;
        }
        int c = getc(file);
        if (c == EOF) return false;
        if (c == delim) return true;
        str->push_back(c);
    }
}

namespace {

template <typename CHAR>
size_t lcpyT(CHAR* dst, const CHAR* src, size_t dst_size) {
    for (size_t i = 0; i < dst_size; ++i) {
        if ((dst[i] = src[i]) == 0) // We hit and copied the terminating NULL.
            return i;
    }

    // We were left off at dst_size.  We over copied 1 byte.  Null terminate.
    if (dst_size != 0) dst[dst_size - 1] = 0;

    // Count the rest of the |src|, and return it's length in characters.
    while (src[dst_size]) ++dst_size;
    return dst_size;
}

} // namespace

size_t strings::strlcpy(char* dst, const char* src, size_t dst_size) {
    return lcpyT<char>(dst, src, dst_size);
}
