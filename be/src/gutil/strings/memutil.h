//
// Copyright (C) 2001 and onwards Google, Inc.
//
// (Please see comments in strutil.h near the include of <asm/string.h>
//  if you feel compelled to try to provide more efficient implementations
//  of these routines.)
//
// These routines provide mem versions of standard C string routines,
// such a strpbrk.  They function exactly the same as the str version,
// so if you wonder what they are, replace the word "mem" by
// "str" and check out the man page.  I could return void*, as the
// strutil.h mem*() routines tend to do, but I return char* instead
// since this is by far the most common way these functions are called.
//
// The difference between the mem and str versions is the mem version
// takes a pointer and a length, rather than a NULL-terminated string.
// The memcase* routines defined here assume the locale is "C"
// (they use ascii_tolower instead of tolower).
//
// These routines are based on the BSD library.
//
// Here's a list of routines from string.h, and their mem analogues.
// Functions in lowercase are defined in string.h; those in UPPERCASE
// are defined here:
//
// strlen                  --
// strcat strncat          MEMCAT
// strcpy strncpy          memcpy
// --                      memccpy   (very cool function, btw)
// --                      memmove
// --                      memset
// strcmp strncmp          memcmp
// strcasecmp strncasecmp  MEMCASECMP
// strchr                  memchr
// strcoll                 --
// strxfrm                 --
// strdup strndup          MEMDUP
// strrchr                 MEMRCHR
// strspn                  MEMSPN
// strcspn                 MEMCSPN
// strpbrk                 MEMPBRK
// strstr                  MEMSTR MEMMEM
// (g)strcasestr           MEMCASESTR MEMCASEMEM
// strtok                  --
// strprefix               MEMPREFIX      (strprefix is from strutil.h)
// strcaseprefix           MEMCASEPREFIX  (strcaseprefix is from strutil.h)
// strsuffix               MEMSUFFIX      (strsuffix is from strutil.h)
// strcasesuffix           MEMCASESUFFIX  (strcasesuffix is from strutil.h)
// --                      MEMIS
// --                      MEMCASEIS
// strcount                MEMCOUNT       (strcount is from strutil.h)

#ifndef STRINGS_MEMUTIL_H_
#define STRINGS_MEMUTIL_H_

#include <stddef.h>
#include <string.h> // to get the POSIX mem*() routines

inline char* memcat(char* dest, size_t destlen, const char* src, size_t srclen) {
    return reinterpret_cast<char*>(memcpy(dest + destlen, src, srclen));
}

int memcasecmp(const char* s1, const char* s2, size_t len);
char* memdup(const char* s, size_t slen);
char* memrchr(const char* s, int c, size_t slen);
size_t memspn(const char* s, size_t slen, const char* accept);
size_t memcspn(const char* s, size_t slen, const char* reject);
char* mempbrk(const char* s, size_t slen, const char* accept);

// This is for internal use only.  Don't call this directly
template <bool case_sensitive>
const char* int_memmatch(const char* phaystack, size_t haylen, const char* pneedle, size_t neelen);

// These are the guys you can call directly
inline const char* memstr(const char* phaystack, size_t haylen, const char* pneedle) {
    return int_memmatch<true>(phaystack, haylen, pneedle, strlen(pneedle));
}

inline const char* memcasestr(const char* phaystack, size_t haylen, const char* pneedle) {
    return int_memmatch<false>(phaystack, haylen, pneedle, strlen(pneedle));
}

inline const char* memmem(const char* phaystack, size_t haylen, const char* pneedle,
                          size_t needlelen) {
    return int_memmatch<true>(phaystack, haylen, pneedle, needlelen);
}

inline const char* memcasemem(const char* phaystack, size_t haylen, const char* pneedle,
                              size_t needlelen) {
    return int_memmatch<false>(phaystack, haylen, pneedle, needlelen);
}

// This is significantly faster for case-sensitive matches with very
// few possible matches.  See unit test for benchmarks.
const char* memmatch(const char* phaystack, size_t haylen, const char* pneedle, size_t neelen);

// The ""'s catch people who don't pass in a literal for "str"
#define strliterallen(str) (sizeof("" str "") - 1)

// Must use a string literal for prefix.
#define memprefix(str, len, prefix)                                                        \
    ((((len) >= strliterallen(prefix)) && memcmp(str, prefix, strliterallen(prefix)) == 0) \
             ? str + strliterallen(prefix)                                                 \
             : NULL)

#define memcaseprefix(str, len, prefix)                                                        \
    ((((len) >= strliterallen(prefix)) && memcasecmp(str, prefix, strliterallen(prefix)) == 0) \
             ? str + strliterallen(prefix)                                                     \
             : NULL)

// Must use a string literal for suffix.
#define memsuffix(str, len, suffix)                                                  \
    ((((len) >= strliterallen(suffix)) &&                                            \
      memcmp(str + (len)-strliterallen(suffix), suffix, strliterallen(suffix)) == 0) \
             ? str + (len)-strliterallen(suffix)                                     \
             : NULL)

#define memcasesuffix(str, len, suffix)                                                  \
    ((((len) >= strliterallen(suffix)) &&                                                \
      memcasecmp(str + (len)-strliterallen(suffix), suffix, strliterallen(suffix)) == 0) \
             ? str + (len)-strliterallen(suffix)                                         \
             : NULL)

#define memis(str, len, literal) \
    ((((len) == strliterallen(literal)) && memcmp(str, literal, strliterallen(literal)) == 0))

#define memcaseis(str, len, literal) \
    ((((len) == strliterallen(literal)) && memcasecmp(str, literal, strliterallen(literal)) == 0))

inline int memcount(const char* buf, size_t len, char c) {
    int num = 0;
    for (int i = 0; i < len; i++) {
        if (buf[i] == c) num++;
    }
    return num;
}

#endif // STRINGS_MEMUTIL_H_
