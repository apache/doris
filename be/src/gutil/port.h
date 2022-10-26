//
// Copyright (C) 1999 and onwards Google, Inc.
//
//
// These are weird things we need to do to get this compiling on
// random systems (and on SWIG).

#pragma once

#include <limits.h> // So we can set the bounds of our types
#include <stdlib.h> // for free()
#include <string.h> // for memcpy()

#if defined(__APPLE__)
// for getpagesize() on mac
#ifndef _POSIX_C_SOURCE
#include <unistd.h>
#else
#include <mach/vm_page_size.h>
#endif

#elif defined(OS_CYGWIN)
#include <malloc.h> // for memalign()
#endif

#include "gutil/integral_types.h"

// Must happens before inttypes.h inclusion */
#if defined(__APPLE__)
/* From MacOSX's inttypes.h:
 * "C++ implementations should define these macros only when
 *  __STDC_FORMAT_MACROS is defined before <inttypes.h> is included." */
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif /* __STDC_FORMAT_MACROS */
#endif /* __APPLE__ */

/* Default for most OSes */
/* We use SIGPWR since that seems unlikely to be used for other reasons. */
#define GOOGLE_OBSCURE_SIGNAL SIGPWR

#if defined OS_LINUX || defined OS_CYGWIN

// _BIG_ENDIAN
#include <endian.h>

// The uint mess:
// mysql.h sets _GNU_SOURCE which sets __USE_MISC in <features.h>
// sys/types.h typedefs uint if __USE_MISC
// mysql typedefs uint if HAVE_UINT not set
// The following typedef is carefully considered, and should not cause
//  any clashes
#if !defined(__USE_MISC)
#if !defined(HAVE_UINT)
#define HAVE_UINT 1
typedef unsigned int uint;
#endif
#if !defined(HAVE_USHORT)
#define HAVE_USHORT 1
typedef unsigned short ushort;
#endif
#if !defined(HAVE_ULONG)
#define HAVE_ULONG 1
typedef unsigned long ulong;
#endif
#endif

#if defined(__cplusplus)
#include <cstddef> // For _GLIBCXX macros
#endif

#if !defined(HAVE_TLS) && defined(_GLIBCXX_HAVE_TLS) && defined(__x86_64__)
#define HAVE_TLS 1
#endif

#elif defined OS_FREEBSD

// _BIG_ENDIAN
#include <machine/endian.h>

#elif defined OS_SOLARIS

// _BIG_ENDIAN
#include <sys/isa_defs.h>

// Solaris doesn't define sig_t (function taking an int, returning void)
typedef void (*sig_t)(int);

// Solaris only defines strtoll, not strtoq
#define strtoq strtoll
#define strtouq strtoull

// It doesn't define the posix-standard(?) u_int_16
#include <sys/int_types.h> // NOLINT(build/include)
typedef uint16_t u_int16_t;

#elif defined __APPLE__

// BIG_ENDIAN
#include <machine/endian.h> // NOLINT(build/include)
/* Let's try and follow the Linux convention */
#define __BYTE_ORDER BYTE_ORDER
#define __LITTLE_ENDIAN LITTLE_ENDIAN
#define __BIG_ENDIAN BIG_ENDIAN

#endif

// The following guarenty declaration of the byte swap functions, and
// define __BYTE_ORDER for MSVC
#ifdef _MSC_VER
#include <stdlib.h> // NOLINT(build/include)
#define __BYTE_ORDER __LITTLE_ENDIAN
#define bswap_16(x) _byteswap_ushort(x)
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)

#elif defined(__APPLE__)
// Mac OS X / Darwin features
#include <libkern/OSByteOrder.h>
#define bswap_16(x) OSSwapInt16(x)
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)

#elif defined(__GLIBC__)
#include <byteswap.h> // IWYU pragma: export

#else

static inline uint16 bswap_16(uint16 x) {
    return ((x & 0xFF) << 8) | ((x & 0xFF00) >> 8);
}
#define bswap_16(x) bswap_16(x)
static inline uint32 bswap_32(uint32 x) {
    return (((x & 0xFF) << 24) | ((x & 0xFF00) << 8) | ((x & 0xFF0000) >> 8) |
            ((x & 0xFF000000) >> 24));
}
#define bswap_32(x) bswap_32(x)
static inline uint64 bswap_64(uint64 x) {
    return (((x & GG_ULONGLONG(0xFF)) << 56) | ((x & GG_ULONGLONG(0xFF00)) << 40) |
            ((x & GG_ULONGLONG(0xFF0000)) << 24) | ((x & GG_ULONGLONG(0xFF000000)) << 8) |
            ((x & GG_ULONGLONG(0xFF00000000)) >> 8) | ((x & GG_ULONGLONG(0xFF0000000000)) >> 24) |
            ((x & GG_ULONGLONG(0xFF000000000000)) >> 40) |
            ((x & GG_ULONGLONG(0xFF00000000000000)) >> 56));
}
#define bswap_64(x) bswap_64(x)

#endif

// define the macros IS_LITTLE_ENDIAN or IS_BIG_ENDIAN
// using the above endian defintions from endian.h if
// endian.h was included
#ifdef __BYTE_ORDER
#if __BYTE_ORDER == __LITTLE_ENDIAN
#define IS_LITTLE_ENDIAN
#endif

#if __BYTE_ORDER == __BIG_ENDIAN
#define IS_BIG_ENDIAN
#endif

#else

#if defined(__LITTLE_ENDIAN__)
#define IS_LITTLE_ENDIAN
#elif defined(__BIG_ENDIAN__)
#define IS_BIG_ENDIAN
#endif

// there is also PDP endian ...

#endif // __BYTE_ORDER

// Define the OS's path separator
#ifdef __cplusplus // C won't merge duplicate const variables at link time
// Some headers provide a macro for this (GCC's system.h), remove it so that we
// can use our own.
#undef PATH_SEPARATOR
#if defined(OS_WINDOWS)
const char PATH_SEPARATOR = '\\';
#else
const char PATH_SEPARATOR = '/';
#endif
#endif

// Windows has O_BINARY as a flag to open() (like "b" for fopen).
// Linux doesn't need make this distinction.
#if defined OS_LINUX && !defined O_BINARY
#define O_BINARY 0
#endif

// va_copy portability definitions
#ifdef _MSC_VER
// MSVC doesn't have va_copy yet.
// This is believed to work for 32-bit msvc.  This may not work at all for
// other platforms.
// If va_list uses the single-element-array trick, you will probably get
// a compiler error here.
//
#include <stdarg.h>
inline void va_copy(va_list& a, va_list& b) {
    a = b;
}

// Nor does it have uid_t
typedef int uid_t;

#endif

// Mac OS X / Darwin features

#if defined(__APPLE__)

// For mmap, Linux defines both MAP_ANONYMOUS and MAP_ANON and says MAP_ANON is
// deprecated. In Darwin, MAP_ANON is all there is.
#if !defined MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

// Linux has this in <sys/cdefs.h>
#define __ptr_t void*

// Linux has this in <linux/errno.h>
#define EXFULL ENOMEM // not really that great a translation...

// Darwin doesn't have strnlen. No comment.
inline size_t strnlen(const char* s, size_t maxlen) {
    const char* end = (const char*)memchr(s, '\0', maxlen);
    if (end) return end - s;
    return maxlen;
}

namespace std {}     // namespace std
using namespace std; // Just like VC++, we need a using here.

// Doesn't exist on OSX; used in google.cc for send() to mean "no flags".
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

// No SIGPWR on MacOSX.  SIGINFO seems suitably obscure.
#undef GOOGLE_OBSCURE_SIGNAL
#define GOOGLE_OBSCURE_SIGNAL SIGINFO

#elif defined(OS_CYGWIN) // Cygwin-specific behavior.

#if defined(__CYGWIN32__)
#define __WORDSIZE 32
#else
// It's probably possible to support 64-bit, but the #defines will need checked.
#error "Cygwin is currently only 32-bit."
#endif

// No signalling on Windows.
#undef GOOGLE_OBSCURE_SIGNAL
#define GOOGLE_OBSCURE_SIGNAL 0

struct stack_t {
    void* ss_sp;
    int ss_flags;
    size_t ss_size;
};
inline int sigaltstack(stack_t* ss, stack_t* oss) {
    return 0;
}

#define PTHREAD_STACK_MIN 0 // Not provided by cygwin

// Scans memory for a character.
// memrchr is used in a few places, but it's linux-specific.
inline void* memrchr(const void* bytes, int find_char, size_t len) {
    const unsigned char* cursor = reinterpret_cast<const unsigned char*>(bytes) + len - 1;
    unsigned char actual_char = find_char;
    for (; cursor >= bytes; --cursor) {
        if (*cursor == actual_char) {
            return const_cast<void*>(reinterpret_cast<const void*>(cursor));
        }
    }
    return NULL;
}

#endif

// Klocwork static analysis tool's C/C++ complier kwcc
#if defined(__KLOCWORK__)
#define STATIC_ANALYSIS
#endif // __KLOCWORK__

// Annotate a function indicating the caller must examine the return value.
// Use like:
//   int foo() WARN_UNUSED_RESULT;
// To explicitly ignore a result, see |ignore_result()| in <base/basictypes.h>.
#if defined(__GNUC__)
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#else
#define WARN_UNUSED_RESULT
#endif

// GCC-specific features

#if (defined(__GNUC__) || defined(__APPLE__)) && !defined(SWIG)

//
// Tell the compiler to do printf format string checking if the
// compiler supports it; see the 'format' attribute in
// <http://gcc.gnu.org/onlinedocs/gcc-4.3.0/gcc/Function-Attributes.html>.
//
// N.B.: As the GCC manual states, "[s]ince non-static C++ methods
// have an implicit 'this' argument, the arguments of such methods
// should be counted from two, not one."
//
#define PRINTF_ATTRIBUTE(string_index, first_to_check) \
    __attribute__((__format__(__printf__, string_index, first_to_check)))
#define SCANF_ATTRIBUTE(string_index, first_to_check) \
    __attribute__((__format__(__scanf__, string_index, first_to_check)))

//
// Prevent the compiler from padding a structure to natural alignment
//
#define PACKED __attribute__((packed))

// Cache line alignment
#if defined(__i386__) || defined(__x86_64__)
#define CACHELINE_SIZE 64
#elif defined(__powerpc64__)
// TODO(user) This is the L1 D-cache line size of our Power7 machines.
// Need to check if this is appropriate for other PowerPC64 systems.
#define CACHELINE_SIZE 128
#elif defined(__arm__)
// Cache line sizes for ARM: These values are not strictly correct since
// cache line sizes depend on implementations, not architectures.  There
// are even implementations with cache line sizes configurable at boot
// time.
#if defined(__ARM_ARCH_5T__)
#define CACHELINE_SIZE 32
#elif defined(__ARM_ARCH_7A__)
#define CACHELINE_SIZE 64
#endif
#endif

// This is a NOP if CACHELINE_SIZE is not defined.
#ifdef CACHELINE_SIZE
#define CACHELINE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#else
#define CACHELINE_ALIGNED
#endif

//
// Prevent the compiler from complaining about or optimizing away variables
// that appear unused
// (careful, others e.g. third_party/libxml/xmlversion.h also define this)
#undef ATTRIBUTE_UNUSED
#define ATTRIBUTE_UNUSED __attribute__((unused))

// Same as above, but for class members.
// As of 10/2013 this appears to only be supported in Clang/LLVM.
// See http://patchwork.ozlabs.org/patch/232594/ which is not yet committed
// in gcc trunk.
#if defined(__llvm__)
#define ATTRIBUTE_MEMBER_UNUSED ATTRIBUTE_UNUSED
#else
#define ATTRIBUTE_MEMBER_UNUSED
#endif

//
// For functions we want to force inline or not inline.
// Introduced in gcc 3.1.
#define ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))
#define HAVE_ATTRIBUTE_ALWAYS_INLINE 1
#define ATTRIBUTE_NOINLINE __attribute__((noinline))
#define HAVE_ATTRIBUTE_NOINLINE 1

// For weak functions
#undef ATTRIBUTE_WEAK
#define ATTRIBUTE_WEAK __attribute__((weak))
#define HAVE_ATTRIBUTE_WEAK 1

// For deprecated functions or variables, generate a warning at usage sites.
// Verified to work as early as GCC 3.1.1 and clang 3.2 (so we'll assume any
// clang is new enough).
#if defined(__clang__) || \
        (defined(COMPILER_GCC) && (__GNUC__ * 10000 + __GNUC_MINOR__ * 100) >= 30200)
#define ATTRIBUTE_DEPRECATED(msg) __attribute__((deprecated(msg)))
#else
#define ATTRIBUTE_DEPRECATED(msg)
#endif

// Tell the compiler to use "initial-exec" mode for a thread-local variable.
// See http://people.redhat.com/drepper/tls.pdf for the gory details.
#define ATTRIBUTE_INITIAL_EXEC __attribute__((tls_model("initial-exec")))

//
// Tell the compiler that some function parameters should be non-null pointers.
// Note: As the GCC manual states, "[s]ince non-static C++ methods
// have an implicit 'this' argument, the arguments of such methods
// should be counted from two, not one."
//
#define ATTRIBUTE_NONNULL(arg_index) __attribute__((nonnull(arg_index)))

//
// Tell the compiler that a given function never returns
//
#define ATTRIBUTE_NORETURN __attribute__((noreturn))

// Tell AddressSanitizer (or other memory testing tools) to ignore a given
// function. Useful for cases when a function reads random locations on stack,
// calls _exit from a cloned subprocess, deliberately accesses buffer
// out of bounds or does other scary things with memory.
#ifdef ADDRESS_SANITIZER
#define ATTRIBUTE_NO_ADDRESS_SAFETY_ANALYSIS __attribute__((no_address_safety_analysis))
#else
#define ATTRIBUTE_NO_ADDRESS_SAFETY_ANALYSIS
#endif

// Tell ThreadSanitizer to ignore a given function. This can dramatically reduce
// the running time and memory requirements for racy code when TSAN is active.
// GCC does not support this attribute at the time of this writing (GCC 4.8).
#if defined(__llvm__)
#define ATTRIBUTE_NO_SANITIZE_THREAD __attribute__((no_sanitize_thread))
#else
#define ATTRIBUTE_NO_SANITIZE_THREAD
#endif

#ifndef HAVE_ATTRIBUTE_SECTION // may have been pre-set to 0, e.g. for Darwin
#define HAVE_ATTRIBUTE_SECTION 1
#endif

//
// The legacy prod71 libc does not provide the stack alignment required for use
// of SSE intrinsics.  In order to properly use the intrinsics you need to use
// a trampoline function which aligns the stack prior to calling your code,
// or as of crosstool v10 with gcc 4.2.0 there is an attribute which asks
// gcc to do this for you.
//
// It has also been discovered that crosstool up to and including v10 does not
// provide proper alignment for pthread_once() functions in x86-64 code either.
// Unfortunately gcc does not provide force_align_arg_pointer as an option in
// x86-64 code, so this requires us to always have a trampoline.
//
// For an example of using this see util/hash/adler32*

#if defined(__i386__) && (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC __attribute__((force_align_arg_pointer))
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#elif defined(__i386__) || defined(__x86_64__)
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (1)
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#else
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#endif

//
// Tell the compiler to warn about unused return values for functions declared
// with this macro.  The macro should be used on function declarations
// following the argument list:
//
//   Sprocket* AllocateSprocket() MUST_USE_RESULT;
//
#if defined(SWIG)
#define MUST_USE_RESULT
#elif __GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 4)
#define MUST_USE_RESULT __attribute__((warn_unused_result))
#else
#define MUST_USE_RESULT
#endif

// Annotate a virtual method indicating it must be overriding a virtual
// method in the parent class.
// Use like:
//   virtual void foo() OVERRIDE;
#if defined(COMPILER_MSVC)
#define OVERRIDE override
#elif defined(__clang__)
#define OVERRIDE override
#elif defined(COMPILER_GCC) && __cplusplus >= 201103 && \
        (__GNUC__ * 10000 + __GNUC_MINOR__ * 100) >= 40700
// GCC 4.7 supports explicit virtual overrides when C++11 support is enabled.
#define OVERRIDE override
#else
#define OVERRIDE
#endif

// Annotate a virtual method indicating that subclasses must not override it,
// or annotate a class to indicate that it cannot be subclassed.
// Use like:
//   virtual void foo() FINAL;
//   class B FINAL : public A {};
#if defined(COMPILER_MSVC)
// TODO(jered): Change this to "final" when chromium no longer uses MSVC 2010.
#define FINAL sealed
#elif defined(__clang__)
#define FINAL final
#elif defined(COMPILER_GCC) && __cplusplus >= 201103 && \
        (__GNUC__ * 10000 + __GNUC_MINOR__ * 100) >= 40700
// GCC 4.7 supports explicit virtual overrides when C++11 support is enabled.
#define FINAL final
#else
#define FINAL
#endif

#if defined(__GNUC__)
// Defined behavior on some of the uarchs:
// PREFETCH_HINT_T0:
//   prefetch to all levels of the hierarchy (except on p4: prefetch to L2)
// PREFETCH_HINT_NTA:
//   p4: fetch to L2, but limit to 1 way (out of the 8 ways)
//   core: skip L2, go directly to L1
//   k8 rev E and later: skip L2, can go to either of the 2-ways in L1
enum PrefetchHint {
    PREFETCH_HINT_T0 = 3, // More temporal locality
    PREFETCH_HINT_T1 = 2,
    PREFETCH_HINT_T2 = 1, // Less temporal locality
    PREFETCH_HINT_NTA = 0 // No temporal locality
};
#else
// prefetch is a no-op for this target. Feel free to add more sections above.
#endif

extern inline void prefetch(const char* x, int hint) {
#if defined(__llvm__)
    // In the gcc version of prefetch(), hint is only a constant _after_ inlining
    // (assumed to have been successful).  llvm views things differently, and
    // checks constant-ness _before_ inlining.  This leads to compilation errors
    // with using the other version of this code with llvm.
    //
    // One way round this is to use a switch statement to explicitly match
    // prefetch hint enumerations, and invoke __builtin_prefetch for each valid
    // value.  llvm's optimization removes the switch and unused case statements
    // after inlining, so that this boils down in the end to the same as for gcc;
    // that is, a single inlined prefetchX instruction.
    //
    // Note that this version of prefetch() cannot verify constant-ness of hint.
    // If client code calls prefetch() with a variable value for hint, it will
    // receive the full expansion of the switch below, perhaps also not inlined.
    // This should however not be a problem in the general case of well behaved
    // caller code that uses the supplied prefetch hint enumerations.
    switch (hint) {
    case PREFETCH_HINT_T0:
        __builtin_prefetch(x, 0, PREFETCH_HINT_T0);
        break;
    case PREFETCH_HINT_T1:
        __builtin_prefetch(x, 0, PREFETCH_HINT_T1);
        break;
    case PREFETCH_HINT_T2:
        __builtin_prefetch(x, 0, PREFETCH_HINT_T2);
        break;
    case PREFETCH_HINT_NTA:
        __builtin_prefetch(x, 0, PREFETCH_HINT_NTA);
        break;
    default:
        __builtin_prefetch(x);
        break;
    }
#elif defined(__GNUC__)
#if !defined(__i386) || defined(__SSE__)
    if (__builtin_constant_p(hint)) {
        __builtin_prefetch(x, 0, hint);
    } else {
        // Defaults to PREFETCH_HINT_T0
        __builtin_prefetch(x);
    }
#else
    // We want a __builtin_prefetch, but we build with the default -march=i386
    // where __builtin_prefetch quietly turns into nothing.
    // Once we crank up to -march=pentium3 or higher the __SSE__
    // clause above will kick in with the builtin.
    // -- mec 2006-06-06
    if (hint == PREFETCH_HINT_NTA) __asm__ __volatile__("prefetchnta (%0)" : : "r"(x));
#endif
#else
    // You get no effect.  Feel free to add more sections above.
#endif
}

#ifdef __cplusplus
// prefetch intrinsic (bring data to L1 without polluting L2 cache)
extern inline void prefetch(const char* x) {
    return prefetch(x, 0);
}
#endif // ifdef __cplusplus

//
// GCC can be told that a certain branch is not likely to be taken (for
// instance, a CHECK failure), and use that information in static analysis.
// Giving it this information can help it optimize for the common case in
// the absence of better information (ie. -fprofile-arcs).
//
#if defined(__GNUC__)
#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define PREDICT_FALSE(x) x
#define PREDICT_TRUE(x) x
#endif

//
// Tell GCC that a function is hot or cold. GCC can use this information to
// improve static analysis, i.e. a conditional branch to a cold function
// is likely to be not-taken.
// This annotation is used for function declarations, e.g.:
//   int foo() ATTRIBUTE_HOT;
//
#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 3)
#define ATTRIBUTE_HOT __attribute__((hot))
#define ATTRIBUTE_COLD __attribute__((cold))
#else
#define ATTRIBUTE_HOT
#define ATTRIBUTE_COLD
#endif

#define FTELLO ftello
#define FSEEKO fseeko

#if !defined(__cplusplus) && !defined(__APPLE__) && !defined(OS_CYGWIN)
// stdlib.h only declares this in C++, not in C, so we declare it here.
// Also make sure to avoid declaring it on platforms which don't support it.
extern int posix_memalign(void** memptr, size_t alignment, size_t size);
#endif

inline void* aligned_malloc(size_t size, int minimum_alignment) {
#if defined(__APPLE__)
    // mac lacks memalign(), posix_memalign(), however, according to
    // http://stackoverflow.com/questions/196329/osx-lacks-memalign
    // mac allocs are already 16-byte aligned.
    if (minimum_alignment <= 16) return malloc(size);
    // next, try to return page-aligned memory. perhaps overkill
    #ifndef _POSIX_C_SOURCE
    int page_size = getpagesize();
    #else
    int page_size = vm_page_size;
    #endif
    if (minimum_alignment <= page_size) return valloc(size);
    // give up
    return NULL;
#elif defined(OS_CYGWIN)
    return memalign(minimum_alignment, size);
#else // !__APPLE__ && !OS_CYGWIN
    void* ptr = NULL;
    if (posix_memalign(&ptr, minimum_alignment, size) != 0)
        return NULL;
    else
        return ptr;
#endif
}

#else // not GCC

#define PRINTF_ATTRIBUTE(string_index, first_to_check)
#define SCANF_ATTRIBUTE(string_index, first_to_check)
#define PACKED
#define CACHELINE_ALIGNED
#define ATTRIBUTE_UNUSED
#define ATTRIBUTE_ALWAYS_INLINE
#define ATTRIBUTE_NOINLINE
#define ATTRIBUTE_HOT
#define ATTRIBUTE_COLD
#define ATTRIBUTE_WEAK
#define HAVE_ATTRIBUTE_WEAK 0
#define ATTRIBUTE_INITIAL_EXEC
#define ATTRIBUTE_NONNULL(arg_index)
#define ATTRIBUTE_NORETURN
#define ATTRIBUTE_STACK_ALIGN_FOR_OLD_LIBC
#define REQUIRE_STACK_ALIGN_TRAMPOLINE (0)
#define MUST_USE_RESULT
extern inline void prefetch(const char* x) {}
#define PREDICT_FALSE(x) x
#define PREDICT_TRUE(x) x

// These should be redefined appropriately if better alternatives to
// ftell/fseek exist in the compiler
#define FTELLO ftell
#define FSEEKO fseek

#endif // GCC

//
// Provides a char array with the exact same alignment as another type. The
// first parameter must be a complete type, the second parameter is how many
// of that type to provide space for.
//
//   ALIGNED_CHAR_ARRAY(struct stat, 16) storage_;
//
#if defined(__cplusplus)
#undef ALIGNED_CHAR_ARRAY
// Because MSVC and older GCCs require that the argument to their alignment
// construct to be a literal constant integer, we use a template instantiated
// at all the possible powers of two.
#ifndef SWIG
template <int alignment, int size>
struct AlignType {};
template <int size>
struct AlignType<0, size> {
    typedef char result[size];
};
#if defined(_MSC_VER)
#define BASE_PORT_H_ALIGN_ATTRIBUTE(X) __declspec(align(X))
#define BASE_PORT_H_ALIGN_OF(T) __alignof(T)
#elif defined(__GNUC__)
#define BASE_PORT_H_ALIGN_ATTRIBUTE(X) __attribute__((aligned(X)))
#define BASE_PORT_H_ALIGN_OF(T) __alignof__(T)
#endif

#if defined(BASE_PORT_H_ALIGN_ATTRIBUTE)

#define BASE_PORT_H_ALIGNTYPE_TEMPLATE(X)                         \
    template <int size>                                           \
    struct AlignType<X, size> {                                   \
        typedef BASE_PORT_H_ALIGN_ATTRIBUTE(X) char result[size]; \
    }

BASE_PORT_H_ALIGNTYPE_TEMPLATE(1);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(2);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(4);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(8);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(16);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(32);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(64);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(128);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(256);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(512);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(1024);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(2048);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(4096);
BASE_PORT_H_ALIGNTYPE_TEMPLATE(8192);
// Any larger and MSVC++ will complain.

#define ALIGNED_CHAR_ARRAY(T, Size) \
    typename AlignType<BASE_PORT_H_ALIGN_OF(T), sizeof(T) * Size>::result

#undef BASE_PORT_H_ALIGNTYPE_TEMPLATE
#undef BASE_PORT_H_ALIGN_ATTRIBUTE

#else // defined(BASE_PORT_H_ALIGN_ATTRIBUTE)
#define ALIGNED_CHAR_ARRAY you_must_define_ALIGNED_CHAR_ARRAY_for_your_compiler_in_base_port_h
#endif // defined(BASE_PORT_H_ALIGN_ATTRIBUTE)

#else // !SWIG

// SWIG can't represent alignment and doesn't care about alignment on data
// members (it works fine without it).
template <typename Size>
struct AlignType {
    typedef char result[Size];
};
#define ALIGNED_CHAR_ARRAY(T, Size) AlignType<Size * sizeof(T)>::result

#endif // !SWIG
#else // __cpluscplus
#define ALIGNED_CHAR_ARRAY ALIGNED_CHAR_ARRAY_is_not_available_without_Cplusplus
#endif // __cplusplus

#ifdef _MSC_VER /* if Visual C++ */

// This compiler flag can be easily overlooked on MSVC.
// _CHAR_UNSIGNED gets set with the /J flag.
#ifndef _CHAR_UNSIGNED
#error chars must be unsigned!  Use the /J flag on the compiler command line.
#endif

// MSVC is a little hyper-active in its warnings
// Signed vs. unsigned comparison is ok.
#pragma warning(disable : 4018)
// We know casting from a long to a char may lose data
#pragma warning(disable : 4244)
// Don't need performance warnings about converting ints to bools
#pragma warning(disable : 4800)
// Integral constant overflow is apparently ok too
// for example:
//  short k;  int n;
//  k = k + n;
#pragma warning(disable : 4307)
// It's ok to use this* in constructor
// Example:
//  class C {
//   Container cont_;
//   C() : cont_(this) { ...
#pragma warning(disable : 4355)
// Truncating from double to float is ok
#pragma warning(disable : 4305)

#include <assert.h>
#include <windows.h>
#include <winsock2.h>
#undef ERROR

#include <float.h> // for nextafter functionality on windows
#include <math.h> // for HUGE_VAL

#ifndef HUGE_VALF
#define HUGE_VALF (static_cast<float>(HUGE_VAL))
#endif

namespace std {} // namespace std
using namespace std;

// VC++ doesn't understand "uint"
#ifndef HAVE_UINT
#define HAVE_UINT 1
typedef unsigned int uint;
#endif

// VC++ doesn't understand "ssize_t"
#ifndef HAVE_SSIZET
#define HAVE_SSIZET 1
// The following correctly defines ssize_t on most (all?) VC++ versions:
//   #include <BaseTsd.h>
//   typedef SSIZE_T ssize_t;
// However, several projects in googleclient already use plain 'int', e.g.,
//   googleclient/posix/unistd.h
//   googleclient/earth/client/libs/base/types.h
// so to avoid conflicts with those definitions, we do the same here.
typedef int ssize_t;
#endif

#define strtoq _strtoi64
#define strtouq _strtoui64
#define strtoll _strtoi64
#define strtoull _strtoui64
#define atoll _atoi64

// VC++ 6 and before ship without an ostream << operator for 64-bit ints
#if (_MSC_VER <= 1200)
#include <iosfwd>
using std::ostream;
inline ostream& operator<<(ostream& os, const unsigned __int64& num) {
    // Fake operator; doesn't actually do anything.
    LOG(FATAL) << "64-bit ostream operator << not supported in VC++ 6";
    return os;
}
#endif

// You say tomato, I say atotom
#define PATH_MAX MAX_PATH

// You say tomato, I say _tomato
#define vsnprintf _vsnprintf
#define snprintf _snprintf
#define strcasecmp _stricmp
#define strncasecmp _strnicmp

#define nextafter _nextafter

#define hypot _hypot
#define hypotf _hypotf

#define strdup _strdup
#define tempnam _tempnam
#define chdir _chdir
#define getcwd _getcwd
#define putenv _putenv

// You say tomato, I say toma
#define random() rand()
#define srandom(x) srand(x)

// You say juxtapose, I say transpose
#define bcopy(s, d, n) memcpy(d, s, n)

inline void* aligned_malloc(size_t size, int minimum_alignment) {
    return _aligned_malloc(size, minimum_alignment);
}

// ----- BEGIN VC++ STUBS & FAKE DEFINITIONS ---------------------------------

// See http://en.wikipedia.org/wiki/IEEE_754 for details of
// floating point format.

enum {
    FP_NAN,      //  is "Not a Number"
    FP_INFINITE, //  is either plus or minus infinity.
    FP_ZERO,
    FP_SUBNORMAL, // is too small to be represented in normalized format.
    FP_NORMAL     // if nothing of the above is correct that it must be a
                  // normal floating-point number.
};

inline int fpclassify_double(double x) {
    const int float_point_class = _fpclass(x);
    int c99_class;
    switch (float_point_class) {
    case _FPCLASS_SNAN: // Signaling NaN
    case _FPCLASS_QNAN: // Quiet NaN
        c99_class = FP_NAN;
        break;
    case _FPCLASS_NZ: // Negative zero ( -0)
    case _FPCLASS_PZ: // Positive 0 (+0)
        c99_class = FP_ZERO;
        break;
    case _FPCLASS_NINF: // Negative infinity ( -INF)
    case _FPCLASS_PINF: // Positive infinity (+INF)
        c99_class = FP_INFINITE;
        break;
    case _FPCLASS_ND: // Negative denormalized
    case _FPCLASS_PD: // Positive denormalized
        c99_class = FP_SUBNORMAL;
        break;
    case _FPCLASS_NN: // Negative normalized non-zero
    case _FPCLASS_PN: // Positive normalized non-zero
        c99_class = FP_NORMAL;
        break;
    default:
        c99_class = FP_NAN; // Should never happen
        break;
    }
    return c99_class;
}

// This function handle the special subnormal case for float; it will
// become a normal number while casting to double.
// bit_cast is avoided to simplify dependency and to create a code that is
// easy to deploy in C code
inline int fpclassify_float(float x) {
    uint32 bitwise_representation;
    memcpy(&bitwise_representation, &x, 4);
    if ((bitwise_representation & 0x7f800000) == 0 && (bitwise_representation & 0x007fffff) != 0)
        return FP_SUBNORMAL;
    return fpclassify_double(x);
}
//
// This define takes care of the denormalized float; the casting to
// double make it a normal number
#define fpclassify(x) ((sizeof(x) == sizeof(float)) ? fpclassify_float(x) : fpclassify_double(x))

#define isnan _isnan

inline int isinf(double x) {
    const int float_point_class = _fpclass(x);
    if (float_point_class == _FPCLASS_PINF) return 1;
    if (float_point_class == _FPCLASS_NINF) return -1;
    return 0;
}

// #include "conflict-signal.h"
typedef void (*sig_t)(int);

// These actually belong in errno.h but there's a name confilict in errno
// on WinNT. They (and a ton more) are also found in Winsock2.h, but
// if'd out under NT. We need this subset at minimum.
#define EXFULL ENOMEM // not really that great a translation...
// The following are already defined in VS2010.
#if (_MSC_VER < 1600)
#define EWOULDBLOCK WSAEWOULDBLOCK
#ifndef PTHREADS_REDHAT_WIN32
#define ETIMEDOUT WSAETIMEDOUT
#endif
#define ENOTSOCK WSAENOTSOCK
#define EINPROGRESS WSAEINPROGRESS
#define ECONNRESET WSAECONNRESET
#endif

//
// Really from <string.h>
//

inline void bzero(void* s, int n) {
    memset(s, 0, n);
}

// From glob.h
#define __ptr_t void*

// Defined all over the place.
typedef int pid_t;

// From stat.h
typedef unsigned int mode_t;

// u_int16_t, int16_t don't exist in MSVC
typedef unsigned short u_int16_t;
typedef short int16_t;

// ----- END VC++ STUBS & FAKE DEFINITIONS ----------------------------------

#endif // _MSC_VER

#ifdef STL_MSVC // not always the same as _MSC_VER
#include "base/port_hash.h"
#else
struct PortableHashBase {};
#endif

#if defined(OS_WINDOWS) || defined(__APPLE__)
// gethostbyname() *is* thread-safe for Windows native threads. It is also
// safe on Mac OS X, where it uses thread-local storage, even though the
// manpages claim otherwise. For details, see
// http://lists.apple.com/archives/Darwin-dev/2006/May/msg00008.html
#else
// gethostbyname() is not thread-safe.  So disallow its use.  People
// should either use the HostLookup::Lookup*() methods, or gethostbyname_r()
#define gethostbyname gethostbyname_is_not_thread_safe_DO_NOT_USE
#endif

// create macros in which the programmer should enclose all specializations
// for hash_maps and hash_sets. This is necessary since these classes are not
// STL standardized. Depending on the STL implementation they are in different
// namespaces. Right now the right namespace is passed by the Makefile
// Examples: gcc3: -DHASH_NAMESPACE=__gnu_cxx
//           icc:  -DHASH_NAMESPACE=std
//           gcc2: empty

#ifndef HASH_NAMESPACE
#define HASH_NAMESPACE_DECLARATION_START
#define HASH_NAMESPACE_DECLARATION_END
#else
#define HASH_NAMESPACE_DECLARATION_START namespace HASH_NAMESPACE {
#define HASH_NAMESPACE_DECLARATION_END }
#endif

// Our STL-like classes use __STD.
#if defined(__GNUC__) || defined(__APPLE__) || defined(_MSC_VER)
#define __STD std
#endif

#if defined __GNUC__
#define STREAM_SET(s, bit) (s).setstate(ios_base::bit)
#define STREAM_SETF(s, flag) (s).setf(ios_base::flag)
#else
#define STREAM_SET(s, bit) (s).set(ios::bit)
#define STREAM_SETF(s, flag) (s).setf(ios::flag)
#endif

// Portable handling of unaligned loads, stores, and copies.
// On some platforms, like ARM, the copy functions can be more efficient
// then a load and a store.

#if defined(__i386) || defined(ARCH_ATHLON) || defined(__x86_64__) || defined(_ARCH_PPC)

// x86 and x86-64 can perform unaligned loads/stores directly;
// modern PowerPC hardware can also do unaligned integer loads and stores;
// but note: the FPU still sends unaligned loads and stores to a trap handler!

#define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16*>(_p))
#define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32*>(_p))
#define UNALIGNED_LOAD64(_p) (*reinterpret_cast<const uint64*>(_p))

#define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16*>(_p) = (_val))
#define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32*>(_p) = (_val))
#define UNALIGNED_STORE64(_p, _val) (*reinterpret_cast<uint64*>(_p) = (_val))

#elif defined(__arm__) && !defined(__ARM_ARCH_5__) && !defined(__ARM_ARCH_5T__) &&               \
        !defined(__ARM_ARCH_5TE__) && !defined(__ARM_ARCH_5TEJ__) && !defined(__ARM_ARCH_6__) && \
        !defined(__ARM_ARCH_6J__) && !defined(__ARM_ARCH_6K__) && !defined(__ARM_ARCH_6Z__) &&   \
        !defined(__ARM_ARCH_6ZK__) && !defined(__ARM_ARCH_6T2__)

// ARMv7 and newer support native unaligned accesses, but only of 16-bit
// and 32-bit values (not 64-bit); older versions either raise a fatal signal,
// do an unaligned read and rotate the words around a bit, or do the reads very
// slowly (trip through kernel mode). There's no simple #define that says just
// “ARMv7 or higher”, so we have to filter away all ARMv5 and ARMv6
// sub-architectures. Newer gcc (>= 4.6) set an __ARM_FEATURE_ALIGNED #define,
// so in time, maybe we can move on to that.
//
// This is a mess, but there's not much we can do about it.

#define UNALIGNED_LOAD16(_p) (*reinterpret_cast<const uint16*>(_p))
#define UNALIGNED_LOAD32(_p) (*reinterpret_cast<const uint32*>(_p))

#define UNALIGNED_STORE16(_p, _val) (*reinterpret_cast<uint16*>(_p) = (_val))
#define UNALIGNED_STORE32(_p, _val) (*reinterpret_cast<uint32*>(_p) = (_val))

// TODO(user): NEON supports unaligned 64-bit loads and stores.
// See if that would be more efficient on platforms supporting it,
// at least for copies.

inline uint64 UNALIGNED_LOAD64(const void* p) {
    uint64 t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline void UNALIGNED_STORE64(void* p, uint64 v) {
    memcpy(p, &v, sizeof v);
}

#else

#define NEED_ALIGNED_LOADS

// These functions are provided for architectures that don't support
// unaligned loads and stores.

inline uint16 UNALIGNED_LOAD16(const void* p) {
    uint16 t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline uint32 UNALIGNED_LOAD32(const void* p) {
    uint32 t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline uint64 UNALIGNED_LOAD64(const void* p) {
    uint64 t;
    memcpy(&t, p, sizeof t);
    return t;
}

inline void UNALIGNED_STORE16(void* p, uint16 v) {
    memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE32(void* p, uint32 v) {
    memcpy(p, &v, sizeof v);
}

inline void UNALIGNED_STORE64(void* p, uint64 v) {
    memcpy(p, &v, sizeof v);
}

#endif

#ifdef _LP64
#define UNALIGNED_LOADW(_p) UNALIGNED_LOAD64(_p)
#define UNALIGNED_STOREW(_p, _val) UNALIGNED_STORE64(_p, _val)
#else
#define UNALIGNED_LOADW(_p) UNALIGNED_LOAD32(_p)
#define UNALIGNED_STOREW(_p, _val) UNALIGNED_STORE32(_p, _val)
#endif

// NOTE(user): These are only exported to C++ because the macros they depend on
// use C++-only syntax. This #ifdef can be removed if/when the macros are fixed.

#if defined(__cplusplus)

inline void UnalignedCopy16(const void* src, void* dst) {
    UNALIGNED_STORE16(dst, UNALIGNED_LOAD16(src));
}

inline void UnalignedCopy32(const void* src, void* dst) {
    UNALIGNED_STORE32(dst, UNALIGNED_LOAD32(src));
}

inline void UnalignedCopy64(const void* src, void* dst) {
    if (sizeof(void*) == 8) {
        UNALIGNED_STORE64(dst, UNALIGNED_LOAD64(src));
    } else {
        const char* src_char = reinterpret_cast<const char*>(src);
        char* dst_char = reinterpret_cast<char*>(dst);

        UNALIGNED_STORE32(dst_char, UNALIGNED_LOAD32(src_char));
        UNALIGNED_STORE32(dst_char + 4, UNALIGNED_LOAD32(src_char + 4));
    }
}

#endif // defined(__cpluscplus)

// printf macros for size_t, in the style of inttypes.h
#ifdef _LP64
#define __PRIS_PREFIX "z"
#else
#define __PRIS_PREFIX
#endif

// Use these macros after a % in a printf format string
// to get correct 32/64 bit behavior, like this:
// size_t size = records.size();
// printf("%" PRIuS "\n", size);

#define PRIdS __PRIS_PREFIX "d"
#define PRIxS __PRIS_PREFIX "x"
#define PRIuS __PRIS_PREFIX "u"
#define PRIXS __PRIS_PREFIX "X"
#define PRIoS __PRIS_PREFIX "o"

#define GPRIuPTHREAD "lu"
#define GPRIxPTHREAD "lx"
#ifdef OS_CYGWIN
#define PRINTABLE_PTHREAD(pthreadt) reinterpret_cast<uintptr_t>(pthreadt)
#else
#define PRINTABLE_PTHREAD(pthreadt) pthreadt
#endif

#define SIZEOF_MEMBER(t, f) sizeof(((t*)4096)->f)

#define OFFSETOF_MEMBER(t, f) \
    (reinterpret_cast<char*>(&reinterpret_cast<t*>(16)->f) - reinterpret_cast<char*>(16))

#ifdef PTHREADS_REDHAT_WIN32
#include <iosfwd>
using std::ostream; // NOLINT(build/include)
#include <pthread.h> // NOLINT(build/include)
// pthread_t is not a simple integer or pointer on Win32
std::ostream& operator<<(std::ostream& out, const pthread_t& thread_id);
#endif

// GXX_EXPERIMENTAL_CXX0X is defined by gcc and clang up to at least
// gcc-4.7 and clang-3.1 (2011-12-13).  __cplusplus was defined to 1
// in gcc before 4.7 (Crosstool 16) and clang before 3.1, but is
// defined according to the language version in effect thereafter.  I
// believe MSVC will also define __cplusplus according to the language
// version, but haven't checked that.
#if defined(__GXX_EXPERIMENTAL_CXX0X__) || __cplusplus >= 201103L
// Define this to 1 if the code is compiled in C++11 mode; leave it
// undefined otherwise.  Do NOT define it to 0 -- that causes
// '#ifdef LANG_CXX11' to behave differently from '#if LANG_CXX11'.
#define LANG_CXX11 1
#endif

// On some platforms, a "function pointer" points to a function descriptor
// rather than directly to the function itself.  Use FUNC_PTR_TO_CHAR_PTR(func)
// to get a char-pointer to the first instruction of the function func.
#if defined(__powerpc__) || defined(__ia64)
// use opd section for function descriptors on these platforms, the function
// address is the first word of the descriptor
enum { kPlatformUsesOPDSections = 1 };
#define FUNC_PTR_TO_CHAR_PTR(func) (reinterpret_cast<char**>(func)[0])
#else
enum { kPlatformUsesOPDSections = 0 };
#define FUNC_PTR_TO_CHAR_PTR(func) (reinterpret_cast<char*>(func))
#endif
