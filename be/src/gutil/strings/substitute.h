// Copyright 2008 Google Inc.  All rights reserved.

#include <string.h>

#include <string>
using std::string;

#include "gutil/basictypes.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/stringpiece.h"

#ifndef STRINGS_SUBSTITUTE_H_
#define STRINGS_SUBSTITUTE_H_

namespace strings {

// ----------------------------------------------------------------------
// strings::Substitute()
// strings::SubstituteAndAppend()
//   Kind of like StringPrintf, but different.
//
//   Example:
//     string GetMessage(string first_name, string last_name, int age) {
//       return strings::Substitute("My name is $0 $1 and I am $2 years old.",
//                                  first_name, last_name, age);
//     }
//
//   Differences from StringPrintf:
//   * The format string does not identify the types of arguments.
//     Instead, the magic of C++ deals with this for us.  See below
//     for a list of accepted types.
//   * Substitutions in the format string are identified by a '$'
//     followed by a digit.  So, you can use arguments out-of-order and
//     use the same argument multiple times.
//   * '$$' in the format string means output a literal '$' character.
//   * It's much faster than StringPrintf.
//
//   Supported types:
//   * StringPiece (const char*, const string&) (NULL is equivalent to "")
//     * Note that this means you do not have to add .c_str() to all of
//       your strings.  In fact, you shouldn't; it will be slower.
//   * int32, int64, uint32, uint64
//   * float, double
//   * bool:  Printed as "true" or "false".
//   * pointer types other than char*: Printed as "0x<lower case hex string>",
//             except that NULL is printed as "NULL".
//
//   If not enough arguments are supplied, a LOG(DFATAL) will be issued and
//   the empty string will be returned. If too many arguments are supplied,
//   just the first ones will be used (no warning).
//
//   SubstituteAndAppend() is like Substitute() but appends the result to
//   *output.  Example:
//
//     string str;
//     strings::SubstituteAndAppend(&str,
//                                  "My name is $0 $1 and I am $2 years old.",
//                                  first_name, last_name, age);
//
//   Substitute() is significantly faster than StringPrintf().  For very
//   large strings, it may be orders of magnitude faster.
// ----------------------------------------------------------------------

namespace internal { // Implementation details.

// This class has implicit constructors.
// Style guide exception granted:
// http://goto/style-guide-exception-20978288

class SubstituteArg {
public:
    // We must explicitly overload char* so that the compiler doesn't try to
    // cast it to bool to construct a DynamicSubstituteArg.  Might as well
    // overload const string& as well, since this allows us to avoid a temporary
    // object.
    inline SubstituteArg(const char* value) // NOLINT(runtime/explicit)
            : text_(value), size_(value == NULL ? 0 : strlen(text_)) {}
    inline SubstituteArg(const string& value) // NOLINT(runtime/explicit)
            : text_(value.data()), size_(value.size()) {}
    inline SubstituteArg(const StringPiece& value) // NOLINT(runtime/explicit)
            : text_(value.data()), size_(value.size()) {}

    // Primitives
    // We don't overload for signed and unsigned char because if people are
    // explicitly declaring their chars as signed or unsigned then they are
    // probably actually using them as 8-bit integers and would probably
    // prefer an integer representation.  But, we don't really know.  So, we
    // make the caller decide what to do.
    inline SubstituteArg(char value) // NOLINT(runtime/explicit)
            : text_(scratch_), size_(1) {
        scratch_[0] = value;
    }
    inline SubstituteArg(short value) // NOLINT(runtime/explicit)
            : text_(scratch_), size_(FastInt32ToBufferLeft(value, scratch_) - scratch_) {}
    inline SubstituteArg(unsigned short value) // NOLINT(runtime/explicit)
            : text_(scratch_), size_(FastUInt32ToBufferLeft(value, scratch_) - scratch_) {}
    inline SubstituteArg(int value) // NOLINT(runtime/explicit)
            : text_(scratch_), size_(FastInt32ToBufferLeft(value, scratch_) - scratch_) {}
    inline SubstituteArg(unsigned int value) // NOLINT(runtime/explicit)
            : text_(scratch_), size_(FastUInt32ToBufferLeft(value, scratch_) - scratch_) {}
    inline SubstituteArg(long value) // NOLINT(runtime/explicit)
            : text_(scratch_),
              size_((sizeof(value) == 4 ? FastInt32ToBufferLeft(value, scratch_)
                                        : FastInt64ToBufferLeft(value, scratch_)) -
                    scratch_) {}
    inline SubstituteArg(unsigned long value) // NOLINT(runtime/explicit)
            : text_(scratch_),
              size_((sizeof(value) == 4 ? FastUInt32ToBufferLeft(value, scratch_)
                                        : FastUInt64ToBufferLeft(value, scratch_)) -
                    scratch_) {}
    inline SubstituteArg(long long value) // NOLINT(runtime/explicit)
            : text_(scratch_), size_(FastInt64ToBufferLeft(value, scratch_) - scratch_) {}
    inline SubstituteArg(unsigned long long value) // NOLINT(runtime/explicit)
            : text_(scratch_), size_(FastUInt64ToBufferLeft(value, scratch_) - scratch_) {}
    inline SubstituteArg(float value) // NOLINT(runtime/explicit)
            : text_(FloatToBuffer(value, scratch_)), size_(strlen(text_)) {}
    inline SubstituteArg(double value) // NOLINT(runtime/explicit)
            : text_(DoubleToBuffer(value, scratch_)), size_(strlen(text_)) {}
    inline SubstituteArg(bool value) // NOLINT(runtime/explicit)
            : text_(value ? "true" : "false"), size_(strlen(text_)) {}
    // void* values, with the exception of char*, are printed as
    // StringPrintf with format "%p" would ("0x<hex value>"), with the
    // exception of NULL, which is printed as "NULL".
    SubstituteArg(const void* value); // NOLINT(runtime/explicit)

    inline const char* data() const { return text_; }
    inline int size() const { return size_; }

    // Indicates that no argument was given.
    static const SubstituteArg NoArg;

private:
    inline SubstituteArg() : text_(NULL), size_(-1) {}

    const char* text_;
    int size_;
    char scratch_[kFastToBufferSize];
};

// Return the length of the resulting string after performing the given
// substitution.
int SubstitutedSize(StringPiece format, const SubstituteArg* const* args_array);

// Perform the given substitution into 'target'. 'target' must have
// space for the result -- use SubstitutedSize() to determine how many
// bytes are required.  Returns a pointer to the next byte following
// the result in 'target'.
char* SubstituteToBuffer(StringPiece format, const SubstituteArg* const* args_array, char* target);

} // namespace internal

void SubstituteAndAppend(string* output, StringPiece format,
                         const internal::SubstituteArg& arg0 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg1 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg2 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg3 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg4 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg5 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg6 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg7 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg8 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg9 = internal::SubstituteArg::NoArg);

inline string Substitute(StringPiece format,
                         const internal::SubstituteArg& arg0 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg1 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg2 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg3 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg4 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg5 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg6 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg7 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg8 = internal::SubstituteArg::NoArg,
                         const internal::SubstituteArg& arg9 = internal::SubstituteArg::NoArg) {
    string result;
    SubstituteAndAppend(&result, format, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8,
                        arg9);
    return result;
}

} // namespace strings

#endif // STRINGS_SUBSTITUTE_H_
