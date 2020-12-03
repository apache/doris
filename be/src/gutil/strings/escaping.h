// Copyright 2006 Google Inc. All Rights Reserved.
// Authors: Numerous. Principal maintainers are csilvers and zunger.
//
// This is a grab-bag file for string utilities involved in escaping and
// unescaping strings in various ways. Who knew there were so many?
//
// NOTE: Although the functions declared here have been imported into
// the global namespace, the using statements are slated for removal.
// Do not refer to these symbols without properly namespace-qualifying
// them with "strings::". Of course you may also use "using" statements
// within a .cc file.
//
// There are more escaping functions in:
//   webutil/html/tagutils.h (Escaping strings for HTML, PRE, JavaScript, etc.)
//   webutil/url/url.h (Escaping for URL's, both RFC-2396 and other methods)
//   template/template_modifiers.h (All sorts of stuff)
//   util/regex/re2/re2.h (Escaping for literals within regular expressions
//                         - see RE2::QuoteMeta).
// And probably many more places, as well.

#ifndef STRINGS_ESCAPING_H_
#define STRINGS_ESCAPING_H_

#include <stddef.h>

#include <string>
using std::string;
#include <vector>
using std::vector;

#include <common/logging.h>

#include "gutil/strings/ascii_ctype.h"
#include "gutil/strings/charset.h"
#include "gutil/strings/stringpiece.h"

namespace strings {

// ----------------------------------------------------------------------
// EscapeStrForCSV()
//    Escapes the quotes in 'src' by doubling them. This is necessary
//    for generating CSV files (see SplitCSVLine).
//    Returns the number of characters written into dest (not counting
//    the \0) or -1 if there was insufficient space.
//
//    Example: [some "string" to test] --> [some ""string"" to test]
// ----------------------------------------------------------------------
int EscapeStrForCSV(const char* src, char* dest, int dest_len);

// ----------------------------------------------------------------------
// UnescapeCEscapeSequences()
//    Copies "source" to "dest", rewriting C-style escape sequences
//    -- '\n', '\r', '\\', '\ooo', etc -- to their ASCII
//    equivalents.  "dest" must be sufficiently large to hold all
//    the characters in the rewritten string (i.e. at least as large
//    as strlen(source) + 1 should be safe, since the replacements
//    are always shorter than the original escaped sequences).  It's
//    safe for source and dest to be the same.  RETURNS the length
//    of dest.
//
//    It allows hex sequences \xhh, or generally \xhhhhh with an
//    arbitrary number of hex digits, but all of them together must
//    specify a value of a single byte (e.g. \x0045 is equivalent
//    to \x45, and \x1234 is erroneous). If the value is too large,
//    it is truncated to 8 bits and an error is set. This is also
//    true of octal values that exceed 0xff.
//
//    It also allows escape sequences of the form \uhhhh (exactly four
//    hex digits, upper or lower case) or \Uhhhhhhhh (exactly eight
//    hex digits, upper or lower case) to specify a Unicode code
//    point. The dest array will contain the UTF8-encoded version of
//    that code-point (e.g., if source contains \u2019, then dest will
//    contain the three bytes 0xE2, 0x80, and 0x99). For the inverse
//    transformation, use UniLib::UTF8EscapeString
//    (util/utf8/public/unilib.h), not CEscapeString.
//
//    Errors: In the first form of the call, errors are reported with
//    LOG(ERROR). The same is true for the second form of the call if
//    the pointer to the string vector is NULL; otherwise, error
//    messages are stored in the vector. In either case, the effect on
//    the dest array is not defined, but rest of the source will be
//    processed.
//
//    *** DEPRECATED: Use CUnescape() in new code ***
//    ----------------------------------------------------------------------
int UnescapeCEscapeSequences(const char* source, char* dest);
int UnescapeCEscapeSequences(const char* source, char* dest, vector<string>* errors);

// ----------------------------------------------------------------------
// UnescapeCEscapeString()
//    This does the same thing as UnescapeCEscapeSequences, but creates
//    a new string. The caller does not need to worry about allocating
//    a dest buffer. This should be used for non performance critical
//    tasks such as printing debug messages. It is safe for src and dest
//    to be the same.
//
//    The second call stores its errors in a supplied string vector.
//    If the string vector pointer is NULL, it reports the errors with LOG().
//
//    In the first and second calls, the length of dest is returned. In the
//    the third call, the new string is returned.
//
//    *** DEPRECATED: Use CUnescape() in new code ***
// ----------------------------------------------------------------------
int UnescapeCEscapeString(const string& src, string* dest);
int UnescapeCEscapeString(const string& src, string* dest, vector<string>* errors);
string UnescapeCEscapeString(const string& src);

// ----------------------------------------------------------------------
// CUnescape()
//    Copies "source" to "dest", rewriting C-style escape sequences
//    -- '\n', '\r', '\\', '\ooo', etc -- to their ASCII
//    equivalents.  "dest" must be sufficiently large to hold all
//    the characters in the rewritten string (i.e. at least as large
//    as source.size() should be safe, since the replacements
//    are never longer than the original escaped sequences).  It's
//    safe for source and dest to be the same.  RETURNS true if
//    conversion was successful, false otherwise. Stores the size of
//    the result in 'dest_len'.
//
//    It allows hex sequences \xhh, or generally \xhhhhh with an
//    arbitrary number of hex digits, but all of them together must
//    specify a value of a single byte (e.g. \x0045 is equivalent
//    to \x45, and \x1234 is erroneous). If the value is too large,
//    an error is set. This is also true of octal values that exceed 0xff.
//
//    It also allows escape sequences of the form \uhhhh (exactly four
//    hex digits, upper or lower case) or \Uhhhhhhhh (exactly eight
//    hex digits, upper or lower case) to specify a Unicode code
//    point. The dest array will contain the UTF8-encoded version of
//    that code-point (e.g., if source contains \u2019, then dest will
//    contain the three bytes 0xE2, 0x80, and 0x99). For the inverse
//    transformation, use UniLib::UTF8EscapeString
//    (util/utf8/public/unilib.h), not CEscapeString.
//
//    Errors: Sets the description of the first encountered error in
//    'error'. To disable error reporting, set 'error' to NULL.
// ----------------------------------------------------------------------
bool CUnescape(const StringPiece& source, char* dest, int* dest_len, string* error);

bool CUnescape(const StringPiece& source, string* dest, string* error);

// A version with no error reporting.
inline bool CUnescape(const StringPiece& source, string* dest) {
    return CUnescape(source, dest, NULL);
}

// ----------------------------------------------------------------------
// CUnescapeForNullTerminatedString()
//
// This has the same behavior as CUnescape, except that each octal, hex,
// or Unicode escape sequence that resolves to a null character ('\0')
// is left in its original escaped form.  The result is a
// display-formatted string that can be interpreted as a null-terminated
// const char* and will not be cut short if it contains embedded null
// characters.
//
// ----------------------------------------------------------------------

bool CUnescapeForNullTerminatedString(const StringPiece& source, char* dest, int* dest_len,
                                      string* error);

bool CUnescapeForNullTerminatedString(const StringPiece& source, string* dest, string* error);

// A version with no error reporting.
inline bool CUnescapeForNullTerminatedString(const StringPiece& source, string* dest) {
    return CUnescapeForNullTerminatedString(source, dest, NULL);
}

// ----------------------------------------------------------------------
// CEscapeString()
// CHexEscapeString()
// Utf8SafeCEscapeString()
// Utf8SafeCHexEscapeString()
//    Copies 'src' to 'dest', escaping dangerous characters using
//    C-style escape sequences. This is very useful for preparing query
//    flags. 'src' and 'dest' should not overlap. The 'Hex' version uses
//    hexadecimal rather than octal sequences. The 'Utf8Safe' version
//    doesn't touch UTF-8 bytes.
//    Returns the number of bytes written to 'dest' (not including the \0)
//    or -1 if there was insufficient space.
//
//    Currently only \n, \r, \t, ", ', \ and !ascii_isprint() chars are escaped.
// ----------------------------------------------------------------------
int CEscapeString(const char* src, int src_len, char* dest, int dest_len);
int CHexEscapeString(const char* src, int src_len, char* dest, int dest_len);
int Utf8SafeCEscapeString(const char* src, int src_len, char* dest, int dest_len);
int Utf8SafeCHexEscapeString(const char* src, int src_len, char* dest, int dest_len);

// ----------------------------------------------------------------------
// CEscape()
// CHexEscape()
// Utf8SafeCEscape()
// Utf8SafeCHexEscape()
//    More convenient form of CEscapeString: returns result as a "string".
//    This version is slower than CEscapeString() because it does more
//    allocation.  However, it is much more convenient to use in
//    non-speed-critical code like logging messages etc.
// ----------------------------------------------------------------------
string CEscape(const StringPiece& src);
string CHexEscape(const StringPiece& src);
string Utf8SafeCEscape(const StringPiece& src);
string Utf8SafeCHexEscape(const StringPiece& src);

// ----------------------------------------------------------------------
// BackslashEscape()
//    Given a string and a list of characters to escape, replace any
//    instance of one of those characters with \ + that character. For
//    example, when exporting maps to /varz, label values need to have
//    all dots escaped. Appends the result to dest.
// BackslashUnescape()
//    Replace \ + any of the indicated "unescape me" characters with just
//    that character. Appends the result to dest.
//
//    IMPORTANT:
//    This function does not escape \ by default, so if you do not include
//    it in the chars to escape you will most certainly get an undesirable
//    result. That is, it won't be a reversible operation:
//      string src = "foo\\:bar";
//      BackslashUnescape(BackslashEscape(src, ":"), ":") == "foo\\\\:bar"
//    On the other hand, for all strings "src", the following is true:
//      BackslashUnescape(BackslashEscape(src, ":\\"), ":\\") == src
// ----------------------------------------------------------------------
void BackslashEscape(const StringPiece& src, const strings::CharSet& to_escape, string* dest);
void BackslashUnescape(const StringPiece& src, const strings::CharSet& to_unescape, string* dest);

inline string BackslashEscape(const StringPiece& src, const strings::CharSet& to_escape) {
    string s;
    BackslashEscape(src, to_escape, &s);
    return s;
}

inline string BackslashUnescape(const StringPiece& src, const strings::CharSet& to_unescape) {
    string s;
    BackslashUnescape(src, to_unescape, &s);
    return s;
}

// ----------------------------------------------------------------------
// QuotedPrintableUnescape()
//    Check out http://www.cis.ohio-state.edu/htbin/rfc/rfc2045.html for
//    more details, only briefly implemented. But from the web...
//    Quoted-printable is an encoding method defined in the MIME
//    standard. It is used primarily to encode 8-bit text (such as text
//    that includes foreign characters) into 7-bit US ASCII, creating a
//    document that is mostly readable by humans, even in its encoded
//    form. All MIME compliant applications can decode quoted-printable
//    text, though they may not necessarily be able to properly display the
//    document as it was originally intended. As quoted-printable encoding
//    is implemented most commonly, printable ASCII characters (values 33
//    through 126, excluding 61), tabs and spaces that do not appear at the
//    end of lines, and end-of-line characters are not encoded. Other
//    characters are represented by an equal sign (=) immediately followed
//    by that character's hexadecimal value. Lines that are longer than 76
//    characters are shortened by line breaks, with the equal sign marking
//    where the breaks occurred.
//
//    Note that QuotedPrintableUnescape is different from 'Q'-encoding as
//    defined in rfc2047. In particular, This does not treat '_'s as spaces.
//
//    See QEncodingUnescape().
//
//    Copies "src" to "dest", rewriting quoted printable escape sequences
//    =XX to their ASCII equivalents. src is not null terminated, instead
//    specify len. I recommend that slen<szdest, but we honor szdest
//    anyway.
//    RETURNS the length of dest.
// ----------------------------------------------------------------------
int QuotedPrintableUnescape(const char* src, int slen, char* dest, int szdest);

// ----------------------------------------------------------------------
// QEncodingUnescape()
//    This is very similar to QuotedPrintableUnescape except that we convert
//    '_'s into spaces. (See RFC 2047)
//    http://www.faqs.org/rfcs/rfc2047.html.
//
//    Copies "src" to "dest", rewriting q-encoding escape sequences
//    =XX to their ASCII equivalents. src is not null terminated, instead
//    specify len. I recommend that slen<szdest, but we honour szdest
//    anyway.
//    RETURNS the length of dest.
// ----------------------------------------------------------------------
int QEncodingUnescape(const char* src, int slen, char* dest, int szdest);

// ----------------------------------------------------------------------
// Base64Unescape()
// WebSafeBase64Unescape()
//    Copies "src" to "dest", where src is in base64 and is written to its
//    ASCII equivalents. src is not null terminated, instead specify len.
//    I recommend that slen<szdest, but we honor szdest anyway.
//    RETURNS the length of dest, or -1 if src contains invalid chars.
//    The WebSafe variation use '-' instead of '+' and '_' instead of '/'.
//    The variations that store into a string clear the string first, and
//    return false (with dest empty) if src contains invalid chars; for
//    these versions src and dest must be different strings.
// ----------------------------------------------------------------------
int Base64Unescape(const char* src, int slen, char* dest, int szdest);
bool Base64Unescape(const char* src, int slen, string* dest);
inline bool Base64Unescape(const string& src, string* dest) {
    return Base64Unescape(src.data(), src.size(), dest);
}

int WebSafeBase64Unescape(const char* src, int slen, char* dest, int szdest);
bool WebSafeBase64Unescape(const char* src, int slen, string* dest);
inline bool WebSafeBase64Unescape(const string& src, string* dest) {
    return WebSafeBase64Unescape(src.data(), src.size(), dest);
}

// Return the length to use for the output buffer given to the base64 escape
// routines. Make sure to use the same value for do_padding in both.
// This function may return incorrect results if given input_len values that
// are extremely high, which should happen rarely.
int CalculateBase64EscapedLen(int input_len, bool do_padding);
// Use this version when calling Base64Escape without a do_padding arg.
int CalculateBase64EscapedLen(int input_len);

// ----------------------------------------------------------------------
// Base64Escape()
// WebSafeBase64Escape()
//    Encode "src" to "dest" using base64 encoding.
//    src is not null terminated, instead specify len.
//    'dest' should have at least CalculateBase64EscapedLen() length.
//    RETURNS the length of dest.
//    The WebSafe variation use '-' instead of '+' and '_' instead of '/'
//    so that we can place the out in the URL or cookies without having
//    to escape them.  It also has an extra parameter "do_padding",
//    which when set to false will prevent padding with "=".
// ----------------------------------------------------------------------
int Base64Escape(const unsigned char* src, int slen, char* dest, int szdest);
int WebSafeBase64Escape(const unsigned char* src, int slen, char* dest, int szdest,
                        bool do_padding);
// Encode src into dest with padding.
void Base64Escape(const string& src, string* dest);
// Encode src into dest web-safely without padding.
void WebSafeBase64Escape(const string& src, string* dest);
// Encode src into dest web-safely with padding.
void WebSafeBase64EscapeWithPadding(const string& src, string* dest);

void Base64Escape(const unsigned char* src, int szsrc, string* dest, bool do_padding);
void WebSafeBase64Escape(const unsigned char* src, int szsrc, string* dest, bool do_padding);

// ----------------------------------------------------------------------
// Base32Unescape()
//    Copies "src" to "dest", where src is in base32 and is written to its
//    ASCII equivalents. src is not null terminated, instead specify len.
//    RETURNS the length of dest, or -1 if src contains invalid chars.
// ----------------------------------------------------------------------
int Base32Unescape(const char* src, int slen, char* dest, int szdest);
bool Base32Unescape(const char* src, int slen, string* dest);
inline bool Base32Unescape(const string& src, string* dest) {
    return Base32Unescape(src.data(), src.size(), dest);
}

// ----------------------------------------------------------------------
// Base32Escape()
//    Encode "src" to "dest" using base32 encoding.
//    src is not null terminated, instead specify len.
//    'dest' should have at least CalculateBase32EscapedLen() length.
//    RETURNS the length of dest. RETURNS 0 if szsrc is zero, or szdest is
//    too small to fit the fully encoded result.  'dest' is padded with '='.
//
//    Note that this is "Base 32 Encoding" from RFC 4648 section 6.
// ----------------------------------------------------------------------
int Base32Escape(const unsigned char* src, size_t szsrc, char* dest, size_t szdest);
bool Base32Escape(const string& src, string* dest);

// ----------------------------------------------------------------------
// Base32HexEscape()
//    Encode "src" to "dest" using base32hex encoding.
//    src is not null terminated, instead specify len.
//    'dest' should have at least CalculateBase32EscapedLen() length.
//    RETURNS the length of dest. RETURNS 0 if szsrc is zero, or szdest is
//    too small to fit the fully encoded result.  'dest' is padded with '='.
//
//    Note that this is "Base 32 Encoding with Extended Hex Alphabet"
//    from RFC 4648 section 7.
// ----------------------------------------------------------------------
int Base32HexEscape(const unsigned char* src, size_t szsrc, char* dest, size_t szdest);
bool Base32HexEscape(const string& src, string* dest);

// Return the length to use for the output buffer given to the base32 escape
// routines.  This function may return incorrect results if given input_len
// values that are extremely high, which should happen rarely.
int CalculateBase32EscapedLen(size_t input_len);

// ----------------------------------------------------------------------
// EightBase32DigitsToTenHexDigits()
// TenHexDigitsToEightBase32Digits()
//    Convert base32 to and from hex.
//
//   for EightBase32DigitsToTenHexDigits():
//     *in must point to 8 base32 digits.
//     *out must point to 10 bytes.
//
//   for TenHexDigitsToEightBase32Digits():
//     *in must point to 10 hex digits.
//     *out must point to 8 bytes.
//
//   Note that the Base64 functions above are different. They convert base64
//   to and from binary data. We convert to and from string representations
//   of hex. They deal with arbitrary lengths and we deal with single,
//   whole base32 quanta.
//
//   See RFC3548 at http://www.ietf.org/rfc/rfc3548.txt
//   for details on base32.
// ----------------------------------------------------------------------
void EightBase32DigitsToTenHexDigits(const char* in, char* out);
void TenHexDigitsToEightBase32Digits(const char* in, char* out);

// ----------------------------------------------------------------------
// EightBase32DigitsToFiveBytes()
// FiveBytesToEightBase32Digits()
//   Convert base32 to and from binary
//
//   for EightBase32DigitsToTenHexDigits():
//     *in must point to 8 base32 digits.
//     *out must point to 5 bytes.
//
//   for TenHexDigitsToEightBase32Digits():
//     *in must point to 5 bytes.
//     *out must point to 8 bytes.
//
//   Note that the Base64 functions above are different.  They deal with
//   arbitrary lengths and we deal with single, whole base32 quanta.
// ----------------------------------------------------------------------
void EightBase32DigitsToFiveBytes(const char* in, unsigned char* bytes_out);
void FiveBytesToEightBase32Digits(const unsigned char* in_bytes, char* out);

// ----------------------------------------------------------------------
// EscapeFileName()
// UnescapeFileName()
//   Utility functions to (un)escape strings to make them suitable for use in
//   filenames. Characters not in [a-zA-Z0-9-_.] will be escaped into %XX.
//   E.g: "Hello, world!" will be escaped as "Hello%2c%20world%21"
//
//   NB that this function escapes slashes, so the output will be a flat
//   filename and will not keep the directory structure. Slashes are replaced
//   with '~', instead of a %XX sequence to make it easier for people to
//   understand the escaped form when the original string is a file path.
//
//   WARNING: filenames produced by these functions may not be compatible with
//   Colossus FS. In particular, the '%' character has a special meaning in
//   CFS.
//
//   The versions that receive a string for the output will append to it.
// ----------------------------------------------------------------------
void EscapeFileName(const StringPiece& src, string* dst);
void UnescapeFileName(const StringPiece& src, string* dst);
inline string EscapeFileName(const StringPiece& src) {
    string r;
    EscapeFileName(src, &r);
    return r;
}
inline string UnescapeFileName(const StringPiece& src) {
    string r;
    UnescapeFileName(src, &r);
    return r;
}

// ----------------------------------------------------------------------
// Here are a couple utility methods to change ints to hex chars & back
// ----------------------------------------------------------------------

inline int int_to_hex_digit(int i) {
    DCHECK((i >= 0) && (i <= 15));
    return ((i < 10) ? (i + '0') : ((i - 10) + 'A'));
}

inline int int_to_lower_hex_digit(int i) {
    DCHECK((i >= 0) && (i <= 15));
    return (i < 10) ? (i + '0') : ((i - 10) + 'a');
}

inline int hex_digit_to_int(char c) {
    /* Assume ASCII. */
    DCHECK('0' == 0x30 && 'A' == 0x41 && 'a' == 0x61);
    DCHECK(ascii_isxdigit(c));
    int x = static_cast<unsigned char>(c);
    if (x > '9') {
        x += 9;
    }
    return x & 0xf;
}

// ----------------------------------------------------------------------
// a2b_hex()
//  Description: Ascii-to-Binary hex conversion.  This converts
//         2*'num' hexadecimal characters to 'num' binary data.
//        Return value: 'num' bytes of binary data (via the 'to' argument)
// ----------------------------------------------------------------------
void a2b_hex(const char* from, unsigned char* to, int num);
void a2b_hex(const char* from, char* to, int num);
void a2b_hex(const char* from, string* to, int num);
string a2b_hex(const string& a);

// ----------------------------------------------------------------------
// a2b_bin()
//  Description: Ascii-to-Binary binary conversion.  This converts
//        a.size() binary characters (ascii '0' or '1') to
//        ceil(a.size()/8) bytes of binary data.  The first character is
//        considered the most significant if byte_order_msb is set.  a is
//        considered to be padded with trailing 0s if its size is not a
//        multiple of 8.
//        Return value: ceil(a.size()/8) bytes of binary data
// ----------------------------------------------------------------------
string a2b_bin(const string& a, bool byte_order_msb);

// ----------------------------------------------------------------------
// b2a_hex()
//  Description: Binary-to-Ascii hex conversion.  This converts
//   'num' bytes of binary to a 2*'num'-character hexadecimal representation
//    Return value: 2*'num' characters of ascii text (via the 'to' argument)
// ----------------------------------------------------------------------
void b2a_hex(const unsigned char* from, char* to, int num);
void b2a_hex(const unsigned char* from, string* to, int num);

// ----------------------------------------------------------------------
// b2a_hex()
//  Description: Binary-to-Ascii hex conversion.  This converts
//   'num' bytes of binary to a 2*'num'-character hexadecimal representation
//    Return value: 2*'num' characters of ascii string
// ----------------------------------------------------------------------
string b2a_hex(const char* from, int num);
string b2a_hex(const StringPiece& b);

// ----------------------------------------------------------------------
// b2a_bin()
//  Description: Binary-to-Ascii binary conversion.  This converts
//   b.size() bytes of binary to a 8*b.size() character representation
//   (ascii '0' or '1').  The highest order bit in each byte is returned
//   first in the string if byte_order_msb is set.
//   Return value: 8*b.size() characters of ascii text
// ----------------------------------------------------------------------
string b2a_bin(const string& b, bool byte_order_msb);

// ----------------------------------------------------------------------
// ShellEscape
//   Make a shell command argument from a string.
//   Returns a Bourne shell string literal such that, once the shell finishes
//   expanding the argument, the argument passed on to the program being
//   run will be the same as whatever you passed in.
//   NOTE: This is "ported" from python2.2's commands.mkarg(); it should be
//         safe for Bourne shell syntax (i.e. sh, bash), but mileage may vary
//         with other shells.
// ----------------------------------------------------------------------
string ShellEscape(StringPiece src);

// Runs ShellEscape() on the arguments, concatenates them with a space, and
// returns the resulting string.
template <class InputIterator>
string ShellEscapeCommandLine(InputIterator begin, const InputIterator& end) {
    string result;
    for (; begin != end; ++begin) {
        if (!result.empty()) result.append(" ");
        result.append(ShellEscape(*begin));
    }
    return result;
}

// Reads at most bytes_to_read from binary_string and writes it to
// ascii_string in lower case hex.
void ByteStringToAscii(const string& binary_string, int bytes_to_read, string* ascii_string);

inline string ByteStringToAscii(const string& binary_string, int bytes_to_read) {
    string result;
    ByteStringToAscii(binary_string, bytes_to_read, &result);
    return result;
}

// Converts the hex from ascii_string into binary data and
// writes the binary data into binary_string.
// Empty input successfully converts to empty output.
// Returns false and may modify output if it is
// unable to parse the hex string.
bool ByteStringFromAscii(const string& ascii_string, string* binary_string);

// Clean up a multi-line string to conform to Unix line endings.
// Reads from src and appends to dst, so usually dst should be empty.
// If there is no line ending at the end of a non-empty string, it can
// be added automatically.
//
// Four different types of input are correctly handled:
//
//   - Unix/Linux files: line ending is LF, pass through unchanged
//
//   - DOS/Windows files: line ending is CRLF: convert to LF
//
//   - Legacy Mac files: line ending is CR: convert to LF
//
//   - Garbled files: random line endings, covert gracefully
//                    lonely CR, lonely LF, CRLF: convert to LF
//
//   @param src The multi-line string to convert
//   @param dst The converted string is appended to this string
//   @param auto_end_last_line Automatically terminate the last line
//
//   Limitations:
//
//     This does not do the right thing for CRCRLF files created by
//     broken programs that do another Unix->DOS conversion on files
//     that are already in CRLF format.
void CleanStringLineEndings(const string& src, string* dst, bool auto_end_last_line);

// Same as above, but transforms the argument in place.
void CleanStringLineEndings(string* str, bool auto_end_last_line);

} // namespace strings

// The following functions used to be defined in strutil.h in the top-level
// namespace, so we alias them here. Do not add new functions here.
//
//             Talk to him if you want to help.
//
// DEPRECATED(mec): Using these names in the global namespace is deprecated.
// Use the strings:: names.

using strings::EscapeStrForCSV;
using strings::UnescapeCEscapeSequences;
using strings::UnescapeCEscapeString;
using strings::CEscapeString;
using strings::CHexEscapeString;
using strings::CEscape;
using strings::CHexEscape;
using strings::BackslashEscape;
using strings::BackslashUnescape;
using strings::QuotedPrintableUnescape;
using strings::QEncodingUnescape;
using strings::Base64Unescape;
using strings::WebSafeBase64Unescape;
using strings::CalculateBase64EscapedLen;
using strings::Base64Escape;
using strings::WebSafeBase64Escape;
using strings::WebSafeBase64EscapeWithPadding;
using strings::Base32Escape;
using strings::Base32HexEscape;
using strings::CalculateBase32EscapedLen;
using strings::EightBase32DigitsToTenHexDigits;
using strings::TenHexDigitsToEightBase32Digits;
using strings::EightBase32DigitsToFiveBytes;
using strings::FiveBytesToEightBase32Digits;
using strings::int_to_hex_digit;
using strings::int_to_lower_hex_digit;
using strings::hex_digit_to_int;
using strings::a2b_hex;
using strings::a2b_bin;
using strings::b2a_hex;
using strings::b2a_bin;
using strings::ShellEscape;
using strings::ShellEscapeCommandLine;
using strings::ByteStringFromAscii;
using strings::ByteStringToAscii;
using strings::CleanStringLineEndings;

#endif // STRINGS_ESCAPING_H_
