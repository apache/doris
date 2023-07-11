/* Copyright (c) 2015, 2023, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file json_path.cc

  This file contains implementation support for the JSON path abstraction.
  The path abstraction is described by the functional spec
  attached to WL#7909.
*/

/*
 * this file is copied from 
 * https://github.com/mysql/mysql-server/blob/8.0/sql-common/json_path.cc
 * and modified by Doris
*/

#include "json_path.h"

#include <cassert>
#include <cstdlib>
#include <cstddef>
#include <algorithm>  // any_of
#include <string>

#define PATH_BUF_SIZE 80
#define PATH_LEG_BUF_SIZE 16

namespace doris {

namespace {

constexpr char SCOPE = '$';
constexpr char BEGIN_MEMBER = '.';
constexpr char BEGIN_ARRAY = '[';
constexpr char END_ARRAY = ']';
constexpr char DOUBLE_QUOTE = '"';
constexpr char WILDCARD = '*';
constexpr char MINUS = '-';
constexpr char LAST[] = "last";

class Stream;

}  // namespace

static bool parse_path(Stream *, Json_path *);
static bool parse_path_leg(Stream *, Json_path *);
static bool parse_ellipsis_leg(Stream *, Json_path *);
static bool parse_array_leg(Stream *, Json_path *);
static bool parse_member_leg(Stream *, Json_path *);

static void append_array_index(std::string* buf, const size_t index, bool from_end) {
  if (!from_end) {
    buf->append(std::to_string(index));
    return;
  }

  buf->append(LAST, strlen(LAST));
  if (index > 0) { 
    *buf += MINUS;
    buf->append(std::to_string(index));
  }
}

// Json_path_leg

std::string Json_path_leg::to_string() const {
  std::string buf;
  buf.reserve(PATH_LEG_BUF_SIZE);

  switch (m_leg_type) {
    case jpl_member:
      buf += BEGIN_MEMBER;
      buf += "\"";
      buf.append(m_member_name.data(), m_member_name.length());
      buf += "\"";

    case jpl_array_cell:
      buf += BEGIN_ARRAY;
      append_array_index(&buf, m_first_array_index, m_first_array_index_from_end);
      buf += END_ARRAY;

    case jpl_array_range:
      buf += BEGIN_ARRAY;
      append_array_index(&buf, m_first_array_index, m_first_array_index_from_end);
      buf.append(" to ");
      append_array_index(&buf, m_last_array_index, m_last_array_index_from_end);
      buf += END_ARRAY;

    case jpl_member_wildcard:
      buf += BEGIN_MEMBER;
      buf += WILDCARD;

    case jpl_array_cell_wildcard:
      buf += BEGIN_ARRAY;
      buf += WILDCARD;
      buf += END_ARRAY;

    case jpl_ellipsis:
      buf += WILDCARD;
      buf += WILDCARD;
  }

  // Unknown leg type.
  assert(false); /* purecov: inspected */
  return buf;    /* purecov: inspected */
}

bool Json_path_leg::is_autowrap() const {
  switch (m_leg_type) {
    case jpl_array_cell:
      /*
        If the array cell index matches an element in a single-element
        array (`0` or `last`), it will also match a non-array value
        which is auto-wrapped in a single-element array.
      */
      return first_array_index(1).within_bounds();
    case jpl_array_range: {
      /*
        If the range matches an element in a single-element array, it
        will also match a non-array which is auto-wrapped in a
        single-element array.
      */
      Array_range range = get_array_range(1);
      return range.m_begin < range.m_end;
    }
    default:
      return false;
  }
}

bool Json_path_leg::contains(const Json_path_leg& other) const {
  switch (this->get_type()) {
  case jpl_member: {
      if (other.get_type() == jpl_member && this->get_member_name() == other.get_member_name()) {
          return true;
      }
  } break;
  case jpl_array_cell: {
      if (other.get_type() == jpl_array_cell &&
          this->get_first_array_index() == other.get_first_array_index()) {
          return true;
      }
  } break;
  case jpl_array_range: {
      if (other.get_type() == jpl_array_cell &&
          other.get_first_array_index() >= this->get_first_array_index() &&
          other.get_first_array_index() <= this->get_last_array_index()) {
          return true;
      } else if (other.get_type() == jpl_array_range &&
                 other.get_first_array_index() >= this->get_first_array_index() &&
                 other.get_last_array_index() <= this->get_last_array_index()) {
          return true;
      }
  } break;
  case jpl_member_wildcard: {
      if (other.get_type() == jpl_member || other.get_type() == jpl_member_wildcard) {
          return true;
      }
  };
  case jpl_array_cell_wildcard: {
      if (other.get_type() == jpl_array_cell || other.get_type() == jpl_array_range) {
          return true;
      }
  } break;
  case jpl_ellipsis: {
      return true;
  } break;
  default:
      // unknown leg type.
      assert(false && "Unknown leg type");
  }

  return false;
}

Json_path_leg::Array_range Json_path_leg::get_array_range(
    size_t array_length) const {
  if (m_leg_type == jpl_array_cell_wildcard) return {0, array_length};

  assert(m_leg_type == jpl_array_range);

  // Get the beginning of the range.
  size_t begin = first_array_index(array_length).position();

  // Get the (exclusive) end of the range.
  Json_array_index last = last_array_index(array_length);
  size_t end = last.within_bounds() ? last.position() + 1 : last.position();

  return {begin, end};
}

// Json_path
std::string Json_path::to_string() const {
  std::string buf;
  buf.reserve(PATH_BUF_SIZE);

  buf += SCOPE;
  for (const Json_path_leg *leg : *this) {
    buf += leg->to_string();
  }

  return buf;
}

bool Json_path::can_match_many() const {
  return std::any_of(begin(), end(), [](const Json_path_leg *leg) -> bool {
    switch (leg->get_type()) {
      case jpl_member_wildcard:
      case jpl_array_cell_wildcard:
      case jpl_ellipsis:
      case jpl_array_range:
        return true;
      default:
        return false;
    }
  });
}

bool Json_path::contains(const Json_path& other) const {
  // a strictly longer path cannot contain a shorter path.
  const size_t my_path_len = this->leg_count();
  const size_t other_path_len = other.leg_count();
  if (my_path_len > other_path_len) {
    return false;
  }

  for (size_t i = 0; i < my_path_len; ++i) {
    const Json_path_leg* my_leg = (*this)[i];
    const Json_path_leg* other_leg = other[i];
    if (my_leg->get_type() == jpl_ellipsis) {
          return true;
    }
    if (!my_leg->contains(*other_leg)) {
          return false;
    }
  }

  return true;
}

// Json_path parsing

namespace {

/// A simple input stream class for the JSON path parser.
class Stream {
 public:
  /// Creates an input stream reading from a character string.
  /// @param string  the input string
  /// @param length  the length of the input string
  Stream(const char *string, size_t length)
      : m_position(string), m_end(string + length) {}

  /// Returns a pointer to the current position in the stream.
  const char *position() const { return m_position; }

  /// Returns a pointer to the position just after the end of the stream.
  const char *end() const { return m_end; }

  /// Returns the number of bytes remaining in the stream.
  size_t remaining() const {
    assert(m_position <= m_end);
    return m_end - m_position;
  }

  /// Tells if the stream has been exhausted.
  bool exhausted() const { return remaining() == 0; }

  /// Reads the next byte from the stream and moves the position forward.
  char read() {
    assert(!exhausted());
    return *m_position++;
  }

  /// Reads the next byte from the stream without moving the position forward.
  char peek() const {
    assert(!exhausted());
    return *m_position;
  }

  /// Moves the position to the next non-whitespace character.
  void skip_whitespace() {
    m_position = std::find_if_not(m_position, m_end,
                                  [](char c) { return std::iswspace(c); });
  }

  /// Moves the position n bytes forward.
  void skip(size_t n) {
    assert(remaining() >= n);
    m_position += n;
  }

 private:
  /// The current position in the stream.
  const char *m_position;

  /// The end of the stream.
  const char *const m_end;
};

}  // namespace

/** Top level parsing factory method */
bool parse_path(size_t path_length, const char *path_expression,
                Json_path *path, size_t *bad_index) {
  Stream stream(path_expression, path_length);
  if (parse_path(&stream, path)) {
    *bad_index = stream.position() - path_expression;
    return true;
  }

  *bad_index = 0;
  return false;
}

/**
   Fills in a Json_path from a path expression.

   @param[in,out] stream The stream to read the path expression from.
   @param[in,out] path The Json_path object to fill.

   @return true on error, false on success
*/
static bool parse_path(Stream *stream, Json_path *path) {
  path->clear();

  // the first non-whitespace character must be $
  stream->skip_whitespace();
  if (stream->exhausted() || stream->read() != SCOPE) return true;

  // now add the legs
  stream->skip_whitespace();
  while (!stream->exhausted()) {
    if (parse_path_leg(stream, path)) return true;
    stream->skip_whitespace();
  }

  // a path may not end with an ellipsis
  if (path->leg_count() > 0 && path->last_leg()->get_type() == jpl_ellipsis) {
    return true;
  }

  return false;
}

/**
   Parses a single path leg and appends it to a Json_path object.

   @param[in,out] stream The stream to read the path expression from.
   @param[in,out] path The Json_path object to fill.

   @return true on error, false on success
*/
static bool parse_path_leg(Stream *stream, Json_path *path) {
  switch (stream->peek()) {
    case BEGIN_ARRAY:
      return parse_array_leg(stream, path);
    case BEGIN_MEMBER:
      return parse_member_leg(stream, path);
    case WILDCARD:
      return parse_ellipsis_leg(stream, path);
    default:
      return true;
  }
}

/**
   Parses a single ellipsis leg and appends it to a Json_path object.

   @param[in,out] stream The stream to read the path expression from.
   @param[in,out] path The Json_path object to fill.

   @return true on error, false on success
*/
static bool parse_ellipsis_leg(Stream *stream, Json_path *path) {
  // advance past the first *
  assert(stream->peek() == WILDCARD);
  stream->skip(1);

  // must be followed by a second *
  if (stream->exhausted() || stream->read() != WILDCARD) {
    return true;
  }

  // may not be the last leg
  if (stream->exhausted()) {
    return true;
  }

  // forbid the hard-to-read *** combination
  if (stream->peek() == WILDCARD) {
    return true;
  }

  return path->append(Json_path_leg(jpl_ellipsis));
}

/**
  Parse an array index in an array cell index or array range path leg.

  An array index is either a non-negative integer (a 0-based index relative to
  the beginning of the array), or the keyword "last" (which means the last
  element in the array), or the keyword "last" followed by a minus ("-") and a
  non-negative integer (which is the 0-based index relative to the end of the
  array).

  @param[in,out] stream    the stream to read the path expression from
  @param[out] array_index  gets set to the parsed array index
  @param[out] from_end     gets set to true if the array index is
                           relative to the end of the array

  @return true on error, false on success
*/
static bool parse_array_index(Stream *stream, size_t *array_index, bool *from_end) {
  *from_end = false;

  // Do we have the "last" token?
  if (stream->remaining() >= 4 &&
      std::equal(LAST, LAST + 4, stream->position())) {
    stream->skip(4);
    *from_end = true;

    stream->skip_whitespace();

    if (!stream->exhausted() && stream->peek() == MINUS) {
      // Found a minus sign, go on parsing to find the array index.
      stream->skip(1);
      stream->skip_whitespace();
    } else {
      // Didn't find any minus sign after "last", so we're done.
      *array_index = 0;
      return false;
    }
  }

  if (stream->exhausted() || !std::isdigit(stream->peek())) {
    return true;
  }

  char* endp;
  const size_t idx = strtoull(stream->position(), &endp, 10);

  stream->skip(endp - stream->position());
  *array_index = idx;
  return false;
}

/**
   Parses a single array leg and appends it to a Json_path object.

   @param[in,out] stream The stream to read the path expression from.
   @param[in,out] path The Json_path object to fill.

   @return true on error, false on success
*/
static bool parse_array_leg(Stream *stream, Json_path *path) {
  // advance past the [
  assert(stream->peek() == BEGIN_ARRAY);
  stream->skip(1);

  stream->skip_whitespace();
  if (stream->exhausted()) return true;

  if (stream->peek() == WILDCARD) {
    stream->skip(1);
    if (path->append(Json_path_leg(jpl_array_cell_wildcard)))
      return true; /* purecov: inspected */
  } else {
    /*
      Not a WILDCARD. The next token must be an array index (either
      the single index of a jpl_array_cell path leg, or the start
      index of a jpl_array_range path leg.
    */
    size_t cell_index1;
    bool from_end1;
    if (parse_array_index(stream, &cell_index1, &from_end1)) return true;

    stream->skip_whitespace();
    if (stream->exhausted()) return true;

    // Is this a range, <arrayIndex> to <arrayIndex>?
    const char *const pos = stream->position();
    if (stream->remaining() > 3 && std::iswspace(pos[-1]) && pos[0] == 't' &&
        pos[1] == 'o' && std::iswspace(pos[2])) {
      // A range. Skip over the "to" token and any whitespace.
      stream->skip(3);
      stream->skip_whitespace();

      size_t cell_index2;
      bool from_end2;
      if (parse_array_index(stream, &cell_index2, &from_end2)) return true;

      /*
        Reject pointless paths that can never return any matches, regardless of
        which array they are evaluated against. We know this if both indexes
        count from the same side of the array, and the start index is after the
        end index.
      */
      if (from_end1 == from_end2 && ((from_end1 && cell_index1 < cell_index2) ||
                                     (!from_end1 && cell_index2 < cell_index1)))
        return true;

      if (path->append(
              Json_path_leg(cell_index1, from_end1, cell_index2, from_end2)))
        return true; /* purecov: inspected */
    } else {
      // A single array cell.
      if (path->append(Json_path_leg(cell_index1, from_end1)))
        return true; /* purecov: inspected */
    }
  }

  // the next non-whitespace should be the closing ]
  stream->skip_whitespace();
  return stream->exhausted() || stream->read() != END_ARRAY;
}

/**
  Find the end of a member name in a JSON path. The name could be
  either a quoted or an unquoted identifier.

  @param start the start of the member name
  @param end the end of the JSON path expression
  @return pointer to the position right after the end of the name, or
  to the position right after the end of the string if the input
  string is an unterminated quoted identifier
*/
static const char *find_end_of_member_name(const char *start, const char *end) {
  const char *str = start;

  /*
    If we have a double-quoted name, the end of the name is the next
    unescaped double quote.
  */
  if (*str == DOUBLE_QUOTE) {
    str++;  // Advance past the opening double quote.
    while (str < end) {
      switch (*str++) {
        case '\\':
          /*
            Skip the next character after a backslash. It cannot mark
            the end of the quoted string.
          */
          str++;
          break;
        case DOUBLE_QUOTE:
          // An unescaped double quote marks the end of the quoted string.
          return str;
      }
    }

    /*
      Whoops. No terminating quote was found. Just return the end of
      the string. When we send the unterminated string through the
      JSON parser, it will detect and report the syntax error, so
      there is no need to handle the syntax error here.
    */
    return end;
  }

  /*
    If we have an unquoted name, the name is terminated by whitespace
    or [ or . or * or end-of-string.
  */
  const auto is_terminator = [](const char c) {
    return std::iswspace(c) || c == BEGIN_ARRAY || c == BEGIN_MEMBER ||
           c == WILDCARD;
  };
  return std::find_if(str, end, is_terminator);
}

/**
   Parses a single member leg and appends it to a Json_path object.

   @param[in,out] stream The stream to read the path expression from.
   @param[in,out] path The Json_path object to fill.

   @return true on error, false on success
*/
static bool parse_member_leg(Stream *stream, Json_path *path) {
  // advance past the .
  assert(stream->peek() == BEGIN_MEMBER);
  stream->skip(1);

  stream->skip_whitespace();
  if (stream->exhausted()) return true;

  if (stream->peek() == WILDCARD) {
    stream->skip(1);

    if (path->append(Json_path_leg(jpl_member_wildcard)))
      return true; /* purecov: inspected */
  } else {
    const char *const key_start = stream->position();
    const char *const key_end =
        find_end_of_member_name(key_start, stream->end());
    const bool was_quoted = (*key_start == DOUBLE_QUOTE);
    stream->skip(key_end - key_start);

    const size_t key_len = key_end - key_start;
    std::string member_name(key_start, key_len);
    if (was_quoted) {
        member_name = member_name.substr(1, key_len - 1);
    } else {
      // warning: we do not allow unquoted member name currently.
      return true;
    }

    // Looking good.
    if (path->append(Json_path_leg(member_name)))
      return true; /* purecov: inspected */
  }

  return false;
}

} // namespace doris
