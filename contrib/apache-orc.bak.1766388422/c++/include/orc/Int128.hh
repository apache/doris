/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ORC_INT_128
#define ORC_INT_128

#include "orc/orc-config.hh"

#include <stdexcept>
#include <string>

namespace orc {

  /**
   * Represents a signed 128-bit integer in two's complement.
   * Calculations wrap around and overflow is ignored.
   *
   * For a discussion of the algorithms, look at Knuth's volume 2,
   * Semi-numerical Algorithms section 4.3.1.
   *
   */
  class Int128 {
   public:
    Int128() {
      highbits = 0;
      lowbits = 0;
    }

    /**
     * Convert a signed 64 bit value into an Int128.
     */
    Int128(int64_t right) {
      if (right >= 0) {
        highbits = 0;
        lowbits = static_cast<uint64_t>(right);
      } else {
        highbits = -1;
        lowbits = static_cast<uint64_t>(right);
      }
    }

    /**
     * Create from the twos complement representation.
     */
    Int128(int64_t high, uint64_t low) {
      highbits = high;
      lowbits = low;
    }

    /**
     * Parse the number from a base 10 string representation.
     */
    explicit Int128(const std::string&);

    /**
     * Maximum positive value allowed by the type.
     */
    static Int128 maximumValue();

    /**
     * Minimum negative value allowed by the type.
     */
    static Int128 minimumValue();

    Int128& negate() {
      lowbits = ~lowbits + 1;
      highbits = ~highbits;
      if (lowbits == 0) {
        highbits += 1;
      }
      return *this;
    }

    Int128& abs() {
      if (highbits < 0) {
        negate();
      }
      return *this;
    }

    Int128 abs() const {
      Int128 value = *this;
      value.abs();
      return value;
    }

    Int128& invert() {
      lowbits = ~lowbits;
      highbits = ~highbits;
      return *this;
    }

    /**
     * Add a number to this one. The result is truncated to 128 bits.
     * @param right the number to add
     * @return *this
     */
    Int128& operator+=(const Int128& right) {
      uint64_t sum = lowbits + right.lowbits;
      highbits += right.highbits;
      if (sum < lowbits) {
        highbits += 1;
      }
      lowbits = sum;
      return *this;
    }

    /**
     * Subtract a number from this one. The result is truncated to 128 bits.
     * @param right the number to subtract
     * @return *this
     */
    Int128& operator-=(const Int128& right) {
      uint64_t diff = lowbits - right.lowbits;
      highbits -= right.highbits;
      if (diff > lowbits) {
        highbits -= 1;
      }
      lowbits = diff;
      return *this;
    }

    /**
     * Multiply this number by a number. The result is truncated to 128 bits.
     * @param right the number to multiply by
     * @return *this
     */
    Int128& operator*=(const Int128& right);

    /**
     * Divide this number by right and return the result. This operation is
     * not destructive.
     *
     * The answer rounds to zero. Signs work like:
     *    21 /  5 ->  4,  1
     *   -21 /  5 -> -4, -1
     *    21 / -5 -> -4,  1
     *   -21 / -5 ->  4, -1
     * @param right the number to divide by
     * @param remainder the remainder after the division
     */
    Int128 divide(const Int128& right, Int128& remainder) const;

    /**
     * Logical or between two Int128.
     * @param right the number to or in
     * @return *this
     */
    Int128& operator|=(const Int128& right) {
      lowbits |= right.lowbits;
      highbits |= right.highbits;
      return *this;
    }

    /**
     * Logical and between two Int128.
     * @param right the number to and in
     * @return *this
     */
    Int128& operator&=(const Int128& right) {
      lowbits &= right.lowbits;
      highbits &= right.highbits;
      return *this;
    }

    /**
     * Logical and between two Int128.
     * @param right the number to and in
     * @return logical and result
     */
    Int128 operator&(const Int128& right) {
      Int128 value = *this;
      value &= right;
      return value;
    }

    /**
     * Shift left by the given number of bits.
     * Values larger than 2**127 will shift into the sign bit.
     */
    Int128& operator<<=(uint32_t bits) {
      if (bits != 0) {
        if (bits < 64) {
          highbits <<= bits;
          highbits |= (lowbits >> (64 - bits));
          lowbits <<= bits;
        } else if (bits < 128) {
          highbits = static_cast<int64_t>(lowbits) << (bits - 64);
          lowbits = 0;
        } else {
          highbits = 0;
          lowbits = 0;
        }
      }
      return *this;
    }

    /**
     * Shift right by the given number of bits. Negative values will
     * sign extend and fill with one bits.
     */
    Int128& operator>>=(uint32_t bits) {
      if (bits != 0) {
        if (bits < 64) {
          lowbits >>= bits;
          lowbits |= static_cast<uint64_t>(highbits << (64 - bits));
          highbits = static_cast<int64_t>(static_cast<uint64_t>(highbits) >> bits);
        } else if (bits < 128) {
          lowbits = static_cast<uint64_t>(highbits >> (bits - 64));
          highbits = highbits >= 0 ? 0 : -1l;
        } else {
          highbits = highbits >= 0 ? 0 : -1l;
          lowbits = static_cast<uint64_t>(highbits);
        }
      }
      return *this;
    }

    bool operator==(const Int128& right) const {
      return highbits == right.highbits && lowbits == right.lowbits;
    }

    bool operator!=(const Int128& right) const {
      return highbits != right.highbits || lowbits != right.lowbits;
    }

    bool operator<(const Int128& right) const {
      if (highbits == right.highbits) {
        return lowbits < right.lowbits;
      } else {
        return highbits < right.highbits;
      }
    }

    bool operator<=(const Int128& right) const {
      if (highbits == right.highbits) {
        return lowbits <= right.lowbits;
      } else {
        return highbits <= right.highbits;
      }
    }

    bool operator>(const Int128& right) const {
      if (highbits == right.highbits) {
        return lowbits > right.lowbits;
      } else {
        return highbits > right.highbits;
      }
    }

    bool operator>=(const Int128& right) const {
      if (highbits == right.highbits) {
        return lowbits >= right.lowbits;
      } else {
        return highbits >= right.highbits;
      }
    }

    uint32_t hash() const {
      return static_cast<uint32_t>(highbits >> 32) ^ static_cast<uint32_t>(highbits) ^
             static_cast<uint32_t>(lowbits >> 32) ^ static_cast<uint32_t>(lowbits);
    }

    /**
     * Does this value fit into a long?
     */
    bool fitsInLong() const {
      switch (highbits) {
        case 0:
          return 0 == (lowbits & LONG_SIGN_BIT);
        case -1:
          return 0 != (lowbits & LONG_SIGN_BIT);
        default:
          return false;
      }
    }

    /**
     * Convert the value to a long and
     */
    int64_t toLong() const {
      if (fitsInLong()) {
        return static_cast<int64_t>(lowbits);
      }
      throw std::range_error("Int128 too large to convert to long");
    }

    /**
     * Return the base 10 string representation of the integer.
     */
    std::string toString() const;

    /**
     * Return the base 10 string representation with a decimal point,
     * the given number of places after the decimal.
     *
     * @param scale scale of the Int128 to be interpreted as a decimal value
     * @param trimTrailingZeros whether or not to trim trailing zeros
     * @return converted string representation
     */
    std::string toDecimalString(int32_t scale = 0, bool trimTrailingZeros = false) const;

    /**
     * Return the base 16 string representation of the two's complement with
     * a prefix of "0x".
     * Int128(-1).toHexString() = "0xffffffffffffffffffffffffffffffff".
     */
    std::string toHexString() const;

    /**
     * Get the high bits of the twos complement representation of the number.
     */
    int64_t getHighBits() {
      return highbits;
    }

    /**
     * Get the low bits of the twos complement representation of the number.
     */
    uint64_t getLowBits() {
      return lowbits;
    }

    /**
     * Represent the absolute number as a list of uint32.
     * Visible for testing only.
     * @param array the array that is set to the value of the number
     * @param wasNegative set to true if the original number was negative
     * @return the number of elements that were set in the array (1 to 4)
     */
    int64_t fillInArray(uint32_t* array, bool& wasNegative) const;

   private:
    static const uint64_t LONG_SIGN_BIT = 0x8000000000000000u;
    int64_t highbits;
    uint64_t lowbits;
  };

  /**
   * Scales up an Int128 value
   * @param value the Int128 value to scale
   * @param power the scale offset. Result of a negative factor is undefined.
   * @param overflow returns whether the result overflows or not
   * @return the scaled value
   */
  Int128 scaleUpInt128ByPowerOfTen(Int128 value, int32_t power, bool& overflow);
  /**
   * Scales down an Int128 value
   * @param value the Int128 value to scale
   * @param power the scale offset. Result of a negative factor is undefined.
   * @return the scaled value
   */
  Int128 scaleDownInt128ByPowerOfTen(Int128 value, int32_t power);
}  // namespace orc
#endif
