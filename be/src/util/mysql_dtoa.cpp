// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/mysql_dtoa.h"
#include <stddef.h>
#include <stdint.h>
#include <malloc.h>
#include <string.h>
#include <float.h>
#include "util/mysql_global.h"
#include <cmath>
#include <iostream>
namespace palo {
/*
  We want to use the 'e' format in some cases even if we have enough space
  for the 'f' one just to mimic sprintf("%.15g") behavior for large integers,
  and to improve it for numbers < 10^(-4).
  That is, for |x| < 1 we require |x| >= 10^(-15), and for |x| > 1 we require
  it to be integer and be <= 10^DBL_DIG for the 'f' format to be used.
  We don't lose precision, but make cases like "1e200" or "0.00001" look nicer.
*/
const int MAX_DECPT_FOR_F_FORMAT2  = 15;
const double EPSILON = 1e-9;
/**
   Appears to suffice to not call malloc() in most cases.
   @todo
     see if it is possible to get rid of malloc().
     this constant is sufficient to avoid malloc() on all inputs I have tried.
*/
const int DTOA_BUFF_SIZE = (460 * sizeof(void *));

/* Magic value returned by dtoa() to indicate overflow */
const int DTOA_OVERFLOW = 9999;

static char* dtoa(double, int, int, int*, int*, char**, char*, size_t);
static void dtoa_free(char*, char*, size_t);

/**
   @brief
   Converts a given floating point number to a zero-terminated string
   representation with a given field width using the 'e' format
   (aka scientific notation) or the 'f' one.

   @details
   The format is chosen automatically to provide the most number of significant
   digits (and thus, precision) with a given field width. In many cases, the
   result is similar to that of sprintf(to, "%g", x) with a few notable
   differences:
   - the conversion is usually more precise than C library functions.
   - there is no 'precision' argument. instead, we specify the number of
     characters available for conversion (i.e. a field width).
   - the result never exceeds the specified field width. If the field is too
     short to contain even a rounded decimal representation, my_gcvt()
     indicates overflow and truncates the output string to the specified width.
   - float-type arguments are handled differently than double ones. For a
     float input number (i.e. when the 'type' argument is MY_GCVT_ARG_FLOAT)
     we deliberately limit the precision of conversion by FLT_DIG digits to
     avoid garbage past the significant digits.
   - unlike sprintf(), in cases where the 'e' format is preferred,  we don't
     zero-pad the exponent to save space for significant digits. The '+' sign
     for a positive exponent does not appear for the same reason.

   @param x           the input floating point number.
   @param type        is either MY_GCVT_ARG_FLOAT or MY_GCVT_ARG_DOUBLE.
                      Specifies the type of the input number (see notes above).
   @param width       field width in characters. The minimal field width to
                      hold any number representation (albeit rounded) is 7
                      characters ("-Ne-NNN").
   @param to          pointer to the output buffer. The result is always
                      zero-terminated, and the longest returned string is thus
                      'width + 1' bytes.
   @param error       if not NULL, points to a location where the status of
                      conversion is stored upon return.
                      FALSE  successful conversion
                      TRUE   the input number is [-,+]infinity or nan.
                             The output string in this case is always '0'.
   @return            number of written characters (excluding terminating '\0')

   @todo
   Check if it is possible and  makes sense to do our own rounding on top of
   dtoa() instead of calling dtoa() twice in (rare) cases when the resulting
   string representation does not fit in the specified field width and we want
   to re-round the input number with fewer significant digits. Examples:

     my_gcvt(-9e-3, ..., 4, ...);
     my_gcvt(-9e-3, ..., 2, ...);
     my_gcvt(1.87e-3, ..., 4, ...);
     my_gcvt(55, ..., 1, ...);

   We do our best to minimize such cases by:

   - passing to dtoa() the field width as the number of significant digits

   - removing the sign of the number early (and decreasing the width before
     passing it to dtoa())

   - choosing the proper format to preserve the most number of significant
     digits.
*/
void test();
size_t my_gcvt(double x, my_gcvt_arg_type type, int width, char* to,
        bool* error) {
    test();
    int decpt = 0;
    int sign = 0;
    int len = 0;
    int exp_len = 0;
    char* res = NULL;
    char* src = NULL;
    char* end = NULL;
    char* dst = to;
    char* dend = dst + width;
    char buf[DTOA_BUFF_SIZE];
    bool have_space = false;
    bool force_e_format = false;

    /* We want to remove '-' from equations early */
    if (x < 0.) {
        width--;
    }

    res = dtoa(x, 4, type == MY_GCVT_ARG_DOUBLE ? width : std::min(width, FLT_DIG),
            &decpt, &sign, &end, buf, sizeof(buf));

    if (decpt == DTOA_OVERFLOW) {
        dtoa_free(res, buf, sizeof(buf));
        *to++ = '0';
        *to = '\0';

        if (error != NULL) {
            *error = true;
        }

        return 1;
    }

    if (error != NULL) {
        *error = false;
    }

    src = res;
    len = int(end - res);

    /*
      Number of digits in the exponent from the 'e' conversion.
       The sign of the exponent is taken into account separetely, we don't need
       to count it here.
     */
    exp_len = 1 + (decpt >= 101 || decpt <= -99) + (decpt >= 11 || decpt <= -9);

    /*
       Do we have enough space for all digits in the 'f' format?
       Let 'len' be the number of significant digits returned by dtoa,
       and F be the length of the resulting decimal representation.
       Consider the following cases:
       1. decpt <= 0, i.e. we have "0.NNN" => F = len - decpt + 2
       2. 0 < decpt < len, i.e. we have "NNN.NNN" => F = len + 1
       3. len <= decpt, i.e. we have "NNN00" => F = decpt
    */
    have_space = (decpt <= 0 ? len - decpt + 2 :
                 decpt > 0 && decpt < len ? len + 1 :
                 decpt) <= width;
    /*
      The following is true when no significant digits can be placed with the
      specified field width using the 'f' format, and the 'e' format
      will not be truncated.
    */
    force_e_format = (decpt <= 0 && width <= 2 - decpt && width >= 3 + exp_len);

    /*
      Assume that we don't have enough space to place all significant digits in
      the 'f' format. We have to choose between the 'e' format and the 'f' one
      to keep as many significant digits as possible.
      Let E and F be the lengths of decimal representaion in the 'e' and 'f'
      formats, respectively. We want to use the 'f' format if, and only if F <= E.
      Consider the following cases:
      1. decpt <= 0.
         F = len - decpt + 2 (see above)
         E = len + (len > 1) + 1 + 1 (decpt <= -99) + (decpt <= -9) + 1
         ("N.NNe-MMM")
         (F <= E) <=> (len == 1 && decpt >= -1) || (len > 1 && decpt >= -2)
         We also need to ensure that if the 'f' format is chosen,
         the field width allows us to place at least one significant digit
         (i.e. width > 2 - decpt). If not, we prefer the 'e' format.
      2. 0 < decpt < len
         F = len + 1 (see above)
         E = len + 1 + 1 + ... ("N.NNeMMM")
         F is always less than E.
      3. len <= decpt <= width
         In this case we have enough space to represent the number in the 'f'
         format, so we prefer it with some exceptions.
      4. width < decpt
         The number cannot be represented in the 'f' format at all, always use
         the 'e' 'one.
    */
    if ((have_space ||
            /*
              Not enough space, let's see if the 'f' format provides the most number
              of significant digits.
            */
            ((decpt <= width && (decpt >= -1 || (decpt == -2 &&
                    (len > 1 || !force_e_format)))) &&
                    !force_e_format)) &&

            /*
              Use the 'e' format in some cases even if we have enough space for the
              'f' one. See comment for MAX_DECPT_FOR_F_FORMAT2.
            */
            (!have_space || (decpt >= -MAX_DECPT_FOR_F_FORMAT2 + 1 &&
                    (decpt <= MAX_DECPT_FOR_F_FORMAT2 || len > decpt)))) {
        /* 'f' format */
        int i = 0;

        width -= (decpt < len) + (decpt <= 0 ? 1 - decpt : 0);

        /* Do we have to truncate any digits? */
        if (width < len) {
            if (width < decpt) {
                if (error != NULL) {
                    *error = true;
                }

                width = decpt;
            }

            /*
                We want to truncate (len - width) least significant digits after the
                decimal point. For this we are calling dtoa with mode=5, passing the
                number of significant digits = (len-decpt) - (len-width) = width-decpt
            */
            dtoa_free(res, buf, sizeof(buf));
            res = dtoa(x, 5, width - decpt, &decpt, &sign, &end, buf, sizeof(buf));
            src = res;
            len = int(end - res);
        }

        if (len == 0) {
            /* Underflow. Just print '0' and exit */
            *dst++ = '0';
            dtoa_free(res, buf, sizeof(buf));
            *dst = '\0';

            return dst - to;
        }

        /*
            At this point we are sure we have enough space to put all digits
            returned by dtoa
        */
        if (sign && dst < dend) {
            *dst++ = '-';
        }

        if (decpt <= 0) {
            if (dst < dend) {
                *dst++ = '0';
            }

            if (len > 0 && dst < dend) {
                *dst++ = '.';
            }

            for (; decpt < 0 && dst < dend; decpt++) {
                *dst++ = '0';
            }
        }

        for (i = 1; i <= len && dst < dend; i++) {
            *dst++ = *src++;

            if (i == decpt && i < len && dst < dend) {
                *dst++ = '.';
            }
        }

        while (i++ <= decpt && dst < dend) {
            *dst++ = '0';
        }
    } else {
        /* 'e' format */
        int decpt_sign = 0;

        if (--decpt < 0) {
            decpt = -decpt;
            width--;
            decpt_sign = 1;
        }

        width -= 1 + exp_len; /* eNNN */

        if (len > 1) {
            width--;
        }

        if (width <= 0) {
            /* Overflow */
            if (error != NULL) {
                *error = true;
            }

            width = 0;
        }

        /* Do we have to truncate any digits? */
        if (width < len) {
            /* Yes, re-convert with a smaller width */
            dtoa_free(res, buf, sizeof(buf));
            res = dtoa(x, 4, width, &decpt, &sign, &end, buf, sizeof(buf));
            src = res;
            len = int(end - res);

            if (--decpt < 0) {
                decpt = -decpt;
            }
        }

        /*
            At this point we are sure we have enough space to put all digits
            returned by dtoa
        */
        if (sign && dst < dend) {
            *dst++ = '-';
        }

        if (dst < dend) {
            *dst++ = *src++;
        }

        if (len > 1 && dst < dend) {
            *dst++ = '.';

            while (src < end && dst < dend) {
                *dst++ = *src++;
            }
        }

        if (dst < dend) {
            *dst++ = 'e';
        }

        if (decpt_sign && dst < dend) {
            *dst++ = '-';
        }

        if (decpt >= 100 && dst < dend) {
            *dst++ = char(decpt / 100 + '0');
            decpt %= 100;

            if (dst < dend) {
                *dst++ = char(decpt / 10 + '0');
            }
        } else if (decpt >= 10 && dst < dend) {
            *dst++ = char(decpt / 10 + '0');
        }

        if (dst < dend) {
            *dst++ = char(decpt % 10 + '0');
        }

    }

    dtoa_free(res, buf, sizeof(buf));
    *dst = '\0';

    return dst - to;
}

/****************************************************************
 *
 * The author of this software is David M. Gay.
 *
 * Copyright (c) 1991, 2000, 2001 by Lucent Technologies.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose without fee is hereby granted, provided that this entire notice
 * is included in all copies of any software which is or includes a copy
 * or modification of this software and in all copies of the supporting
 * documentation for such software.
 *
 * THIS SOFTWARE IS BEING PROVIDED "AS IS", WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTY.  IN PARTICULAR, NEITHER THE AUTHOR NOR LUCENT MAKES ANY
 * REPRESENTATION OR WARRANTY OF ANY KIND CONCERNING THE MERCHANTABILITY
 * OF THIS SOFTWARE OR ITS FITNESS FOR ANY PARTICULAR PURPOSE.
 *
 ***************************************************************/

/*
    Original copy of the software is located at http://www.netlib.org/fp/dtoa.c
    It was adjusted to serve MySQL server needs:
    * strtod() was modified to not expect a zero-terminated string.
      It now honors 'se' (end of string) argument as the input parameter,
      not just as the output one.
    * in dtoa(), in case of overflow/underflow/NaN result string now contains "0";
      decpt is set to DTOA_OVERFLOW to indicate overflow.
    * support for VAX, IBM mainframe and 16-bit hardware removed
    * we always assume that 64-bit integer type is available
    * support for Kernigan-Ritchie style headers (pre-ANSI compilers)
      removed
    * all gcc warnings ironed out
    * we always assume multithreaded environment, so we had to change
      memory allocation procedures to use stack in most cases;
      malloc is used as the last resort.
    * pow5mult rewritten to use pre-calculated pow5 list instead of
      the one generated on the fly.
*/

typedef int32_t Long;
typedef uint32_t ULong;
typedef int64_t LLong;
typedef uint64_t ULLong;

typedef union {
    double d;
    ULong L[2];
} U;

const int Exp_shift = 20;
const int Exp_shift1 = 20;
const int P = 53;
const int Bias = 1023;
const int Emin = (-1022);
const int Ebits = 11;
const int Ten_pmax = 22;
const int Quick_max = 14;
const int Int_max = 14;

void test()
{
    //#ifdef FLT_ROUNDS
    //    std::cout << "FLT_ROUNDS yes=" << FLT_ROUNDS << std::endl;
    //#else
    //    std::cout << "FLT_ROUNDS no" << std::endl;
    //#endif
    //#ifdef Check_FLT_ROUNDS
    //    std::cout << "Check_FLT_ROUNDS yes=" << Check_FLT_ROUNDS << std::endl;
    //#else
    //    std::cout << "Check_FLT_ROUNDS no" << std::endl;
    //#endif
    //#ifdef DEBUG
    //    std::cout << "DEBUG yes" << std::endl;
    //#else
    //    std::cout << "DEUB no" << std::endl;
    //#endif
    //#ifdef MAX_DECPT_FOR_F_FORMAT2
    //    std::cout << "MAX_DECPT_FOR_F_FORMAT2 yes =" << MAX_DECPT_FOR_F_FORMAT2 <<std::endl;
    //#else
    //    std::cout << "MAX_DECPT_FOR_F_FORMAT2 no" << std::endl;
    //#endif 
    //#ifdef WORDS_BIGENDIAN
    //    std::cout << "WORDS_BIGENDIAN yes =" << WORDS_BIGENDIAN <<std::endl;
    //#else
    //    std::cout << "WORDS_BIGENDIAN no" << std::endl;
    //#endif 
    //#ifdef __FLOAT_WORD_ORDER
    //    std::cout << "__FLOAT_WORD_ORDER yes =" << __FLOAT_WORD_ORDER <<std::endl;
    //#else
    //    std::cout << "__FLOAT_WORD_ORDER no" << std::endl;
    //#endif 
    //#ifdef __BIG_ENDIAN
    //    std::cout << "__BIG_ENDIAN yes =" << __BIG_ENDIAN <<std::endl;
    //#else
    //    std::cout << "__BIG_ENDIAN no" << std::endl;
    //#endif 
    //#ifdef Flt_Rounds
    //    std::cout << "Flt_Rounds yes =" << Flt_Rounds <<std::endl;
    //#else
    //    std::cout << "Flt_Rounds no" << std::endl;
    //#endif 
    //#ifdef Honor_FLT_ROUNDS
    //    std::cout << "Honor_FLT_ROUNDS yes =" << Honor_FLT_ROUNDS <<std::endl;
    //#else
    //    std::cout << "Honor_FLT_ROUNDS no" << std::endl;
    //#endif 
    //#ifdef Rounding
    //    std::cout << "Rounding yes =" << Rounding <<std::endl;
    //#else
    //    std::cout << "Rounding no" << std::endl;
    //#endif 
}

const int Big1 = 0xffffffff;
const uint32_t FFFFFFFF = 0xffffffffUL;

/* This is tested to be enough for dtoa */
const int Kmax = 15;

/* Arbitrary-length integer */

typedef struct Bigint {
    union {
        ULong* x;              /* points right after this Bigint object */
        struct Bigint* next;   /* to maintain free lists */
    } p;
    int k;                   /* 2^k = maxwds */
    int maxwds;              /* maximum length in 32-bit words */
    int sign;                /* not zero if number is negative */
    int wds;                 /* current length in 32-bit words */
} Bigint;

/* A simple stack-memory based allocator for Bigints */

typedef struct Stack_alloc {
    char* begin;
    char* free;
    char* end;
    /*
      Having list of free blocks lets us reduce maximum required amount
      of memory from ~4000 bytes to < 1680 (tested on x86).
    */
    Bigint* freelist[Kmax + 1];
} Stack_alloc;

/*
    Try to allocate object on stack, and resort to malloc if all
    stack memory is used. Ensure allocated objects to be aligned by the pointer
    size in order to not break the alignment rules when storing a pointer to a
    Bigint.
*/

static Bigint* balloc(int k, Stack_alloc* alloc) {
    Bigint* rv;

    if (k <= Kmax &&  alloc->freelist[k]) {
        rv = alloc->freelist[k];
        alloc->freelist[k] = rv->p.next;
    } else {
        int x = 0;
        int len = 0;

        x = 1 << k;
        len = int(MY_ALIGN(sizeof(Bigint) + x * sizeof(ULong), SIZEOF_CHARP));

        if (alloc->free + len <= alloc->end) {
            rv = (Bigint*) alloc->free;
            alloc->free += len;
        } else {
            rv = (Bigint*) malloc(len);
        }

        rv->k = k;
        rv->maxwds = x;
    }

    rv->sign = rv->wds = 0;
    rv->p.x = (ULong*)(rv + 1);
    return rv;
}

/*
    If object was allocated on stack, try putting it to the free
    list. Otherwise call free().
*/

static void bfree(Bigint* v, Stack_alloc* alloc) {
    char* gptr = (char*) v;                      /* generic pointer */

    if (gptr < alloc->begin || gptr >= alloc->end) {
        free(gptr);
    } else if (v->k <= Kmax) {
        /*
          Maintain free lists only for stack objects: this way we don't
          have to bother with freeing lists in the end of dtoa;
          heap should not be used normally anyway.
        */
        v->p.next = alloc->freelist[v->k];
        alloc->freelist[v->k] = v;
    }
}

/*
    This is to place return value of dtoa in: tries to use stack
    as well, but passes by free lists management and just aligns len by
    the pointer size in order to not break the alignment rules when storing a
    pointer to a Bigint.
*/

static char* dtoa_alloc(int i, Stack_alloc* alloc) {
    char* rv = NULL;
    int aligned_size = MY_ALIGN(i, SIZEOF_CHARP);

    if (alloc->free + aligned_size <= alloc->end) {
        rv = alloc->free;
        alloc->free += aligned_size;
    } else {
        rv = (char*)malloc(i);
    }

    return rv;
}

/*
    dtoa_free() must be used to free values s returned by dtoa()
    This is the counterpart of dtoa_alloc()
*/

static void dtoa_free(char* gptr, char* buf, size_t buf_size) {
    if (gptr < buf || gptr >= buf + buf_size) {
        free(gptr);
    }
}

/* Bigint arithmetic functions */

/* Multiply by m and add a */

static Bigint* multadd(Bigint* b, int m, int a, Stack_alloc* alloc) {
    int i = 0;
    int wds = 0;
    ULong* x = NULL;
    ULLong carry;
    ULLong y;
    Bigint* b1;

    wds = b->wds;
    x = b->p.x;
    i = 0;
    carry = a;

    do {
        y = *x * (ULLong)m + carry;
        carry = y >> 32;
        *x++ = (ULong)(y & FFFFFFFF);
    } while (++i < wds);

    if (carry) {
        if (wds >= b->maxwds) {
            b1 = balloc(b->k + 1, alloc);
            memcpy((char *)&b1->sign, (char *)&b->sign, 2*sizeof(int) + b->wds*sizeof(ULong));
            bfree(b, alloc);
            b = b1;
        }

        b->p.x[wds++] = (ULong) carry;
        b->wds = wds;
    }

    return b;
}

static int hi0bits(register ULong x) {
    register int k = 0;

    if (!(x & 0xffff0000)) {
        k = 16;
        x <<= 16;
    }

    if (!(x & 0xff000000)) {
        k += 8;
        x <<= 8;
    }

    if (!(x & 0xf0000000)) {
        k += 4;
        x <<= 4;
    }

    if (!(x & 0xc0000000)) {
        k += 2;
        x <<= 2;
    }

    if (!(x & 0x80000000)) {
        k++;

        if (!(x & 0x40000000)) {
            return 32;
        }
    }

    return k;
}

static int lo0bits(ULong* y) {
    register int k = 0;
    register ULong x = *y;

    if (x & 7) {
        if (x & 1) {
            return 0;
        }

        if (x & 2) {
            *y = x >> 1;
            return 1;
        }

        *y = x >> 2;
        return 2;
    }

    k = 0;

    if (!(x & 0xffff)) {
        k = 16;
        x >>= 16;
    }

    if (!(x & 0xff)) {
        k += 8;
        x >>= 8;
    }

    if (!(x & 0xf)) {
        k += 4;
        x >>= 4;
    }

    if (!(x & 0x3)) {
        k += 2;
        x >>= 2;
    }

    if (!(x & 1)) {
        k++;
        x >>= 1;

        if (!x) {
            return 32;
        }
    }

    *y = x;
    return k;
}

/* Convert integer to Bigint number */

static Bigint* i2b(int i, Stack_alloc* alloc) {
    Bigint* b;

    b = balloc(1, alloc);
    b->p.x[0] = i;
    b->wds = 1;
    return b;
}

/* Multiply two Bigint numbers */

static Bigint* mult(Bigint* a, Bigint* b, Stack_alloc* alloc) {
    Bigint* c;
    int k = 0;
    int wa = 0;
    int wb = 0;
    int wc = 0;
    ULong* x = NULL;
    ULong* xa = NULL;
    ULong* xae = NULL;
    ULong* xb = NULL;
    ULong* xbe = NULL;
    ULong* xc = NULL;
    ULong* xc0 = NULL;
    ULong y;
    ULLong carry;
    ULLong z;

    if (a->wds < b->wds) {
        c = a;
        a = b;
        b = c;
    }

    k = a->k;
    wa = a->wds;
    wb = b->wds;
    wc = wa + wb;

    if (wc > a->maxwds) {
        k++;
    }

    c = balloc(k, alloc);

    for (x = c->p.x, xa = x + wc; x < xa; x++) {
        *x = 0;
    }

    xa = a->p.x;
    xae = xa + wa;
    xb = b->p.x;
    xbe = xb + wb;
    xc0 = c->p.x;

    for (; xb < xbe; xc0++) {
        if ((y = *xb++)) {
            x = xa;
            xc = xc0;
            carry = 0;

            do {
                z = *x++ * (ULLong)y + *xc + carry;
                carry = z >> 32;
                *xc++ = (ULong)(z & FFFFFFFF);
            } while (x < xae);

            *xc = (ULong) carry;
        }
    }

    for (xc0 = c->p.x, xc = xc0 + wc; wc > 0 && !*--xc; --wc) {}

    c->wds = wc;
    return c;
}

/*
    Precalculated array of powers of 5: tested to be enough for
    vasting majority of dtoa_r cases.
*/

static ULong powers5[] = {
    625UL,

    390625UL,

    2264035265UL, 35UL,

    2242703233UL, 762134875UL,  1262UL,

    3211403009UL, 1849224548UL, 3668416493UL, 3913284084UL, 1593091UL,

    781532673UL,  64985353UL,   253049085UL,  594863151UL,  3553621484UL,
    3288652808UL, 3167596762UL, 2788392729UL, 3911132675UL, 590UL,

    2553183233UL, 3201533787UL, 3638140786UL, 303378311UL, 1809731782UL,
    3477761648UL, 3583367183UL, 649228654UL, 2915460784UL, 487929380UL,
    1011012442UL, 1677677582UL, 3428152256UL, 1710878487UL, 1438394610UL,
    2161952759UL, 4100910556UL, 1608314830UL, 349175UL
};

static Bigint p5_a[] = {
    /*  { x } - k - maxwds - sign - wds */
    { { powers5 }, 1, 1, 0, 1 },
    { { powers5 + 1 }, 1, 1, 0, 1 },
    { { powers5 + 2 }, 1, 2, 0, 2 },
    { { powers5 + 4 }, 2, 3, 0, 3 },
    { { powers5 + 7 }, 3, 5, 0, 5 },
    { { powers5 + 12 }, 4, 10, 0, 10 },
    { { powers5 + 22 }, 5, 19, 0, 19 }
};

static Bigint* pow5mult(Bigint* b, int k, Stack_alloc* alloc) {
    Bigint* b1 = NULL;
    Bigint* p5 = NULL;
    Bigint* p51 = NULL;
    int i = 0;
    static int p05[3] = { 5, 25, 125 };
    bool overflow = false;

    if ((i = k & 3)) {
        b = multadd(b, p05[i - 1], 0, alloc);
    }

    if (!(k >>= 2)) {
        return b;
    }

    p5 = p5_a;

    for (;;) {
        if (k & 1) {
            b1 = mult(b, p5, alloc);
            bfree(b, alloc);
            b = b1;
        }

        if (!(k >>= 1)) {
            break;
        }

        /* Calculate next power of 5 */
        if (overflow) {
            p51 = mult(p5, p5, alloc);
            bfree(p5, alloc);
            p5 = p51;
        } else if (p5 < p5_a + (sizeof(p5_a) / sizeof(*p5_a) - 1)) {
            ++p5;
        } else if (p5 == p5_a + (sizeof(p5_a) / sizeof(*p5_a) - 1)) {
            p5 = mult(p5, p5, alloc);
            overflow = true;
        }
    }

    if (p51) {
        bfree(p51, alloc);
    }

    return b;
}

static Bigint* lshift(Bigint* b, int k, Stack_alloc* alloc) {
    int i = 0;
    int k1 = 0;
    int n = 0;
    int n1 = 0;
    Bigint* b1 = NULL;
    ULong* x = NULL;
    ULong* x1 = NULL;
    ULong* xe = NULL;
    ULong z;

    n = k >> 5;
    k1 = b->k;
    n1 = n + b->wds + 1;

    for (i = b->maxwds; n1 > i; i <<= 1) {
        k1++;
    }

    b1 = balloc(k1, alloc);
    x1 = b1->p.x;

    for (i = 0; i < n; i++) {
        *x1++ = 0;
    }

    x = b->p.x;
    xe = x + b->wds;

    if (k &= 0x1f) {
        k1 = 32 - k;
        z = 0;

        do {
            *x1++ = *x << k | z;
            z = *x++ >> k1;
        } while (x < xe);

        if ((*x1 = z)) {
            ++n1;
        }
    } else
        do {
            *x1++ = *x++;
        } while (x < xe);

    b1->wds = n1 - 1;
    bfree(b, alloc);
    return b1;
}

static int cmp(Bigint* a, Bigint* b) {
    ULong* xa = NULL;
    ULong* xa0 = NULL;
    ULong* xb = NULL; 
    ULong* xb0 = NULL;
    int i = 0;
    int j = 0;

    i = a->wds;
    j = b->wds;

    if (i -= j) {
        return i;
    }

    xa0 = a->p.x;
    xa = xa0 + j;
    xb0 = b->p.x;
    xb = xb0 + j;

    for (;;) {
        if (*--xa != *--xb) {
            return *xa < *xb ? -1 : 1;
        }

        if (xa <= xa0) {
            break;
        }
    }

    return 0;
}

static Bigint* diff(Bigint* a, Bigint* b, Stack_alloc* alloc) {
    Bigint* c = NULL;
    int i = 0; 
    int wa = 0;
    int  wb = 0;
    ULong* xa = NULL;
    ULong* xae = NULL; 
    ULong* xb = NULL; 
    ULong* xbe = NULL; 
    ULong* xc = NULL;
    ULLong borrow;
    ULLong y;

    i = cmp(a, b);

    if (!i) {
        c = balloc(0, alloc);
        c->wds = 1;
        c->p.x[0] = 0;
        return c;
    }

    if (i < 0) {
        c = a;
        a = b;
        b = c;
        i = 1;
    } else {
        i = 0;
    }

    c = balloc(a->k, alloc);
    c->sign = i;
    wa = a->wds;
    xa = a->p.x;
    xae = xa + wa;
    wb = b->wds;
    xb = b->p.x;
    xbe = xb + wb;
    xc = c->p.x;
    borrow = 0;

    do {
        y = (ULLong) * xa++ - *xb++ - borrow;
        borrow = y >> 32 & (ULong)1;
        *xc++ = (ULong)(y & FFFFFFFF);
    } while (xb < xbe);

    while (xa < xae) {
        y = *xa++ - borrow;
        borrow = y >> 32 & (ULong)1;
        *xc++ = (ULong)(y & FFFFFFFF);
    }

    while (!*--xc) {
        wa--;
    }

    c->wds = wa;
    return c;
}

static Bigint* d2b(U* d, int* e, int* bits, Stack_alloc* alloc) {
    Bigint* b = NULL;
    int de = 0;
    int k = 0;
    ULong* x = NULL;
    ULong y;
    ULong z;
    int i = 0;

    b = balloc(1, alloc);
    x = b->p.x;

    z = (d)->L[1] & 0xfffff;
    (d)->L[1] &= 0x7fffffff;       /* clear sign bit, which we ignore */

    if ((de = (int)((d)->L[1] >> Exp_shift))) {
        z |= 0x100000;
    }

    if ((y = (d)->L[0])) {
        if ((k = lo0bits(&y))) {
            x[0] = y | z << (32 - k);
            z >>= k;
        } else {
            x[0] = y;
        }

        i = b->wds = (x[1] = z) ? 2 : 1;
    } else {
        k = lo0bits(&z);
        x[0] = z;
        i = b->wds = 1;
        k += 32;
    }

    if (de) {
        *e = de - Bias - (P - 1) + k;
        *bits = P - k;
    } else {
        *e = de - Bias - (P - 1) + 1 + k;
        *bits = 32 * i - hi0bits(x[i - 1]);
    }

    return b;
}

static const double tens[] = {
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
    1e20, 1e21, 1e22
};

static const double bigtens[] = { 1e16, 1e32, 1e64, 1e128, 1e256 };
static const double tinytens[] = {
    1e-16, 1e-32, 1e-64, 1e-128,
    9007199254740992.*9007199254740992.e-256 /* = 2^106 * 1e-53 */
};
/*
    The factor of 2^53 in tinytens[4] helps us avoid setting the underflow
    flag unnecessarily.  It leads to a song and dance at the end of strtod.
*/
const int  n_bigtens = 5;

static int quorem(Bigint* b, Bigint* S) {
    int n = 0;
    ULong* bx = NULL;
    ULong *bxe = NULL; 
    ULong q;
    ULong *sx = NULL;
    ULong *sxe = NULL;
    ULLong borrow;
    ULLong carry;
    ULLong y;
    ULLong ys;

    n = S->wds;

    if (b->wds < n) {
        return 0;
    }

    sx = S->p.x;
    sxe = sx + --n;
    bx = b->p.x;
    bxe = bx + n;
    q = *bxe / (*sxe + 1); /* ensure q <= true quotient */

    if (q) {
        borrow = 0;
        carry = 0;

        do {
            ys = *sx++ * (ULLong)q + carry;
            carry = ys >> 32;
            y = *bx - (ys & FFFFFFFF) - borrow;
            borrow = y >> 32 & (ULong)1;
            *bx++ = (ULong)(y & FFFFFFFF);
        } while (sx <= sxe);

        if (!*bxe) {
            bx = b->p.x;

            while (--bxe > bx && !*bxe) {
                --n;
            }

            b->wds = n;
        }
    }

    if (cmp(b, S) >= 0) {
        q++;
        borrow = 0;
        carry = 0;
        bx = b->p.x;
        sx = S->p.x;

        do {
            ys = *sx++ + carry;
            carry = ys >> 32;
            y = *bx - (ys & FFFFFFFF) - borrow;
            borrow = y >> 32 & (ULong)1;
            *bx++ = (ULong)(y & FFFFFFFF);
        } while (sx <= sxe);

        bx = b->p.x;
        bxe = bx + n;

        if (!*bxe) {
            while (--bxe > bx && !*bxe) {
                --n;
            }

            b->wds = n;
        }
    }

    return q;
}


/*
     dtoa for IEEE arithmetic (dmg): convert double to ASCII string.

     Inspired by "How to Print Floating-Point Numbers Accurately" by
     Guy L. Steele, Jr. and Jon L. White [Proc. ACM SIGPLAN '90, pp. 112-126].

     Modifications:
          1. Rather than iterating, we use a simple numeric overestimate
             to determine k= floor(log10(d)).  We scale relevant
             quantities using O(log2(k)) rather than O(k) multiplications.
          2. For some modes > 2 (corresponding to ecvt and fcvt), we don't
             try to generate digits strictly left to right.  Instead, we
             compute with fewer bits and propagate the carry if necessary
                 when rounding the final digit up.  This is often faster.
              3. Under the assumption that input will be rounded nearest,
                 mode 0 renders 1e23 as 1e23 rather than 9.999999999999999e22.
                 That is, we allow equality in stopping tests when the
                 round-nearest rule will give the same floating-point value
                 as would satisfaction of the stopping test with strict
                 inequality.
              4. We remove common factors of powers of 2 from relevant
                 quantities.
              5. When converting floating-point integers less than 1e16,
                 we use floating-point arithmetic rather than resorting
                 to multiple-precision integers.
              6. When asked to produce fewer than 15 digits, we first try
                 to get by with floating-point arithmetic; we resort to
                 multiple-precision integer arithmetic only if we cannot
                 guarantee that the floating-point calculation has given
                 the correctly rounded result.  For k requested digits and
                 "uniformly" distributed input, the probability is
                 something like 10^(k-15) that we must resort to the Long
                 calculation.
     */

    static char* dtoa(double dd, int mode, int ndigits, int* decpt, int* sign,
            char** rve, char* buf, size_t buf_size) {
        /*
          Arguments ndigits, decpt, sign are similar to those
          of ecvt and fcvt; trailing zeros are suppressed from
          the returned string.  If not null, *rve is set to point
          to the end of the return value.  If d is +-Infinity or NaN,
          then *decpt is set to DTOA_OVERFLOW.

          mode:
                0 ==> shortest string that yields d when read in
                      and rounded to nearest.
                1 ==> like 0, but with Steele & White stopping rule;
                      e.g. with IEEE P754 arithmetic , mode 0 gives
                      1e23 whereas mode 1 gives 9.999999999999999e22.
                2 ==> max(1,ndigits) significant digits.  This gives a
                      return value similar to that of ecvt, except
                      that trailing zeros are suppressed.
                3 ==> through ndigits past the decimal point.  This
                      gives a return value similar to that from fcvt,
                      except that trailing zeros are suppressed, and
                      ndigits can be negative.
                4,5 ==> similar to 2 and 3, respectively, but (in
                      round-nearest mode) with the tests of mode 0 to
                      possibly return a shorter string that rounds to d.
                      With IEEE arithmetic and compilation with
                      -DHonor_FLT_ROUNDS, modes 4 and 5 behave the same
                      as modes 2 and 3 when FLT_ROUNDS != 1.
                6-9 ==> Debugging modes similar to mode - 4:  don't try
                      fast floating-point estimate (if applicable).

            Values of mode other than 0-9 are treated as mode 0.

          Sufficient space is allocated to the return value
          to hold the suppressed trailing zeros.
        */

        int bbits = 0;
        int b2 = 0;
        int b5 = 0;
        int be = 0;
        int dig = 0;
        int i = 0;
        int ieps = 0;
        int ilim = 0;
        int ilim0 = 0;
        int ilim1 = 0;
        int j = 0;
        int j1 = 0;
        int k = 0;
        int k0 = 0;
        int k_check = 0;
        int leftright = 0;
        int m2 = 0;
        int m5 = 0;
        int s2 = 0;
        int s5 = 0;
        int spec_case = 0;
        int try_quick = 0;
        Long L = 0;
        int denorm = 0;
        ULong x = 0;
        Bigint* b = NULL;
        Bigint* b1 = NULL;
        Bigint* delta = NULL;
        Bigint* mlo = NULL;
        Bigint* mhi = NULL;
        Bigint* S = NULL;
        U d2;
        U eps;
        U u;
        double ds = 0;
        char* s = NULL;
        char* s0 = NULL;
        Stack_alloc alloc;

        alloc.begin = alloc.free = buf;
        alloc.end = buf + buf_size;
        memset(alloc.freelist, 0, sizeof(alloc.freelist));

        u.d = dd;

        if ((&u)->L[1] & 0x80000000) {
            /* set sign for everything, including 0's and NaNs */
            *sign = 1;
            (&u)->L[1] &= ~0x80000000;  /* clear sign bit */
        } else {
            *sign = 0;
        }

        /* If infinity, set decpt to DTOA_OVERFLOW, if 0 set it to 1 */
        if ((((&u)->L[1] & 0x7ff00000) == 0x7ff00000 && (*decpt = DTOA_OVERFLOW)) ||
                (!(&u)->d && (*decpt = 1))) {
            /* Infinity, NaN, 0 */
            char* res = (char*) dtoa_alloc(2, &alloc);
            res[0] = '0';
            res[1] = '\0';

            if (rve) {
                *rve = res + 1;
            }

            return res;
        }

        b = d2b(&u, &be, &bbits, &alloc);

        if ((i = (int)((&u)->L[1] >> Exp_shift1 & (0x7ff00000 >> Exp_shift1)))) {
            (&d2)->d = (&u)->d;
            (&d2)->L[1] &= 0xfffff;
            (&d2)->L[1] |= 0x3ff00000;

            /*
              log(x)       ~=~ log(1.5) + (x-1.5)/1.5
              log10(x)      =  log(x) / log(10)
                           ~=~ log(1.5)/log(10) + (x-1.5)/(1.5*log(10))
              log10(d)= (i-Bias)*log(2)/log(10) + log10(d2)

              This suggests computing an approximation k to log10(d) by

              k= (i - Bias)*0.301029995663981
                   + ( (d2-1.5)*0.289529654602168 + 0.176091259055681 );

              We want k to be too large rather than too small.
              The error in the first-order Taylor series approximation
              is in our favor, so we just round up the constant enough
              to compensate for any error in the multiplication of
              (i - Bias) by 0.301029995663981; since |i - Bias| <= 1077,
              and 1077 * 0.30103 * 2^-52 ~=~ 7.2e-14,
              adding 1e-13 to the constant term more than suffices.
              Hence we adjust the constant term to 0.1760912590558.
              (We could get a more accurate k by invoking log10,
               but this is probably not worthwhile.)
            */

            i -= Bias;
            denorm = 0;
        } else {
            /* d is denormalized */

            i = bbits + be + (Bias + (P - 1) - 1);
            x = i > 32  ? (&u)->L[1] << (64 - i) | (&u)->L[0] >> (i - 32)
                : (&u)->L[0] << (32 - i);
            (&d2)->d = x;
            (&d2)->L[1] -= 31 * 0x100000; /* adjust exponent */
            i -= (Bias + (P - 1) - 1) + 1;
            denorm = 1;
        }

        ds = ((&d2)->d - 1.5) * 0.289529654602168 + 0.1760912590558 + i * 0.301029995663981;
        k = (int)ds;
        if (ds < 0. && fabs(ds - k) >= EPSILON) {
            k--;    /* want k= floor(ds) */
        }

        k_check = 1;

        if (k >= 0 && k <= Ten_pmax) {
            if ((&u)->d < tens[k]) {
                k--;
            }

            k_check = 0;
        }

        j = bbits - i - 1;

        if (j >= 0) {
            b2 = 0;
            s2 = j;
        } else {
            b2 = -j;
            s2 = 0;
        }

        if (k >= 0) {
            b5 = 0;
            s5 = k;
            s2 += k;
        } else {
            b2 -= k;
            b5 = -k;
            s5 = 0;
        }

        if (mode < 0 || mode > 9) {
            mode = 0;
        }

    try_quick = 1;

    if (mode > 5) {
        mode -= 4;
        try_quick = 0;
    }

    leftright = 1;

    switch (mode) {
        case 0:
        case 1:
            ilim = ilim1 = -1;
            i = 18;
            ndigits = 0;
            break;

        case 2:
            leftright = 0;

            /* no break */
        case 4:
            if (ndigits <= 0) {
                ndigits = 1;
            }

            ilim = ilim1 = i = ndigits;
            break;

        case 3:
            leftright = 0;

            /* no break */
        case 5:
            i = ndigits + k + 1;
            ilim = i;
            ilim1 = i - 1;

            if (i <= 0) {
                i = 1;
            }
    }

    s = s0 = dtoa_alloc(i, &alloc);

    if (ilim >= 0 && ilim <= Quick_max && try_quick) {
        /* Try to get by with floating-point arithmetic. */
        i = 0;
        (&d2)->d = (&u)->d;
        k0 = k;
        ilim0 = ilim;
        ieps = 2; /* conservative */

        if (k > 0) {
            ds = tens[k & 0xf];
            j = k >> 4;

            if (j & 0x10) {
                /* prevent overflows */
                j &= 0x10 - 1;
                (&u)->d /= bigtens[n_bigtens - 1];
                ieps++;
            }

            for (; j; j >>= 1, i++) {
                if (j & 1) {
                    ieps++;
                    ds *= bigtens[i];
                }
            }

            (&u)->d /= ds;
        } else if ((j1 = -k)) {
            (&u)->d *= tens[j1 & 0xf];

            for (j = j1 >> 4; j; j >>= 1, i++) {
                if (j & 1) {
                    ieps++;
                    (&u)->d *= bigtens[i];
                }
            }
        }

        if (k_check && (&u)->d < 1. && ilim > 0) {
            if (ilim1 <= 0) {
                goto fast_failed;
            }

            ilim = ilim1;
            k--;
            (&u)->d *= 10.;
            ieps++;
        }

        (&eps)->d = ieps * (&u)->d + 7.;
        (&eps)->L[1] -= (P - 1) * 0x100000;

        if (ilim == 0) {
            S = mhi = 0;
            (&u)->d -= 5.;

            if ((&u)->d > (&eps)->d) {
                goto one_digit;
            }

            if ((&u)->d < -(&eps)->d) {
                goto no_digits;
            }

            goto fast_failed;
        }

        if (leftright) {
            /* Use Steele & White method of only generating digits needed. */
            (&eps)->d = 0.5 / tens[ilim - 1] - (&eps)->d;

            for (i = 0;;) {
                L = (Long) (&u)->d;
                (&u)->d -= L;
                *s++ = char('0' + (int)L);

                if ((&u)->d < (&eps)->d) {
                    goto ret1;
                }

                if (1. - (&u)->d < (&eps)->d) {
                    goto bump_up;
                }

                if (++i >= ilim) {
                    break;
                }

                (&eps)->d *= 10.;
                (&u)->d *= 10.;
            }
        } else {
            /* Generate ilim digits, then fix them up. */
            (&eps)->d *= tens[ilim - 1];

            for (i = 1;; i++, (&u)->d *= 10.) {
                L = (Long)((&u)->d);

                if (!((&u)->d -= L)) {
                    ilim = i;
                }

                *s++ = char('0' + (int)L);

                if (i == ilim) {
                    if ((&u)->d > 0.5 + (&eps)->d) {
                        goto bump_up;
                    } else if ((&u)->d < 0.5 - (&eps)->d) {
                        while (*--s == '0') {}

                        s++;
                        goto ret1;
                    }

                    break;
                }
            }
        }

fast_failed:
        s = s0;
        (&u)->d = (&d2)->d;
        k = k0;
        ilim = ilim0;
    }

    /* Do we have a "small" integer? */

    if (be >= 0 && k <= Int_max) {
        /* Yes. */
        ds = tens[k];

        if (ndigits < 0 && ilim <= 0) {
            S = mhi = 0;

            if (ilim < 0 || (&u)->d <= 5 * ds) {
                goto no_digits;
            }

            goto one_digit;
        }

        for (i = 1;; i++, (&u)->d *= 10.) {
            L = (Long)((&u)->d / ds);
            (&u)->d -= L * ds;
            *s++ = char('0' + (int)L);

            if (!(&u)->d) {
                break;
            }

            if (i == ilim) {
                (&u)->d += (&u)->d;

                if ((&u)->d > ds || ((&u)->d == ds && L & 1)) {
bump_up:

                    while (*--s == '9') {
                        if (s == s0) {
                            k++;
                            *s = '0';
                            break;
                        }
                    }

                    ++*s++;
                }

                break;
            }
        }

        goto ret1;
    }

    m2 = b2;
    m5 = b5;
    mhi = mlo = 0;

    if (leftright) {
        i = denorm ? be + (Bias + (P - 1) - 1 + 1) : 1 + P - bbits;
        b2 += i;
        s2 += i;
        mhi = i2b(1, &alloc);
    }

    if (m2 > 0 && s2 > 0) {
        i = m2 < s2 ? m2 : s2;
        b2 -= i;
        m2 -= i;
        s2 -= i;
    }

    if (b5 > 0) {
        if (leftright) {
            if (m5 > 0) {
                mhi = pow5mult(mhi, m5, &alloc);
                b1 = mult(mhi, b, &alloc);
                bfree(b, &alloc);
                b = b1;
            }

            if ((j = b5 - m5)) {
                b = pow5mult(b, j, &alloc);
            }
        } else {
            b = pow5mult(b, b5, &alloc);
        }
    }

    S = i2b(1, &alloc);

    if (s5 > 0) {
        S = pow5mult(S, s5, &alloc);
    }

    /* Check for special case that d is a normalized power of 2. */

    spec_case = 0;

    if ((mode < 2 || leftright)) {
        if (!(&u)->L[0] && !((&u)->L[1] & 0xfffff) &&
                (&u)->L[1] & (0x7ff00000 & ~0x100000)
           ) {
            /* The special case */
            b2 += 1;
            s2 += 1;
            spec_case = 1;
        }
    }

    /*
      Arrange for convenient computation of quotients:
      shift left if necessary so divisor has 4 leading 0 bits.

      Perhaps we should just compute leading 28 bits of S once
      a nd for all and pass them and a shift to quorem, so it
      can do shifts and ors to compute the numerator for q.
    */
    if ((i = ((s5 ? 32 - hi0bits(S->p.x[S->wds - 1]) : 1) + s2) & 0x1f)) {
        i = 32 - i;
    }

    if (i > 4) {
        i -= 4;
        b2 += i;
        m2 += i;
        s2 += i;
    } else if (i < 4) {
        i += 28;
        b2 += i;
        m2 += i;
        s2 += i;
    }

    if (b2 > 0) {
        b = lshift(b, b2, &alloc);
    }

    if (s2 > 0) {
        S = lshift(S, s2, &alloc);
    }

    if (k_check) {
        if (cmp(b, S) < 0) {
            k--;
            /* we botched the k estimate */
            b = multadd(b, 10, 0, &alloc);

            if (leftright) {
                mhi = multadd(mhi, 10, 0, &alloc);
            }

            ilim = ilim1;
        }
    }

    if (ilim <= 0 && (mode == 3 || mode == 5)) {
        if (ilim < 0 || cmp(b, S = multadd(S, 5, 0, &alloc)) <= 0) {
            /* no digits, fcvt style */
no_digits:
            k = -1 - ndigits;
            goto ret;
        }

one_digit:
        *s++ = '1';
        k++;
        goto ret;
    }

    if (leftright) {
        if (m2 > 0) {
            mhi = lshift(mhi, m2, &alloc);
        }

        /*
          Compute mlo -- check for special case that d is a normalized power of 2.
        */

        mlo = mhi;

        if (spec_case) {
            mhi = balloc(mhi->k, &alloc);

            memcpy((char *)&mhi->sign, (char *)&mlo->sign, 2*sizeof(int) + mlo->wds*sizeof(ULong));
            mhi = lshift(mhi, 1, &alloc);
        }

        for (i = 1;; i++) {
            dig = quorem(b, S) + '0';
            /* Do we yet have the shortest decimal string that will round to d? */
            j = cmp(b, mlo);
            delta = diff(S, mhi, &alloc);
            j1 = delta->sign ? 1 : cmp(b, delta);
            bfree(delta, &alloc);

            if (j1 == 0 && mode != 1 && !((&u)->L[0] & 1)) {
                if (dig == '9') {
                    goto round_9_up;
                }

                if (j > 0) {
                    dig++;
                }

                *s++ = char(dig);
                goto ret;
            }

            if (j < 0 || (j == 0 && mode != 1 && !((&u)->L[0] & 1))) {
                if (!b->p.x[0] && b->wds <= 1) {
                    goto accept_dig;
                }

                if (j1 > 0) {
                    b = lshift(b, 1, &alloc);
                    j1 = cmp(b, S);

                    if ((j1 > 0 || (j1 == 0 && dig & 1))
                            && dig++ == '9') {
                        goto round_9_up;
                    }
                }

accept_dig:
                *s++ = char(dig);
                goto ret;
            }

            if (j1 > 0) {
                if (dig == '9') {
                    /* possible if i == 1 */
round_9_up:
                    *s++ = '9';
                    goto roundoff;
                }

                *s++ = char(dig + 1);
                goto ret;
            }

            *s++ = char(dig);

            if (i == ilim) {
                break;
            }

            b = multadd(b, 10, 0, &alloc);

            if (mlo == mhi) {
                mlo = mhi = multadd(mhi, 10, 0, &alloc);
            } else {
                mlo = multadd(mlo, 10, 0, &alloc);
                mhi = multadd(mhi, 10, 0, &alloc);
            }
        }
    } else
        for (i = 1;; i++) {
            *s++ = char(dig = quorem(b, S) + '0');

            if (!b->p.x[0] && b->wds <= 1) {
                goto ret;
            }

            if (i >= ilim) {
                break;
            }

            b = multadd(b, 10, 0, &alloc);
        }

    /* Round off last digit */

    b = lshift(b, 1, &alloc);
    j = cmp(b, S);

    if (j > 0 || (j == 0 && dig & 1)) {
roundoff:

        while (*--s == '9') {
            if (s == s0) {
                k++;
                *s++ = '1';
                goto ret;
            }
        }

        ++*s++;
    } else {
        while (*--s == '0') {}

        s++;
    }

ret:
    bfree(S, &alloc);

    if (mhi) {
        if (mlo && mlo != mhi) {
            bfree(mlo, &alloc);
        }

        bfree(mhi, &alloc);
    }

ret1:
    bfree(b, &alloc);
    *s = 0;
    *decpt = k + 1;

    if (rve) {
        *rve = s;
    }

    return s0;
}
}
