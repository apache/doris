/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

#include <stddef.h>
#include "common.h"

#if defined(DOUBLE)
#define FLOAT_TYPE double
#elif defined(SINGLE)
#define FLOAT_TYPE float
#else
#endif

/* Notes for algorithm:
 * - Input denormal treated as zero
 * - Force to be QNAN
 */
static void bf16to_kernel_1(BLASLONG n, const bfloat16 * in, BLASLONG inc_in, FLOAT_TYPE * out, BLASLONG inc_out)
{
    BLASLONG register index_in  = 0;
    BLASLONG register index_out = 0;
    BLASLONG register index     = 0;
    uint16_t * tmp = NULL;
#if defined(DOUBLE)
    float             float_out = 0.0;
#endif

    while(index<n) {
#if defined(DOUBLE)
        float_out = 0.0;
        tmp = (uint16_t*)(&float_out);
#else
        *(out+index_out) = 0;
        tmp = (uint16_t*)(out+index_out);
#endif

        switch((*(in+index_in)) & 0xff80u) {
            case (0x0000u):   /* Type 1: Positive denormal */
                tmp[1] = 0x0000u;
                tmp[0] = 0x0000u;
                break;
            case (0x8000u):   /* Type 2: Negative denormal */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                tmp[1] = 0x8000u;
                tmp[0] = 0x0000u;
#else
                tmp[1] = 0x0000u;
                tmp[0] = 0x8000u;
#endif
                break;
            case (0x7f80u):   /* Type 3: Positive infinity or NAN */
            case (0xff80u):   /* Type 4: Negative infinity or NAN */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                tmp[1] = *(in+index_in);
#else
                tmp[0] = *(in+index_in);
#endif
                /* Specific for NAN */
                if (((*(in+index_in)) & 0x007fu) != 0) {
                    /* Force to be QNAN */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                    tmp[1] |= 0x0040u;
#else
                    tmp[0] |= 0x0040u;
#endif
                }
                break;
            default:              /* Type 5: Normal case */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
                tmp[1] = *(in+index_in);
#else
                tmp[0] = *(in+index_in);
#endif
                break;
        }
#if defined(DOUBLE)
	*(out+index_out) = (double)float_out;
#endif
        index_in  += inc_in;
        index_out += inc_out;
        index++;
    }
}

void CNAME(BLASLONG n, bfloat16 * in, BLASLONG inc_in, FLOAT_TYPE * out, BLASLONG inc_out)
{
    if (n <= 0)  return;

    bf16to_kernel_1(n, in, inc_in, out, inc_out);
}
