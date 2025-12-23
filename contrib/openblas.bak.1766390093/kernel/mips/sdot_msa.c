/*******************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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
*******************************************************************************/

#include "common.h"
#include "macros_msa.h"

#if defined(DSDOT)
double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#else
FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#endif
{
    BLASLONG i = 0;
    double dot = 0.0;
    FLOAT x0, x1, x2, x3, y0, y1, y2, y3;
    v4f32 vx0, vx1, vx2, vx3, vx4, vx5, vx6, vx7;
    v4f32 vy0, vy1, vy2, vy3, vy4, vy5, vy6, vy7;
#if defined(DSDOT)
    v2f64 dvx0, dvx1, dvx2, dvx3, dvx4, dvx5, dvx6, dvx7;
    v2f64 dvy0, dvy1, dvy2, dvy3, dvy4, dvy5, dvy6, dvy7;
    v2f64 dot0 = {0, 0};
    v2f64 dot1 = {0, 0};
    v2f64 dot2 = {0, 0};
    v2f64 dot3 = {0, 0};
#else
    v4f32 dot0 = {0, 0, 0, 0};
    v4f32 dot1 = {0, 0, 0, 0};
    v4f32 dot2 = {0, 0, 0, 0};
    v4f32 dot3 = {0, 0, 0, 0};
#endif

    if (n < 1) return (dot);

    if ((1 == inc_x) && (1 == inc_y))
    {
        FLOAT *x_pref, *y_pref;
        BLASLONG pref_offset;

        pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
        if (pref_offset > 0)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }
        x_pref = x + pref_offset + 64;

        pref_offset = (BLASLONG)y & (L1_DATA_LINESIZE - 1);
        if (pref_offset > 0)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }
        y_pref = y + pref_offset + 64;

        for (i = (n >> 5); i--;)
        {
            LD_SP8_INC(x, 4, vx0, vx1, vx2, vx3, vx4, vx5, vx6, vx7);
            LD_SP8_INC(y, 4, vy0, vy1, vy2, vy3, vy4, vy5, vy6, vy7);

            PREF_OFFSET(x_pref, 0);
            PREF_OFFSET(x_pref, 32);
            PREF_OFFSET(x_pref, 64);
            PREF_OFFSET(x_pref, 96);
            PREF_OFFSET(y_pref, 0);
            PREF_OFFSET(y_pref, 32);
            PREF_OFFSET(y_pref, 64);
            PREF_OFFSET(y_pref, 96);
            x_pref += 32;
            y_pref += 32;

#if defined(DSDOT)
            /* Extend single precision to double precision */
            dvy0 = __msa_fexupr_d(vy0);
            dvy1 = __msa_fexupr_d(vy1);
            dvy2 = __msa_fexupr_d(vy2);
            dvy3 = __msa_fexupr_d(vy3);
            dvy4 = __msa_fexupr_d(vy4);
            dvy5 = __msa_fexupr_d(vy5);
            dvy6 = __msa_fexupr_d(vy6);
            dvy7 = __msa_fexupr_d(vy7);

            vy0 = (v4f32)__msa_fexupl_d(vy0);
            vy1 = (v4f32)__msa_fexupl_d(vy1);
            vy2 = (v4f32)__msa_fexupl_d(vy2);
            vy3 = (v4f32)__msa_fexupl_d(vy3);
            vy4 = (v4f32)__msa_fexupl_d(vy4);
            vy5 = (v4f32)__msa_fexupl_d(vy5);
            vy6 = (v4f32)__msa_fexupl_d(vy6);
            vy7 = (v4f32)__msa_fexupl_d(vy7);

            dvx0 = __msa_fexupr_d(vx0);
            dvx1 = __msa_fexupr_d(vx1);
            dvx2 = __msa_fexupr_d(vx2);
            dvx3 = __msa_fexupr_d(vx3);
            dvx4 = __msa_fexupr_d(vx4);
            dvx5 = __msa_fexupr_d(vx5);
            dvx6 = __msa_fexupr_d(vx6);
            dvx7 = __msa_fexupr_d(vx7);

            vx0 = (v4f32)__msa_fexupl_d(vx0);
            vx1 = (v4f32)__msa_fexupl_d(vx1);
            vx2 = (v4f32)__msa_fexupl_d(vx2);
            vx3 = (v4f32)__msa_fexupl_d(vx3);
            vx4 = (v4f32)__msa_fexupl_d(vx4);
            vx5 = (v4f32)__msa_fexupl_d(vx5);
            vx6 = (v4f32)__msa_fexupl_d(vx6);
            vx7 = (v4f32)__msa_fexupl_d(vx7);

            dot0 += (dvy0 * dvx0);
            dot1 += (dvy1 * dvx1);
            dot2 += (dvy2 * dvx2);
            dot3 += (dvy3 * dvx3);
            dot0 += (dvy4 * dvx4);
            dot1 += (dvy5 * dvx5);
            dot2 += (dvy6 * dvx6);
            dot3 += (dvy7 * dvx7);
            dot0 += ((v2f64)vy0 * (v2f64)vx0);
            dot1 += ((v2f64)vy1 * (v2f64)vx1);
            dot2 += ((v2f64)vy2 * (v2f64)vx2);
            dot3 += ((v2f64)vy3 * (v2f64)vx3);
            dot0 += ((v2f64)vy4 * (v2f64)vx4);
            dot1 += ((v2f64)vy5 * (v2f64)vx5);
            dot2 += ((v2f64)vy6 * (v2f64)vx6);
            dot3 += ((v2f64)vy7 * (v2f64)vx7);
#else
            dot0 += (vy0 * vx0);
            dot1 += (vy1 * vx1);
            dot2 += (vy2 * vx2);
            dot3 += (vy3 * vx3);
            dot0 += (vy4 * vx4);
            dot1 += (vy5 * vx5);
            dot2 += (vy6 * vx6);
            dot3 += (vy7 * vx7);
#endif
        }

        if (n & 31)
        {
            if (n & 16)
            {
                LD_SP4_INC(x, 4, vx0, vx1, vx2, vx3);
                LD_SP4_INC(y, 4, vy0, vy1, vy2, vy3);

#if defined(DSDOT)
                dvy0 = __msa_fexupr_d(vy0);
                dvy1 = __msa_fexupr_d(vy1);
                dvy2 = __msa_fexupr_d(vy2);
                dvy3 = __msa_fexupr_d(vy3);

                vy0 = (v4f32)__msa_fexupl_d(vy0);
                vy1 = (v4f32)__msa_fexupl_d(vy1);
                vy2 = (v4f32)__msa_fexupl_d(vy2);
                vy3 = (v4f32)__msa_fexupl_d(vy3);

                dvx0 = __msa_fexupr_d(vx0);
                dvx1 = __msa_fexupr_d(vx1);
                dvx2 = __msa_fexupr_d(vx2);
                dvx3 = __msa_fexupr_d(vx3);

                vx0 = (v4f32)__msa_fexupl_d(vx0);
                vx1 = (v4f32)__msa_fexupl_d(vx1);
                vx2 = (v4f32)__msa_fexupl_d(vx2);
                vx3 = (v4f32)__msa_fexupl_d(vx3);

                dot0 += (dvy0 * dvx0);
                dot1 += (dvy1 * dvx1);
                dot2 += (dvy2 * dvx2);
                dot3 += (dvy3 * dvx3);
                dot0 += ((v2f64)vy0 * (v2f64)vx0);
                dot1 += ((v2f64)vy1 * (v2f64)vx1);
                dot2 += ((v2f64)vy2 * (v2f64)vx2);
                dot3 += ((v2f64)vy3 * (v2f64)vx3);
#else
                dot0 += (vy0 * vx0);
                dot1 += (vy1 * vx1);
                dot2 += (vy2 * vx2);
                dot3 += (vy3 * vx3);
#endif
            }

            if (n & 8)
            {
                LD_SP2_INC(x, 4, vx0, vx1);
                LD_SP2_INC(y, 4, vy0, vy1);

#if defined(DSDOT)
                dvy0 = __msa_fexupr_d(vy0);
                dvy1 = __msa_fexupr_d(vy1);

                vy0 = (v4f32)__msa_fexupl_d(vy0);
                vy1 = (v4f32)__msa_fexupl_d(vy1);

                dvx0 = __msa_fexupr_d(vx0);
                dvx1 = __msa_fexupr_d(vx1);

                vx0 = (v4f32)__msa_fexupl_d(vx0);
                vx1 = (v4f32)__msa_fexupl_d(vx1);

                dot0 += (dvy0 * dvx0);
                dot1 += (dvy1 * dvx1);
                dot0 += ((v2f64)vy0 * (v2f64)vx0);
                dot1 += ((v2f64)vy1 * (v2f64)vx1);
#else
                dot0 += (vy0 * vx0);
                dot1 += (vy1 * vx1);
#endif
            }

            if (n & 4)
            {
                vx0 = LD_SP(x); x += 4;
                vy0 = LD_SP(y); y += 4;

#if defined(DSDOT)
                dvy0 = __msa_fexupr_d(vy0);
                vy0 = (v4f32)__msa_fexupl_d(vy0);
                dvx0 = __msa_fexupr_d(vx0);
                vx0 = (v4f32)__msa_fexupl_d(vx0);
                dot0 += (dvy0 * dvx0);
                dot0 += ((v2f64)vy0 * (v2f64)vx0);
#else
                dot0 += (vy0 * vx0);
#endif
            }

            if (n & 2)
            {
                LD_GP2_INC(x, 1, x0, x1);
                LD_GP2_INC(y, 1, y0, y1);

#if defined(DSDOT)
                dot += ((double)y0 * (double)x0);
                dot += ((double)y1 * (double)x1);
#else
                dot += (y0 * x0);
                dot += (y1 * x1);
#endif
            }

            if (n & 1)
            {
                x0 = *x;
                y0 = *y;

#if defined(DSDOT)
                dot += ((double)y0 * (double)x0);
#else
                dot += (y0 * x0);
#endif
            }
        }

        dot0 += dot1 + dot2 + dot3;

        dot += dot0[0];
        dot += dot0[1];
#if !defined(DSDOT)
        dot += dot0[2];
        dot += dot0[3];
#endif
    }
    else
    {
        for (i = (n >> 2); i--;)
        {
            LD_GP4_INC(x, inc_x, x0, x1, x2, x3);
            LD_GP4_INC(y, inc_y, y0, y1, y2, y3);

#if defined(DSDOT)
            dot += ((double)y0 * (double)x0);
            dot += ((double)y1 * (double)x1);
            dot += ((double)y2 * (double)x2);
            dot += ((double)y3 * (double)x3);
#else
            dot += (y0 * x0);
            dot += (y1 * x1);
            dot += (y2 * x2);
            dot += (y3 * x3);
#endif
        }

        if (n & 2)
        {
            LD_GP2_INC(x, inc_x, x0, x1);
            LD_GP2_INC(y, inc_y, y0, y1);

#if defined(DSDOT)
            dot += ((double)y0 * (double)x0);
            dot += ((double)y1 * (double)x1);
#else
            dot += (y0 * x0);
            dot += (y1 * x1);
#endif
        }

        if (n & 1)
        {
            x0 = *x;
            y0 = *y;

#if defined(DSDOT)
            dot += ((double)y0 * (double)x0);
#else
            dot += (y0 * x0);
#endif
        }
    }

    return (dot);
}
