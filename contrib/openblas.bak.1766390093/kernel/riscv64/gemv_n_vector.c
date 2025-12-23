/***************************************************************************
Copyright (c) 2020, The OpenBLAS Project
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

#include "common.h"
#if !defined(DOUBLE)
#define VSETVL(n) RISCV_RVV(vsetvl_e32m8)(n)
#define FLOAT_V_T vfloat32m8_t
#define VLEV_FLOAT RISCV_RVV(vle32_v_f32m8)
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m8)
#define VSEV_FLOAT RISCV_RVV(vse32_v_f32m8)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m8)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m8)
#define VFMUL_VF_FLOAT RISCV_RVV(vfmul_vf_f32m8)
#define VFILL_ZERO_FLOAT RISCV_RVV(vfsub_vv_f32m8)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define FLOAT_V_T vfloat64m4_t
#define VLEV_FLOAT RISCV_RVV(vle64_v_f64m4)
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSEV_FLOAT RISCV_RVV(vse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFMUL_VF_FLOAT RISCV_RVV(vfmul_vf_f64m4)
#define VFILL_ZERO_FLOAT RISCV_RVV(vfsub_vv_f64m4)
#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
    BLASLONG i = 0, j = 0, k = 0;
    BLASLONG ix = 0, iy = 0;

    if(n < 0)  return(0);
    FLOAT *a_ptr = a;
    FLOAT temp[4];
    FLOAT_V_T va0, va1, vy0, vy1,vy0_temp, vy1_temp , temp_v ,va0_0 , va0_1 , va1_0 ,va1_1 ,va2_0 ,va2_1 ,va3_0 ,va3_1 ;
    unsigned int gvl = 0;
    if(inc_y == 1 && inc_x == 1){
        gvl = VSETVL(m);
        if(gvl <= m/2){
            for(k=0,j=0; k<m/(2*gvl); k++){
                a_ptr = a;
                ix = 0;
                vy0_temp = VLEV_FLOAT(&y[j], gvl);
                vy1_temp = VLEV_FLOAT(&y[j+gvl], gvl);
                vy0 = VFILL_ZERO_FLOAT(vy0 , vy0 , gvl);
                vy1 = VFILL_ZERO_FLOAT(vy1 , vy1 , gvl);
                int i;

                int remainder = n % 4;
                for(i = 0; i < remainder; i++){
                    temp[0] = x[ix];
                    va0 = VLEV_FLOAT(&a_ptr[j], gvl);
                    vy0 = VFMACCVF_FLOAT(vy0, temp[0], va0, gvl);

                    va1 = VLEV_FLOAT(&a_ptr[j+gvl], gvl);
                    vy1 = VFMACCVF_FLOAT(vy1, temp[0], va1, gvl);
                    a_ptr += lda;
                    ix ++;
                }

                for(i = remainder; i < n; i += 4){
                    va0_0 = VLEV_FLOAT(&(a_ptr)[j], gvl);
                    va0_1 = VLEV_FLOAT(&(a_ptr)[j+gvl], gvl);
                    va1_0 = VLEV_FLOAT(&(a_ptr+lda * 1)[j], gvl);
                    va1_1 = VLEV_FLOAT(&(a_ptr+lda * 1)[j+gvl], gvl);
                    va2_0 = VLEV_FLOAT(&(a_ptr+lda * 2)[j], gvl);
                    va2_1 = VLEV_FLOAT(&(a_ptr+lda * 2)[j+gvl], gvl);
                    va3_0 = VLEV_FLOAT(&(a_ptr+lda * 3)[j], gvl);
                    va3_1 = VLEV_FLOAT(&(a_ptr+lda * 3)[j+gvl], gvl);

                    vy0 = VFMACCVF_FLOAT(vy0, x[ix], va0_0, gvl);
                    vy1 = VFMACCVF_FLOAT(vy1, x[ix], va0_1, gvl);

                    vy0 = VFMACCVF_FLOAT(vy0, x[ix+1], va1_0, gvl);
                    vy1 = VFMACCVF_FLOAT(vy1, x[ix+1], va1_1, gvl);

                    vy0 = VFMACCVF_FLOAT(vy0, x[ix+2], va2_0, gvl);
                    vy1 = VFMACCVF_FLOAT(vy1, x[ix+2], va2_1, gvl);

                    vy0 = VFMACCVF_FLOAT(vy0, x[ix+3], va3_0, gvl);
                    vy1 = VFMACCVF_FLOAT(vy1, x[ix+3], va3_1, gvl);
                    a_ptr += 4 * lda;
                    ix +=4;
                }
                vy0 = VFMACCVF_FLOAT(vy0_temp, alpha, vy0, gvl);
                vy1 = VFMACCVF_FLOAT(vy1_temp, alpha, vy1, gvl);
                VSEV_FLOAT(&y[j], vy0, gvl);
                VSEV_FLOAT(&y[j+gvl], vy1, gvl);
                j += gvl * 2;
            }
        }
        //tail
		if(gvl <= m - j ){
			a_ptr = a;
			ix = 0;
			vy0_temp = VLEV_FLOAT(&y[j], gvl);
			vy0 = VFILL_ZERO_FLOAT(vy0 , vy0 , gvl);
			int i;

			int remainder = n % 4;
			for(i = 0; i < remainder; i++){
				temp[0] = x[ix];
				va0 = VLEV_FLOAT(&a_ptr[j], gvl);
				vy0 = VFMACCVF_FLOAT(vy0, temp[0], va0, gvl);
				a_ptr += lda;
				ix ++;
			}

			for(i = remainder; i < n; i += 4){
				va0_0 = VLEV_FLOAT(&(a_ptr)[j], gvl);			
				va1_0 = VLEV_FLOAT(&(a_ptr+lda * 1)[j], gvl);		
				va2_0 = VLEV_FLOAT(&(a_ptr+lda * 2)[j], gvl);	
				va3_0 = VLEV_FLOAT(&(a_ptr+lda * 3)[j], gvl);
				vy0 = VFMACCVF_FLOAT(vy0, x[ix], va0_0, gvl);
				vy0 = VFMACCVF_FLOAT(vy0, x[ix+1], va1_0, gvl);
				vy0 = VFMACCVF_FLOAT(vy0, x[ix+2], va2_0, gvl);
				vy0 = VFMACCVF_FLOAT(vy0, x[ix+3], va3_0, gvl);
				a_ptr += 4 * lda;
				ix +=4;
			}
			vy0 = VFMACCVF_FLOAT(vy0_temp, alpha, vy0, gvl);
		
			VSEV_FLOAT(&y[j], vy0, gvl);
			
			j += gvl ;
        }
		

        for(;j < m;){
            gvl = VSETVL(m-j);
            a_ptr = a;
            ix = 0;
            vy0 = VLEV_FLOAT(&y[j], gvl);
            for(i = 0; i < n; i++){
                temp[0] = alpha * x[ix];
                va0 = VLEV_FLOAT(&a_ptr[j], gvl);
                vy0 = VFMACCVF_FLOAT(vy0, temp[0], va0, gvl);

                a_ptr += lda;
                ix += inc_x;
            }
            VSEV_FLOAT(&y[j], vy0, gvl);
            j += gvl;
        }
    }else if (inc_y == 1 && inc_x !=1) {
        gvl = VSETVL(m);
        if(gvl <= m/2){
            for(k=0,j=0; k<m/(2*gvl); k++){
                a_ptr = a;
                ix = 0;
                vy0 = VLEV_FLOAT(&y[j], gvl);
                vy1 = VLEV_FLOAT(&y[j+gvl], gvl);
                for(i = 0; i < n; i++){
                    temp[0] = alpha * x[ix];
                    va0 = VLEV_FLOAT(&a_ptr[j], gvl);
                    vy0 = VFMACCVF_FLOAT(vy0, temp[0], va0, gvl);

                    va1 = VLEV_FLOAT(&a_ptr[j+gvl], gvl);
                    vy1 = VFMACCVF_FLOAT(vy1, temp[0], va1, gvl);
                    a_ptr += lda;
                    ix += inc_x;
                }
                VSEV_FLOAT(&y[j], vy0, gvl);
                VSEV_FLOAT(&y[j+gvl], vy1, gvl);
                j += gvl * 2;
            }
        }
        //tail
        for(;j < m;){
            gvl = VSETVL(m-j);
            a_ptr = a;
            ix = 0;
            vy0 = VLEV_FLOAT(&y[j], gvl);
            for(i = 0; i < n; i++){
                temp[0] = alpha * x[ix];
                va0 = VLEV_FLOAT(&a_ptr[j], gvl);
                vy0 = VFMACCVF_FLOAT(vy0, temp[0], va0, gvl);

                a_ptr += lda;
                ix += inc_x;
            }
            VSEV_FLOAT(&y[j], vy0, gvl);
            j += gvl;
        }
    }else{
        BLASLONG stride_y = inc_y * sizeof(FLOAT);
        gvl = VSETVL(m);
        if(gvl <= m/2){
            BLASLONG inc_yv = inc_y * gvl;
            for(k=0,j=0; k<m/(2*gvl); k++){
                a_ptr = a;
                ix = 0;
                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                vy1 = VLSEV_FLOAT(&y[iy+inc_yv], stride_y, gvl);
                for(i = 0; i < n; i++){
                    temp[0] = alpha * x[ix];
                    va0 = VLEV_FLOAT(&a_ptr[j], gvl);
                    vy0 = VFMACCVF_FLOAT(vy0, temp[0], va0, gvl);

                    va1 = VLEV_FLOAT(&a_ptr[j+gvl], gvl);
                    vy1 = VFMACCVF_FLOAT(vy1, temp[0], va1, gvl);
                    a_ptr += lda;
                    ix += inc_x;
                }
                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy1, gvl);
                j += gvl * 2;
                iy += inc_yv * 2;
            }
        }
        //tail
        for(;j < m;){
            gvl = VSETVL(m-j);
            a_ptr = a;
            ix = 0;
            vy0 = VLSEV_FLOAT(&y[j*inc_y], stride_y, gvl);
            for(i = 0; i < n; i++){
                temp[0] = alpha * x[ix];
                va0 = VLEV_FLOAT(&a_ptr[j], gvl);
                vy0 = VFMACCVF_FLOAT(vy0, temp[0], va0, gvl);

                a_ptr += lda;
                ix += inc_x;
            }
            VSSEV_FLOAT(&y[j*inc_y], stride_y, vy0, gvl);
            j += gvl;
        }
    }
    return(0);
}