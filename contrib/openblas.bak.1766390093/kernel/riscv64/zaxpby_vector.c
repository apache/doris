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
#define VSETVL(n) RISCV_RVV(vsetvl_e32m4)(n)
#define FLOAT_V_T vfloat32m4_t
#define VLSEV_FLOAT RISCV_RVV(vlse32_v_f32m4)
#define VSSEV_FLOAT RISCV_RVV(vsse32_v_f32m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f32m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f32m4)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f32m4)
#define VFMSACVF_FLOAT RISCV_RVV(vfmsac_vf_f32m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f32m4)
#else
#define VSETVL(n) RISCV_RVV(vsetvl_e64m4)(n)
#define FLOAT_V_T vfloat64m4_t
#define VLSEV_FLOAT RISCV_RVV(vlse64_v_f64m4)
#define VSSEV_FLOAT RISCV_RVV(vsse64_v_f64m4)
#define VFMACCVF_FLOAT RISCV_RVV(vfmacc_vf_f64m4)
#define VFMVVF_FLOAT RISCV_RVV(vfmv_v_f_f64m4)
#define VFMULVF_FLOAT RISCV_RVV(vfmul_vf_f64m4)
#define VFMSACVF_FLOAT RISCV_RVV(vfmsac_vf_f64m4)
#define VFNMSACVF_FLOAT RISCV_RVV(vfnmsac_vf_f64m4)
#endif

int CNAME(BLASLONG n, FLOAT alpha_r, FLOAT alpha_i, FLOAT *x, BLASLONG inc_x, FLOAT beta_r, FLOAT beta_i, FLOAT *y, BLASLONG inc_y)
{
	if (n <= 0)  return(0);

	BLASLONG i=0, j=0;
	unsigned int gvl = 0;
	FLOAT_V_T vx0, vx1;
        FLOAT_V_T vy0, vy1;

	BLASLONG stride_x, stride_y, ix = 0, iy = 0;
        stride_x = inc_x * 2 * sizeof(FLOAT);
        stride_y = inc_y * 2 * sizeof(FLOAT);

	if (inc_x == 0 || inc_y == 0) {

	FLOAT temp;
	BLASLONG inc_x2, inc_y2;

	inc_x2 = 2 * inc_x;
	inc_y2 = 2 * inc_y;

	if ( beta_r == 0.0 && beta_i == 0.0)
	{
		if ( alpha_r == 0.0 && alpha_i == 0.0 )
		{

			while(i < n)
			{
				y[iy]   = 0.0 ;
				y[iy+1] = 0.0 ;
				iy += inc_y2 ;
				i++ ;
			}

		}
		else
		{

			while(i < n)
			{
				y[iy]   = ( alpha_r * x[ix]   - alpha_i * x[ix+1] ) ;
				y[iy+1] = ( alpha_r * x[ix+1] + alpha_i * x[ix]   ) ;
				ix += inc_x2 ;
				iy += inc_y2 ;
				i++ ;
			}


		}

	}
	else
	{
		if ( alpha_r == 0.0 && alpha_i == 0.0 )
		{

			while(i < n)
			{
				temp    = ( beta_r * y[iy]   - beta_i * y[iy+1] ) ;
				y[iy+1] = ( beta_r * y[iy+1] + beta_i * y[iy]   ) ;
				y[iy]   = temp;
				iy += inc_y2 ;
				i++ ;
			}

		}
		else
		{

			while(i < n)
			{
				temp    = ( alpha_r * x[ix]   - alpha_i * x[ix+1] ) + ( beta_r * y[iy]   - beta_i * y[iy+1] ) ;
				y[iy+1] = ( alpha_r * x[ix+1] + alpha_i * x[ix]   ) + ( beta_r * y[iy+1] + beta_i * y[iy]   ) ;
				y[iy]   = temp;
				ix += inc_x2 ;
				iy += inc_y2 ;
				i++ ;
			}


		}



	}
	return(0);

	} else {

        if(beta_r == 0.0 && beta_i == 0.0){
                if(alpha_r == 0.0 && alpha_i == 0.0){
                        if(inc_y == 1){
                                memset(&y[0], 0, 2 * n * sizeof(FLOAT));
                        }else{
                                gvl = VSETVL(n);
                                if(gvl <= n/2){
                                        vy0 = VFMVVF_FLOAT(0.0, gvl);
                                        BLASLONG inc_yv = inc_y * gvl * 2;
                                        for(i=0,j=0;i<n/(gvl*2);i++){
                                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                                                VSSEV_FLOAT(&y[iy+1], stride_y, vy0, gvl);
                                                VSSEV_FLOAT(&y[iy+inc_yv], stride_y, vy0, gvl);
                                                VSSEV_FLOAT(&y[iy+1+inc_yv], stride_y, vy0, gvl);
                                                j += gvl * 2;
                                                iy += inc_yv * 2;
                                        }
                                }
                                for(;j<n;){
                                        gvl = VSETVL(n-j);
                                        vy0 = VFMVVF_FLOAT(0.0, gvl);
                                        VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                                        VSSEV_FLOAT(&y[iy+1], stride_y, vy0, gvl);
                                        j += gvl;
                                        iy += inc_y * gvl * 2;
                                }
                        }
		}else{
                        gvl = VSETVL(n);
                        BLASLONG inc_xv = inc_x * gvl * 2;
                        BLASLONG inc_yv = inc_y * gvl * 2;
                        for(i=0,j=0; i<n/gvl; i++){
                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
                                vy0 = VFMULVF_FLOAT(vx1, alpha_i, gvl);
                                vy0 = VFMSACVF_FLOAT(vy0, alpha_r, vx0, gvl);
                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                                vy1 = VFMULVF_FLOAT(vx1, alpha_r, gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, alpha_i, vx0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, vy1, gvl);

                                j += gvl;
                                ix += inc_xv;
                                iy += inc_yv;
                        }
                        if(j<n){
                                gvl = VSETVL(n-j);
                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
                                vy0 = VFMULVF_FLOAT(vx1, alpha_i, gvl);
                                vy0 = VFMSACVF_FLOAT(vy0, alpha_r, vx0, gvl);
                                VSSEV_FLOAT(&y[iy], stride_y, vy0, gvl);
                                vy1 = VFMULVF_FLOAT(vx1, alpha_r, gvl);
                                vy1 = VFMACCVF_FLOAT(vy1, alpha_i, vx0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, vy1, gvl);
                        }
                }
        }else{
	        FLOAT_V_T v0, v1;
                if(alpha_r == 0.0 && alpha_i == 0.0){
                        gvl = VSETVL(n);
                        BLASLONG inc_yv = inc_y * gvl * 2;
                        for(i=0,j=0;i<n/gvl;i++){
                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);
                                v0 = VFMULVF_FLOAT(vy1, beta_i, gvl);
                                v0 = VFMSACVF_FLOAT(v0, beta_r, vy0, gvl);
                                VSSEV_FLOAT(&y[iy], stride_y, v0, gvl);
                                v1 = VFMULVF_FLOAT(vy1, beta_r, gvl);
                                v1 = VFMACCVF_FLOAT(v1, beta_i, vy0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, v1, gvl);
                                j += gvl;
                                iy += inc_yv;
                        }
                        if(j<n){
                                gvl = VSETVL(n-j);
                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);
                                v0 = VFMULVF_FLOAT(vy1, beta_i, gvl);
                                v0 = VFMSACVF_FLOAT(v0, beta_r, vy0, gvl);
                                VSSEV_FLOAT(&y[iy], stride_y, v0, gvl);
                                v1 = VFMULVF_FLOAT(vy1, beta_r, gvl);
                                v1 = VFMACCVF_FLOAT(v1, beta_i, vy0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, v1, gvl);
                        }
		}else{
                        gvl = VSETVL(n);
                        BLASLONG inc_xv = inc_x * gvl * 2;
                        BLASLONG inc_yv = inc_y * gvl * 2;
                        for(i=0,j=0; i<n/gvl; i++){
                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);
                                v0 = VFMULVF_FLOAT(vx0, alpha_r, gvl);
                                v0 = VFNMSACVF_FLOAT(v0, alpha_i, vx1, gvl);
                                v0 = VFMACCVF_FLOAT(v0, beta_r, vy0, gvl);
                                v0 = VFNMSACVF_FLOAT(v0, beta_i, vy1, gvl);
                                VSSEV_FLOAT(&y[iy], stride_y, v0, gvl);
                                v1 = VFMULVF_FLOAT(vx1, alpha_r, gvl);
                                v1 = VFMACCVF_FLOAT(v1, alpha_i, vx0, gvl);
                                v1 = VFMACCVF_FLOAT(v1, beta_r, vy1, gvl);
                                v1 = VFMACCVF_FLOAT(v1, beta_i, vy0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, v1, gvl);

                                j += gvl;
                                ix += inc_xv;
                                iy += inc_yv;
                        }
                        if(j<n){
                                gvl = VSETVL(n-j);
                                vx0 = VLSEV_FLOAT(&x[ix], stride_x, gvl);
                                vx1 = VLSEV_FLOAT(&x[ix+1], stride_x, gvl);
                                vy0 = VLSEV_FLOAT(&y[iy], stride_y, gvl);
                                vy1 = VLSEV_FLOAT(&y[iy+1], stride_y, gvl);
                                v0 = VFMULVF_FLOAT(vx0, alpha_r, gvl);
                                v0 = VFNMSACVF_FLOAT(v0, alpha_i, vx1, gvl);
                                v0 = VFMACCVF_FLOAT(v0, beta_r, vy0, gvl);
                                v0 = VFNMSACVF_FLOAT(v0, beta_i, vy1, gvl);
                                VSSEV_FLOAT(&y[iy], stride_y, v0, gvl);
                                v1 = VFMULVF_FLOAT(vx1, alpha_r, gvl);
                                v1 = VFMACCVF_FLOAT(v1, alpha_i, vx0, gvl);
                                v1 = VFMACCVF_FLOAT(v1, beta_r, vy1, gvl);
                                v1 = VFMACCVF_FLOAT(v1, beta_i, vy0, gvl);
                                VSSEV_FLOAT(&y[iy+1], stride_y, v1, gvl);
                        }
                }
        }
	return(0);
	}
}

