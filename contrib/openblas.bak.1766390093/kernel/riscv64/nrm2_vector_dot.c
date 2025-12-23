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
#define VSETVL(n) vsetvl_e32m8(n)
#define VSETVL_MAX vsetvlmax_e32m1()
#define FLOAT_V_T vfloat32m8_t
#define FLOAT_V_T_M1 vfloat32m1_t
#define VLEV_FLOAT vle32_v_f32m8
#define VLSEV_FLOAT vlse32_v_f32m8
#define VFMVFS_FLOAT vfmv_f_s_f32m1_f32
#define VFREDSUM_FLOAT vfredusum_vs_f32m8_f32m1
#define VFMACCVV_FLOAT vfmacc_vv_f32m8
#define VFMVVF_FLOAT vfmv_v_f_f32m8
#define VFMVVF_FLOAT_M1 vfmv_v_f_f32m1
#define VFDOTVV_FLOAT vfdot_vv_f32m8
#define ABS fabsf
#else
#define VSETVL(n) vsetvl_e64m8(n)
#define VSETVL_MAX vsetvlmax_e64m1()
#define FLOAT_V_T vfloat64m8_t
#define FLOAT_V_T_M1 vfloat64m1_t
#define VLEV_FLOAT vle64_v_f64m8
#define VLSEV_FLOAT vlse64_v_f64m8
#define VFMVFS_FLOAT vfmv_f_s_f64m1_f64
#define VFREDSUM_FLOAT vfredusum_vs_f64m8_f64m1
#define VFMACCVV_FLOAT vfmacc_vv_f64m8
#define VFMVVF_FLOAT vfmv_v_f_f64m8
#define VFMVVF_FLOAT_M1 vfmv_v_f_f64m1
#define VFDOTVV_FLOAT vfdot_vv_f64m8
#define ABS fabs
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	BLASLONG i=0, j=0;
	double len = 0.0 ;

	if ( n <= 0 )  return(0.0);
        if(n == 1) return (ABS(x[0]));

        FLOAT_V_T vr, v0, v1;
        unsigned int gvl = 0;
        FLOAT_V_T_M1 v_res, v_z0;
        gvl = VSETVL_MAX;
        v_res = VFMVVF_FLOAT_M1(0, gvl);
        v_z0 = VFMVVF_FLOAT_M1(0, gvl);

        if(inc_x == 1){
                gvl = VSETVL(n);
                if(gvl < n/2){
                        vr = VFMVVF_FLOAT(0, gvl);
                        for(i=0,j=0; i<n/(2*gvl); i++){
                                v0 = VLEV_FLOAT(&x[j], gvl);
                                vr = VFMACCVV_FLOAT(vr, v0, v0, gvl);
                                j += gvl;

                                v1 = VLEV_FLOAT(&x[j], gvl);
                                vr = VFMACCVV_FLOAT(vr, v1, v1, gvl);
                                j += gvl;
                        }
                        v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
                        len += VFMVFS_FLOAT(v_res);
                }
                //tail
                for(;j < n;){
                        gvl = VSETVL(n-j);
                        v0 = VLEV_FLOAT(&x[j], gvl);
                        //v1 = 0
                        //v1 = VFMVVF_FLOAT(0, gvl);
                        //vr = VFDOTVV_FLOAT(v0, v0, gvl);
                        vr = VFMACCVV_FLOAT(v1, v0, v0, gvl);
                        v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
                        len += VFMVFS_FLOAT(v_res);

                        j += gvl;
                }
        }else{
                gvl = VSETVL(n);
                unsigned int stride_x = inc_x * sizeof(FLOAT);
                if(gvl < n/2){
                        vr = VFMVVF_FLOAT(0, gvl);
                        for(i=0,j=0; i<n/(2*gvl); i++){
                                v0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                                vr = VFMACCVV_FLOAT(vr, v0, v0, gvl);
                                j += gvl;

                                v1 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                                vr = VFMACCVV_FLOAT(vr, v1, v1, gvl);
                                j += gvl;
                        }
                        v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
                        len += VFMVFS_FLOAT(v_res);
                }
                //tail
                for(;j < n;){
                        gvl = VSETVL(n-j);
                        v0 = VLSEV_FLOAT(&x[j*inc_x], stride_x, gvl);
                        //v1 = 0
                        //v1 = VFMVVF_FLOAT(0, gvl);
                        //vr = VFDOTVV_FLOAT(v0, v0, gvl);
                        vr = VFMACCVV_FLOAT(v1, v0, v0, gvl);
                        v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
                        len += VFMVFS_FLOAT(v_res);

                        j += gvl;
                }
        }
	return(sqrt(len));
}


