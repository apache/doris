/***************************************************************************
Copyright (c) 2023, The OpenBLAS Project
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

double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
        BLASLONG i=0, j=0;
        double dot = 0.0 ;

        if ( n < 1 )  return(dot);
        vfloat64m4_t vr;
        vfloat32m2_t vx, vy;
        unsigned int gvl = 0;
        vfloat64m1_t v_res, v_z0;
        gvl = vsetvlmax_e64m1();
        v_res = vfmv_v_f_f64m1(0, gvl);
        v_z0 = vfmv_v_f_f64m1(0, gvl);

        if(inc_x == 1 && inc_y == 1){
                gvl = vsetvl_e64m4(n);
                vr = vfmv_v_f_f64m4(0, gvl);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = vle32_v_f32m2(&x[j], gvl);
                        vy = vle32_v_f32m2(&y[j], gvl);
                        vr = vfwmacc_vv_f64m4(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);
                }
                //tail
                if(j < n){
                        gvl = vsetvl_e64m4(n-j);
                        vx = vle32_v_f32m2(&x[j], gvl);
                        vy = vle32_v_f32m2(&y[j], gvl);
                        vfloat64m4_t vz = vfmv_v_f_f64m4(0, gvl);
                        //vr = vfdot_vv_f32m2(vx, vy, gvl);
                        vr = vfwmacc_vv_f64m4(vz, vx, vy, gvl);
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);
                }
        }else if(inc_y == 1){
                gvl = vsetvl_e64m4(n);
                vr = vfmv_v_f_f64m4(0, gvl);
                 int stride_x = inc_x * sizeof(FLOAT);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = vlse32_v_f32m2(&x[j*inc_x], stride_x, gvl);
                        vy = vle32_v_f32m2(&y[j], gvl);
                        vr = vfwmacc_vv_f64m4(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);

                }
                //tail
                if(j < n){
                        gvl = vsetvl_e64m4(n-j);
                        vx = vlse32_v_f32m2(&x[j*inc_x], stride_x, gvl);
                        vy = vle32_v_f32m2(&y[j], gvl);
                        vfloat64m4_t vz = vfmv_v_f_f64m4(0, gvl);
                        //vr = vfdot_vv_f32m2(vx, vy, gvl);
                        vr = vfwmacc_vv_f64m4(vz, vx, vy, gvl);
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);

                }
        }else if(inc_x == 1){
                gvl = vsetvl_e64m4(n);
                vr = vfmv_v_f_f64m4(0, gvl);
                 int stride_y = inc_y * sizeof(FLOAT);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = vle32_v_f32m2(&x[j], gvl);
                        vy = vlse32_v_f32m2(&y[j*inc_y], stride_y, gvl);
                        vr = vfwmacc_vv_f64m4(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);

                }
                //tail
                if(j < n){
                        gvl = vsetvl_e64m4(n-j);
                        vx = vle32_v_f32m2(&x[j], gvl);
                        vy = vlse32_v_f32m2(&y[j*inc_y], stride_y, gvl);
                        vfloat64m4_t vz = vfmv_v_f_f64m4(0, gvl);
                        //vr = vfdot_vv_f32m2(vx, vy, gvl);
                        vr = vfwmacc_vv_f64m4(vz, vx, vy, gvl);
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);

                }
        }else{
                gvl = vsetvl_e64m4(n);
                vr = vfmv_v_f_f64m4(0, gvl);
                 int stride_x = inc_x * sizeof(FLOAT);
                 int stride_y = inc_y * sizeof(FLOAT);
                for(i=0,j=0; i<n/gvl; i++){
                        vx = vlse32_v_f32m2(&x[j*inc_x], stride_x, gvl);
                        vy = vlse32_v_f32m2(&y[j*inc_y], stride_y, gvl);
                        vr = vfwmacc_vv_f64m4(vr, vx, vy, gvl);
                        j += gvl;
                }
                if(j > 0){
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);

                }
                //tail
                if(j < n){
                        gvl = vsetvl_e64m4(n-j);
                        vx = vlse32_v_f32m2(&x[j*inc_x], stride_x, gvl);
                        vy = vlse32_v_f32m2(&y[j*inc_y], stride_y, gvl);
                        vfloat64m4_t vz = vfmv_v_f_f64m4(0, gvl);
                        //vr = vfdot_vv_f32m2(vx, vy, gvl);
                        vr = vfwmacc_vv_f64m4(vz, vx, vy, gvl);
                        v_res = vfredusum_vs_f64m4_f64m1(v_res, vr, v_z0, gvl);
                        dot += (double)vfmv_f_s_f64m1_f64(v_res);

                }
        }
        return(dot);
}
