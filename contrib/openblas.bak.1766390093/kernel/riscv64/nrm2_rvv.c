/***************************************************************************
Copyright (c) 2022, The OpenBLAS Project
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
#define VSETVL(n)           __riscv_vsetvl_e32m4(n)
#define VSETVL_MAX          __riscv_vsetvlmax_e32m4()
#define FLOAT_V_T           vfloat32m4_t
#define FLOAT_V_T_M1        vfloat32m1_t
#define MASK_T              vbool8_t
#define VLEV_FLOAT          __riscv_vle32_v_f32m4
#define VLSEV_FLOAT         __riscv_vlse32_v_f32m4
#define VFREDSUM_FLOAT      __riscv_vfredusum_vs_f32m4_f32m1_tu
#define VFMACCVV_FLOAT_TU   __riscv_vfmacc_vv_f32m4_tu
#define VFMVVF_FLOAT        __riscv_vfmv_v_f_f32m4
#define VFMVVF_FLOAT_M1     __riscv_vfmv_v_f_f32m1
#define VMFIRSTM            __riscv_vfirst_m_b8
#define VFREDMAXVS_FLOAT_TU __riscv_vfredmax_vs_f32m4_f32m1_tu
#define VFMVFS_FLOAT        __riscv_vfmv_f_s_f32m1_f32
#define VMFGTVF_FLOAT       __riscv_vmfgt_vf_f32m4_b8
#define VFDIVVF_FLOAT       __riscv_vfdiv_vf_f32m4
#define VFABSV_FLOAT        __riscv_vfabs_v_f32m4
#define ABS fabsf
#else
#define VSETVL(n)           __riscv_vsetvl_e64m4(n)
#define VSETVL_MAX          __riscv_vsetvlmax_e64m4()
#define FLOAT_V_T           vfloat64m4_t
#define FLOAT_V_T_M1        vfloat64m1_t
#define MASK_T              vbool16_t
#define VLEV_FLOAT          __riscv_vle64_v_f64m4
#define VLSEV_FLOAT         __riscv_vlse64_v_f64m4
#define VFREDSUM_FLOAT      __riscv_vfredusum_vs_f64m4_f64m1_tu
#define VFMACCVV_FLOAT_TU   __riscv_vfmacc_vv_f64m4_tu
#define VFMVVF_FLOAT        __riscv_vfmv_v_f_f64m4
#define VFMVVF_FLOAT_M1     __riscv_vfmv_v_f_f64m1
#define VMFIRSTM            __riscv_vfirst_m_b16
#define VFREDMAXVS_FLOAT_TU __riscv_vfredmax_vs_f64m4_f64m1_tu
#define VFMVFS_FLOAT        __riscv_vfmv_f_s_f64m1_f64
#define VMFGTVF_FLOAT       __riscv_vmfgt_vf_f64m4_b16
#define VFDIVVF_FLOAT       __riscv_vfdiv_vf_f64m4
#define VFABSV_FLOAT        __riscv_vfabs_v_f64m4
#define ABS fabs
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
    if (n <= 0 || inc_x == 0) return(0.0);
    if ( n == 1 ) return( ABS(x[0]) );

    BLASLONG i = 0, j = 0;
	FLOAT scale = 0.0, ssq = 0.0;

	if( inc_x > 0 ){
		FLOAT_V_T vr, v0, v_zero;
		unsigned int gvl = 0;
		FLOAT_V_T_M1 v_res, v_z0;
		gvl = VSETVL_MAX;
		v_res = VFMVVF_FLOAT_M1(0, gvl);
		v_z0 = VFMVVF_FLOAT_M1(0, gvl);
		MASK_T mask;
		BLASLONG index = 0;

		if (inc_x == 1) {
			gvl = VSETVL(n);
			vr = VFMVVF_FLOAT(0, gvl);
			v_zero = VFMVVF_FLOAT(0, gvl);
			for (i = 0, j = 0; i < n / gvl; i++) {
				v0 = VLEV_FLOAT(&x[j], gvl);
				// fabs(vector)
				v0 = VFABSV_FLOAT(v0, gvl);
				// if scale change
				mask = VMFGTVF_FLOAT(v0, scale, gvl);
				index = VMFIRSTM(mask, gvl);
				if (index == -1) {	// no elements greater than scale
					if (scale != 0.0) {
						v0 = VFDIVVF_FLOAT(v0, scale, gvl);
						vr = VFMACCVV_FLOAT_TU(vr, v0, v0, gvl);
					}
				}
				else {	// found greater element
					// ssq in vector vr: vr[0]
					v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
					// total ssq before current vector
					ssq += VFMVFS_FLOAT(v_res);
					// find max
					v_res = VFREDMAXVS_FLOAT_TU(v_res, v0, v_z0, gvl);
					// update ssq before max_index
					ssq = ssq * (scale / VFMVFS_FLOAT(v_res)) * (scale / VFMVFS_FLOAT(v_res));
					// update scale
					scale = VFMVFS_FLOAT(v_res);
					// ssq in vector vr
					v0 = VFDIVVF_FLOAT(v0, scale, gvl);
					vr = VFMACCVV_FLOAT_TU(v_zero, v0, v0, gvl);
				}
				j += gvl;
			}
			// ssq in vector vr: vr[0]
			v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
			// total ssq now
			ssq += VFMVFS_FLOAT(v_res);

			// tail processing
			if(j < n){
				gvl = VSETVL(n-j);
				v0 = VLEV_FLOAT(&x[j], gvl);
				// fabs(vector)
				v0 = VFABSV_FLOAT(v0, gvl);
				// if scale change
				mask = VMFGTVF_FLOAT(v0, scale, gvl);
				index = VMFIRSTM(mask, gvl);
				if (index == -1) {	// no elements greater than scale
					if(scale != 0.0)
						v0 = VFDIVVF_FLOAT(v0, scale, gvl);
				} else {	// found greater element
					// find max
					v_res = VFREDMAXVS_FLOAT_TU(v_res, v0, v_z0, gvl);
					// update ssq before max_index
					ssq = ssq * (scale / VFMVFS_FLOAT(v_res))*(scale / VFMVFS_FLOAT(v_res));
					// update scale
					scale = VFMVFS_FLOAT(v_res);
					v0 = VFDIVVF_FLOAT(v0, scale, gvl);
				}
				vr = VFMACCVV_FLOAT_TU(v_zero, v0, v0, gvl);
				// ssq in vector vr: vr[0]
				v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
				// total ssq now
				ssq += VFMVFS_FLOAT(v_res);
			}
		}
		else {
			gvl = VSETVL(n);
			vr = VFMVVF_FLOAT(0, gvl);
			v_zero = VFMVVF_FLOAT(0, gvl);
			unsigned int stride_x = inc_x * sizeof(FLOAT);
			int idx = 0, inc_v = inc_x * gvl;
			for (i = 0, j = 0; i < n / gvl; i++) {
				v0 = VLSEV_FLOAT(&x[idx], stride_x, gvl);
				// fabs(vector)
				v0 = VFABSV_FLOAT(v0, gvl);
				// if scale change
				mask = VMFGTVF_FLOAT(v0, scale, gvl);
				index = VMFIRSTM(mask, gvl);
				if (index == -1) {// no elements greater than scale
					if(scale != 0.0){
						v0 = VFDIVVF_FLOAT(v0, scale, gvl);
						vr = VFMACCVV_FLOAT_TU(vr, v0, v0, gvl);
					}
				}
				else {	// found greater element
					// ssq in vector vr: vr[0]
					v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
					// total ssq before current vector
					ssq += VFMVFS_FLOAT(v_res);
					// find max
					v_res = VFREDMAXVS_FLOAT_TU(v_res, v0, v_z0, gvl);
					// update ssq before max_index
					ssq = ssq * (scale / VFMVFS_FLOAT(v_res))*(scale / VFMVFS_FLOAT(v_res));
					// update scale
					scale = VFMVFS_FLOAT(v_res);
					// ssq in vector vr
					v0 = VFDIVVF_FLOAT(v0, scale, gvl);
					vr = VFMACCVV_FLOAT_TU(v_zero, v0, v0, gvl);
				}
				j += gvl;
				idx += inc_v;
			}
			// ssq in vector vr: vr[0]
			v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
			// total ssq now
			ssq += VFMVFS_FLOAT(v_res);

			// tail processing
			if (j < n) {
				gvl = VSETVL(n-j);
				v0 = VLSEV_FLOAT(&x[idx], stride_x, gvl);
				// fabs(vector)
				v0 = VFABSV_FLOAT(v0, gvl);
				// if scale change
				mask = VMFGTVF_FLOAT(v0, scale, gvl);
				index = VMFIRSTM(mask, gvl);
				if(index == -1) {	// no elements greater than scale
					if(scale != 0.0) {
						v0 = VFDIVVF_FLOAT(v0, scale, gvl);
						vr = VFMACCVV_FLOAT_TU(v_zero, v0, v0, gvl);
					}
				}
				else {	// found greater element
					// find max
					v_res = VFREDMAXVS_FLOAT_TU(v_res, v0, v_z0, gvl);
					// update ssq before max_index
					ssq = ssq * (scale / VFMVFS_FLOAT(v_res))*(scale / VFMVFS_FLOAT(v_res));
					// update scale
					scale = VFMVFS_FLOAT(v_res);
					v0 = VFDIVVF_FLOAT(v0, scale, gvl);
					vr = VFMACCVV_FLOAT_TU(v_zero, v0, v0, gvl);
				}
				// ssq in vector vr: vr[0]
				v_res = VFREDSUM_FLOAT(v_res, vr, v_z0, gvl);
				// total ssq now
				ssq += VFMVFS_FLOAT(v_res);
			}
		}
	}
	else{
        // using scalar ops when inc_x < 0
        n *= inc_x;
        while(abs(i) < abs(n)){
			if ( x[i] != 0.0 ){
				FLOAT absxi = ABS( x[i] );
				if ( scale < absxi ){
					ssq = 1 + ssq * ( scale / absxi ) * ( scale / absxi );
					scale = absxi ;
				}
				else{
					ssq += ( absxi/scale ) * ( absxi/scale );
				}

			}
			i += inc_x;
        }
	}
	return(scale * sqrt(ssq));
}


