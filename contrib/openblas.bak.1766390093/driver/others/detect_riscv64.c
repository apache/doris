/*****************************************************************************
Copyright (c) 2024, The OpenBLAS Project
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
      derived from this software without specific prior written
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************************/

#include <stdint.h>

#ifdef __riscv_v_intrinsic
#include <riscv_vector.h>
#endif

unsigned detect_riscv64_get_vlenb(void) {
#ifdef __riscv_v_intrinsic
	return __riscv_vlenb();
#else
	return 0;
#endif
}

/*
 * Based on the approach taken here:
 * https://code.videolan.org/videolan/dav1d/-/merge_requests/1629
 *
 * Only to be called after we've determined we have some sort of
 * RVV support.
 */

uint64_t detect_riscv64_rvv100(void)
{
	uint64_t rvv10_supported;

	/*
	 * After the vsetvli statement vtype will either be a value > 0 if the
	 * vsetvli succeeded or less than 0 if it failed.  If 0 < vtype
	 * we're good and the function will return 1, otherwise there's no
	 * RVV 1.0 and we return 0.
	 */

	asm volatile("vsetvli x0, x0, e8, m1, ta, ma\n\t"
		     "csrr %0, vtype\n\t"
		     "slt %0, x0, %0\n"
		     : "=r" (rvv10_supported)
		     :
		     :);

	return rvv10_supported;
}

