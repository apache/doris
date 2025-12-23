#include "common.h"
/* helper for the direct sgemm code written by Arjan van der Ven */




int CNAME(BLASLONG M, BLASLONG N, BLASLONG K)
{
	unsigned long long mnk = M * N * K;
	/* large matrixes -> not performant */
	if (mnk >= 28 * 512 * 512)
		return 0;

	/*
	 * if the B matrix is not a nice multiple if 4 we get many unaligned accesses,
	 * and the regular sgemm copy/realignment of data pays off much quicker
	 */
	if ((N & 3) != 0 && (mnk >= 8 * 512 * 512))
		return 0;

#ifdef SMP
	/* if we can run multithreaded, the threading changes the based threshold */
	if (mnk > 2 * 350 * 512 && num_cpu_avail(3)> 1)
		return 0;
#endif

	return 1;
}


