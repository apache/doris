/* the direct sgemm code written by Arjan van der Ven */
#include "common.h"

#if defined(SKYLAKEX) || defined (COOPERLAKE) || defined (SAPPHIRERAPIDS)

#include <immintrin.h>


/*
 * "Direct sgemm" code. This code operates directly on the inputs and outputs
 * of the sgemm call, avoiding the copies, memory realignments and threading,
 * and only supports alpha = 1 and beta = 0.
 * This is a common case and provides value for relatively small matrixes.
 * For larger matrixes the "regular" sgemm code is superior, there the cost of
 * copying/shuffling the B matrix really pays off.
 */



#define DECLARE_RESULT_512(N,M) __m512 result##N##M = _mm512_setzero_ps()
#define BROADCAST_LOAD_A_512(N,M) __m512 Aval##M = _mm512_broadcastss_ps(_mm_load_ss(&A[k  + strideA * (i+M)]))
#define LOAD_B_512(N,M)  __m512 Bval##N = _mm512_loadu_ps(&B[strideB * k + j + (N*16)])
#define MATMUL_512(N,M)  result##N##M = _mm512_fmadd_ps(Aval##M, Bval##N , result##N##M)
#define STORE_512(N,M) _mm512_storeu_ps(&R[(i+M) * strideR + j+(N*16)], result##N##M)


#define DECLARE_RESULT_256(N,M) __m256 result##N##M = _mm256_setzero_ps()
#define BROADCAST_LOAD_A_256(N,M) __m256 Aval##M = _mm256_broadcastss_ps(_mm_load_ss(&A[k  + strideA * (i+M)]))
#define LOAD_B_256(N,M)  __m256 Bval##N = _mm256_loadu_ps(&B[strideB * k + j + (N*8)])
#define MATMUL_256(N,M)  result##N##M = _mm256_fmadd_ps(Aval##M, Bval##N , result##N##M)
#define STORE_256(N,M) _mm256_storeu_ps(&R[(i+M) * strideR + j+(N*8)], result##N##M)

#define DECLARE_RESULT_128(N,M) __m128 result##N##M = _mm_setzero_ps()
#define BROADCAST_LOAD_A_128(N,M) __m128 Aval##M = _mm_broadcastss_ps(_mm_load_ss(&A[k  + strideA * (i+M)]))
#define LOAD_B_128(N,M)  __m128 Bval##N = _mm_loadu_ps(&B[strideB * k + j + (N*4)])
#define MATMUL_128(N,M)  result##N##M = _mm_fmadd_ps(Aval##M, Bval##N , result##N##M)
#define STORE_128(N,M) _mm_storeu_ps(&R[(i+M) * strideR + j+(N*4)], result##N##M)

#define DECLARE_RESULT_SCALAR(N,M) float result##N##M = 0;
#define BROADCAST_LOAD_A_SCALAR(N,M) float Aval##M = A[k + strideA * (i + M)];
#define LOAD_B_SCALAR(N,M)  float Bval##N  = B[k * strideB + j + N];
#define MATMUL_SCALAR(N,M) result##N##M +=  Aval##M * Bval##N;
#define STORE_SCALAR(N,M)  R[(i+M) * strideR + j + N] = result##N##M;

#if 0
int sgemm_kernel_direct_performant(BLASLONG M, BLASLONG N, BLASLONG K)
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

#endif

//void sgemm_kernel_direct (BLASLONG M, BLASLONG N, BLASLONG K, float * __restrict A, BLASLONG strideA, float * __restrict B, BLASLONG strideB , float * __restrict R, BLASLONG strideR)
void CNAME (BLASLONG M, BLASLONG N, BLASLONG K, float * __restrict A, BLASLONG strideA, float * __restrict B, BLASLONG strideB , float * __restrict R, BLASLONG strideR)
{
	int i, j, k;

        int m4 = M & ~3;
	int m2 = M & ~1;

	int n64 = N & ~63;
	int n32 = N & ~31;
	int n16 = N & ~15;
	int n8 = N & ~7;
	int n4 = N & ~3;
	int n2 = N & ~1;

	i = 0;

	for (i = 0; i < m4; i+=4) {

		for (j = 0; j < n64; j+= 64) {
			k = 0;
			DECLARE_RESULT_512(0, 0);    DECLARE_RESULT_512(1, 0);    			DECLARE_RESULT_512(2, 0);    DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1);    DECLARE_RESULT_512(1, 1);    			DECLARE_RESULT_512(2, 1);    DECLARE_RESULT_512(3, 1);
			DECLARE_RESULT_512(0, 2);    DECLARE_RESULT_512(1, 2);    			DECLARE_RESULT_512(2, 2);    DECLARE_RESULT_512(3, 2);
			DECLARE_RESULT_512(0, 3);    DECLARE_RESULT_512(1, 3);    			DECLARE_RESULT_512(2, 3);    DECLARE_RESULT_512(3, 3);


			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				BROADCAST_LOAD_A_512(x, 1);
				BROADCAST_LOAD_A_512(x, 2);
				BROADCAST_LOAD_A_512(x, 3);

				LOAD_B_512(0, x);		LOAD_B_512(1, x);			LOAD_B_512(2, x);		LOAD_B_512(3, x);

				MATMUL_512(0, 0);		MATMUL_512(1, 0);			MATMUL_512(2, 0);		MATMUL_512(3, 0);
				MATMUL_512(0, 1);		MATMUL_512(1, 1);			MATMUL_512(2, 1);		MATMUL_512(3, 1);
				MATMUL_512(0, 2);		MATMUL_512(1, 2);			MATMUL_512(2, 2);		MATMUL_512(3, 2);
				MATMUL_512(0, 3);		MATMUL_512(1, 3);			MATMUL_512(2, 3);		MATMUL_512(3, 3);
			}
			STORE_512(0, 0);		STORE_512(1, 0);			STORE_512(2, 0);		STORE_512(3, 0);
			STORE_512(0, 1);		STORE_512(1, 1);			STORE_512(2, 1);		STORE_512(3, 1);
			STORE_512(0, 2);		STORE_512(1, 2);			STORE_512(2, 2);		STORE_512(3, 2);
			STORE_512(0, 3);		STORE_512(1, 3);			STORE_512(2, 3);		STORE_512(3, 3);
		}

		for (; j < n32; j+= 32) {
			DECLARE_RESULT_512(0, 0);    DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1);    DECLARE_RESULT_512(1, 1);
			DECLARE_RESULT_512(0, 2);    DECLARE_RESULT_512(1, 2);
			DECLARE_RESULT_512(0, 3);    DECLARE_RESULT_512(1, 3);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				BROADCAST_LOAD_A_512(x, 1);
				BROADCAST_LOAD_A_512(x, 2);
				BROADCAST_LOAD_A_512(x, 3);

				LOAD_B_512(0, x);		LOAD_B_512(1, x);

				MATMUL_512(0, 0);		MATMUL_512(1, 0);
				MATMUL_512(0, 1);		MATMUL_512(1, 1);
				MATMUL_512(0, 2);		MATMUL_512(1, 2);
				MATMUL_512(0, 3);		MATMUL_512(1, 3);
			}
			STORE_512(0, 0);		STORE_512(1, 0);
			STORE_512(0, 1);		STORE_512(1, 1);
			STORE_512(0, 2);		STORE_512(1, 2);
			STORE_512(0, 3);		STORE_512(1, 3);
		}

		for (; j < n16; j+= 16) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);
			DECLARE_RESULT_512(0, 2);
			DECLARE_RESULT_512(0, 3);

		 	for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				BROADCAST_LOAD_A_512(x, 1);
				BROADCAST_LOAD_A_512(x, 2);
				BROADCAST_LOAD_A_512(x, 3);

				LOAD_B_512(0, x);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
				MATMUL_512(0, 2);
				MATMUL_512(0, 3);
			}
			STORE_512(0, 0);
			STORE_512(0, 1);
			STORE_512(0, 2);
			STORE_512(0, 3);
		}

		for (; j < n8; j+= 8) {
			DECLARE_RESULT_256(0, 0);
			DECLARE_RESULT_256(0, 1);
			DECLARE_RESULT_256(0, 2);
			DECLARE_RESULT_256(0, 3);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_256(x, 0);
				BROADCAST_LOAD_A_256(x, 1);
				BROADCAST_LOAD_A_256(x, 2);
				BROADCAST_LOAD_A_256(x, 3);

				LOAD_B_256(0, x);

				MATMUL_256(0, 0);
				MATMUL_256(0, 1);
				MATMUL_256(0, 2);
				MATMUL_256(0, 3);
			}
			STORE_256(0, 0);
			STORE_256(0, 1);
			STORE_256(0, 2);
			STORE_256(0, 3);
		}

		for (; j < n4; j+= 4) {
			DECLARE_RESULT_128(0, 0);
			DECLARE_RESULT_128(0, 1);
			DECLARE_RESULT_128(0, 2);
			DECLARE_RESULT_128(0, 3);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_128(x, 0);
				BROADCAST_LOAD_A_128(x, 1);
				BROADCAST_LOAD_A_128(x, 2);
				BROADCAST_LOAD_A_128(x, 3);

				LOAD_B_128(0, x);

				MATMUL_128(0, 0);
				MATMUL_128(0, 1);
				MATMUL_128(0, 2);
				MATMUL_128(0, 3);
			}
			STORE_128(0, 0);
			STORE_128(0, 1);
			STORE_128(0, 2);
			STORE_128(0, 3);
		}

		for (; j < n2; j+= 2) {
			DECLARE_RESULT_SCALAR(0, 0);	DECLARE_RESULT_SCALAR(1, 0);
			DECLARE_RESULT_SCALAR(0, 1);	DECLARE_RESULT_SCALAR(1, 1);
			DECLARE_RESULT_SCALAR(0, 2);	DECLARE_RESULT_SCALAR(1, 2);
			DECLARE_RESULT_SCALAR(0, 3);	DECLARE_RESULT_SCALAR(1, 3);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_SCALAR(x, 0);
				BROADCAST_LOAD_A_SCALAR(x, 1);
				BROADCAST_LOAD_A_SCALAR(x, 2);
				BROADCAST_LOAD_A_SCALAR(x, 3);

				LOAD_B_SCALAR(0, x);	LOAD_B_SCALAR(1, x);

				MATMUL_SCALAR(0, 0);	MATMUL_SCALAR(1, 0);
				MATMUL_SCALAR(0, 1);	MATMUL_SCALAR(1, 1);
				MATMUL_SCALAR(0, 2);	MATMUL_SCALAR(1, 2);
				MATMUL_SCALAR(0, 3);	MATMUL_SCALAR(1, 3);
			}
			STORE_SCALAR(0, 0);	STORE_SCALAR(1, 0);
			STORE_SCALAR(0, 1);	STORE_SCALAR(1, 1);
			STORE_SCALAR(0, 2);	STORE_SCALAR(1, 2);
			STORE_SCALAR(0, 3);	STORE_SCALAR(1, 3);
		}

		for (; j < N; j++) {
			DECLARE_RESULT_SCALAR(0, 0)
			DECLARE_RESULT_SCALAR(0, 1)
			DECLARE_RESULT_SCALAR(0, 2)
			DECLARE_RESULT_SCALAR(0, 3)

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_SCALAR(0, 0);
				BROADCAST_LOAD_A_SCALAR(0, 1);
				BROADCAST_LOAD_A_SCALAR(0, 2);
				BROADCAST_LOAD_A_SCALAR(0, 3);

				LOAD_B_SCALAR(0, 0);

				MATMUL_SCALAR(0, 0);
				MATMUL_SCALAR(0, 1);
				MATMUL_SCALAR(0, 2);
				MATMUL_SCALAR(0, 3);
			}
			STORE_SCALAR(0, 0);
			STORE_SCALAR(0, 1);
			STORE_SCALAR(0, 2);
			STORE_SCALAR(0, 3);
		}
	}

	for (; i < m2; i+=2) {
		j = 0;

		for (; j < n64; j+= 64) {
			DECLARE_RESULT_512(0, 0);    DECLARE_RESULT_512(1, 0);    			DECLARE_RESULT_512(2, 0);    DECLARE_RESULT_512(3, 0);
			DECLARE_RESULT_512(0, 1);    DECLARE_RESULT_512(1, 1);    			DECLARE_RESULT_512(2, 1);    DECLARE_RESULT_512(3, 1);


			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				BROADCAST_LOAD_A_512(x, 1);

				LOAD_B_512(0, x);		LOAD_B_512(1, x);			LOAD_B_512(2, x);		LOAD_B_512(3, x);

				MATMUL_512(0, 0);		MATMUL_512(1, 0);			MATMUL_512(2, 0);		MATMUL_512(3, 0);
				MATMUL_512(0, 1);		MATMUL_512(1, 1);			MATMUL_512(2, 1);		MATMUL_512(3, 1);
			}
			STORE_512(0, 0);		STORE_512(1, 0);			STORE_512(2, 0);		STORE_512(3, 0);
			STORE_512(0, 1);		STORE_512(1, 1);			STORE_512(2, 1);		STORE_512(3, 1);
		}

		for (; j < n32; j+= 32) {
			DECLARE_RESULT_512(0, 0);    DECLARE_RESULT_512(1, 0);
			DECLARE_RESULT_512(0, 1);    DECLARE_RESULT_512(1, 1);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				BROADCAST_LOAD_A_512(x, 1);

				LOAD_B_512(0, x);		LOAD_B_512(1, x);

				MATMUL_512(0, 0);		MATMUL_512(1, 0);
				MATMUL_512(0, 1);		MATMUL_512(1, 1);
			}
			STORE_512(0, 0);		STORE_512(1, 0);
			STORE_512(0, 1);		STORE_512(1, 1);
		}


		for (; j < n16; j+= 16) {
			DECLARE_RESULT_512(0, 0);
			DECLARE_RESULT_512(0, 1);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				BROADCAST_LOAD_A_512(x, 1);

				LOAD_B_512(0, x);

				MATMUL_512(0, 0);
				MATMUL_512(0, 1);
			}
			STORE_512(0, 0);
			STORE_512(0, 1);
		}

		for (; j < n8; j+= 8) {
			DECLARE_RESULT_256(0, 0);
			DECLARE_RESULT_256(0, 1);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_256(x, 0);
				BROADCAST_LOAD_A_256(x, 1);

				LOAD_B_256(0, x);

				MATMUL_256(0, 0);
				MATMUL_256(0, 1);
			}
			STORE_256(0, 0);
			STORE_256(0, 1);
		}

		for (; j < n4; j+= 4) {
			DECLARE_RESULT_128(0, 0);
			DECLARE_RESULT_128(0, 1);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_128(x, 0);
				BROADCAST_LOAD_A_128(x, 1);

				LOAD_B_128(0, x);

				MATMUL_128(0, 0);
				MATMUL_128(0, 1);
			}
			STORE_128(0, 0);
			STORE_128(0, 1);
		}
		for (; j < n2; j+= 2) {
			DECLARE_RESULT_SCALAR(0, 0);	DECLARE_RESULT_SCALAR(1, 0);
			DECLARE_RESULT_SCALAR(0, 1);	DECLARE_RESULT_SCALAR(1, 1);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_SCALAR(x, 0);
				BROADCAST_LOAD_A_SCALAR(x, 1);

				LOAD_B_SCALAR(0, x);	LOAD_B_SCALAR(1, x);

				MATMUL_SCALAR(0, 0);	MATMUL_SCALAR(1, 0);
				MATMUL_SCALAR(0, 1);	MATMUL_SCALAR(1, 1);
			}
			STORE_SCALAR(0, 0);	STORE_SCALAR(1, 0);
			STORE_SCALAR(0, 1);	STORE_SCALAR(1, 1);
		}

		for (; j < N; j++) {
			DECLARE_RESULT_SCALAR(0, 0);
			DECLARE_RESULT_SCALAR(0, 1);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_SCALAR(0, 0);
				BROADCAST_LOAD_A_SCALAR(0, 1);

				LOAD_B_SCALAR(0, 0);

				MATMUL_SCALAR(0, 0);
				MATMUL_SCALAR(0, 1);
			}
			STORE_SCALAR(0, 0);
			STORE_SCALAR(0, 1);
		}
	}

	for (; i < M; i+=1) {
		j = 0;
		for (; j < n64; j+= 64) {
			DECLARE_RESULT_512(0, 0);    DECLARE_RESULT_512(1, 0);    			DECLARE_RESULT_512(2, 0);    DECLARE_RESULT_512(3, 0);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				LOAD_B_512(0, x);		LOAD_B_512(1, x);			LOAD_B_512(2, x);		LOAD_B_512(3, x);
				MATMUL_512(0, 0);		MATMUL_512(1, 0);			MATMUL_512(2, 0);		MATMUL_512(3, 0);
			}
			STORE_512(0, 0);		STORE_512(1, 0);			STORE_512(2, 0);		STORE_512(3, 0);
		}
		for (; j < n32; j+= 32) {
			DECLARE_RESULT_512(0, 0);    DECLARE_RESULT_512(1, 0);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);
				LOAD_B_512(0, x);		LOAD_B_512(1, x);
				MATMUL_512(0, 0);		MATMUL_512(1, 0);
			}
			STORE_512(0, 0);		STORE_512(1, 0);
		}


		for (; j < n16; j+= 16) {
			DECLARE_RESULT_512(0, 0);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_512(x, 0);

				LOAD_B_512(0, x);

				MATMUL_512(0, 0);
			}
			STORE_512(0, 0);
		}

		for (; j < n8; j+= 8) {
			DECLARE_RESULT_256(0, 0);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_256(x, 0);
				LOAD_B_256(0, x);
				MATMUL_256(0, 0);
			}
			STORE_256(0, 0);
		}

		for (; j < n4; j+= 4) {
			DECLARE_RESULT_128(0, 0);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_128(x, 0);
				LOAD_B_128(0, x);
				MATMUL_128(0, 0);
			}
			STORE_128(0, 0);
		}

		for (; j < n2; j+= 2) {
			DECLARE_RESULT_SCALAR(0, 0);	DECLARE_RESULT_SCALAR(1, 0);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_SCALAR(x, 0);
				LOAD_B_SCALAR(0, 0);	LOAD_B_SCALAR(1, 0);
				MATMUL_SCALAR(0, 0);	MATMUL_SCALAR(1, 0);
			}
			STORE_SCALAR(0, 0);	STORE_SCALAR(1, 0);
		}

		for (; j < N; j++) {
			DECLARE_RESULT_SCALAR(0, 0);

			for (k = 0; k < K; k++) {
				BROADCAST_LOAD_A_SCALAR(0, 0);
				LOAD_B_SCALAR(0, 0);
				MATMUL_SCALAR(0, 0);
			}
			STORE_SCALAR(0, 0);
		}
	}
}
#else

void CNAME (BLASLONG M, BLASLONG N, BLASLONG K, float * __restrict A, BLASLONG strideA, float * __restrict B, BLASLONG strideB , float * __restrict R, BLASLONG strideR)
{}
#endif
