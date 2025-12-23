#include "common.h"


/* for debugging/unit tests
 * this is a drop-in replacement for zgemm/cgemm/ztrmm/ctrmm kernels that supports arbitrary combinations of unroll values
 */

#ifdef TRMMKERNEL
    #if defined(LEFT) != defined(TRANSA)
    #define BACKWARDS
    #endif
#endif

#ifdef DOUBLE

#define UNROLL_M ZGEMM_DEFAULT_UNROLL_M
#define UNROLL_N ZGEMM_DEFAULT_UNROLL_N

#else

#define UNROLL_M CGEMM_DEFAULT_UNROLL_M
#define UNROLL_N CGEMM_DEFAULT_UNROLL_N

#endif

int CNAME(BLASLONG M,BLASLONG N,BLASLONG K,FLOAT alphar,FLOAT alphai,FLOAT* A,FLOAT* B,FLOAT* C,BLASLONG ldc
#ifdef TRMMKERNEL
    ,BLASLONG offset
#endif    
    )
{
    FLOAT res[UNROLL_M*UNROLL_N*2];

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    FLOAT sign[4] = { 1, -1,  1,  1};
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
    FLOAT sign[4] = { 1,  1,  1, -1};
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    FLOAT sign[4] = { 1,  1, -1,  1};
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
    FLOAT sign[4] = { 1, -1, -1, -1};
#endif

    BLASLONG n_packing = UNROLL_N;
    BLASLONG n_top = 0;

    while(n_top < N)
    {
        while( n_top+n_packing > N )
            n_packing >>= 1;

        BLASLONG m_packing = UNROLL_M;
        BLASLONG m_top = 0;
        while (m_top < M)
        {
            while( m_top+m_packing > M )
                m_packing >>= 1;

            BLASLONG ai = K*m_top*2;
            BLASLONG bi = K*n_top*2;

            BLASLONG pass_K = K;


            #ifdef TRMMKERNEL
                #ifdef LEFT
                    BLASLONG off = offset + m_top;
                #else
                    BLASLONG off = -offset + n_top;
                #endif
                #ifdef BACKWARDS
                    ai += off * m_packing*2;
                    bi += off * n_packing*2;
                    pass_K -= off; 
                #else
                    #ifdef LEFT
                        pass_K = off + m_packing;
                    #else
                        pass_K = off + n_packing;
                    #endif
                #endif
            #endif

            memset( res, 0, UNROLL_M*UNROLL_N*2*sizeof(FLOAT) );

            for (BLASLONG k=0; k<pass_K; k+=1)
            {
                for( BLASLONG ki = 0; ki < n_packing; ++ki )
                {
                    FLOAT B0 = B[bi+ki*2+0];
                    FLOAT B1 = B[bi+ki*2+1];

                    for( BLASLONG kj = 0; kj < m_packing; ++kj )
                    {
                        FLOAT A0 = A[ai+kj*2+0];
                        FLOAT A1 = A[ai+kj*2+1];

                        res[(ki*UNROLL_M+kj)*2+0] += sign[0]*A0*B0 +sign[1]*A1*B1;
                        res[(ki*UNROLL_M+kj)*2+1] += sign[2]*A1*B0 +sign[3]*A0*B1;
                    }
                }

                ai += m_packing*2;
                bi += n_packing*2;
            }

            BLASLONG cofs = ldc * n_top + m_top;
            for( BLASLONG ki = 0; ki < n_packing; ++ki )
            {
                for( BLASLONG kj = 0; kj < m_packing; ++kj )
                {
                    #ifdef TRMMKERNEL
                    FLOAT Cr = 0;
                    FLOAT Ci = 0;
                    #else
                    FLOAT Cr = C[(cofs+ki*ldc+kj)*2+0];
                    FLOAT Ci = C[(cofs+ki*ldc+kj)*2+1];
                    #endif

                    Cr += res[(ki*UNROLL_M+kj)*2+0]*alphar;
                    Cr += -res[(ki*UNROLL_M+kj)*2+1]*alphai;
                    Ci += res[(ki*UNROLL_M+kj)*2+1]*alphar;
                    Ci += res[(ki*UNROLL_M+kj)*2+0]*alphai;

                    C[(cofs+ki*ldc+kj)*2+0] = Cr; 
                    C[(cofs+ki*ldc+kj)*2+1] = Ci;
                }
            }

            m_top += m_packing;
        }

        n_top += n_packing;
    }

    return 0;
}
