#include "common.h"

int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc ,BLASLONG offset)
{

   BLASLONG i,j,k;
   FLOAT *C0,*C1,*C2,*C3,*ptrba,*ptrbb;

   FLOAT res0_0;
   FLOAT res0_1;
   FLOAT res0_2;
   FLOAT res0_3;
   FLOAT res0_4;
   FLOAT res0_5;
   FLOAT res0_6;
   FLOAT res0_7;

   FLOAT res0_8;
   FLOAT res0_9;
   FLOAT res0_10;
   FLOAT res0_11;
   FLOAT res0_12;
   FLOAT res0_13;
   FLOAT res0_14;
   FLOAT res0_15;

   FLOAT res1_0;
   FLOAT res1_1;
   FLOAT res1_2;
   FLOAT res1_3;
   FLOAT res1_4;
   FLOAT res1_5;
   FLOAT res1_6;
   FLOAT res1_7;

   FLOAT res1_8;
   FLOAT res1_9;
   FLOAT res1_10;
   FLOAT res1_11;
   FLOAT res1_12;
   FLOAT res1_13;
   FLOAT res1_14;
   FLOAT res1_15;

   FLOAT res2_0;
   FLOAT res2_1;
   FLOAT res2_2;
   FLOAT res2_3;
   FLOAT res2_4;
   FLOAT res2_5;
   FLOAT res2_6;
   FLOAT res2_7;

   FLOAT res2_8;
   FLOAT res2_9;
   FLOAT res2_10;
   FLOAT res2_11;
   FLOAT res2_12;
   FLOAT res2_13;
   FLOAT res2_14;
   FLOAT res2_15;

   FLOAT res3_0;
   FLOAT res3_1;
   FLOAT res3_2;
   FLOAT res3_3;
   FLOAT res3_4;
   FLOAT res3_5;
   FLOAT res3_6;
   FLOAT res3_7;

   FLOAT res3_8;
   FLOAT res3_9;
   FLOAT res3_10;
   FLOAT res3_11;
   FLOAT res3_12;
   FLOAT res3_13;
   FLOAT res3_14;
   FLOAT res3_15;

   FLOAT a0;
   FLOAT a1;

   FLOAT b0;
   FLOAT b1;
   FLOAT b2;
   FLOAT b3;

   BLASLONG off, temp;

#if !defined(LEFT)
   off = -offset;
#else
   off = 0;
#endif

   for (j=0; j<bn/4; j+=1)
   {
        C0 = C;
        C1 = C0+ldc;
        C2 = C0+2*ldc;
        C3 = C0+3*ldc;

#if defined(TRMMKERNEL) && defined(LEFT)
	off = offset;
#endif


        ptrba = ba;


        for (i=0; i<bm/16; i+=1)
        {

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*16;
		ptrbb = bb + off*4;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;
		res0_4 = 0;
		res0_5 = 0;
		res0_6 = 0;
		res0_7 = 0;

		res0_8  = 0;
		res0_9  = 0;
		res0_10 = 0;
		res0_11 = 0;
		res0_12 = 0;
		res0_13 = 0;
		res0_14 = 0;
		res0_15 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;
		res1_4 = 0;
		res1_5 = 0;
		res1_6 = 0;
		res1_7 = 0;

		res1_8  = 0;
		res1_9  = 0;
		res1_10 = 0;
		res1_11 = 0;
		res1_12 = 0;
		res1_13 = 0;
		res1_14 = 0;
		res1_15 = 0;

		res2_0 = 0;
		res2_1 = 0;
		res2_2 = 0;
		res2_3 = 0;
		res2_4 = 0;
		res2_5 = 0;
		res2_6 = 0;
		res2_7 = 0;

		res2_8  = 0;
		res2_9  = 0;
		res2_10 = 0;
		res2_11 = 0;
		res2_12 = 0;
		res2_13 = 0;
		res2_14 = 0;
		res2_15 = 0;

		res3_0 = 0;
		res3_1 = 0;
		res3_2 = 0;
		res3_3 = 0;
		res3_4 = 0;
		res3_5 = 0;
		res3_6 = 0;
		res3_7 = 0;

		res3_8  = 0;
		res3_9  = 0;
		res3_10 = 0;
		res3_11 = 0;
		res3_12 = 0;
		res3_13 = 0;
		res3_14 = 0;
		res3_15 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+16;	// number of values in A
#else
		temp = off+4;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;
			res2_2 += a0*b2;
			res3_2 += a0*b3;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;
			res2_3 += a1*b2;
			res3_3 += a1*b3;

			a0 = ptrba[4];
			res0_4 += a0*b0;
			res1_4 += a0*b1;
			res2_4 += a0*b2;
			res3_4 += a0*b3;

			a1 = ptrba[5];
			res0_5 += a1*b0;
			res1_5 += a1*b1;
			res2_5 += a1*b2;
			res3_5 += a1*b3;

			a0 = ptrba[6];
			res0_6 += a0*b0;
			res1_6 += a0*b1;
			res2_6 += a0*b2;
			res3_6 += a0*b3;

			a1 = ptrba[7];
			res0_7 += a1*b0;
			res1_7 += a1*b1;
			res2_7 += a1*b2;
			res3_7 += a1*b3;

			a0 = ptrba[8];
			res0_8 += a0*b0;
			res1_8 += a0*b1;
			res2_8 += a0*b2;
			res3_8 += a0*b3;

			a1 = ptrba[9];
			res0_9 += a1*b0;
			res1_9 += a1*b1;
			res2_9 += a1*b2;
			res3_9 += a1*b3;

			a0 = ptrba[10];
			res0_10 += a0*b0;
			res1_10 += a0*b1;
			res2_10 += a0*b2;
			res3_10 += a0*b3;

			a1 = ptrba[11];
			res0_11 += a1*b0;
			res1_11 += a1*b1;
			res2_11 += a1*b2;
			res3_11 += a1*b3;

			a0 = ptrba[12];
			res0_12 += a0*b0;
			res1_12 += a0*b1;
			res2_12 += a0*b2;
			res3_12 += a0*b3;

			a1 = ptrba[13];
			res0_13 += a1*b0;
			res1_13 += a1*b1;
			res2_13 += a1*b2;
			res3_13 += a1*b3;

			a0 = ptrba[14];
			res0_14 += a0*b0;
			res1_14 += a0*b1;
			res2_14 += a0*b2;
			res3_14 += a0*b3;

			a1 = ptrba[15];
			res0_15 += a1*b0;
			res1_15 += a1*b1;
			res2_15 += a1*b2;
			res3_15 += a1*b3;


			ptrba = ptrba+16;
			ptrbb = ptrbb+4;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;
		res0_4 *= alpha;
		res0_5 *= alpha;
		res0_6 *= alpha;
		res0_7 *= alpha;

		res0_8  *= alpha;
		res0_9  *= alpha;
		res0_10 *= alpha;
		res0_11 *= alpha;
		res0_12 *= alpha;
		res0_13 *= alpha;
		res0_14 *= alpha;
		res0_15 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;
		res1_4 *= alpha;
		res1_5 *= alpha;
		res1_6 *= alpha;
		res1_7 *= alpha;

		res1_8  *= alpha;
		res1_9  *= alpha;
		res1_10 *= alpha;
		res1_11 *= alpha;
		res1_12 *= alpha;
		res1_13 *= alpha;
		res1_14 *= alpha;
		res1_15 *= alpha;
		
		res2_0 *= alpha;
		res2_1 *= alpha;
		res2_2 *= alpha;
		res2_3 *= alpha;
		res2_4 *= alpha;
		res2_5 *= alpha;
		res2_6 *= alpha;
		res2_7 *= alpha;

		res2_8  *= alpha;
		res2_9  *= alpha;
		res2_10 *= alpha;
		res2_11 *= alpha;
		res2_12 *= alpha;
		res2_13 *= alpha;
		res2_14 *= alpha;
		res2_15 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;
		res3_2 *= alpha;
		res3_3 *= alpha;
		res3_4 *= alpha;
		res3_5 *= alpha;
		res3_6 *= alpha;
		res3_7 *= alpha;

		res3_8  *= alpha;
		res3_9  *= alpha;
		res3_10 *= alpha;
		res3_11 *= alpha;
		res3_12 *= alpha;
		res3_13 *= alpha;
		res3_14 *= alpha;
		res3_15 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;
		C0[4] = res0_4;
		C0[5] = res0_5;
		C0[6] = res0_6;
		C0[7] = res0_7;

		C0[8]  = res0_8;
		C0[9]  = res0_9;
		C0[10] = res0_10;
		C0[11] = res0_11;
		C0[12] = res0_12;
		C0[13] = res0_13;
		C0[14] = res0_14;
		C0[15] = res0_15;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;
		C1[4] = res1_4;
		C1[5] = res1_5;
		C1[6] = res1_6;
		C1[7] = res1_7;

		C1[8]  = res1_8;
		C1[9]  = res1_9;
		C1[10] = res1_10;
		C1[11] = res1_11;
		C1[12] = res1_12;
		C1[13] = res1_13;
		C1[14] = res1_14;
		C1[15] = res1_15;

		C2[0] = res2_0;
		C2[1] = res2_1;
		C2[2] = res2_2;
		C2[3] = res2_3;
		C2[4] = res2_4;
		C2[5] = res2_5;
		C2[6] = res2_6;
		C2[7] = res2_7;

		C2[8]  = res2_8;
		C2[9]  = res2_9;
		C2[10] = res2_10;
		C2[11] = res2_11;
		C2[12] = res2_12;
		C2[13] = res2_13;
		C2[14] = res2_14;
		C2[15] = res2_15;

		C3[0] = res3_0;
		C3[1] = res3_1;
		C3[2] = res3_2;
		C3[3] = res3_3;
		C3[4] = res3_4;
		C3[5] = res3_5;
		C3[6] = res3_6;
		C3[7] = res3_7;

		C3[8]  = res3_8;
		C3[9]  = res3_9;
		C3[10] = res3_10;
		C3[11] = res3_11;
		C3[12] = res3_12;
		C3[13] = res3_13;
		C3[14] = res3_14;
		C3[15] = res3_15;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 16; // number of values in A
#else
		temp -= 4; // number of values in B
#endif
		ptrba += temp*16;
		ptrbb += temp*4;
#endif

#ifdef LEFT
		off += 16; // number of values in A
#endif

		C0 = C0+16;
		C1 = C1+16;
		C2 = C2+16;
		C3 = C3+16;
	}


        if ( bm & 8)
        {

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*8;
		ptrbb = bb + off*4;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;
		res0_4 = 0;
		res0_5 = 0;
		res0_6 = 0;
		res0_7 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;
		res1_4 = 0;
		res1_5 = 0;
		res1_6 = 0;
		res1_7 = 0;

		res2_0 = 0;
		res2_1 = 0;
		res2_2 = 0;
		res2_3 = 0;
		res2_4 = 0;
		res2_5 = 0;
		res2_6 = 0;
		res2_7 = 0;

		res3_0 = 0;
		res3_1 = 0;
		res3_2 = 0;
		res3_3 = 0;
		res3_4 = 0;
		res3_5 = 0;
		res3_6 = 0;
		res3_7 = 0;

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+8;	// number of values in A
#else
		temp = off+4;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;
			res2_2 += a0*b2;
			res3_2 += a0*b3;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;
			res2_3 += a1*b2;
			res3_3 += a1*b3;

			a0 = ptrba[4];
			res0_4 += a0*b0;
			res1_4 += a0*b1;
			res2_4 += a0*b2;
			res3_4 += a0*b3;

			a1 = ptrba[5];
			res0_5 += a1*b0;
			res1_5 += a1*b1;
			res2_5 += a1*b2;
			res3_5 += a1*b3;

			a0 = ptrba[6];
			res0_6 += a0*b0;
			res1_6 += a0*b1;
			res2_6 += a0*b2;
			res3_6 += a0*b3;

			a1 = ptrba[7];
			res0_7 += a1*b0;
			res1_7 += a1*b1;
			res2_7 += a1*b2;
			res3_7 += a1*b3;

			ptrba = ptrba+8;
			ptrbb = ptrbb+4;

                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;
		res0_4 *= alpha;
		res0_5 *= alpha;
		res0_6 *= alpha;
		res0_7 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;
		res1_4 *= alpha;
		res1_5 *= alpha;
		res1_6 *= alpha;
		res1_7 *= alpha;

		res2_0 *= alpha;
		res2_1 *= alpha;
		res2_2 *= alpha;
		res2_3 *= alpha;
		res2_4 *= alpha;
		res2_5 *= alpha;
		res2_6 *= alpha;
		res2_7 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;
		res3_2 *= alpha;
		res3_3 *= alpha;
		res3_4 *= alpha;
		res3_5 *= alpha;
		res3_6 *= alpha;
		res3_7 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;
		C0[4] = res0_4;
		C0[5] = res0_5;
		C0[6] = res0_6;
		C0[7] = res0_7;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;
		C1[4] = res1_4;
		C1[5] = res1_5;
		C1[6] = res1_6;
		C1[7] = res1_7;

		C2[0] = res2_0;
		C2[1] = res2_1;
		C2[2] = res2_2;
		C2[3] = res2_3;
		C2[4] = res2_4;
		C2[5] = res2_5;
		C2[6] = res2_6;
		C2[7] = res2_7;

		C3[0] = res3_0;
		C3[1] = res3_1;
		C3[2] = res3_2;
		C3[3] = res3_3;
		C3[4] = res3_4;
		C3[5] = res3_5;
		C3[6] = res3_6;
		C3[7] = res3_7;

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 8; // number of values in A
#else
		temp -= 4; // number of values in B
#endif
		ptrba += temp*8;
		ptrbb += temp*4;
#endif

#ifdef LEFT
		off += 8; // number of values in A
#endif

		C0 = C0+8;
		C1 = C1+8;
		C2 = C2+8;
		C3 = C3+8;
	}

	if ( bm & 4 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*4;
		ptrbb = bb + off*4;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;
		
		res2_0 = 0;
		res2_1 = 0;
		res2_2 = 0;
		res2_3 = 0;

		res3_0 = 0;
		res3_1 = 0;
		res3_2 = 0;
		res3_3 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+4;	// number of values in A
#else
		temp = off+4;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;
			res2_2 += a0*b2;
			res3_2 += a0*b3;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;
			res2_3 += a1*b2;
			res3_3 += a1*b3;

			ptrba = ptrba+4;
			ptrbb = ptrbb+4;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;

		res2_0 *= alpha;
		res2_1 *= alpha;
		res2_2 *= alpha;
		res2_3 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;
		res3_2 *= alpha;
		res3_3 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;


		C2[0] = res2_0;
		C2[1] = res2_1;
		C2[2] = res2_2;
		C2[3] = res2_3;

		C3[0] = res3_0;
		C3[1] = res3_1;
		C3[2] = res3_2;
		C3[3] = res3_3;

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 4; // number of values in A
#else
		temp -= 4; // number of values in B
#endif
		ptrba += temp*4;
		ptrbb += temp*4;
#endif

#ifdef LEFT
		off += 4; // number of values in A
#endif

		C0 = C0+4;
		C1 = C1+4;
		C2 = C2+4;
		C3 = C3+4;
	}

	if ( bm & 2 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*2;
		ptrbb = bb + off*4;
#endif

		res0_0 = 0;
		res0_1 = 0;

		res1_0 = 0;
		res1_1 = 0;
		
		res2_0 = 0;
		res2_1 = 0;

		res3_0 = 0;
		res3_1 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+2;	// number of values in A
#else
		temp = off+4;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;

			ptrba = ptrba+2;
			ptrbb = ptrbb+4;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;

		res2_0 *= alpha;
		res2_1 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;

		C1[0] = res1_0;
		C1[1] = res1_1;

		C2[0] = res2_0;
		C2[1] = res2_1;

		C3[0] = res3_0;
		C3[1] = res3_1;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 2; // number of values in A
#else
		temp -= 4; // number of values in B
#endif
		ptrba += temp*2;
		ptrbb += temp*4;
#endif

#ifdef LEFT
		off += 2; // number of values in A
#endif

		C0 = C0+2;
		C1 = C1+2;
		C2 = C2+2;
		C3 = C3+2;
	}
	
	if ( bm & 1 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*1;
		ptrbb = bb + off*4;
#endif

		res0_0 = 0;
		res1_0 = 0;
		res2_0 = 0;
		res3_0 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+1;	// number of values in A
#else
		temp = off+4;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			ptrba = ptrba+1;
			ptrbb = ptrbb+4;
                }
		res0_0 *= alpha;
		res1_0 *= alpha;
		res2_0 *= alpha;
		res3_0 *= alpha;

		C0[0] = res0_0;
		C1[0] = res1_0;
		C2[0] = res2_0;
		C3[0] = res3_0;

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 1; // number of values in A
#else
		temp -= 4; // number of values in B
#endif
		ptrba += temp*1;
		ptrbb += temp*4;
#endif

#ifdef LEFT
		off += 1; // number of values in A
#endif

		C0 = C0+1;
		C1 = C1+1;
		C2 = C2+1;
		C3 = C3+1;

	}


#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 4;
#endif

        k = (bk<<2);
        bb = bb+k;
        i = (ldc<<2);
        C = C+i;
    }


   if(bn&2)
   {
        C0 = C;
        C1 = C0+ldc;

#if defined(TRMMKERNEL) && defined(LEFT)
	off = offset;
#endif


        ptrba = ba;


        for (i=0; i<bm/16; i+=1)
        {

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*16;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;
		res0_4 = 0;
		res0_5 = 0;
		res0_6 = 0;
		res0_7 = 0;

		res0_8  = 0;
		res0_9  = 0;
		res0_10 = 0;
		res0_11 = 0;
		res0_12 = 0;
		res0_13 = 0;
		res0_14 = 0;
		res0_15 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;
		res1_4 = 0;
		res1_5 = 0;
		res1_6 = 0;
		res1_7 = 0;

		res1_8  = 0;
		res1_9  = 0;
		res1_10 = 0;
		res1_11 = 0;
		res1_12 = 0;
		res1_13 = 0;
		res1_14 = 0;
		res1_15 = 0;




#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+16;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;

			a0 = ptrba[4];
			res0_4 += a0*b0;
			res1_4 += a0*b1;

			a1 = ptrba[5];
			res0_5 += a1*b0;
			res1_5 += a1*b1;

			a0 = ptrba[6];
			res0_6 += a0*b0;
			res1_6 += a0*b1;

			a1 = ptrba[7];
			res0_7 += a1*b0;
			res1_7 += a1*b1;

			a0 = ptrba[8];
			res0_8 += a0*b0;
			res1_8 += a0*b1;

			a1 = ptrba[9];
			res0_9 += a1*b0;
			res1_9 += a1*b1;

			a0 = ptrba[10];
			res0_10 += a0*b0;
			res1_10 += a0*b1;

			a1 = ptrba[11];
			res0_11 += a1*b0;
			res1_11 += a1*b1;

			a0 = ptrba[12];
			res0_12 += a0*b0;
			res1_12 += a0*b1;

			a1 = ptrba[13];
			res0_13 += a1*b0;
			res1_13 += a1*b1;

			a0 = ptrba[14];
			res0_14 += a0*b0;
			res1_14 += a0*b1;

			a1 = ptrba[15];
			res0_15 += a1*b0;
			res1_15 += a1*b1;


			ptrba = ptrba+16;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;
		res0_4 *= alpha;
		res0_5 *= alpha;
		res0_6 *= alpha;
		res0_7 *= alpha;

		res0_8  *= alpha;
		res0_9  *= alpha;
		res0_10 *= alpha;
		res0_11 *= alpha;
		res0_12 *= alpha;
		res0_13 *= alpha;
		res0_14 *= alpha;
		res0_15 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;
		res1_4 *= alpha;
		res1_5 *= alpha;
		res1_6 *= alpha;
		res1_7 *= alpha;

		res1_8  *= alpha;
		res1_9  *= alpha;
		res1_10 *= alpha;
		res1_11 *= alpha;
		res1_12 *= alpha;
		res1_13 *= alpha;
		res1_14 *= alpha;
		res1_15 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;
		C0[4] = res0_4;
		C0[5] = res0_5;
		C0[6] = res0_6;
		C0[7] = res0_7;

		C0[8]  = res0_8;
		C0[9]  = res0_9;
		C0[10] = res0_10;
		C0[11] = res0_11;
		C0[12] = res0_12;
		C0[13] = res0_13;
		C0[14] = res0_14;
		C0[15] = res0_15;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;
		C1[4] = res1_4;
		C1[5] = res1_5;
		C1[6] = res1_6;
		C1[7] = res1_7;

		C1[8]  = res1_8;
		C1[9]  = res1_9;
		C1[10] = res1_10;
		C1[11] = res1_11;
		C1[12] = res1_12;
		C1[13] = res1_13;
		C1[14] = res1_14;
		C1[15] = res1_15;



#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 16; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*16;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 16; // number of values in A
#endif

		C0 = C0+16;
		C1 = C1+16;
	}




        if ( bm & 8)
        {

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*8;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;
		res0_4 = 0;
		res0_5 = 0;
		res0_6 = 0;
		res0_7 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;
		res1_4 = 0;
		res1_5 = 0;
		res1_6 = 0;
		res1_7 = 0;



#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+8;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;

			a0 = ptrba[4];
			res0_4 += a0*b0;
			res1_4 += a0*b1;

			a1 = ptrba[5];
			res0_5 += a1*b0;
			res1_5 += a1*b1;

			a0 = ptrba[6];
			res0_6 += a0*b0;
			res1_6 += a0*b1;

			a1 = ptrba[7];
			res0_7 += a1*b0;
			res1_7 += a1*b1;

			ptrba = ptrba+8;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;
		res0_4 *= alpha;
		res0_5 *= alpha;
		res0_6 *= alpha;
		res0_7 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;
		res1_4 *= alpha;
		res1_5 *= alpha;
		res1_6 *= alpha;
		res1_7 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;
		C0[4] = res0_4;
		C0[5] = res0_5;
		C0[6] = res0_6;
		C0[7] = res0_7;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;
		C1[4] = res1_4;
		C1[5] = res1_5;
		C1[6] = res1_6;
		C1[7] = res1_7;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 8; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*8;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 8; // number of values in A
#endif

		C0 = C0+8;
		C1 = C1+8;
	}

	if ( bm & 4 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*4;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+4;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;

			ptrba = ptrba+4;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 4; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*4;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 4; // number of values in A
#endif

		C0 = C0+4;
		C1 = C1+4;

	}

	if ( bm & 2 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*2;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;
		res0_1 = 0;

		res1_0 = 0;
		res1_1 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+2;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;

			ptrba = ptrba+2;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;

		C1[0] = res1_0;
		C1[1] = res1_1;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 2; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*2;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 2; // number of values in A
#endif

		C0 = C0+2;
		C1 = C1+2;

	}

	if ( bm & 1 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*1;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;

		res1_0 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+1;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			ptrba = ptrba+1;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;

		res1_0 *= alpha;

		C0[0] = res0_0;

		C1[0] = res1_0;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 1; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*1;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 1; // number of values in A
#endif

		C0 = C0+1;
		C1 = C1+1;

	}


#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 2;
#endif

        k = (bk<<1);
        bb = bb+k;
        i = (ldc<<1);
        C = C+i;
    }


   for (j=0; j<(bn&1); j+=1)
   {
        C0 = C;

#if defined(TRMMKERNEL) &&  defined(LEFT)
	off = offset;
#endif

        ptrba = ba;


        for (i=0; i<bm/16; i+=1)
        {

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*16;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;
		res0_4 = 0;
		res0_5 = 0;
		res0_6 = 0;
		res0_7 = 0;

		res0_8  = 0;
		res0_9  = 0;
		res0_10 = 0;
		res0_11 = 0;
		res0_12 = 0;
		res0_13 = 0;
		res0_14 = 0;
		res0_15 = 0;




#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+16;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			a1 = ptrba[1];
			res0_1 += a1*b0;

			a0 = ptrba[2];
			res0_2 += a0*b0;

			a1 = ptrba[3];
			res0_3 += a1*b0;

			a0 = ptrba[4];
			res0_4 += a0*b0;

			a1 = ptrba[5];
			res0_5 += a1*b0;

			a0 = ptrba[6];
			res0_6 += a0*b0;

			a1 = ptrba[7];
			res0_7 += a1*b0;

			a0 = ptrba[8];
			res0_8 += a0*b0;

			a1 = ptrba[9];
			res0_9 += a1*b0;

			a0 = ptrba[10];
			res0_10 += a0*b0;

			a1 = ptrba[11];
			res0_11 += a1*b0;

			a0 = ptrba[12];
			res0_12 += a0*b0;

			a1 = ptrba[13];
			res0_13 += a1*b0;

			a0 = ptrba[14];
			res0_14 += a0*b0;

			a1 = ptrba[15];
			res0_15 += a1*b0;


			ptrba = ptrba+16;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;
		res0_4 *= alpha;
		res0_5 *= alpha;
		res0_6 *= alpha;
		res0_7 *= alpha;

		res0_8  *= alpha;
		res0_9  *= alpha;
		res0_10 *= alpha;
		res0_11 *= alpha;
		res0_12 *= alpha;
		res0_13 *= alpha;
		res0_14 *= alpha;
		res0_15 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;
		C0[4] = res0_4;
		C0[5] = res0_5;
		C0[6] = res0_6;
		C0[7] = res0_7;

		C0[8]  = res0_8;
		C0[9]  = res0_9;
		C0[10] = res0_10;
		C0[11] = res0_11;
		C0[12] = res0_12;
		C0[13] = res0_13;
		C0[14] = res0_14;
		C0[15] = res0_15;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 16; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*16;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 16; // number of values in A
#endif

		C0 = C0+16;
	}




        if ( bm & 8 )
        {

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*8;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;
		res0_4 = 0;
		res0_5 = 0;
		res0_6 = 0;
		res0_7 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+8;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			a1 = ptrba[1];
			res0_1 += a1*b0;

			a0 = ptrba[2];
			res0_2 += a0*b0;

			a1 = ptrba[3];
			res0_3 += a1*b0;

			a0 = ptrba[4];
			res0_4 += a0*b0;

			a1 = ptrba[5];
			res0_5 += a1*b0;

			a0 = ptrba[6];
			res0_6 += a0*b0;

			a1 = ptrba[7];
			res0_7 += a1*b0;

			ptrba = ptrba+8;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;
		res0_4 *= alpha;
		res0_5 *= alpha;
		res0_6 *= alpha;
		res0_7 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;
		C0[4] = res0_4;
		C0[5] = res0_5;
		C0[6] = res0_6;
		C0[7] = res0_7;

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 8; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*8;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 8; // number of values in A
#endif

		C0 = C0+8;
	}

	if ( bm & 4 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*4;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+4;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			a1 = ptrba[1];
			res0_1 += a1*b0;

			a0 = ptrba[2];
			res0_2 += a0*b0;

			a1 = ptrba[3];
			res0_3 += a1*b0;

			ptrba = ptrba+4;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 4; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*4;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 4; // number of values in A
#endif

		C0 = C0+4;

	}

	if ( bm & 2 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*2;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;
		res0_1 = 0;



#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+2;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			a1 = ptrba[1];
			res0_1 += a1*b0;

			ptrba = ptrba+2;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 2; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*2;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 2; // number of values in A
#endif

		C0 = C0+2;

	}

	if ( bm & 1 )
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*1;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+1;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			ptrba = ptrba+1;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;

		C0[0] = res0_0;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 1; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*1;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 1; // number of values in A
#endif

		C0 = C0+1;

	}



#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 1;
#endif

        k = (bk<<0);
        bb = bb+k;
        C = C+ldc;
   }
   return 0;
}
