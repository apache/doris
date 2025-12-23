/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
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

/***********************************************************
 * 2014-06-10 Saar
 * 2015-09-07 grisuthedragon 
***********************************************************/

#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#if defined(DOUBLE)
#define ERROR_NAME "ZIMATCOPY"
#else
#define ERROR_NAME "CIMATCOPY"
#endif

#define BlasRowMajor     0
#define BlasColMajor     1
#define BlasNoTrans      0
#define BlasTrans        1
#define BlasTransConj    2
#define BlasConj         3

#define NEW_IMATCOPY 

#ifndef CBLAS
void NAME( char* ORDER, char* TRANS, blasint *rows, blasint *cols, FLOAT *alpha, FLOAT *a, blasint *lda, blasint *ldb)
{

	char Order, Trans;
	int order=-1,trans=-1;
	blasint info = -1;
	FLOAT *b;
	size_t msize;

	Order = *ORDER;
	Trans = *TRANS;

	TOUPPER(Order);
	TOUPPER(Trans);

	if ( Order == 'C' ) order = BlasColMajor;
	if ( Order == 'R' ) order = BlasRowMajor;
	if ( Trans == 'N' ) trans = BlasNoTrans;
	if ( Trans == 'T' ) trans = BlasTrans;
	if ( Trans == 'C' ) trans = BlasTransConj;
	if ( Trans == 'R' ) trans = BlasConj;

#else 
void CNAME( enum CBLAS_ORDER CORDER, enum CBLAS_TRANSPOSE CTRANS, blasint crows, blasint ccols, FLOAT *alpha, FLOAT *a, blasint clda, blasint cldb)
{

	blasint *rows, *cols, *lda, *ldb; 
	int order=-1,trans=-1;
	blasint info = -1;
	FLOAT *b;
	size_t msize;

	if ( CORDER == CblasColMajor ) order = BlasColMajor; 
	if ( CORDER == CblasRowMajor ) order = BlasRowMajor; 

	if ( CTRANS == CblasNoTrans) trans = BlasNoTrans; 
	if ( CTRANS == CblasConjNoTrans ) trans = BlasConj; 
	if ( CTRANS == CblasTrans) trans = BlasTrans; 
	if ( CTRANS == CblasConjTrans) trans = BlasTransConj; 

	rows = &crows; 
	cols = &ccols; 
	lda  = &clda; 
	ldb  = &cldb; 
#endif

	if ( order == BlasColMajor)
	{
        	if ( trans == BlasNoTrans      &&  *ldb < MAX(1,*rows) ) info = 9;
        	if ( trans == BlasConj         &&  *ldb < MAX(1,*rows) ) info = 9;
        	if ( trans == BlasTrans        &&  *ldb < MAX(1,*cols) ) info = 9;
        	if ( trans == BlasTransConj    &&  *ldb < MAX(1,*cols) ) info = 9;
	}
	if ( order == BlasRowMajor)
	{
        	if ( trans == BlasNoTrans    &&  *ldb < MAX(1,*cols) ) info = 9;
        	if ( trans == BlasConj       &&  *ldb < MAX(1,*cols) ) info = 9;
        	if ( trans == BlasTrans      &&  *ldb < MAX(1,*rows) ) info = 9;
        	if ( trans == BlasTransConj  &&  *ldb < MAX(1,*rows) ) info = 9;
	}

	if ( order == BlasColMajor &&  *lda < MAX(1,*rows) ) info = 7;
	if ( order == BlasRowMajor &&  *lda < MAX(1,*cols) ) info = 7;
	if ( *cols < 0 ) info = 4;
	if ( *rows < 0 ) info = 3;
	if ( trans < 0 ) info = 2;
	if ( order < 0 ) info = 1;

	if (info >= 0) {
    		BLASFUNC(xerbla)(ERROR_NAME, &info, sizeof(ERROR_NAME));
    		return;
  	}

	if ((*rows == 0) || (*cols == 0)) return;

#ifdef NEW_IMATCOPY
    if (*lda == *ldb ) {
        if ( order == BlasColMajor )
        {

            if ( trans == BlasNoTrans )
            {
                IMATCOPY_K_CN(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }
            if ( trans == BlasConj )
            {
                IMATCOPY_K_CNC(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }
            if ( trans == BlasTrans && *rows == *cols )
            {
                IMATCOPY_K_CT(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }
            if ( trans == BlasTransConj && *rows == *cols )
            {
                IMATCOPY_K_CTC(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }

        }
        else
        {

            if ( trans == BlasNoTrans )
            {
                IMATCOPY_K_RN(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }
            if ( trans == BlasConj )
            {
                IMATCOPY_K_RNC(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }
            if ( trans == BlasTrans && *rows == *cols )
            {
                IMATCOPY_K_RT(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }
            if ( trans == BlasTransConj && *rows == *cols )
            {
                IMATCOPY_K_RTC(*rows, *cols, alpha[0], alpha[1], a, *lda );
                return;
            }

        }
    }
#endif

		if ( *rows >  *cols )
                msize = (size_t)(*rows) * (*ldb)  * sizeof(FLOAT) * 2;
        else
                msize = (size_t)(*cols) * (*ldb)  * sizeof(FLOAT) * 2;

	b = malloc(msize);
	if ( b == NULL )
	{
		printf("Memory alloc failed in zimatcopy\n");
		exit(1);
	}

	if ( order == BlasColMajor )
	{

		if ( trans == BlasNoTrans )
		{
	  		OMATCOPY_K_CN(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *rows );
	  		OMATCOPY_K_CN(*rows, *cols, (FLOAT) 1.0, (FLOAT) 0.0 , b, *rows, a, *ldb );
		}
		else if ( trans == BlasConj )
		{
			OMATCOPY_K_CNC(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *rows );
	  		OMATCOPY_K_CN(*rows, *cols, (FLOAT) 1.0, (FLOAT) 0.0 , b, *rows, a, *ldb );
		}
		else if ( trans == BlasTrans )
		{
			OMATCOPY_K_CT(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *cols );
	  		OMATCOPY_K_CN(*cols, *rows, (FLOAT) 1.0, (FLOAT) 0.0 , b, *cols, a, *ldb );
		}
		else if ( trans == BlasTransConj )
		{
			OMATCOPY_K_CTC(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *cols );
	  		OMATCOPY_K_CN(*cols, *rows, (FLOAT) 1.0, (FLOAT) 0.0 , b, *cols, a, *ldb );
		}

	}
	else
	{

		if ( trans == BlasNoTrans )
		{
			OMATCOPY_K_RN(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *cols );
	  		OMATCOPY_K_RN(*rows, *cols, (FLOAT) 1.0, (FLOAT) 0.0 , b, *cols, a, *ldb );
		}
		else if ( trans == BlasConj )
		{
			OMATCOPY_K_RNC(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *cols );
	  		OMATCOPY_K_RN(*rows, *cols, (FLOAT) 1.0, (FLOAT) 0.0 , b, *cols, a, *ldb );
		}
		else if ( trans == BlasTrans )
		{
			OMATCOPY_K_RT(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *rows );
	  		OMATCOPY_K_RN(*cols, *rows, (FLOAT) 1.0, (FLOAT) 0.0 , b, *rows, a, *ldb );
		}
		else if ( trans == BlasTransConj )
		{
			OMATCOPY_K_RTC(*rows, *cols, alpha[0], alpha[1], a, *lda, b, *rows );
	  		OMATCOPY_K_RN(*cols, *rows, (FLOAT) 1.0, (FLOAT) 0.0 , b, *rows, a, *ldb );
		}

	}

	free(b);
	return;

}


