/*****************************************************************************
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

#include "common.h"

/**
 * Generate random array
 */
void srand_generate(float *alpha, blasint n)
{
    blasint i;
    for (i = 0; i < n; i++)
        alpha[i] = (float)rand() / (float)RAND_MAX;
}

void drand_generate(double *alpha, blasint n)
{
    blasint i;
    for (i = 0; i < n; i++)
        alpha[i] = (double)rand() / (double)RAND_MAX;
}

/**
 * Find difference between two rectangle matrix
 * return norm of differences
 */
float smatrix_difference(float *a, float *b, blasint cols, blasint rows, blasint ld)
{
    blasint i = 0;
    blasint j = 0;
    blasint inc = 1;
    float norm = 0.0f;

    float *a_ptr = a;
    float *b_ptr = b;

    for(i = 0; i < rows; i++)
    {
        for (j = 0; j < cols; j++) {
            a_ptr[j] -= b_ptr[j];
        }
        norm += BLASFUNC(snrm2)(&cols, a_ptr, &inc);
        
        a_ptr += ld;
        b_ptr += ld;
    }
    return norm/(float)(rows);
}

double dmatrix_difference(double *a, double *b, blasint cols, blasint rows, blasint ld)
{
    blasint i = 0;
    blasint j = 0;
    blasint inc = 1;
    double norm = 0.0;

    double *a_ptr = a;
    double *b_ptr = b;

    for(i = 0; i < rows; i++)
    {
        for (j = 0; j < cols; j++) {
            a_ptr[j] -= b_ptr[j];
        }
        norm += BLASFUNC(dnrm2)(&cols, a_ptr, &inc);
        
        a_ptr += ld;
        b_ptr += ld;
    }
    return norm/(double)(rows);
}

/**
 * Complex conjugate operation for vector
 * 
 * param n specifies number of elements in vector x
 * param inc_x specifies increment of vector x
 * param x_ptr specifies buffer holding vector x
 */
void cconjugate_vector(blasint n, blasint inc_x, float *x_ptr)
{
    blasint i;
    inc_x *= 2;

    for (i = 0; i < n; i++)
    {
        x_ptr[1] *= (-1.0f);
        x_ptr +=  inc_x;
    }
}

void zconjugate_vector(blasint n, blasint inc_x, double *x_ptr)
{
    blasint i;
    inc_x *= 2;

    for (i = 0; i < n; i++)
    {
        x_ptr[1] *= (-1.0);
        x_ptr +=  inc_x;
    }
}

/**
 * Transpose matrix
 * 
 * param rows specifies number of rows of A
 * param cols specifies number of columns of A
 * param alpha specifies scaling factor for matrix A
 * param a_src - buffer holding input matrix A
 * param lda_src - leading dimension of the matrix A
 * param a_dst - buffer holding output matrix A
 * param lda_dst - leading dimension of output matrix A
 */
void stranspose(blasint rows, blasint cols, float alpha, float *a_src, int lda_src, 
                float *a_dst, blasint lda_dst)
{
    blasint i, j;
    for (i = 0; i != cols; i++)
    {
        for (j = 0; j != rows; j++)
            a_dst[i*lda_dst+j] = alpha*a_src[j*lda_src+i];
    }
}

void dtranspose(blasint rows, blasint cols, double alpha, double *a_src, int lda_src, 
                double *a_dst, blasint lda_dst)
{
    blasint i, j;
    for (i = 0; i != cols; i++)
    {
        for (j = 0; j != rows; j++)
            a_dst[i*lda_dst+j] = alpha*a_src[j*lda_src+i];
    }
}

void ctranspose(blasint rows, blasint cols, float *alpha, float *a_src, int lda_src, 
                float *a_dst, blasint lda_dst, int conj)
{
    blasint i, j;
    lda_dst *= 2;
    lda_src *= 2;
    for (i = 0; i != cols*2; i+=2)
    {
        for (j = 0; j != rows*2; j+=2){
            a_dst[(i/2)*lda_dst+j] = alpha[0] * a_src[(j/2)*lda_src+i] + conj * alpha[1] * a_src[(j/2)*lda_src+i+1];
            a_dst[(i/2)*lda_dst+j+1] = (-1.0f) * conj * alpha[0] * a_src[(j/2)*lda_src+i+1] + alpha[1] * a_src[(j/2)*lda_src+i];
        } 
    }
}

void ztranspose(blasint rows, blasint cols, double *alpha, double *a_src, int lda_src, 
                double *a_dst, blasint lda_dst, int conj)
{
    blasint i, j;
    lda_dst *= 2;
    lda_src *= 2;
    for (i = 0; i != cols*2; i+=2)
    {
        for (j = 0; j != rows*2; j+=2){
            a_dst[(i/2)*lda_dst+j] = alpha[0] * a_src[(j/2)*lda_src+i] + conj * alpha[1] * a_src[(j/2)*lda_src+i+1];
            a_dst[(i/2)*lda_dst+j+1] = (-1.0) * conj * alpha[0] * a_src[(j/2)*lda_src+i+1] + alpha[1] * a_src[(j/2)*lda_src+i];
        } 
    }
}

/**
 * Copy matrix from source A to destination A
 * 
 * param rows specifies number of rows of A
 * param cols specifies number of columns of A
 * param alpha specifies scaling factor for matrix A
 * param a_src - buffer holding input matrix A
 * param lda_src - leading dimension of the matrix A
 * param a_dst - buffer holding output matrix A
 * param lda_dst - leading dimension of output matrix A
 * param conj specifies conjugation
 */
void my_scopy(blasint rows, blasint cols, float alpha, float *a_src, int lda_src, 
           float *a_dst, blasint lda_dst)
{
    blasint i, j;
    for (i = 0; i != rows; i++)
    {
        for (j = 0; j != cols; j++)
            a_dst[i*lda_dst+j] = alpha*a_src[i*lda_src+j];
    }
}

void my_dcopy(blasint rows, blasint cols, double alpha, double *a_src, int lda_src, 
           double *a_dst, blasint lda_dst)
{
    blasint i, j;
    for (i = 0; i != rows; i++)
    {
        for (j = 0; j != cols; j++)
            a_dst[i*lda_dst+j] = alpha*a_src[i*lda_src+j];
    }
}

void my_ccopy(blasint rows, blasint cols, float *alpha, float *a_src, int lda_src, 
           float *a_dst, blasint lda_dst, int conj)
{
    blasint i, j;
    lda_dst *= 2;
    lda_src *= 2;
    for (i = 0; i != rows; i++)
    {
        for (j = 0; j != cols*2; j+=2){
            a_dst[i*lda_dst+j] = alpha[0] * a_src[i*lda_src+j] + conj * alpha[1] * a_src[i*lda_src+j+1];
            a_dst[i*lda_dst+j+1] = (-1.0f) * conj *alpha[0] * a_src[i*lda_src+j+1] + alpha[1] * a_src[i*lda_src+j];
        }
    }
}

void my_zcopy(blasint rows, blasint cols, double *alpha, double *a_src, int lda_src, 
           double *a_dst, blasint lda_dst, int conj)
{
    blasint i, j;
    lda_dst *= 2;
    lda_src *= 2;
    for (i = 0; i != rows; i++)
    {
        for (j = 0; j != cols*2; j+=2){
            a_dst[i*lda_dst+j] = alpha[0] * a_src[i*lda_src+j] + conj * alpha[1] * a_src[i*lda_src+j+1];
            a_dst[i*lda_dst+j+1] = (-1.0) * conj *alpha[0] * a_src[i*lda_src+j+1] + alpha[1] * a_src[i*lda_src+j];
        }
    }
}
