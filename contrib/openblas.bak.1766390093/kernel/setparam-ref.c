/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* Copyright 2023 The OpenBLAS Project.                              */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include <stdio.h>
#include <string.h>
#include "common.h"

#ifdef BUILD_KERNEL
#include "kernelTS.h"
#endif

#undef DEBUG

static void init_parameter(void);

gotoblas_t TABLE_NAME = {
  DTB_DEFAULT_ENTRIES,

  SWITCH_RATIO,

  GEMM_DEFAULT_OFFSET_A, GEMM_DEFAULT_OFFSET_B, GEMM_DEFAULT_ALIGN,

#ifdef BUILD_BFLOAT16
  0, 0, 0,
  SBGEMM_DEFAULT_UNROLL_M, SBGEMM_DEFAULT_UNROLL_N,
#ifdef SBGEMM_DEFAULT_UNROLL_MN
 SBGEMM_DEFAULT_UNROLL_MN,
#else
 MAX(SBGEMM_DEFAULT_UNROLL_M, SBGEMM_DEFAULT_UNROLL_N),
#endif

  SBGEMM_ALIGN_K,
  0, // need_amxtile_permission

  sbstobf16_kTS, sbdtobf16_kTS, sbf16tos_kTS, dbf16tod_kTS,

  samax_kTS,  samin_kTS,  smax_kTS,  smin_kTS,
  isamax_kTS, isamin_kTS, ismax_kTS, ismin_kTS,
  snrm2_kTS,  sasum_kTS,  ssum_kTS, scopy_kTS, sbdot_kTS,
  dsdot_kTS,
  srot_kTS,   srotm_kTS,  saxpy_kTS,  sscal_kTS, sswap_kTS,
  sbgemv_nTS, sbgemv_tTS, sger_kTS,
  ssymv_LTS, ssymv_UTS,

  sbgemm_kernelTS, sbgemm_betaTS,
#if SBGEMM_DEFAULT_UNROLL_M != SBGEMM_DEFAULT_UNROLL_N
  sbgemm_incopyTS, sbgemm_itcopyTS,
#else
  sbgemm_oncopyTS, sbgemm_otcopyTS,
#endif
  sbgemm_oncopyTS, sbgemm_otcopyTS,

  strsm_kernel_LNTS, strsm_kernel_LTTS, strsm_kernel_RNTS, strsm_kernel_RTTS,
#if SGEMM_DEFAULT_UNROLL_M != SGEMM_DEFAULT_UNROLL_N
  strsm_iunucopyTS, strsm_iunncopyTS, strsm_iutucopyTS, strsm_iutncopyTS,
  strsm_ilnucopyTS, strsm_ilnncopyTS, strsm_iltucopyTS, strsm_iltncopyTS,
#else
  strsm_ounucopyTS, strsm_ounncopyTS, strsm_outucopyTS, strsm_outncopyTS,
  strsm_olnucopyTS, strsm_olnncopyTS, strsm_oltucopyTS, strsm_oltncopyTS,
#endif
  strsm_ounucopyTS, strsm_ounncopyTS, strsm_outucopyTS, strsm_outncopyTS,
  strsm_olnucopyTS, strsm_olnncopyTS, strsm_oltucopyTS, strsm_oltncopyTS,
  strmm_kernel_RNTS, strmm_kernel_RTTS, strmm_kernel_LNTS, strmm_kernel_LTTS,
#if SGEMM_DEFAULT_UNROLL_M != SGEMM_DEFAULT_UNROLL_N
  strmm_iunucopyTS, strmm_iunncopyTS, strmm_iutucopyTS, strmm_iutncopyTS,
  strmm_ilnucopyTS, strmm_ilnncopyTS, strmm_iltucopyTS, strmm_iltncopyTS,
#else
  strmm_ounucopyTS, strmm_ounncopyTS, strmm_outucopyTS, strmm_outncopyTS,
  strmm_olnucopyTS, strmm_olnncopyTS, strmm_oltucopyTS, strmm_oltncopyTS,
#endif
  strmm_ounucopyTS, strmm_ounncopyTS, strmm_outucopyTS, strmm_outncopyTS,
  strmm_olnucopyTS, strmm_olnncopyTS, strmm_oltucopyTS, strmm_oltncopyTS,
#if SGEMM_DEFAULT_UNROLL_M != SGEMM_DEFAULT_UNROLL_N
  ssymm_iutcopyTS, ssymm_iltcopyTS,
#else
  ssymm_outcopyTS, ssymm_oltcopyTS,
#endif
  ssymm_outcopyTS, ssymm_oltcopyTS,

#ifndef NO_LAPACK
  sneg_tcopyTS, slaswp_ncopyTS,
#else
  NULL,NULL,
#endif
#ifdef SMALL_MATRIX_OPT
  sbgemm_small_matrix_permitTS,
  sbgemm_small_kernel_nnTS, sbgemm_small_kernel_ntTS, sbgemm_small_kernel_tnTS, sbgemm_small_kernel_ttTS,
  sbgemm_small_kernel_b0_nnTS, sbgemm_small_kernel_b0_ntTS, sbgemm_small_kernel_b0_tnTS, sbgemm_small_kernel_b0_ttTS,
#endif
#endif

#if ( BUILD_SINGLE==1) || (BUILD_DOUBLE==1) || (BUILD_COMPLEX==1) || (BUILD_COMPLEX16==1)
  0, 0, 0,
  SGEMM_DEFAULT_UNROLL_M, SGEMM_DEFAULT_UNROLL_N,
#ifdef SGEMM_DEFAULT_UNROLL_MN
 SGEMM_DEFAULT_UNROLL_MN,
#else
 MAX(SGEMM_DEFAULT_UNROLL_M, SGEMM_DEFAULT_UNROLL_N),
#endif
#endif

#ifdef HAVE_EXCLUSIVE_CACHE
  1,
#else
  0,
#endif

#if (BUILD_SINGLE==1 ) || (BUILD_COMPLEX==1)
  samax_kTS,  samin_kTS,  smax_kTS,  smin_kTS,
#endif
#if (BUILD_SINGLE==1) || (BUILD_DOUBLE==1) || (BUILD_COMPLEX==1)
  isamax_kTS,
#endif 
#if (BUILD_SINGLE==1 ) || (BUILD_COMPLEX==1)
  isamin_kTS, ismax_kTS, ismin_kTS,
  snrm2_kTS,  sasum_kTS,
#endif 
#if BUILD_SINGLE == 1  
  ssum_kTS,
#endif

#if (BUILD_SINGLE==1) || (BUILD_DOUBLE==1) || (BUILD_COMPLEX==1)
  scopy_kTS, sdot_kTS,
//  dsdot_kTS,
  srot_kTS,  srotm_kTS,  saxpy_kTS,
#endif
#if (BUILD_SINGLE==1) || (BUILD_DOUBLE==1) || (BUILD_COMPLEX==1) || (BUILD_COMPLEX16==1)
  sscal_kTS,
#endif 
#if (BUILD_SINGLE==1) || (BUILD_DOUBLE==1) || (BUILD_COMPLEX==1)
  sswap_kTS,
  sgemv_nTS,  sgemv_tTS,
#endif
#if BUILD_SINGLE == 1
  sger_kTS,
#endif
#if BUILD_SINGLE == 1  
  ssymv_LTS, ssymv_UTS,
#endif

#if (BUILD_SINGLE==1) || (BUILD_DOUBLE==1) || (BUILD_COMPLEX==1)
#ifdef ARCH_X86_64
  sgemm_directTS,
  sgemm_direct_performantTS,	
#endif
#ifdef ARCH_ARM64
  sgemm_directTS,
#endif

  sgemm_kernelTS, sgemm_betaTS,
#if SGEMM_DEFAULT_UNROLL_M != SGEMM_DEFAULT_UNROLL_N
  sgemm_incopyTS, sgemm_itcopyTS,
#else
  sgemm_oncopyTS, sgemm_otcopyTS,
#endif
  sgemm_oncopyTS, sgemm_otcopyTS,
#endif

#if BUILD_SINGLE == 1 || BUILD_DOUBLE == 1 || BUILD_COMPLEX == 1
#ifdef SMALL_MATRIX_OPT
  sgemm_small_matrix_permitTS,
  sgemm_small_kernel_nnTS, sgemm_small_kernel_ntTS, sgemm_small_kernel_tnTS, sgemm_small_kernel_ttTS,
  sgemm_small_kernel_b0_nnTS, sgemm_small_kernel_b0_ntTS, sgemm_small_kernel_b0_tnTS, sgemm_small_kernel_b0_ttTS,
#endif
#endif

#if (BUILD_SINGLE==1) || (BUILD_DOUBLE==1) || (BUILD_COMPLEX == 1)
  strsm_kernel_LNTS, strsm_kernel_LTTS, strsm_kernel_RNTS, strsm_kernel_RTTS,
#if SGEMM_DEFAULT_UNROLL_M != SGEMM_DEFAULT_UNROLL_N
  strsm_iunucopyTS, strsm_iunncopyTS, strsm_iutucopyTS, strsm_iutncopyTS,
  strsm_ilnucopyTS, strsm_ilnncopyTS, strsm_iltucopyTS, strsm_iltncopyTS,
#else
  strsm_ounucopyTS, strsm_ounncopyTS, strsm_outucopyTS, strsm_outncopyTS,
  strsm_olnucopyTS, strsm_olnncopyTS, strsm_oltucopyTS, strsm_oltncopyTS,
#endif
  strsm_ounucopyTS, strsm_ounncopyTS, strsm_outucopyTS, strsm_outncopyTS,
  strsm_olnucopyTS, strsm_olnncopyTS, strsm_oltucopyTS, strsm_oltncopyTS,
#endif
#if (BUILD_SINGLE==1)
  strmm_kernel_RNTS, strmm_kernel_RTTS, strmm_kernel_LNTS, strmm_kernel_LTTS,
#if SGEMM_DEFAULT_UNROLL_M != SGEMM_DEFAULT_UNROLL_N
  strmm_iunucopyTS, strmm_iunncopyTS, strmm_iutucopyTS, strmm_iutncopyTS,
  strmm_ilnucopyTS, strmm_ilnncopyTS, strmm_iltucopyTS, strmm_iltncopyTS,
#else
  strmm_ounucopyTS, strmm_ounncopyTS, strmm_outucopyTS, strmm_outncopyTS,
  strmm_olnucopyTS, strmm_olnncopyTS, strmm_oltucopyTS, strmm_oltncopyTS,
#endif
  strmm_ounucopyTS, strmm_ounncopyTS, strmm_outucopyTS, strmm_outncopyTS,
  strmm_olnucopyTS, strmm_olnncopyTS, strmm_oltucopyTS, strmm_oltncopyTS,
#if SGEMM_DEFAULT_UNROLL_M != SGEMM_DEFAULT_UNROLL_N
  ssymm_iutcopyTS, ssymm_iltcopyTS,
#else
  ssymm_outcopyTS, ssymm_oltcopyTS,
#endif
  ssymm_outcopyTS, ssymm_oltcopyTS,
#ifndef NO_LAPACK
  sneg_tcopyTS, slaswp_ncopyTS,
#else
  NULL,NULL,
#endif
#endif

#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)  
  0, 0, 0,
  DGEMM_DEFAULT_UNROLL_M, DGEMM_DEFAULT_UNROLL_N,
#ifdef DGEMM_DEFAULT_UNROLL_MN
 DGEMM_DEFAULT_UNROLL_MN,
#else
 MAX(DGEMM_DEFAULT_UNROLL_M, DGEMM_DEFAULT_UNROLL_N),
#endif
#endif


#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)  
  damax_kTS,  damin_kTS,  dmax_kTS,  dmin_kTS,
  idamax_kTS, idamin_kTS, idmax_kTS, idmin_kTS,
  dnrm2_kTS, dasum_kTS,
#endif  
#if  (BUILD_DOUBLE==1)  
  dsum_kTS,
#endif
#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)  
  dcopy_kTS, ddot_kTS,
#endif
#if  (BUILD_SINGLE==1) || (BUILD_DOUBLE==1)  
  dsdot_kTS,
#endif
#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)  
  drot_kTS,
  drotm_kTS,
  daxpy_kTS,
  dscal_kTS, 
  dswap_kTS,
  dgemv_nTS,  dgemv_tTS,
#endif
#if  (BUILD_DOUBLE==1)  
  dger_kTS,
  dsymv_LTS,  dsymv_UTS,
#endif

#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)  
  dgemm_kernelTS, dgemm_betaTS,
#if DGEMM_DEFAULT_UNROLL_M != DGEMM_DEFAULT_UNROLL_N
  dgemm_incopyTS, dgemm_itcopyTS,
#else
  dgemm_oncopyTS, dgemm_otcopyTS,
#endif
  dgemm_oncopyTS, dgemm_otcopyTS,
#endif

#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)  
#ifdef SMALL_MATRIX_OPT
  dgemm_small_matrix_permitTS,
  dgemm_small_kernel_nnTS, dgemm_small_kernel_ntTS, dgemm_small_kernel_tnTS, dgemm_small_kernel_ttTS,
  dgemm_small_kernel_b0_nnTS, dgemm_small_kernel_b0_ntTS, dgemm_small_kernel_b0_tnTS, dgemm_small_kernel_b0_ttTS,
#endif
#endif
#if  (BUILD_DOUBLE==1)   
  dtrsm_kernel_LNTS, dtrsm_kernel_LTTS, dtrsm_kernel_RNTS, dtrsm_kernel_RTTS,
#if DGEMM_DEFAULT_UNROLL_M != DGEMM_DEFAULT_UNROLL_N
  dtrsm_iunucopyTS, dtrsm_iunncopyTS, dtrsm_iutucopyTS, dtrsm_iutncopyTS,
  dtrsm_ilnucopyTS, dtrsm_ilnncopyTS, dtrsm_iltucopyTS, dtrsm_iltncopyTS,
#else
  dtrsm_ounucopyTS, dtrsm_ounncopyTS, dtrsm_outucopyTS, dtrsm_outncopyTS,
  dtrsm_olnucopyTS, dtrsm_olnncopyTS, dtrsm_oltucopyTS, dtrsm_oltncopyTS,
#endif
  dtrsm_ounucopyTS, dtrsm_ounncopyTS, dtrsm_outucopyTS, dtrsm_outncopyTS,
  dtrsm_olnucopyTS, dtrsm_olnncopyTS, dtrsm_oltucopyTS, dtrsm_oltncopyTS,
  dtrmm_kernel_RNTS, dtrmm_kernel_RTTS, dtrmm_kernel_LNTS, dtrmm_kernel_LTTS,
#if DGEMM_DEFAULT_UNROLL_M != DGEMM_DEFAULT_UNROLL_N
  dtrmm_iunucopyTS, dtrmm_iunncopyTS, dtrmm_iutucopyTS, dtrmm_iutncopyTS,
  dtrmm_ilnucopyTS, dtrmm_ilnncopyTS, dtrmm_iltucopyTS, dtrmm_iltncopyTS,
#else
  dtrmm_ounucopyTS, dtrmm_ounncopyTS, dtrmm_outucopyTS, dtrmm_outncopyTS,
  dtrmm_olnucopyTS, dtrmm_olnncopyTS, dtrmm_oltucopyTS, dtrmm_oltncopyTS,
#endif
  dtrmm_ounucopyTS, dtrmm_ounncopyTS, dtrmm_outucopyTS, dtrmm_outncopyTS,
  dtrmm_olnucopyTS, dtrmm_olnncopyTS, dtrmm_oltucopyTS, dtrmm_oltncopyTS,
#if DGEMM_DEFAULT_UNROLL_M != DGEMM_DEFAULT_UNROLL_N
  dsymm_iutcopyTS, dsymm_iltcopyTS,
#else
  dsymm_outcopyTS, dsymm_oltcopyTS,
#endif
  dsymm_outcopyTS, dsymm_oltcopyTS,

#ifndef NO_LAPACK
  dneg_tcopyTS, dlaswp_ncopyTS,
#else
  NULL, NULL,
#endif

#endif

#ifdef EXPRECISION

  0, 0, 0,
  QGEMM_DEFAULT_UNROLL_M, QGEMM_DEFAULT_UNROLL_N, MAX(QGEMM_DEFAULT_UNROLL_M, QGEMM_DEFAULT_UNROLL_N),

  qamax_kTS,  qamin_kTS,  qmax_kTS,  qmin_kTS,
  iqamax_kTS, iqamin_kTS, iqmax_kTS, iqmin_kTS,
  qnrm2_kTS,  qasum_kTS,  qsum_kTS, qcopy_kTS, qdot_kTS,
  qrot_kTS,   qrotm_kTS,  qaxpy_kTS,  qscal_kTS, qswap_kTS,
  qgemv_nTS,  qgemv_tTS,  qger_kTS,
  qsymv_LTS,  qsymv_UTS,
  qgemm_kernelTS, qgemm_betaTS,
#if QGEMM_DEFAULT_UNROLL_M != QGEMM_DEFAULT_UNROLL_N
  qgemm_incopyTS, qgemm_itcopyTS,
#else
  qgemm_oncopyTS, qgemm_otcopyTS,
#endif
  qgemm_oncopyTS, qgemm_otcopyTS,
  qtrsm_kernel_LNTS, qtrsm_kernel_LTTS, qtrsm_kernel_RNTS, qtrsm_kernel_RTTS,
#if QGEMM_DEFAULT_UNROLL_M != QGEMM_DEFAULT_UNROLL_N
  qtrsm_iunucopyTS, qtrsm_iunncopyTS, qtrsm_iutucopyTS, qtrsm_iutncopyTS,
  qtrsm_ilnucopyTS, qtrsm_ilnncopyTS, qtrsm_iltucopyTS, qtrsm_iltncopyTS,
#else
  qtrsm_ounucopyTS, qtrsm_ounncopyTS, qtrsm_outucopyTS, qtrsm_outncopyTS,
  qtrsm_olnucopyTS, qtrsm_olnncopyTS, qtrsm_oltucopyTS, qtrsm_oltncopyTS,
#endif
  qtrsm_ounucopyTS, qtrsm_ounncopyTS, qtrsm_outucopyTS, qtrsm_outncopyTS,
  qtrsm_olnucopyTS, qtrsm_olnncopyTS, qtrsm_oltucopyTS, qtrsm_oltncopyTS,
  qtrmm_kernel_RNTS, qtrmm_kernel_RTTS, qtrmm_kernel_LNTS, qtrmm_kernel_LTTS,
#if QGEMM_DEFAULT_UNROLL_M != QGEMM_DEFAULT_UNROLL_N
  qtrmm_iunucopyTS, qtrmm_iunncopyTS, qtrmm_iutucopyTS, qtrmm_iutncopyTS,
  qtrmm_ilnucopyTS, qtrmm_ilnncopyTS, qtrmm_iltucopyTS, qtrmm_iltncopyTS,
#else
  qtrmm_ounucopyTS, qtrmm_ounncopyTS, qtrmm_outucopyTS, qtrmm_outncopyTS,
  qtrmm_olnucopyTS, qtrmm_olnncopyTS, qtrmm_oltucopyTS, qtrmm_oltncopyTS,
#endif
  qtrmm_ounucopyTS, qtrmm_ounncopyTS, qtrmm_outucopyTS, qtrmm_outncopyTS,
  qtrmm_olnucopyTS, qtrmm_olnncopyTS, qtrmm_oltucopyTS, qtrmm_oltncopyTS,
#if QGEMM_DEFAULT_UNROLL_M != QGEMM_DEFAULT_UNROLL_N
  qsymm_iutcopyTS, qsymm_iltcopyTS,
#else
  qsymm_outcopyTS, qsymm_oltcopyTS,
#endif
  qsymm_outcopyTS, qsymm_oltcopyTS,

#ifndef NO_LAPACK
  qneg_tcopyTS, qlaswp_ncopyTS,
#else
  NULL, NULL,
#endif

#endif

#if (BUILD_COMPLEX)
  0, 0, 0,
  CGEMM_DEFAULT_UNROLL_M, CGEMM_DEFAULT_UNROLL_N,
#ifdef CGEMM_DEFAULT_UNROLL_MN
 CGEMM_DEFAULT_UNROLL_MN,
#else
 MAX(CGEMM_DEFAULT_UNROLL_M, CGEMM_DEFAULT_UNROLL_N),
#endif
#if (BUILD_COMPLEX)
  camax_kTS, camin_kTS,
#endif
#if (BUILD_COMPLEX)
  icamax_kTS, 
#endif
#if (BUILD_COMPLEX)
  icamin_kTS,
  cnrm2_kTS, casum_kTS, csum_kTS,
#endif
#if (BUILD_COMPLEX)
  ccopy_kTS, cdotu_kTS, cdotc_kTS,
#endif
#if (BUILD_COMPLEX)
 csrot_kTS,
#endif
#if (BUILD_COMPLEX)
  caxpy_kTS,
  caxpyc_kTS, 
  cscal_kTS, 
  cswap_kTS,

  cgemv_nTS, cgemv_tTS, cgemv_rTS, cgemv_cTS,
  cgemv_oTS, cgemv_uTS, cgemv_sTS, cgemv_dTS,
#endif
#if (BUILD_COMPLEX)
  cgeru_kTS, cgerc_kTS, cgerv_kTS, cgerd_kTS,
  csymv_LTS, csymv_UTS,
  chemv_LTS, chemv_UTS, chemv_MTS, chemv_VTS,
#endif
#if (BUILD_COMPLEX)
  cgemm_kernel_nTS, cgemm_kernel_lTS, cgemm_kernel_rTS, cgemm_kernel_bTS,
  cgemm_betaTS,
#if CGEMM_DEFAULT_UNROLL_M != CGEMM_DEFAULT_UNROLL_N
  cgemm_incopyTS, cgemm_itcopyTS,
#else
  cgemm_oncopyTS, cgemm_otcopyTS,
#endif
  cgemm_oncopyTS, cgemm_otcopyTS,

#ifdef SMALL_MATRIX_OPT
  cgemm_small_matrix_permitTS,
  cgemm_small_kernel_nnTS, cgemm_small_kernel_ntTS, cgemm_small_kernel_nrTS, cgemm_small_kernel_ncTS,
  cgemm_small_kernel_tnTS, cgemm_small_kernel_ttTS, cgemm_small_kernel_trTS, cgemm_small_kernel_tcTS,
  cgemm_small_kernel_rnTS, cgemm_small_kernel_rtTS, cgemm_small_kernel_rrTS, cgemm_small_kernel_rcTS,
  cgemm_small_kernel_cnTS, cgemm_small_kernel_ctTS, cgemm_small_kernel_crTS, cgemm_small_kernel_ccTS,
  cgemm_small_kernel_b0_nnTS, cgemm_small_kernel_b0_ntTS, cgemm_small_kernel_b0_nrTS, cgemm_small_kernel_b0_ncTS,
  cgemm_small_kernel_b0_tnTS, cgemm_small_kernel_b0_ttTS, cgemm_small_kernel_b0_trTS, cgemm_small_kernel_b0_tcTS,
  cgemm_small_kernel_b0_rnTS, cgemm_small_kernel_b0_rtTS, cgemm_small_kernel_b0_rrTS, cgemm_small_kernel_b0_rcTS,
  cgemm_small_kernel_b0_cnTS, cgemm_small_kernel_b0_ctTS, cgemm_small_kernel_b0_crTS, cgemm_small_kernel_b0_ccTS,
#endif

  ctrsm_kernel_LNTS, ctrsm_kernel_LTTS, ctrsm_kernel_LRTS, ctrsm_kernel_LCTS,
  ctrsm_kernel_RNTS, ctrsm_kernel_RTTS, ctrsm_kernel_RRTS, ctrsm_kernel_RCTS,

#if CGEMM_DEFAULT_UNROLL_M != CGEMM_DEFAULT_UNROLL_N
  ctrsm_iunucopyTS,  ctrsm_iunncopyTS,  ctrsm_iutucopyTS,  ctrsm_iutncopyTS,
  ctrsm_ilnucopyTS,  ctrsm_ilnncopyTS,  ctrsm_iltucopyTS,  ctrsm_iltncopyTS,
#else
  ctrsm_ounucopyTS,  ctrsm_ounncopyTS,  ctrsm_outucopyTS,  ctrsm_outncopyTS,
  ctrsm_olnucopyTS,  ctrsm_olnncopyTS,  ctrsm_oltucopyTS,  ctrsm_oltncopyTS,
#endif
  ctrsm_ounucopyTS,  ctrsm_ounncopyTS,  ctrsm_outucopyTS,  ctrsm_outncopyTS,
  ctrsm_olnucopyTS,  ctrsm_olnncopyTS,  ctrsm_oltucopyTS,  ctrsm_oltncopyTS,
#endif
#endif
#if (BUILD_COMPLEX)

  ctrmm_kernel_RNTS,  ctrmm_kernel_RTTS,  ctrmm_kernel_RRTS,  ctrmm_kernel_RCTS,
  ctrmm_kernel_LNTS,  ctrmm_kernel_LTTS,  ctrmm_kernel_LRTS,  ctrmm_kernel_LCTS,

#if CGEMM_DEFAULT_UNROLL_M != CGEMM_DEFAULT_UNROLL_N
  ctrmm_iunucopyTS,  ctrmm_iunncopyTS,  ctrmm_iutucopyTS,  ctrmm_iutncopyTS,
  ctrmm_ilnucopyTS,  ctrmm_ilnncopyTS,  ctrmm_iltucopyTS,  ctrmm_iltncopyTS,
#else
  ctrmm_ounucopyTS,  ctrmm_ounncopyTS,  ctrmm_outucopyTS,  ctrmm_outncopyTS,
  ctrmm_olnucopyTS,  ctrmm_olnncopyTS,  ctrmm_oltucopyTS,  ctrmm_oltncopyTS,
#endif
  ctrmm_ounucopyTS,  ctrmm_ounncopyTS,  ctrmm_outucopyTS,  ctrmm_outncopyTS,
  ctrmm_olnucopyTS,  ctrmm_olnncopyTS,  ctrmm_oltucopyTS,  ctrmm_oltncopyTS,

#if CGEMM_DEFAULT_UNROLL_M != CGEMM_DEFAULT_UNROLL_N
  csymm_iutcopyTS,  csymm_iltcopyTS,
#else
  csymm_outcopyTS,  csymm_oltcopyTS,
#endif
  csymm_outcopyTS,  csymm_oltcopyTS,
#if CGEMM_DEFAULT_UNROLL_M != CGEMM_DEFAULT_UNROLL_N
  chemm_iutcopyTS,  chemm_iltcopyTS,
#else
  chemm_outcopyTS,  chemm_oltcopyTS,
#endif
  chemm_outcopyTS,  chemm_oltcopyTS,

  0, 0, 0,

#if (USE_GEMM3M)
#ifdef CGEMM3M_DEFAULT_UNROLL_M
  CGEMM3M_DEFAULT_UNROLL_M, CGEMM3M_DEFAULT_UNROLL_N, MAX(CGEMM3M_DEFAULT_UNROLL_M, CGEMM3M_DEFAULT_UNROLL_N),
#else
  SGEMM_DEFAULT_UNROLL_M, SGEMM_DEFAULT_UNROLL_N, MAX(SGEMM_DEFAULT_UNROLL_M, SGEMM_DEFAULT_UNROLL_N),
#endif


  cgemm3m_kernelTS,

  cgemm3m_incopybTS,  cgemm3m_incopyrTS,
  cgemm3m_incopyiTS,  cgemm3m_itcopybTS,
  cgemm3m_itcopyrTS,  cgemm3m_itcopyiTS,
  cgemm3m_oncopybTS,  cgemm3m_oncopyrTS,
  cgemm3m_oncopyiTS,  cgemm3m_otcopybTS,
  cgemm3m_otcopyrTS,  cgemm3m_otcopyiTS,

  csymm3m_iucopybTS,  csymm3m_ilcopybTS,
  csymm3m_iucopyrTS,  csymm3m_ilcopyrTS,
  csymm3m_iucopyiTS,  csymm3m_ilcopyiTS,
  csymm3m_oucopybTS,  csymm3m_olcopybTS,
  csymm3m_oucopyrTS,  csymm3m_olcopyrTS,
  csymm3m_oucopyiTS,  csymm3m_olcopyiTS,

  chemm3m_iucopybTS,  chemm3m_ilcopybTS,
  chemm3m_iucopyrTS,  chemm3m_ilcopyrTS,
  chemm3m_iucopyiTS,  chemm3m_ilcopyiTS,

  chemm3m_oucopybTS,  chemm3m_olcopybTS,
  chemm3m_oucopyrTS,  chemm3m_olcopyrTS,
  chemm3m_oucopyiTS,  chemm3m_olcopyiTS,
#else
  0, 0, 0,

  NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
#endif
#endif

#if (BUILD_COMPLEX)
#ifndef NO_LAPACK
  cneg_tcopyTS,
  
   claswp_ncopyTS,
#else
  NULL, NULL,
#endif

#endif

#if BUILD_COMPLEX16 == 1
  0, 0, 0,
  ZGEMM_DEFAULT_UNROLL_M, ZGEMM_DEFAULT_UNROLL_N,
#ifdef ZGEMM_DEFAULT_UNROLL_MN
 ZGEMM_DEFAULT_UNROLL_MN,
#else
 MAX(ZGEMM_DEFAULT_UNROLL_M, ZGEMM_DEFAULT_UNROLL_N),
#endif

  zamax_kTS, zamin_kTS, izamax_kTS, izamin_kTS,
  znrm2_kTS, zasum_kTS, zsum_kTS, zcopy_kTS,
  zdotu_kTS, zdotc_kTS, zdrot_kTS,
  zaxpy_kTS, zaxpyc_kTS, zscal_kTS, zswap_kTS,

  zgemv_nTS, zgemv_tTS, zgemv_rTS, zgemv_cTS,
  zgemv_oTS, zgemv_uTS, zgemv_sTS, zgemv_dTS,
  zgeru_kTS, zgerc_kTS, zgerv_kTS, zgerd_kTS,
  zsymv_LTS, zsymv_UTS,
  zhemv_LTS, zhemv_UTS, zhemv_MTS, zhemv_VTS,

  zgemm_kernel_nTS, zgemm_kernel_lTS, zgemm_kernel_rTS, zgemm_kernel_bTS,
  zgemm_betaTS,

#if ZGEMM_DEFAULT_UNROLL_M != ZGEMM_DEFAULT_UNROLL_N
  zgemm_incopyTS, zgemm_itcopyTS,
#else
  zgemm_oncopyTS, zgemm_otcopyTS,
#endif
  zgemm_oncopyTS, zgemm_otcopyTS,

#ifdef SMALL_MATRIX_OPT
  zgemm_small_matrix_permitTS,
  zgemm_small_kernel_nnTS, zgemm_small_kernel_ntTS, zgemm_small_kernel_nrTS, zgemm_small_kernel_ncTS,
  zgemm_small_kernel_tnTS, zgemm_small_kernel_ttTS, zgemm_small_kernel_trTS, zgemm_small_kernel_tcTS,
  zgemm_small_kernel_rnTS, zgemm_small_kernel_rtTS, zgemm_small_kernel_rrTS, zgemm_small_kernel_rcTS,
  zgemm_small_kernel_cnTS, zgemm_small_kernel_ctTS, zgemm_small_kernel_crTS, zgemm_small_kernel_ccTS,
  zgemm_small_kernel_b0_nnTS, zgemm_small_kernel_b0_ntTS, zgemm_small_kernel_b0_nrTS, zgemm_small_kernel_b0_ncTS,
  zgemm_small_kernel_b0_tnTS, zgemm_small_kernel_b0_ttTS, zgemm_small_kernel_b0_trTS, zgemm_small_kernel_b0_tcTS,
  zgemm_small_kernel_b0_rnTS, zgemm_small_kernel_b0_rtTS, zgemm_small_kernel_b0_rrTS, zgemm_small_kernel_b0_rcTS,
  zgemm_small_kernel_b0_cnTS, zgemm_small_kernel_b0_ctTS, zgemm_small_kernel_b0_crTS, zgemm_small_kernel_b0_ccTS,
#endif

  ztrsm_kernel_LNTS, ztrsm_kernel_LTTS, ztrsm_kernel_LRTS, ztrsm_kernel_LCTS,
  ztrsm_kernel_RNTS, ztrsm_kernel_RTTS, ztrsm_kernel_RRTS, ztrsm_kernel_RCTS,

#if ZGEMM_DEFAULT_UNROLL_M != ZGEMM_DEFAULT_UNROLL_N
  ztrsm_iunucopyTS,  ztrsm_iunncopyTS,  ztrsm_iutucopyTS,  ztrsm_iutncopyTS,
  ztrsm_ilnucopyTS,  ztrsm_ilnncopyTS,  ztrsm_iltucopyTS,  ztrsm_iltncopyTS,
#else
  ztrsm_ounucopyTS,  ztrsm_ounncopyTS,  ztrsm_outucopyTS,  ztrsm_outncopyTS,
  ztrsm_olnucopyTS,  ztrsm_olnncopyTS,  ztrsm_oltucopyTS,  ztrsm_oltncopyTS,
#endif
  ztrsm_ounucopyTS,  ztrsm_ounncopyTS,  ztrsm_outucopyTS,  ztrsm_outncopyTS,
  ztrsm_olnucopyTS,  ztrsm_olnncopyTS,  ztrsm_oltucopyTS,  ztrsm_oltncopyTS,

  ztrmm_kernel_RNTS,  ztrmm_kernel_RTTS,  ztrmm_kernel_RRTS,  ztrmm_kernel_RCTS,
  ztrmm_kernel_LNTS,  ztrmm_kernel_LTTS,  ztrmm_kernel_LRTS,  ztrmm_kernel_LCTS,

#if ZGEMM_DEFAULT_UNROLL_M != ZGEMM_DEFAULT_UNROLL_N
  ztrmm_iunucopyTS,  ztrmm_iunncopyTS,  ztrmm_iutucopyTS,  ztrmm_iutncopyTS,
  ztrmm_ilnucopyTS,  ztrmm_ilnncopyTS,  ztrmm_iltucopyTS,  ztrmm_iltncopyTS,
#else
  ztrmm_ounucopyTS,  ztrmm_ounncopyTS,  ztrmm_outucopyTS,  ztrmm_outncopyTS,
  ztrmm_olnucopyTS,  ztrmm_olnncopyTS,  ztrmm_oltucopyTS,  ztrmm_oltncopyTS,
#endif
  ztrmm_ounucopyTS,  ztrmm_ounncopyTS,  ztrmm_outucopyTS,  ztrmm_outncopyTS,
  ztrmm_olnucopyTS,  ztrmm_olnncopyTS,  ztrmm_oltucopyTS,  ztrmm_oltncopyTS,

#if ZGEMM_DEFAULT_UNROLL_M != ZGEMM_DEFAULT_UNROLL_N
  zsymm_iutcopyTS,  zsymm_iltcopyTS,
#else
  zsymm_outcopyTS,  zsymm_oltcopyTS,
#endif
  zsymm_outcopyTS,  zsymm_oltcopyTS,
#if ZGEMM_DEFAULT_UNROLL_M != ZGEMM_DEFAULT_UNROLL_N
  zhemm_iutcopyTS,  zhemm_iltcopyTS,
#else
  zhemm_outcopyTS,  zhemm_oltcopyTS,
#endif
  zhemm_outcopyTS,  zhemm_oltcopyTS,

  0, 0, 0,
#if (USE_GEMM3M)
#ifdef ZGEMM3M_DEFAULT_UNROLL_M
  ZGEMM3M_DEFAULT_UNROLL_M, ZGEMM3M_DEFAULT_UNROLL_N, MAX(ZGEMM3M_DEFAULT_UNROLL_M, ZGEMM3M_DEFAULT_UNROLL_N),
#else
  DGEMM_DEFAULT_UNROLL_M, DGEMM_DEFAULT_UNROLL_N, MAX(DGEMM_DEFAULT_UNROLL_M, DGEMM_DEFAULT_UNROLL_N),
#endif


  zgemm3m_kernelTS,

  zgemm3m_incopybTS,  zgemm3m_incopyrTS,
  zgemm3m_incopyiTS,  zgemm3m_itcopybTS,
  zgemm3m_itcopyrTS,  zgemm3m_itcopyiTS,
  zgemm3m_oncopybTS,  zgemm3m_oncopyrTS,
  zgemm3m_oncopyiTS,  zgemm3m_otcopybTS,
  zgemm3m_otcopyrTS,  zgemm3m_otcopyiTS,

  zsymm3m_iucopybTS,  zsymm3m_ilcopybTS,
  zsymm3m_iucopyrTS,  zsymm3m_ilcopyrTS,
  zsymm3m_iucopyiTS,  zsymm3m_ilcopyiTS,
  zsymm3m_oucopybTS,  zsymm3m_olcopybTS,
  zsymm3m_oucopyrTS,  zsymm3m_olcopyrTS,
  zsymm3m_oucopyiTS,  zsymm3m_olcopyiTS,

  zhemm3m_iucopybTS,  zhemm3m_ilcopybTS,
  zhemm3m_iucopyrTS,  zhemm3m_ilcopyrTS,
  zhemm3m_iucopyiTS,  zhemm3m_ilcopyiTS,

  zhemm3m_oucopybTS,  zhemm3m_olcopybTS,
  zhemm3m_oucopyrTS,  zhemm3m_olcopyrTS,
  zhemm3m_oucopyiTS,  zhemm3m_olcopyiTS,
#else
  0, 0, 0,

  NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
#endif

#ifndef NO_LAPACK
  zneg_tcopyTS, zlaswp_ncopyTS,
#else
  NULL, NULL,
#endif

#endif

#ifdef EXPRECISION

  0, 0, 0,
  XGEMM_DEFAULT_UNROLL_M, XGEMM_DEFAULT_UNROLL_N, MAX(XGEMM_DEFAULT_UNROLL_M, XGEMM_DEFAULT_UNROLL_N),

  xamax_kTS, xamin_kTS, ixamax_kTS, ixamin_kTS,
  xnrm2_kTS, xasum_kTS, xsum_kTS, xcopy_kTS,
  xdotu_kTS, xdotc_kTS, xqrot_kTS,
  xaxpy_kTS, xaxpyc_kTS, xscal_kTS, xswap_kTS,

  xgemv_nTS, xgemv_tTS, xgemv_rTS, xgemv_cTS,
  xgemv_oTS, xgemv_uTS, xgemv_sTS, xgemv_dTS,
  xgeru_kTS, xgerc_kTS, xgerv_kTS, xgerd_kTS,
  xsymv_LTS, xsymv_UTS,
  xhemv_LTS, xhemv_UTS, xhemv_MTS, xhemv_VTS,

  xgemm_kernel_nTS, xgemm_kernel_lTS, xgemm_kernel_rTS, xgemm_kernel_bTS,
  xgemm_betaTS,

#if XGEMM_DEFAULT_UNROLL_M != XGEMM_DEFAULT_UNROLL_N
  xgemm_incopyTS, xgemm_itcopyTS,
#else
  xgemm_oncopyTS, xgemm_otcopyTS,
#endif
  xgemm_oncopyTS, xgemm_otcopyTS,

  xtrsm_kernel_LNTS, xtrsm_kernel_LTTS, xtrsm_kernel_LRTS, xtrsm_kernel_LCTS,
  xtrsm_kernel_RNTS, xtrsm_kernel_RTTS, xtrsm_kernel_RRTS, xtrsm_kernel_RCTS,

#if XGEMM_DEFAULT_UNROLL_M != XGEMM_DEFAULT_UNROLL_N
  xtrsm_iunucopyTS,  xtrsm_iunncopyTS,  xtrsm_iutucopyTS,  xtrsm_iutncopyTS,
  xtrsm_ilnucopyTS,  xtrsm_ilnncopyTS,  xtrsm_iltucopyTS,  xtrsm_iltncopyTS,
#else
  xtrsm_ounucopyTS,  xtrsm_ounncopyTS,  xtrsm_outucopyTS,  xtrsm_outncopyTS,
  xtrsm_olnucopyTS,  xtrsm_olnncopyTS,  xtrsm_oltucopyTS,  xtrsm_oltncopyTS,
#endif
  xtrsm_ounucopyTS,  xtrsm_ounncopyTS,  xtrsm_outucopyTS,  xtrsm_outncopyTS,
  xtrsm_olnucopyTS,  xtrsm_olnncopyTS,  xtrsm_oltucopyTS,  xtrsm_oltncopyTS,

  xtrmm_kernel_RNTS,  xtrmm_kernel_RTTS,  xtrmm_kernel_RRTS,  xtrmm_kernel_RCTS,
  xtrmm_kernel_LNTS,  xtrmm_kernel_LTTS,  xtrmm_kernel_LRTS,  xtrmm_kernel_LCTS,

#if XGEMM_DEFAULT_UNROLL_M != XGEMM_DEFAULT_UNROLL_N
  xtrmm_iunucopyTS,  xtrmm_iunncopyTS,  xtrmm_iutucopyTS,  xtrmm_iutncopyTS,
  xtrmm_ilnucopyTS,  xtrmm_ilnncopyTS,  xtrmm_iltucopyTS,  xtrmm_iltncopyTS,
#else
  xtrmm_ounucopyTS,  xtrmm_ounncopyTS,  xtrmm_outucopyTS,  xtrmm_outncopyTS,
  xtrmm_olnucopyTS,  xtrmm_olnncopyTS,  xtrmm_oltucopyTS,  xtrmm_oltncopyTS,
#endif
  xtrmm_ounucopyTS,  xtrmm_ounncopyTS,  xtrmm_outucopyTS,  xtrmm_outncopyTS,
  xtrmm_olnucopyTS,  xtrmm_olnncopyTS,  xtrmm_oltucopyTS,  xtrmm_oltncopyTS,

#if XGEMM_DEFAULT_UNROLL_M != XGEMM_DEFAULT_UNROLL_N
  xsymm_iutcopyTS,  xsymm_iltcopyTS,
#else
  xsymm_outcopyTS,  xsymm_oltcopyTS,
#endif
  xsymm_outcopyTS,  xsymm_oltcopyTS,
#if XGEMM_DEFAULT_UNROLL_M != XGEMM_DEFAULT_UNROLL_N
  xhemm_iutcopyTS,  xhemm_iltcopyTS,
#else
  xhemm_outcopyTS,  xhemm_oltcopyTS,
#endif
  xhemm_outcopyTS,  xhemm_oltcopyTS,

  0, 0, 0,
#if (USE_GEMM3M)
  QGEMM_DEFAULT_UNROLL_M, QGEMM_DEFAULT_UNROLL_N, MAX(QGEMM_DEFAULT_UNROLL_M, QGEMM_DEFAULT_UNROLL_N),

  xgemm3m_kernelTS,

  xgemm3m_incopybTS,  xgemm3m_incopyrTS,
  xgemm3m_incopyiTS,  xgemm3m_itcopybTS,
  xgemm3m_itcopyrTS,  xgemm3m_itcopyiTS,
  xgemm3m_oncopybTS,  xgemm3m_oncopyrTS,
  xgemm3m_oncopyiTS,  xgemm3m_otcopybTS,
  xgemm3m_otcopyrTS,  xgemm3m_otcopyiTS,

  xsymm3m_iucopybTS,  xsymm3m_ilcopybTS,
  xsymm3m_iucopyrTS,  xsymm3m_ilcopyrTS,
  xsymm3m_iucopyiTS,  xsymm3m_ilcopyiTS,
  xsymm3m_oucopybTS,  xsymm3m_olcopybTS,
  xsymm3m_oucopyrTS,  xsymm3m_olcopyrTS,
  xsymm3m_oucopyiTS,  xsymm3m_olcopyiTS,

  xhemm3m_iucopybTS,  xhemm3m_ilcopybTS,
  xhemm3m_iucopyrTS,  xhemm3m_ilcopyrTS,
  xhemm3m_iucopyiTS,  xhemm3m_ilcopyiTS,

  xhemm3m_oucopybTS,  xhemm3m_olcopybTS,
  xhemm3m_oucopyrTS,  xhemm3m_olcopyrTS,
  xhemm3m_oucopyiTS,  xhemm3m_olcopyiTS,
#else
  0, 0, 0,

  NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,

  NULL, NULL,
  NULL, NULL,
  NULL, NULL,
#endif

#ifndef NO_LAPACK
  xneg_tcopyTS, xlaswp_ncopyTS,
#else
  NULL, NULL,
#endif

#endif

  init_parameter,

  SNUMOPT, DNUMOPT, QNUMOPT,
#if BUILD_SINGLE == 1
  saxpby_kTS,
#endif
#if BUILD_DOUBLE  == 1
  daxpby_kTS,
#endif
#if BUILD_COMPLEX == 1
  caxpby_kTS,
#endif
#if BUILD_COMPLEX16== 1
  zaxpby_kTS,
#endif

#if BUILD_SINGLE == 1
  somatcopy_k_cnTS, somatcopy_k_ctTS, somatcopy_k_rnTS, somatcopy_k_rtTS,
#endif
#if BUILD_DOUBLE== 1
  domatcopy_k_cnTS, domatcopy_k_ctTS, domatcopy_k_rnTS, domatcopy_k_rtTS,
#endif
#if BUILD_COMPLEX == 1
  comatcopy_k_cnTS, comatcopy_k_ctTS, comatcopy_k_rnTS, comatcopy_k_rtTS,
  comatcopy_k_cncTS, comatcopy_k_ctcTS, comatcopy_k_rncTS, comatcopy_k_rtcTS,
#endif
#if BUILD_COMPLEX16 == 1
  zomatcopy_k_cnTS, zomatcopy_k_ctTS, zomatcopy_k_rnTS, zomatcopy_k_rtTS,
  zomatcopy_k_cncTS, zomatcopy_k_ctcTS, zomatcopy_k_rncTS, zomatcopy_k_rtcTS,
#endif

#if BUILD_SINGLE == 1
  simatcopy_k_cnTS, simatcopy_k_ctTS, simatcopy_k_rnTS, simatcopy_k_rtTS,
#endif
#if BUILD_DOUBLE== 1
  dimatcopy_k_cnTS, dimatcopy_k_ctTS, dimatcopy_k_rnTS, dimatcopy_k_rtTS,
#endif
#if BUILD_COMPLEX== 1
  cimatcopy_k_cnTS, cimatcopy_k_ctTS, cimatcopy_k_rnTS, cimatcopy_k_rtTS,
  cimatcopy_k_cncTS, cimatcopy_k_ctcTS, cimatcopy_k_rncTS, cimatcopy_k_rtcTS,
#endif
#if BUILD_COMPLEX16==1
  zimatcopy_k_cnTS, zimatcopy_k_ctTS, zimatcopy_k_rnTS, zimatcopy_k_rtTS,
  zimatcopy_k_cncTS, zimatcopy_k_ctcTS, zimatcopy_k_rncTS, zimatcopy_k_rtcTS,
#endif

#if BUILD_SINGLE == 1
  sgeadd_kTS,
#endif
#if BUILD_DOUBLE==1
  dgeadd_kTS,
#endif
#if BUILD_COMPLEX==1
  cgeadd_kTS,
#endif
#if BUILD_COMPLEX16==1
  zgeadd_kTS,
#endif
};

#if (ARCH_ARM64)
static void init_parameter(void) {
#if (BUILD_BFLOAT16)
  TABLE_NAME.sbgemm_p = SBGEMM_DEFAULT_P;
#endif
#if (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE == 1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif

#if (BUILD_BFLOAT16)
  TABLE_NAME.sbgemm_q = SBGEMM_DEFAULT_Q;
#endif
#if BUILD_SINGLE == 1 || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_q = SGEMM_DEFAULT_Q;
#endif
#if BUILD_DOUBLE== 1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_q = DGEMM_DEFAULT_Q;
#endif
#if BUILD_COMPLEX== 1
  TABLE_NAME.cgemm_q = CGEMM_DEFAULT_Q;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_q = ZGEMM_DEFAULT_Q;
#endif

#if (BUILD_BFLOAT16)
  TABLE_NAME.sbgemm_r = SBGEMM_DEFAULT_R;
#endif
#if BUILD_SINGLE == 1 || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_r = SGEMM_DEFAULT_R;
#endif
#if BUILD_DOUBLE==1  || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_r = DGEMM_DEFAULT_R;
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_r = CGEMM_DEFAULT_R;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_r = ZGEMM_DEFAULT_R;
#endif

#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
  TABLE_NAME.qgemm_q = QGEMM_DEFAULT_Q;
  TABLE_NAME.xgemm_q = XGEMM_DEFAULT_Q;
  TABLE_NAME.qgemm_r = QGEMM_DEFAULT_R;
  TABLE_NAME.xgemm_r = XGEMM_DEFAULT_R;
#endif

#if (USE_GEMM3M)
#ifdef CGEMM3M_DEFAULT_P
  TABLE_NAME.cgemm3m_p = CGEMM3M_DEFAULT_P;
#else
  TABLE_NAME.cgemm3m_p = TABLE_NAME.sgemm_p;
#endif

#ifdef ZGEMM3M_DEFAULT_P
  TABLE_NAME.zgemm3m_p = ZGEMM3M_DEFAULT_P;
#else
  TABLE_NAME.zgemm3m_p = TABLE_NAME.dgemm_p;
#endif

#ifdef CGEMM3M_DEFAULT_Q
  TABLE_NAME.cgemm3m_q = CGEMM3M_DEFAULT_Q;
#else
  TABLE_NAME.cgemm3m_q = TABLE_NAME.sgemm_q;
#endif

#ifdef ZGEMM3M_DEFAULT_Q
  TABLE_NAME.zgemm3m_q = ZGEMM3M_DEFAULT_Q;
#else
  TABLE_NAME.zgemm3m_q = TABLE_NAME.dgemm_q;
#endif

#ifdef CGEMM3M_DEFAULT_R
  TABLE_NAME.cgemm3m_r = CGEMM3M_DEFAULT_R;
#else
  TABLE_NAME.cgemm3m_r = TABLE_NAME.sgemm_r;
#endif

#ifdef ZGEMM3M_DEFAULT_R
  TABLE_NAME.zgemm3m_r = ZGEMM3M_DEFAULT_R;
#else
  TABLE_NAME.zgemm3m_r = TABLE_NAME.dgemm_r;
#endif

#ifdef EXPRECISION
  TABLE_NAME.xgemm3m_p = TABLE_NAME.qgemm_p;
  TABLE_NAME.xgemm3m_q = TABLE_NAME.qgemm_q;
  TABLE_NAME.xgemm3m_r = TABLE_NAME.qgemm_r;
#endif
#endif

}
#else // (ARCH_ARM64)
#if defined(ARCH_MIPS64)
static void init_parameter(void) {
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;

  TABLE_NAME.sgemm_q = SGEMM_DEFAULT_Q;
  TABLE_NAME.dgemm_q = DGEMM_DEFAULT_Q;
  TABLE_NAME.cgemm_q = CGEMM_DEFAULT_Q;
  TABLE_NAME.zgemm_q = ZGEMM_DEFAULT_Q;

  TABLE_NAME.sgemm_r = SGEMM_DEFAULT_R;
  TABLE_NAME.dgemm_r = 640;
  TABLE_NAME.cgemm_r = CGEMM_DEFAULT_R;
  TABLE_NAME.zgemm_r = ZGEMM_DEFAULT_R;

#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
  TABLE_NAME.qgemm_q = QGEMM_DEFAULT_Q;
  TABLE_NAME.xgemm_q = XGEMM_DEFAULT_Q;
  TABLE_NAME.qgemm_r = QGEMM_DEFAULT_R;
  TABLE_NAME.xgemm_r = XGEMM_DEFAULT_R;
#endif

#if defined(USE_GEMM3M)
#ifdef CGEMM3M_DEFAULT_P
  TABLE_NAME.cgemm3m_p = CGEMM3M_DEFAULT_P;
#else
  TABLE_NAME.cgemm3m_p = TABLE_NAME.sgemm_p;
#endif

#ifdef ZGEMM3M_DEFAULT_P
  TABLE_NAME.zgemm3m_p = ZGEMM3M_DEFAULT_P;
#else
  TABLE_NAME.zgemm3m_p = TABLE_NAME.dgemm_p;
#endif

#ifdef CGEMM3M_DEFAULT_Q
  TABLE_NAME.cgemm3m_q = CGEMM3M_DEFAULT_Q;
#else
  TABLE_NAME.cgemm3m_q = TABLE_NAME.sgemm_q;
#endif

#ifdef ZGEMM3M_DEFAULT_Q
  TABLE_NAME.zgemm3m_q = ZGEMM3M_DEFAULT_Q;
#else
  TABLE_NAME.zgemm3m_q = TABLE_NAME.dgemm_q;
#endif

#ifdef CGEMM3M_DEFAULT_R
  TABLE_NAME.cgemm3m_r = CGEMM3M_DEFAULT_R;
#else
  TABLE_NAME.cgemm3m_r = TABLE_NAME.sgemm_r;
#endif

#ifdef ZGEMM3M_DEFAULT_R
  TABLE_NAME.zgemm3m_r = ZGEMM3M_DEFAULT_R;
#else
  TABLE_NAME.zgemm3m_r = TABLE_NAME.dgemm_r;
#endif

#ifdef EXPRECISION
  TABLE_NAME.xgemm3m_p = TABLE_NAME.qgemm_p;
  TABLE_NAME.xgemm3m_q = TABLE_NAME.qgemm_q;
  TABLE_NAME.xgemm3m_r = TABLE_NAME.qgemm_r;
#endif
#endif
}
#else // (ARCH_MIPS64)
#if (ARCH_LOONGARCH64)
static int get_L3_size() {
  int ret = 0, id = 0x14;
  __asm__ volatile (
    "cpucfg %[ret], %[id]"
    : [ret]"=r"(ret)
    : [id]"r"(id)
    : "memory"
  );
  return ((ret & 0xffff) + 1) * pow(2, ((ret >> 16) & 0xff)) * pow(2, ((ret >> 24) & 0x7f)) / 1024 / 1024; // MB
}
static void init_parameter(void) {

#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_p = SBGEMM_DEFAULT_P;
#endif

#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_r = SBGEMM_DEFAULT_R;
#endif

#if defined(LA464)
  int L3_size = get_L3_size();
#ifdef SMP
  if(blas_num_threads == 1){
#endif
    //single thread
    if (L3_size == 32){ // 3C5000 and 3D5000
      TABLE_NAME.sgemm_p = 256;
      TABLE_NAME.sgemm_q = 384;
      TABLE_NAME.sgemm_r = 8192;

      TABLE_NAME.dgemm_p = 112;
      TABLE_NAME.dgemm_q = 289;
      TABLE_NAME.dgemm_r = 4096;

      TABLE_NAME.cgemm_p = 128;
      TABLE_NAME.cgemm_q = 256;
      TABLE_NAME.cgemm_r = 4096;

      TABLE_NAME.zgemm_p = 128;
      TABLE_NAME.zgemm_q = 128;
      TABLE_NAME.zgemm_r = 2048;
    } else { // 3A5000 and 3C5000L
      TABLE_NAME.sgemm_p = 256;
      TABLE_NAME.sgemm_q = 384;
      TABLE_NAME.sgemm_r = 4096;

      TABLE_NAME.dgemm_p = 112;
      TABLE_NAME.dgemm_q = 300;
      TABLE_NAME.dgemm_r = 3024;

      TABLE_NAME.cgemm_p = 128;
      TABLE_NAME.cgemm_q = 256;
      TABLE_NAME.cgemm_r = 2048;

      TABLE_NAME.zgemm_p = 128;
      TABLE_NAME.zgemm_q = 128;
      TABLE_NAME.zgemm_r = 1024;
    }
#ifdef SMP
  }else{
    //multi thread
    if (L3_size == 32){ // 3C5000 and 3D5000
      TABLE_NAME.sgemm_p = 256;
      TABLE_NAME.sgemm_q = 384;
      TABLE_NAME.sgemm_r = 1024;

      TABLE_NAME.dgemm_p = 112;
      TABLE_NAME.dgemm_q = 289;
      TABLE_NAME.dgemm_r = 342;

      TABLE_NAME.cgemm_p = 128;
      TABLE_NAME.cgemm_q = 256;
      TABLE_NAME.cgemm_r = 512;

      TABLE_NAME.zgemm_p = 128;
      TABLE_NAME.zgemm_q = 128;
      TABLE_NAME.zgemm_r = 512;
    } else { // 3A5000 and 3C5000L
      TABLE_NAME.sgemm_p = 256;
      TABLE_NAME.sgemm_q = 384;
      TABLE_NAME.sgemm_r = 2048;

      TABLE_NAME.dgemm_p = 112;
      TABLE_NAME.dgemm_q = 300;
      TABLE_NAME.dgemm_r = 738;

      TABLE_NAME.cgemm_p = 128;
      TABLE_NAME.cgemm_q = 256;
      TABLE_NAME.cgemm_r = 1024;

      TABLE_NAME.zgemm_p = 128;
      TABLE_NAME.zgemm_q = 128;
      TABLE_NAME.zgemm_r = 1024;
    }
  }
#endif
#else
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;

  TABLE_NAME.sgemm_q = SGEMM_DEFAULT_Q;
  TABLE_NAME.dgemm_q = DGEMM_DEFAULT_Q;
  TABLE_NAME.cgemm_q = CGEMM_DEFAULT_Q;
  TABLE_NAME.zgemm_q = ZGEMM_DEFAULT_Q;

  TABLE_NAME.sgemm_r = SGEMM_DEFAULT_R;
  TABLE_NAME.dgemm_r = DGEMM_DEFAULT_R;
  TABLE_NAME.cgemm_r = CGEMM_DEFAULT_R;
  TABLE_NAME.zgemm_r = ZGEMM_DEFAULT_R;
#endif

#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_q = SBGEMM_DEFAULT_Q;
#endif
}
#else // (ARCH_LOONGARCH64)
#if (ARCH_POWER)
static void init_parameter(void) {

#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_p = SBGEMM_DEFAULT_P;
#endif
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;

#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_r = SBGEMM_DEFAULT_R;
#endif
  TABLE_NAME.sgemm_r = SGEMM_DEFAULT_R;
  TABLE_NAME.dgemm_r = DGEMM_DEFAULT_R;
  TABLE_NAME.cgemm_r = CGEMM_DEFAULT_R;
  TABLE_NAME.zgemm_r = ZGEMM_DEFAULT_R;


#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_q = SBGEMM_DEFAULT_Q;
#endif
  TABLE_NAME.sgemm_q = SGEMM_DEFAULT_Q;
  TABLE_NAME.dgemm_q = DGEMM_DEFAULT_Q;
  TABLE_NAME.cgemm_q = CGEMM_DEFAULT_Q;
  TABLE_NAME.zgemm_q = ZGEMM_DEFAULT_Q;
}
#else //POWER

#if (ARCH_ZARCH)
static void init_parameter(void) {
#ifdef BUILD_BFLOAT16
	TABLE_NAME.sbgemm_p = SBGEMM_DEFAULT_P;
#endif
	TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
	TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
	TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
	TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;

#ifdef BUILD_BFLOAT16
	TABLE_NAME.sbgemm_r = SBGEMM_DEFAULT_R;
#endif
	TABLE_NAME.sgemm_r = SGEMM_DEFAULT_R;
	TABLE_NAME.dgemm_r = DGEMM_DEFAULT_R;
	TABLE_NAME.cgemm_r = CGEMM_DEFAULT_R;
	TABLE_NAME.zgemm_r = ZGEMM_DEFAULT_R;


#ifdef BUILD_BFLOAT16
	TABLE_NAME.sbgemm_q = SBGEMM_DEFAULT_Q;
#endif
	TABLE_NAME.sgemm_q = SGEMM_DEFAULT_Q;
	TABLE_NAME.dgemm_q = DGEMM_DEFAULT_Q;
	TABLE_NAME.cgemm_q = CGEMM_DEFAULT_Q;
	TABLE_NAME.zgemm_q = ZGEMM_DEFAULT_Q;
}
#else //ZARCH

#if (ARCH_RISCV64)
static void init_parameter(void) {

#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_p = SBGEMM_DEFAULT_P;
#endif
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;

#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_r = SBGEMM_DEFAULT_R;
#endif
  TABLE_NAME.sgemm_r = SGEMM_DEFAULT_R;
  TABLE_NAME.dgemm_r = DGEMM_DEFAULT_R;
  TABLE_NAME.cgemm_r = CGEMM_DEFAULT_R;
  TABLE_NAME.zgemm_r = ZGEMM_DEFAULT_R;


#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_q = SBGEMM_DEFAULT_Q;
#endif
  TABLE_NAME.sgemm_q = SGEMM_DEFAULT_Q;
  TABLE_NAME.dgemm_q = DGEMM_DEFAULT_Q;
  TABLE_NAME.cgemm_q = CGEMM_DEFAULT_Q;
  TABLE_NAME.zgemm_q = ZGEMM_DEFAULT_Q;
}
#else //RISCV64

#ifdef ARCH_X86
static int get_l2_size_old(void){
  int i, eax, ebx, ecx, edx, cpuid_level;
  int info[15];

  cpuid(2, &eax, &ebx, &ecx, &edx);

  info[ 0] = BITMASK(eax,  8, 0xff);
  info[ 1] = BITMASK(eax, 16, 0xff);
  info[ 2] = BITMASK(eax, 24, 0xff);

  info[ 3] = BITMASK(ebx,  0, 0xff);
  info[ 4] = BITMASK(ebx,  8, 0xff);
  info[ 5] = BITMASK(ebx, 16, 0xff);
  info[ 6] = BITMASK(ebx, 24, 0xff);

  info[ 7] = BITMASK(ecx,  0, 0xff);
  info[ 8] = BITMASK(ecx,  8, 0xff);
  info[ 9] = BITMASK(ecx, 16, 0xff);
  info[10] = BITMASK(ecx, 24, 0xff);

  info[11] = BITMASK(edx,  0, 0xff);
  info[12] = BITMASK(edx,  8, 0xff);
  info[13] = BITMASK(edx, 16, 0xff);
  info[14] = BITMASK(edx, 24, 0xff);

  for (i = 0; i < 15; i++){

    switch (info[i]){

      /* This table is from http://www.sandpile.org/ia32/cpuid.htm */

    case 0x1a :
      return 96;

    case 0x39 :
    case 0x3b :
    case 0x41 :
    case 0x79 :
    case 0x81 :
      return 128;

    case 0x3a :
      return 192;

    case 0x21 :
    case 0x3c :
    case 0x42 :
    case 0x7a :
    case 0x7e :
    case 0x82 :
      return 256;

    case 0x3d :
      return 384;

    case 0x3e :
    case 0x43 :
    case 0x7b :
    case 0x7f :
    case 0x83 :
    case 0x86 :
      return 512;

    case 0x44 :
    case 0x78 :
    case 0x7c :
    case 0x84 :
    case 0x87 :
      return 1024;

    case 0x45 :
    case 0x7d :
    case 0x85 :
      return 2048;

    case 0x48 :
      return 3184;

    case 0x49 :
      return 4096;

    case 0x4e :
      return 6144;
    }
  }
//  return 0;
fprintf (stderr,"OpenBLAS WARNING - could not determine the L2 cache size on this system, assuming 256k\n");
return 256;
}
#endif

static __inline__ int get_l2_size(void){

  int eax, ebx, ecx, edx, l2;

  l2 = readenv_atoi("OPENBLAS_L2_SIZE");
  if (l2 != 0)
    return l2;

  cpuid(0x80000006, &eax, &ebx, &ecx, &edx);

  l2 = BITMASK(ecx, 16, 0xffff);

#ifndef ARCH_X86
  if (l2 <= 0) {
     fprintf (stderr,"OpenBLAS WARNING - could not determine the L2 cache size on this system, assuming 256k\n");
     return 256;
  }
  return l2;

#else

  if (l2 > 0) return l2;

  return get_l2_size_old();
#endif
}

static __inline__ int get_l3_size(void){

  int eax, ebx, ecx, edx;

  cpuid(0x80000006, &eax, &ebx, &ecx, &edx);

  return BITMASK(edx, 18, 0x3fff) * 512;
}


static void init_parameter(void) {

  int l2 = get_l2_size();

  (void) l2; /* dirty trick to suppress unused variable warning for targets */
             /* where the GEMM unrolling parameters do not depend on l2 */
  
#ifdef BUILD_BFLOAT16
  TABLE_NAME.sbgemm_p = SBGEMM_DEFAULT_P;
  TABLE_NAME.sbgemm_q = SBGEMM_DEFAULT_Q;
#endif
#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_q = SGEMM_DEFAULT_Q;
#endif
#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16)
  TABLE_NAME.dgemm_q = DGEMM_DEFAULT_Q;
#endif
#if BUILD_COMPLEX == 1
  TABLE_NAME.cgemm_q = CGEMM_DEFAULT_Q;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_q = ZGEMM_DEFAULT_Q;
#endif

#if BUILD_COMPLEX == 1
#ifdef CGEMM3M_DEFAULT_Q
  TABLE_NAME.cgemm3m_q = CGEMM3M_DEFAULT_Q;
#else
  TABLE_NAME.cgemm3m_q = SGEMM_DEFAULT_Q;
#endif
#endif

#if BUILD_COMPLEX16 == 1
#ifdef ZGEMM3M_DEFAULT_Q
  TABLE_NAME.zgemm3m_q = ZGEMM3M_DEFAULT_Q;
#else
  TABLE_NAME.zgemm3m_q = DGEMM_DEFAULT_Q;
#endif
#endif

#ifdef EXPRECISION
  TABLE_NAME.qgemm_q = QGEMM_DEFAULT_Q;
  TABLE_NAME.xgemm_q = XGEMM_DEFAULT_Q;
  TABLE_NAME.xgemm3m_q = QGEMM_DEFAULT_Q;
#endif

#if defined(CORE_KATMAI)  || defined(CORE_COPPERMINE) || defined(CORE_BANIAS) || defined(CORE_YONAH) || defined(CORE_ATHLON)

#ifdef DEBUG
  fprintf(stderr, "Katmai, Coppermine, Banias, Athlon\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p =  64 * (l2 >> 7);
#endif
#if BUILD_DOUBLE == 1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p =  32 * (l2 >> 7);
#endif
#if BUILD_COMPLEX==1 
  TABLE_NAME.cgemm_p =  32 * (l2 >> 7);
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p =  16 * (l2 >> 7);
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  16 * (l2 >> 7);
  TABLE_NAME.xgemm_p =   8 * (l2 >> 7);
#endif
#endif

#ifdef CORE_NORTHWOOD

#ifdef DEBUG
  fprintf(stderr, "Northwood\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p =  96 * (l2 >> 7);
#endif
#if BUILD_DOUBLE == 1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p =  48 * (l2 >> 7);
#endif
#if BUILD_COMPLEX==1 
  TABLE_NAME.cgemm_p =  48 * (l2 >> 7);
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p =  24 * (l2 >> 7);
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  24 * (l2 >> 7);
  TABLE_NAME.xgemm_p =  12 * (l2 >> 7);
#endif
#endif

#ifdef ATOM

#ifdef DEBUG
  fprintf(stderr, "Atom\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = 256;
#endif
#if BUILD_DOUBLE ==1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = 128;
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_p = 128;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p =  64;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  64;
  TABLE_NAME.xgemm_p =  32;
#endif
#endif

#ifdef CORE_PRESCOTT

#ifdef DEBUG
  fprintf(stderr, "Prescott\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p =  56 * (l2 >> 7);
#endif
#if BUILD_DOUBLE ==1  || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p =  28 * (l2 >> 7);
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_p =  28 * (l2 >> 7);
#endif
#if BUILD_COMPLEX16 == 1
  TABLE_NAME.zgemm_p =  14 * (l2 >> 7);
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  14 * (l2 >> 7);
  TABLE_NAME.xgemm_p =   7 * (l2 >> 7);
#endif
#endif

#ifdef CORE2

#ifdef DEBUG
  fprintf(stderr, "Core2\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p =  92 * (l2 >> 9) + 8;
#endif
#if BUILD_DOUBLE==1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p =  46 * (l2 >> 9) + 8;
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_p =  46 * (l2 >> 9) + 4;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p =  23 * (l2 >> 9) + 4;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  92 * (l2 >> 9) + 8;
  TABLE_NAME.xgemm_p =  46 * (l2 >> 9) + 4;
#endif
#endif

#ifdef PENRYN

#ifdef DEBUG
  fprintf(stderr, "Penryn\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p =  42 * (l2 >> 9) + 8;
#endif
#if BUILD_DOUBLE == 1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p =  42 * (l2 >> 9) + 8;
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_p =  21 * (l2 >> 9) + 4;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p =  21 * (l2 >> 9) + 4;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  42 * (l2 >> 9) + 8;
  TABLE_NAME.xgemm_p =  21 * (l2 >> 9) + 4;
#endif
#endif

#ifdef DUNNINGTON

#ifdef DEBUG
  fprintf(stderr, "Dunnington\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p =  42 * (l2 >> 9) + 8;
#endif
#if BUILD_DOUBLE ==1 || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p =  42 * (l2 >> 9) + 8;
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_p =  21 * (l2 >> 9) + 4;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p =  21 * (l2 >> 9) + 4;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  42 * (l2 >> 9) + 8;
  TABLE_NAME.xgemm_p =  21 * (l2 >> 9) + 4;
#endif
#endif


#ifdef NEHALEM

#ifdef DEBUG
  fprintf(stderr, "Nehalem\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#ifdef SANDYBRIDGE

#ifdef DEBUG
  fprintf(stderr, "Sandybridge\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#ifdef HASWELL

#ifdef DEBUG
  fprintf(stderr, "Haswell\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if (BUILD_DOUBLE==1) || (BUILD_COMPLEX16)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#if defined(SKYLAKEX) || defined(COOPERLAKE) || defined(SAPPHIRERAPIDS)

#ifdef DEBUG
  fprintf(stderr, "SkylakeX\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif


#ifdef OPTERON

#ifdef DEBUG
  fprintf(stderr, "Opteron\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = 224 +  56 * (l2 >> 7);
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = 112 +  28 * (l2 >> 7);
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = 112 +  28 * (l2 >> 7);
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p =  56 +  14 * (l2 >> 7);
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p =  56 +  14 * (l2 >> 7);
  TABLE_NAME.xgemm_p =  28 +   7 * (l2 >> 7);
#endif
#endif

#ifdef BARCELONA

#ifdef DEBUG
  fprintf(stderr, "Barcelona\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#ifdef BOBCAT

#ifdef DEBUG
  fprintf(stderr, "Bobcate\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#ifdef BULLDOZER

#ifdef DEBUG
  fprintf(stderr, "Bulldozer\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#ifdef EXCAVATOR

#ifdef DEBUG
  fprintf(stderr, "Excavator\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif


#ifdef PILEDRIVER

#ifdef DEBUG
  fprintf(stderr, "Piledriver\n");
#endif

#if (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#ifdef STEAMROLLER

#ifdef DEBUG
  fprintf(stderr, "Steamroller\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if BUILD_DOUBLE || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif

#ifdef ZEN

#ifdef DEBUG
  fprintf(stderr, "Zen\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if BUILD_COMPLEX16
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif
#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif
#endif


#ifdef NANO

#ifdef DEBUG
  fprintf(stderr, "NANO\n");
#endif

#if  (BUILD_SINGLE==1) || (BUILD_COMPLEX==1)
  TABLE_NAME.sgemm_p = SGEMM_DEFAULT_P;
#endif
#if  (BUILD_DOUBLE==1) || (BUILD_COMPLEX16==1)
  TABLE_NAME.dgemm_p = DGEMM_DEFAULT_P;
#endif
#if (BUILD_COMPLEX==1)
  TABLE_NAME.cgemm_p = CGEMM_DEFAULT_P;
#endif
#if (BUILD_COMPLEX16==1)
  TABLE_NAME.zgemm_p = ZGEMM_DEFAULT_P;
#endif


#ifdef EXPRECISION
  TABLE_NAME.qgemm_p = QGEMM_DEFAULT_P;
  TABLE_NAME.xgemm_p = XGEMM_DEFAULT_P;
#endif

#endif

#ifdef SAPPHIRERAPIDS
#if (BUILD_BFLOAT16 == 1)
  TABLE_NAME.need_amxtile_permission = 1;
#endif
#endif

#if BUILD_COMPLEX==1
#ifdef CGEMM3M_DEFAULT_P
  TABLE_NAME.cgemm3m_p = CGEMM3M_DEFAULT_P;
#else
  TABLE_NAME.cgemm3m_p = TABLE_NAME.sgemm_p;
#endif
#endif

#if BUILD_COMPLEX16==1
#ifdef ZGEMM3M_DEFAULT_P
  TABLE_NAME.zgemm3m_p = ZGEMM3M_DEFAULT_P;
#else
  TABLE_NAME.zgemm3m_p = TABLE_NAME.dgemm_p;
#endif
#endif

#ifdef EXPRECISION
  TABLE_NAME.xgemm3m_p = TABLE_NAME.qgemm_p;
#endif


#if BUILD_SINGLE == 1
  TABLE_NAME.sgemm_p = ((TABLE_NAME.sgemm_p + SGEMM_DEFAULT_UNROLL_M - 1)/SGEMM_DEFAULT_UNROLL_M) * SGEMM_DEFAULT_UNROLL_M;
#endif
#if BUILD_DOUBLE== 1
  TABLE_NAME.dgemm_p = ((TABLE_NAME.dgemm_p + DGEMM_DEFAULT_UNROLL_M - 1)/DGEMM_DEFAULT_UNROLL_M) * DGEMM_DEFAULT_UNROLL_M;
#endif
#if BUILD_COMPLEX==1
  TABLE_NAME.cgemm_p = ((TABLE_NAME.cgemm_p + CGEMM_DEFAULT_UNROLL_M - 1)/CGEMM_DEFAULT_UNROLL_M) * CGEMM_DEFAULT_UNROLL_M;
#endif
#if BUILD_COMPLEX16==1
  TABLE_NAME.zgemm_p = ((TABLE_NAME.zgemm_p + ZGEMM_DEFAULT_UNROLL_M - 1)/ZGEMM_DEFAULT_UNROLL_M) * ZGEMM_DEFAULT_UNROLL_M;
#endif

#if BUILD_COMPLEX==1
#ifdef CGEMM3M_DEFAULT_UNROLL_M
  TABLE_NAME.cgemm3m_p = ((TABLE_NAME.cgemm3m_p + CGEMM3M_DEFAULT_UNROLL_M - 1)/CGEMM3M_DEFAULT_UNROLL_M) * CGEMM3M_DEFAULT_UNROLL_M;
#else
  TABLE_NAME.cgemm3m_p = ((TABLE_NAME.cgemm3m_p + SGEMM_DEFAULT_UNROLL_M - 1)/SGEMM_DEFAULT_UNROLL_M) * SGEMM_DEFAULT_UNROLL_M;
#endif
#endif

#if BUILD_COMPLEX16==1
#ifdef ZGEMM3M_DEFAULT_UNROLL_M
  TABLE_NAME.zgemm3m_p = ((TABLE_NAME.zgemm3m_p + ZGEMM3M_DEFAULT_UNROLL_M - 1)/ZGEMM3M_DEFAULT_UNROLL_M) * ZGEMM3M_DEFAULT_UNROLL_M;
#else
  TABLE_NAME.zgemm3m_p = ((TABLE_NAME.zgemm3m_p + DGEMM_DEFAULT_UNROLL_M - 1)/DGEMM_DEFAULT_UNROLL_M) * DGEMM_DEFAULT_UNROLL_M;
#endif
#endif

#ifdef QUAD_PRECISION
  TABLE_NAME.qgemm_p = ((TABLE_NAME.qgemm_p + QGEMM_DEFAULT_UNROLL_M - 1)/QGEMM_DEFAULT_UNROLL_M) * QGEMM_DEFAULT_UNROLL_M;
  TABLE_NAME.xgemm_p = ((TABLE_NAME.xgemm_p + XGEMM_DEFAULT_UNROLL_M - 1)/XGEMM_DEFAULT_UNROLL_M) * XGEMM_DEFAULT_UNROLL_M;
  TABLE_NAME.xgemm3m_p = ((TABLE_NAME.xgemm3m_p + QGEMM_DEFAULT_UNROLL_M - 1)/QGEMM_DEFAULT_UNROLL_M) * QGEMM_DEFAULT_UNROLL_M;
#endif

#ifdef DEBUG
  fprintf(stderr, "L2 = %8d DGEMM_P  .. %d\n", l2, TABLE_NAME.dgemm_p);
#endif

#if BUILD_BFLOAT16==1
  TABLE_NAME.sbgemm_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.sbgemm_p * TABLE_NAME.sbgemm_q *  4 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.sbgemm_q *  4) - 15) & ~15);
#endif

#if BUILD_SINGLE==1
  TABLE_NAME.sgemm_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.sgemm_p * TABLE_NAME.sgemm_q *  4 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.sgemm_q *  4) - 15) & ~15);
#endif

#if BUILD_DOUBLE==1
  TABLE_NAME.dgemm_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.dgemm_p * TABLE_NAME.dgemm_q *  8 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.dgemm_q *  8) - 15) & ~15);
#endif

#ifdef EXPRECISION
  TABLE_NAME.qgemm_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.qgemm_p * TABLE_NAME.qgemm_q * 16 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.qgemm_q * 16) - 15) & ~15);
#endif

#if BUILD_COMPLEX ==1 
  TABLE_NAME.cgemm_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.cgemm_p * TABLE_NAME.cgemm_q *  8 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.cgemm_q *  8) - 15) & ~15);
#endif

#if BUILD_COMPLEX16 ==1
  TABLE_NAME.zgemm_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.zgemm_p * TABLE_NAME.zgemm_q * 16 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.zgemm_q * 16) - 15) & ~15);
#endif

#if BUILD_COMPLEX == 1
  TABLE_NAME.cgemm3m_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.cgemm3m_p * TABLE_NAME.cgemm3m_q *  8 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.cgemm3m_q *  8) - 15) & ~15);
#endif

#if BUILD_COMPLEX16 == 1
  TABLE_NAME.zgemm3m_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.zgemm3m_p * TABLE_NAME.zgemm3m_q * 16 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
			       ) / (TABLE_NAME.zgemm3m_q * 16) - 15) & ~15);
#endif



#ifdef EXPRECISION
  TABLE_NAME.xgemm_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.xgemm_p * TABLE_NAME.xgemm_q * 32 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
		       ) / (TABLE_NAME.xgemm_q * 32) - 15) & ~15);

  TABLE_NAME.xgemm3m_r = (((BUFFER_SIZE -
			       ((TABLE_NAME.xgemm3m_p * TABLE_NAME.xgemm3m_q * 32 + TABLE_NAME.offsetA
				 + TABLE_NAME.align) & ~TABLE_NAME.align)
		       ) / (TABLE_NAME.xgemm3m_q * 32) - 15) & ~15);

#endif



}
#endif //RISCV64
#endif //POWER
#endif //ZARCH
#endif //(ARCH_LOONGARCH64)
#endif //(ARCH_MIPS64)
#endif //(ARCH_ARM64)
