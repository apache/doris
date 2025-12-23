#ifndef LAPACK_H
#define LAPACK_H

/*
*  Turn on HAVE_LAPACK_CONFIG_H to redefine C-LAPACK datatypes
*/
#ifdef HAVE_LAPACK_CONFIG_H
#include "lapacke_config.h"
#endif

#include "lapacke_mangling.h"

#include <stdlib.h>
#include <stdarg.h>
#include <inttypes.h>

/* It seems all current Fortran compilers put strlen at end.
*  Some historical compilers put strlen after the str argument
*  or make the str argument into a struct. */
#ifndef __EMSCRIPTEN__
#define LAPACK_FORTRAN_STRLEN_END
#endif

#ifndef FORTRAN_STRLEN
  #define FORTRAN_STRLEN size_t
#endif

/* Complex types are structures equivalent to the
* Fortran complex types COMPLEX(4) and COMPLEX(8).
*
* One can also redefine the types with his own types
* for example by including in the code definitions like
*
* #define lapack_complex_float std::complex<float>
* #define lapack_complex_double std::complex<double>
*
* or define these types in the command line:
*
* -Dlapack_complex_float="std::complex<float>"
* -Dlapack_complex_double="std::complex<double>"
*/

#ifndef LAPACK_COMPLEX_CUSTOM

/* Complex type (single precision) */
#ifndef lapack_complex_float
#ifndef __cplusplus
#include <complex.h>
#else
#include <complex>
#endif
#define lapack_complex_float    float _Complex
#endif

#ifndef lapack_complex_float_real
#define lapack_complex_float_real(z)       (creal(z))
#endif

#ifndef lapack_complex_float_imag
#define lapack_complex_float_imag(z)       (cimag(z))
#endif

/* Complex type (double precision) */
#ifndef lapack_complex_double
#ifndef __cplusplus
#include <complex.h>
#else
#include <complex>
#endif
#define lapack_complex_double   double _Complex
#endif

#ifndef lapack_complex_double_real
#define lapack_complex_double_real(z)      (creal(z))
#endif

#ifndef lapack_complex_double_imag
#define lapack_complex_double_imag(z)       (cimag(z))
#endif

#endif /* LAPACK_COMPLEX_CUSTOM */


#ifdef __cplusplus
extern "C" {
#endif

/*----------------------------------------------------------------------------*/
#ifndef lapack_int
#if defined(LAPACK_ILP64)
#define lapack_int        int64_t
#else
#define lapack_int        int32_t
#endif
#endif

/*
 * Integer format string
 */
#ifndef LAPACK_IFMT
#if defined(LAPACK_ILP64)
#define LAPACK_IFMT       PRId64
#else
#define LAPACK_IFMT       PRId32
#endif
#endif

#ifndef lapack_logical
#define lapack_logical    lapack_int
#endif

/* f2c, hence clapack and MacOS Accelerate, returns double instead of float
 * for sdot, slange, clange, etc. */
#if defined(LAPACK_F2C)
    typedef double lapack_float_return;
#else
    typedef float lapack_float_return;
#endif


/* Callback logical functions of one, two, or three arguments are used
*  to select eigenvalues to sort to the top left of the Schur form.
*  The value is selected if function returns TRUE (non-zero). */

typedef lapack_logical (*LAPACK_S_SELECT2) ( const float*, const float* );
typedef lapack_logical (*LAPACK_S_SELECT3)
    ( const float*, const float*, const float* );
typedef lapack_logical (*LAPACK_D_SELECT2) ( const double*, const double* );
typedef lapack_logical (*LAPACK_D_SELECT3)
    ( const double*, const double*, const double* );

typedef lapack_logical (*LAPACK_C_SELECT1) ( const lapack_complex_float* );
typedef lapack_logical (*LAPACK_C_SELECT2)
    ( const lapack_complex_float*, const lapack_complex_float* );
typedef lapack_logical (*LAPACK_Z_SELECT1) ( const lapack_complex_double* );
typedef lapack_logical (*LAPACK_Z_SELECT2)
    ( const lapack_complex_double*, const lapack_complex_double* );

#define LAPACK_lsame_base LAPACK_GLOBAL(lsame,LSAME)
lapack_logical LAPACK_lsame_base( const char* ca,  const char* cb
#ifndef __EMSCRIPTEN__
                            ,  lapack_int lca, lapack_int lcb
#endif
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_lsame(...) LAPACK_lsame_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_lsame(...) LAPACK_lsame_base(__VA_ARGS__)
#endif


/*----------------------------------------------------------------------------*/
/* This is in alphabetical order (ignoring leading precision). */

#define LAPACK_cbbcsd_base LAPACK_GLOBAL(cbbcsd,CBBCSD)
void LAPACK_cbbcsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    float* theta,
    float* phi,
    lapack_complex_float* U1, lapack_int const* ldu1,
    lapack_complex_float* U2, lapack_int const* ldu2,
    lapack_complex_float* V1T, lapack_int const* ldv1t,
    lapack_complex_float* V2T, lapack_int const* ldv2t,
    float* B11D,
    float* B11E,
    float* B12D,
    float* B12E,
    float* B21D,
    float* B21E,
    float* B22D,
    float* B22E,
    float* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cbbcsd(...) LAPACK_cbbcsd_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_cbbcsd(...) LAPACK_cbbcsd_base(__VA_ARGS__)
#endif

#define LAPACK_dbbcsd_base LAPACK_GLOBAL(dbbcsd,DBBCSD)
void LAPACK_dbbcsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    double* theta,
    double* phi,
    double* U1, lapack_int const* ldu1,
    double* U2, lapack_int const* ldu2,
    double* V1T, lapack_int const* ldv1t,
    double* V2T, lapack_int const* ldv2t,
    double* B11D,
    double* B11E,
    double* B12D,
    double* B12E,
    double* b21d,
    double* b21e,
    double* b22d,
    double* b22e,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dbbcsd(...) LAPACK_dbbcsd_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_dbbcsd(...) LAPACK_dbbcsd_base(__VA_ARGS__)
#endif

#define LAPACK_sbbcsd_base LAPACK_GLOBAL(sbbcsd,SBBCSD)
void LAPACK_sbbcsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    float* theta,
    float* phi,
    float* U1, lapack_int const* ldu1,
    float* U2, lapack_int const* ldu2,
    float* V1T, lapack_int const* ldv1t,
    float* V2T, lapack_int const* ldv2t,
    float* B11D,
    float* B11E,
    float* B12D,
    float* B12E,
    float* B21D,
    float* B21E,
    float* B22D,
    float* B22E,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sbbcsd(...) LAPACK_sbbcsd_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_sbbcsd(...) LAPACK_sbbcsd_base(__VA_ARGS__)
#endif

#define LAPACK_zbbcsd_base LAPACK_GLOBAL(zbbcsd,ZBBCSD)
void LAPACK_zbbcsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    double* theta,
    double* phi,
    lapack_complex_double* U1, lapack_int const* ldu1,
    lapack_complex_double* U2, lapack_int const* ldu2,
    lapack_complex_double* V1T, lapack_int const* ldv1t,
    lapack_complex_double* V2T, lapack_int const* ldv2t,
    double* B11D,
    double* B11E,
    double* B12D,
    double* B12E,
    double* B21D,
    double* B21E,
    double* B22D,
    double* B22E,
    double* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zbbcsd(...) LAPACK_zbbcsd_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_zbbcsd(...) LAPACK_zbbcsd_base(__VA_ARGS__)
#endif

#define LAPACK_dbdsdc_base LAPACK_GLOBAL(dbdsdc,DBDSDC)
void LAPACK_dbdsdc_base(
    char const* uplo, char const* compq,
    lapack_int const* n,
    double* D,
    double* E,
    double* U, lapack_int const* ldu,
    double* VT, lapack_int const* ldvt,
    double* Q, lapack_int* IQ,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dbdsdc(...) LAPACK_dbdsdc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dbdsdc(...) LAPACK_dbdsdc_base(__VA_ARGS__)
#endif

#define LAPACK_sbdsdc_base LAPACK_GLOBAL(sbdsdc,SBDSDC)
void LAPACK_sbdsdc_base(
    char const* uplo, char const* compq,
    lapack_int const* n,
    float* D,
    float* E,
    float* U, lapack_int const* ldu,
    float* VT, lapack_int const* ldvt,
    float* Q, lapack_int* IQ,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sbdsdc(...) LAPACK_sbdsdc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sbdsdc(...) LAPACK_sbdsdc_base(__VA_ARGS__)
#endif

#define LAPACK_cbdsqr_base LAPACK_GLOBAL(cbdsqr,CBDSQR)
void LAPACK_cbdsqr_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* ncvt, lapack_int const* nru, lapack_int const* ncc,
    float* D,
    float* E,
    lapack_complex_float* VT, lapack_int const* ldvt,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* C, lapack_int const* ldc,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cbdsqr(...) LAPACK_cbdsqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cbdsqr(...) LAPACK_cbdsqr_base(__VA_ARGS__)
#endif

#define LAPACK_dbdsqr_base LAPACK_GLOBAL(dbdsqr,DBDSQR)
void LAPACK_dbdsqr_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* ncvt, lapack_int const* nru, lapack_int const* ncc,
    double* D,
    double* E,
    double* VT, lapack_int const* ldvt,
    double* U, lapack_int const* ldu,
    double* C, lapack_int const* ldc,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dbdsqr(...) LAPACK_dbdsqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dbdsqr(...) LAPACK_dbdsqr_base(__VA_ARGS__)
#endif

#define LAPACK_sbdsqr_base LAPACK_GLOBAL(sbdsqr,SBDSQR)
void LAPACK_sbdsqr_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* ncvt, lapack_int const* nru, lapack_int const* ncc,
    float* D,
    float* E,
    float* VT, lapack_int const* ldvt,
    float* U, lapack_int const* ldu,
    float* C, lapack_int const* ldc,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sbdsqr(...) LAPACK_sbdsqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sbdsqr(...) LAPACK_sbdsqr_base(__VA_ARGS__)
#endif

#define LAPACK_zbdsqr_base LAPACK_GLOBAL(zbdsqr,ZBDSQR)
void LAPACK_zbdsqr_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* ncvt, lapack_int const* nru, lapack_int const* ncc,
    double* D,
    double* E,
    lapack_complex_double* VT, lapack_int const* ldvt,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* C, lapack_int const* ldc,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zbdsqr(...) LAPACK_zbdsqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zbdsqr(...) LAPACK_zbdsqr_base(__VA_ARGS__)
#endif

#define LAPACK_dbdsvdx_base LAPACK_GLOBAL(dbdsvdx,DBDSVDX)
void LAPACK_dbdsvdx_base(
    char const* uplo, char const* jobz, char const* range,
    lapack_int const* n,
    double const* D,
    double const* E,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* ns,
    double* S,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dbdsvdx(...) LAPACK_dbdsvdx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dbdsvdx(...) LAPACK_dbdsvdx_base(__VA_ARGS__)
#endif

#define LAPACK_sbdsvdx_base LAPACK_GLOBAL(sbdsvdx,SBDSVDX)
void LAPACK_sbdsvdx_base(
    char const* uplo, char const* jobz, char const* range,
    lapack_int const* n,
    float const* D,
    float const* E,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* ns,
    float* S,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sbdsvdx(...) LAPACK_sbdsvdx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sbdsvdx(...) LAPACK_sbdsvdx_base(__VA_ARGS__)
#endif

#define LAPACK_ddisna_base LAPACK_GLOBAL(ddisna,DDISNA)
void LAPACK_ddisna_base(
    char const* job,
    lapack_int const* m, lapack_int const* n,
    double const* D,
    double* SEP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ddisna(...) LAPACK_ddisna_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ddisna(...) LAPACK_ddisna_base(__VA_ARGS__)
#endif

#define LAPACK_sdisna_base LAPACK_GLOBAL(sdisna,SDISNA)
void LAPACK_sdisna_base(
    char const* job,
    lapack_int const* m, lapack_int const* n,
    float const* D,
    float* SEP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sdisna(...) LAPACK_sdisna_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sdisna(...) LAPACK_sdisna_base(__VA_ARGS__)
#endif

#define LAPACK_cgbbrd_base LAPACK_GLOBAL(cgbbrd,CGBBRD)
void LAPACK_cgbbrd_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* ncc, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_float* AB, lapack_int const* ldab,
    float* D,
    float* E,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* PT, lapack_int const* ldpt,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgbbrd(...) LAPACK_cgbbrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgbbrd(...) LAPACK_cgbbrd_base(__VA_ARGS__)
#endif

#define LAPACK_dgbbrd_base LAPACK_GLOBAL(dgbbrd,DGBBRD)
void LAPACK_dgbbrd_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* ncc, lapack_int const* kl, lapack_int const* ku,
    double* AB, lapack_int const* ldab,
    double* D,
    double* E,
    double* Q, lapack_int const* ldq,
    double* PT, lapack_int const* ldpt,
    double* C, lapack_int const* ldc,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgbbrd(...) LAPACK_dgbbrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgbbrd(...) LAPACK_dgbbrd_base(__VA_ARGS__)
#endif

#define LAPACK_sgbbrd_base LAPACK_GLOBAL(sgbbrd,SGBBRD)
void LAPACK_sgbbrd_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* ncc, lapack_int const* kl, lapack_int const* ku,
    float* AB, lapack_int const* ldab,
    float* D,
    float* E,
    float* Q, lapack_int const* ldq,
    float* PT, lapack_int const* ldpt,
    float* C, lapack_int const* ldc,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgbbrd(...) LAPACK_sgbbrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgbbrd(...) LAPACK_sgbbrd_base(__VA_ARGS__)
#endif

#define LAPACK_zgbbrd_base LAPACK_GLOBAL(zgbbrd,ZGBBRD)
void LAPACK_zgbbrd_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* ncc, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_double* AB, lapack_int const* ldab,
    double* D,
    double* E,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* PT, lapack_int const* ldpt,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgbbrd(...) LAPACK_zgbbrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgbbrd(...) LAPACK_zgbbrd_base(__VA_ARGS__)
#endif

#define LAPACK_cgbcon_base LAPACK_GLOBAL(cgbcon,CGBCON)
void LAPACK_cgbcon_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_float const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgbcon(...) LAPACK_cgbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgbcon(...) LAPACK_cgbcon_base(__VA_ARGS__)
#endif

#define LAPACK_dgbcon_base LAPACK_GLOBAL(dgbcon,DGBCON)
void LAPACK_dgbcon_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    double const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgbcon(...) LAPACK_dgbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgbcon(...) LAPACK_dgbcon_base(__VA_ARGS__)
#endif

#define LAPACK_sgbcon_base LAPACK_GLOBAL(sgbcon,SGBCON)
void LAPACK_sgbcon_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    float const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgbcon(...) LAPACK_sgbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgbcon(...) LAPACK_sgbcon_base(__VA_ARGS__)
#endif

#define LAPACK_zgbcon_base LAPACK_GLOBAL(zgbcon,ZGBCON)
void LAPACK_zgbcon_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_double const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgbcon(...) LAPACK_zgbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgbcon(...) LAPACK_zgbcon_base(__VA_ARGS__)
#endif

#define LAPACK_cgbequ LAPACK_GLOBAL(cgbequ,CGBEQU)
void LAPACK_cgbequ(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_dgbequ LAPACK_GLOBAL(dgbequ,DGBEQU)
void LAPACK_dgbequ(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    double const* AB, lapack_int const* ldab,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_sgbequ LAPACK_GLOBAL(sgbequ,SGBEQU)
void LAPACK_sgbequ(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    float const* AB, lapack_int const* ldab,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_zgbequ LAPACK_GLOBAL(zgbequ,ZGBEQU)
void LAPACK_zgbequ(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_cgbequb LAPACK_GLOBAL(cgbequb,CGBEQUB)
void LAPACK_cgbequb(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_dgbequb LAPACK_GLOBAL(dgbequb,DGBEQUB)
void LAPACK_dgbequb(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    double const* AB, lapack_int const* ldab,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_sgbequb LAPACK_GLOBAL(sgbequb,SGBEQUB)
void LAPACK_sgbequb(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    float const* AB, lapack_int const* ldab,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_zgbequb LAPACK_GLOBAL(zgbequb,ZGBEQUB)
void LAPACK_zgbequb(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_cgbrfs_base LAPACK_GLOBAL(cgbrfs,CGBRFS)
void LAPACK_cgbrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_float const* AB, lapack_int const* ldab,
    lapack_complex_float const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgbrfs(...) LAPACK_cgbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgbrfs(...) LAPACK_cgbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_dgbrfs_base LAPACK_GLOBAL(dgbrfs,DGBRFS)
void LAPACK_dgbrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    double const* AB, lapack_int const* ldab,
    double const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgbrfs(...) LAPACK_dgbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgbrfs(...) LAPACK_dgbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_sgbrfs_base LAPACK_GLOBAL(sgbrfs,SGBRFS)
void LAPACK_sgbrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    float const* AB, lapack_int const* ldab,
    float const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgbrfs(...) LAPACK_sgbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgbrfs(...) LAPACK_sgbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_zgbrfs_base LAPACK_GLOBAL(zgbrfs,ZGBRFS)
void LAPACK_zgbrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_double const* AB, lapack_int const* ldab,
    lapack_complex_double const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgbrfs(...) LAPACK_zgbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgbrfs(...) LAPACK_zgbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_cgbrfsx_base LAPACK_GLOBAL(cgbrfsx,CGBRFSX)
void LAPACK_cgbrfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_float const* AB, lapack_int const* ldab,
    lapack_complex_float const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    const float* R,
    const float* C,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgbrfsx(...) LAPACK_cgbrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgbrfsx(...) LAPACK_cgbrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_dgbrfsx_base LAPACK_GLOBAL(dgbrfsx,DGBRFSX)
void LAPACK_dgbrfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    double const* AB, lapack_int const* ldab,
    double const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    const double* R,
    const double* C,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgbrfsx(...) LAPACK_dgbrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgbrfsx(...) LAPACK_dgbrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_sgbrfsx_base LAPACK_GLOBAL(sgbrfsx,SGBRFSX)
void LAPACK_sgbrfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    float const* AB, lapack_int const* ldab,
    float const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    const float* R,
    const float* C,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgbrfsx(...) LAPACK_sgbrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgbrfsx(...) LAPACK_sgbrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_zgbrfsx_base LAPACK_GLOBAL(zgbrfsx,ZGBRFSX)
void LAPACK_zgbrfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_double const* AB, lapack_int const* ldab,
    lapack_complex_double const* AFB, lapack_int const* ldafb, lapack_int const* ipiv,
    const double* R,
    const double* C,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgbrfsx(...) LAPACK_zgbrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgbrfsx(...) LAPACK_zgbrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_cgbsv LAPACK_GLOBAL(cgbsv,CGBSV)
void LAPACK_cgbsv(
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_float* AB, lapack_int const* ldab, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_dgbsv LAPACK_GLOBAL(dgbsv,DGBSV)
void LAPACK_dgbsv(
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    double* AB, lapack_int const* ldab, lapack_int* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_sgbsv LAPACK_GLOBAL(sgbsv,SGBSV)
void LAPACK_sgbsv(
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    float* AB, lapack_int const* ldab, lapack_int* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_zgbsv LAPACK_GLOBAL(zgbsv,ZGBSV)
void LAPACK_zgbsv(
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_double* AB, lapack_int const* ldab, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_cgbsvx_base LAPACK_GLOBAL(cgbsvx,CGBSVX)
void LAPACK_cgbsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgbsvx(...) LAPACK_cgbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgbsvx(...) LAPACK_cgbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_dgbsvx_base LAPACK_GLOBAL(dgbsvx,DGBSVX)
void LAPACK_dgbsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    double* AB, lapack_int const* ldab,
    double* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgbsvx(...) LAPACK_dgbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgbsvx(...) LAPACK_dgbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_sgbsvx_base LAPACK_GLOBAL(sgbsvx,SGBSVX)
void LAPACK_sgbsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    float* AB, lapack_int const* ldab,
    float* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgbsvx(...) LAPACK_sgbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgbsvx(...) LAPACK_sgbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_zgbsvx_base LAPACK_GLOBAL(zgbsvx,ZGBSVX)
void LAPACK_zgbsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgbsvx(...) LAPACK_zgbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgbsvx(...) LAPACK_zgbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_cgbsvxx_base LAPACK_GLOBAL(cgbsvxx,CGBSVXX)
void LAPACK_cgbsvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgbsvxx(...) LAPACK_cgbsvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgbsvxx(...) LAPACK_cgbsvxx_base(__VA_ARGS__)
#endif

#define LAPACK_dgbsvxx_base LAPACK_GLOBAL(dgbsvxx,DGBSVXX)
void LAPACK_dgbsvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    double* AB, lapack_int const* ldab,
    double* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgbsvxx(...) LAPACK_dgbsvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgbsvxx(...) LAPACK_dgbsvxx_base(__VA_ARGS__)
#endif

#define LAPACK_sgbsvxx_base LAPACK_GLOBAL(sgbsvxx,SGBSVXX)
void LAPACK_sgbsvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    float* AB, lapack_int const* ldab,
    float* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgbsvxx(...) LAPACK_sgbsvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgbsvxx(...) LAPACK_sgbsvxx_base(__VA_ARGS__)
#endif

#define LAPACK_zgbsvxx_base LAPACK_GLOBAL(zgbsvxx,ZGBSVXX)
void LAPACK_zgbsvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* AFB, lapack_int const* ldafb, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgbsvxx(...) LAPACK_zgbsvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgbsvxx(...) LAPACK_zgbsvxx_base(__VA_ARGS__)
#endif

#define LAPACK_cgbtrf LAPACK_GLOBAL(cgbtrf,CGBTRF)
void LAPACK_cgbtrf(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_float* AB, lapack_int const* ldab, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_dgbtrf LAPACK_GLOBAL(dgbtrf,DGBTRF)
void LAPACK_dgbtrf(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    double* AB, lapack_int const* ldab, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_sgbtrf LAPACK_GLOBAL(sgbtrf,SGBTRF)
void LAPACK_sgbtrf(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    float* AB, lapack_int const* ldab, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_zgbtrf LAPACK_GLOBAL(zgbtrf,ZGBTRF)
void LAPACK_zgbtrf(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_double* AB, lapack_int const* ldab, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_cgbtrs_base LAPACK_GLOBAL(cgbtrs,CGBTRS)
void LAPACK_cgbtrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_float const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgbtrs(...) LAPACK_cgbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgbtrs(...) LAPACK_cgbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_dgbtrs_base LAPACK_GLOBAL(dgbtrs,DGBTRS)
void LAPACK_dgbtrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    double const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgbtrs(...) LAPACK_dgbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgbtrs(...) LAPACK_dgbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_sgbtrs_base LAPACK_GLOBAL(sgbtrs,SGBTRS)
void LAPACK_sgbtrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    float const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgbtrs(...) LAPACK_sgbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgbtrs(...) LAPACK_sgbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_zgbtrs_base LAPACK_GLOBAL(zgbtrs,ZGBTRS)
void LAPACK_zgbtrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku, lapack_int const* nrhs,
    lapack_complex_double const* AB, lapack_int const* ldab, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgbtrs(...) LAPACK_zgbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgbtrs(...) LAPACK_zgbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_cgebak_base LAPACK_GLOBAL(cgebak,CGEBAK)
void LAPACK_cgebak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float const* scale, lapack_int const* m,
    lapack_complex_float* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgebak(...) LAPACK_cgebak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgebak(...) LAPACK_cgebak_base(__VA_ARGS__)
#endif

#define LAPACK_dgebak_base LAPACK_GLOBAL(dgebak,DGEBAK)
void LAPACK_dgebak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double const* scale, lapack_int const* m,
    double* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgebak(...) LAPACK_dgebak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgebak(...) LAPACK_dgebak_base(__VA_ARGS__)
#endif

#define LAPACK_sgebak_base LAPACK_GLOBAL(sgebak,SGEBAK)
void LAPACK_sgebak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float const* scale, lapack_int const* m,
    float* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgebak(...) LAPACK_sgebak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgebak(...) LAPACK_sgebak_base(__VA_ARGS__)
#endif

#define LAPACK_zgebak_base LAPACK_GLOBAL(zgebak,ZGEBAK)
void LAPACK_zgebak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double const* scale, lapack_int const* m,
    lapack_complex_double* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgebak(...) LAPACK_zgebak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgebak(...) LAPACK_zgebak_base(__VA_ARGS__)
#endif

#define LAPACK_cgebal_base LAPACK_GLOBAL(cgebal,CGEBAL)
void LAPACK_cgebal_base(
    char const* job,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ilo, lapack_int* ihi,
    float* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgebal(...) LAPACK_cgebal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgebal(...) LAPACK_cgebal_base(__VA_ARGS__)
#endif

#define LAPACK_dgebal_base LAPACK_GLOBAL(dgebal,DGEBAL)
void LAPACK_dgebal_base(
    char const* job,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* ilo, lapack_int* ihi,
    double* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgebal(...) LAPACK_dgebal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgebal(...) LAPACK_dgebal_base(__VA_ARGS__)
#endif

#define LAPACK_sgebal_base LAPACK_GLOBAL(sgebal,SGEBAL)
void LAPACK_sgebal_base(
    char const* job,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* ilo, lapack_int* ihi,
    float* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgebal(...) LAPACK_sgebal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgebal(...) LAPACK_sgebal_base(__VA_ARGS__)
#endif

#define LAPACK_zgebal_base LAPACK_GLOBAL(zgebal,ZGEBAL)
void LAPACK_zgebal_base(
    char const* job,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ilo, lapack_int* ihi,
    double* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgebal(...) LAPACK_zgebal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgebal(...) LAPACK_zgebal_base(__VA_ARGS__)
#endif

#define LAPACK_cgebrd LAPACK_GLOBAL(cgebrd,CGEBRD)
void LAPACK_cgebrd(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* D,
    float* E,
    lapack_complex_float* tauq,
    lapack_complex_float* taup,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgebrd LAPACK_GLOBAL(dgebrd,DGEBRD)
void LAPACK_dgebrd(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* D,
    double* E,
    double* tauq,
    double* taup,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgebrd LAPACK_GLOBAL(sgebrd,SGEBRD)
void LAPACK_sgebrd(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* D,
    float* E,
    float* tauq,
    float* taup,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgebrd LAPACK_GLOBAL(zgebrd,ZGEBRD)
void LAPACK_zgebrd(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* D,
    double* E,
    lapack_complex_double* tauq,
    lapack_complex_double* taup,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgecon_base LAPACK_GLOBAL(cgecon,CGECON)
void LAPACK_cgecon_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgecon(...) LAPACK_cgecon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgecon(...) LAPACK_cgecon_base(__VA_ARGS__)
#endif

#define LAPACK_dgecon_base LAPACK_GLOBAL(dgecon,DGECON)
void LAPACK_dgecon_base(
    char const* norm,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgecon(...) LAPACK_dgecon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgecon(...) LAPACK_dgecon_base(__VA_ARGS__)
#endif

#define LAPACK_sgecon_base LAPACK_GLOBAL(sgecon,SGECON)
void LAPACK_sgecon_base(
    char const* norm,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgecon(...) LAPACK_sgecon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgecon(...) LAPACK_sgecon_base(__VA_ARGS__)
#endif

#define LAPACK_zgecon_base LAPACK_GLOBAL(zgecon,ZGECON)
void LAPACK_zgecon_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgecon(...) LAPACK_zgecon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgecon(...) LAPACK_zgecon_base(__VA_ARGS__)
#endif

#define LAPACK_cgeequ LAPACK_GLOBAL(cgeequ,CGEEQU)
void LAPACK_cgeequ(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_dgeequ LAPACK_GLOBAL(dgeequ,DGEEQU)
void LAPACK_dgeequ(
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_sgeequ LAPACK_GLOBAL(sgeequ,SGEEQU)
void LAPACK_sgeequ(
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_zgeequ LAPACK_GLOBAL(zgeequ,ZGEEQU)
void LAPACK_zgeequ(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_cgeequb LAPACK_GLOBAL(cgeequb,CGEEQUB)
void LAPACK_cgeequb(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_dgeequb LAPACK_GLOBAL(dgeequb,DGEEQUB)
void LAPACK_dgeequb(
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_sgeequb LAPACK_GLOBAL(sgeequb,SGEEQUB)
void LAPACK_sgeequb(
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* R,
    float* C,
    float* rowcnd,
    float* colcnd,
    float* amax,
    lapack_int* info );

#define LAPACK_zgeequb LAPACK_GLOBAL(zgeequb,ZGEEQUB)
void LAPACK_zgeequb(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* R,
    double* C,
    double* rowcnd,
    double* colcnd,
    double* amax,
    lapack_int* info );

#define LAPACK_cgees_base LAPACK_GLOBAL(cgees,CGEES)
void LAPACK_cgees_base(
    char const* jobvs, char const* sort, LAPACK_C_SELECT1 select,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* sdim,
    lapack_complex_float* W,
    lapack_complex_float* VS, lapack_int const* ldvs,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgees(...) LAPACK_cgees_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgees(...) LAPACK_cgees_base(__VA_ARGS__)
#endif

#define LAPACK_dgees_base LAPACK_GLOBAL(dgees,DGEES)
void LAPACK_dgees_base(
    char const* jobvs, char const* sort, LAPACK_D_SELECT2 select,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* sdim,
    double* WR,
    double* WI,
    double* VS, lapack_int const* ldvs,
    double* work, lapack_int const* lwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgees(...) LAPACK_dgees_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgees(...) LAPACK_dgees_base(__VA_ARGS__)
#endif

#define LAPACK_sgees_base LAPACK_GLOBAL(sgees,SGEES)
void LAPACK_sgees_base(
    char const* jobvs, char const* sort, LAPACK_S_SELECT2 select,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* sdim,
    float* WR,
    float* WI,
    float* VS, lapack_int const* ldvs,
    float* work, lapack_int const* lwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgees(...) LAPACK_sgees_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgees(...) LAPACK_sgees_base(__VA_ARGS__)
#endif

#define LAPACK_zgees_base LAPACK_GLOBAL(zgees,ZGEES)
void LAPACK_zgees_base(
    char const* jobvs, char const* sort, LAPACK_Z_SELECT1 select,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* sdim,
    lapack_complex_double* W,
    lapack_complex_double* VS, lapack_int const* ldvs,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgees(...) LAPACK_zgees_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgees(...) LAPACK_zgees_base(__VA_ARGS__)
#endif

#define LAPACK_cgeesx_base LAPACK_GLOBAL(cgeesx,CGEESX)
void LAPACK_cgeesx_base(
    char const* jobvs, char const* sort, LAPACK_C_SELECT1 select, char const* sense,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* sdim,
    lapack_complex_float* W,
    lapack_complex_float* VS, lapack_int const* ldvs,
    float* rconde,
    float* rcondv,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgeesx(...) LAPACK_cgeesx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgeesx(...) LAPACK_cgeesx_base(__VA_ARGS__)
#endif

#define LAPACK_dgeesx_base LAPACK_GLOBAL(dgeesx,DGEESX)
void LAPACK_dgeesx_base(
    char const* jobvs, char const* sort, LAPACK_D_SELECT2 select, char const* sense,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* sdim,
    double* WR,
    double* WI,
    double* VS, lapack_int const* ldvs,
    double* rconde,
    double* rcondv,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgeesx(...) LAPACK_dgeesx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgeesx(...) LAPACK_dgeesx_base(__VA_ARGS__)
#endif

#define LAPACK_sgeesx_base LAPACK_GLOBAL(sgeesx,SGEESX)
void LAPACK_sgeesx_base(
    char const* jobvs, char const* sort, LAPACK_S_SELECT2 select, char const* sense,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* sdim,
    float* WR,
    float* WI,
    float* VS, lapack_int const* ldvs,
    float* rconde,
    float* rcondv,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgeesx(...) LAPACK_sgeesx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgeesx(...) LAPACK_sgeesx_base(__VA_ARGS__)
#endif

#define LAPACK_zgeesx_base LAPACK_GLOBAL(zgeesx,ZGEESX)
void LAPACK_zgeesx_base(
    char const* jobvs, char const* sort, LAPACK_Z_SELECT1 select, char const* sense,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* sdim,
    lapack_complex_double* W,
    lapack_complex_double* VS, lapack_int const* ldvs,
    double* rconde,
    double* rcondv,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgeesx(...) LAPACK_zgeesx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgeesx(...) LAPACK_zgeesx_base(__VA_ARGS__)
#endif

#define LAPACK_cgeev_base LAPACK_GLOBAL(cgeev,CGEEV)
void LAPACK_cgeev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* W,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgeev(...) LAPACK_cgeev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgeev(...) LAPACK_cgeev_base(__VA_ARGS__)
#endif

#define LAPACK_dgeev_base LAPACK_GLOBAL(dgeev,DGEEV)
void LAPACK_dgeev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* WR,
    double* WI,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgeev(...) LAPACK_dgeev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgeev(...) LAPACK_dgeev_base(__VA_ARGS__)
#endif

#define LAPACK_sgeev_base LAPACK_GLOBAL(sgeev,SGEEV)
void LAPACK_sgeev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* WR,
    float* WI,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgeev(...) LAPACK_sgeev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgeev(...) LAPACK_sgeev_base(__VA_ARGS__)
#endif

#define LAPACK_zgeev_base LAPACK_GLOBAL(zgeev,ZGEEV)
void LAPACK_zgeev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* W,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgeev(...) LAPACK_zgeev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgeev(...) LAPACK_zgeev_base(__VA_ARGS__)
#endif

#define LAPACK_cgeevx_base LAPACK_GLOBAL(cgeevx,CGEEVX)
void LAPACK_cgeevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* W,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    float* scale,
    float* abnrm,
    float* rconde,
    float* rcondv,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgeevx(...) LAPACK_cgeevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_cgeevx(...) LAPACK_cgeevx_base(__VA_ARGS__)
#endif

#define LAPACK_dgeevx_base LAPACK_GLOBAL(dgeevx,DGEEVX)
void LAPACK_dgeevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* WR,
    double* WI,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    double* scale,
    double* abnrm,
    double* rconde,
    double* rcondv,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgeevx(...) LAPACK_dgeevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_dgeevx(...) LAPACK_dgeevx_base(__VA_ARGS__)
#endif

#define LAPACK_sgeevx_base LAPACK_GLOBAL(sgeevx,SGEEVX)
void LAPACK_sgeevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* WR,
    float* WI,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    float* scale,
    float* abnrm,
    float* rconde,
    float* rcondv,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgeevx(...) LAPACK_sgeevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_sgeevx(...) LAPACK_sgeevx_base(__VA_ARGS__)
#endif

#define LAPACK_zgeevx_base LAPACK_GLOBAL(zgeevx,ZGEEVX)
void LAPACK_zgeevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* W,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    double* scale,
    double* abnrm,
    double* rconde,
    double* rcondv,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgeevx(...) LAPACK_zgeevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_zgeevx(...) LAPACK_zgeevx_base(__VA_ARGS__)
#endif

#define LAPACK_cgehrd LAPACK_GLOBAL(cgehrd,CGEHRD)
void LAPACK_cgehrd(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgehrd LAPACK_GLOBAL(dgehrd,DGEHRD)
void LAPACK_dgehrd(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double* A, lapack_int const* lda,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgehrd LAPACK_GLOBAL(sgehrd,SGEHRD)
void LAPACK_sgehrd(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float* A, lapack_int const* lda,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgehrd LAPACK_GLOBAL(zgehrd,ZGEHRD)
void LAPACK_zgehrd(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgejsv_base LAPACK_GLOBAL(cgejsv,CGEJSV)
void LAPACK_cgejsv_base(
    char const* joba, char const* jobu, char const* jobv, char const* jobr, char const* jobt, char const* jobp,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* SVA,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* V, lapack_int const* ldv,
    lapack_complex_float* cwork, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgejsv(...) LAPACK_cgejsv_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_cgejsv(...) LAPACK_cgejsv_base(__VA_ARGS__)
#endif

#define LAPACK_dgejsv_base LAPACK_GLOBAL(dgejsv,DGEJSV)
void LAPACK_dgejsv_base(
    char const* joba, char const* jobu, char const* jobv, char const* jobr, char const* jobt, char const* jobp,
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* SVA,
    double* U, lapack_int const* ldu,
    double* V, lapack_int const* ldv,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgejsv(...) LAPACK_dgejsv_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_dgejsv(...) LAPACK_dgejsv_base(__VA_ARGS__)
#endif

#define LAPACK_sgejsv_base LAPACK_GLOBAL(sgejsv,SGEJSV)
void LAPACK_sgejsv_base(
    char const* joba, char const* jobu, char const* jobv, char const* jobr, char const* jobt, char const* jobp,
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* SVA,
    float* U, lapack_int const* ldu,
    float* V, lapack_int const* ldv,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgejsv(...) LAPACK_sgejsv_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_sgejsv(...) LAPACK_sgejsv_base(__VA_ARGS__)
#endif

#define LAPACK_zgejsv_base LAPACK_GLOBAL(zgejsv,ZGEJSV)
void LAPACK_zgejsv_base(
    char const* joba, char const* jobu, char const* jobv, char const* jobr, char const* jobt, char const* jobp,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* SVA,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* V, lapack_int const* ldv,
    lapack_complex_double* cwork, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgejsv(...) LAPACK_zgejsv_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_zgejsv(...) LAPACK_zgejsv_base(__VA_ARGS__)
#endif

#define LAPACK_cgelq LAPACK_GLOBAL(cgelq,CGELQ)
void LAPACK_cgelq(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* T, lapack_int const* tsize,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgelq LAPACK_GLOBAL(dgelq,DGELQ)
void LAPACK_dgelq(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* T, lapack_int const* tsize,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgelq LAPACK_GLOBAL(sgelq,SGELQ)
void LAPACK_sgelq(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* T, lapack_int const* tsize,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgelq LAPACK_GLOBAL(zgelq,ZGELQ)
void LAPACK_zgelq(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* T, lapack_int const* tsize,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgelq2 LAPACK_GLOBAL(cgelq2,CGELQ2)
void LAPACK_cgelq2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dgelq2 LAPACK_GLOBAL(dgelq2,DGELQ2)
void LAPACK_dgelq2(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work,
    lapack_int* info );

#define LAPACK_sgelq2 LAPACK_GLOBAL(sgelq2,SGELQ2)
void LAPACK_sgelq2(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work,
    lapack_int* info );

#define LAPACK_zgelq2 LAPACK_GLOBAL(zgelq2,ZGELQ2)
void LAPACK_zgelq2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_cgelqf LAPACK_GLOBAL(cgelqf,CGELQF)
void LAPACK_cgelqf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgelqf LAPACK_GLOBAL(dgelqf,DGELQF)
void LAPACK_dgelqf(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgelqf LAPACK_GLOBAL(sgelqf,SGELQF)
void LAPACK_sgelqf(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgelqf LAPACK_GLOBAL(zgelqf,ZGELQF)
void LAPACK_zgelqf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgels_base LAPACK_GLOBAL(cgels,CGELS)
void LAPACK_cgels_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgels(...) LAPACK_cgels_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgels(...) LAPACK_cgels_base(__VA_ARGS__)
#endif

#define LAPACK_dgels_base LAPACK_GLOBAL(dgels,DGELS)
void LAPACK_dgels_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgels(...) LAPACK_dgels_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgels(...) LAPACK_dgels_base(__VA_ARGS__)
#endif

#define LAPACK_sgels_base LAPACK_GLOBAL(sgels,SGELS)
void LAPACK_sgels_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgels(...) LAPACK_sgels_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgels(...) LAPACK_sgels_base(__VA_ARGS__)
#endif

#define LAPACK_zgels_base LAPACK_GLOBAL(zgels,ZGELS)
void LAPACK_zgels_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgels(...) LAPACK_zgels_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgels(...) LAPACK_zgels_base(__VA_ARGS__)
#endif

#define LAPACK_cgelsd LAPACK_GLOBAL(cgelsd,CGELSD)
void LAPACK_cgelsd(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float* S,
    float const* rcond, lapack_int* rank,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork,
    lapack_int* info );

#define LAPACK_dgelsd LAPACK_GLOBAL(dgelsd,DGELSD)
void LAPACK_dgelsd(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* S,
    double const* rcond, lapack_int* rank,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info );

#define LAPACK_sgelsd LAPACK_GLOBAL(sgelsd,SGELSD)
void LAPACK_sgelsd(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* S,
    float const* rcond, lapack_int* rank,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info );

#define LAPACK_zgelsd LAPACK_GLOBAL(zgelsd,ZGELSD)
void LAPACK_zgelsd(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double* S,
    double const* rcond, lapack_int* rank,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork,
    lapack_int* info );

#define LAPACK_cgelss LAPACK_GLOBAL(cgelss,CGELSS)
void LAPACK_cgelss(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float* S,
    float const* rcond, lapack_int* rank,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info );

#define LAPACK_dgelss LAPACK_GLOBAL(dgelss,DGELSS)
void LAPACK_dgelss(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* S,
    double const* rcond, lapack_int* rank,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgelss LAPACK_GLOBAL(sgelss,SGELSS)
void LAPACK_sgelss(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* S,
    float const* rcond, lapack_int* rank,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgelss LAPACK_GLOBAL(zgelss,ZGELSS)
void LAPACK_zgelss(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double* S,
    double const* rcond, lapack_int* rank,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info );

#define LAPACK_cgelsy LAPACK_GLOBAL(cgelsy,CGELSY)
void LAPACK_cgelsy(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb, lapack_int* JPVT,
    float const* rcond, lapack_int* rank,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info );

#define LAPACK_dgelsy LAPACK_GLOBAL(dgelsy,DGELSY)
void LAPACK_dgelsy(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb, lapack_int* JPVT,
    double const* rcond, lapack_int* rank,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgelsy LAPACK_GLOBAL(sgelsy,SGELSY)
void LAPACK_sgelsy(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb, lapack_int* JPVT,
    float const* rcond, lapack_int* rank,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgelsy LAPACK_GLOBAL(zgelsy,ZGELSY)
void LAPACK_zgelsy(
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb, lapack_int* JPVT,
    double const* rcond, lapack_int* rank,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info );

#define LAPACK_cgemlq_base LAPACK_GLOBAL(cgemlq,CGEMLQ)
void LAPACK_cgemlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* T, lapack_int const* tsize,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgemlq(...) LAPACK_cgemlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgemlq(...) LAPACK_cgemlq_base(__VA_ARGS__)
#endif

#define LAPACK_dgemlq_base LAPACK_GLOBAL(dgemlq,DGEMLQ)
void LAPACK_dgemlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* A, lapack_int const* lda,
    double const* T, lapack_int const* tsize,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgemlq(...) LAPACK_dgemlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgemlq(...) LAPACK_dgemlq_base(__VA_ARGS__)
#endif

#define LAPACK_sgemlq_base LAPACK_GLOBAL(sgemlq,SGEMLQ)
void LAPACK_sgemlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* A, lapack_int const* lda,
    float const* T, lapack_int const* tsize,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgemlq(...) LAPACK_sgemlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgemlq(...) LAPACK_sgemlq_base(__VA_ARGS__)
#endif

#define LAPACK_zgemlq_base LAPACK_GLOBAL(zgemlq,ZGEMLQ)
void LAPACK_zgemlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* T, lapack_int const* tsize,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgemlq(...) LAPACK_zgemlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgemlq(...) LAPACK_zgemlq_base(__VA_ARGS__)
#endif

#define LAPACK_cgemqr_base LAPACK_GLOBAL(cgemqr,CGEMQR)
void LAPACK_cgemqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* T, lapack_int const* tsize,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgemqr(...) LAPACK_cgemqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgemqr(...) LAPACK_cgemqr_base(__VA_ARGS__)
#endif

#define LAPACK_dgemqr_base LAPACK_GLOBAL(dgemqr,DGEMQR)
void LAPACK_dgemqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* A, lapack_int const* lda,
    double const* T, lapack_int const* tsize,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgemqr(...) LAPACK_dgemqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgemqr(...) LAPACK_dgemqr_base(__VA_ARGS__)
#endif

#define LAPACK_sgemqr_base LAPACK_GLOBAL(sgemqr,SGEMQR)
void LAPACK_sgemqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* A, lapack_int const* lda,
    float const* T, lapack_int const* tsize,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgemqr(...) LAPACK_sgemqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgemqr(...) LAPACK_sgemqr_base(__VA_ARGS__)
#endif

#define LAPACK_zgemqr_base LAPACK_GLOBAL(zgemqr,ZGEMQR)
void LAPACK_zgemqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* T, lapack_int const* tsize,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgemqr(...) LAPACK_zgemqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgemqr(...) LAPACK_zgemqr_base(__VA_ARGS__)
#endif

#define LAPACK_cgemqrt_base LAPACK_GLOBAL(cgemqrt,CGEMQRT)
void LAPACK_cgemqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* nb,
    lapack_complex_float const* V, lapack_int const* ldv,
    lapack_complex_float const* T, lapack_int const* ldt,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgemqrt(...) LAPACK_cgemqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgemqrt(...) LAPACK_cgemqrt_base(__VA_ARGS__)
#endif

#define LAPACK_dgemqrt_base LAPACK_GLOBAL(dgemqrt,DGEMQRT)
void LAPACK_dgemqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* nb,
    double const* V, lapack_int const* ldv,
    double const* T, lapack_int const* ldt,
    double* C, lapack_int const* ldc,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgemqrt(...) LAPACK_dgemqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgemqrt(...) LAPACK_dgemqrt_base(__VA_ARGS__)
#endif

#define LAPACK_sgemqrt_base LAPACK_GLOBAL(sgemqrt,SGEMQRT)
void LAPACK_sgemqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* nb,
    float const* V, lapack_int const* ldv,
    float const* T, lapack_int const* ldt,
    float* C, lapack_int const* ldc,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgemqrt(...) LAPACK_sgemqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgemqrt(...) LAPACK_sgemqrt_base(__VA_ARGS__)
#endif

#define LAPACK_zgemqrt_base LAPACK_GLOBAL(zgemqrt,ZGEMQRT)
void LAPACK_zgemqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* nb,
    lapack_complex_double const* V, lapack_int const* ldv,
    lapack_complex_double const* T, lapack_int const* ldt,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgemqrt(...) LAPACK_zgemqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgemqrt(...) LAPACK_zgemqrt_base(__VA_ARGS__)
#endif

#define LAPACK_cgeql2 LAPACK_GLOBAL(cgeql2,CGEQL2)
void LAPACK_cgeql2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dgeql2 LAPACK_GLOBAL(dgeql2,DGEQL2)
void LAPACK_dgeql2(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work,
    lapack_int* info );

#define LAPACK_sgeql2 LAPACK_GLOBAL(sgeql2,SGEQL2)
void LAPACK_sgeql2(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work,
    lapack_int* info );

#define LAPACK_zgeql2 LAPACK_GLOBAL(zgeql2,ZGEQL2)
void LAPACK_zgeql2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_cgeqlf LAPACK_GLOBAL(cgeqlf,CGEQLF)
void LAPACK_cgeqlf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgeqlf LAPACK_GLOBAL(dgeqlf,DGEQLF)
void LAPACK_dgeqlf(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgeqlf LAPACK_GLOBAL(sgeqlf,SGEQLF)
void LAPACK_sgeqlf(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgeqlf LAPACK_GLOBAL(zgeqlf,ZGEQLF)
void LAPACK_zgeqlf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgeqpf LAPACK_GLOBAL(sgeqpf,SGEQPF)
void LAPACK_sgeqpf( lapack_int* m, lapack_int* n, float* a, lapack_int* lda,
                    lapack_int* jpvt, float* tau, float* work,
                    lapack_int *info );

#define LAPACK_dgeqpf LAPACK_GLOBAL(dgeqpf,DGEQPF)
void LAPACK_dgeqpf( lapack_int* m, lapack_int* n, double* a, lapack_int* lda,
                    lapack_int* jpvt, double* tau, double* work,
                    lapack_int *info );

#define LAPACK_cgeqpf LAPACK_GLOBAL(cgeqpf,CGEQPF)
void LAPACK_cgeqpf( lapack_int* m, lapack_int* n, lapack_complex_float* a,
                    lapack_int* lda, lapack_int* jpvt,
                    lapack_complex_float* tau, lapack_complex_float* work,
                    float* rwork, lapack_int *info );

#define LAPACK_zgeqpf LAPACK_GLOBAL(zgeqpf,ZGEQPF)
void LAPACK_zgeqpf( lapack_int* m, lapack_int* n, lapack_complex_double* a,
                    lapack_int* lda, lapack_int* jpvt,
                    lapack_complex_double* tau, lapack_complex_double* work,
                    double* rwork, lapack_int *info );

#define LAPACK_cgeqp3 LAPACK_GLOBAL(cgeqp3,CGEQP3)
void LAPACK_cgeqp3(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* JPVT,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info );

#define LAPACK_dgeqp3 LAPACK_GLOBAL(dgeqp3,DGEQP3)
void LAPACK_dgeqp3(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* JPVT,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgeqp3 LAPACK_GLOBAL(sgeqp3,SGEQP3)
void LAPACK_sgeqp3(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* JPVT,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgeqp3 LAPACK_GLOBAL(zgeqp3,ZGEQP3)
void LAPACK_zgeqp3(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* JPVT,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info );

#define LAPACK_cgeqr LAPACK_GLOBAL(cgeqr,CGEQR)
void LAPACK_cgeqr(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* T, lapack_int const* tsize,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgeqr LAPACK_GLOBAL(dgeqr,DGEQR)
void LAPACK_dgeqr(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* T, lapack_int const* tsize,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgeqr LAPACK_GLOBAL(sgeqr,SGEQR)
void LAPACK_sgeqr(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* T, lapack_int const* tsize,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgeqr LAPACK_GLOBAL(zgeqr,ZGEQR)
void LAPACK_zgeqr(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* T, lapack_int const* tsize,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgeqr2 LAPACK_GLOBAL(cgeqr2,CGEQR2)
void LAPACK_cgeqr2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dgeqr2 LAPACK_GLOBAL(dgeqr2,DGEQR2)
void LAPACK_dgeqr2(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work,
    lapack_int* info );

#define LAPACK_sgeqr2 LAPACK_GLOBAL(sgeqr2,SGEQR2)
void LAPACK_sgeqr2(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work,
    lapack_int* info );

#define LAPACK_zgeqr2 LAPACK_GLOBAL(zgeqr2,ZGEQR2)
void LAPACK_zgeqr2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_cgeqrf LAPACK_GLOBAL(cgeqrf,CGEQRF)
void LAPACK_cgeqrf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgeqrf LAPACK_GLOBAL(dgeqrf,DGEQRF)
void LAPACK_dgeqrf(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgeqrf LAPACK_GLOBAL(sgeqrf,SGEQRF)
void LAPACK_sgeqrf(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgeqrf LAPACK_GLOBAL(zgeqrf,ZGEQRF)
void LAPACK_zgeqrf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgeqrfp LAPACK_GLOBAL(cgeqrfp,CGEQRFP)
void LAPACK_cgeqrfp(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgeqrfp LAPACK_GLOBAL(dgeqrfp,DGEQRFP)
void LAPACK_dgeqrfp(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgeqrfp LAPACK_GLOBAL(sgeqrfp,SGEQRFP)
void LAPACK_sgeqrfp(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgeqrfp LAPACK_GLOBAL(zgeqrfp,ZGEQRFP)
void LAPACK_zgeqrfp(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgeqrt LAPACK_GLOBAL(cgeqrt,CGEQRT)
void LAPACK_cgeqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* nb,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dgeqrt LAPACK_GLOBAL(dgeqrt,DGEQRT)
void LAPACK_dgeqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* nb,
    double* A, lapack_int const* lda,
    double* T, lapack_int const* ldt,
    double* work,
    lapack_int* info );

#define LAPACK_sgeqrt LAPACK_GLOBAL(sgeqrt,SGEQRT)
void LAPACK_sgeqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* nb,
    float* A, lapack_int const* lda,
    float* T, lapack_int const* ldt,
    float* work,
    lapack_int* info );

#define LAPACK_zgeqrt LAPACK_GLOBAL(zgeqrt,ZGEQRT)
void LAPACK_zgeqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* nb,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_cgeqrt2 LAPACK_GLOBAL(cgeqrt2,CGEQRT2)
void LAPACK_cgeqrt2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_dgeqrt2 LAPACK_GLOBAL(dgeqrt2,DGEQRT2)
void LAPACK_dgeqrt2(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_sgeqrt2 LAPACK_GLOBAL(sgeqrt2,SGEQRT2)
void LAPACK_sgeqrt2(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_zgeqrt2 LAPACK_GLOBAL(zgeqrt2,ZGEQRT2)
void LAPACK_zgeqrt2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_cgeqrt3 LAPACK_GLOBAL(cgeqrt3,CGEQRT3)
void LAPACK_cgeqrt3(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_dgeqrt3 LAPACK_GLOBAL(dgeqrt3,DGEQRT3)
void LAPACK_dgeqrt3(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_sgeqrt3 LAPACK_GLOBAL(sgeqrt3,SGEQRT3)
void LAPACK_sgeqrt3(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_zgeqrt3 LAPACK_GLOBAL(zgeqrt3,ZGEQRT3)
void LAPACK_zgeqrt3(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_cgerfs_base LAPACK_GLOBAL(cgerfs,CGERFS)
void LAPACK_cgerfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgerfs(...) LAPACK_cgerfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgerfs(...) LAPACK_cgerfs_base(__VA_ARGS__)
#endif

#define LAPACK_dgerfs_base LAPACK_GLOBAL(dgerfs,DGERFS)
void LAPACK_dgerfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgerfs(...) LAPACK_dgerfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgerfs(...) LAPACK_dgerfs_base(__VA_ARGS__)
#endif

#define LAPACK_sgerfs_base LAPACK_GLOBAL(sgerfs,SGERFS)
void LAPACK_sgerfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgerfs(...) LAPACK_sgerfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgerfs(...) LAPACK_sgerfs_base(__VA_ARGS__)
#endif

#define LAPACK_zgerfs_base LAPACK_GLOBAL(zgerfs,ZGERFS)
void LAPACK_zgerfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgerfs(...) LAPACK_zgerfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgerfs(...) LAPACK_zgerfs_base(__VA_ARGS__)
#endif

#define LAPACK_cgerfsx_base LAPACK_GLOBAL(cgerfsx,CGERFSX)
void LAPACK_cgerfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    float const* R,
    float const* C,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgerfsx(...) LAPACK_cgerfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgerfsx(...) LAPACK_cgerfsx_base(__VA_ARGS__)
#endif

#define LAPACK_dgerfsx_base LAPACK_GLOBAL(dgerfsx,DGERFSX)
void LAPACK_dgerfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    double const* R,
    double const* C,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgerfsx(...) LAPACK_dgerfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgerfsx(...) LAPACK_dgerfsx_base(__VA_ARGS__)
#endif

#define LAPACK_sgerfsx_base LAPACK_GLOBAL(sgerfsx,SGERFSX)
void LAPACK_sgerfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    float const* R,
    float const* C,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgerfsx(...) LAPACK_sgerfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgerfsx(...) LAPACK_sgerfsx_base(__VA_ARGS__)
#endif

#define LAPACK_zgerfsx_base LAPACK_GLOBAL(zgerfsx,ZGERFSX)
void LAPACK_zgerfsx_base(
    char const* trans, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    double const* R,
    double const* C,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgerfsx(...) LAPACK_zgerfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgerfsx(...) LAPACK_zgerfsx_base(__VA_ARGS__)
#endif

#define LAPACK_cgerq2 LAPACK_GLOBAL(cgerq2,CGERQ2)
void LAPACK_cgerq2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dgerq2 LAPACK_GLOBAL(dgerq2,DGERQ2)
void LAPACK_dgerq2(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work,
    lapack_int* info );

#define LAPACK_sgerq2 LAPACK_GLOBAL(sgerq2,SGERQ2)
void LAPACK_sgerq2(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work,
    lapack_int* info );

#define LAPACK_zgerq2 LAPACK_GLOBAL(zgerq2,ZGERQ2)
void LAPACK_zgerq2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_cgerqf LAPACK_GLOBAL(cgerqf,CGERQF)
void LAPACK_cgerqf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgerqf LAPACK_GLOBAL(dgerqf,DGERQF)
void LAPACK_dgerqf(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgerqf LAPACK_GLOBAL(sgerqf,SGERQF)
void LAPACK_sgerqf(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgerqf LAPACK_GLOBAL(zgerqf,ZGERQF)
void LAPACK_zgerqf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgesdd_base LAPACK_GLOBAL(cgesdd,CGESDD)
void LAPACK_cgesdd_base(
    char const* jobz,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* S,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* VT, lapack_int const* ldvt,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgesdd(...) LAPACK_cgesdd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgesdd(...) LAPACK_cgesdd_base(__VA_ARGS__)
#endif

#define LAPACK_dgesdd_base LAPACK_GLOBAL(dgesdd,DGESDD)
void LAPACK_dgesdd_base(
    char const* jobz,
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* S,
    double* U, lapack_int const* ldu,
    double* VT, lapack_int const* ldvt,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgesdd(...) LAPACK_dgesdd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgesdd(...) LAPACK_dgesdd_base(__VA_ARGS__)
#endif

#define LAPACK_sgesdd_base LAPACK_GLOBAL(sgesdd,SGESDD)
void LAPACK_sgesdd_base(
    char const* jobz,
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* S,
    float* U, lapack_int const* ldu,
    float* VT, lapack_int const* ldvt,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgesdd(...) LAPACK_sgesdd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgesdd(...) LAPACK_sgesdd_base(__VA_ARGS__)
#endif

#define LAPACK_zgesdd_base LAPACK_GLOBAL(zgesdd,ZGESDD)
void LAPACK_zgesdd_base(
    char const* jobz,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* S,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* VT, lapack_int const* ldvt,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgesdd(...) LAPACK_zgesdd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgesdd(...) LAPACK_zgesdd_base(__VA_ARGS__)
#endif

#define LAPACK_cgedmd_base LAPACK_GLOBAL(cgedmd,CGEDMD)
void LAPACK_cgedmd_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobf,
    lapack_int const* whtsvd, lapack_int const* m, lapack_int const* n,
    lapack_complex_float* x, lapack_int const* ldx,
    lapack_complex_float* y, lapack_int const* ldy, lapack_int const* nrnk,
    const float* tol, lapack_int* k, lapack_complex_float* eigs,
    lapack_complex_float* z, lapack_int const* ldz, float* res,
    lapack_complex_float* b, lapack_int const* ldb,
    lapack_complex_float* w, lapack_int const* ldw,
    lapack_complex_float* s, lapack_int const* lds,
    lapack_complex_float* zwork, lapack_int const* lzwork,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgedmd(...) LAPACK_cgedmd_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_cgedmd(...) LAPACK_cgedmd_base(__VA_ARGS__)
#endif


#define LAPACK_dgedmd_base LAPACK_GLOBAL(dgedmd,DGEDMD)
void LAPACK_dgedmd_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobf,
    lapack_int const* whtsvd, lapack_int const* m, lapack_int const* n,
    double* x, lapack_int const* ldx,
    double* y, lapack_int const* ldy, lapack_int const* nrnk,
    const double* tol, lapack_int* k, double* reig, double* imeig,
    double* z, lapack_int const* ldz, double* res,
    double* b, lapack_int const* ldb,
    double* w, lapack_int const* ldw,
    double* s, lapack_int const* lds,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgedmd(...) LAPACK_dgedmd_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_dgedmd(...) LAPACK_dgedmd_base(__VA_ARGS__)
#endif

#define LAPACK_sgedmd_base LAPACK_GLOBAL(sgedmd,SGEDMD)
void LAPACK_sgedmd_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobf,
    lapack_int const* whtsvd, lapack_int const* m, lapack_int const* n,
    float* x, lapack_int const* ldx,
    float* y, lapack_int const* ldy, lapack_int const* nrnk,
    const float* tol, lapack_int* k, float* reig, float *imeig,
    float* z, lapack_int const* ldz, float* res,
    float* b, lapack_int const* ldb,
    float* w, lapack_int const* ldw,
    float* s, lapack_int const* lds,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgedmd(...) LAPACK_sgedmd_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_sgedmd(...) LAPACK_sgedmd_base(__VA_ARGS__)
#endif

#define LAPACK_zgedmd_base LAPACK_GLOBAL(zgedmd,ZGEDMD)
void LAPACK_zgedmd_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobf,
    lapack_int const* whtsvd, lapack_int const* m, lapack_int const* n,
    lapack_complex_double* x, lapack_int const* ldx,
    lapack_complex_double* y, lapack_int const* ldy, lapack_int const* nrnk,
    const double* tol, lapack_int *k, lapack_complex_double* eigs,
    lapack_complex_double* z, lapack_int const* ldz, double* res,
    lapack_complex_double* b, lapack_int const* ldb,
    lapack_complex_double* w, lapack_int const* ldw,
    lapack_complex_double* s, lapack_int const* lds,
    lapack_complex_double* zwork, lapack_int const* lzwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgedmd(...) LAPACK_zgedmd_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_zgedmd(...) LAPACK_zgedmd_base(__VA_ARGS__)
#endif

#define LAPACK_cgedmdq_base LAPACK_GLOBAL(cgedmdq,CGEDMDQ)
void LAPACK_cgedmdq_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobq,
    char const* jobt, char const* jobf, lapack_int const* whtsvd,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* f, lapack_int const* ldf,
    lapack_complex_float* x, lapack_int const* ldx,
    lapack_complex_float* y, lapack_int const* ldy, lapack_int const* nrnk,
    float const* tol, lapack_int const* k,
    lapack_complex_float* eigs,
    lapack_complex_float* z, lapack_int const* ldz, float* res,
    lapack_complex_float* b, lapack_int const* ldb,
    lapack_complex_float* v, lapack_int const* ldv,
    lapack_complex_float* s, lapack_int const* lds,
    lapack_complex_float* zwork, lapack_int const* lzwork,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgedmdq(...) LAPACK_cgedmdq_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_cgedmdq(...) LAPACK_cgedmdq_base(__VA_ARGS__)
#endif

#define LAPACK_dgedmdq_base LAPACK_GLOBAL(dgedmdq,DGEDMDQ)
void LAPACK_dgedmdq_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobq,
    char const* jobt, char const* jobf, lapack_int const* whtsvd,
    lapack_int const* m, lapack_int const* n,
    double* f, lapack_int const* ldf,
    double* x, lapack_int const* ldx,
    double* y, lapack_int const* ldy, lapack_int const* nrnk,
    double const* tol, lapack_int* k,
    double* reig, double *imeig,
    double* z, lapack_int const* ldz, double* res,
    double* b, lapack_int const* ldb,
    double* v, lapack_int const* ldv,
    double* s, lapack_int const* lds,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgedmdq(...) LAPACK_dgedmdq_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_dgedmdq(...) LAPACK_dgedmdq_base(__VA_ARGS__)
#endif

#define LAPACK_sgedmdq_base LAPACK_GLOBAL(sgedmdq,SGEDMDQ)
void LAPACK_sgedmdq_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobq,
    char const* jobt, char const* jobf, lapack_int const* whtsvd,
    lapack_int const* m, lapack_int const* n,
    float* f, lapack_int const* ldf,
    float* x, lapack_int const* ldx,
    float* y, lapack_int const* ldy, lapack_int const* nrnk,
    float const* tol, lapack_int const* k,
    float* reig, float* imeig,
    float* z, lapack_int const* ldz, float* res,
    float* b, lapack_int const* ldb,
    float* v, lapack_int const* ldv,
    float* s, lapack_int const* lds,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgedmdq(...) LAPACK_sgedmdq_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_sgedmdq(...) LAPACK_sgedmdq_base(__VA_ARGS__)
#endif

#define LAPACK_zgedmdq_base LAPACK_GLOBAL(zgedmdq,ZGEDMDQ)
void LAPACK_zgedmdq_base(
    char const* jobs, char const* jobz, char const* jobr, char const* jobq,
    char const* jobt, char const* jobf, lapack_int const* whtsvd,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* f, lapack_int const* ldf,
    lapack_complex_double* x, lapack_int const* ldx,
    lapack_complex_double* y, lapack_int const* ldy, lapack_int const* nrnk,
    double const* tol, lapack_int const* k,
    lapack_complex_double* eigs,
    lapack_complex_double* z, lapack_int const* ldz, double* res,
    lapack_complex_double* b, lapack_int const* ldb,
    lapack_complex_double* v, lapack_int const* ldv,
    lapack_complex_double* s, lapack_int const* lds,
    lapack_complex_double* zwork, lapack_int const* lzwork,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info

#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgedmdq(...) LAPACK_zgedmdq_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_zgedmdq(...) LAPACK_zgedmdq_base(__VA_ARGS__)
#endif

#define LAPACK_cgesv LAPACK_GLOBAL(cgesv,CGESV)
lapack_int LAPACK_cgesv(
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_dgesv LAPACK_GLOBAL(dgesv,DGESV)
lapack_int LAPACK_dgesv(
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_sgesv LAPACK_GLOBAL(sgesv,SGESV)
lapack_int LAPACK_sgesv(
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_zgesv LAPACK_GLOBAL(zgesv,ZGESV)
lapack_int LAPACK_zgesv(
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_dsgesv LAPACK_GLOBAL(dsgesv,DSGESV)
void LAPACK_dsgesv(
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* work,
    float* swork, lapack_int* iter,
    lapack_int* info );

#define LAPACK_zcgesv LAPACK_GLOBAL(zcgesv,ZCGESV)
void LAPACK_zcgesv(
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    lapack_complex_double* work,
    lapack_complex_float* swork,
    double* rwork, lapack_int* iter,
    lapack_int* info );

#define LAPACK_cgesvd_base LAPACK_GLOBAL(cgesvd,CGESVD)
void LAPACK_cgesvd_base(
    char const* jobu, char const* jobvt,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* S,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* VT, lapack_int const* ldvt,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgesvd(...) LAPACK_cgesvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgesvd(...) LAPACK_cgesvd_base(__VA_ARGS__)
#endif

#define LAPACK_dgesvd_base LAPACK_GLOBAL(dgesvd,DGESVD)
void LAPACK_dgesvd_base(
    char const* jobu, char const* jobvt,
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* S,
    double* U, lapack_int const* ldu,
    double* VT, lapack_int const* ldvt,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgesvd(...) LAPACK_dgesvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgesvd(...) LAPACK_dgesvd_base(__VA_ARGS__)
#endif

#define LAPACK_sgesvd_base LAPACK_GLOBAL(sgesvd,SGESVD)
void LAPACK_sgesvd_base(
    char const* jobu, char const* jobvt,
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* S,
    float* U, lapack_int const* ldu,
    float* VT, lapack_int const* ldvt,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgesvd(...) LAPACK_sgesvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgesvd(...) LAPACK_sgesvd_base(__VA_ARGS__)
#endif

#define LAPACK_zgesvd_base LAPACK_GLOBAL(zgesvd,ZGESVD)
void LAPACK_zgesvd_base(
    char const* jobu, char const* jobvt,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* S,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* VT, lapack_int const* ldvt,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgesvd(...) LAPACK_zgesvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgesvd(...) LAPACK_zgesvd_base(__VA_ARGS__)
#endif

#define LAPACK_cgesvdq_base LAPACK_GLOBAL(cgesvdq,CGESVDQ)
void LAPACK_cgesvdq_base(
    char const* joba, char const* jobp, char const* jobr, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* S,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* V, lapack_int const* ldv, lapack_int* numrank,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_complex_float* cwork, lapack_int* lcwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgesvdq(...) LAPACK_cgesvdq_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_cgesvdq(...) LAPACK_cgesvdq_base(__VA_ARGS__)
#endif

#define LAPACK_dgesvdq_base LAPACK_GLOBAL(dgesvdq,DGESVDQ)
void LAPACK_dgesvdq_base(
    char const* joba, char const* jobp, char const* jobr, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* S,
    double* U, lapack_int const* ldu,
    double* V, lapack_int const* ldv, lapack_int* numrank,
    lapack_int* iwork, lapack_int const* liwork,
    double* work, lapack_int* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgesvdq(...) LAPACK_dgesvdq_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_dgesvdq(...) LAPACK_dgesvdq_base(__VA_ARGS__)
#endif

#define LAPACK_sgesvdq_base LAPACK_GLOBAL(sgesvdq,SGESVDQ)
void LAPACK_sgesvdq_base(
    char const* joba, char const* jobp, char const* jobr, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* S,
    float* U, lapack_int const* ldu,
    float* V, lapack_int const* ldv, lapack_int* numrank,
    lapack_int* iwork, lapack_int const* liwork,
    float* work, lapack_int* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgesvdq(...) LAPACK_sgesvdq_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_sgesvdq(...) LAPACK_sgesvdq_base(__VA_ARGS__)
#endif

#define LAPACK_zgesvdq_base LAPACK_GLOBAL(zgesvdq,ZGESVDQ)
void LAPACK_zgesvdq_base(
    char const* joba, char const* jobp, char const* jobr, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* S,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* V, lapack_int const* ldv, lapack_int* numrank,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_complex_double* cwork, lapack_int* lcwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgesvdq(...) LAPACK_zgesvdq_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_zgesvdq(...) LAPACK_zgesvdq_base(__VA_ARGS__)
#endif

#define LAPACK_cgesvdx_base LAPACK_GLOBAL(cgesvdx,CGESVDX)
void LAPACK_cgesvdx_base(
    char const* jobu, char const* jobvt, char const* range,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* ns,
    float* S,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* VT, lapack_int const* ldvt,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgesvdx(...) LAPACK_cgesvdx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgesvdx(...) LAPACK_cgesvdx_base(__VA_ARGS__)
#endif


#define LAPACK_dgesvdx_base LAPACK_GLOBAL(dgesvdx,DGESVDX)
void LAPACK_dgesvdx_base(
    char const* jobu, char const* jobvt, char const* range,
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* ns,
    double* S,
    double* U, lapack_int const* ldu,
    double* VT, lapack_int const* ldvt,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgesvdx(...) LAPACK_dgesvdx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgesvdx(...) LAPACK_dgesvdx_base(__VA_ARGS__)
#endif

#define LAPACK_sgesvdx_base LAPACK_GLOBAL(sgesvdx,SGESVDX)
void LAPACK_sgesvdx_base(
    char const* jobu, char const* jobvt, char const* range,
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* ns,
    float* S,
    float* U, lapack_int const* ldu,
    float* VT, lapack_int const* ldvt,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgesvdx(...) LAPACK_sgesvdx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgesvdx(...) LAPACK_sgesvdx_base(__VA_ARGS__)
#endif

#define LAPACK_zgesvdx_base LAPACK_GLOBAL(zgesvdx,ZGESVDX)
void LAPACK_zgesvdx_base(
    char const* jobu, char const* jobvt, char const* range,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* ns,
    double* S,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* VT, lapack_int const* ldvt,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgesvdx(...) LAPACK_zgesvdx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgesvdx(...) LAPACK_zgesvdx_base(__VA_ARGS__)
#endif

#define LAPACK_cgesvj_base LAPACK_GLOBAL(cgesvj,CGESVJ)
void LAPACK_cgesvj_base(
    char const* joba, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* SVA, lapack_int const* mv,
    lapack_complex_float* V, lapack_int const* ldv,
    lapack_complex_float* cwork, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgesvj(...) LAPACK_cgesvj_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgesvj(...) LAPACK_cgesvj_base(__VA_ARGS__)
#endif

#define LAPACK_dgesvj_base LAPACK_GLOBAL(dgesvj,DGESVJ)
void LAPACK_dgesvj_base(
    char const* joba, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* SVA, lapack_int const* mv,
    double* V, lapack_int const* ldv,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgesvj(...) LAPACK_dgesvj_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgesvj(...) LAPACK_dgesvj_base(__VA_ARGS__)
#endif

#define LAPACK_sgesvj_base LAPACK_GLOBAL(sgesvj,SGESVJ)
void LAPACK_sgesvj_base(
    char const* joba, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* SVA, lapack_int const* mv,
    float* V, lapack_int const* ldv,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgesvj(...) LAPACK_sgesvj_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgesvj(...) LAPACK_sgesvj_base(__VA_ARGS__)
#endif

#define LAPACK_zgesvj_base LAPACK_GLOBAL(zgesvj,ZGESVJ)
void LAPACK_zgesvj_base(
    char const* joba, char const* jobu, char const* jobv,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* SVA, lapack_int const* mv,
    lapack_complex_double* V, lapack_int const* ldv,
    lapack_complex_double* cwork, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgesvj(...) LAPACK_zgesvj_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgesvj(...) LAPACK_zgesvj_base(__VA_ARGS__)
#endif

#define LAPACK_cgesvx_base LAPACK_GLOBAL(cgesvx,CGESVX)
void LAPACK_cgesvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgesvx(...) LAPACK_cgesvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgesvx(...) LAPACK_cgesvx_base(__VA_ARGS__)
#endif

#define LAPACK_dgesvx_base LAPACK_GLOBAL(dgesvx,DGESVX)
void LAPACK_dgesvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgesvx(...) LAPACK_dgesvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgesvx(...) LAPACK_dgesvx_base(__VA_ARGS__)
#endif

#define LAPACK_sgesvx_base LAPACK_GLOBAL(sgesvx,SGESVX)
void LAPACK_sgesvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgesvx(...) LAPACK_sgesvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgesvx(...) LAPACK_sgesvx_base(__VA_ARGS__)
#endif

#define LAPACK_zgesvx_base LAPACK_GLOBAL(zgesvx,ZGESVX)
void LAPACK_zgesvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgesvx(...) LAPACK_zgesvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgesvx(...) LAPACK_zgesvx_base(__VA_ARGS__)
#endif

#define LAPACK_cgesvxx_base LAPACK_GLOBAL(cgesvxx,CGESVXX)
void LAPACK_cgesvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgesvxx(...) LAPACK_cgesvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgesvxx(...) LAPACK_cgesvxx_base(__VA_ARGS__)
#endif

#define LAPACK_dgesvxx_base LAPACK_GLOBAL(dgesvxx,DGESVXX)
void LAPACK_dgesvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgesvxx(...) LAPACK_dgesvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgesvxx(...) LAPACK_dgesvxx_base(__VA_ARGS__)
#endif

#define LAPACK_sgesvxx_base LAPACK_GLOBAL(sgesvxx,SGESVXX)
void LAPACK_sgesvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    float* R,
    float* C,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgesvxx(...) LAPACK_sgesvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgesvxx(...) LAPACK_sgesvxx_base(__VA_ARGS__)
#endif

#define LAPACK_zgesvxx_base LAPACK_GLOBAL(zgesvxx,ZGESVXX)
void LAPACK_zgesvxx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    double* R,
    double* C,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgesvxx(...) LAPACK_zgesvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgesvxx(...) LAPACK_zgesvxx_base(__VA_ARGS__)
#endif

#define LAPACK_cgetf2 LAPACK_GLOBAL(cgetf2,CGETF2)
lapack_int LAPACK_cgetf2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_dgetf2 LAPACK_GLOBAL(dgetf2,DGETF2)
lapack_int LAPACK_dgetf2(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_sgetf2 LAPACK_GLOBAL(sgetf2,SGETF2)
lapack_int LAPACK_sgetf2(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_zgetf2 LAPACK_GLOBAL(zgetf2,ZGETF2)
lapack_int LAPACK_zgetf2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_cgetrf LAPACK_GLOBAL(cgetrf,CGETRF)
lapack_int LAPACK_cgetrf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_dgetrf LAPACK_GLOBAL(dgetrf,DGETRF)
lapack_int LAPACK_dgetrf(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_sgetrf LAPACK_GLOBAL(sgetrf,SGETRF)
lapack_int LAPACK_sgetrf(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_zgetrf LAPACK_GLOBAL(zgetrf,ZGETRF)
lapack_int LAPACK_zgetrf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_cgetrf2 LAPACK_GLOBAL(cgetrf2,CGETRF2)
void LAPACK_cgetrf2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_dgetrf2 LAPACK_GLOBAL(dgetrf2,DGETRF2)
void LAPACK_dgetrf2(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_sgetrf2 LAPACK_GLOBAL(sgetrf2,SGETRF2)
void LAPACK_sgetrf2(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_zgetrf2 LAPACK_GLOBAL(zgetrf2,ZGETRF2)
void LAPACK_zgetrf2(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_cgetri LAPACK_GLOBAL(cgetri,CGETRI)
void LAPACK_cgetri(
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgetri LAPACK_GLOBAL(dgetri,DGETRI)
void LAPACK_dgetri(
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int const* ipiv,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgetri LAPACK_GLOBAL(sgetri,SGETRI)
void LAPACK_sgetri(
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int const* ipiv,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgetri LAPACK_GLOBAL(zgetri,ZGETRI)
void LAPACK_zgetri(
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgetrs_base LAPACK_GLOBAL(cgetrs,CGETRS)
lapack_int LAPACK_cgetrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgetrs(...) LAPACK_cgetrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgetrs(...) LAPACK_cgetrs_base(__VA_ARGS__)
#endif

#define LAPACK_dgetrs_base LAPACK_GLOBAL(dgetrs,DGETRS)
lapack_int LAPACK_dgetrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgetrs(...) LAPACK_dgetrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgetrs(...) LAPACK_dgetrs_base(__VA_ARGS__)
#endif

#define LAPACK_sgetrs_base LAPACK_GLOBAL(sgetrs,SGETRS)
lapack_int LAPACK_sgetrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgetrs(...) LAPACK_sgetrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgetrs(...) LAPACK_sgetrs_base(__VA_ARGS__)
#endif

#define LAPACK_zgetrs_base LAPACK_GLOBAL(zgetrs,ZGETRS)
lapack_int LAPACK_zgetrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgetrs(...) LAPACK_zgetrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgetrs(...) LAPACK_zgetrs_base(__VA_ARGS__)
#endif

#define LAPACK_cgetsls_base LAPACK_GLOBAL(cgetsls,CGETSLS)
void LAPACK_cgetsls_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgetsls(...) LAPACK_cgetsls_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgetsls(...) LAPACK_cgetsls_base(__VA_ARGS__)
#endif

#define LAPACK_dgetsls_base LAPACK_GLOBAL(dgetsls,DGETSLS)
void LAPACK_dgetsls_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgetsls(...) LAPACK_dgetsls_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgetsls(...) LAPACK_dgetsls_base(__VA_ARGS__)
#endif

#define LAPACK_sgetsls_base LAPACK_GLOBAL(sgetsls,SGETSLS)
void LAPACK_sgetsls_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgetsls(...) LAPACK_sgetsls_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgetsls(...) LAPACK_sgetsls_base(__VA_ARGS__)
#endif

#define LAPACK_zgetsls_base LAPACK_GLOBAL(zgetsls,ZGETSLS)
void LAPACK_zgetsls_base(
    char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgetsls(...) LAPACK_zgetsls_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgetsls(...) LAPACK_zgetsls_base(__VA_ARGS__)
#endif

#define LAPACK_cgetsqrhrt LAPACK_GLOBAL(cgetsqrhrt,CGETSQRHRT)
void LAPACK_cgetsqrhrt(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb1, lapack_int const* nb1, lapack_int const* nb2,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgetsqrhrt LAPACK_GLOBAL(dgetsqrhrt,DGETSQRHRT)
void LAPACK_dgetsqrhrt(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb1, lapack_int const* nb1, lapack_int const* nb2,
    double* A, lapack_int const* lda,
    double* T, lapack_int const* ldt,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgetsqrhrt LAPACK_GLOBAL(sgetsqrhrt,SGETSQRHRT)
void LAPACK_sgetsqrhrt(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb1, lapack_int const* nb1, lapack_int const* nb2,
    float* A, lapack_int const* lda,
    float* T, lapack_int const* ldt,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgetsqrhrt LAPACK_GLOBAL(zgetsqrhrt,ZGETSQRHRT)
void LAPACK_zgetsqrhrt(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb1, lapack_int const* nb1, lapack_int const* nb2,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cggbak_base LAPACK_GLOBAL(cggbak,CGGBAK)
void LAPACK_cggbak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float const* lscale,
    float const* rscale, lapack_int const* m,
    lapack_complex_float* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggbak(...) LAPACK_cggbak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cggbak(...) LAPACK_cggbak_base(__VA_ARGS__)
#endif

#define LAPACK_dggbak_base LAPACK_GLOBAL(dggbak,DGGBAK)
void LAPACK_dggbak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double const* lscale,
    double const* rscale, lapack_int const* m,
    double* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggbak(...) LAPACK_dggbak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dggbak(...) LAPACK_dggbak_base(__VA_ARGS__)
#endif

#define LAPACK_sggbak_base LAPACK_GLOBAL(sggbak,SGGBAK)
void LAPACK_sggbak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float const* lscale,
    float const* rscale, lapack_int const* m,
    float* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggbak(...) LAPACK_sggbak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sggbak(...) LAPACK_sggbak_base(__VA_ARGS__)
#endif

#define LAPACK_zggbak_base LAPACK_GLOBAL(zggbak,ZGGBAK)
void LAPACK_zggbak_base(
    char const* job, char const* side,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double const* lscale,
    double const* rscale, lapack_int const* m,
    lapack_complex_double* V, lapack_int const* ldv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggbak(...) LAPACK_zggbak_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zggbak(...) LAPACK_zggbak_base(__VA_ARGS__)
#endif

#define LAPACK_cggbal_base LAPACK_GLOBAL(cggbal,CGGBAL)
void LAPACK_cggbal_base(
    char const* job,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb, lapack_int* ilo, lapack_int* ihi,
    float* lscale,
    float* rscale,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggbal(...) LAPACK_cggbal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cggbal(...) LAPACK_cggbal_base(__VA_ARGS__)
#endif

#define LAPACK_dggbal_base LAPACK_GLOBAL(dggbal,DGGBAL)
void LAPACK_dggbal_base(
    char const* job,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb, lapack_int* ilo, lapack_int* ihi,
    double* lscale,
    double* rscale,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggbal(...) LAPACK_dggbal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dggbal(...) LAPACK_dggbal_base(__VA_ARGS__)
#endif

#define LAPACK_sggbal_base LAPACK_GLOBAL(sggbal,SGGBAL)
void LAPACK_sggbal_base(
    char const* job,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb, lapack_int* ilo, lapack_int* ihi,
    float* lscale,
    float* rscale,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggbal(...) LAPACK_sggbal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sggbal(...) LAPACK_sggbal_base(__VA_ARGS__)
#endif

#define LAPACK_zggbal_base LAPACK_GLOBAL(zggbal,ZGGBAL)
void LAPACK_zggbal_base(
    char const* job,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb, lapack_int* ilo, lapack_int* ihi,
    double* lscale,
    double* rscale,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggbal(...) LAPACK_zggbal_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zggbal(...) LAPACK_zggbal_base(__VA_ARGS__)
#endif

#define LAPACK_cgges_base LAPACK_GLOBAL(cgges,CGGES)
void LAPACK_cgges_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_C_SELECT2 selctg,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb, lapack_int* sdim,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* VSL, lapack_int const* ldvsl,
    lapack_complex_float* VSR, lapack_int const* ldvsr,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgges(...) LAPACK_cgges_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgges(...) LAPACK_cgges_base(__VA_ARGS__)
#endif

#define LAPACK_dgges_base LAPACK_GLOBAL(dgges,DGGES)
void LAPACK_dgges_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_D_SELECT3 selctg,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb, lapack_int* sdim,
    double* alphar,
    double* alphai,
    double* beta,
    double* VSL, lapack_int const* ldvsl,
    double* VSR, lapack_int const* ldvsr,
    double* work, lapack_int const* lwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgges(...) LAPACK_dgges_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgges(...) LAPACK_dgges_base(__VA_ARGS__)
#endif

#define LAPACK_sgges_base LAPACK_GLOBAL(sgges,SGGES)
void LAPACK_sgges_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_S_SELECT3 selctg,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb, lapack_int* sdim,
    float* alphar,
    float* alphai,
    float* beta,
    float* VSL, lapack_int const* ldvsl,
    float* VSR, lapack_int const* ldvsr,
    float* work, lapack_int const* lwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgges(...) LAPACK_sgges_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgges(...) LAPACK_sgges_base(__VA_ARGS__)
#endif

#define LAPACK_zgges_base LAPACK_GLOBAL(zgges,ZGGES)
void LAPACK_zgges_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_Z_SELECT2 selctg,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb, lapack_int* sdim,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* VSL, lapack_int const* ldvsl,
    lapack_complex_double* VSR, lapack_int const* ldvsr,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgges(...) LAPACK_zgges_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgges(...) LAPACK_zgges_base(__VA_ARGS__)
#endif

#define LAPACK_cgges3_base LAPACK_GLOBAL(cgges3,CGGES3)
void LAPACK_cgges3_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_C_SELECT2 selctg,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb, lapack_int* sdim,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* VSL, lapack_int const* ldvsl,
    lapack_complex_float* VSR, lapack_int const* ldvsr,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgges3(...) LAPACK_cgges3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cgges3(...) LAPACK_cgges3_base(__VA_ARGS__)
#endif

#define LAPACK_dgges3_base LAPACK_GLOBAL(dgges3,DGGES3)
void LAPACK_dgges3_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_D_SELECT3 selctg,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb, lapack_int* sdim,
    double* alphar,
    double* alphai,
    double* beta,
    double* VSL, lapack_int const* ldvsl,
    double* VSR, lapack_int const* ldvsr,
    double* work, lapack_int const* lwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgges3(...) LAPACK_dgges3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dgges3(...) LAPACK_dgges3_base(__VA_ARGS__)
#endif

#define LAPACK_sgges3_base LAPACK_GLOBAL(sgges3,SGGES3)
void LAPACK_sgges3_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_S_SELECT3 selctg,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb, lapack_int* sdim,
    float* alphar,
    float* alphai,
    float* beta,
    float* VSL, lapack_int const* ldvsl,
    float* VSR, lapack_int const* ldvsr,
    float* work, lapack_int const* lwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgges3(...) LAPACK_sgges3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sgges3(...) LAPACK_sgges3_base(__VA_ARGS__)
#endif

#define LAPACK_zgges3_base LAPACK_GLOBAL(zgges3,ZGGES3)
void LAPACK_zgges3_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_Z_SELECT2 selctg,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb, lapack_int* sdim,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* VSL, lapack_int const* ldvsl,
    lapack_complex_double* VSR, lapack_int const* ldvsr,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgges3(...) LAPACK_zgges3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zgges3(...) LAPACK_zgges3_base(__VA_ARGS__)
#endif

#define LAPACK_cggesx_base LAPACK_GLOBAL(cggesx,CGGESX)
void LAPACK_cggesx_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_C_SELECT2 selctg, char const* sense,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb, lapack_int* sdim,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* VSL, lapack_int const* ldvsl,
    lapack_complex_float* VSR, lapack_int const* ldvsr,
    float* rconde,
    float* rcondv,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork, lapack_int const* liwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggesx(...) LAPACK_cggesx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_cggesx(...) LAPACK_cggesx_base(__VA_ARGS__)
#endif

#define LAPACK_dggesx_base LAPACK_GLOBAL(dggesx,DGGESX)
void LAPACK_dggesx_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_D_SELECT3 selctg, char const* sense,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb, lapack_int* sdim,
    double* alphar,
    double* alphai,
    double* beta,
    double* VSL, lapack_int const* ldvsl,
    double* VSR, lapack_int const* ldvsr,
    double* rconde,
    double* rcondv,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggesx(...) LAPACK_dggesx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_dggesx(...) LAPACK_dggesx_base(__VA_ARGS__)
#endif

#define LAPACK_sggesx_base LAPACK_GLOBAL(sggesx,SGGESX)
void LAPACK_sggesx_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_S_SELECT3 selctg, char const* sense,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb, lapack_int* sdim,
    float* alphar,
    float* alphai,
    float* beta,
    float* VSL, lapack_int const* ldvsl,
    float* VSR, lapack_int const* ldvsr,
    float* rconde,
    float* rcondv,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggesx(...) LAPACK_sggesx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_sggesx(...) LAPACK_sggesx_base(__VA_ARGS__)
#endif

#define LAPACK_zggesx_base LAPACK_GLOBAL(zggesx,ZGGESX)
void LAPACK_zggesx_base(
    char const* jobvsl, char const* jobvsr, char const* sort, LAPACK_Z_SELECT2 selctg, char const* sense,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb, lapack_int* sdim,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* VSL, lapack_int const* ldvsl,
    lapack_complex_double* VSR, lapack_int const* ldvsr,
    double* rconde,
    double* rcondv,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork, lapack_int const* liwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggesx(...) LAPACK_zggesx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_zggesx(...) LAPACK_zggesx_base(__VA_ARGS__)
#endif

#define LAPACK_cggev_base LAPACK_GLOBAL(cggev,CGGEV)
void LAPACK_cggev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggev(...) LAPACK_cggev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cggev(...) LAPACK_cggev_base(__VA_ARGS__)
#endif

#define LAPACK_dggev_base LAPACK_GLOBAL(dggev,DGGEV)
void LAPACK_dggev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* alphar,
    double* alphai,
    double* beta,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggev(...) LAPACK_dggev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dggev(...) LAPACK_dggev_base(__VA_ARGS__)
#endif

#define LAPACK_sggev_base LAPACK_GLOBAL(sggev,SGGEV)
void LAPACK_sggev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* alphar,
    float* alphai,
    float* beta,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggev(...) LAPACK_sggev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sggev(...) LAPACK_sggev_base(__VA_ARGS__)
#endif

#define LAPACK_zggev_base LAPACK_GLOBAL(zggev,ZGGEV)
void LAPACK_zggev_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggev(...) LAPACK_zggev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zggev(...) LAPACK_zggev_base(__VA_ARGS__)
#endif

#define LAPACK_cggev3_base LAPACK_GLOBAL(cggev3,CGGEV3)
void LAPACK_cggev3_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggev3(...) LAPACK_cggev3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cggev3(...) LAPACK_cggev3_base(__VA_ARGS__)
#endif

#define LAPACK_dggev3_base LAPACK_GLOBAL(dggev3,DGGEV3)
void LAPACK_dggev3_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* alphar,
    double* alphai,
    double* beta,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggev3(...) LAPACK_dggev3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dggev3(...) LAPACK_dggev3_base(__VA_ARGS__)
#endif

#define LAPACK_sggev3_base LAPACK_GLOBAL(sggev3,SGGEV3)
void LAPACK_sggev3_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* alphar,
    float* alphai,
    float* beta,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggev3(...) LAPACK_sggev3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sggev3(...) LAPACK_sggev3_base(__VA_ARGS__)
#endif

#define LAPACK_zggev3_base LAPACK_GLOBAL(zggev3,ZGGEV3)
void LAPACK_zggev3_base(
    char const* jobvl, char const* jobvr,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggev3(...) LAPACK_zggev3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zggev3(...) LAPACK_zggev3_base(__VA_ARGS__)
#endif

#define LAPACK_cggevx_base LAPACK_GLOBAL(cggevx,CGGEVX)
void LAPACK_cggevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    float* lscale,
    float* rscale,
    float* abnrm,
    float* bbnrm,
    float* rconde,
    float* rcondv,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggevx(...) LAPACK_cggevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_cggevx(...) LAPACK_cggevx_base(__VA_ARGS__)
#endif

#define LAPACK_dggevx_base LAPACK_GLOBAL(dggevx,DGGEVX)
void LAPACK_dggevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* alphar,
    double* alphai,
    double* beta,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    double* lscale,
    double* rscale,
    double* abnrm,
    double* bbnrm,
    double* rconde,
    double* rcondv,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggevx(...) LAPACK_dggevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_dggevx(...) LAPACK_dggevx_base(__VA_ARGS__)
#endif

#define LAPACK_sggevx_base LAPACK_GLOBAL(sggevx,SGGEVX)
void LAPACK_sggevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* alphar,
    float* alphai,
    float* beta,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    float* lscale,
    float* rscale,
    float* abnrm,
    float* bbnrm,
    float* rconde,
    float* rcondv,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggevx(...) LAPACK_sggevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_sggevx(...) LAPACK_sggevx_base(__VA_ARGS__)
#endif

#define LAPACK_zggevx_base LAPACK_GLOBAL(zggevx,ZGGEVX)
void LAPACK_zggevx_base(
    char const* balanc, char const* jobvl, char const* jobvr, char const* sense,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr, lapack_int* ilo, lapack_int* ihi,
    double* lscale,
    double* rscale,
    double* abnrm,
    double* bbnrm,
    double* rconde,
    double* rcondv,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork, lapack_logical* BWORK,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggevx(...) LAPACK_zggevx_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_zggevx(...) LAPACK_zggevx_base(__VA_ARGS__)
#endif

#define LAPACK_cggglm LAPACK_GLOBAL(cggglm,CGGGLM)
void LAPACK_cggglm(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* D,
    lapack_complex_float* X,
    lapack_complex_float* Y,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dggglm LAPACK_GLOBAL(dggglm,DGGGLM)
void LAPACK_dggglm(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* D,
    double* X,
    double* Y,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sggglm LAPACK_GLOBAL(sggglm,SGGGLM)
void LAPACK_sggglm(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* D,
    float* X,
    float* Y,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zggglm LAPACK_GLOBAL(zggglm,ZGGGLM)
void LAPACK_zggglm(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* D,
    lapack_complex_double* X,
    lapack_complex_double* Y,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cgghd3_base LAPACK_GLOBAL(cgghd3,CGGHD3)
void LAPACK_cgghd3_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgghd3(...) LAPACK_cgghd3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgghd3(...) LAPACK_cgghd3_base(__VA_ARGS__)
#endif

#define LAPACK_dgghd3_base LAPACK_GLOBAL(dgghd3,DGGHD3)
void LAPACK_dgghd3_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* Q, lapack_int const* ldq,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgghd3(...) LAPACK_dgghd3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgghd3(...) LAPACK_dgghd3_base(__VA_ARGS__)
#endif

#define LAPACK_sgghd3_base LAPACK_GLOBAL(sgghd3,SGGHD3)
void LAPACK_sgghd3_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* Q, lapack_int const* ldq,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgghd3(...) LAPACK_sgghd3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgghd3(...) LAPACK_sgghd3_base(__VA_ARGS__)
#endif

#define LAPACK_zgghd3_base LAPACK_GLOBAL(zgghd3,ZGGHD3)
void LAPACK_zgghd3_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgghd3(...) LAPACK_zgghd3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgghd3(...) LAPACK_zgghd3_base(__VA_ARGS__)
#endif

#define LAPACK_cgghrd_base LAPACK_GLOBAL(cgghrd,CGGHRD)
void LAPACK_cgghrd_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgghrd(...) LAPACK_cgghrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgghrd(...) LAPACK_cgghrd_base(__VA_ARGS__)
#endif

#define LAPACK_dgghrd_base LAPACK_GLOBAL(dgghrd,DGGHRD)
void LAPACK_dgghrd_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* Q, lapack_int const* ldq,
    double* Z, lapack_int const* ldz,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgghrd(...) LAPACK_dgghrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgghrd(...) LAPACK_dgghrd_base(__VA_ARGS__)
#endif

#define LAPACK_sgghrd_base LAPACK_GLOBAL(sgghrd,SGGHRD)
void LAPACK_sgghrd_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* Q, lapack_int const* ldq,
    float* Z, lapack_int const* ldz,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgghrd(...) LAPACK_sgghrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgghrd(...) LAPACK_sgghrd_base(__VA_ARGS__)
#endif

#define LAPACK_zgghrd_base LAPACK_GLOBAL(zgghrd,ZGGHRD)
void LAPACK_zgghrd_base(
    char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgghrd(...) LAPACK_zgghrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgghrd(...) LAPACK_zgghrd_base(__VA_ARGS__)
#endif

#define LAPACK_cgglse LAPACK_GLOBAL(cgglse,CGGLSE)
void LAPACK_cgglse(
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* C,
    lapack_complex_float* D,
    lapack_complex_float* X,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dgglse LAPACK_GLOBAL(dgglse,DGGLSE)
void LAPACK_dgglse(
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* C,
    double* D,
    double* X,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sgglse LAPACK_GLOBAL(sgglse,SGGLSE)
void LAPACK_sgglse(
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* C,
    float* D,
    float* X,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zgglse LAPACK_GLOBAL(zgglse,ZGGLSE)
void LAPACK_zgglse(
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* C,
    lapack_complex_double* D,
    lapack_complex_double* X,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cggqrf LAPACK_GLOBAL(cggqrf,CGGQRF)
void LAPACK_cggqrf(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* taua,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* taub,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dggqrf LAPACK_GLOBAL(dggqrf,DGGQRF)
void LAPACK_dggqrf(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    double* A, lapack_int const* lda,
    double* taua,
    double* B, lapack_int const* ldb,
    double* taub,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sggqrf LAPACK_GLOBAL(sggqrf,SGGQRF)
void LAPACK_sggqrf(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    float* A, lapack_int const* lda,
    float* taua,
    float* B, lapack_int const* ldb,
    float* taub,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zggqrf LAPACK_GLOBAL(zggqrf,ZGGQRF)
void LAPACK_zggqrf(
    lapack_int const* n, lapack_int const* m, lapack_int const* p,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* taua,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* taub,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cggrqf LAPACK_GLOBAL(cggrqf,CGGRQF)
void LAPACK_cggrqf(
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* taua,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* taub,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dggrqf LAPACK_GLOBAL(dggrqf,DGGRQF)
void LAPACK_dggrqf(
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* taua,
    double* B, lapack_int const* ldb,
    double* taub,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sggrqf LAPACK_GLOBAL(sggrqf,SGGRQF)
void LAPACK_sggrqf(
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* taua,
    float* B, lapack_int const* ldb,
    float* taub,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zggrqf LAPACK_GLOBAL(zggrqf,ZGGRQF)
void LAPACK_zggrqf(
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* taua,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* taub,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cggsvd_base LAPACK_GLOBAL(cggsvd,CGGSVD)
void LAPACK_cggsvd_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    lapack_int* k, lapack_int* l,
    lapack_complex_float* a, lapack_int const* lda,
    lapack_complex_float* b, lapack_int const* ldb,
    float* alpha, float* beta,
    lapack_complex_float* u, lapack_int const* ldu,
    lapack_complex_float* v, lapack_int const* ldv,
    lapack_complex_float* q, lapack_int const* ldq,
    lapack_complex_float* work, float* rwork,
    lapack_int* iwork, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggsvd(...) LAPACK_cggsvd_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cggsvd(...) LAPACK_cggsvd_base(__VA_ARGS__)
#endif

#define LAPACK_sggsvd_base LAPACK_GLOBAL(sggsvd,SGGSVD)
void LAPACK_sggsvd_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    lapack_int* k, lapack_int* l,
    float* a, lapack_int const* lda,
    float* b, lapack_int const* ldb,
    float* alpha, float* beta,
    float* u, lapack_int const* ldu,
    float* v, lapack_int const* ldv,
    float* q, lapack_int const* ldq,
    float* work, lapack_int* iwork, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggsvd(...) LAPACK_sggsvd_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sggsvd(...) LAPACK_sggsvd_base(__VA_ARGS__)
#endif

#define LAPACK_dggsvd_base LAPACK_GLOBAL(dggsvd,DGGSVD)
void LAPACK_dggsvd_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    lapack_int* k, lapack_int* l,
    double* a, lapack_int const* lda,
    double* b, lapack_int const* ldb,
    double* alpha, double* beta,
    double* u, lapack_int const* ldu,
    double* v, lapack_int const* ldv,
    double* q, lapack_int const* ldq,
    double* work, lapack_int* iwork, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggsvd(...) LAPACK_dggsvd_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dggsvd(...) LAPACK_dggsvd_base(__VA_ARGS__)
#endif

#define LAPACK_zggsvd_base LAPACK_GLOBAL(zggsvd,ZGGSVD)
void LAPACK_zggsvd_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p,
    lapack_int* k, lapack_int* l,
    lapack_complex_double* a, lapack_int const* lda,
    lapack_complex_double* b, lapack_int const* ldb,
    double* alpha, double* beta,
    lapack_complex_double* u, lapack_int const* ldu,
    lapack_complex_double* v, lapack_int const* ldv,
    lapack_complex_double* q, lapack_int const* ldq,
    lapack_complex_double* work, double* rwork,
    lapack_int* iwork, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggsvd(...) LAPACK_zggsvd_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zggsvd(...) LAPACK_zggsvd_base(__VA_ARGS__)
#endif

#define LAPACK_cggsvd3_base LAPACK_GLOBAL(cggsvd3,CGGSVD3)
void LAPACK_cggsvd3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p, lapack_int* k, lapack_int* l,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float* alpha,
    float* beta,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* V, lapack_int const* ldv,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggsvd3(...) LAPACK_cggsvd3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cggsvd3(...) LAPACK_cggsvd3_base(__VA_ARGS__)
#endif

#define LAPACK_dggsvd3_base LAPACK_GLOBAL(dggsvd3,DGGSVD3)
void LAPACK_dggsvd3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p, lapack_int* k, lapack_int* l,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* alpha,
    double* beta,
    double* U, lapack_int const* ldu,
    double* V, lapack_int const* ldv,
    double* Q, lapack_int const* ldq,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggsvd3(...) LAPACK_dggsvd3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dggsvd3(...) LAPACK_dggsvd3_base(__VA_ARGS__)
#endif

#define LAPACK_sggsvd3_base LAPACK_GLOBAL(sggsvd3,SGGSVD3)
void LAPACK_sggsvd3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p, lapack_int* k, lapack_int* l,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* alpha,
    float* beta,
    float* U, lapack_int const* ldu,
    float* V, lapack_int const* ldv,
    float* Q, lapack_int const* ldq,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggsvd3(...) LAPACK_sggsvd3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sggsvd3(...) LAPACK_sggsvd3_base(__VA_ARGS__)
#endif

#define LAPACK_zggsvd3_base LAPACK_GLOBAL(zggsvd3,ZGGSVD3)
void LAPACK_zggsvd3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* n, lapack_int const* p, lapack_int* k, lapack_int* l,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double* alpha,
    double* beta,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* V, lapack_int const* ldv,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggsvd3(...) LAPACK_zggsvd3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zggsvd3(...) LAPACK_zggsvd3_base(__VA_ARGS__)
#endif

#define LAPACK_sggsvp_base LAPACK_GLOBAL(sggsvp,SGGSVP)
void LAPACK_sggsvp_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    float* a, lapack_int const* lda,
    float* b, lapack_int const* ldb,
    float* tola, float* tolb,
    lapack_int* k, lapack_int* l,
    float* u, lapack_int const* ldu,
    float* v, lapack_int const* ldv,
    float* q, lapack_int const* ldq,
    lapack_int* iwork, float* tau,
    float* work, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggsvp(...) LAPACK_sggsvp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sggsvp(...) LAPACK_sggsvp_base(__VA_ARGS__)
#endif

#define LAPACK_dggsvp_base LAPACK_GLOBAL(dggsvp,DGGSVP)
void LAPACK_dggsvp_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    double* a, lapack_int const* lda,
    double* b, lapack_int const* ldb,
    double* tola, double* tolb,
    lapack_int* k, lapack_int* l,
    double* u, lapack_int const* ldu,
    double* v, lapack_int const* ldv,
    double* q, lapack_int const* ldq,
    lapack_int* iwork, double* tau,
    double* work, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggsvp(...) LAPACK_dggsvp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dggsvp(...) LAPACK_dggsvp_base(__VA_ARGS__)
#endif

#define LAPACK_cggsvp_base LAPACK_GLOBAL(cggsvp,CGGSVP)
void LAPACK_cggsvp_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    lapack_complex_float* a, lapack_int const* lda,
    lapack_complex_float* b, lapack_int const* ldb,
    float* tola, float* tolb, lapack_int* k, lapack_int* l,
    lapack_complex_float* u, lapack_int const* ldu,
    lapack_complex_float* v, lapack_int const* ldv,
    lapack_complex_float* q, lapack_int const* ldq,
    lapack_int* iwork, float* rwork, lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggsvp(...) LAPACK_cggsvp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cggsvp(...) LAPACK_cggsvp_base(__VA_ARGS__)
#endif

#define LAPACK_zggsvp_base LAPACK_GLOBAL(zggsvp,ZGGSVP)
void LAPACK_zggsvp_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    lapack_complex_double* a, lapack_int const* lda,
    lapack_complex_double* b, lapack_int const* ldb,
    double* tola, double* tolb, lapack_int* k, lapack_int* l,
    lapack_complex_double* u, lapack_int const* ldu,
    lapack_complex_double* v, lapack_int const* ldv,
    lapack_complex_double* q, lapack_int const* ldq,
    lapack_int* iwork, double* rwork, lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggsvp(...) LAPACK_zggsvp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zggsvp(...) LAPACK_zggsvp_base(__VA_ARGS__)
#endif

#define LAPACK_cggsvp3_base LAPACK_GLOBAL(cggsvp3,CGGSVP3)
void LAPACK_cggsvp3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float const* tola,
    float const* tolb, lapack_int* k, lapack_int* l,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* V, lapack_int const* ldv,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_int* iwork,
    float* rwork,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cggsvp3(...) LAPACK_cggsvp3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cggsvp3(...) LAPACK_cggsvp3_base(__VA_ARGS__)
#endif

#define LAPACK_dggsvp3_base LAPACK_GLOBAL(dggsvp3,DGGSVP3)
void LAPACK_dggsvp3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double const* tola,
    double const* tolb, lapack_int* k, lapack_int* l,
    double* U, lapack_int const* ldu,
    double* V, lapack_int const* ldv,
    double* Q, lapack_int const* ldq,
    lapack_int* iwork,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dggsvp3(...) LAPACK_dggsvp3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dggsvp3(...) LAPACK_dggsvp3_base(__VA_ARGS__)
#endif

#define LAPACK_sggsvp3_base LAPACK_GLOBAL(sggsvp3,SGGSVP3)
void LAPACK_sggsvp3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float const* tola,
    float const* tolb, lapack_int* k, lapack_int* l,
    float* U, lapack_int const* ldu,
    float* V, lapack_int const* ldv,
    float* Q, lapack_int const* ldq,
    lapack_int* iwork,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sggsvp3(...) LAPACK_sggsvp3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sggsvp3(...) LAPACK_sggsvp3_base(__VA_ARGS__)
#endif

#define LAPACK_zggsvp3_base LAPACK_GLOBAL(zggsvp3,ZGGSVP3)
void LAPACK_zggsvp3_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double const* tola,
    double const* tolb, lapack_int* k, lapack_int* l,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* V, lapack_int const* ldv,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_int* iwork,
    double* rwork,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zggsvp3(...) LAPACK_zggsvp3_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zggsvp3(...) LAPACK_zggsvp3_base(__VA_ARGS__)
#endif

#define LAPACK_cgtcon_base LAPACK_GLOBAL(cgtcon,CGTCON)
void LAPACK_cgtcon_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_float const* DL,
    lapack_complex_float const* D,
    lapack_complex_float const* DU,
    lapack_complex_float const* DU2, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgtcon(...) LAPACK_cgtcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgtcon(...) LAPACK_cgtcon_base(__VA_ARGS__)
#endif

#define LAPACK_dgtcon_base LAPACK_GLOBAL(dgtcon,DGTCON)
void LAPACK_dgtcon_base(
    char const* norm,
    lapack_int const* n,
    double const* DL,
    double const* D,
    double const* DU,
    double const* DU2, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgtcon(...) LAPACK_dgtcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgtcon(...) LAPACK_dgtcon_base(__VA_ARGS__)
#endif

#define LAPACK_sgtcon_base LAPACK_GLOBAL(sgtcon,SGTCON)
void LAPACK_sgtcon_base(
    char const* norm,
    lapack_int const* n,
    float const* DL,
    float const* D,
    float const* DU,
    float const* DU2, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgtcon(...) LAPACK_sgtcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgtcon(...) LAPACK_sgtcon_base(__VA_ARGS__)
#endif

#define LAPACK_zgtcon_base LAPACK_GLOBAL(zgtcon,ZGTCON)
void LAPACK_zgtcon_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_double const* DL,
    lapack_complex_double const* D,
    lapack_complex_double const* DU,
    lapack_complex_double const* DU2, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgtcon(...) LAPACK_zgtcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgtcon(...) LAPACK_zgtcon_base(__VA_ARGS__)
#endif

#define LAPACK_cgtrfs_base LAPACK_GLOBAL(cgtrfs,CGTRFS)
void LAPACK_cgtrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* DL,
    lapack_complex_float const* D,
    lapack_complex_float const* DU,
    lapack_complex_float const* DLF,
    lapack_complex_float const* DF,
    lapack_complex_float const* DUF,
    lapack_complex_float const* DU2, lapack_int const* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgtrfs(...) LAPACK_cgtrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgtrfs(...) LAPACK_cgtrfs_base(__VA_ARGS__)
#endif

#define LAPACK_dgtrfs_base LAPACK_GLOBAL(dgtrfs,DGTRFS)
void LAPACK_dgtrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    double const* DL,
    double const* D,
    double const* DU,
    double const* DLF,
    double const* DF,
    double const* DUF,
    double const* DU2, lapack_int const* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgtrfs(...) LAPACK_dgtrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgtrfs(...) LAPACK_dgtrfs_base(__VA_ARGS__)
#endif

#define LAPACK_sgtrfs_base LAPACK_GLOBAL(sgtrfs,SGTRFS)
void LAPACK_sgtrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    float const* DL,
    float const* D,
    float const* DU,
    float const* DLF,
    float const* DF,
    float const* DUF,
    float const* DU2, lapack_int const* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgtrfs(...) LAPACK_sgtrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgtrfs(...) LAPACK_sgtrfs_base(__VA_ARGS__)
#endif

#define LAPACK_zgtrfs_base LAPACK_GLOBAL(zgtrfs,ZGTRFS)
void LAPACK_zgtrfs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* DL,
    lapack_complex_double const* D,
    lapack_complex_double const* DU,
    lapack_complex_double const* DLF,
    lapack_complex_double const* DF,
    lapack_complex_double const* DUF,
    lapack_complex_double const* DU2, lapack_int const* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgtrfs(...) LAPACK_zgtrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgtrfs(...) LAPACK_zgtrfs_base(__VA_ARGS__)
#endif

#define LAPACK_cgtsv LAPACK_GLOBAL(cgtsv,CGTSV)
void LAPACK_cgtsv(
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* DL,
    lapack_complex_float* D,
    lapack_complex_float* DU,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_dgtsv LAPACK_GLOBAL(dgtsv,DGTSV)
void LAPACK_dgtsv(
    lapack_int const* n, lapack_int const* nrhs,
    double* DL,
    double* D,
    double* DU,
    double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_sgtsv LAPACK_GLOBAL(sgtsv,SGTSV)
void LAPACK_sgtsv(
    lapack_int const* n, lapack_int const* nrhs,
    float* DL,
    float* D,
    float* DU,
    float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_zgtsv LAPACK_GLOBAL(zgtsv,ZGTSV)
void LAPACK_zgtsv(
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* DL,
    lapack_complex_double* D,
    lapack_complex_double* DU,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_cgtsvx_base LAPACK_GLOBAL(cgtsvx,CGTSVX)
void LAPACK_cgtsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* DL,
    lapack_complex_float const* D,
    lapack_complex_float const* DU,
    lapack_complex_float* DLF,
    lapack_complex_float* DF,
    lapack_complex_float* DUF,
    lapack_complex_float* DU2, lapack_int* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgtsvx(...) LAPACK_cgtsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cgtsvx(...) LAPACK_cgtsvx_base(__VA_ARGS__)
#endif

#define LAPACK_dgtsvx_base LAPACK_GLOBAL(dgtsvx,DGTSVX)
void LAPACK_dgtsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    double const* DL,
    double const* D,
    double const* DU,
    double* DLF,
    double* DF,
    double* DUF,
    double* DU2, lapack_int* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgtsvx(...) LAPACK_dgtsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dgtsvx(...) LAPACK_dgtsvx_base(__VA_ARGS__)
#endif

#define LAPACK_sgtsvx_base LAPACK_GLOBAL(sgtsvx,SGTSVX)
void LAPACK_sgtsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    float const* DL,
    float const* D,
    float const* DU,
    float* DLF,
    float* DF,
    float* DUF,
    float* DU2, lapack_int* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgtsvx(...) LAPACK_sgtsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sgtsvx(...) LAPACK_sgtsvx_base(__VA_ARGS__)
#endif

#define LAPACK_zgtsvx_base LAPACK_GLOBAL(zgtsvx,ZGTSVX)
void LAPACK_zgtsvx_base(
    char const* fact, char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* DL,
    lapack_complex_double const* D,
    lapack_complex_double const* DU,
    lapack_complex_double* DLF,
    lapack_complex_double* DF,
    lapack_complex_double* DUF,
    lapack_complex_double* DU2, lapack_int* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgtsvx(...) LAPACK_zgtsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zgtsvx(...) LAPACK_zgtsvx_base(__VA_ARGS__)
#endif

#define LAPACK_cgttrf LAPACK_GLOBAL(cgttrf,CGTTRF)
void LAPACK_cgttrf(
    lapack_int const* n,
    lapack_complex_float* DL,
    lapack_complex_float* D,
    lapack_complex_float* DU,
    lapack_complex_float* DU2, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_dgttrf LAPACK_GLOBAL(dgttrf,DGTTRF)
void LAPACK_dgttrf(
    lapack_int const* n,
    double* DL,
    double* D,
    double* DU,
    double* DU2, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_sgttrf LAPACK_GLOBAL(sgttrf,SGTTRF)
void LAPACK_sgttrf(
    lapack_int const* n,
    float* DL,
    float* D,
    float* DU,
    float* DU2, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_zgttrf LAPACK_GLOBAL(zgttrf,ZGTTRF)
void LAPACK_zgttrf(
    lapack_int const* n,
    lapack_complex_double* DL,
    lapack_complex_double* D,
    lapack_complex_double* DU,
    lapack_complex_double* DU2, lapack_int* ipiv,
    lapack_int* info );

#define LAPACK_cgttrs_base LAPACK_GLOBAL(cgttrs,CGTTRS)
void LAPACK_cgttrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* DL,
    lapack_complex_float const* D,
    lapack_complex_float const* DU,
    lapack_complex_float const* DU2, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cgttrs(...) LAPACK_cgttrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cgttrs(...) LAPACK_cgttrs_base(__VA_ARGS__)
#endif

#define LAPACK_dgttrs_base LAPACK_GLOBAL(dgttrs,DGTTRS)
void LAPACK_dgttrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    double const* DL,
    double const* D,
    double const* DU,
    double const* DU2, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dgttrs(...) LAPACK_dgttrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dgttrs(...) LAPACK_dgttrs_base(__VA_ARGS__)
#endif

#define LAPACK_sgttrs_base LAPACK_GLOBAL(sgttrs,SGTTRS)
void LAPACK_sgttrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    float const* DL,
    float const* D,
    float const* DU,
    float const* DU2, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sgttrs(...) LAPACK_sgttrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sgttrs(...) LAPACK_sgttrs_base(__VA_ARGS__)
#endif

#define LAPACK_zgttrs_base LAPACK_GLOBAL(zgttrs,ZGTTRS)
void LAPACK_zgttrs_base(
    char const* trans,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* DL,
    lapack_complex_double const* D,
    lapack_complex_double const* DU,
    lapack_complex_double const* DU2, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zgttrs(...) LAPACK_zgttrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zgttrs(...) LAPACK_zgttrs_base(__VA_ARGS__)
#endif

#define LAPACK_chbev_base LAPACK_GLOBAL(chbev,CHBEV)
void LAPACK_chbev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbev(...) LAPACK_chbev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbev(...) LAPACK_chbev_base(__VA_ARGS__)
#endif

#define LAPACK_zhbev_base LAPACK_GLOBAL(zhbev,ZHBEV)
void LAPACK_zhbev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbev(...) LAPACK_zhbev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbev(...) LAPACK_zhbev_base(__VA_ARGS__)
#endif

#define LAPACK_chbev_2stage_base LAPACK_GLOBAL(chbev_2stage,CHBEV_2STAGE)
void LAPACK_chbev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbev_2stage(...) LAPACK_chbev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbev_2stage(...) LAPACK_chbev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhbev_2stage_base LAPACK_GLOBAL(zhbev_2stage,ZHBEV_2STAGE)
void LAPACK_zhbev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbev_2stage(...) LAPACK_zhbev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbev_2stage(...) LAPACK_zhbev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chbevd_base LAPACK_GLOBAL(chbevd,CHBEVD)
void LAPACK_chbevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbevd(...) LAPACK_chbevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbevd(...) LAPACK_chbevd_base(__VA_ARGS__)
#endif

#define LAPACK_zhbevd_base LAPACK_GLOBAL(zhbevd,ZHBEVD)
void LAPACK_zhbevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbevd(...) LAPACK_zhbevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbevd(...) LAPACK_zhbevd_base(__VA_ARGS__)
#endif

#define LAPACK_chbevd_2stage_base LAPACK_GLOBAL(chbevd_2stage,CHBEVD_2STAGE)
void LAPACK_chbevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbevd_2stage(...) LAPACK_chbevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbevd_2stage(...) LAPACK_chbevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhbevd_2stage_base LAPACK_GLOBAL(zhbevd_2stage,ZHBEVD_2STAGE)
void LAPACK_zhbevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbevd_2stage(...) LAPACK_zhbevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbevd_2stage(...) LAPACK_zhbevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chbevx_base LAPACK_GLOBAL(chbevx,CHBEVX)
void LAPACK_chbevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* Q, lapack_int const* ldq,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbevx(...) LAPACK_chbevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chbevx(...) LAPACK_chbevx_base(__VA_ARGS__)
#endif

#define LAPACK_zhbevx_base LAPACK_GLOBAL(zhbevx,ZHBEVX)
void LAPACK_zhbevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* Q, lapack_int const* ldq,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbevx(...) LAPACK_zhbevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhbevx(...) LAPACK_zhbevx_base(__VA_ARGS__)
#endif

#define LAPACK_chbevx_2stage_base LAPACK_GLOBAL(chbevx_2stage,CHBEVX_2STAGE)
void LAPACK_chbevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* Q, lapack_int const* ldq,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbevx_2stage(...) LAPACK_chbevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chbevx_2stage(...) LAPACK_chbevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhbevx_2stage_base LAPACK_GLOBAL(zhbevx_2stage,ZHBEVX_2STAGE)
void LAPACK_zhbevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* Q, lapack_int const* ldq,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbevx_2stage(...) LAPACK_zhbevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhbevx_2stage(...) LAPACK_zhbevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chbgst_base LAPACK_GLOBAL(chbgst,CHBGST)
void LAPACK_chbgst_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float const* BB, lapack_int const* ldbb,
    lapack_complex_float* X, lapack_int const* ldx,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbgst(...) LAPACK_chbgst_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbgst(...) LAPACK_chbgst_base(__VA_ARGS__)
#endif

#define LAPACK_zhbgst_base LAPACK_GLOBAL(zhbgst,ZHBGST)
void LAPACK_zhbgst_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double const* BB, lapack_int const* ldbb,
    lapack_complex_double* X, lapack_int const* ldx,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbgst(...) LAPACK_zhbgst_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbgst(...) LAPACK_zhbgst_base(__VA_ARGS__)
#endif

#define LAPACK_chbgv_base LAPACK_GLOBAL(chbgv,CHBGV)
void LAPACK_chbgv_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* BB, lapack_int const* ldbb,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbgv(...) LAPACK_chbgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbgv(...) LAPACK_chbgv_base(__VA_ARGS__)
#endif

#define LAPACK_zhbgv_base LAPACK_GLOBAL(zhbgv,ZHBGV)
void LAPACK_zhbgv_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* BB, lapack_int const* ldbb,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbgv(...) LAPACK_zhbgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbgv(...) LAPACK_zhbgv_base(__VA_ARGS__)
#endif

#define LAPACK_chbgvd_base LAPACK_GLOBAL(chbgvd,CHBGVD)
void LAPACK_chbgvd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* BB, lapack_int const* ldbb,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbgvd(...) LAPACK_chbgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbgvd(...) LAPACK_chbgvd_base(__VA_ARGS__)
#endif

#define LAPACK_zhbgvd_base LAPACK_GLOBAL(zhbgvd,ZHBGVD)
void LAPACK_zhbgvd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* BB, lapack_int const* ldbb,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbgvd(...) LAPACK_zhbgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbgvd(...) LAPACK_zhbgvd_base(__VA_ARGS__)
#endif

#define LAPACK_chbgvx_base LAPACK_GLOBAL(chbgvx,CHBGVX)
void LAPACK_chbgvx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* BB, lapack_int const* ldbb,
    lapack_complex_float* Q, lapack_int const* ldq,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbgvx(...) LAPACK_chbgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chbgvx(...) LAPACK_chbgvx_base(__VA_ARGS__)
#endif

#define LAPACK_zhbgvx_base LAPACK_GLOBAL(zhbgvx,ZHBGVX)
void LAPACK_zhbgvx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* BB, lapack_int const* ldbb,
    lapack_complex_double* Q, lapack_int const* ldq,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbgvx(...) LAPACK_zhbgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhbgvx(...) LAPACK_zhbgvx_base(__VA_ARGS__)
#endif

#define LAPACK_chbtrd_base LAPACK_GLOBAL(chbtrd,CHBTRD)
void LAPACK_chbtrd_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    float* D,
    float* E,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chbtrd(...) LAPACK_chbtrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chbtrd(...) LAPACK_chbtrd_base(__VA_ARGS__)
#endif

#define LAPACK_zhbtrd_base LAPACK_GLOBAL(zhbtrd,ZHBTRD)
void LAPACK_zhbtrd_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    double* D,
    double* E,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhbtrd(...) LAPACK_zhbtrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhbtrd(...) LAPACK_zhbtrd_base(__VA_ARGS__)
#endif

#define LAPACK_checon_base LAPACK_GLOBAL(checon,CHECON)
void LAPACK_checon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_checon(...) LAPACK_checon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_checon(...) LAPACK_checon_base(__VA_ARGS__)
#endif

#define LAPACK_zhecon_base LAPACK_GLOBAL(zhecon,ZHECON)
void LAPACK_zhecon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhecon(...) LAPACK_zhecon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhecon(...) LAPACK_zhecon_base(__VA_ARGS__)
#endif

#define LAPACK_checon_3_base LAPACK_GLOBAL(checon_3,CHECON_3)
void LAPACK_checon_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* E, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_checon_3(...) LAPACK_checon_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_checon_3(...) LAPACK_checon_3_base(__VA_ARGS__)
#endif

#define LAPACK_zhecon_3_base LAPACK_GLOBAL(zhecon_3,ZHECON_3)
void LAPACK_zhecon_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* E, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhecon_3(...) LAPACK_zhecon_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhecon_3(...) LAPACK_zhecon_3_base(__VA_ARGS__)
#endif

#define LAPACK_cheequb_base LAPACK_GLOBAL(cheequb,CHEEQUB)
void LAPACK_cheequb_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* S,
    float* scond,
    float* amax,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheequb(...) LAPACK_cheequb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cheequb(...) LAPACK_cheequb_base(__VA_ARGS__)
#endif

#define LAPACK_zheequb_base LAPACK_GLOBAL(zheequb,ZHEEQUB)
void LAPACK_zheequb_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* S,
    double* scond,
    double* amax,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheequb(...) LAPACK_zheequb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zheequb(...) LAPACK_zheequb_base(__VA_ARGS__)
#endif

#define LAPACK_cheev_base LAPACK_GLOBAL(cheev,CHEEV)
void LAPACK_cheev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* W,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheev(...) LAPACK_cheev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cheev(...) LAPACK_cheev_base(__VA_ARGS__)
#endif

#define LAPACK_zheev_base LAPACK_GLOBAL(zheev,ZHEEV)
void LAPACK_zheev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* W,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheev(...) LAPACK_zheev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zheev(...) LAPACK_zheev_base(__VA_ARGS__)
#endif

#define LAPACK_cheev_2stage_base LAPACK_GLOBAL(cheev_2stage,CHEEV_2STAGE)
void LAPACK_cheev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* W,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheev_2stage(...) LAPACK_cheev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cheev_2stage(...) LAPACK_cheev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zheev_2stage_base LAPACK_GLOBAL(zheev_2stage,ZHEEV_2STAGE)
void LAPACK_zheev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* W,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheev_2stage(...) LAPACK_zheev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zheev_2stage(...) LAPACK_zheev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_cheevd_base LAPACK_GLOBAL(cheevd,CHEEVD)
void LAPACK_cheevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* W,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheevd(...) LAPACK_cheevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cheevd(...) LAPACK_cheevd_base(__VA_ARGS__)
#endif

#define LAPACK_zheevd_base LAPACK_GLOBAL(zheevd,ZHEEVD)
void LAPACK_zheevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* W,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheevd(...) LAPACK_zheevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zheevd(...) LAPACK_zheevd_base(__VA_ARGS__)
#endif

#define LAPACK_cheevd_2stage_base LAPACK_GLOBAL(cheevd_2stage,CHEEVD_2STAGE)
void LAPACK_cheevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* W,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheevd_2stage(...) LAPACK_cheevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cheevd_2stage(...) LAPACK_cheevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zheevd_2stage_base LAPACK_GLOBAL(zheevd_2stage,ZHEEVD_2STAGE)
void LAPACK_zheevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* W,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheevd_2stage(...) LAPACK_zheevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zheevd_2stage(...) LAPACK_zheevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_cheevr_base LAPACK_GLOBAL(cheevr,CHEEVR)
void LAPACK_cheevr_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheevr(...) LAPACK_cheevr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cheevr(...) LAPACK_cheevr_base(__VA_ARGS__)
#endif

#define LAPACK_zheevr_base LAPACK_GLOBAL(zheevr,ZHEEVR)
void LAPACK_zheevr_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheevr(...) LAPACK_zheevr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zheevr(...) LAPACK_zheevr_base(__VA_ARGS__)
#endif

#define LAPACK_cheevr_2stage_base LAPACK_GLOBAL(cheevr_2stage,CHEEVR_2STAGE)
void LAPACK_cheevr_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheevr_2stage(...) LAPACK_cheevr_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cheevr_2stage(...) LAPACK_cheevr_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zheevr_2stage_base LAPACK_GLOBAL(zheevr_2stage,ZHEEVR_2STAGE)
void LAPACK_zheevr_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheevr_2stage(...) LAPACK_zheevr_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zheevr_2stage(...) LAPACK_zheevr_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_cheevx_base LAPACK_GLOBAL(cheevx,CHEEVX)
void LAPACK_cheevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheevx(...) LAPACK_cheevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cheevx(...) LAPACK_cheevx_base(__VA_ARGS__)
#endif

#define LAPACK_zheevx_base LAPACK_GLOBAL(zheevx,ZHEEVX)
void LAPACK_zheevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheevx(...) LAPACK_zheevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zheevx(...) LAPACK_zheevx_base(__VA_ARGS__)
#endif

#define LAPACK_cheevx_2stage_base LAPACK_GLOBAL(cheevx_2stage,CHEEVX_2STAGE)
void LAPACK_cheevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheevx_2stage(...) LAPACK_cheevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cheevx_2stage(...) LAPACK_cheevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zheevx_2stage_base LAPACK_GLOBAL(zheevx_2stage,ZHEEVX_2STAGE)
void LAPACK_zheevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheevx_2stage(...) LAPACK_zheevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zheevx_2stage(...) LAPACK_zheevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chegst_base LAPACK_GLOBAL(chegst,CHEGST)
void LAPACK_chegst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    const lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chegst(...) LAPACK_chegst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chegst(...) LAPACK_chegst_base(__VA_ARGS__)
#endif

#define LAPACK_zhegst_base LAPACK_GLOBAL(zhegst,ZHEGST)
void LAPACK_zhegst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    const lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhegst(...) LAPACK_zhegst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhegst(...) LAPACK_zhegst_base(__VA_ARGS__)
#endif

#define LAPACK_chegv_base LAPACK_GLOBAL(chegv,CHEGV)
void LAPACK_chegv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float* W,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chegv(...) LAPACK_chegv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chegv(...) LAPACK_chegv_base(__VA_ARGS__)
#endif

#define LAPACK_zhegv_base LAPACK_GLOBAL(zhegv,ZHEGV)
void LAPACK_zhegv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double* W,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhegv(...) LAPACK_zhegv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhegv(...) LAPACK_zhegv_base(__VA_ARGS__)
#endif

#define LAPACK_chegv_2stage_base LAPACK_GLOBAL(chegv_2stage,CHEGV_2STAGE)
void LAPACK_chegv_2stage_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float* W,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chegv_2stage(...) LAPACK_chegv_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chegv_2stage(...) LAPACK_chegv_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhegv_2stage_base LAPACK_GLOBAL(zhegv_2stage,ZHEGV_2STAGE)
void LAPACK_zhegv_2stage_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double* W,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhegv_2stage(...) LAPACK_zhegv_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhegv_2stage(...) LAPACK_zhegv_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chegvd_base LAPACK_GLOBAL(chegvd,CHEGVD)
void LAPACK_chegvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float* W,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chegvd(...) LAPACK_chegvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chegvd(...) LAPACK_chegvd_base(__VA_ARGS__)
#endif

#define LAPACK_zhegvd_base LAPACK_GLOBAL(zhegvd,ZHEGVD)
void LAPACK_zhegvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double* W,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhegvd(...) LAPACK_zhegvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhegvd(...) LAPACK_zhegvd_base(__VA_ARGS__)
#endif

#define LAPACK_chegvx_base LAPACK_GLOBAL(chegvx,CHEGVX)
void LAPACK_chegvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chegvx(...) LAPACK_chegvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chegvx(...) LAPACK_chegvx_base(__VA_ARGS__)
#endif

#define LAPACK_zhegvx_base LAPACK_GLOBAL(zhegvx,ZHEGVX)
void LAPACK_zhegvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhegvx(...) LAPACK_zhegvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhegvx(...) LAPACK_zhegvx_base(__VA_ARGS__)
#endif

#define LAPACK_cherfs_base LAPACK_GLOBAL(cherfs,CHERFS)
void LAPACK_cherfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cherfs(...) LAPACK_cherfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cherfs(...) LAPACK_cherfs_base(__VA_ARGS__)
#endif

#define LAPACK_zherfs_base LAPACK_GLOBAL(zherfs,ZHERFS)
void LAPACK_zherfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zherfs(...) LAPACK_zherfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zherfs(...) LAPACK_zherfs_base(__VA_ARGS__)
#endif

#define LAPACK_cherfsx_base LAPACK_GLOBAL(cherfsx,CHERFSX)
void LAPACK_cherfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    const float* S,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cherfsx(...) LAPACK_cherfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cherfsx(...) LAPACK_cherfsx_base(__VA_ARGS__)
#endif

#define LAPACK_zherfsx_base LAPACK_GLOBAL(zherfsx,ZHERFSX)
void LAPACK_zherfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    const double* S,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zherfsx(...) LAPACK_zherfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zherfsx(...) LAPACK_zherfsx_base(__VA_ARGS__)
#endif

#define LAPACK_chesv_base LAPACK_GLOBAL(chesv,CHESV)
void LAPACK_chesv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chesv(...) LAPACK_chesv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chesv(...) LAPACK_chesv_base(__VA_ARGS__)
#endif

#define LAPACK_zhesv_base LAPACK_GLOBAL(zhesv,ZHESV)
void LAPACK_zhesv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhesv(...) LAPACK_zhesv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhesv(...) LAPACK_zhesv_base(__VA_ARGS__)
#endif

#define LAPACK_chesv_aa_base LAPACK_GLOBAL(chesv_aa,CHESV_AA)
void LAPACK_chesv_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chesv_aa(...) LAPACK_chesv_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chesv_aa(...) LAPACK_chesv_aa_base(__VA_ARGS__)
#endif

#define LAPACK_zhesv_aa_base LAPACK_GLOBAL(zhesv_aa,ZHESV_AA)
void LAPACK_zhesv_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhesv_aa(...) LAPACK_zhesv_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhesv_aa(...) LAPACK_zhesv_aa_base(__VA_ARGS__)
#endif

#define LAPACK_chesv_aa_2stage_base LAPACK_GLOBAL(chesv_aa_2stage,CHESV_AA_2STAGE)
void LAPACK_chesv_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chesv_aa_2stage(...) LAPACK_chesv_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chesv_aa_2stage(...) LAPACK_chesv_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhesv_aa_2stage_base LAPACK_GLOBAL(zhesv_aa_2stage,ZHESV_AA_2STAGE)
void LAPACK_zhesv_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhesv_aa_2stage(...) LAPACK_zhesv_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhesv_aa_2stage(...) LAPACK_zhesv_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chesv_rk_base LAPACK_GLOBAL(chesv_rk,CHESV_RK)
void LAPACK_chesv_rk_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* E, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chesv_rk(...) LAPACK_chesv_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chesv_rk(...) LAPACK_chesv_rk_base(__VA_ARGS__)
#endif

#define LAPACK_zhesv_rk_base LAPACK_GLOBAL(zhesv_rk,ZHESV_RK)
void LAPACK_zhesv_rk_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* E, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhesv_rk(...) LAPACK_zhesv_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhesv_rk(...) LAPACK_zhesv_rk_base(__VA_ARGS__)
#endif

#define LAPACK_chesv_rook_base LAPACK_GLOBAL(chesv_rook,CHESV_ROOK)
void LAPACK_chesv_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chesv_rook(...) LAPACK_chesv_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chesv_rook(...) LAPACK_chesv_rook_base(__VA_ARGS__)
#endif

#define LAPACK_zhesv_rook_base LAPACK_GLOBAL(zhesv_rook,ZHESV_ROOK)
void LAPACK_zhesv_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhesv_rook(...) LAPACK_zhesv_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhesv_rook(...) LAPACK_zhesv_rook_base(__VA_ARGS__)
#endif

#define LAPACK_chesvx_base LAPACK_GLOBAL(chesvx,CHESVX)
void LAPACK_chesvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, lapack_int* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chesvx(...) LAPACK_chesvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chesvx(...) LAPACK_chesvx_base(__VA_ARGS__)
#endif

#define LAPACK_zhesvx_base LAPACK_GLOBAL(zhesvx,ZHESVX)
void LAPACK_zhesvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, lapack_int* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhesvx(...) LAPACK_zhesvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhesvx(...) LAPACK_zhesvx_base(__VA_ARGS__)
#endif

#define LAPACK_chesvxx_base LAPACK_GLOBAL(chesvxx,CHESVXX)
void LAPACK_chesvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    float* S,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chesvxx(...) LAPACK_chesvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chesvxx(...) LAPACK_chesvxx_base(__VA_ARGS__)
#endif

#define LAPACK_zhesvxx_base LAPACK_GLOBAL(zhesvxx,ZHESVXX)
void LAPACK_zhesvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    double* S,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhesvxx(...) LAPACK_zhesvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhesvxx(...) LAPACK_zhesvxx_base(__VA_ARGS__)
#endif

#define LAPACK_cheswapr_base LAPACK_GLOBAL(cheswapr,CHESWAPR)
void LAPACK_cheswapr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* i1, lapack_int const* i2
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cheswapr(...) LAPACK_cheswapr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cheswapr(...) LAPACK_cheswapr_base(__VA_ARGS__)
#endif

#define LAPACK_zheswapr_base LAPACK_GLOBAL(zheswapr,ZHESWAPR)
void LAPACK_zheswapr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* i1, lapack_int const* i2
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zheswapr(...) LAPACK_zheswapr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zheswapr(...) LAPACK_zheswapr_base(__VA_ARGS__)
#endif

#define LAPACK_chetrd_base LAPACK_GLOBAL(chetrd,CHETRD)
void LAPACK_chetrd_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* D,
    float* E,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrd(...) LAPACK_chetrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrd(...) LAPACK_chetrd_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrd_base LAPACK_GLOBAL(zhetrd,ZHETRD)
void LAPACK_zhetrd_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* D,
    double* E,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrd(...) LAPACK_zhetrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrd(...) LAPACK_zhetrd_base(__VA_ARGS__)
#endif

#define LAPACK_chetrd_2stage_base LAPACK_GLOBAL(chetrd_2stage,CHETRD_2STAGE)
void LAPACK_chetrd_2stage_base(
    char const* vect, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    float* D,
    float* E,
    lapack_complex_float* tau,
    lapack_complex_float* HOUS2, lapack_int const* lhous2,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrd_2stage(...) LAPACK_chetrd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chetrd_2stage(...) LAPACK_chetrd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrd_2stage_base LAPACK_GLOBAL(zhetrd_2stage,ZHETRD_2STAGE)
void LAPACK_zhetrd_2stage_base(
    char const* vect, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    double* D,
    double* E,
    lapack_complex_double* tau,
    lapack_complex_double* HOUS2, lapack_int const* lhous2,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrd_2stage(...) LAPACK_zhetrd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhetrd_2stage(...) LAPACK_zhetrd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chetrf_base LAPACK_GLOBAL(chetrf,CHETRF)
void LAPACK_chetrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrf(...) LAPACK_chetrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrf(...) LAPACK_chetrf_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrf_base LAPACK_GLOBAL(zhetrf,ZHETRF)
void LAPACK_zhetrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrf(...) LAPACK_zhetrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrf(...) LAPACK_zhetrf_base(__VA_ARGS__)
#endif

#define LAPACK_chetrf_aa_base LAPACK_GLOBAL(chetrf_aa,CHETRF_AA)
void LAPACK_chetrf_aa_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrf_aa(...) LAPACK_chetrf_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrf_aa(...) LAPACK_chetrf_aa_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrf_aa_base LAPACK_GLOBAL(zhetrf_aa,ZHETRF_AA)
void LAPACK_zhetrf_aa_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrf_aa(...) LAPACK_zhetrf_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrf_aa(...) LAPACK_zhetrf_aa_base(__VA_ARGS__)
#endif

#define LAPACK_chetrf_aa_2stage_base LAPACK_GLOBAL(chetrf_aa_2stage,CHETRF_AA_2STAGE)
void LAPACK_chetrf_aa_2stage_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrf_aa_2stage(...) LAPACK_chetrf_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrf_aa_2stage(...) LAPACK_chetrf_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrf_aa_2stage_base LAPACK_GLOBAL(zhetrf_aa_2stage,ZHETRF_AA_2STAGE)
void LAPACK_zhetrf_aa_2stage_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrf_aa_2stage(...) LAPACK_zhetrf_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrf_aa_2stage(...) LAPACK_zhetrf_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chetrf_rk_base LAPACK_GLOBAL(chetrf_rk,CHETRF_RK)
void LAPACK_chetrf_rk_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* E, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrf_rk(...) LAPACK_chetrf_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrf_rk(...) LAPACK_chetrf_rk_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrf_rk_base LAPACK_GLOBAL(zhetrf_rk,ZHETRF_RK)
void LAPACK_zhetrf_rk_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* E, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrf_rk(...) LAPACK_zhetrf_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrf_rk(...) LAPACK_zhetrf_rk_base(__VA_ARGS__)
#endif

#define LAPACK_chetrf_rook_base LAPACK_GLOBAL(chetrf_rook,CHETRF_ROOK)
void LAPACK_chetrf_rook_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrf_rook(...) LAPACK_chetrf_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrf_rook(...) LAPACK_chetrf_rook_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrf_rook_base LAPACK_GLOBAL(zhetrf_rook,ZHETRF_ROOK)
void LAPACK_zhetrf_rook_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrf_rook(...) LAPACK_zhetrf_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrf_rook(...) LAPACK_zhetrf_rook_base(__VA_ARGS__)
#endif

#define LAPACK_chetri_base LAPACK_GLOBAL(chetri,CHETRI)
void LAPACK_chetri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetri(...) LAPACK_chetri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetri(...) LAPACK_chetri_base(__VA_ARGS__)
#endif

#define LAPACK_zhetri_base LAPACK_GLOBAL(zhetri,ZHETRI)
void LAPACK_zhetri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetri(...) LAPACK_zhetri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetri(...) LAPACK_zhetri_base(__VA_ARGS__)
#endif

#define LAPACK_chetri2_base LAPACK_GLOBAL(chetri2,CHETRI2)
void LAPACK_chetri2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetri2(...) LAPACK_chetri2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetri2(...) LAPACK_chetri2_base(__VA_ARGS__)
#endif

#define LAPACK_zhetri2_base LAPACK_GLOBAL(zhetri2,ZHETRI2)
void LAPACK_zhetri2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetri2(...) LAPACK_zhetri2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetri2(...) LAPACK_zhetri2_base(__VA_ARGS__)
#endif

#define LAPACK_chetri2x_base LAPACK_GLOBAL(chetri2x,CHETRI2X)
void LAPACK_chetri2x_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* work, lapack_int const* nb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetri2x(...) LAPACK_chetri2x_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetri2x(...) LAPACK_chetri2x_base(__VA_ARGS__)
#endif

#define LAPACK_zhetri2x_base LAPACK_GLOBAL(zhetri2x,ZHETRI2X)
void LAPACK_zhetri2x_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* work, lapack_int const* nb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetri2x(...) LAPACK_zhetri2x_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetri2x(...) LAPACK_zhetri2x_base(__VA_ARGS__)
#endif

#define LAPACK_chetri_3_base LAPACK_GLOBAL(chetri_3,CHETRI_3)
void LAPACK_chetri_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* E, lapack_int const* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetri_3(...) LAPACK_chetri_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetri_3(...) LAPACK_chetri_3_base(__VA_ARGS__)
#endif

#define LAPACK_zhetri_3_base LAPACK_GLOBAL(zhetri_3,ZHETRI_3)
void LAPACK_zhetri_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* E, lapack_int const* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetri_3(...) LAPACK_zhetri_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetri_3(...) LAPACK_zhetri_3_base(__VA_ARGS__)
#endif

#define LAPACK_chetrs_base LAPACK_GLOBAL(chetrs,CHETRS)
void LAPACK_chetrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrs(...) LAPACK_chetrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrs(...) LAPACK_chetrs_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrs_base LAPACK_GLOBAL(zhetrs,ZHETRS)
void LAPACK_zhetrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrs(...) LAPACK_zhetrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrs(...) LAPACK_zhetrs_base(__VA_ARGS__)
#endif

#define LAPACK_chetrs2_base LAPACK_GLOBAL(chetrs2,CHETRS2)
void LAPACK_chetrs2_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrs2(...) LAPACK_chetrs2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrs2(...) LAPACK_chetrs2_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrs2_base LAPACK_GLOBAL(zhetrs2,ZHETRS2)
void LAPACK_zhetrs2_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrs2(...) LAPACK_zhetrs2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrs2(...) LAPACK_zhetrs2_base(__VA_ARGS__)
#endif

#define LAPACK_chetrs_3_base LAPACK_GLOBAL(chetrs_3,CHETRS_3)
void LAPACK_chetrs_3_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* E, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrs_3(...) LAPACK_chetrs_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrs_3(...) LAPACK_chetrs_3_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrs_3_base LAPACK_GLOBAL(zhetrs_3,ZHETRS_3)
void LAPACK_zhetrs_3_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* E, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrs_3(...) LAPACK_zhetrs_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrs_3(...) LAPACK_zhetrs_3_base(__VA_ARGS__)
#endif

#define LAPACK_chetrs_aa_base LAPACK_GLOBAL(chetrs_aa,CHETRS_AA)
void LAPACK_chetrs_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrs_aa(...) LAPACK_chetrs_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrs_aa(...) LAPACK_chetrs_aa_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrs_aa_base LAPACK_GLOBAL(zhetrs_aa,ZHETRS_AA)
void LAPACK_zhetrs_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrs_aa(...) LAPACK_zhetrs_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrs_aa(...) LAPACK_zhetrs_aa_base(__VA_ARGS__)
#endif

#define LAPACK_chetrs_aa_2stage_base LAPACK_GLOBAL(chetrs_aa_2stage,CHETRS_AA_2STAGE)
void LAPACK_chetrs_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* TB, lapack_int const* ltb, lapack_int const* ipiv, lapack_int const* ipiv2,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrs_aa_2stage(...) LAPACK_chetrs_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrs_aa_2stage(...) LAPACK_chetrs_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrs_aa_2stage_base LAPACK_GLOBAL(zhetrs_aa_2stage,ZHETRS_AA_2STAGE)
void LAPACK_zhetrs_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* TB, lapack_int const* ltb, lapack_int const* ipiv, lapack_int const* ipiv2,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrs_aa_2stage(...) LAPACK_zhetrs_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrs_aa_2stage(...) LAPACK_zhetrs_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_chetrs_rook_base LAPACK_GLOBAL(chetrs_rook,CHETRS_ROOK)
void LAPACK_chetrs_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chetrs_rook(...) LAPACK_chetrs_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chetrs_rook(...) LAPACK_chetrs_rook_base(__VA_ARGS__)
#endif

#define LAPACK_zhetrs_rook_base LAPACK_GLOBAL(zhetrs_rook,ZHETRS_ROOK)
void LAPACK_zhetrs_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhetrs_rook(...) LAPACK_zhetrs_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhetrs_rook(...) LAPACK_zhetrs_rook_base(__VA_ARGS__)
#endif

#define LAPACK_chfrk_base LAPACK_GLOBAL(chfrk,CHFRK)
void LAPACK_chfrk_base(
    char const* transr, char const* uplo, char const* trans,
    lapack_int const* n, lapack_int const* k,
    float const* alpha,
    lapack_complex_float const* A, lapack_int const* lda,
    float const* beta,
    lapack_complex_float* C
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chfrk(...) LAPACK_chfrk_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chfrk(...) LAPACK_chfrk_base(__VA_ARGS__)
#endif

#define LAPACK_zhfrk_base LAPACK_GLOBAL(zhfrk,ZHFRK)
void LAPACK_zhfrk_base(
    char const* transr, char const* uplo, char const* trans,
    lapack_int const* n, lapack_int const* k,
    double const* alpha,
    lapack_complex_double const* A, lapack_int const* lda,
    double const* beta,
    lapack_complex_double* C
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhfrk(...) LAPACK_zhfrk_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhfrk(...) LAPACK_zhfrk_base(__VA_ARGS__)
#endif

#define LAPACK_chgeqz_base LAPACK_GLOBAL(chgeqz,CHGEQZ)
void LAPACK_chgeqz_base(
    char const* job, char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_float* H, lapack_int const* ldh,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chgeqz(...) LAPACK_chgeqz_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chgeqz(...) LAPACK_chgeqz_base(__VA_ARGS__)
#endif

#define LAPACK_dhgeqz_base LAPACK_GLOBAL(dhgeqz,DHGEQZ)
void LAPACK_dhgeqz_base(
    char const* job, char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double* H, lapack_int const* ldh,
    double* T, lapack_int const* ldt,
    double* alphar,
    double* alphai,
    double* beta,
    double* Q, lapack_int const* ldq,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dhgeqz(...) LAPACK_dhgeqz_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dhgeqz(...) LAPACK_dhgeqz_base(__VA_ARGS__)
#endif

#define LAPACK_shgeqz_base LAPACK_GLOBAL(shgeqz,SHGEQZ)
void LAPACK_shgeqz_base(
    char const* job, char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float* H, lapack_int const* ldh,
    float* T, lapack_int const* ldt,
    float* alphar,
    float* alphai,
    float* beta,
    float* Q, lapack_int const* ldq,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_shgeqz(...) LAPACK_shgeqz_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_shgeqz(...) LAPACK_shgeqz_base(__VA_ARGS__)
#endif

#define LAPACK_zhgeqz_base LAPACK_GLOBAL(zhgeqz,ZHGEQZ)
void LAPACK_zhgeqz_base(
    char const* job, char const* compq, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_double* H, lapack_int const* ldh,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhgeqz(...) LAPACK_zhgeqz_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhgeqz(...) LAPACK_zhgeqz_base(__VA_ARGS__)
#endif

#define LAPACK_chpcon_base LAPACK_GLOBAL(chpcon,CHPCON)
void LAPACK_chpcon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpcon(...) LAPACK_chpcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chpcon(...) LAPACK_chpcon_base(__VA_ARGS__)
#endif

#define LAPACK_zhpcon_base LAPACK_GLOBAL(zhpcon,ZHPCON)
void LAPACK_zhpcon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpcon(...) LAPACK_zhpcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhpcon(...) LAPACK_zhpcon_base(__VA_ARGS__)
#endif

#define LAPACK_chpev_base LAPACK_GLOBAL(chpev,CHPEV)
void LAPACK_chpev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpev(...) LAPACK_chpev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chpev(...) LAPACK_chpev_base(__VA_ARGS__)
#endif

#define LAPACK_zhpev_base LAPACK_GLOBAL(zhpev,ZHPEV)
void LAPACK_zhpev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpev(...) LAPACK_zhpev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhpev(...) LAPACK_zhpev_base(__VA_ARGS__)
#endif

#define LAPACK_chpevd_base LAPACK_GLOBAL(chpevd,CHPEVD)
void LAPACK_chpevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpevd(...) LAPACK_chpevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chpevd(...) LAPACK_chpevd_base(__VA_ARGS__)
#endif

#define LAPACK_zhpevd_base LAPACK_GLOBAL(zhpevd,ZHPEVD)
void LAPACK_zhpevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpevd(...) LAPACK_zhpevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhpevd(...) LAPACK_zhpevd_base(__VA_ARGS__)
#endif

#define LAPACK_chpevx_base LAPACK_GLOBAL(chpevx,CHPEVX)
void LAPACK_chpevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpevx(...) LAPACK_chpevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chpevx(...) LAPACK_chpevx_base(__VA_ARGS__)
#endif

#define LAPACK_zhpevx_base LAPACK_GLOBAL(zhpevx,ZHPEVX)
void LAPACK_zhpevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpevx(...) LAPACK_zhpevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhpevx(...) LAPACK_zhpevx_base(__VA_ARGS__)
#endif

#define LAPACK_chpgst_base LAPACK_GLOBAL(chpgst,CHPGST)
void LAPACK_chpgst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    lapack_complex_float const* BP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpgst(...) LAPACK_chpgst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chpgst(...) LAPACK_chpgst_base(__VA_ARGS__)
#endif

#define LAPACK_zhpgst_base LAPACK_GLOBAL(zhpgst,ZHPGST)
void LAPACK_zhpgst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    lapack_complex_double const* BP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpgst(...) LAPACK_zhpgst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhpgst(...) LAPACK_zhpgst_base(__VA_ARGS__)
#endif

#define LAPACK_chpgv_base LAPACK_GLOBAL(chpgv,CHPGV)
void LAPACK_chpgv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    lapack_complex_float* BP,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpgv(...) LAPACK_chpgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chpgv(...) LAPACK_chpgv_base(__VA_ARGS__)
#endif

#define LAPACK_zhpgv_base LAPACK_GLOBAL(zhpgv,ZHPGV)
void LAPACK_zhpgv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    lapack_complex_double* BP,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpgv(...) LAPACK_zhpgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhpgv(...) LAPACK_zhpgv_base(__VA_ARGS__)
#endif

#define LAPACK_chpgvd_base LAPACK_GLOBAL(chpgvd,CHPGVD)
void LAPACK_chpgvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    lapack_complex_float* BP,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpgvd(...) LAPACK_chpgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chpgvd(...) LAPACK_chpgvd_base(__VA_ARGS__)
#endif

#define LAPACK_zhpgvd_base LAPACK_GLOBAL(zhpgvd,ZHPGVD)
void LAPACK_zhpgvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    lapack_complex_double* BP,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpgvd(...) LAPACK_zhpgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhpgvd(...) LAPACK_zhpgvd_base(__VA_ARGS__)
#endif

#define LAPACK_chpgvx_base LAPACK_GLOBAL(chpgvx,CHPGVX)
void LAPACK_chpgvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    lapack_complex_float* BP,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpgvx(...) LAPACK_chpgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chpgvx(...) LAPACK_chpgvx_base(__VA_ARGS__)
#endif

#define LAPACK_zhpgvx_base LAPACK_GLOBAL(zhpgvx,ZHPGVX)
void LAPACK_zhpgvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    lapack_complex_double* BP,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpgvx(...) LAPACK_zhpgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhpgvx(...) LAPACK_zhpgvx_base(__VA_ARGS__)
#endif

#define LAPACK_chprfs_base LAPACK_GLOBAL(chprfs,CHPRFS)
void LAPACK_chprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float const* AFP, lapack_int const* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chprfs(...) LAPACK_chprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chprfs(...) LAPACK_chprfs_base(__VA_ARGS__)
#endif

#define LAPACK_zhprfs_base LAPACK_GLOBAL(zhprfs,ZHPRFS)
void LAPACK_zhprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double const* AFP, lapack_int const* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhprfs(...) LAPACK_zhprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhprfs(...) LAPACK_zhprfs_base(__VA_ARGS__)
#endif

#define LAPACK_chpsv_base LAPACK_GLOBAL(chpsv,CHPSV)
void LAPACK_chpsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* AP, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpsv(...) LAPACK_chpsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chpsv(...) LAPACK_chpsv_base(__VA_ARGS__)
#endif

#define LAPACK_zhpsv_base LAPACK_GLOBAL(zhpsv,ZHPSV)
void LAPACK_zhpsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* AP, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpsv(...) LAPACK_zhpsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhpsv(...) LAPACK_zhpsv_base(__VA_ARGS__)
#endif

#define LAPACK_chpsvx_base LAPACK_GLOBAL(chpsvx,CHPSVX)
void LAPACK_chpsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float* AFP, lapack_int* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chpsvx(...) LAPACK_chpsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chpsvx(...) LAPACK_chpsvx_base(__VA_ARGS__)
#endif

#define LAPACK_zhpsvx_base LAPACK_GLOBAL(zhpsvx,ZHPSVX)
void LAPACK_zhpsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double* AFP, lapack_int* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhpsvx(...) LAPACK_zhpsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhpsvx(...) LAPACK_zhpsvx_base(__VA_ARGS__)
#endif

#define LAPACK_chptrd_base LAPACK_GLOBAL(chptrd,CHPTRD)
void LAPACK_chptrd_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    float* D,
    float* E,
    lapack_complex_float* tau,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chptrd(...) LAPACK_chptrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chptrd(...) LAPACK_chptrd_base(__VA_ARGS__)
#endif

#define LAPACK_zhptrd_base LAPACK_GLOBAL(zhptrd,ZHPTRD)
void LAPACK_zhptrd_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    double* D,
    double* E,
    lapack_complex_double* tau,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhptrd(...) LAPACK_zhptrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhptrd(...) LAPACK_zhptrd_base(__VA_ARGS__)
#endif

#define LAPACK_chptrf_base LAPACK_GLOBAL(chptrf,CHPTRF)
void LAPACK_chptrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP, lapack_int* ipiv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chptrf(...) LAPACK_chptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chptrf(...) LAPACK_chptrf_base(__VA_ARGS__)
#endif

#define LAPACK_zhptrf_base LAPACK_GLOBAL(zhptrf,ZHPTRF)
void LAPACK_zhptrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP, lapack_int* ipiv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhptrf(...) LAPACK_zhptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhptrf(...) LAPACK_zhptrf_base(__VA_ARGS__)
#endif

#define LAPACK_chptri_base LAPACK_GLOBAL(chptri,CHPTRI)
void LAPACK_chptri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP, lapack_int const* ipiv,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chptri(...) LAPACK_chptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chptri(...) LAPACK_chptri_base(__VA_ARGS__)
#endif

#define LAPACK_zhptri_base LAPACK_GLOBAL(zhptri,ZHPTRI)
void LAPACK_zhptri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP, lapack_int const* ipiv,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhptri(...) LAPACK_zhptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhptri(...) LAPACK_zhptri_base(__VA_ARGS__)
#endif

#define LAPACK_chptrs_base LAPACK_GLOBAL(chptrs,CHPTRS)
void LAPACK_chptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chptrs(...) LAPACK_chptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_chptrs(...) LAPACK_chptrs_base(__VA_ARGS__)
#endif

#define LAPACK_zhptrs_base LAPACK_GLOBAL(zhptrs,ZHPTRS)
void LAPACK_zhptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhptrs(...) LAPACK_zhptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zhptrs(...) LAPACK_zhptrs_base(__VA_ARGS__)
#endif

#define LAPACK_chsein_base LAPACK_GLOBAL(chsein,CHSEIN)
void LAPACK_chsein_base(
    char const* side, char const* eigsrc, char const* initv,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_float const* H, lapack_int const* ldh,
    lapack_complex_float* W,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_float* work,
    float* rwork, lapack_int* IFAILL, lapack_int* IFAILR,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chsein(...) LAPACK_chsein_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_chsein(...) LAPACK_chsein_base(__VA_ARGS__)
#endif

#define LAPACK_dhsein_base LAPACK_GLOBAL(dhsein,DHSEIN)
void LAPACK_dhsein_base(
    char const* side, char const* eigsrc, char const* initv,
    lapack_logical* select,
    lapack_int const* n,
    double const* H, lapack_int const* ldh,
    double* WR,
    double const* WI,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    double* work, lapack_int* IFAILL, lapack_int* IFAILR,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dhsein(...) LAPACK_dhsein_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dhsein(...) LAPACK_dhsein_base(__VA_ARGS__)
#endif

#define LAPACK_shsein_base LAPACK_GLOBAL(shsein,SHSEIN)
void LAPACK_shsein_base(
    char const* side, char const* eigsrc, char const* initv,
    lapack_logical* select,
    lapack_int const* n,
    float const* H, lapack_int const* ldh,
    float* WR,
    float const* WI,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    float* work, lapack_int* IFAILL, lapack_int* IFAILR,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_shsein(...) LAPACK_shsein_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_shsein(...) LAPACK_shsein_base(__VA_ARGS__)
#endif

#define LAPACK_zhsein_base LAPACK_GLOBAL(zhsein,ZHSEIN)
void LAPACK_zhsein_base(
    char const* side, char const* eigsrc, char const* initv,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_double const* H, lapack_int const* ldh,
    lapack_complex_double* W,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_double* work,
    double* rwork, lapack_int* IFAILL, lapack_int* IFAILR,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhsein(...) LAPACK_zhsein_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zhsein(...) LAPACK_zhsein_base(__VA_ARGS__)
#endif

#define LAPACK_chseqr_base LAPACK_GLOBAL(chseqr,CHSEQR)
void LAPACK_chseqr_base(
    char const* job, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_float* H, lapack_int const* ldh,
    lapack_complex_float* W,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_chseqr(...) LAPACK_chseqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_chseqr(...) LAPACK_chseqr_base(__VA_ARGS__)
#endif

#define LAPACK_dhseqr_base LAPACK_GLOBAL(dhseqr,DHSEQR)
void LAPACK_dhseqr_base(
    char const* job, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double* H, lapack_int const* ldh,
    double* WR,
    double* WI,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dhseqr(...) LAPACK_dhseqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dhseqr(...) LAPACK_dhseqr_base(__VA_ARGS__)
#endif

#define LAPACK_shseqr_base LAPACK_GLOBAL(shseqr,SHSEQR)
void LAPACK_shseqr_base(
    char const* job, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float* H, lapack_int const* ldh,
    float* WR,
    float* WI,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_shseqr(...) LAPACK_shseqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_shseqr(...) LAPACK_shseqr_base(__VA_ARGS__)
#endif

#define LAPACK_zhseqr_base LAPACK_GLOBAL(zhseqr,ZHSEQR)
void LAPACK_zhseqr_base(
    char const* job, char const* compz,
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_double* H, lapack_int const* ldh,
    lapack_complex_double* W,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zhseqr(...) LAPACK_zhseqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zhseqr(...) LAPACK_zhseqr_base(__VA_ARGS__)
#endif

#define LAPACK_clacgv LAPACK_GLOBAL(clacgv,CLACGV)
void LAPACK_clacgv(
    lapack_int const* n,
    lapack_complex_float* X, lapack_int const* incx );

#define LAPACK_zlacgv LAPACK_GLOBAL(zlacgv,ZLACGV)
void LAPACK_zlacgv(
    lapack_int const* n,
    lapack_complex_double* X, lapack_int const* incx );

#define LAPACK_clacn2 LAPACK_GLOBAL(clacn2,CLACN2)
void LAPACK_clacn2(
    lapack_int const* n,
    lapack_complex_float* V,
    lapack_complex_float* X,
    float* est, lapack_int* kase, lapack_int* ISAVE );

#define LAPACK_dlacn2 LAPACK_GLOBAL(dlacn2,DLACN2)
void LAPACK_dlacn2(
    lapack_int const* n,
    double* V,
    double* X, lapack_int* ISGN,
    double* est, lapack_int* kase, lapack_int* ISAVE );

#define LAPACK_slacn2 LAPACK_GLOBAL(slacn2,SLACN2)
void LAPACK_slacn2(
    lapack_int const* n,
    float* V,
    float* X, lapack_int* ISGN,
    float* est, lapack_int* kase, lapack_int* ISAVE );

#define LAPACK_zlacn2 LAPACK_GLOBAL(zlacn2,ZLACN2)
void LAPACK_zlacn2(
    lapack_int const* n,
    lapack_complex_double* V,
    lapack_complex_double* X,
    double* est, lapack_int* kase, lapack_int* ISAVE );

#define LAPACK_clacp2_base LAPACK_GLOBAL(clacp2,CLACP2)
void LAPACK_clacp2_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clacp2(...) LAPACK_clacp2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clacp2(...) LAPACK_clacp2_base(__VA_ARGS__)
#endif

#define LAPACK_zlacp2_base LAPACK_GLOBAL(zlacp2,ZLACP2)
void LAPACK_zlacp2_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlacp2(...) LAPACK_zlacp2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlacp2(...) LAPACK_zlacp2_base(__VA_ARGS__)
#endif

#define LAPACK_clacpy_base LAPACK_GLOBAL(clacpy,CLACPY)
void LAPACK_clacpy_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clacpy(...) LAPACK_clacpy_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clacpy(...) LAPACK_clacpy_base(__VA_ARGS__)
#endif

#define LAPACK_dlacpy_base LAPACK_GLOBAL(dlacpy,DLACPY)
void LAPACK_dlacpy_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlacpy(...) LAPACK_dlacpy_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlacpy(...) LAPACK_dlacpy_base(__VA_ARGS__)
#endif

#define LAPACK_slacpy_base LAPACK_GLOBAL(slacpy,SLACPY)
void LAPACK_slacpy_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slacpy(...) LAPACK_slacpy_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slacpy(...) LAPACK_slacpy_base(__VA_ARGS__)
#endif

#define LAPACK_zlacpy_base LAPACK_GLOBAL(zlacpy,ZLACPY)
void LAPACK_zlacpy_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlacpy(...) LAPACK_zlacpy_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlacpy(...) LAPACK_zlacpy_base(__VA_ARGS__)
#endif

#define LAPACK_clacrm LAPACK_GLOBAL(clacrm,CLACRM)
void LAPACK_clacrm(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float const* B, lapack_int const* ldb,
    lapack_complex_float* C, lapack_int const* ldc,
    float* rwork );

#define LAPACK_zlacrm LAPACK_GLOBAL(zlacrm,ZLACRM)
void LAPACK_zlacrm(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    lapack_complex_double* C, lapack_int const* ldc,
    double* rwork );

#define LAPACK_zlag2c LAPACK_GLOBAL(zlag2c,ZLAG2C)
void LAPACK_zlag2c(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_float* SA, lapack_int const* ldsa,
    lapack_int* info );

#define LAPACK_slag2d LAPACK_GLOBAL(slag2d,SLAG2D)
void LAPACK_slag2d(
    lapack_int const* m, lapack_int const* n,
    float const* SA, lapack_int const* ldsa,
    double* A, lapack_int const* lda,
    lapack_int* info );

#define LAPACK_dlag2s LAPACK_GLOBAL(dlag2s,DLAG2S)
void LAPACK_dlag2s(
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    float* SA, lapack_int const* ldsa,
    lapack_int* info );

#define LAPACK_clag2z LAPACK_GLOBAL(clag2z,CLAG2Z)
void LAPACK_clag2z(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* SA, lapack_int const* ldsa,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info );

#define LAPACK_clagge LAPACK_GLOBAL(clagge,CLAGGE)
void LAPACK_clagge(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    float const* D,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* iseed,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dlagge LAPACK_GLOBAL(dlagge,DLAGGE)
void LAPACK_dlagge(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    double const* D,
    double* A, lapack_int const* lda, lapack_int* iseed,
    double* work,
    lapack_int* info );

#define LAPACK_slagge LAPACK_GLOBAL(slagge,SLAGGE)
void LAPACK_slagge(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    float const* D,
    float* A, lapack_int const* lda, lapack_int* iseed,
    float* work,
    lapack_int* info );

#define LAPACK_zlagge LAPACK_GLOBAL(zlagge,ZLAGGE)
void LAPACK_zlagge(
    lapack_int const* m, lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    double const* D,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* iseed,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_claghe LAPACK_GLOBAL(claghe,CLAGHE)
void LAPACK_claghe(
    lapack_int const* n, lapack_int const* k,
    float const* D,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* iseed,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_zlaghe LAPACK_GLOBAL(zlaghe,ZLAGHE)
void LAPACK_zlaghe(
    lapack_int const* n, lapack_int const* k,
    double const* D,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* iseed,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_clagsy LAPACK_GLOBAL(clagsy,CLAGSY)
void LAPACK_clagsy(
    lapack_int const* n, lapack_int const* k,
    float const* D,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* iseed,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dlagsy LAPACK_GLOBAL(dlagsy,DLAGSY)
void LAPACK_dlagsy(
    lapack_int const* n, lapack_int const* k,
    double const* D,
    double* A, lapack_int const* lda, lapack_int* iseed,
    double* work,
    lapack_int* info );

#define LAPACK_slagsy LAPACK_GLOBAL(slagsy,SLAGSY)
void LAPACK_slagsy(
    lapack_int const* n, lapack_int const* k,
    float const* D,
    float* A, lapack_int const* lda, lapack_int* iseed,
    float* work,
    lapack_int* info );

#define LAPACK_zlagsy LAPACK_GLOBAL(zlagsy,ZLAGSY)
void LAPACK_zlagsy(
    lapack_int const* n, lapack_int const* k,
    double const* D,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* iseed,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_dlamch_base LAPACK_GLOBAL(dlamch,DLAMCH)
double LAPACK_dlamch_base(
    char const* cmach
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlamch(...) LAPACK_dlamch_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlamch(...) LAPACK_dlamch_base(__VA_ARGS__)
#endif

#define LAPACK_slamch_base LAPACK_GLOBAL(slamch,SLAMCH)
lapack_float_return LAPACK_slamch_base(
    char const* cmach
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slamch(...) LAPACK_slamch_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slamch(...) LAPACK_slamch_base(__VA_ARGS__)
#endif

#define LAPACK_clangb_base LAPACK_GLOBAL(clangb,CLANGB)
lapack_float_return LAPACK_clangb_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clangb(...) LAPACK_clangb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clangb(...) LAPACK_clangb_base(__VA_ARGS__)
#endif

#define LAPACK_dlangb_base LAPACK_GLOBAL(dlangb,DLANGB)
double LAPACK_dlangb_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    double const* AB, lapack_int const* ldab,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlangb(...) LAPACK_dlangb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlangb(...) LAPACK_dlangb_base(__VA_ARGS__)
#endif

#define LAPACK_slangb_base LAPACK_GLOBAL(slangb,SLANGB)
lapack_float_return LAPACK_slangb_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    float const* AB, lapack_int const* ldab,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slangb(...) LAPACK_slangb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slangb(...) LAPACK_slangb_base(__VA_ARGS__)
#endif

#define LAPACK_zlangb_base LAPACK_GLOBAL(zlangb,ZLANGB)
double LAPACK_zlangb_base(
    char const* norm,
    lapack_int const* n, lapack_int const* kl, lapack_int const* ku,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlangb(...) LAPACK_zlangb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlangb(...) LAPACK_zlangb_base(__VA_ARGS__)
#endif

#define LAPACK_clange_base LAPACK_GLOBAL(clange,CLANGE)
lapack_float_return LAPACK_clange_base(
    char const* norm,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clange(...) LAPACK_clange_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clange(...) LAPACK_clange_base(__VA_ARGS__)
#endif

#define LAPACK_dlange_base LAPACK_GLOBAL(dlange,DLANGE)
double LAPACK_dlange_base(
    char const* norm,
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlange(...) LAPACK_dlange_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlange(...) LAPACK_dlange_base(__VA_ARGS__)
#endif

#define LAPACK_slange_base LAPACK_GLOBAL(slange,SLANGE)
lapack_float_return LAPACK_slange_base(
    char const* norm,
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slange(...) LAPACK_slange_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slange(...) LAPACK_slange_base(__VA_ARGS__)
#endif

#define LAPACK_zlange_base LAPACK_GLOBAL(zlange,ZLANGE)
double LAPACK_zlange_base(
    char const* norm,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlange(...) LAPACK_zlange_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlange(...) LAPACK_zlange_base(__VA_ARGS__)
#endif

#define LAPACK_clangt_base LAPACK_GLOBAL(clangt,CLANGT)
lapack_float_return LAPACK_clangt_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_float const* DL,
    lapack_complex_float const* D,
    lapack_complex_float const* DU
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clangt(...) LAPACK_clangt_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clangt(...) LAPACK_clangt_base(__VA_ARGS__)
#endif

#define LAPACK_dlangt_base LAPACK_GLOBAL(dlangt,DLANGT)
double LAPACK_dlangt_base(
    char const* norm,
    lapack_int const* n,
    double const* DL,
    double const* D,
    double const* DU
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlangt(...) LAPACK_dlangt_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlangt(...) LAPACK_dlangt_base(__VA_ARGS__)
#endif

#define LAPACK_slangt_base LAPACK_GLOBAL(slangt,SLANGT)
lapack_float_return LAPACK_slangt_base(
    char const* norm,
    lapack_int const* n,
    float const* DL,
    float const* D,
    float const* DU
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slangt(...) LAPACK_slangt_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slangt(...) LAPACK_slangt_base(__VA_ARGS__)
#endif

#define LAPACK_zlangt_base LAPACK_GLOBAL(zlangt,ZLANGT)
double LAPACK_zlangt_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_double const* DL,
    lapack_complex_double const* D,
    lapack_complex_double const* DU
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlangt(...) LAPACK_zlangt_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlangt(...) LAPACK_zlangt_base(__VA_ARGS__)
#endif

#define LAPACK_clanhb_base LAPACK_GLOBAL(clanhb,CLANHB)
lapack_float_return LAPACK_clanhb_base(
    char const* norm, char const* uplo,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clanhb(...) LAPACK_clanhb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_clanhb(...) LAPACK_clanhb_base(__VA_ARGS__)
#endif

#define LAPACK_zlanhb_base LAPACK_GLOBAL(zlanhb,ZLANHB)
double LAPACK_zlanhb_base(
    char const* norm, char const* uplo,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlanhb(...) LAPACK_zlanhb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zlanhb(...) LAPACK_zlanhb_base(__VA_ARGS__)
#endif

#define LAPACK_clanhe_base LAPACK_GLOBAL(clanhe,CLANHE)
lapack_float_return LAPACK_clanhe_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clanhe(...) LAPACK_clanhe_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_clanhe(...) LAPACK_clanhe_base(__VA_ARGS__)
#endif

#define LAPACK_zlanhe_base LAPACK_GLOBAL(zlanhe,ZLANHE)
double LAPACK_zlanhe_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlanhe(...) LAPACK_zlanhe_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zlanhe(...) LAPACK_zlanhe_base(__VA_ARGS__)
#endif

#define LAPACK_clanhp_base LAPACK_GLOBAL(clanhp,CLANHP)
lapack_float_return LAPACK_clanhp_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clanhp(...) LAPACK_clanhp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_clanhp(...) LAPACK_clanhp_base(__VA_ARGS__)
#endif

#define LAPACK_zlanhp_base LAPACK_GLOBAL(zlanhp,ZLANHP)
double LAPACK_zlanhp_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlanhp(...) LAPACK_zlanhp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zlanhp(...) LAPACK_zlanhp_base(__VA_ARGS__)
#endif

#define LAPACK_clanhs_base LAPACK_GLOBAL(clanhs,CLANHS)
lapack_float_return LAPACK_clanhs_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clanhs(...) LAPACK_clanhs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clanhs(...) LAPACK_clanhs_base(__VA_ARGS__)
#endif

#define LAPACK_dlanhs_base LAPACK_GLOBAL(dlanhs,DLANHS)
double LAPACK_dlanhs_base(
    char const* norm,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlanhs(...) LAPACK_dlanhs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlanhs(...) LAPACK_dlanhs_base(__VA_ARGS__)
#endif

#define LAPACK_slanhs_base LAPACK_GLOBAL(slanhs,SLANHS)
lapack_float_return LAPACK_slanhs_base(
    char const* norm,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slanhs(...) LAPACK_slanhs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slanhs(...) LAPACK_slanhs_base(__VA_ARGS__)
#endif

#define LAPACK_zlanhs_base LAPACK_GLOBAL(zlanhs,ZLANHS)
double LAPACK_zlanhs_base(
    char const* norm,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlanhs(...) LAPACK_zlanhs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlanhs(...) LAPACK_zlanhs_base(__VA_ARGS__)
#endif

#define LAPACK_clanht_base LAPACK_GLOBAL(clanht,CLANHT)
lapack_float_return LAPACK_clanht_base(
    char const* norm,
    lapack_int const* n,
    float const* D,
    lapack_complex_float const* E
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clanht(...) LAPACK_clanht_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clanht(...) LAPACK_clanht_base(__VA_ARGS__)
#endif

#define LAPACK_zlanht_base LAPACK_GLOBAL(zlanht,ZLANHT)
double LAPACK_zlanht_base(
    char const* norm,
    lapack_int const* n,
    double const* D,
    lapack_complex_double const* E
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlanht(...) LAPACK_zlanht_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlanht(...) LAPACK_zlanht_base(__VA_ARGS__)
#endif

#define LAPACK_clansb_base LAPACK_GLOBAL(clansb,CLANSB)
lapack_float_return LAPACK_clansb_base(
    char const* norm, char const* uplo,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clansb(...) LAPACK_clansb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_clansb(...) LAPACK_clansb_base(__VA_ARGS__)
#endif

#define LAPACK_dlansb_base LAPACK_GLOBAL(dlansb,DLANSB)
double LAPACK_dlansb_base(
    char const* norm, char const* uplo,
    lapack_int const* n, lapack_int const* k,
    double const* AB, lapack_int const* ldab,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlansb(...) LAPACK_dlansb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dlansb(...) LAPACK_dlansb_base(__VA_ARGS__)
#endif

#define LAPACK_slansb_base LAPACK_GLOBAL(slansb,SLANSB)
lapack_float_return LAPACK_slansb_base(
    char const* norm, char const* uplo,
    lapack_int const* n, lapack_int const* k,
    float const* AB, lapack_int const* ldab,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slansb(...) LAPACK_slansb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_slansb(...) LAPACK_slansb_base(__VA_ARGS__)
#endif

#define LAPACK_zlansb_base LAPACK_GLOBAL(zlansb,ZLANSB)
double LAPACK_zlansb_base(
    char const* norm, char const* uplo,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlansb(...) LAPACK_zlansb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zlansb(...) LAPACK_zlansb_base(__VA_ARGS__)
#endif

#define LAPACK_clansp_base LAPACK_GLOBAL(clansp,CLANSP)
lapack_float_return LAPACK_clansp_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clansp(...) LAPACK_clansp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_clansp(...) LAPACK_clansp_base(__VA_ARGS__)
#endif

#define LAPACK_dlansp_base LAPACK_GLOBAL(dlansp,DLANSP)
double LAPACK_dlansp_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    double const* AP,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlansp(...) LAPACK_dlansp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dlansp(...) LAPACK_dlansp_base(__VA_ARGS__)
#endif

#define LAPACK_slansp_base LAPACK_GLOBAL(slansp,SLANSP)
lapack_float_return LAPACK_slansp_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    float const* AP,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slansp(...) LAPACK_slansp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_slansp(...) LAPACK_slansp_base(__VA_ARGS__)
#endif

#define LAPACK_zlansp_base LAPACK_GLOBAL(zlansp,ZLANSP)
double LAPACK_zlansp_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlansp(...) LAPACK_zlansp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zlansp(...) LAPACK_zlansp_base(__VA_ARGS__)
#endif

#define LAPACK_dlanst_base LAPACK_GLOBAL(dlanst,DLANST)
double LAPACK_dlanst_base(
    char const* norm,
    lapack_int const* n,
    double const* D,
    double const* E
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlanst(...) LAPACK_dlanst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlanst(...) LAPACK_dlanst_base(__VA_ARGS__)
#endif

#define LAPACK_slanst_base LAPACK_GLOBAL(slanst,SLANST)
lapack_float_return LAPACK_slanst_base(
    char const* norm,
    lapack_int const* n,
    float const* D,
    float const* E
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slanst(...) LAPACK_slanst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slanst(...) LAPACK_slanst_base(__VA_ARGS__)
#endif

#define LAPACK_clansy_base LAPACK_GLOBAL(clansy,CLANSY)
lapack_float_return LAPACK_clansy_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clansy(...) LAPACK_clansy_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_clansy(...) LAPACK_clansy_base(__VA_ARGS__)
#endif

#define LAPACK_dlansy_base LAPACK_GLOBAL(dlansy,DLANSY)
double LAPACK_dlansy_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlansy(...) LAPACK_dlansy_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dlansy(...) LAPACK_dlansy_base(__VA_ARGS__)
#endif

#define LAPACK_slansy_base LAPACK_GLOBAL(slansy,SLANSY)
lapack_float_return LAPACK_slansy_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slansy(...) LAPACK_slansy_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_slansy(...) LAPACK_slansy_base(__VA_ARGS__)
#endif

#define LAPACK_zlansy_base LAPACK_GLOBAL(zlansy,ZLANSY)
double LAPACK_zlansy_base(
    char const* norm, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlansy(...) LAPACK_zlansy_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zlansy(...) LAPACK_zlansy_base(__VA_ARGS__)
#endif

#define LAPACK_clantb_base LAPACK_GLOBAL(clantb,CLANTB)
lapack_float_return LAPACK_clantb_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clantb(...) LAPACK_clantb_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_clantb(...) LAPACK_clantb_base(__VA_ARGS__)
#endif

#define LAPACK_dlantb_base LAPACK_GLOBAL(dlantb,DLANTB)
double LAPACK_dlantb_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* k,
    double const* AB, lapack_int const* ldab,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlantb(...) LAPACK_dlantb_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dlantb(...) LAPACK_dlantb_base(__VA_ARGS__)
#endif

#define LAPACK_slantb_base LAPACK_GLOBAL(slantb,SLANTB)
lapack_float_return LAPACK_slantb_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* k,
    float const* AB, lapack_int const* ldab,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slantb(...) LAPACK_slantb_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_slantb(...) LAPACK_slantb_base(__VA_ARGS__)
#endif

#define LAPACK_zlantb_base LAPACK_GLOBAL(zlantb,ZLANTB)
double LAPACK_zlantb_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlantb(...) LAPACK_zlantb_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zlantb(...) LAPACK_zlantb_base(__VA_ARGS__)
#endif

#define LAPACK_clantp_base LAPACK_GLOBAL(clantp,CLANTP)
lapack_float_return LAPACK_clantp_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_float const* AP,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clantp(...) LAPACK_clantp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_clantp(...) LAPACK_clantp_base(__VA_ARGS__)
#endif

#define LAPACK_dlantp_base LAPACK_GLOBAL(dlantp,DLANTP)
double LAPACK_dlantp_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    double const* AP,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlantp(...) LAPACK_dlantp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dlantp(...) LAPACK_dlantp_base(__VA_ARGS__)
#endif

#define LAPACK_slantp_base LAPACK_GLOBAL(slantp,SLANTP)
lapack_float_return LAPACK_slantp_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    float const* AP,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slantp(...) LAPACK_slantp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_slantp(...) LAPACK_slantp_base(__VA_ARGS__)
#endif

#define LAPACK_zlantp_base LAPACK_GLOBAL(zlantp,ZLANTP)
double LAPACK_zlantp_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_double const* AP,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlantp(...) LAPACK_zlantp_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zlantp(...) LAPACK_zlantp_base(__VA_ARGS__)
#endif

#define LAPACK_clantr_base LAPACK_GLOBAL(clantr,CLANTR)
lapack_float_return LAPACK_clantr_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clantr(...) LAPACK_clantr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_clantr(...) LAPACK_clantr_base(__VA_ARGS__)
#endif

#define LAPACK_dlantr_base LAPACK_GLOBAL(dlantr,DLANTR)
double LAPACK_dlantr_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlantr(...) LAPACK_dlantr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dlantr(...) LAPACK_dlantr_base(__VA_ARGS__)
#endif

#define LAPACK_slantr_base LAPACK_GLOBAL(slantr,SLANTR)
lapack_float_return LAPACK_slantr_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slantr(...) LAPACK_slantr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_slantr(...) LAPACK_slantr_base(__VA_ARGS__)
#endif

#define LAPACK_zlantr_base LAPACK_GLOBAL(zlantr,ZLANTR)
double LAPACK_zlantr_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlantr(...) LAPACK_zlantr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zlantr(...) LAPACK_zlantr_base(__VA_ARGS__)
#endif

#define LAPACK_clapmr LAPACK_GLOBAL(clapmr,CLAPMR)
void LAPACK_clapmr(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    lapack_complex_float* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_dlapmr LAPACK_GLOBAL(dlapmr,DLAPMR)
void LAPACK_dlapmr(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    double* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_slapmr LAPACK_GLOBAL(slapmr,SLAPMR)
void LAPACK_slapmr(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    float* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_zlapmr LAPACK_GLOBAL(zlapmr,ZLAPMR)
void LAPACK_zlapmr(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    lapack_complex_double* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_clapmt LAPACK_GLOBAL(clapmt,CLAPMT)
void LAPACK_clapmt(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    lapack_complex_float* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_dlapmt LAPACK_GLOBAL(dlapmt,DLAPMT)
void LAPACK_dlapmt(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    double* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_slapmt LAPACK_GLOBAL(slapmt,SLAPMT)
void LAPACK_slapmt(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    float* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_zlapmt LAPACK_GLOBAL(zlapmt,ZLAPMT)
void LAPACK_zlapmt(
    lapack_logical const* forwrd, lapack_int const* m, lapack_int const* n,
    lapack_complex_double* X, lapack_int const* ldx, lapack_int* K );

#define LAPACK_dlapy2 LAPACK_GLOBAL(dlapy2,DLAPY2)
double LAPACK_dlapy2(
    double const* x,
    double const* y );

#define LAPACK_slapy2 LAPACK_GLOBAL(slapy2,SLAPY2)
lapack_float_return LAPACK_slapy2(
    float const* x,
    float const* y );

#define LAPACK_dlapy3 LAPACK_GLOBAL(dlapy3,DLAPY3)
double LAPACK_dlapy3(
    double const* x,
    double const* y,
    double const* z );

#define LAPACK_slapy3 LAPACK_GLOBAL(slapy3,SLAPY3)
lapack_float_return LAPACK_slapy3(
    float const* x,
    float const* y,
    float const* z );

#define LAPACK_clarcm LAPACK_GLOBAL(clarcm,CLARCM)
void LAPACK_clarcm(
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* C, lapack_int const* ldc,
    float* rwork );

#define LAPACK_zlarcm LAPACK_GLOBAL(zlarcm,ZLARCM)
void LAPACK_zlarcm(
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* C, lapack_int const* ldc,
    double* rwork );

#define LAPACK_clarf_base LAPACK_GLOBAL(clarf,CLARF)
void LAPACK_clarf_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* V, lapack_int const* incv,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clarf(...) LAPACK_clarf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clarf(...) LAPACK_clarf_base(__VA_ARGS__)
#endif

#define LAPACK_dlarf_base LAPACK_GLOBAL(dlarf,DLARF)
void LAPACK_dlarf_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    double const* V, lapack_int const* incv,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlarf(...) LAPACK_dlarf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlarf(...) LAPACK_dlarf_base(__VA_ARGS__)
#endif

#define LAPACK_slarf_base LAPACK_GLOBAL(slarf,SLARF)
void LAPACK_slarf_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    float const* V, lapack_int const* incv,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slarf(...) LAPACK_slarf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slarf(...) LAPACK_slarf_base(__VA_ARGS__)
#endif

#define LAPACK_zlarf_base LAPACK_GLOBAL(zlarf,ZLARF)
void LAPACK_zlarf_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* V, lapack_int const* incv,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlarf(...) LAPACK_zlarf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlarf(...) LAPACK_zlarf_base(__VA_ARGS__)
#endif

#define LAPACK_clarfb_base LAPACK_GLOBAL(clarfb,CLARFB)
void LAPACK_clarfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* V, lapack_int const* ldv,
    lapack_complex_float const* T, lapack_int const* ldt,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clarfb(...) LAPACK_clarfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_clarfb(...) LAPACK_clarfb_base(__VA_ARGS__)
#endif

#define LAPACK_dlarfb_base LAPACK_GLOBAL(dlarfb,DLARFB)
void LAPACK_dlarfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* V, lapack_int const* ldv,
    double const* T, lapack_int const* ldt,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlarfb(...) LAPACK_dlarfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_dlarfb(...) LAPACK_dlarfb_base(__VA_ARGS__)
#endif

#define LAPACK_slarfb_base LAPACK_GLOBAL(slarfb,SLARFB)
void LAPACK_slarfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* V, lapack_int const* ldv,
    float const* T, lapack_int const* ldt,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slarfb(...) LAPACK_slarfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_slarfb(...) LAPACK_slarfb_base(__VA_ARGS__)
#endif

#define LAPACK_zlarfb_base LAPACK_GLOBAL(zlarfb,ZLARFB)
void LAPACK_zlarfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* V, lapack_int const* ldv,
    lapack_complex_double const* T, lapack_int const* ldt,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlarfb(...) LAPACK_zlarfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_zlarfb(...) LAPACK_zlarfb_base(__VA_ARGS__)
#endif

#define LAPACK_clarfg LAPACK_GLOBAL(clarfg,CLARFG)
void LAPACK_clarfg(
    lapack_int const* n,
    lapack_complex_float* alpha,
    lapack_complex_float* X, lapack_int const* incx,
    lapack_complex_float* tau );

#define LAPACK_dlarfg LAPACK_GLOBAL(dlarfg,DLARFG)
void LAPACK_dlarfg(
    lapack_int const* n,
    double* alpha,
    double* X, lapack_int const* incx,
    double* tau );

#define LAPACK_slarfg LAPACK_GLOBAL(slarfg,SLARFG)
void LAPACK_slarfg(
    lapack_int const* n,
    float* alpha,
    float* X, lapack_int const* incx,
    float* tau );

#define LAPACK_zlarfg LAPACK_GLOBAL(zlarfg,ZLARFG)
void LAPACK_zlarfg(
    lapack_int const* n,
    lapack_complex_double* alpha,
    lapack_complex_double* X, lapack_int const* incx,
    lapack_complex_double* tau );

#define LAPACK_clarft_base LAPACK_GLOBAL(clarft,CLARFT)
void LAPACK_clarft_base(
    char const* direct, char const* storev,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* V, lapack_int const* ldv,
    lapack_complex_float const* tau,
    lapack_complex_float* T, lapack_int const* ldt
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clarft(...) LAPACK_clarft_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_clarft(...) LAPACK_clarft_base(__VA_ARGS__)
#endif

#define LAPACK_dlarft_base LAPACK_GLOBAL(dlarft,DLARFT)
void LAPACK_dlarft_base(
    char const* direct, char const* storev,
    lapack_int const* n, lapack_int const* k,
    double const* V, lapack_int const* ldv,
    double const* tau,
    double* T, lapack_int const* ldt
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlarft(...) LAPACK_dlarft_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dlarft(...) LAPACK_dlarft_base(__VA_ARGS__)
#endif

#define LAPACK_slarft_base LAPACK_GLOBAL(slarft,SLARFT)
void LAPACK_slarft_base(
    char const* direct, char const* storev,
    lapack_int const* n, lapack_int const* k,
    float const* V, lapack_int const* ldv,
    float const* tau,
    float* T, lapack_int const* ldt
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slarft(...) LAPACK_slarft_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_slarft(...) LAPACK_slarft_base(__VA_ARGS__)
#endif

#define LAPACK_zlarft_base LAPACK_GLOBAL(zlarft,ZLARFT)
void LAPACK_zlarft_base(
    char const* direct, char const* storev,
    lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* V, lapack_int const* ldv,
    lapack_complex_double const* tau,
    lapack_complex_double* T, lapack_int const* ldt
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlarft(...) LAPACK_zlarft_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zlarft(...) LAPACK_zlarft_base(__VA_ARGS__)
#endif

#define LAPACK_clarfx_base LAPACK_GLOBAL(clarfx,CLARFX)
void LAPACK_clarfx_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* V,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clarfx(...) LAPACK_clarfx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clarfx(...) LAPACK_clarfx_base(__VA_ARGS__)
#endif

#define LAPACK_dlarfx_base LAPACK_GLOBAL(dlarfx,DLARFX)
void LAPACK_dlarfx_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    double const* V,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlarfx(...) LAPACK_dlarfx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlarfx(...) LAPACK_dlarfx_base(__VA_ARGS__)
#endif

#define LAPACK_slarfx_base LAPACK_GLOBAL(slarfx,SLARFX)
void LAPACK_slarfx_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    float const* V,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slarfx(...) LAPACK_slarfx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slarfx(...) LAPACK_slarfx_base(__VA_ARGS__)
#endif

#define LAPACK_zlarfx_base LAPACK_GLOBAL(zlarfx,ZLARFX)
void LAPACK_zlarfx_base(
    char const* side,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* V,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlarfx(...) LAPACK_zlarfx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlarfx(...) LAPACK_zlarfx_base(__VA_ARGS__)
#endif

#define LAPACK_clarnv LAPACK_GLOBAL(clarnv,CLARNV)
void LAPACK_clarnv(
    lapack_int const* idist, lapack_int* iseed, lapack_int const* n,
    lapack_complex_float* X );

#define LAPACK_dlarnv LAPACK_GLOBAL(dlarnv,DLARNV)
void LAPACK_dlarnv(
    lapack_int const* idist, lapack_int* iseed, lapack_int const* n,
    double* X );

#define LAPACK_slarnv LAPACK_GLOBAL(slarnv,SLARNV)
void LAPACK_slarnv(
    lapack_int const* idist, lapack_int* iseed, lapack_int const* n,
    float* X );

#define LAPACK_zlarnv LAPACK_GLOBAL(zlarnv,ZLARNV)
void LAPACK_zlarnv(
    lapack_int const* idist, lapack_int* iseed, lapack_int const* n,
    lapack_complex_double* X );

#define LAPACK_dlartgp LAPACK_GLOBAL(dlartgp,DLARTGP)
void LAPACK_dlartgp(
    double const* f,
    double const* g,
    double* cs,
    double* sn,
    double* r );

#define LAPACK_slartgp LAPACK_GLOBAL(slartgp,SLARTGP)
void LAPACK_slartgp(
    float const* f,
    float const* g,
    float* cs,
    float* sn,
    float* r );

#define LAPACK_dlartgs LAPACK_GLOBAL(dlartgs,DLARTGS)
void LAPACK_dlartgs(
    double const* x,
    double const* y,
    double const* sigma,
    double* cs,
    double* sn );

#define LAPACK_slartgs LAPACK_GLOBAL(slartgs,SLARTGS)
void LAPACK_slartgs(
    float const* x,
    float const* y,
    float const* sigma,
    float* cs,
    float* sn );

#define LAPACK_clascl_base LAPACK_GLOBAL(clascl,CLASCL)
void LAPACK_clascl_base(
    char const* type,
    lapack_int const* kl, lapack_int const* ku,
    float const* cfrom,
    float const* cto, lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clascl(...) LAPACK_clascl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clascl(...) LAPACK_clascl_base(__VA_ARGS__)
#endif

#define LAPACK_dlascl_base LAPACK_GLOBAL(dlascl,DLASCL)
void LAPACK_dlascl_base(
    char const* type,
    lapack_int const* kl, lapack_int const* ku,
    double const* cfrom,
    double const* cto, lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlascl(...) LAPACK_dlascl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlascl(...) LAPACK_dlascl_base(__VA_ARGS__)
#endif

#define LAPACK_slascl_base LAPACK_GLOBAL(slascl,SLASCL)
void LAPACK_slascl_base(
    char const* type,
    lapack_int const* kl, lapack_int const* ku,
    float const* cfrom,
    float const* cto, lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slascl(...) LAPACK_slascl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slascl(...) LAPACK_slascl_base(__VA_ARGS__)
#endif

#define LAPACK_zlascl_base LAPACK_GLOBAL(zlascl,ZLASCL)
void LAPACK_zlascl_base(
    char const* type,
    lapack_int const* kl, lapack_int const* ku,
    double const* cfrom,
    double const* cto, lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlascl(...) LAPACK_zlascl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlascl(...) LAPACK_zlascl_base(__VA_ARGS__)
#endif

#define LAPACK_claset_base LAPACK_GLOBAL(claset,CLASET)
void LAPACK_claset_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* alpha,
    lapack_complex_float const* beta,
    lapack_complex_float* A, lapack_int const* lda
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_claset(...) LAPACK_claset_base(__VA_ARGS__, 1)
#else
    #define LAPACK_claset(...) LAPACK_claset_base(__VA_ARGS__)
#endif

#define LAPACK_dlaset_base LAPACK_GLOBAL(dlaset,DLASET)
void LAPACK_dlaset_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    double const* alpha,
    double const* beta,
    double* A, lapack_int const* lda
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlaset(...) LAPACK_dlaset_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlaset(...) LAPACK_dlaset_base(__VA_ARGS__)
#endif

#define LAPACK_slaset_base LAPACK_GLOBAL(slaset,SLASET)
void LAPACK_slaset_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    float const* alpha,
    float const* beta,
    float* A, lapack_int const* lda
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slaset(...) LAPACK_slaset_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slaset(...) LAPACK_slaset_base(__VA_ARGS__)
#endif

#define LAPACK_zlaset_base LAPACK_GLOBAL(zlaset,ZLASET)
void LAPACK_zlaset_base(
    char const* uplo,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* alpha,
    lapack_complex_double const* beta,
    lapack_complex_double* A, lapack_int const* lda
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlaset(...) LAPACK_zlaset_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlaset(...) LAPACK_zlaset_base(__VA_ARGS__)
#endif

#define LAPACK_dlasrt_base LAPACK_GLOBAL(dlasrt,DLASRT)
void LAPACK_dlasrt_base(
    char const* id,
    lapack_int const* n,
    double* D,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlasrt(...) LAPACK_dlasrt_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlasrt(...) LAPACK_dlasrt_base(__VA_ARGS__)
#endif

#define LAPACK_slasrt_base LAPACK_GLOBAL(slasrt,SLASRT)
void LAPACK_slasrt_base(
    char const* id,
    lapack_int const* n,
    float* D,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slasrt(...) LAPACK_slasrt_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slasrt(...) LAPACK_slasrt_base(__VA_ARGS__)
#endif

#define LAPACK_classq LAPACK_GLOBAL(classq,CLASSQ)
void LAPACK_classq(
    lapack_int const* n,
    lapack_complex_float const* X, lapack_int const* incx,
    float* scale,
    float* sumsq );

#define LAPACK_dlassq LAPACK_GLOBAL(dlassq,DLASSQ)
void LAPACK_dlassq(
    lapack_int const* n,
    double const* X, lapack_int const* incx,
    double* scale,
    double* sumsq );

#define LAPACK_slassq LAPACK_GLOBAL(slassq,SLASSQ)
void LAPACK_slassq(
    lapack_int const* n,
    float const* X, lapack_int const* incx,
    float* scale,
    float* sumsq );

#define LAPACK_zlassq LAPACK_GLOBAL(zlassq,ZLASSQ)
void LAPACK_zlassq(
    lapack_int const* n,
    lapack_complex_double const* X, lapack_int const* incx,
    double* scale,
    double* sumsq );

#define LAPACK_claswp LAPACK_GLOBAL(claswp,CLASWP)
lapack_int LAPACK_claswp(
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* k1, lapack_int const* k2, lapack_int const* ipiv, lapack_int const* incx );

#define LAPACK_dlaswp LAPACK_GLOBAL(dlaswp,DLASWP)
lapack_int LAPACK_dlaswp(
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int const* k1, lapack_int const* k2, lapack_int const* ipiv, lapack_int const* incx );

#define LAPACK_slaswp LAPACK_GLOBAL(slaswp,SLASWP)
lapack_int LAPACK_slaswp(
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int const* k1, lapack_int const* k2, lapack_int const* ipiv, lapack_int const* incx );

#define LAPACK_zlaswp LAPACK_GLOBAL(zlaswp,ZLASWP)
lapack_int LAPACK_zlaswp(
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* k1, lapack_int const* k2, lapack_int const* ipiv, lapack_int const* incx );

#define LAPACK_clatms_base LAPACK_GLOBAL(clatms,CLATMS)
void LAPACK_clatms_base(
    lapack_int const* m, lapack_int const* n, char const* dist,
    lapack_int* iseed, char const* sym,
    float* D,
    lapack_int const* mode,
    float const* cond,
    float const* dmax, lapack_int const* kl, lapack_int const* ku, char const* pack,
    lapack_complex_float* A,
    lapack_int const* lda,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clatms(...) LAPACK_clatms_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_clatms(...) LAPACK_clatms_base(__VA_ARGS__)
#endif

#define LAPACK_dlatms_base LAPACK_GLOBAL(dlatms,DLATMS)
void LAPACK_dlatms_base(
    lapack_int const* m, lapack_int const* n, char const* dist,
    lapack_int* iseed, char const* sym,
    double* D,
    lapack_int const* mode,
    double const* cond,
    double const* dmax, lapack_int const* kl, lapack_int const* ku, char const* pack,
    double* A,
    lapack_int const* lda,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlatms(...) LAPACK_dlatms_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dlatms(...) LAPACK_dlatms_base(__VA_ARGS__)
#endif

#define LAPACK_slatms_base LAPACK_GLOBAL(slatms,SLATMS)
void LAPACK_slatms_base(
    lapack_int const* m, lapack_int const* n, char const* dist,
    lapack_int* iseed, char const* sym,
    float* D,
    lapack_int const* mode,
    float const* cond,
    float const* dmax, lapack_int const* kl, lapack_int const* ku, char const* pack,
    float* A,
    lapack_int const* lda,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slatms(...) LAPACK_slatms_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_slatms(...) LAPACK_slatms_base(__VA_ARGS__)
#endif

#define LAPACK_zlatms_base LAPACK_GLOBAL(zlatms,ZLATMS)
void LAPACK_zlatms_base(
    lapack_int const* m, lapack_int const* n, char const* dist,
    lapack_int* iseed, char const* sym,
    double* D,
    lapack_int const* mode,
    double const* cond,
    double const* dmax, lapack_int const* kl, lapack_int const* ku, char const* pack,
    lapack_complex_double* A,
    lapack_int const* lda,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlatms(...) LAPACK_zlatms_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zlatms(...) LAPACK_zlatms_base(__VA_ARGS__)
#endif

#define LAPACK_clauum_base LAPACK_GLOBAL(clauum,CLAUUM)
lapack_int LAPACK_clauum_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_clauum(...) LAPACK_clauum_base(__VA_ARGS__, 1)
#else
    #define LAPACK_clauum(...) LAPACK_clauum_base(__VA_ARGS__)
#endif

#define LAPACK_dlauum_base LAPACK_GLOBAL(dlauum,DLAUUM)
lapack_int LAPACK_dlauum_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dlauum(...) LAPACK_dlauum_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dlauum(...) LAPACK_dlauum_base(__VA_ARGS__)
#endif

#define LAPACK_slauum_base LAPACK_GLOBAL(slauum,SLAUUM)
lapack_int LAPACK_slauum_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_slauum(...) LAPACK_slauum_base(__VA_ARGS__, 1)
#else
    #define LAPACK_slauum(...) LAPACK_slauum_base(__VA_ARGS__)
#endif

#define LAPACK_zlauum_base LAPACK_GLOBAL(zlauum,ZLAUUM)
lapack_int LAPACK_zlauum_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zlauum(...) LAPACK_zlauum_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zlauum(...) LAPACK_zlauum_base(__VA_ARGS__)
#endif

#define LAPACK_ilaver LAPACK_GLOBAL(ilaver,ILAVER)
lapack_int LAPACK_ilaver(
    lapack_int* vers_major, lapack_int* vers_minor, lapack_int* vers_patch );

#define LAPACK_dopgtr_base LAPACK_GLOBAL(dopgtr,DOPGTR)
void LAPACK_dopgtr_base(
    char const* uplo,
    lapack_int const* n,
    double const* AP,
    double const* tau,
    double* Q, lapack_int const* ldq,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dopgtr(...) LAPACK_dopgtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dopgtr(...) LAPACK_dopgtr_base(__VA_ARGS__)
#endif

#define LAPACK_sopgtr_base LAPACK_GLOBAL(sopgtr,SOPGTR)
void LAPACK_sopgtr_base(
    char const* uplo,
    lapack_int const* n,
    float const* AP,
    float const* tau,
    float* Q, lapack_int const* ldq,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sopgtr(...) LAPACK_sopgtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sopgtr(...) LAPACK_sopgtr_base(__VA_ARGS__)
#endif

#define LAPACK_dopmtr_base LAPACK_GLOBAL(dopmtr,DOPMTR)
void LAPACK_dopmtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    double const* AP,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dopmtr(...) LAPACK_dopmtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dopmtr(...) LAPACK_dopmtr_base(__VA_ARGS__)
#endif

#define LAPACK_sopmtr_base LAPACK_GLOBAL(sopmtr,SOPMTR)
void LAPACK_sopmtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    float const* AP,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sopmtr(...) LAPACK_sopmtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sopmtr(...) LAPACK_sopmtr_base(__VA_ARGS__)
#endif

#define LAPACK_dorbdb_base LAPACK_GLOBAL(dorbdb,DORBDB)
void LAPACK_dorbdb_base(
    char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    double* X11, lapack_int const* ldx11,
    double* X12, lapack_int const* ldx12,
    double* X21, lapack_int const* ldx21,
    double* X22, lapack_int const* ldx22,
    double* theta,
    double* phi,
    double* TAUP1,
    double* TAUP2,
    double* TAUQ1,
    double* TAUQ2,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dorbdb(...) LAPACK_dorbdb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dorbdb(...) LAPACK_dorbdb_base(__VA_ARGS__)
#endif

#define LAPACK_sorbdb_base LAPACK_GLOBAL(sorbdb,SORBDB)
void LAPACK_sorbdb_base(
    char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    float* X11, lapack_int const* ldx11,
    float* X12, lapack_int const* ldx12,
    float* X21, lapack_int const* ldx21,
    float* X22, lapack_int const* ldx22,
    float* theta,
    float* phi,
    float* TAUP1,
    float* TAUP2,
    float* TAUQ1,
    float* TAUQ2,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sorbdb(...) LAPACK_sorbdb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sorbdb(...) LAPACK_sorbdb_base(__VA_ARGS__)
#endif

#define LAPACK_dorcsd_base LAPACK_GLOBAL(dorcsd,DORCSD)
void LAPACK_dorcsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    double* X11, lapack_int const* ldx11,
    double* X12, lapack_int const* ldx12,
    double* X21, lapack_int const* ldx21,
    double* X22, lapack_int const* ldx22,
    double* theta,
    double* U1, lapack_int const* ldu1,
    double* U2, lapack_int const* ldu2,
    double* V1T, lapack_int const* ldv1t,
    double* V2T, lapack_int const* ldv2t,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dorcsd(...) LAPACK_dorcsd_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_dorcsd(...) LAPACK_dorcsd_base(__VA_ARGS__)
#endif

#define LAPACK_sorcsd_base LAPACK_GLOBAL(sorcsd,SORCSD)
void LAPACK_sorcsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    float* X11, lapack_int const* ldx11,
    float* X12, lapack_int const* ldx12,
    float* X21, lapack_int const* ldx21,
    float* X22, lapack_int const* ldx22,
    float* theta,
    float* U1, lapack_int const* ldu1,
    float* U2, lapack_int const* ldu2,
    float* V1T, lapack_int const* ldv1t,
    float* V2T, lapack_int const* ldv2t,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sorcsd(...) LAPACK_sorcsd_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_sorcsd(...) LAPACK_sorcsd_base(__VA_ARGS__)
#endif

#define LAPACK_dorcsd2by1_base LAPACK_GLOBAL(dorcsd2by1,DORCSD2BY1)
void LAPACK_dorcsd2by1_base(
    char const* jobu1, char const* jobu2, char const* jobv1t,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    double* X11, lapack_int const* ldx11,
    double* X21, lapack_int const* ldx21,
    double* theta,
    double* U1, lapack_int const* ldu1,
    double* U2, lapack_int const* ldu2,
    double* V1T, lapack_int const* ldv1t,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dorcsd2by1(...) LAPACK_dorcsd2by1_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dorcsd2by1(...) LAPACK_dorcsd2by1_base(__VA_ARGS__)
#endif

#define LAPACK_sorcsd2by1_base LAPACK_GLOBAL(sorcsd2by1,SORCSD2BY1)
void LAPACK_sorcsd2by1_base(
    char const* jobu1, char const* jobu2, char const* jobv1t,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    float* X11, lapack_int const* ldx11,
    float* X21, lapack_int const* ldx21,
    float* theta,
    float* U1, lapack_int const* ldu1,
    float* U2, lapack_int const* ldu2,
    float* V1T, lapack_int const* ldv1t,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sorcsd2by1(...) LAPACK_sorcsd2by1_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sorcsd2by1(...) LAPACK_sorcsd2by1_base(__VA_ARGS__)
#endif

#define LAPACK_dorgbr_base LAPACK_GLOBAL(dorgbr,DORGBR)
void LAPACK_dorgbr_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double* A, lapack_int const* lda,
    double const* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dorgbr(...) LAPACK_dorgbr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dorgbr(...) LAPACK_dorgbr_base(__VA_ARGS__)
#endif

#define LAPACK_sorgbr_base LAPACK_GLOBAL(sorgbr,SORGBR)
void LAPACK_sorgbr_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float* A, lapack_int const* lda,
    float const* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sorgbr(...) LAPACK_sorgbr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sorgbr(...) LAPACK_sorgbr_base(__VA_ARGS__)
#endif

#define LAPACK_dorghr LAPACK_GLOBAL(dorghr,DORGHR)
void LAPACK_dorghr(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double* A, lapack_int const* lda,
    double const* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sorghr LAPACK_GLOBAL(sorghr,SORGHR)
void LAPACK_sorghr(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float* A, lapack_int const* lda,
    float const* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dorglq LAPACK_GLOBAL(dorglq,DORGLQ)
void LAPACK_dorglq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double* A, lapack_int const* lda,
    double const* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sorglq LAPACK_GLOBAL(sorglq,SORGLQ)
void LAPACK_sorglq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float* A, lapack_int const* lda,
    float const* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dorgql LAPACK_GLOBAL(dorgql,DORGQL)
void LAPACK_dorgql(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double* A, lapack_int const* lda,
    double const* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sorgql LAPACK_GLOBAL(sorgql,SORGQL)
void LAPACK_sorgql(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float* A, lapack_int const* lda,
    float const* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dorgqr LAPACK_GLOBAL(dorgqr,DORGQR)
void LAPACK_dorgqr(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double* A, lapack_int const* lda,
    double const* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sorgqr LAPACK_GLOBAL(sorgqr,SORGQR)
void LAPACK_sorgqr(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float* A, lapack_int const* lda,
    float const* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dorgrq LAPACK_GLOBAL(dorgrq,DORGRQ)
void LAPACK_dorgrq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double* A, lapack_int const* lda,
    double const* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sorgrq LAPACK_GLOBAL(sorgrq,SORGRQ)
void LAPACK_sorgrq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float* A, lapack_int const* lda,
    float const* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dorgtr_base LAPACK_GLOBAL(dorgtr,DORGTR)
void LAPACK_dorgtr_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dorgtr(...) LAPACK_dorgtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dorgtr(...) LAPACK_dorgtr_base(__VA_ARGS__)
#endif

#define LAPACK_sorgtr_base LAPACK_GLOBAL(sorgtr,SORGTR)
void LAPACK_sorgtr_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sorgtr(...) LAPACK_sorgtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sorgtr(...) LAPACK_sorgtr_base(__VA_ARGS__)
#endif

#define LAPACK_dorgtsqr_row LAPACK_GLOBAL(dorgtsqr_row,DORGTSQR_ROW)
void LAPACK_dorgtsqr_row(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb, lapack_int const* nb,
    double* A, lapack_int const* lda,
    double const* T, lapack_int const* ldt,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_sorgtsqr_row LAPACK_GLOBAL(sorgtsqr_row,SORGTSQR_ROW)
void LAPACK_sorgtsqr_row(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb, lapack_int const* nb,
    float* A, lapack_int const* lda,
    float const* T, lapack_int const* ldt,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dorhr_col LAPACK_GLOBAL(dorhr_col,DORHR_COL)
void LAPACK_dorhr_col(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* nb, double* A,
    lapack_int const* lda, double* T,
    lapack_int const* ldt, double* D,
    lapack_int* info );

#define LAPACK_sorhr_col LAPACK_GLOBAL(sorhr_col,SORHR_COL)
void LAPACK_sorhr_col(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* nb, float* A,
    lapack_int const* lda, float* T,
    lapack_int const* ldt, float* D,
    lapack_int* info );

#define LAPACK_dormbr_base LAPACK_GLOBAL(dormbr,DORMBR)
void LAPACK_dormbr_base(
    char const* vect, char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormbr(...) LAPACK_dormbr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dormbr(...) LAPACK_dormbr_base(__VA_ARGS__)
#endif

#define LAPACK_sormbr_base LAPACK_GLOBAL(sormbr,SORMBR)
void LAPACK_sormbr_base(
    char const* vect, char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormbr(...) LAPACK_sormbr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sormbr(...) LAPACK_sormbr_base(__VA_ARGS__)
#endif

#define LAPACK_dormhr_base LAPACK_GLOBAL(dormhr,DORMHR)
void LAPACK_dormhr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormhr(...) LAPACK_dormhr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dormhr(...) LAPACK_dormhr_base(__VA_ARGS__)
#endif

#define LAPACK_sormhr_base LAPACK_GLOBAL(sormhr,SORMHR)
void LAPACK_sormhr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormhr(...) LAPACK_sormhr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sormhr(...) LAPACK_sormhr_base(__VA_ARGS__)
#endif

#define LAPACK_dormlq_base LAPACK_GLOBAL(dormlq,DORMLQ)
void LAPACK_dormlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormlq(...) LAPACK_dormlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dormlq(...) LAPACK_dormlq_base(__VA_ARGS__)
#endif

#define LAPACK_sormlq_base LAPACK_GLOBAL(sormlq,SORMLQ)
void LAPACK_sormlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormlq(...) LAPACK_sormlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sormlq(...) LAPACK_sormlq_base(__VA_ARGS__)
#endif

#define LAPACK_dormql_base LAPACK_GLOBAL(dormql,DORMQL)
void LAPACK_dormql_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormql(...) LAPACK_dormql_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dormql(...) LAPACK_dormql_base(__VA_ARGS__)
#endif

#define LAPACK_sormql_base LAPACK_GLOBAL(sormql,SORMQL)
void LAPACK_sormql_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormql(...) LAPACK_sormql_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sormql(...) LAPACK_sormql_base(__VA_ARGS__)
#endif

#define LAPACK_dormqr_base LAPACK_GLOBAL(dormqr,DORMQR)
void LAPACK_dormqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormqr(...) LAPACK_dormqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dormqr(...) LAPACK_dormqr_base(__VA_ARGS__)
#endif

#define LAPACK_sormqr_base LAPACK_GLOBAL(sormqr,SORMQR)
void LAPACK_sormqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormqr(...) LAPACK_sormqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sormqr(...) LAPACK_sormqr_base(__VA_ARGS__)
#endif

#define LAPACK_dormrq_base LAPACK_GLOBAL(dormrq,DORMRQ)
void LAPACK_dormrq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormrq(...) LAPACK_dormrq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dormrq(...) LAPACK_dormrq_base(__VA_ARGS__)
#endif

#define LAPACK_sormrq_base LAPACK_GLOBAL(sormrq,SORMRQ)
void LAPACK_sormrq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormrq(...) LAPACK_sormrq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sormrq(...) LAPACK_sormrq_base(__VA_ARGS__)
#endif

#define LAPACK_dormrz_base LAPACK_GLOBAL(dormrz,DORMRZ)
void LAPACK_dormrz_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormrz(...) LAPACK_dormrz_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dormrz(...) LAPACK_dormrz_base(__VA_ARGS__)
#endif

#define LAPACK_sormrz_base LAPACK_GLOBAL(sormrz,SORMRZ)
void LAPACK_sormrz_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormrz(...) LAPACK_sormrz_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sormrz(...) LAPACK_sormrz_base(__VA_ARGS__)
#endif

#define LAPACK_dormtr_base LAPACK_GLOBAL(dormtr,DORMTR)
void LAPACK_dormtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* tau,
    double* C, lapack_int const* ldc,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dormtr(...) LAPACK_dormtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dormtr(...) LAPACK_dormtr_base(__VA_ARGS__)
#endif

#define LAPACK_sormtr_base LAPACK_GLOBAL(sormtr,SORMTR)
void LAPACK_sormtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* tau,
    float* C, lapack_int const* ldc,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sormtr(...) LAPACK_sormtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sormtr(...) LAPACK_sormtr_base(__VA_ARGS__)
#endif

#define LAPACK_cpbcon_base LAPACK_GLOBAL(cpbcon,CPBCON)
void LAPACK_cpbcon_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbcon(...) LAPACK_cpbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpbcon(...) LAPACK_cpbcon_base(__VA_ARGS__)
#endif

#define LAPACK_dpbcon_base LAPACK_GLOBAL(dpbcon,DPBCON)
void LAPACK_dpbcon_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double const* AB, lapack_int const* ldab,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbcon(...) LAPACK_dpbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpbcon(...) LAPACK_dpbcon_base(__VA_ARGS__)
#endif

#define LAPACK_spbcon_base LAPACK_GLOBAL(spbcon,SPBCON)
void LAPACK_spbcon_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float const* AB, lapack_int const* ldab,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbcon(...) LAPACK_spbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spbcon(...) LAPACK_spbcon_base(__VA_ARGS__)
#endif

#define LAPACK_zpbcon_base LAPACK_GLOBAL(zpbcon,ZPBCON)
void LAPACK_zpbcon_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbcon(...) LAPACK_zpbcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpbcon(...) LAPACK_zpbcon_base(__VA_ARGS__)
#endif

#define LAPACK_cpbequ_base LAPACK_GLOBAL(cpbequ,CPBEQU)
void LAPACK_cpbequ_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbequ(...) LAPACK_cpbequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpbequ(...) LAPACK_cpbequ_base(__VA_ARGS__)
#endif

#define LAPACK_dpbequ_base LAPACK_GLOBAL(dpbequ,DPBEQU)
void LAPACK_dpbequ_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double const* AB, lapack_int const* ldab,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbequ(...) LAPACK_dpbequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpbequ(...) LAPACK_dpbequ_base(__VA_ARGS__)
#endif

#define LAPACK_spbequ_base LAPACK_GLOBAL(spbequ,SPBEQU)
void LAPACK_spbequ_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float const* AB, lapack_int const* ldab,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbequ(...) LAPACK_spbequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spbequ(...) LAPACK_spbequ_base(__VA_ARGS__)
#endif

#define LAPACK_zpbequ_base LAPACK_GLOBAL(zpbequ,ZPBEQU)
void LAPACK_zpbequ_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbequ(...) LAPACK_zpbequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpbequ(...) LAPACK_zpbequ_base(__VA_ARGS__)
#endif

#define LAPACK_cpbrfs_base LAPACK_GLOBAL(cpbrfs,CPBRFS)
void LAPACK_cpbrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_float const* AB, lapack_int const* ldab,
    lapack_complex_float const* AFB, lapack_int const* ldafb,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbrfs(...) LAPACK_cpbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpbrfs(...) LAPACK_cpbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_dpbrfs_base LAPACK_GLOBAL(dpbrfs,DPBRFS)
void LAPACK_dpbrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    double const* AB, lapack_int const* ldab,
    double const* AFB, lapack_int const* ldafb,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbrfs(...) LAPACK_dpbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpbrfs(...) LAPACK_dpbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_spbrfs_base LAPACK_GLOBAL(spbrfs,SPBRFS)
void LAPACK_spbrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    float const* AB, lapack_int const* ldab,
    float const* AFB, lapack_int const* ldafb,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbrfs(...) LAPACK_spbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spbrfs(...) LAPACK_spbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_zpbrfs_base LAPACK_GLOBAL(zpbrfs,ZPBRFS)
void LAPACK_zpbrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_double const* AB, lapack_int const* ldab,
    lapack_complex_double const* AFB, lapack_int const* ldafb,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbrfs(...) LAPACK_zpbrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpbrfs(...) LAPACK_zpbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_cpbstf_base LAPACK_GLOBAL(cpbstf,CPBSTF)
void LAPACK_cpbstf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbstf(...) LAPACK_cpbstf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpbstf(...) LAPACK_cpbstf_base(__VA_ARGS__)
#endif

#define LAPACK_dpbstf_base LAPACK_GLOBAL(dpbstf,DPBSTF)
void LAPACK_dpbstf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbstf(...) LAPACK_dpbstf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpbstf(...) LAPACK_dpbstf_base(__VA_ARGS__)
#endif

#define LAPACK_spbstf_base LAPACK_GLOBAL(spbstf,SPBSTF)
void LAPACK_spbstf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbstf(...) LAPACK_spbstf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spbstf(...) LAPACK_spbstf_base(__VA_ARGS__)
#endif

#define LAPACK_zpbstf_base LAPACK_GLOBAL(zpbstf,ZPBSTF)
void LAPACK_zpbstf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbstf(...) LAPACK_zpbstf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpbstf(...) LAPACK_zpbstf_base(__VA_ARGS__)
#endif

#define LAPACK_cpbsv_base LAPACK_GLOBAL(cpbsv,CPBSV)
void LAPACK_cpbsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbsv(...) LAPACK_cpbsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpbsv(...) LAPACK_cpbsv_base(__VA_ARGS__)
#endif

#define LAPACK_dpbsv_base LAPACK_GLOBAL(dpbsv,DPBSV)
void LAPACK_dpbsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    double* AB, lapack_int const* ldab,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbsv(...) LAPACK_dpbsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpbsv(...) LAPACK_dpbsv_base(__VA_ARGS__)
#endif

#define LAPACK_spbsv_base LAPACK_GLOBAL(spbsv,SPBSV)
void LAPACK_spbsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    float* AB, lapack_int const* ldab,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbsv(...) LAPACK_spbsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spbsv(...) LAPACK_spbsv_base(__VA_ARGS__)
#endif

#define LAPACK_zpbsv_base LAPACK_GLOBAL(zpbsv,ZPBSV)
void LAPACK_zpbsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbsv(...) LAPACK_zpbsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpbsv(...) LAPACK_zpbsv_base(__VA_ARGS__)
#endif

#define LAPACK_cpbsvx_base LAPACK_GLOBAL(cpbsvx,CPBSVX)
void LAPACK_cpbsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_complex_float* AFB, lapack_int const* ldafb, char* equed,
    float* S,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbsvx(...) LAPACK_cpbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cpbsvx(...) LAPACK_cpbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_dpbsvx_base LAPACK_GLOBAL(dpbsvx,DPBSVX)
void LAPACK_dpbsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    double* AB, lapack_int const* ldab,
    double* AFB, lapack_int const* ldafb, char* equed,
    double* S,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbsvx(...) LAPACK_dpbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dpbsvx(...) LAPACK_dpbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_spbsvx_base LAPACK_GLOBAL(spbsvx,SPBSVX)
void LAPACK_spbsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    float* AB, lapack_int const* ldab,
    float* AFB, lapack_int const* ldafb, char* equed,
    float* S,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbsvx(...) LAPACK_spbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_spbsvx(...) LAPACK_spbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_zpbsvx_base LAPACK_GLOBAL(zpbsvx,ZPBSVX)
void LAPACK_zpbsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_complex_double* AFB, lapack_int const* ldafb, char* equed,
    double* S,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbsvx(...) LAPACK_zpbsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zpbsvx(...) LAPACK_zpbsvx_base(__VA_ARGS__)
#endif

#define LAPACK_cpbtrf_base LAPACK_GLOBAL(cpbtrf,CPBTRF)
void LAPACK_cpbtrf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbtrf(...) LAPACK_cpbtrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpbtrf(...) LAPACK_cpbtrf_base(__VA_ARGS__)
#endif

#define LAPACK_dpbtrf_base LAPACK_GLOBAL(dpbtrf,DPBTRF)
void LAPACK_dpbtrf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbtrf(...) LAPACK_dpbtrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpbtrf(...) LAPACK_dpbtrf_base(__VA_ARGS__)
#endif

#define LAPACK_spbtrf_base LAPACK_GLOBAL(spbtrf,SPBTRF)
void LAPACK_spbtrf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbtrf(...) LAPACK_spbtrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spbtrf(...) LAPACK_spbtrf_base(__VA_ARGS__)
#endif

#define LAPACK_zpbtrf_base LAPACK_GLOBAL(zpbtrf,ZPBTRF)
void LAPACK_zpbtrf_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double* AB, lapack_int const* ldab,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbtrf(...) LAPACK_zpbtrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpbtrf(...) LAPACK_zpbtrf_base(__VA_ARGS__)
#endif

#define LAPACK_cpbtrs_base LAPACK_GLOBAL(cpbtrs,CPBTRS)
void LAPACK_cpbtrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_float const* AB, lapack_int const* ldab,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpbtrs(...) LAPACK_cpbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpbtrs(...) LAPACK_cpbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_dpbtrs_base LAPACK_GLOBAL(dpbtrs,DPBTRS)
void LAPACK_dpbtrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    double const* AB, lapack_int const* ldab,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpbtrs(...) LAPACK_dpbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpbtrs(...) LAPACK_dpbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_spbtrs_base LAPACK_GLOBAL(spbtrs,SPBTRS)
void LAPACK_spbtrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    float const* AB, lapack_int const* ldab,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spbtrs(...) LAPACK_spbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spbtrs(...) LAPACK_spbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_zpbtrs_base LAPACK_GLOBAL(zpbtrs,ZPBTRS)
void LAPACK_zpbtrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_double const* AB, lapack_int const* ldab,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpbtrs(...) LAPACK_zpbtrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpbtrs(...) LAPACK_zpbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_cpftrf_base LAPACK_GLOBAL(cpftrf,CPFTRF)
void LAPACK_cpftrf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpftrf(...) LAPACK_cpftrf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cpftrf(...) LAPACK_cpftrf_base(__VA_ARGS__)
#endif

#define LAPACK_dpftrf_base LAPACK_GLOBAL(dpftrf,DPFTRF)
void LAPACK_dpftrf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    double* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpftrf(...) LAPACK_dpftrf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dpftrf(...) LAPACK_dpftrf_base(__VA_ARGS__)
#endif

#define LAPACK_spftrf_base LAPACK_GLOBAL(spftrf,SPFTRF)
void LAPACK_spftrf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    float* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spftrf(...) LAPACK_spftrf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_spftrf(...) LAPACK_spftrf_base(__VA_ARGS__)
#endif

#define LAPACK_zpftrf_base LAPACK_GLOBAL(zpftrf,ZPFTRF)
void LAPACK_zpftrf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpftrf(...) LAPACK_zpftrf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zpftrf(...) LAPACK_zpftrf_base(__VA_ARGS__)
#endif

#define LAPACK_cpftri_base LAPACK_GLOBAL(cpftri,CPFTRI)
void LAPACK_cpftri_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpftri(...) LAPACK_cpftri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cpftri(...) LAPACK_cpftri_base(__VA_ARGS__)
#endif

#define LAPACK_dpftri_base LAPACK_GLOBAL(dpftri,DPFTRI)
void LAPACK_dpftri_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    double* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpftri(...) LAPACK_dpftri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dpftri(...) LAPACK_dpftri_base(__VA_ARGS__)
#endif

#define LAPACK_spftri_base LAPACK_GLOBAL(spftri,SPFTRI)
void LAPACK_spftri_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    float* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spftri(...) LAPACK_spftri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_spftri(...) LAPACK_spftri_base(__VA_ARGS__)
#endif

#define LAPACK_zpftri_base LAPACK_GLOBAL(zpftri,ZPFTRI)
void LAPACK_zpftri_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpftri(...) LAPACK_zpftri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zpftri(...) LAPACK_zpftri_base(__VA_ARGS__)
#endif

#define LAPACK_cpftrs_base LAPACK_GLOBAL(cpftrs,CPFTRS)
void LAPACK_cpftrs_base(
    char const* transr, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpftrs(...) LAPACK_cpftrs_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cpftrs(...) LAPACK_cpftrs_base(__VA_ARGS__)
#endif

#define LAPACK_dpftrs_base LAPACK_GLOBAL(dpftrs,DPFTRS)
void LAPACK_dpftrs_base(
    char const* transr, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpftrs(...) LAPACK_dpftrs_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dpftrs(...) LAPACK_dpftrs_base(__VA_ARGS__)
#endif

#define LAPACK_spftrs_base LAPACK_GLOBAL(spftrs,SPFTRS)
void LAPACK_spftrs_base(
    char const* transr, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spftrs(...) LAPACK_spftrs_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_spftrs(...) LAPACK_spftrs_base(__VA_ARGS__)
#endif

#define LAPACK_zpftrs_base LAPACK_GLOBAL(zpftrs,ZPFTRS)
void LAPACK_zpftrs_base(
    char const* transr, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpftrs(...) LAPACK_zpftrs_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zpftrs(...) LAPACK_zpftrs_base(__VA_ARGS__)
#endif

#define LAPACK_cpocon_base LAPACK_GLOBAL(cpocon,CPOCON)
void LAPACK_cpocon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpocon(...) LAPACK_cpocon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpocon(...) LAPACK_cpocon_base(__VA_ARGS__)
#endif

#define LAPACK_dpocon_base LAPACK_GLOBAL(dpocon,DPOCON)
void LAPACK_dpocon_base(
    char const* uplo,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpocon(...) LAPACK_dpocon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpocon(...) LAPACK_dpocon_base(__VA_ARGS__)
#endif

#define LAPACK_spocon_base LAPACK_GLOBAL(spocon,SPOCON)
void LAPACK_spocon_base(
    char const* uplo,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spocon(...) LAPACK_spocon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spocon(...) LAPACK_spocon_base(__VA_ARGS__)
#endif

#define LAPACK_zpocon_base LAPACK_GLOBAL(zpocon,ZPOCON)
void LAPACK_zpocon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpocon(...) LAPACK_zpocon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpocon(...) LAPACK_zpocon_base(__VA_ARGS__)
#endif

#define LAPACK_cpoequ LAPACK_GLOBAL(cpoequ,CPOEQU)
void LAPACK_cpoequ(
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info );

#define LAPACK_dpoequ LAPACK_GLOBAL(dpoequ,DPOEQU)
void LAPACK_dpoequ(
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info );

#define LAPACK_spoequ LAPACK_GLOBAL(spoequ,SPOEQU)
void LAPACK_spoequ(
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info );

#define LAPACK_zpoequ LAPACK_GLOBAL(zpoequ,ZPOEQU)
void LAPACK_zpoequ(
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info );

#define LAPACK_cpoequb LAPACK_GLOBAL(cpoequb,CPOEQUB)
void LAPACK_cpoequb(
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info );

#define LAPACK_dpoequb LAPACK_GLOBAL(dpoequb,DPOEQUB)
void LAPACK_dpoequb(
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info );

#define LAPACK_spoequb LAPACK_GLOBAL(spoequb,SPOEQUB)
void LAPACK_spoequb(
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info );

#define LAPACK_zpoequb LAPACK_GLOBAL(zpoequb,ZPOEQUB)
void LAPACK_zpoequb(
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info );

#define LAPACK_cporfs_base LAPACK_GLOBAL(cporfs,CPORFS)
void LAPACK_cporfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cporfs(...) LAPACK_cporfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cporfs(...) LAPACK_cporfs_base(__VA_ARGS__)
#endif

#define LAPACK_dporfs_base LAPACK_GLOBAL(dporfs,DPORFS)
void LAPACK_dporfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* AF, lapack_int const* ldaf,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dporfs(...) LAPACK_dporfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dporfs(...) LAPACK_dporfs_base(__VA_ARGS__)
#endif

#define LAPACK_sporfs_base LAPACK_GLOBAL(sporfs,SPORFS)
void LAPACK_sporfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* AF, lapack_int const* ldaf,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sporfs(...) LAPACK_sporfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sporfs(...) LAPACK_sporfs_base(__VA_ARGS__)
#endif

#define LAPACK_zporfs_base LAPACK_GLOBAL(zporfs,ZPORFS)
void LAPACK_zporfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zporfs(...) LAPACK_zporfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zporfs(...) LAPACK_zporfs_base(__VA_ARGS__)
#endif

#define LAPACK_cporfsx_base LAPACK_GLOBAL(cporfsx,CPORFSX)
void LAPACK_cporfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf,
    const float* S,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cporfsx(...) LAPACK_cporfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cporfsx(...) LAPACK_cporfsx_base(__VA_ARGS__)
#endif

#define LAPACK_dporfsx_base LAPACK_GLOBAL(dporfsx,DPORFSX)
void LAPACK_dporfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* AF, lapack_int const* ldaf,
    const double* S,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dporfsx(...) LAPACK_dporfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dporfsx(...) LAPACK_dporfsx_base(__VA_ARGS__)
#endif

#define LAPACK_sporfsx_base LAPACK_GLOBAL(sporfsx,SPORFSX)
void LAPACK_sporfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* AF, lapack_int const* ldaf,
    const float* S,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sporfsx(...) LAPACK_sporfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sporfsx(...) LAPACK_sporfsx_base(__VA_ARGS__)
#endif

#define LAPACK_zporfsx_base LAPACK_GLOBAL(zporfsx,ZPORFSX)
void LAPACK_zporfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf,
    const double* S,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zporfsx(...) LAPACK_zporfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zporfsx(...) LAPACK_zporfsx_base(__VA_ARGS__)
#endif

#define LAPACK_cposv_base LAPACK_GLOBAL(cposv,CPOSV)
void LAPACK_cposv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cposv(...) LAPACK_cposv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cposv(...) LAPACK_cposv_base(__VA_ARGS__)
#endif

#define LAPACK_dposv_base LAPACK_GLOBAL(dposv,DPOSV)
void LAPACK_dposv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dposv(...) LAPACK_dposv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dposv(...) LAPACK_dposv_base(__VA_ARGS__)
#endif

#define LAPACK_sposv_base LAPACK_GLOBAL(sposv,SPOSV)
void LAPACK_sposv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sposv(...) LAPACK_sposv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sposv(...) LAPACK_sposv_base(__VA_ARGS__)
#endif

#define LAPACK_zposv_base LAPACK_GLOBAL(zposv,ZPOSV)
void LAPACK_zposv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zposv(...) LAPACK_zposv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zposv(...) LAPACK_zposv_base(__VA_ARGS__)
#endif

#define LAPACK_dsposv_base LAPACK_GLOBAL(dsposv,DSPOSV)
void LAPACK_dsposv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* work,
    float* swork, lapack_int* iter,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsposv(...) LAPACK_dsposv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsposv(...) LAPACK_dsposv_base(__VA_ARGS__)
#endif

#define LAPACK_zcposv_base LAPACK_GLOBAL(zcposv,ZCPOSV)
void LAPACK_zcposv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    lapack_complex_double* work,
    lapack_complex_float* swork,
    double* rwork, lapack_int* iter,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zcposv(...) LAPACK_zcposv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zcposv(...) LAPACK_zcposv_base(__VA_ARGS__)
#endif

#define LAPACK_cposvx_base LAPACK_GLOBAL(cposvx,CPOSVX)
void LAPACK_cposvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, char* equed,
    float* S,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cposvx(...) LAPACK_cposvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cposvx(...) LAPACK_cposvx_base(__VA_ARGS__)
#endif

#define LAPACK_dposvx_base LAPACK_GLOBAL(dposvx,DPOSVX)
void LAPACK_dposvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* AF, lapack_int const* ldaf, char* equed,
    double* S,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dposvx(...) LAPACK_dposvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dposvx(...) LAPACK_dposvx_base(__VA_ARGS__)
#endif

#define LAPACK_sposvx_base LAPACK_GLOBAL(sposvx,SPOSVX)
void LAPACK_sposvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* AF, lapack_int const* ldaf, char* equed,
    float* S,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sposvx(...) LAPACK_sposvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sposvx(...) LAPACK_sposvx_base(__VA_ARGS__)
#endif

#define LAPACK_zposvx_base LAPACK_GLOBAL(zposvx,ZPOSVX)
void LAPACK_zposvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, char* equed,
    double* S,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zposvx(...) LAPACK_zposvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zposvx(...) LAPACK_zposvx_base(__VA_ARGS__)
#endif

#define LAPACK_cposvxx_base LAPACK_GLOBAL(cposvxx,CPOSVXX)
void LAPACK_cposvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, char* equed,
    float* S,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cposvxx(...) LAPACK_cposvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cposvxx(...) LAPACK_cposvxx_base(__VA_ARGS__)
#endif

#define LAPACK_dposvxx_base LAPACK_GLOBAL(dposvxx,DPOSVXX)
void LAPACK_dposvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* AF, lapack_int const* ldaf, char* equed,
    double* S,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dposvxx(...) LAPACK_dposvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dposvxx(...) LAPACK_dposvxx_base(__VA_ARGS__)
#endif

#define LAPACK_sposvxx_base LAPACK_GLOBAL(sposvxx,SPOSVXX)
void LAPACK_sposvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* AF, lapack_int const* ldaf, char* equed,
    float* S,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sposvxx(...) LAPACK_sposvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sposvxx(...) LAPACK_sposvxx_base(__VA_ARGS__)
#endif

#define LAPACK_zposvxx_base LAPACK_GLOBAL(zposvxx,ZPOSVXX)
void LAPACK_zposvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, char* equed,
    double* S,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zposvxx(...) LAPACK_zposvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zposvxx(...) LAPACK_zposvxx_base(__VA_ARGS__)
#endif

#define LAPACK_cpotf2_base LAPACK_GLOBAL(cpotf2,CPOTF2)
void LAPACK_cpotf2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpotf2(...) LAPACK_cpotf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpotf2(...) LAPACK_cpotf2_base(__VA_ARGS__)
#endif

#define LAPACK_dpotf2_base LAPACK_GLOBAL(dpotf2,DPOTF2)
void LAPACK_dpotf2_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpotf2(...) LAPACK_dpotf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpotf2(...) LAPACK_dpotf2_base(__VA_ARGS__)
#endif

#define LAPACK_spotf2_base LAPACK_GLOBAL(spotf2,SPOTF2)
void LAPACK_spotf2_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spotf2(...) LAPACK_spotf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spotf2(...) LAPACK_spotf2_base(__VA_ARGS__)
#endif

#define LAPACK_zpotf2_base LAPACK_GLOBAL(zpotf2,ZPOTF2)
void LAPACK_zpotf2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpotf2(...) LAPACK_zpotf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpotf2(...) LAPACK_zpotf2_base(__VA_ARGS__)
#endif

#define LAPACK_cpotrf_base LAPACK_GLOBAL(cpotrf,CPOTRF)
lapack_int LAPACK_cpotrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpotrf(...) LAPACK_cpotrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpotrf(...) LAPACK_cpotrf_base(__VA_ARGS__)
#endif

#define LAPACK_dpotrf_base LAPACK_GLOBAL(dpotrf,DPOTRF)
lapack_int LAPACK_dpotrf_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpotrf(...) LAPACK_dpotrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpotrf(...) LAPACK_dpotrf_base(__VA_ARGS__)
#endif

#define LAPACK_spotrf_base LAPACK_GLOBAL(spotrf,SPOTRF)
lapack_int LAPACK_spotrf_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spotrf(...) LAPACK_spotrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spotrf(...) LAPACK_spotrf_base(__VA_ARGS__)
#endif

#define LAPACK_zpotrf_base LAPACK_GLOBAL(zpotrf,ZPOTRF)
lapack_int LAPACK_zpotrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpotrf(...) LAPACK_zpotrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpotrf(...) LAPACK_zpotrf_base(__VA_ARGS__)
#endif

#define LAPACK_cpotrf2_base LAPACK_GLOBAL(cpotrf2,CPOTRF2)
void LAPACK_cpotrf2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpotrf2(...) LAPACK_cpotrf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpotrf2(...) LAPACK_cpotrf2_base(__VA_ARGS__)
#endif

#define LAPACK_dpotrf2_base LAPACK_GLOBAL(dpotrf2,DPOTRF2)
void LAPACK_dpotrf2_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpotrf2(...) LAPACK_dpotrf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpotrf2(...) LAPACK_dpotrf2_base(__VA_ARGS__)
#endif

#define LAPACK_spotrf2_base LAPACK_GLOBAL(spotrf2,SPOTRF2)
void LAPACK_spotrf2_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spotrf2(...) LAPACK_spotrf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spotrf2(...) LAPACK_spotrf2_base(__VA_ARGS__)
#endif

#define LAPACK_zpotrf2_base LAPACK_GLOBAL(zpotrf2,ZPOTRF2)
void LAPACK_zpotrf2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpotrf2(...) LAPACK_zpotrf2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpotrf2(...) LAPACK_zpotrf2_base(__VA_ARGS__)
#endif

#define LAPACK_cpotri_base LAPACK_GLOBAL(cpotri,CPOTRI)
void LAPACK_cpotri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpotri(...) LAPACK_cpotri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpotri(...) LAPACK_cpotri_base(__VA_ARGS__)
#endif

#define LAPACK_dpotri_base LAPACK_GLOBAL(dpotri,DPOTRI)
void LAPACK_dpotri_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpotri(...) LAPACK_dpotri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpotri(...) LAPACK_dpotri_base(__VA_ARGS__)
#endif

#define LAPACK_spotri_base LAPACK_GLOBAL(spotri,SPOTRI)
void LAPACK_spotri_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spotri(...) LAPACK_spotri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spotri(...) LAPACK_spotri_base(__VA_ARGS__)
#endif

#define LAPACK_zpotri_base LAPACK_GLOBAL(zpotri,ZPOTRI)
void LAPACK_zpotri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpotri(...) LAPACK_zpotri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpotri(...) LAPACK_zpotri_base(__VA_ARGS__)
#endif

#define LAPACK_cpotrs_base LAPACK_GLOBAL(cpotrs,CPOTRS)
void LAPACK_cpotrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpotrs(...) LAPACK_cpotrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpotrs(...) LAPACK_cpotrs_base(__VA_ARGS__)
#endif

#define LAPACK_dpotrs_base LAPACK_GLOBAL(dpotrs,DPOTRS)
void LAPACK_dpotrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpotrs(...) LAPACK_dpotrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpotrs(...) LAPACK_dpotrs_base(__VA_ARGS__)
#endif

#define LAPACK_spotrs_base LAPACK_GLOBAL(spotrs,SPOTRS)
void LAPACK_spotrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spotrs(...) LAPACK_spotrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spotrs(...) LAPACK_spotrs_base(__VA_ARGS__)
#endif

#define LAPACK_zpotrs_base LAPACK_GLOBAL(zpotrs,ZPOTRS)
void LAPACK_zpotrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpotrs(...) LAPACK_zpotrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpotrs(...) LAPACK_zpotrs_base(__VA_ARGS__)
#endif

#define LAPACK_cppcon_base LAPACK_GLOBAL(cppcon,CPPCON)
void LAPACK_cppcon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cppcon(...) LAPACK_cppcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cppcon(...) LAPACK_cppcon_base(__VA_ARGS__)
#endif

#define LAPACK_dppcon_base LAPACK_GLOBAL(dppcon,DPPCON)
void LAPACK_dppcon_base(
    char const* uplo,
    lapack_int const* n,
    double const* AP,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dppcon(...) LAPACK_dppcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dppcon(...) LAPACK_dppcon_base(__VA_ARGS__)
#endif

#define LAPACK_sppcon_base LAPACK_GLOBAL(sppcon,SPPCON)
void LAPACK_sppcon_base(
    char const* uplo,
    lapack_int const* n,
    float const* AP,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sppcon(...) LAPACK_sppcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sppcon(...) LAPACK_sppcon_base(__VA_ARGS__)
#endif

#define LAPACK_zppcon_base LAPACK_GLOBAL(zppcon,ZPPCON)
void LAPACK_zppcon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zppcon(...) LAPACK_zppcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zppcon(...) LAPACK_zppcon_base(__VA_ARGS__)
#endif

#define LAPACK_cppequ_base LAPACK_GLOBAL(cppequ,CPPEQU)
void LAPACK_cppequ_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cppequ(...) LAPACK_cppequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cppequ(...) LAPACK_cppequ_base(__VA_ARGS__)
#endif

#define LAPACK_dppequ_base LAPACK_GLOBAL(dppequ,DPPEQU)
void LAPACK_dppequ_base(
    char const* uplo,
    lapack_int const* n,
    double const* AP,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dppequ(...) LAPACK_dppequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dppequ(...) LAPACK_dppequ_base(__VA_ARGS__)
#endif

#define LAPACK_sppequ_base LAPACK_GLOBAL(sppequ,SPPEQU)
void LAPACK_sppequ_base(
    char const* uplo,
    lapack_int const* n,
    float const* AP,
    float* S,
    float* scond,
    float* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sppequ(...) LAPACK_sppequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sppequ(...) LAPACK_sppequ_base(__VA_ARGS__)
#endif

#define LAPACK_zppequ_base LAPACK_GLOBAL(zppequ,ZPPEQU)
void LAPACK_zppequ_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP,
    double* S,
    double* scond,
    double* amax,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zppequ(...) LAPACK_zppequ_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zppequ(...) LAPACK_zppequ_base(__VA_ARGS__)
#endif

#define LAPACK_cpprfs_base LAPACK_GLOBAL(cpprfs,CPPRFS)
void LAPACK_cpprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float const* AFP,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpprfs(...) LAPACK_cpprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpprfs(...) LAPACK_cpprfs_base(__VA_ARGS__)
#endif

#define LAPACK_dpprfs_base LAPACK_GLOBAL(dpprfs,DPPRFS)
void LAPACK_dpprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* AP,
    double const* AFP,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpprfs(...) LAPACK_dpprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpprfs(...) LAPACK_dpprfs_base(__VA_ARGS__)
#endif

#define LAPACK_spprfs_base LAPACK_GLOBAL(spprfs,SPPRFS)
void LAPACK_spprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* AP,
    float const* AFP,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spprfs(...) LAPACK_spprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spprfs(...) LAPACK_spprfs_base(__VA_ARGS__)
#endif

#define LAPACK_zpprfs_base LAPACK_GLOBAL(zpprfs,ZPPRFS)
void LAPACK_zpprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double const* AFP,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpprfs(...) LAPACK_zpprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpprfs(...) LAPACK_zpprfs_base(__VA_ARGS__)
#endif

#define LAPACK_cppsv_base LAPACK_GLOBAL(cppsv,CPPSV)
void LAPACK_cppsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* AP,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cppsv(...) LAPACK_cppsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cppsv(...) LAPACK_cppsv_base(__VA_ARGS__)
#endif

#define LAPACK_dppsv_base LAPACK_GLOBAL(dppsv,DPPSV)
void LAPACK_dppsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* AP,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dppsv(...) LAPACK_dppsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dppsv(...) LAPACK_dppsv_base(__VA_ARGS__)
#endif

#define LAPACK_sppsv_base LAPACK_GLOBAL(sppsv,SPPSV)
void LAPACK_sppsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* AP,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sppsv(...) LAPACK_sppsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sppsv(...) LAPACK_sppsv_base(__VA_ARGS__)
#endif

#define LAPACK_zppsv_base LAPACK_GLOBAL(zppsv,ZPPSV)
void LAPACK_zppsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* AP,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zppsv(...) LAPACK_zppsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zppsv(...) LAPACK_zppsv_base(__VA_ARGS__)
#endif

#define LAPACK_cppsvx_base LAPACK_GLOBAL(cppsvx,CPPSVX)
void LAPACK_cppsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* AP,
    lapack_complex_float* AFP, char* equed,
    float* S,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cppsvx(...) LAPACK_cppsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cppsvx(...) LAPACK_cppsvx_base(__VA_ARGS__)
#endif

#define LAPACK_dppsvx_base LAPACK_GLOBAL(dppsvx,DPPSVX)
void LAPACK_dppsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* AP,
    double* AFP, char* equed,
    double* S,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dppsvx(...) LAPACK_dppsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dppsvx(...) LAPACK_dppsvx_base(__VA_ARGS__)
#endif

#define LAPACK_sppsvx_base LAPACK_GLOBAL(sppsvx,SPPSVX)
void LAPACK_sppsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* AP,
    float* AFP, char* equed,
    float* S,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sppsvx(...) LAPACK_sppsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sppsvx(...) LAPACK_sppsvx_base(__VA_ARGS__)
#endif

#define LAPACK_zppsvx_base LAPACK_GLOBAL(zppsvx,ZPPSVX)
void LAPACK_zppsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* AP,
    lapack_complex_double* AFP, char* equed,
    double* S,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zppsvx(...) LAPACK_zppsvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zppsvx(...) LAPACK_zppsvx_base(__VA_ARGS__)
#endif

#define LAPACK_cpptrf_base LAPACK_GLOBAL(cpptrf,CPPTRF)
void LAPACK_cpptrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpptrf(...) LAPACK_cpptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpptrf(...) LAPACK_cpptrf_base(__VA_ARGS__)
#endif

#define LAPACK_dpptrf_base LAPACK_GLOBAL(dpptrf,DPPTRF)
void LAPACK_dpptrf_base(
    char const* uplo,
    lapack_int const* n,
    double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpptrf(...) LAPACK_dpptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpptrf(...) LAPACK_dpptrf_base(__VA_ARGS__)
#endif

#define LAPACK_spptrf_base LAPACK_GLOBAL(spptrf,SPPTRF)
void LAPACK_spptrf_base(
    char const* uplo,
    lapack_int const* n,
    float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spptrf(...) LAPACK_spptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spptrf(...) LAPACK_spptrf_base(__VA_ARGS__)
#endif

#define LAPACK_zpptrf_base LAPACK_GLOBAL(zpptrf,ZPPTRF)
void LAPACK_zpptrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpptrf(...) LAPACK_zpptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpptrf(...) LAPACK_zpptrf_base(__VA_ARGS__)
#endif

#define LAPACK_cpptri_base LAPACK_GLOBAL(cpptri,CPPTRI)
void LAPACK_cpptri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpptri(...) LAPACK_cpptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpptri(...) LAPACK_cpptri_base(__VA_ARGS__)
#endif

#define LAPACK_dpptri_base LAPACK_GLOBAL(dpptri,DPPTRI)
void LAPACK_dpptri_base(
    char const* uplo,
    lapack_int const* n,
    double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpptri(...) LAPACK_dpptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpptri(...) LAPACK_dpptri_base(__VA_ARGS__)
#endif

#define LAPACK_spptri_base LAPACK_GLOBAL(spptri,SPPTRI)
void LAPACK_spptri_base(
    char const* uplo,
    lapack_int const* n,
    float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spptri(...) LAPACK_spptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spptri(...) LAPACK_spptri_base(__VA_ARGS__)
#endif

#define LAPACK_zpptri_base LAPACK_GLOBAL(zpptri,ZPPTRI)
void LAPACK_zpptri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpptri(...) LAPACK_zpptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpptri(...) LAPACK_zpptri_base(__VA_ARGS__)
#endif

#define LAPACK_cpptrs_base LAPACK_GLOBAL(cpptrs,CPPTRS)
void LAPACK_cpptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpptrs(...) LAPACK_cpptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpptrs(...) LAPACK_cpptrs_base(__VA_ARGS__)
#endif

#define LAPACK_dpptrs_base LAPACK_GLOBAL(dpptrs,DPPTRS)
void LAPACK_dpptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* AP,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpptrs(...) LAPACK_dpptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpptrs(...) LAPACK_dpptrs_base(__VA_ARGS__)
#endif

#define LAPACK_spptrs_base LAPACK_GLOBAL(spptrs,SPPTRS)
void LAPACK_spptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* AP,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spptrs(...) LAPACK_spptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spptrs(...) LAPACK_spptrs_base(__VA_ARGS__)
#endif

#define LAPACK_zpptrs_base LAPACK_GLOBAL(zpptrs,ZPPTRS)
void LAPACK_zpptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpptrs(...) LAPACK_zpptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpptrs(...) LAPACK_zpptrs_base(__VA_ARGS__)
#endif

#define LAPACK_cpstrf_base LAPACK_GLOBAL(cpstrf,CPSTRF)
void LAPACK_cpstrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* piv, lapack_int* rank,
    float const* tol,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpstrf(...) LAPACK_cpstrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpstrf(...) LAPACK_cpstrf_base(__VA_ARGS__)
#endif

#define LAPACK_dpstrf_base LAPACK_GLOBAL(dpstrf,DPSTRF)
void LAPACK_dpstrf_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* piv, lapack_int* rank,
    double const* tol,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpstrf(...) LAPACK_dpstrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpstrf(...) LAPACK_dpstrf_base(__VA_ARGS__)
#endif

#define LAPACK_spstrf_base LAPACK_GLOBAL(spstrf,SPSTRF)
void LAPACK_spstrf_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* piv, lapack_int* rank,
    float const* tol,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spstrf(...) LAPACK_spstrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spstrf(...) LAPACK_spstrf_base(__VA_ARGS__)
#endif

#define LAPACK_zpstrf_base LAPACK_GLOBAL(zpstrf,ZPSTRF)
void LAPACK_zpstrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* piv, lapack_int* rank,
    double const* tol,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpstrf(...) LAPACK_zpstrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpstrf(...) LAPACK_zpstrf_base(__VA_ARGS__)
#endif

#define LAPACK_cptcon LAPACK_GLOBAL(cptcon,CPTCON)
void LAPACK_cptcon(
    lapack_int const* n,
    float const* D,
    lapack_complex_float const* E,
    float const* anorm,
    float* rcond,
    float* rwork,
    lapack_int* info );

#define LAPACK_dptcon LAPACK_GLOBAL(dptcon,DPTCON)
void LAPACK_dptcon(
    lapack_int const* n,
    double const* D,
    double const* E,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* info );

#define LAPACK_sptcon LAPACK_GLOBAL(sptcon,SPTCON)
void LAPACK_sptcon(
    lapack_int const* n,
    float const* D,
    float const* E,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* info );

#define LAPACK_zptcon LAPACK_GLOBAL(zptcon,ZPTCON)
void LAPACK_zptcon(
    lapack_int const* n,
    double const* D,
    lapack_complex_double const* E,
    double const* anorm,
    double* rcond,
    double* rwork,
    lapack_int* info );

#define LAPACK_cpteqr_base LAPACK_GLOBAL(cpteqr,CPTEQR)
void LAPACK_cpteqr_base(
    char const* compz,
    lapack_int const* n,
    float* D,
    float* E,
    lapack_complex_float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpteqr(...) LAPACK_cpteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpteqr(...) LAPACK_cpteqr_base(__VA_ARGS__)
#endif

#define LAPACK_dpteqr_base LAPACK_GLOBAL(dpteqr,DPTEQR)
void LAPACK_dpteqr_base(
    char const* compz,
    lapack_int const* n,
    double* D,
    double* E,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dpteqr(...) LAPACK_dpteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dpteqr(...) LAPACK_dpteqr_base(__VA_ARGS__)
#endif

#define LAPACK_spteqr_base LAPACK_GLOBAL(spteqr,SPTEQR)
void LAPACK_spteqr_base(
    char const* compz,
    lapack_int const* n,
    float* D,
    float* E,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_spteqr(...) LAPACK_spteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_spteqr(...) LAPACK_spteqr_base(__VA_ARGS__)
#endif

#define LAPACK_zpteqr_base LAPACK_GLOBAL(zpteqr,ZPTEQR)
void LAPACK_zpteqr_base(
    char const* compz,
    lapack_int const* n,
    double* D,
    double* E,
    lapack_complex_double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpteqr(...) LAPACK_zpteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpteqr(...) LAPACK_zpteqr_base(__VA_ARGS__)
#endif

#define LAPACK_cptrfs_base LAPACK_GLOBAL(cptrfs,CPTRFS)
void LAPACK_cptrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* D,
    lapack_complex_float const* E,
    float const* DF,
    lapack_complex_float const* EF,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cptrfs(...) LAPACK_cptrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cptrfs(...) LAPACK_cptrfs_base(__VA_ARGS__)
#endif

#define LAPACK_dptrfs LAPACK_GLOBAL(dptrfs,DPTRFS)
void LAPACK_dptrfs(
    lapack_int const* n, lapack_int const* nrhs,
    double const* D,
    double const* E,
    double const* DF,
    double const* EF,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* info );

#define LAPACK_sptrfs LAPACK_GLOBAL(sptrfs,SPTRFS)
void LAPACK_sptrfs(
    lapack_int const* n, lapack_int const* nrhs,
    float const* D,
    float const* E,
    float const* DF,
    float const* EF,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* info );

#define LAPACK_zptrfs_base LAPACK_GLOBAL(zptrfs,ZPTRFS)
void LAPACK_zptrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* D,
    lapack_complex_double const* E,
    double const* DF,
    lapack_complex_double const* EF,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zptrfs(...) LAPACK_zptrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zptrfs(...) LAPACK_zptrfs_base(__VA_ARGS__)
#endif

#define LAPACK_cptsv LAPACK_GLOBAL(cptsv,CPTSV)
void LAPACK_cptsv(
    lapack_int const* n, lapack_int const* nrhs,
    float* D,
    lapack_complex_float* E,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_dptsv LAPACK_GLOBAL(dptsv,DPTSV)
void LAPACK_dptsv(
    lapack_int const* n, lapack_int const* nrhs,
    double* D,
    double* E,
    double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_sptsv LAPACK_GLOBAL(sptsv,SPTSV)
void LAPACK_sptsv(
    lapack_int const* n, lapack_int const* nrhs,
    float* D,
    float* E,
    float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_zptsv LAPACK_GLOBAL(zptsv,ZPTSV)
void LAPACK_zptsv(
    lapack_int const* n, lapack_int const* nrhs,
    double* D,
    lapack_complex_double* E,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_cptsvx_base LAPACK_GLOBAL(cptsvx,CPTSVX)
void LAPACK_cptsvx_base(
    char const* fact,
    lapack_int const* n, lapack_int const* nrhs,
    float const* D,
    lapack_complex_float const* E,
    float* DF,
    lapack_complex_float* EF,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cptsvx(...) LAPACK_cptsvx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cptsvx(...) LAPACK_cptsvx_base(__VA_ARGS__)
#endif

#define LAPACK_dptsvx_base LAPACK_GLOBAL(dptsvx,DPTSVX)
void LAPACK_dptsvx_base(
    char const* fact,
    lapack_int const* n, lapack_int const* nrhs,
    double const* D,
    double const* E,
    double* DF,
    double* EF,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dptsvx(...) LAPACK_dptsvx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dptsvx(...) LAPACK_dptsvx_base(__VA_ARGS__)
#endif

#define LAPACK_sptsvx_base LAPACK_GLOBAL(sptsvx,SPTSVX)
void LAPACK_sptsvx_base(
    char const* fact,
    lapack_int const* n, lapack_int const* nrhs,
    float const* D,
    float const* E,
    float* DF,
    float* EF,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sptsvx(...) LAPACK_sptsvx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sptsvx(...) LAPACK_sptsvx_base(__VA_ARGS__)
#endif

#define LAPACK_zptsvx_base LAPACK_GLOBAL(zptsvx,ZPTSVX)
void LAPACK_zptsvx_base(
    char const* fact,
    lapack_int const* n, lapack_int const* nrhs,
    double const* D,
    lapack_complex_double const* E,
    double* DF,
    lapack_complex_double* EF,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zptsvx(...) LAPACK_zptsvx_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zptsvx(...) LAPACK_zptsvx_base(__VA_ARGS__)
#endif

#define LAPACK_cpttrf LAPACK_GLOBAL(cpttrf,CPTTRF)
void LAPACK_cpttrf(
    lapack_int const* n,
    float* D,
    lapack_complex_float* E,
    lapack_int* info );

#define LAPACK_dpttrf LAPACK_GLOBAL(dpttrf,DPTTRF)
void LAPACK_dpttrf(
    lapack_int const* n,
    double* D,
    double* E,
    lapack_int* info );

#define LAPACK_spttrf LAPACK_GLOBAL(spttrf,SPTTRF)
void LAPACK_spttrf(
    lapack_int const* n,
    float* D,
    float* E,
    lapack_int* info );

#define LAPACK_zpttrf LAPACK_GLOBAL(zpttrf,ZPTTRF)
void LAPACK_zpttrf(
    lapack_int const* n,
    double* D,
    lapack_complex_double* E,
    lapack_int* info );

#define LAPACK_cpttrs_base LAPACK_GLOBAL(cpttrs,CPTTRS)
void LAPACK_cpttrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* D,
    lapack_complex_float const* E,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cpttrs(...) LAPACK_cpttrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cpttrs(...) LAPACK_cpttrs_base(__VA_ARGS__)
#endif

#define LAPACK_dpttrs LAPACK_GLOBAL(dpttrs,DPTTRS)
void LAPACK_dpttrs(
    lapack_int const* n, lapack_int const* nrhs,
    double const* D,
    double const* E,
    double* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_spttrs LAPACK_GLOBAL(spttrs,SPTTRS)
void LAPACK_spttrs(
    lapack_int const* n, lapack_int const* nrhs,
    float const* D,
    float const* E,
    float* B, lapack_int const* ldb,
    lapack_int* info );

#define LAPACK_zpttrs_base LAPACK_GLOBAL(zpttrs,ZPTTRS)
void LAPACK_zpttrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* D,
    lapack_complex_double const* E,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zpttrs(...) LAPACK_zpttrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zpttrs(...) LAPACK_zpttrs_base(__VA_ARGS__)
#endif

#define LAPACK_dsbev_base LAPACK_GLOBAL(dsbev,DSBEV)
void LAPACK_dsbev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbev(...) LAPACK_dsbev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbev(...) LAPACK_dsbev_base(__VA_ARGS__)
#endif

#define LAPACK_ssbev_base LAPACK_GLOBAL(ssbev,SSBEV)
void LAPACK_ssbev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbev(...) LAPACK_ssbev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbev(...) LAPACK_ssbev_base(__VA_ARGS__)
#endif

#define LAPACK_dsbev_2stage_base LAPACK_GLOBAL(dsbev_2stage,DSBEV_2STAGE)
void LAPACK_dsbev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbev_2stage(...) LAPACK_dsbev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbev_2stage(...) LAPACK_dsbev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssbev_2stage_base LAPACK_GLOBAL(ssbev_2stage,SSBEV_2STAGE)
void LAPACK_ssbev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbev_2stage(...) LAPACK_ssbev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbev_2stage(...) LAPACK_ssbev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsbevd_base LAPACK_GLOBAL(dsbevd,DSBEVD)
void LAPACK_dsbevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbevd(...) LAPACK_dsbevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbevd(...) LAPACK_dsbevd_base(__VA_ARGS__)
#endif

#define LAPACK_ssbevd_base LAPACK_GLOBAL(ssbevd,SSBEVD)
void LAPACK_ssbevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbevd(...) LAPACK_ssbevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbevd(...) LAPACK_ssbevd_base(__VA_ARGS__)
#endif

#define LAPACK_dsbevd_2stage_base LAPACK_GLOBAL(dsbevd_2stage,DSBEVD_2STAGE)
void LAPACK_dsbevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbevd_2stage(...) LAPACK_dsbevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbevd_2stage(...) LAPACK_dsbevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssbevd_2stage_base LAPACK_GLOBAL(ssbevd_2stage,SSBEVD_2STAGE)
void LAPACK_ssbevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbevd_2stage(...) LAPACK_ssbevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbevd_2stage(...) LAPACK_ssbevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsbevx_base LAPACK_GLOBAL(dsbevx,DSBEVX)
void LAPACK_dsbevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    double* Q, lapack_int const* ldq,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbevx(...) LAPACK_dsbevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsbevx(...) LAPACK_dsbevx_base(__VA_ARGS__)
#endif

#define LAPACK_ssbevx_base LAPACK_GLOBAL(ssbevx,SSBEVX)
void LAPACK_ssbevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    float* Q, lapack_int const* ldq,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbevx(...) LAPACK_ssbevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssbevx(...) LAPACK_ssbevx_base(__VA_ARGS__)
#endif

#define LAPACK_dsbevx_2stage_base LAPACK_GLOBAL(dsbevx_2stage,DSBEVX_2STAGE)
void LAPACK_dsbevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    double* Q, lapack_int const* ldq,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbevx_2stage(...) LAPACK_dsbevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsbevx_2stage(...) LAPACK_dsbevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssbevx_2stage_base LAPACK_GLOBAL(ssbevx_2stage,SSBEVX_2STAGE)
void LAPACK_ssbevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    float* Q, lapack_int const* ldq,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbevx_2stage(...) LAPACK_ssbevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssbevx_2stage(...) LAPACK_ssbevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsbgst_base LAPACK_GLOBAL(dsbgst,DSBGST)
void LAPACK_dsbgst_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    double* AB, lapack_int const* ldab,
    double const* BB, lapack_int const* ldbb,
    double* X, lapack_int const* ldx,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbgst(...) LAPACK_dsbgst_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbgst(...) LAPACK_dsbgst_base(__VA_ARGS__)
#endif

#define LAPACK_ssbgst_base LAPACK_GLOBAL(ssbgst,SSBGST)
void LAPACK_ssbgst_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    float* AB, lapack_int const* ldab,
    float const* BB, lapack_int const* ldbb,
    float* X, lapack_int const* ldx,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbgst(...) LAPACK_ssbgst_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbgst(...) LAPACK_ssbgst_base(__VA_ARGS__)
#endif

#define LAPACK_dsbgv_base LAPACK_GLOBAL(dsbgv,DSBGV)
void LAPACK_dsbgv_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    double* AB, lapack_int const* ldab,
    double* BB, lapack_int const* ldbb,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbgv(...) LAPACK_dsbgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbgv(...) LAPACK_dsbgv_base(__VA_ARGS__)
#endif

#define LAPACK_ssbgv_base LAPACK_GLOBAL(ssbgv,SSBGV)
void LAPACK_ssbgv_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    float* AB, lapack_int const* ldab,
    float* BB, lapack_int const* ldbb,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbgv(...) LAPACK_ssbgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbgv(...) LAPACK_ssbgv_base(__VA_ARGS__)
#endif

#define LAPACK_dsbgvd_base LAPACK_GLOBAL(dsbgvd,DSBGVD)
void LAPACK_dsbgvd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    double* AB, lapack_int const* ldab,
    double* BB, lapack_int const* ldbb,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbgvd(...) LAPACK_dsbgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbgvd(...) LAPACK_dsbgvd_base(__VA_ARGS__)
#endif

#define LAPACK_ssbgvd_base LAPACK_GLOBAL(ssbgvd,SSBGVD)
void LAPACK_ssbgvd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    float* AB, lapack_int const* ldab,
    float* BB, lapack_int const* ldbb,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbgvd(...) LAPACK_ssbgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbgvd(...) LAPACK_ssbgvd_base(__VA_ARGS__)
#endif

#define LAPACK_dsbgvx_base LAPACK_GLOBAL(dsbgvx,DSBGVX)
void LAPACK_dsbgvx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    double* AB, lapack_int const* ldab,
    double* BB, lapack_int const* ldbb,
    double* Q, lapack_int const* ldq,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbgvx(...) LAPACK_dsbgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsbgvx(...) LAPACK_dsbgvx_base(__VA_ARGS__)
#endif

#define LAPACK_ssbgvx_base LAPACK_GLOBAL(ssbgvx,SSBGVX)
void LAPACK_ssbgvx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n, lapack_int const* ka, lapack_int const* kb,
    float* AB, lapack_int const* ldab,
    float* BB, lapack_int const* ldbb,
    float* Q, lapack_int const* ldq,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbgvx(...) LAPACK_ssbgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssbgvx(...) LAPACK_ssbgvx_base(__VA_ARGS__)
#endif

#define LAPACK_dsbtrd_base LAPACK_GLOBAL(dsbtrd,DSBTRD)
void LAPACK_dsbtrd_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    double* AB, lapack_int const* ldab,
    double* D,
    double* E,
    double* Q, lapack_int const* ldq,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsbtrd(...) LAPACK_dsbtrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsbtrd(...) LAPACK_dsbtrd_base(__VA_ARGS__)
#endif

#define LAPACK_ssbtrd_base LAPACK_GLOBAL(ssbtrd,SSBTRD)
void LAPACK_ssbtrd_base(
    char const* vect, char const* uplo,
    lapack_int const* n, lapack_int const* kd,
    float* AB, lapack_int const* ldab,
    float* D,
    float* E,
    float* Q, lapack_int const* ldq,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssbtrd(...) LAPACK_ssbtrd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssbtrd(...) LAPACK_ssbtrd_base(__VA_ARGS__)
#endif

#define LAPACK_dsfrk_base LAPACK_GLOBAL(dsfrk,DSFRK)
void LAPACK_dsfrk_base(
    char const* transr, char const* uplo, char const* trans,
    lapack_int const* n, lapack_int const* k,
    double const* alpha,
    double const* A, lapack_int const* lda,
    double const* beta,
    double* C
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsfrk(...) LAPACK_dsfrk_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsfrk(...) LAPACK_dsfrk_base(__VA_ARGS__)
#endif

#define LAPACK_ssfrk_base LAPACK_GLOBAL(ssfrk,SSFRK)
void LAPACK_ssfrk_base(
    char const* transr, char const* uplo, char const* trans,
    lapack_int const* n, lapack_int const* k,
    float const* alpha,
    float const* A, lapack_int const* lda,
    float const* beta,
    float* C
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssfrk(...) LAPACK_ssfrk_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssfrk(...) LAPACK_ssfrk_base(__VA_ARGS__)
#endif

#define LAPACK_cspcon_base LAPACK_GLOBAL(cspcon,CSPCON)
void LAPACK_cspcon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cspcon(...) LAPACK_cspcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cspcon(...) LAPACK_cspcon_base(__VA_ARGS__)
#endif

#define LAPACK_dspcon_base LAPACK_GLOBAL(dspcon,DSPCON)
void LAPACK_dspcon_base(
    char const* uplo,
    lapack_int const* n,
    double const* AP, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspcon(...) LAPACK_dspcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dspcon(...) LAPACK_dspcon_base(__VA_ARGS__)
#endif

#define LAPACK_sspcon_base LAPACK_GLOBAL(sspcon,SSPCON)
void LAPACK_sspcon_base(
    char const* uplo,
    lapack_int const* n,
    float const* AP, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspcon(...) LAPACK_sspcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sspcon(...) LAPACK_sspcon_base(__VA_ARGS__)
#endif

#define LAPACK_zspcon_base LAPACK_GLOBAL(zspcon,ZSPCON)
void LAPACK_zspcon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zspcon(...) LAPACK_zspcon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zspcon(...) LAPACK_zspcon_base(__VA_ARGS__)
#endif

#define LAPACK_dspev_base LAPACK_GLOBAL(dspev,DSPEV)
void LAPACK_dspev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    double* AP,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspev(...) LAPACK_dspev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dspev(...) LAPACK_dspev_base(__VA_ARGS__)
#endif

#define LAPACK_sspev_base LAPACK_GLOBAL(sspev,SSPEV)
void LAPACK_sspev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    float* AP,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspev(...) LAPACK_sspev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sspev(...) LAPACK_sspev_base(__VA_ARGS__)
#endif

#define LAPACK_dspevd_base LAPACK_GLOBAL(dspevd,DSPEVD)
void LAPACK_dspevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    double* AP,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspevd(...) LAPACK_dspevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dspevd(...) LAPACK_dspevd_base(__VA_ARGS__)
#endif

#define LAPACK_sspevd_base LAPACK_GLOBAL(sspevd,SSPEVD)
void LAPACK_sspevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    float* AP,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspevd(...) LAPACK_sspevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sspevd(...) LAPACK_sspevd_base(__VA_ARGS__)
#endif

#define LAPACK_dspevx_base LAPACK_GLOBAL(dspevx,DSPEVX)
void LAPACK_dspevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    double* AP,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspevx(...) LAPACK_dspevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dspevx(...) LAPACK_dspevx_base(__VA_ARGS__)
#endif

#define LAPACK_sspevx_base LAPACK_GLOBAL(sspevx,SSPEVX)
void LAPACK_sspevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    float* AP,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspevx(...) LAPACK_sspevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sspevx(...) LAPACK_sspevx_base(__VA_ARGS__)
#endif

#define LAPACK_dspgst_base LAPACK_GLOBAL(dspgst,DSPGST)
void LAPACK_dspgst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    double* AP,
    double const* BP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspgst(...) LAPACK_dspgst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dspgst(...) LAPACK_dspgst_base(__VA_ARGS__)
#endif

#define LAPACK_sspgst_base LAPACK_GLOBAL(sspgst,SSPGST)
void LAPACK_sspgst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    float* AP,
    float const* BP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspgst(...) LAPACK_sspgst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sspgst(...) LAPACK_sspgst_base(__VA_ARGS__)
#endif

#define LAPACK_dspgv_base LAPACK_GLOBAL(dspgv,DSPGV)
void LAPACK_dspgv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    double* AP,
    double* BP,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspgv(...) LAPACK_dspgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dspgv(...) LAPACK_dspgv_base(__VA_ARGS__)
#endif

#define LAPACK_sspgv_base LAPACK_GLOBAL(sspgv,SSPGV)
void LAPACK_sspgv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    float* AP,
    float* BP,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspgv(...) LAPACK_sspgv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sspgv(...) LAPACK_sspgv_base(__VA_ARGS__)
#endif

#define LAPACK_dspgvd_base LAPACK_GLOBAL(dspgvd,DSPGVD)
void LAPACK_dspgvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    double* AP,
    double* BP,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspgvd(...) LAPACK_dspgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dspgvd(...) LAPACK_dspgvd_base(__VA_ARGS__)
#endif

#define LAPACK_sspgvd_base LAPACK_GLOBAL(sspgvd,SSPGVD)
void LAPACK_sspgvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    float* AP,
    float* BP,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspgvd(...) LAPACK_sspgvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sspgvd(...) LAPACK_sspgvd_base(__VA_ARGS__)
#endif

#define LAPACK_dspgvx_base LAPACK_GLOBAL(dspgvx,DSPGVX)
void LAPACK_dspgvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    double* AP,
    double* BP,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspgvx(...) LAPACK_dspgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dspgvx(...) LAPACK_dspgvx_base(__VA_ARGS__)
#endif

#define LAPACK_sspgvx_base LAPACK_GLOBAL(sspgvx,SSPGVX)
void LAPACK_sspgvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    float* AP,
    float* BP,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspgvx(...) LAPACK_sspgvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_sspgvx(...) LAPACK_sspgvx_base(__VA_ARGS__)
#endif

#define LAPACK_csprfs_base LAPACK_GLOBAL(csprfs,CSPRFS)
void LAPACK_csprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float const* AFP, lapack_int const* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csprfs(...) LAPACK_csprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csprfs(...) LAPACK_csprfs_base(__VA_ARGS__)
#endif

#define LAPACK_dsprfs_base LAPACK_GLOBAL(dsprfs,DSPRFS)
void LAPACK_dsprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* AP,
    double const* AFP, lapack_int const* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsprfs(...) LAPACK_dsprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsprfs(...) LAPACK_dsprfs_base(__VA_ARGS__)
#endif

#define LAPACK_ssprfs_base LAPACK_GLOBAL(ssprfs,SSPRFS)
void LAPACK_ssprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* AP,
    float const* AFP, lapack_int const* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssprfs(...) LAPACK_ssprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssprfs(...) LAPACK_ssprfs_base(__VA_ARGS__)
#endif

#define LAPACK_zsprfs_base LAPACK_GLOBAL(zsprfs,ZSPRFS)
void LAPACK_zsprfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double const* AFP, lapack_int const* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsprfs(...) LAPACK_zsprfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsprfs(...) LAPACK_zsprfs_base(__VA_ARGS__)
#endif

#define LAPACK_cspsv_base LAPACK_GLOBAL(cspsv,CSPSV)
void LAPACK_cspsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* AP, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cspsv(...) LAPACK_cspsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cspsv(...) LAPACK_cspsv_base(__VA_ARGS__)
#endif

#define LAPACK_dspsv_base LAPACK_GLOBAL(dspsv,DSPSV)
void LAPACK_dspsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* AP, lapack_int* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspsv(...) LAPACK_dspsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dspsv(...) LAPACK_dspsv_base(__VA_ARGS__)
#endif

#define LAPACK_sspsv_base LAPACK_GLOBAL(sspsv,SSPSV)
void LAPACK_sspsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* AP, lapack_int* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspsv(...) LAPACK_sspsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sspsv(...) LAPACK_sspsv_base(__VA_ARGS__)
#endif

#define LAPACK_zspsv_base LAPACK_GLOBAL(zspsv,ZSPSV)
void LAPACK_zspsv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* AP, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zspsv(...) LAPACK_zspsv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zspsv(...) LAPACK_zspsv_base(__VA_ARGS__)
#endif

#define LAPACK_cspsvx_base LAPACK_GLOBAL(cspsvx,CSPSVX)
void LAPACK_cspsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float* AFP, lapack_int* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cspsvx(...) LAPACK_cspsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cspsvx(...) LAPACK_cspsvx_base(__VA_ARGS__)
#endif

#define LAPACK_dspsvx_base LAPACK_GLOBAL(dspsvx,DSPSVX)
void LAPACK_dspsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* AP,
    double* AFP, lapack_int* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dspsvx(...) LAPACK_dspsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dspsvx(...) LAPACK_dspsvx_base(__VA_ARGS__)
#endif

#define LAPACK_sspsvx_base LAPACK_GLOBAL(sspsvx,SSPSVX)
void LAPACK_sspsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* AP,
    float* AFP, lapack_int* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sspsvx(...) LAPACK_sspsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sspsvx(...) LAPACK_sspsvx_base(__VA_ARGS__)
#endif

#define LAPACK_zspsvx_base LAPACK_GLOBAL(zspsvx,ZSPSVX)
void LAPACK_zspsvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double* AFP, lapack_int* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zspsvx(...) LAPACK_zspsvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zspsvx(...) LAPACK_zspsvx_base(__VA_ARGS__)
#endif

#define LAPACK_dsptrd_base LAPACK_GLOBAL(dsptrd,DSPTRD)
void LAPACK_dsptrd_base(
    char const* uplo,
    lapack_int const* n,
    double* AP,
    double* D,
    double* E,
    double* tau,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsptrd(...) LAPACK_dsptrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsptrd(...) LAPACK_dsptrd_base(__VA_ARGS__)
#endif

#define LAPACK_ssptrd_base LAPACK_GLOBAL(ssptrd,SSPTRD)
void LAPACK_ssptrd_base(
    char const* uplo,
    lapack_int const* n,
    float* AP,
    float* D,
    float* E,
    float* tau,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssptrd(...) LAPACK_ssptrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssptrd(...) LAPACK_ssptrd_base(__VA_ARGS__)
#endif

#define LAPACK_csptrf_base LAPACK_GLOBAL(csptrf,CSPTRF)
void LAPACK_csptrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP, lapack_int* ipiv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csptrf(...) LAPACK_csptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csptrf(...) LAPACK_csptrf_base(__VA_ARGS__)
#endif

#define LAPACK_dsptrf_base LAPACK_GLOBAL(dsptrf,DSPTRF)
void LAPACK_dsptrf_base(
    char const* uplo,
    lapack_int const* n,
    double* AP, lapack_int* ipiv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsptrf(...) LAPACK_dsptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsptrf(...) LAPACK_dsptrf_base(__VA_ARGS__)
#endif

#define LAPACK_ssptrf_base LAPACK_GLOBAL(ssptrf,SSPTRF)
void LAPACK_ssptrf_base(
    char const* uplo,
    lapack_int const* n,
    float* AP, lapack_int* ipiv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssptrf(...) LAPACK_ssptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssptrf(...) LAPACK_ssptrf_base(__VA_ARGS__)
#endif

#define LAPACK_zsptrf_base LAPACK_GLOBAL(zsptrf,ZSPTRF)
void LAPACK_zsptrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP, lapack_int* ipiv,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsptrf(...) LAPACK_zsptrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsptrf(...) LAPACK_zsptrf_base(__VA_ARGS__)
#endif

#define LAPACK_csptri_base LAPACK_GLOBAL(csptri,CSPTRI)
void LAPACK_csptri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* AP, lapack_int const* ipiv,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csptri(...) LAPACK_csptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csptri(...) LAPACK_csptri_base(__VA_ARGS__)
#endif

#define LAPACK_dsptri_base LAPACK_GLOBAL(dsptri,DSPTRI)
void LAPACK_dsptri_base(
    char const* uplo,
    lapack_int const* n,
    double* AP, lapack_int const* ipiv,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsptri(...) LAPACK_dsptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsptri(...) LAPACK_dsptri_base(__VA_ARGS__)
#endif

#define LAPACK_ssptri_base LAPACK_GLOBAL(ssptri,SSPTRI)
void LAPACK_ssptri_base(
    char const* uplo,
    lapack_int const* n,
    float* AP, lapack_int const* ipiv,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssptri(...) LAPACK_ssptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssptri(...) LAPACK_ssptri_base(__VA_ARGS__)
#endif

#define LAPACK_zsptri_base LAPACK_GLOBAL(zsptri,ZSPTRI)
void LAPACK_zsptri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* AP, lapack_int const* ipiv,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsptri(...) LAPACK_zsptri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsptri(...) LAPACK_zsptri_base(__VA_ARGS__)
#endif

#define LAPACK_csptrs_base LAPACK_GLOBAL(csptrs,CSPTRS)
void LAPACK_csptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csptrs(...) LAPACK_csptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csptrs(...) LAPACK_csptrs_base(__VA_ARGS__)
#endif

#define LAPACK_dsptrs_base LAPACK_GLOBAL(dsptrs,DSPTRS)
void LAPACK_dsptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* AP, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsptrs(...) LAPACK_dsptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsptrs(...) LAPACK_dsptrs_base(__VA_ARGS__)
#endif

#define LAPACK_ssptrs_base LAPACK_GLOBAL(ssptrs,SSPTRS)
void LAPACK_ssptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* AP, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssptrs(...) LAPACK_ssptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssptrs(...) LAPACK_ssptrs_base(__VA_ARGS__)
#endif

#define LAPACK_zsptrs_base LAPACK_GLOBAL(zsptrs,ZSPTRS)
void LAPACK_zsptrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsptrs(...) LAPACK_zsptrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsptrs(...) LAPACK_zsptrs_base(__VA_ARGS__)
#endif

#define LAPACK_dstebz_base LAPACK_GLOBAL(dstebz,DSTEBZ)
void LAPACK_dstebz_base(
    char const* range, char const* order,
    lapack_int const* n,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol,
    double const* D,
    double const* E, lapack_int* m, lapack_int* nsplit,
    double* W, lapack_int* IBLOCK, lapack_int* ISPLIT,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstebz(...) LAPACK_dstebz_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dstebz(...) LAPACK_dstebz_base(__VA_ARGS__)
#endif

#define LAPACK_sstebz_base LAPACK_GLOBAL(sstebz,SSTEBZ)
void LAPACK_sstebz_base(
    char const* range, char const* order,
    lapack_int const* n,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol,
    float const* D,
    float const* E, lapack_int* m, lapack_int* nsplit,
    float* W, lapack_int* IBLOCK, lapack_int* ISPLIT,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstebz(...) LAPACK_sstebz_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sstebz(...) LAPACK_sstebz_base(__VA_ARGS__)
#endif

#define LAPACK_cstedc_base LAPACK_GLOBAL(cstedc,CSTEDC)
void LAPACK_cstedc_base(
    char const* compz,
    lapack_int const* n,
    float* D,
    float* E,
    lapack_complex_float* Z, lapack_int const* ldz,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cstedc(...) LAPACK_cstedc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cstedc(...) LAPACK_cstedc_base(__VA_ARGS__)
#endif

#define LAPACK_dstedc_base LAPACK_GLOBAL(dstedc,DSTEDC)
void LAPACK_dstedc_base(
    char const* compz,
    lapack_int const* n,
    double* D,
    double* E,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstedc(...) LAPACK_dstedc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dstedc(...) LAPACK_dstedc_base(__VA_ARGS__)
#endif

#define LAPACK_sstedc_base LAPACK_GLOBAL(sstedc,SSTEDC)
void LAPACK_sstedc_base(
    char const* compz,
    lapack_int const* n,
    float* D,
    float* E,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstedc(...) LAPACK_sstedc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sstedc(...) LAPACK_sstedc_base(__VA_ARGS__)
#endif

#define LAPACK_zstedc_base LAPACK_GLOBAL(zstedc,ZSTEDC)
void LAPACK_zstedc_base(
    char const* compz,
    lapack_int const* n,
    double* D,
    double* E,
    lapack_complex_double* Z, lapack_int const* ldz,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zstedc(...) LAPACK_zstedc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zstedc(...) LAPACK_zstedc_base(__VA_ARGS__)
#endif

#define LAPACK_cstegr_base LAPACK_GLOBAL(cstegr,CSTEGR)
void LAPACK_cstegr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    float* D,
    float* E,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cstegr(...) LAPACK_cstegr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cstegr(...) LAPACK_cstegr_base(__VA_ARGS__)
#endif

#define LAPACK_dstegr_base LAPACK_GLOBAL(dstegr,DSTEGR)
void LAPACK_dstegr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    double* D,
    double* E,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstegr(...) LAPACK_dstegr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dstegr(...) LAPACK_dstegr_base(__VA_ARGS__)
#endif

#define LAPACK_sstegr_base LAPACK_GLOBAL(sstegr,SSTEGR)
void LAPACK_sstegr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    float* D,
    float* E,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstegr(...) LAPACK_sstegr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sstegr(...) LAPACK_sstegr_base(__VA_ARGS__)
#endif

#define LAPACK_zstegr_base LAPACK_GLOBAL(zstegr,ZSTEGR)
void LAPACK_zstegr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    double* D,
    double* E,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zstegr(...) LAPACK_zstegr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zstegr(...) LAPACK_zstegr_base(__VA_ARGS__)
#endif

#define LAPACK_cstein LAPACK_GLOBAL(cstein,CSTEIN)
void LAPACK_cstein(
    lapack_int const* n,
    float const* D,
    float const* E, lapack_int const* m,
    float const* W, lapack_int const* IBLOCK, lapack_int const* ISPLIT,
    lapack_complex_float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info );

#define LAPACK_dstein LAPACK_GLOBAL(dstein,DSTEIN)
void LAPACK_dstein(
    lapack_int const* n,
    double const* D,
    double const* E, lapack_int const* m,
    double const* W, lapack_int const* IBLOCK, lapack_int const* ISPLIT,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info );

#define LAPACK_sstein LAPACK_GLOBAL(sstein,SSTEIN)
void LAPACK_sstein(
    lapack_int const* n,
    float const* D,
    float const* E, lapack_int const* m,
    float const* W, lapack_int const* IBLOCK, lapack_int const* ISPLIT,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info );

#define LAPACK_zstein LAPACK_GLOBAL(zstein,ZSTEIN)
void LAPACK_zstein(
    lapack_int const* n,
    double const* D,
    double const* E, lapack_int const* m,
    double const* W, lapack_int const* IBLOCK, lapack_int const* ISPLIT,
    lapack_complex_double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info );

#define LAPACK_cstemr_base LAPACK_GLOBAL(cstemr,CSTEMR)
void LAPACK_cstemr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    float* D,
    float* E,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* m,
    float* W,
    lapack_complex_float* Z, lapack_int const* ldz, lapack_int const* nzc, lapack_int* ISUPPZ, lapack_logical* tryrac,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cstemr(...) LAPACK_cstemr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cstemr(...) LAPACK_cstemr_base(__VA_ARGS__)
#endif

#define LAPACK_dstemr_base LAPACK_GLOBAL(dstemr,DSTEMR)
void LAPACK_dstemr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    double* D,
    double* E,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz, lapack_int const* nzc, lapack_int* ISUPPZ, lapack_logical* tryrac,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstemr(...) LAPACK_dstemr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dstemr(...) LAPACK_dstemr_base(__VA_ARGS__)
#endif

#define LAPACK_sstemr_base LAPACK_GLOBAL(sstemr,SSTEMR)
void LAPACK_sstemr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    float* D,
    float* E,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz, lapack_int const* nzc, lapack_int* ISUPPZ, lapack_logical* tryrac,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstemr(...) LAPACK_sstemr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sstemr(...) LAPACK_sstemr_base(__VA_ARGS__)
#endif

#define LAPACK_zstemr_base LAPACK_GLOBAL(zstemr,ZSTEMR)
void LAPACK_zstemr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    double* D,
    double* E,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu, lapack_int* m,
    double* W,
    lapack_complex_double* Z, lapack_int const* ldz, lapack_int const* nzc, lapack_int* ISUPPZ, lapack_logical* tryrac,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zstemr(...) LAPACK_zstemr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zstemr(...) LAPACK_zstemr_base(__VA_ARGS__)
#endif

#define LAPACK_csteqr_base LAPACK_GLOBAL(csteqr,CSTEQR)
void LAPACK_csteqr_base(
    char const* compz,
    lapack_int const* n,
    float* D,
    float* E,
    lapack_complex_float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csteqr(...) LAPACK_csteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csteqr(...) LAPACK_csteqr_base(__VA_ARGS__)
#endif

#define LAPACK_dsteqr_base LAPACK_GLOBAL(dsteqr,DSTEQR)
void LAPACK_dsteqr_base(
    char const* compz,
    lapack_int const* n,
    double* D,
    double* E,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsteqr(...) LAPACK_dsteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsteqr(...) LAPACK_dsteqr_base(__VA_ARGS__)
#endif

#define LAPACK_ssteqr_base LAPACK_GLOBAL(ssteqr,SSTEQR)
void LAPACK_ssteqr_base(
    char const* compz,
    lapack_int const* n,
    float* D,
    float* E,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssteqr(...) LAPACK_ssteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssteqr(...) LAPACK_ssteqr_base(__VA_ARGS__)
#endif

#define LAPACK_zsteqr_base LAPACK_GLOBAL(zsteqr,ZSTEQR)
void LAPACK_zsteqr_base(
    char const* compz,
    lapack_int const* n,
    double* D,
    double* E,
    lapack_complex_double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsteqr(...) LAPACK_zsteqr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsteqr(...) LAPACK_zsteqr_base(__VA_ARGS__)
#endif

#define LAPACK_dsterf LAPACK_GLOBAL(dsterf,DSTERF)
void LAPACK_dsterf(
    lapack_int const* n,
    double* D,
    double* E,
    lapack_int* info );

#define LAPACK_ssterf LAPACK_GLOBAL(ssterf,SSTERF)
void LAPACK_ssterf(
    lapack_int const* n,
    float* D,
    float* E,
    lapack_int* info );

#define LAPACK_dstev_base LAPACK_GLOBAL(dstev,DSTEV)
void LAPACK_dstev_base(
    char const* jobz,
    lapack_int const* n,
    double* D,
    double* E,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstev(...) LAPACK_dstev_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dstev(...) LAPACK_dstev_base(__VA_ARGS__)
#endif

#define LAPACK_sstev_base LAPACK_GLOBAL(sstev,SSTEV)
void LAPACK_sstev_base(
    char const* jobz,
    lapack_int const* n,
    float* D,
    float* E,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstev(...) LAPACK_sstev_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sstev(...) LAPACK_sstev_base(__VA_ARGS__)
#endif

#define LAPACK_dstevd_base LAPACK_GLOBAL(dstevd,DSTEVD)
void LAPACK_dstevd_base(
    char const* jobz,
    lapack_int const* n,
    double* D,
    double* E,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstevd(...) LAPACK_dstevd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dstevd(...) LAPACK_dstevd_base(__VA_ARGS__)
#endif

#define LAPACK_sstevd_base LAPACK_GLOBAL(sstevd,SSTEVD)
void LAPACK_sstevd_base(
    char const* jobz,
    lapack_int const* n,
    float* D,
    float* E,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstevd(...) LAPACK_sstevd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_sstevd(...) LAPACK_sstevd_base(__VA_ARGS__)
#endif

#define LAPACK_dstevr_base LAPACK_GLOBAL(dstevr,DSTEVR)
void LAPACK_dstevr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    double* D,
    double* E,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstevr(...) LAPACK_dstevr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dstevr(...) LAPACK_dstevr_base(__VA_ARGS__)
#endif

#define LAPACK_sstevr_base LAPACK_GLOBAL(sstevr,SSTEVR)
void LAPACK_sstevr_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    float* D,
    float* E,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstevr(...) LAPACK_sstevr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sstevr(...) LAPACK_sstevr_base(__VA_ARGS__)
#endif

#define LAPACK_dstevx_base LAPACK_GLOBAL(dstevx,DSTEVX)
void LAPACK_dstevx_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    double* D,
    double* E,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dstevx(...) LAPACK_dstevx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dstevx(...) LAPACK_dstevx_base(__VA_ARGS__)
#endif

#define LAPACK_sstevx_base LAPACK_GLOBAL(sstevx,SSTEVX)
void LAPACK_sstevx_base(
    char const* jobz, char const* range,
    lapack_int const* n,
    float* D,
    float* E,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_sstevx(...) LAPACK_sstevx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_sstevx(...) LAPACK_sstevx_base(__VA_ARGS__)
#endif

#define LAPACK_csycon_base LAPACK_GLOBAL(csycon,CSYCON)
void LAPACK_csycon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csycon(...) LAPACK_csycon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csycon(...) LAPACK_csycon_base(__VA_ARGS__)
#endif

#define LAPACK_dsycon_base LAPACK_GLOBAL(dsycon,DSYCON)
void LAPACK_dsycon_base(
    char const* uplo,
    lapack_int const* n,
    double const* A, lapack_int const* lda, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsycon(...) LAPACK_dsycon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsycon(...) LAPACK_dsycon_base(__VA_ARGS__)
#endif

#define LAPACK_ssycon_base LAPACK_GLOBAL(ssycon,SSYCON)
void LAPACK_ssycon_base(
    char const* uplo,
    lapack_int const* n,
    float const* A, lapack_int const* lda, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssycon(...) LAPACK_ssycon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssycon(...) LAPACK_ssycon_base(__VA_ARGS__)
#endif

#define LAPACK_zsycon_base LAPACK_GLOBAL(zsycon,ZSYCON)
void LAPACK_zsycon_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsycon(...) LAPACK_zsycon_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsycon(...) LAPACK_zsycon_base(__VA_ARGS__)
#endif

#define LAPACK_csycon_3_base LAPACK_GLOBAL(csycon_3,CSYCON_3)
void LAPACK_csycon_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* E, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csycon_3(...) LAPACK_csycon_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csycon_3(...) LAPACK_csycon_3_base(__VA_ARGS__)
#endif

#define LAPACK_dsycon_3_base LAPACK_GLOBAL(dsycon_3,DSYCON_3)
void LAPACK_dsycon_3_base(
    char const* uplo,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* E, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsycon_3(...) LAPACK_dsycon_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsycon_3(...) LAPACK_dsycon_3_base(__VA_ARGS__)
#endif

#define LAPACK_ssycon_3_base LAPACK_GLOBAL(ssycon_3,SSYCON_3)
void LAPACK_ssycon_3_base(
    char const* uplo,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* E, lapack_int const* ipiv,
    float const* anorm,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssycon_3(...) LAPACK_ssycon_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssycon_3(...) LAPACK_ssycon_3_base(__VA_ARGS__)
#endif

#define LAPACK_zsycon_3_base LAPACK_GLOBAL(zsycon_3,ZSYCON_3)
void LAPACK_zsycon_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* E, lapack_int const* ipiv,
    double const* anorm,
    double* rcond,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsycon_3(...) LAPACK_zsycon_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsycon_3(...) LAPACK_zsycon_3_base(__VA_ARGS__)
#endif

#define LAPACK_csyconv_base LAPACK_GLOBAL(csyconv,CSYCONV)
void LAPACK_csyconv_base(
    char const* uplo, char const* way,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* E,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csyconv(...) LAPACK_csyconv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_csyconv(...) LAPACK_csyconv_base(__VA_ARGS__)
#endif

#define LAPACK_dsyconv_base LAPACK_GLOBAL(dsyconv,DSYCONV)
void LAPACK_dsyconv_base(
    char const* uplo, char const* way,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int const* ipiv,
    double* E,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyconv(...) LAPACK_dsyconv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsyconv(...) LAPACK_dsyconv_base(__VA_ARGS__)
#endif

#define LAPACK_ssyconv_base LAPACK_GLOBAL(ssyconv,SSYCONV)
void LAPACK_ssyconv_base(
    char const* uplo, char const* way,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int const* ipiv,
    float* E,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyconv(...) LAPACK_ssyconv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssyconv(...) LAPACK_ssyconv_base(__VA_ARGS__)
#endif

#define LAPACK_zsyconv_base LAPACK_GLOBAL(zsyconv,ZSYCONV)
void LAPACK_zsyconv_base(
    char const* uplo, char const* way,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* E,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsyconv(...) LAPACK_zsyconv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zsyconv(...) LAPACK_zsyconv_base(__VA_ARGS__)
#endif

#define LAPACK_csyequb_base LAPACK_GLOBAL(csyequb,CSYEQUB)
void LAPACK_csyequb_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* S,
    float* scond,
    float* amax,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csyequb(...) LAPACK_csyequb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csyequb(...) LAPACK_csyequb_base(__VA_ARGS__)
#endif

#define LAPACK_dsyequb_base LAPACK_GLOBAL(dsyequb,DSYEQUB)
void LAPACK_dsyequb_base(
    char const* uplo,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* S,
    double* scond,
    double* amax,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyequb(...) LAPACK_dsyequb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsyequb(...) LAPACK_dsyequb_base(__VA_ARGS__)
#endif

#define LAPACK_ssyequb_base LAPACK_GLOBAL(ssyequb,SSYEQUB)
void LAPACK_ssyequb_base(
    char const* uplo,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* S,
    float* scond,
    float* amax,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyequb(...) LAPACK_ssyequb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssyequb(...) LAPACK_ssyequb_base(__VA_ARGS__)
#endif

#define LAPACK_zsyequb_base LAPACK_GLOBAL(zsyequb,ZSYEQUB)
void LAPACK_zsyequb_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* S,
    double* scond,
    double* amax,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsyequb(...) LAPACK_zsyequb_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsyequb(...) LAPACK_zsyequb_base(__VA_ARGS__)
#endif

#define LAPACK_dsyev_base LAPACK_GLOBAL(dsyev,DSYEV)
void LAPACK_dsyev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* W,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyev(...) LAPACK_dsyev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsyev(...) LAPACK_dsyev_base(__VA_ARGS__)
#endif

#define LAPACK_ssyev_base LAPACK_GLOBAL(ssyev,SSYEV)
void LAPACK_ssyev_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* W,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyev(...) LAPACK_ssyev_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssyev(...) LAPACK_ssyev_base(__VA_ARGS__)
#endif

#define LAPACK_dsyev_2stage_base LAPACK_GLOBAL(dsyev_2stage,DSYEV_2STAGE)
void LAPACK_dsyev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* W,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyev_2stage(...) LAPACK_dsyev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsyev_2stage(...) LAPACK_dsyev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssyev_2stage_base LAPACK_GLOBAL(ssyev_2stage,SSYEV_2STAGE)
void LAPACK_ssyev_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* W,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyev_2stage(...) LAPACK_ssyev_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssyev_2stage(...) LAPACK_ssyev_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsyevd_base LAPACK_GLOBAL(dsyevd,DSYEVD)
void LAPACK_dsyevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* W,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyevd(...) LAPACK_dsyevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsyevd(...) LAPACK_dsyevd_base(__VA_ARGS__)
#endif

#define LAPACK_ssyevd_base LAPACK_GLOBAL(ssyevd,SSYEVD)
void LAPACK_ssyevd_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* W,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyevd(...) LAPACK_ssyevd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssyevd(...) LAPACK_ssyevd_base(__VA_ARGS__)
#endif

#define LAPACK_dsyevd_2stage_base LAPACK_GLOBAL(dsyevd_2stage,DSYEVD_2STAGE)
void LAPACK_dsyevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* W,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyevd_2stage(...) LAPACK_dsyevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsyevd_2stage(...) LAPACK_dsyevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssyevd_2stage_base LAPACK_GLOBAL(ssyevd_2stage,SSYEVD_2STAGE)
void LAPACK_ssyevd_2stage_base(
    char const* jobz, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* W,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyevd_2stage(...) LAPACK_ssyevd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssyevd_2stage(...) LAPACK_ssyevd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsyevr_base LAPACK_GLOBAL(dsyevr,DSYEVR)
void LAPACK_dsyevr_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyevr(...) LAPACK_dsyevr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsyevr(...) LAPACK_dsyevr_base(__VA_ARGS__)
#endif

#define LAPACK_ssyevr_base LAPACK_GLOBAL(ssyevr,SSYEVR)
void LAPACK_ssyevr_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyevr(...) LAPACK_ssyevr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssyevr(...) LAPACK_ssyevr_base(__VA_ARGS__)
#endif

#define LAPACK_dsyevr_2stage_base LAPACK_GLOBAL(dsyevr_2stage,DSYEVR_2STAGE)
void LAPACK_dsyevr_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyevr_2stage(...) LAPACK_dsyevr_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsyevr_2stage(...) LAPACK_dsyevr_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssyevr_2stage_base LAPACK_GLOBAL(ssyevr_2stage,SSYEVR_2STAGE)
void LAPACK_ssyevr_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz, lapack_int* ISUPPZ,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyevr_2stage(...) LAPACK_ssyevr_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssyevr_2stage(...) LAPACK_ssyevr_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsyevx_base LAPACK_GLOBAL(dsyevx,DSYEVX)
void LAPACK_dsyevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyevx(...) LAPACK_dsyevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsyevx(...) LAPACK_dsyevx_base(__VA_ARGS__)
#endif

#define LAPACK_ssyevx_base LAPACK_GLOBAL(ssyevx,SSYEVX)
void LAPACK_ssyevx_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyevx(...) LAPACK_ssyevx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssyevx(...) LAPACK_ssyevx_base(__VA_ARGS__)
#endif

#define LAPACK_dsyevx_2stage_base LAPACK_GLOBAL(dsyevx_2stage,DSYEVX_2STAGE)
void LAPACK_dsyevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyevx_2stage(...) LAPACK_dsyevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsyevx_2stage(...) LAPACK_dsyevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssyevx_2stage_base LAPACK_GLOBAL(ssyevx_2stage,SSYEVX_2STAGE)
void LAPACK_ssyevx_2stage_base(
    char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyevx_2stage(...) LAPACK_ssyevx_2stage_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssyevx_2stage(...) LAPACK_ssyevx_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsygst_base LAPACK_GLOBAL(dsygst,DSYGST)
void LAPACK_dsygst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsygst(...) LAPACK_dsygst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsygst(...) LAPACK_dsygst_base(__VA_ARGS__)
#endif

#define LAPACK_ssygst_base LAPACK_GLOBAL(ssygst,SSYGST)
void LAPACK_ssygst_base(
    lapack_int const* itype, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssygst(...) LAPACK_ssygst_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssygst(...) LAPACK_ssygst_base(__VA_ARGS__)
#endif

#define LAPACK_dsygv_base LAPACK_GLOBAL(dsygv,DSYGV)
void LAPACK_dsygv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* W,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsygv(...) LAPACK_dsygv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsygv(...) LAPACK_dsygv_base(__VA_ARGS__)
#endif

#define LAPACK_ssygv_base LAPACK_GLOBAL(ssygv,SSYGV)
void LAPACK_ssygv_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* W,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssygv(...) LAPACK_ssygv_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssygv(...) LAPACK_ssygv_base(__VA_ARGS__)
#endif

#define LAPACK_dsygv_2stage_base LAPACK_GLOBAL(dsygv_2stage,DSYGV_2STAGE)
void LAPACK_dsygv_2stage_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* W,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsygv_2stage(...) LAPACK_dsygv_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsygv_2stage(...) LAPACK_dsygv_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssygv_2stage_base LAPACK_GLOBAL(ssygv_2stage,SSYGV_2STAGE)
void LAPACK_ssygv_2stage_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* W,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssygv_2stage(...) LAPACK_ssygv_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssygv_2stage(...) LAPACK_ssygv_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsygvd_base LAPACK_GLOBAL(dsygvd,DSYGVD)
void LAPACK_dsygvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* W,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsygvd(...) LAPACK_dsygvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsygvd(...) LAPACK_dsygvd_base(__VA_ARGS__)
#endif

#define LAPACK_ssygvd_base LAPACK_GLOBAL(ssygvd,SSYGVD)
void LAPACK_ssygvd_base(
    lapack_int const* itype, char const* jobz, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* W,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssygvd(...) LAPACK_ssygvd_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssygvd(...) LAPACK_ssygvd_base(__VA_ARGS__)
#endif

#define LAPACK_dsygvx_base LAPACK_GLOBAL(dsygvx,DSYGVX)
void LAPACK_dsygvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double const* vl,
    double const* vu, lapack_int const* il, lapack_int const* iu,
    double const* abstol, lapack_int* m,
    double* W,
    double* Z, lapack_int const* ldz,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsygvx(...) LAPACK_dsygvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsygvx(...) LAPACK_dsygvx_base(__VA_ARGS__)
#endif

#define LAPACK_ssygvx_base LAPACK_GLOBAL(ssygvx,SSYGVX)
void LAPACK_ssygvx_base(
    lapack_int const* itype, char const* jobz, char const* range, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float const* vl,
    float const* vu, lapack_int const* il, lapack_int const* iu,
    float const* abstol, lapack_int* m,
    float* W,
    float* Z, lapack_int const* ldz,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int* IFAIL,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssygvx(...) LAPACK_ssygvx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssygvx(...) LAPACK_ssygvx_base(__VA_ARGS__)
#endif

#define LAPACK_csyr_base LAPACK_GLOBAL(csyr,CSYR)
void LAPACK_csyr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* alpha,
    lapack_complex_float const* X, lapack_int const* incx,
    lapack_complex_float* A, lapack_int const* lda
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csyr(...) LAPACK_csyr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csyr(...) LAPACK_csyr_base(__VA_ARGS__)
#endif

#define LAPACK_zsyr_base LAPACK_GLOBAL(zsyr,ZSYR)
void LAPACK_zsyr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* alpha,
    lapack_complex_double const* X, lapack_int const* incx,
    lapack_complex_double* A, lapack_int const* lda
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsyr(...) LAPACK_zsyr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsyr(...) LAPACK_zsyr_base(__VA_ARGS__)
#endif

#define LAPACK_csyrfs_base LAPACK_GLOBAL(csyrfs,CSYRFS)
void LAPACK_csyrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csyrfs(...) LAPACK_csyrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csyrfs(...) LAPACK_csyrfs_base(__VA_ARGS__)
#endif

#define LAPACK_dsyrfs_base LAPACK_GLOBAL(dsyrfs,DSYRFS)
void LAPACK_dsyrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyrfs(...) LAPACK_dsyrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsyrfs(...) LAPACK_dsyrfs_base(__VA_ARGS__)
#endif

#define LAPACK_ssyrfs_base LAPACK_GLOBAL(ssyrfs,SSYRFS)
void LAPACK_ssyrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyrfs(...) LAPACK_ssyrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssyrfs(...) LAPACK_ssyrfs_base(__VA_ARGS__)
#endif

#define LAPACK_zsyrfs_base LAPACK_GLOBAL(zsyrfs,ZSYRFS)
void LAPACK_zsyrfs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsyrfs(...) LAPACK_zsyrfs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsyrfs(...) LAPACK_zsyrfs_base(__VA_ARGS__)
#endif

#define LAPACK_csyrfsx_base LAPACK_GLOBAL(csyrfsx,CSYRFSX)
void LAPACK_csyrfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    const float* S,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csyrfsx(...) LAPACK_csyrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_csyrfsx(...) LAPACK_csyrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_dsyrfsx_base LAPACK_GLOBAL(dsyrfsx,DSYRFSX)
void LAPACK_dsyrfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    const double* S,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyrfsx(...) LAPACK_dsyrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsyrfsx(...) LAPACK_dsyrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_ssyrfsx_base LAPACK_GLOBAL(ssyrfsx,SSYRFSX)
void LAPACK_ssyrfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    const float* S,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyrfsx(...) LAPACK_ssyrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssyrfsx(...) LAPACK_ssyrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_zsyrfsx_base LAPACK_GLOBAL(zsyrfsx,ZSYRFSX)
void LAPACK_zsyrfsx_base(
    char const* uplo, char const* equed,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* AF, lapack_int const* ldaf, lapack_int const* ipiv,
    const double* S,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsyrfsx(...) LAPACK_zsyrfsx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zsyrfsx(...) LAPACK_zsyrfsx_base(__VA_ARGS__)
#endif

#define LAPACK_csysv_base LAPACK_GLOBAL(csysv,CSYSV)
void LAPACK_csysv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csysv(...) LAPACK_csysv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csysv(...) LAPACK_csysv_base(__VA_ARGS__)
#endif

#define LAPACK_dsysv_base LAPACK_GLOBAL(dsysv,DSYSV)
void LAPACK_dsysv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsysv(...) LAPACK_dsysv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsysv(...) LAPACK_dsysv_base(__VA_ARGS__)
#endif

#define LAPACK_ssysv_base LAPACK_GLOBAL(ssysv,SSYSV)
void LAPACK_ssysv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssysv(...) LAPACK_ssysv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssysv(...) LAPACK_ssysv_base(__VA_ARGS__)
#endif

#define LAPACK_zsysv_base LAPACK_GLOBAL(zsysv,ZSYSV)
void LAPACK_zsysv_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsysv(...) LAPACK_zsysv_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsysv(...) LAPACK_zsysv_base(__VA_ARGS__)
#endif

#define LAPACK_csysv_aa_base LAPACK_GLOBAL(csysv_aa,CSYSV_AA)
void LAPACK_csysv_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csysv_aa(...) LAPACK_csysv_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csysv_aa(...) LAPACK_csysv_aa_base(__VA_ARGS__)
#endif

#define LAPACK_dsysv_aa_base LAPACK_GLOBAL(dsysv_aa,DSYSV_AA)
void LAPACK_dsysv_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsysv_aa(...) LAPACK_dsysv_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsysv_aa(...) LAPACK_dsysv_aa_base(__VA_ARGS__)
#endif

#define LAPACK_ssysv_aa_base LAPACK_GLOBAL(ssysv_aa,SSYSV_AA)
void LAPACK_ssysv_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssysv_aa(...) LAPACK_ssysv_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssysv_aa(...) LAPACK_ssysv_aa_base(__VA_ARGS__)
#endif

#define LAPACK_zsysv_aa_base LAPACK_GLOBAL(zsysv_aa,ZSYSV_AA)
void LAPACK_zsysv_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsysv_aa(...) LAPACK_zsysv_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsysv_aa(...) LAPACK_zsysv_aa_base(__VA_ARGS__)
#endif

#define LAPACK_csysv_aa_2stage_base LAPACK_GLOBAL(csysv_aa_2stage,CSYSV_AA_2STAGE)
void LAPACK_csysv_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csysv_aa_2stage(...) LAPACK_csysv_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csysv_aa_2stage(...) LAPACK_csysv_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsysv_aa_2stage_base LAPACK_GLOBAL(dsysv_aa_2stage,DSYSV_AA_2STAGE)
void LAPACK_dsysv_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsysv_aa_2stage(...) LAPACK_dsysv_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsysv_aa_2stage(...) LAPACK_dsysv_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssysv_aa_2stage_base LAPACK_GLOBAL(ssysv_aa_2stage,SSYSV_AA_2STAGE)
void LAPACK_ssysv_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssysv_aa_2stage(...) LAPACK_ssysv_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssysv_aa_2stage(...) LAPACK_ssysv_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zsysv_aa_2stage_base LAPACK_GLOBAL(zsysv_aa_2stage,ZSYSV_AA_2STAGE)
void LAPACK_zsysv_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsysv_aa_2stage(...) LAPACK_zsysv_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsysv_aa_2stage(...) LAPACK_zsysv_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_csysv_rk_base LAPACK_GLOBAL(csysv_rk,CSYSV_RK)
void LAPACK_csysv_rk_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* E, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csysv_rk(...) LAPACK_csysv_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csysv_rk(...) LAPACK_csysv_rk_base(__VA_ARGS__)
#endif

#define LAPACK_dsysv_rk_base LAPACK_GLOBAL(dsysv_rk,DSYSV_RK)
void LAPACK_dsysv_rk_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* E, lapack_int* ipiv,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsysv_rk(...) LAPACK_dsysv_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsysv_rk(...) LAPACK_dsysv_rk_base(__VA_ARGS__)
#endif

#define LAPACK_ssysv_rk_base LAPACK_GLOBAL(ssysv_rk,SSYSV_RK)
void LAPACK_ssysv_rk_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* E, lapack_int* ipiv,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssysv_rk(...) LAPACK_ssysv_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssysv_rk(...) LAPACK_ssysv_rk_base(__VA_ARGS__)
#endif

#define LAPACK_zsysv_rk_base LAPACK_GLOBAL(zsysv_rk,ZSYSV_RK)
void LAPACK_zsysv_rk_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* E, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsysv_rk(...) LAPACK_zsysv_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsysv_rk(...) LAPACK_zsysv_rk_base(__VA_ARGS__)
#endif

#define LAPACK_csysv_rook_base LAPACK_GLOBAL(csysv_rook,CSYSV_ROOK)
void LAPACK_csysv_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csysv_rook(...) LAPACK_csysv_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csysv_rook(...) LAPACK_csysv_rook_base(__VA_ARGS__)
#endif

#define LAPACK_dsysv_rook_base LAPACK_GLOBAL(dsysv_rook,DSYSV_ROOK)
void LAPACK_dsysv_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsysv_rook(...) LAPACK_dsysv_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsysv_rook(...) LAPACK_dsysv_rook_base(__VA_ARGS__)
#endif

#define LAPACK_ssysv_rook_base LAPACK_GLOBAL(ssysv_rook,SSYSV_ROOK)
void LAPACK_ssysv_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssysv_rook(...) LAPACK_ssysv_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssysv_rook(...) LAPACK_ssysv_rook_base(__VA_ARGS__)
#endif

#define LAPACK_zsysv_rook_base LAPACK_GLOBAL(zsysv_rook,ZSYSV_ROOK)
void LAPACK_zsysv_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsysv_rook(...) LAPACK_zsysv_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsysv_rook(...) LAPACK_zsysv_rook_base(__VA_ARGS__)
#endif

#define LAPACK_csysvx_base LAPACK_GLOBAL(csysvx,CSYSVX)
void LAPACK_csysvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, lapack_int* ipiv,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csysvx(...) LAPACK_csysvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_csysvx(...) LAPACK_csysvx_base(__VA_ARGS__)
#endif

#define LAPACK_dsysvx_base LAPACK_GLOBAL(dsysvx,DSYSVX)
void LAPACK_dsysvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double* AF, lapack_int const* ldaf, lapack_int* ipiv,
    double const* B, lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsysvx(...) LAPACK_dsysvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsysvx(...) LAPACK_dsysvx_base(__VA_ARGS__)
#endif

#define LAPACK_ssysvx_base LAPACK_GLOBAL(ssysvx,SSYSVX)
void LAPACK_ssysvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float* AF, lapack_int const* ldaf, lapack_int* ipiv,
    float const* B, lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* ferr,
    float* berr,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssysvx(...) LAPACK_ssysvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssysvx(...) LAPACK_ssysvx_base(__VA_ARGS__)
#endif

#define LAPACK_zsysvx_base LAPACK_GLOBAL(zsysvx,ZSYSVX)
void LAPACK_zsysvx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, lapack_int* ipiv,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* ferr,
    double* berr,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsysvx(...) LAPACK_zsysvx_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zsysvx(...) LAPACK_zsysvx_base(__VA_ARGS__)
#endif

#define LAPACK_csysvxx_base LAPACK_GLOBAL(csysvxx,CSYSVXX)
void LAPACK_csysvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    float* S,
    lapack_complex_float* B,
    lapack_int const* ldb,
    lapack_complex_float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csysvxx(...) LAPACK_csysvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_csysvxx(...) LAPACK_csysvxx_base(__VA_ARGS__)
#endif

#define LAPACK_dsysvxx_base LAPACK_GLOBAL(dsysvxx,DSYSVXX)
void LAPACK_dsysvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double* A, lapack_int const* lda,
    double* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    double* S,
    double* B,
    lapack_int const* ldb,
    double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsysvxx(...) LAPACK_dsysvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dsysvxx(...) LAPACK_dsysvxx_base(__VA_ARGS__)
#endif

#define LAPACK_ssysvxx_base LAPACK_GLOBAL(ssysvxx,SSYSVXX)
void LAPACK_ssysvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float* A, lapack_int const* lda,
    float* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    float* S,
    float* B,
    lapack_int const* ldb,
    float* X, lapack_int const* ldx,
    float* rcond,
    float* rpvgrw,
    float* berr, lapack_int const* n_err_bnds,
    float* err_bnds_norm,
    float* err_bnds_comp, lapack_int const* nparams,
    float* params,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssysvxx(...) LAPACK_ssysvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ssysvxx(...) LAPACK_ssysvxx_base(__VA_ARGS__)
#endif

#define LAPACK_zsysvxx_base LAPACK_GLOBAL(zsysvxx,ZSYSVXX)
void LAPACK_zsysvxx_base(
    char const* fact, char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* AF, lapack_int const* ldaf, lapack_int* ipiv, char* equed,
    double* S,
    lapack_complex_double* B,
    lapack_int const* ldb,
    lapack_complex_double* X, lapack_int const* ldx,
    double* rcond,
    double* rpvgrw,
    double* berr, lapack_int const* n_err_bnds,
    double* err_bnds_norm,
    double* err_bnds_comp, lapack_int const* nparams,
    double* params,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsysvxx(...) LAPACK_zsysvxx_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zsysvxx(...) LAPACK_zsysvxx_base(__VA_ARGS__)
#endif

#define LAPACK_csyswapr_base LAPACK_GLOBAL(csyswapr,CSYSWAPR)
void LAPACK_csyswapr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* i1, lapack_int const* i2
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csyswapr(...) LAPACK_csyswapr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csyswapr(...) LAPACK_csyswapr_base(__VA_ARGS__)
#endif

#define LAPACK_dsyswapr_base LAPACK_GLOBAL(dsyswapr,DSYSWAPR)
void LAPACK_dsyswapr_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int const* i1, lapack_int const* i2
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsyswapr(...) LAPACK_dsyswapr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsyswapr(...) LAPACK_dsyswapr_base(__VA_ARGS__)
#endif

#define LAPACK_ssyswapr_base LAPACK_GLOBAL(ssyswapr,SSYSWAPR)
void LAPACK_ssyswapr_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int const* i1, lapack_int const* i2
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssyswapr(...) LAPACK_ssyswapr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssyswapr(...) LAPACK_ssyswapr_base(__VA_ARGS__)
#endif

#define LAPACK_zsyswapr_base LAPACK_GLOBAL(zsyswapr,ZSYSWAPR)
void LAPACK_zsyswapr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* i1, lapack_int const* i2
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsyswapr(...) LAPACK_zsyswapr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsyswapr(...) LAPACK_zsyswapr_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrd_base LAPACK_GLOBAL(dsytrd,DSYTRD)
void LAPACK_dsytrd_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* D,
    double* E,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrd(...) LAPACK_dsytrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrd(...) LAPACK_dsytrd_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrd_base LAPACK_GLOBAL(ssytrd,SSYTRD)
void LAPACK_ssytrd_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* D,
    float* E,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrd(...) LAPACK_ssytrd_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrd(...) LAPACK_ssytrd_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrd_2stage_base LAPACK_GLOBAL(dsytrd_2stage,DSYTRD_2STAGE)
void LAPACK_dsytrd_2stage_base(
    char const* vect, char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* D,
    double* E,
    double* tau,
    double* HOUS2, lapack_int const* lhous2,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrd_2stage(...) LAPACK_dsytrd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dsytrd_2stage(...) LAPACK_dsytrd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrd_2stage_base LAPACK_GLOBAL(ssytrd_2stage,SSYTRD_2STAGE)
void LAPACK_ssytrd_2stage_base(
    char const* vect, char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* D,
    float* E,
    float* tau,
    float* HOUS2, lapack_int const* lhous2,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrd_2stage(...) LAPACK_ssytrd_2stage_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ssytrd_2stage(...) LAPACK_ssytrd_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_csytrf_base LAPACK_GLOBAL(csytrf,CSYTRF)
void LAPACK_csytrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrf(...) LAPACK_csytrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrf(...) LAPACK_csytrf_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrf_base LAPACK_GLOBAL(dsytrf,DSYTRF)
void LAPACK_dsytrf_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrf(...) LAPACK_dsytrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrf(...) LAPACK_dsytrf_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrf_base LAPACK_GLOBAL(ssytrf,SSYTRF)
void LAPACK_ssytrf_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrf(...) LAPACK_ssytrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrf(...) LAPACK_ssytrf_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrf_base LAPACK_GLOBAL(zsytrf,ZSYTRF)
void LAPACK_zsytrf_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrf(...) LAPACK_zsytrf_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrf(...) LAPACK_zsytrf_base(__VA_ARGS__)
#endif

#define LAPACK_csytrf_aa_base LAPACK_GLOBAL(csytrf_aa,CSYTRF_AA)
void LAPACK_csytrf_aa_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrf_aa(...) LAPACK_csytrf_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrf_aa(...) LAPACK_csytrf_aa_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrf_aa_base LAPACK_GLOBAL(dsytrf_aa,DSYTRF_AA)
void LAPACK_dsytrf_aa_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrf_aa(...) LAPACK_dsytrf_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrf_aa(...) LAPACK_dsytrf_aa_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrf_aa_base LAPACK_GLOBAL(ssytrf_aa,SSYTRF_AA)
void LAPACK_ssytrf_aa_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrf_aa(...) LAPACK_ssytrf_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrf_aa(...) LAPACK_ssytrf_aa_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrf_aa_base LAPACK_GLOBAL(zsytrf_aa,ZSYTRF_AA)
void LAPACK_zsytrf_aa_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrf_aa(...) LAPACK_zsytrf_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrf_aa(...) LAPACK_zsytrf_aa_base(__VA_ARGS__)
#endif

#define LAPACK_csytrf_aa_2stage_base LAPACK_GLOBAL(csytrf_aa_2stage,CSYTRF_AA_2STAGE)
void LAPACK_csytrf_aa_2stage_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrf_aa_2stage(...) LAPACK_csytrf_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrf_aa_2stage(...) LAPACK_csytrf_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrf_aa_2stage_base LAPACK_GLOBAL(dsytrf_aa_2stage,DSYTRF_AA_2STAGE)
void LAPACK_dsytrf_aa_2stage_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrf_aa_2stage(...) LAPACK_dsytrf_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrf_aa_2stage(...) LAPACK_dsytrf_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrf_aa_2stage_base LAPACK_GLOBAL(ssytrf_aa_2stage,SSYTRF_AA_2STAGE)
void LAPACK_ssytrf_aa_2stage_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrf_aa_2stage(...) LAPACK_ssytrf_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrf_aa_2stage(...) LAPACK_ssytrf_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrf_aa_2stage_base LAPACK_GLOBAL(zsytrf_aa_2stage,ZSYTRF_AA_2STAGE)
void LAPACK_zsytrf_aa_2stage_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* TB, lapack_int const* ltb, lapack_int* ipiv, lapack_int* ipiv2,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrf_aa_2stage(...) LAPACK_zsytrf_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrf_aa_2stage(...) LAPACK_zsytrf_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_csytrf_rk_base LAPACK_GLOBAL(csytrf_rk,CSYTRF_RK)
void LAPACK_csytrf_rk_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* E, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrf_rk(...) LAPACK_csytrf_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrf_rk(...) LAPACK_csytrf_rk_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrf_rk_base LAPACK_GLOBAL(dsytrf_rk,DSYTRF_RK)
void LAPACK_dsytrf_rk_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double* E, lapack_int* ipiv,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrf_rk(...) LAPACK_dsytrf_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrf_rk(...) LAPACK_dsytrf_rk_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrf_rk_base LAPACK_GLOBAL(ssytrf_rk,SSYTRF_RK)
void LAPACK_ssytrf_rk_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float* E, lapack_int* ipiv,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrf_rk(...) LAPACK_ssytrf_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrf_rk(...) LAPACK_ssytrf_rk_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrf_rk_base LAPACK_GLOBAL(zsytrf_rk,ZSYTRF_RK)
void LAPACK_zsytrf_rk_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* E, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrf_rk(...) LAPACK_zsytrf_rk_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrf_rk(...) LAPACK_zsytrf_rk_base(__VA_ARGS__)
#endif

#define LAPACK_csytrf_rook_base LAPACK_GLOBAL(csytrf_rook,CSYTRF_ROOK)
void LAPACK_csytrf_rook_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrf_rook(...) LAPACK_csytrf_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrf_rook(...) LAPACK_csytrf_rook_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrf_rook_base LAPACK_GLOBAL(dsytrf_rook,DSYTRF_ROOK)
void LAPACK_dsytrf_rook_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int* ipiv,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrf_rook(...) LAPACK_dsytrf_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrf_rook(...) LAPACK_dsytrf_rook_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrf_rook_base LAPACK_GLOBAL(ssytrf_rook,SSYTRF_ROOK)
void LAPACK_ssytrf_rook_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int* ipiv,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrf_rook(...) LAPACK_ssytrf_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrf_rook(...) LAPACK_ssytrf_rook_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrf_rook_base LAPACK_GLOBAL(zsytrf_rook,ZSYTRF_ROOK)
void LAPACK_zsytrf_rook_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrf_rook(...) LAPACK_zsytrf_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrf_rook(...) LAPACK_zsytrf_rook_base(__VA_ARGS__)
#endif

#define LAPACK_csytri_base LAPACK_GLOBAL(csytri,CSYTRI)
void LAPACK_csytri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytri(...) LAPACK_csytri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytri(...) LAPACK_csytri_base(__VA_ARGS__)
#endif

#define LAPACK_dsytri_base LAPACK_GLOBAL(dsytri,DSYTRI)
void LAPACK_dsytri_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int const* ipiv,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytri(...) LAPACK_dsytri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytri(...) LAPACK_dsytri_base(__VA_ARGS__)
#endif

#define LAPACK_ssytri_base LAPACK_GLOBAL(ssytri,SSYTRI)
void LAPACK_ssytri_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int const* ipiv,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytri(...) LAPACK_ssytri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytri(...) LAPACK_ssytri_base(__VA_ARGS__)
#endif

#define LAPACK_zsytri_base LAPACK_GLOBAL(zsytri,ZSYTRI)
void LAPACK_zsytri_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytri(...) LAPACK_zsytri_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytri(...) LAPACK_zsytri_base(__VA_ARGS__)
#endif

#define LAPACK_csytri2_base LAPACK_GLOBAL(csytri2,CSYTRI2)
void LAPACK_csytri2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytri2(...) LAPACK_csytri2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytri2(...) LAPACK_csytri2_base(__VA_ARGS__)
#endif

#define LAPACK_dsytri2_base LAPACK_GLOBAL(dsytri2,DSYTRI2)
void LAPACK_dsytri2_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int const* ipiv,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytri2(...) LAPACK_dsytri2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytri2(...) LAPACK_dsytri2_base(__VA_ARGS__)
#endif

#define LAPACK_ssytri2_base LAPACK_GLOBAL(ssytri2,SSYTRI2)
void LAPACK_ssytri2_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int const* ipiv,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytri2(...) LAPACK_ssytri2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytri2(...) LAPACK_ssytri2_base(__VA_ARGS__)
#endif

#define LAPACK_zsytri2_base LAPACK_GLOBAL(zsytri2,ZSYTRI2)
void LAPACK_zsytri2_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytri2(...) LAPACK_zsytri2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytri2(...) LAPACK_zsytri2_base(__VA_ARGS__)
#endif

#define LAPACK_csytri2x_base LAPACK_GLOBAL(csytri2x,CSYTRI2X)
void LAPACK_csytri2x_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* work, lapack_int const* nb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytri2x(...) LAPACK_csytri2x_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytri2x(...) LAPACK_csytri2x_base(__VA_ARGS__)
#endif

#define LAPACK_dsytri2x_base LAPACK_GLOBAL(dsytri2x,DSYTRI2X)
void LAPACK_dsytri2x_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda, lapack_int const* ipiv,
    double* work, lapack_int const* nb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytri2x(...) LAPACK_dsytri2x_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytri2x(...) LAPACK_dsytri2x_base(__VA_ARGS__)
#endif

#define LAPACK_ssytri2x_base LAPACK_GLOBAL(ssytri2x,SSYTRI2X)
void LAPACK_ssytri2x_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda, lapack_int const* ipiv,
    float* work, lapack_int const* nb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytri2x(...) LAPACK_ssytri2x_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytri2x(...) LAPACK_ssytri2x_base(__VA_ARGS__)
#endif

#define LAPACK_zsytri2x_base LAPACK_GLOBAL(zsytri2x,ZSYTRI2X)
void LAPACK_zsytri2x_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* work, lapack_int const* nb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytri2x(...) LAPACK_zsytri2x_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytri2x(...) LAPACK_zsytri2x_base(__VA_ARGS__)
#endif

#define LAPACK_csytri_3_base LAPACK_GLOBAL(csytri_3,CSYTRI_3)
void LAPACK_csytri_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* E, lapack_int const* ipiv,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytri_3(...) LAPACK_csytri_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytri_3(...) LAPACK_csytri_3_base(__VA_ARGS__)
#endif

#define LAPACK_dsytri_3_base LAPACK_GLOBAL(dsytri_3,DSYTRI_3)
void LAPACK_dsytri_3_base(
    char const* uplo,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    double const* E, lapack_int const* ipiv,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytri_3(...) LAPACK_dsytri_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytri_3(...) LAPACK_dsytri_3_base(__VA_ARGS__)
#endif

#define LAPACK_ssytri_3_base LAPACK_GLOBAL(ssytri_3,SSYTRI_3)
void LAPACK_ssytri_3_base(
    char const* uplo,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    float const* E, lapack_int const* ipiv,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytri_3(...) LAPACK_ssytri_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytri_3(...) LAPACK_ssytri_3_base(__VA_ARGS__)
#endif

#define LAPACK_zsytri_3_base LAPACK_GLOBAL(zsytri_3,ZSYTRI_3)
void LAPACK_zsytri_3_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* E, lapack_int const* ipiv,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytri_3(...) LAPACK_zsytri_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytri_3(...) LAPACK_zsytri_3_base(__VA_ARGS__)
#endif

#define LAPACK_csytrs_base LAPACK_GLOBAL(csytrs,CSYTRS)
void LAPACK_csytrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrs(...) LAPACK_csytrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrs(...) LAPACK_csytrs_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrs_base LAPACK_GLOBAL(dsytrs,DSYTRS)
void LAPACK_dsytrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrs(...) LAPACK_dsytrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrs(...) LAPACK_dsytrs_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrs_base LAPACK_GLOBAL(ssytrs,SSYTRS)
void LAPACK_ssytrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrs(...) LAPACK_ssytrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrs(...) LAPACK_ssytrs_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrs_base LAPACK_GLOBAL(zsytrs,ZSYTRS)
void LAPACK_zsytrs_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrs(...) LAPACK_zsytrs_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrs(...) LAPACK_zsytrs_base(__VA_ARGS__)
#endif

#define LAPACK_csytrs2_base LAPACK_GLOBAL(csytrs2,CSYTRS2)
void LAPACK_csytrs2_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    const lapack_complex_float* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrs2(...) LAPACK_csytrs2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrs2(...) LAPACK_csytrs2_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrs2_base LAPACK_GLOBAL(dsytrs2,DSYTRS2)
void LAPACK_dsytrs2_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    const double* A, lapack_int const* lda, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrs2(...) LAPACK_dsytrs2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrs2(...) LAPACK_dsytrs2_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrs2_base LAPACK_GLOBAL(ssytrs2,SSYTRS2)
void LAPACK_ssytrs2_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    const float* A, lapack_int const* lda, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrs2(...) LAPACK_ssytrs2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrs2(...) LAPACK_ssytrs2_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrs2_base LAPACK_GLOBAL(zsytrs2,ZSYTRS2)
void LAPACK_zsytrs2_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    const lapack_complex_double* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrs2(...) LAPACK_zsytrs2_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrs2(...) LAPACK_zsytrs2_base(__VA_ARGS__)
#endif

#define LAPACK_csytrs_3_base LAPACK_GLOBAL(csytrs_3,CSYTRS_3)
void LAPACK_csytrs_3_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* E, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrs_3(...) LAPACK_csytrs_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrs_3(...) LAPACK_csytrs_3_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrs_3_base LAPACK_GLOBAL(dsytrs_3,DSYTRS_3)
void LAPACK_dsytrs_3_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* E, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrs_3(...) LAPACK_dsytrs_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrs_3(...) LAPACK_dsytrs_3_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrs_3_base LAPACK_GLOBAL(ssytrs_3,SSYTRS_3)
void LAPACK_ssytrs_3_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* E, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrs_3(...) LAPACK_ssytrs_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrs_3(...) LAPACK_ssytrs_3_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrs_3_base LAPACK_GLOBAL(zsytrs_3,ZSYTRS_3)
void LAPACK_zsytrs_3_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* E, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrs_3(...) LAPACK_zsytrs_3_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrs_3(...) LAPACK_zsytrs_3_base(__VA_ARGS__)
#endif

#define LAPACK_csytrs_aa_base LAPACK_GLOBAL(csytrs_aa,CSYTRS_AA)
void LAPACK_csytrs_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrs_aa(...) LAPACK_csytrs_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrs_aa(...) LAPACK_csytrs_aa_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrs_aa_base LAPACK_GLOBAL(dsytrs_aa,DSYTRS_AA)
void LAPACK_dsytrs_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrs_aa(...) LAPACK_dsytrs_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrs_aa(...) LAPACK_dsytrs_aa_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrs_aa_base LAPACK_GLOBAL(ssytrs_aa,SSYTRS_AA)
void LAPACK_ssytrs_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrs_aa(...) LAPACK_ssytrs_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrs_aa(...) LAPACK_ssytrs_aa_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrs_aa_base LAPACK_GLOBAL(zsytrs_aa,ZSYTRS_AA)
void LAPACK_zsytrs_aa_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrs_aa(...) LAPACK_zsytrs_aa_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrs_aa(...) LAPACK_zsytrs_aa_base(__VA_ARGS__)
#endif

#define LAPACK_csytrs_aa_2stage_base LAPACK_GLOBAL(csytrs_aa_2stage,CSYTRS_AA_2STAGE)
void LAPACK_csytrs_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* TB, lapack_int const* ltb, lapack_int const* ipiv, lapack_int const* ipiv2,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrs_aa_2stage(...) LAPACK_csytrs_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrs_aa_2stage(...) LAPACK_csytrs_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrs_aa_2stage_base LAPACK_GLOBAL(dsytrs_aa_2stage,DSYTRS_AA_2STAGE)
void LAPACK_dsytrs_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double* TB, lapack_int const* ltb, lapack_int const* ipiv, lapack_int const* ipiv2,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrs_aa_2stage(...) LAPACK_dsytrs_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrs_aa_2stage(...) LAPACK_dsytrs_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrs_aa_2stage_base LAPACK_GLOBAL(ssytrs_aa_2stage,SSYTRS_AA_2STAGE)
void LAPACK_ssytrs_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float* TB, lapack_int const* ltb, lapack_int const* ipiv, lapack_int const* ipiv2,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrs_aa_2stage(...) LAPACK_ssytrs_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrs_aa_2stage(...) LAPACK_ssytrs_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrs_aa_2stage_base LAPACK_GLOBAL(zsytrs_aa_2stage,ZSYTRS_AA_2STAGE)
void LAPACK_zsytrs_aa_2stage_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* TB, lapack_int const* ltb, lapack_int const* ipiv, lapack_int const* ipiv2,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrs_aa_2stage(...) LAPACK_zsytrs_aa_2stage_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrs_aa_2stage(...) LAPACK_zsytrs_aa_2stage_base(__VA_ARGS__)
#endif

#define LAPACK_csytrs_rook_base LAPACK_GLOBAL(csytrs_rook,CSYTRS_ROOK)
void LAPACK_csytrs_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_csytrs_rook(...) LAPACK_csytrs_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_csytrs_rook(...) LAPACK_csytrs_rook_base(__VA_ARGS__)
#endif

#define LAPACK_dsytrs_rook_base LAPACK_GLOBAL(dsytrs_rook,DSYTRS_ROOK)
void LAPACK_dsytrs_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda, lapack_int const* ipiv,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dsytrs_rook(...) LAPACK_dsytrs_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dsytrs_rook(...) LAPACK_dsytrs_rook_base(__VA_ARGS__)
#endif

#define LAPACK_ssytrs_rook_base LAPACK_GLOBAL(ssytrs_rook,SSYTRS_ROOK)
void LAPACK_ssytrs_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda, lapack_int const* ipiv,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ssytrs_rook(...) LAPACK_ssytrs_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ssytrs_rook(...) LAPACK_ssytrs_rook_base(__VA_ARGS__)
#endif

#define LAPACK_zsytrs_rook_base LAPACK_GLOBAL(zsytrs_rook,ZSYTRS_ROOK)
void LAPACK_zsytrs_rook_base(
    char const* uplo,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda, lapack_int const* ipiv,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zsytrs_rook(...) LAPACK_zsytrs_rook_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zsytrs_rook(...) LAPACK_zsytrs_rook_base(__VA_ARGS__)
#endif

#define LAPACK_ctbcon_base LAPACK_GLOBAL(ctbcon,CTBCON)
void LAPACK_ctbcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_float const* AB, lapack_int const* ldab,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctbcon(...) LAPACK_ctbcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctbcon(...) LAPACK_ctbcon_base(__VA_ARGS__)
#endif

#define LAPACK_dtbcon_base LAPACK_GLOBAL(dtbcon,DTBCON)
void LAPACK_dtbcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* kd,
    double const* AB, lapack_int const* ldab,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtbcon(...) LAPACK_dtbcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtbcon(...) LAPACK_dtbcon_base(__VA_ARGS__)
#endif

#define LAPACK_stbcon_base LAPACK_GLOBAL(stbcon,STBCON)
void LAPACK_stbcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* kd,
    float const* AB, lapack_int const* ldab,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stbcon(...) LAPACK_stbcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stbcon(...) LAPACK_stbcon_base(__VA_ARGS__)
#endif

#define LAPACK_ztbcon_base LAPACK_GLOBAL(ztbcon,ZTBCON)
void LAPACK_ztbcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n, lapack_int const* kd,
    lapack_complex_double const* AB, lapack_int const* ldab,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztbcon(...) LAPACK_ztbcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztbcon(...) LAPACK_ztbcon_base(__VA_ARGS__)
#endif

#define LAPACK_ctbrfs_base LAPACK_GLOBAL(ctbrfs,CTBRFS)
void LAPACK_ctbrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_float const* AB, lapack_int const* ldab,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float const* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctbrfs(...) LAPACK_ctbrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctbrfs(...) LAPACK_ctbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_dtbrfs_base LAPACK_GLOBAL(dtbrfs,DTBRFS)
void LAPACK_dtbrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    double const* AB, lapack_int const* ldab,
    double const* B, lapack_int const* ldb,
    double const* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtbrfs(...) LAPACK_dtbrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtbrfs(...) LAPACK_dtbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_stbrfs_base LAPACK_GLOBAL(stbrfs,STBRFS)
void LAPACK_stbrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    float const* AB, lapack_int const* ldab,
    float const* B, lapack_int const* ldb,
    float const* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stbrfs(...) LAPACK_stbrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stbrfs(...) LAPACK_stbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_ztbrfs_base LAPACK_GLOBAL(ztbrfs,ZTBRFS)
void LAPACK_ztbrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_double const* AB, lapack_int const* ldab,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double const* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztbrfs(...) LAPACK_ztbrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztbrfs(...) LAPACK_ztbrfs_base(__VA_ARGS__)
#endif

#define LAPACK_ctbtrs_base LAPACK_GLOBAL(ctbtrs,CTBTRS)
void LAPACK_ctbtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_float const* AB, lapack_int const* ldab,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctbtrs(...) LAPACK_ctbtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctbtrs(...) LAPACK_ctbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_dtbtrs_base LAPACK_GLOBAL(dtbtrs,DTBTRS)
void LAPACK_dtbtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    double const* AB, lapack_int const* ldab,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtbtrs(...) LAPACK_dtbtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtbtrs(...) LAPACK_dtbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_stbtrs_base LAPACK_GLOBAL(stbtrs,STBTRS)
void LAPACK_stbtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    float const* AB, lapack_int const* ldab,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stbtrs(...) LAPACK_stbtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stbtrs(...) LAPACK_stbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_ztbtrs_base LAPACK_GLOBAL(ztbtrs,ZTBTRS)
void LAPACK_ztbtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* kd, lapack_int const* nrhs,
    lapack_complex_double const* AB, lapack_int const* ldab,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztbtrs(...) LAPACK_ztbtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztbtrs(...) LAPACK_ztbtrs_base(__VA_ARGS__)
#endif

#define LAPACK_ctfsm_base LAPACK_GLOBAL(ctfsm,CTFSM)
void LAPACK_ctfsm_base(
    char const* transr, char const* side, char const* uplo, char const* trans, char const* diag,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* alpha,
    lapack_complex_float const* A,
    lapack_complex_float* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctfsm(...) LAPACK_ctfsm_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_ctfsm(...) LAPACK_ctfsm_base(__VA_ARGS__)
#endif

#define LAPACK_dtfsm_base LAPACK_GLOBAL(dtfsm,DTFSM)
void LAPACK_dtfsm_base(
    char const* transr, char const* side, char const* uplo, char const* trans, char const* diag,
    lapack_int const* m, lapack_int const* n,
    double const* alpha,
    double const* A,
    double* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtfsm(...) LAPACK_dtfsm_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_dtfsm(...) LAPACK_dtfsm_base(__VA_ARGS__)
#endif

#define LAPACK_stfsm_base LAPACK_GLOBAL(stfsm,STFSM)
void LAPACK_stfsm_base(
    char const* transr, char const* side, char const* uplo, char const* trans, char const* diag,
    lapack_int const* m, lapack_int const* n,
    float const* alpha,
    float const* A,
    float* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stfsm(...) LAPACK_stfsm_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_stfsm(...) LAPACK_stfsm_base(__VA_ARGS__)
#endif

#define LAPACK_ztfsm_base LAPACK_GLOBAL(ztfsm,ZTFSM)
void LAPACK_ztfsm_base(
    char const* transr, char const* side, char const* uplo, char const* trans, char const* diag,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* alpha,
    lapack_complex_double const* A,
    lapack_complex_double* B, lapack_int const* ldb
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztfsm(...) LAPACK_ztfsm_base(__VA_ARGS__, 1, 1, 1, 1, 1)
#else
    #define LAPACK_ztfsm(...) LAPACK_ztfsm_base(__VA_ARGS__)
#endif

#define LAPACK_ctftri_base LAPACK_GLOBAL(ctftri,CTFTRI)
void LAPACK_ctftri_base(
    char const* transr, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_float* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctftri(...) LAPACK_ctftri_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctftri(...) LAPACK_ctftri_base(__VA_ARGS__)
#endif

#define LAPACK_dtftri_base LAPACK_GLOBAL(dtftri,DTFTRI)
void LAPACK_dtftri_base(
    char const* transr, char const* uplo, char const* diag,
    lapack_int const* n,
    double* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtftri(...) LAPACK_dtftri_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtftri(...) LAPACK_dtftri_base(__VA_ARGS__)
#endif

#define LAPACK_stftri_base LAPACK_GLOBAL(stftri,STFTRI)
void LAPACK_stftri_base(
    char const* transr, char const* uplo, char const* diag,
    lapack_int const* n,
    float* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stftri(...) LAPACK_stftri_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stftri(...) LAPACK_stftri_base(__VA_ARGS__)
#endif

#define LAPACK_ztftri_base LAPACK_GLOBAL(ztftri,ZTFTRI)
void LAPACK_ztftri_base(
    char const* transr, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_double* A,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztftri(...) LAPACK_ztftri_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztftri(...) LAPACK_ztftri_base(__VA_ARGS__)
#endif

#define LAPACK_ctfttp_base LAPACK_GLOBAL(ctfttp,CTFTTP)
void LAPACK_ctfttp_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* ARF,
    lapack_complex_float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctfttp(...) LAPACK_ctfttp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctfttp(...) LAPACK_ctfttp_base(__VA_ARGS__)
#endif

#define LAPACK_dtfttp_base LAPACK_GLOBAL(dtfttp,DTFTTP)
void LAPACK_dtfttp_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    double const* ARF,
    double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtfttp(...) LAPACK_dtfttp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtfttp(...) LAPACK_dtfttp_base(__VA_ARGS__)
#endif

#define LAPACK_stfttp_base LAPACK_GLOBAL(stfttp,STFTTP)
void LAPACK_stfttp_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    float const* ARF,
    float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stfttp(...) LAPACK_stfttp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stfttp(...) LAPACK_stfttp_base(__VA_ARGS__)
#endif

#define LAPACK_ztfttp_base LAPACK_GLOBAL(ztfttp,ZTFTTP)
void LAPACK_ztfttp_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* ARF,
    lapack_complex_double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztfttp(...) LAPACK_ztfttp_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztfttp(...) LAPACK_ztfttp_base(__VA_ARGS__)
#endif

#define LAPACK_ctfttr_base LAPACK_GLOBAL(ctfttr,CTFTTR)
void LAPACK_ctfttr_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* ARF,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctfttr(...) LAPACK_ctfttr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctfttr(...) LAPACK_ctfttr_base(__VA_ARGS__)
#endif

#define LAPACK_dtfttr_base LAPACK_GLOBAL(dtfttr,DTFTTR)
void LAPACK_dtfttr_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    double const* ARF,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtfttr(...) LAPACK_dtfttr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtfttr(...) LAPACK_dtfttr_base(__VA_ARGS__)
#endif

#define LAPACK_stfttr_base LAPACK_GLOBAL(stfttr,STFTTR)
void LAPACK_stfttr_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    float const* ARF,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stfttr(...) LAPACK_stfttr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stfttr(...) LAPACK_stfttr_base(__VA_ARGS__)
#endif

#define LAPACK_ztfttr_base LAPACK_GLOBAL(ztfttr,ZTFTTR)
void LAPACK_ztfttr_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* ARF,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztfttr(...) LAPACK_ztfttr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztfttr(...) LAPACK_ztfttr_base(__VA_ARGS__)
#endif

#define LAPACK_ctgevc_base LAPACK_GLOBAL(ctgevc,CTGEVC)
void LAPACK_ctgevc_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_float const* S, lapack_int const* lds,
    lapack_complex_float const* P, lapack_int const* ldp,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctgevc(...) LAPACK_ctgevc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctgevc(...) LAPACK_ctgevc_base(__VA_ARGS__)
#endif

#define LAPACK_dtgevc_base LAPACK_GLOBAL(dtgevc,DTGEVC)
void LAPACK_dtgevc_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    double const* S, lapack_int const* lds,
    double const* P, lapack_int const* ldp,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtgevc(...) LAPACK_dtgevc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtgevc(...) LAPACK_dtgevc_base(__VA_ARGS__)
#endif

#define LAPACK_stgevc_base LAPACK_GLOBAL(stgevc,STGEVC)
void LAPACK_stgevc_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    float const* S, lapack_int const* lds,
    float const* P, lapack_int const* ldp,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stgevc(...) LAPACK_stgevc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stgevc(...) LAPACK_stgevc_base(__VA_ARGS__)
#endif

#define LAPACK_ztgevc_base LAPACK_GLOBAL(ztgevc,ZTGEVC)
void LAPACK_ztgevc_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_double const* S, lapack_int const* lds,
    lapack_complex_double const* P, lapack_int const* ldp,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztgevc(...) LAPACK_ztgevc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztgevc(...) LAPACK_ztgevc_base(__VA_ARGS__)
#endif

#define LAPACK_ctgexc LAPACK_GLOBAL(ctgexc,CTGEXC)
void LAPACK_ctgexc(
    lapack_logical const* wantq, lapack_logical const* wantz, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* Z, lapack_int const* ldz, lapack_int const* ifst, lapack_int* ilst,
    lapack_int* info );

#define LAPACK_dtgexc LAPACK_GLOBAL(dtgexc,DTGEXC)
void LAPACK_dtgexc(
    lapack_logical const* wantq, lapack_logical const* wantz, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* Q, lapack_int const* ldq,
    double* Z, lapack_int const* ldz, lapack_int* ifst, lapack_int* ilst,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_stgexc LAPACK_GLOBAL(stgexc,STGEXC)
void LAPACK_stgexc(
    lapack_logical const* wantq, lapack_logical const* wantz, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* Q, lapack_int const* ldq,
    float* Z, lapack_int const* ldz, lapack_int* ifst, lapack_int* ilst,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_ztgexc LAPACK_GLOBAL(ztgexc,ZTGEXC)
void LAPACK_ztgexc(
    lapack_logical const* wantq, lapack_logical const* wantz, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* Z, lapack_int const* ldz, lapack_int const* ifst, lapack_int* ilst,
    lapack_int* info );

#define LAPACK_ctgsen LAPACK_GLOBAL(ctgsen,CTGSEN)
void LAPACK_ctgsen(
    lapack_int const* ijob, lapack_logical const* wantq, lapack_logical const* wantz, lapack_logical const* select, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* alpha,
    lapack_complex_float* beta,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* Z, lapack_int const* ldz, lapack_int* m,
    float* pl,
    float* pr,
    float* DIF,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info );

#define LAPACK_dtgsen LAPACK_GLOBAL(dtgsen,DTGSEN)
void LAPACK_dtgsen(
    lapack_int const* ijob, lapack_logical const* wantq, lapack_logical const* wantz, lapack_logical const* select, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* alphar,
    double* alphai,
    double* beta,
    double* Q, lapack_int const* ldq,
    double* Z, lapack_int const* ldz, lapack_int* m,
    double* pl,
    double* pr,
    double* DIF,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info );

#define LAPACK_stgsen LAPACK_GLOBAL(stgsen,STGSEN)
void LAPACK_stgsen(
    lapack_int const* ijob, lapack_logical const* wantq, lapack_logical const* wantz, lapack_logical const* select, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* alphar,
    float* alphai,
    float* beta,
    float* Q, lapack_int const* ldq,
    float* Z, lapack_int const* ldz, lapack_int* m,
    float* pl,
    float* pr,
    float* DIF,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info );

#define LAPACK_ztgsen LAPACK_GLOBAL(ztgsen,ZTGSEN)
void LAPACK_ztgsen(
    lapack_int const* ijob, lapack_logical const* wantq, lapack_logical const* wantz, lapack_logical const* select, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* alpha,
    lapack_complex_double* beta,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* Z, lapack_int const* ldz, lapack_int* m,
    double* pl,
    double* pr,
    double* DIF,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info );

#define LAPACK_ctgsja_base LAPACK_GLOBAL(ctgsja,CTGSJA)
void LAPACK_ctgsja_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    float const* tola,
    float const* tolb,
    float* alpha,
    float* beta,
    lapack_complex_float* U, lapack_int const* ldu,
    lapack_complex_float* V, lapack_int const* ldv,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* work, lapack_int* ncycle,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctgsja(...) LAPACK_ctgsja_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctgsja(...) LAPACK_ctgsja_base(__VA_ARGS__)
#endif

#define LAPACK_dtgsja_base LAPACK_GLOBAL(dtgsja,DTGSJA)
void LAPACK_dtgsja_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double const* tola,
    double const* tolb,
    double* alpha,
    double* beta,
    double* U, lapack_int const* ldu,
    double* V, lapack_int const* ldv,
    double* Q, lapack_int const* ldq,
    double* work, lapack_int* ncycle,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtgsja(...) LAPACK_dtgsja_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtgsja(...) LAPACK_dtgsja_base(__VA_ARGS__)
#endif

#define LAPACK_stgsja_base LAPACK_GLOBAL(stgsja,STGSJA)
void LAPACK_stgsja_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float const* tola,
    float const* tolb,
    float* alpha,
    float* beta,
    float* U, lapack_int const* ldu,
    float* V, lapack_int const* ldv,
    float* Q, lapack_int const* ldq,
    float* work, lapack_int* ncycle,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stgsja(...) LAPACK_stgsja_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stgsja(...) LAPACK_stgsja_base(__VA_ARGS__)
#endif

#define LAPACK_ztgsja_base LAPACK_GLOBAL(ztgsja,ZTGSJA)
void LAPACK_ztgsja_base(
    char const* jobu, char const* jobv, char const* jobq,
    lapack_int const* m, lapack_int const* p, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    double const* tola,
    double const* tolb,
    double* alpha,
    double* beta,
    lapack_complex_double* U, lapack_int const* ldu,
    lapack_complex_double* V, lapack_int const* ldv,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* work, lapack_int* ncycle,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztgsja(...) LAPACK_ztgsja_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztgsja(...) LAPACK_ztgsja_base(__VA_ARGS__)
#endif

#define LAPACK_ctgsna_base LAPACK_GLOBAL(ctgsna,CTGSNA)
void LAPACK_ctgsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float const* VL, lapack_int const* ldvl,
    lapack_complex_float const* VR, lapack_int const* ldvr,
    float* S,
    float* DIF, lapack_int const* mm, lapack_int* m,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctgsna(...) LAPACK_ctgsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctgsna(...) LAPACK_ctgsna_base(__VA_ARGS__)
#endif

#define LAPACK_dtgsna_base LAPACK_GLOBAL(dtgsna,DTGSNA)
void LAPACK_dtgsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    double const* VL, lapack_int const* ldvl,
    double const* VR, lapack_int const* ldvr,
    double* S,
    double* DIF, lapack_int const* mm, lapack_int* m,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtgsna(...) LAPACK_dtgsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtgsna(...) LAPACK_dtgsna_base(__VA_ARGS__)
#endif

#define LAPACK_stgsna_base LAPACK_GLOBAL(stgsna,STGSNA)
void LAPACK_stgsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* B, lapack_int const* ldb,
    float const* VL, lapack_int const* ldvl,
    float const* VR, lapack_int const* ldvr,
    float* S,
    float* DIF, lapack_int const* mm, lapack_int* m,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stgsna(...) LAPACK_stgsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stgsna(...) LAPACK_stgsna_base(__VA_ARGS__)
#endif

#define LAPACK_ztgsna_base LAPACK_GLOBAL(ztgsna,ZTGSNA)
void LAPACK_ztgsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double const* VL, lapack_int const* ldvl,
    lapack_complex_double const* VR, lapack_int const* ldvr,
    double* S,
    double* DIF, lapack_int const* mm, lapack_int* m,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztgsna(...) LAPACK_ztgsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztgsna(...) LAPACK_ztgsna_base(__VA_ARGS__)
#endif

#define LAPACK_ctgsyl_base LAPACK_GLOBAL(ctgsyl,CTGSYL)
void LAPACK_ctgsyl_base(
    char const* trans,
    lapack_int const* ijob, lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float const* D, lapack_int const* ldd,
    lapack_complex_float const* E, lapack_int const* lde,
    lapack_complex_float* F, lapack_int const* ldf,
    float* dif,
    float* scale,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctgsyl(...) LAPACK_ctgsyl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ctgsyl(...) LAPACK_ctgsyl_base(__VA_ARGS__)
#endif

#define LAPACK_dtgsyl_base LAPACK_GLOBAL(dtgsyl,DTGSYL)
void LAPACK_dtgsyl_base(
    char const* trans,
    lapack_int const* ijob, lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    double* C, lapack_int const* ldc,
    double const* D, lapack_int const* ldd,
    double const* E, lapack_int const* lde,
    double* F, lapack_int const* ldf,
    double* dif,
    double* scale,
    double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtgsyl(...) LAPACK_dtgsyl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dtgsyl(...) LAPACK_dtgsyl_base(__VA_ARGS__)
#endif

#define LAPACK_stgsyl_base LAPACK_GLOBAL(stgsyl,STGSYL)
void LAPACK_stgsyl_base(
    char const* trans,
    lapack_int const* ijob, lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* B, lapack_int const* ldb,
    float* C, lapack_int const* ldc,
    float const* D, lapack_int const* ldd,
    float const* E, lapack_int const* lde,
    float* F, lapack_int const* ldf,
    float* dif,
    float* scale,
    float* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stgsyl(...) LAPACK_stgsyl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_stgsyl(...) LAPACK_stgsyl_base(__VA_ARGS__)
#endif

#define LAPACK_ztgsyl_base LAPACK_GLOBAL(ztgsyl,ZTGSYL)
void LAPACK_ztgsyl_base(
    char const* trans,
    lapack_int const* ijob, lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double const* D, lapack_int const* ldd,
    lapack_complex_double const* E, lapack_int const* lde,
    lapack_complex_double* F, lapack_int const* ldf,
    double* dif,
    double* scale,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztgsyl(...) LAPACK_ztgsyl_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ztgsyl(...) LAPACK_ztgsyl_base(__VA_ARGS__)
#endif

#define LAPACK_ctpcon_base LAPACK_GLOBAL(ctpcon,CTPCON)
void LAPACK_ctpcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_float const* AP,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctpcon(...) LAPACK_ctpcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctpcon(...) LAPACK_ctpcon_base(__VA_ARGS__)
#endif

#define LAPACK_dtpcon_base LAPACK_GLOBAL(dtpcon,DTPCON)
void LAPACK_dtpcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    double const* AP,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtpcon(...) LAPACK_dtpcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtpcon(...) LAPACK_dtpcon_base(__VA_ARGS__)
#endif

#define LAPACK_stpcon_base LAPACK_GLOBAL(stpcon,STPCON)
void LAPACK_stpcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    float const* AP,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stpcon(...) LAPACK_stpcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stpcon(...) LAPACK_stpcon_base(__VA_ARGS__)
#endif

#define LAPACK_ztpcon_base LAPACK_GLOBAL(ztpcon,ZTPCON)
void LAPACK_ztpcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_double const* AP,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztpcon(...) LAPACK_ztpcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztpcon(...) LAPACK_ztpcon_base(__VA_ARGS__)
#endif

#define LAPACK_ctplqt LAPACK_GLOBAL(ctplqt,CTPLQT)
void LAPACK_ctplqt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* mb,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dtplqt LAPACK_GLOBAL(dtplqt,DTPLQT)
void LAPACK_dtplqt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* mb,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* T, lapack_int const* ldt,
    double* work,
    lapack_int* info );

#define LAPACK_stplqt LAPACK_GLOBAL(stplqt,STPLQT)
void LAPACK_stplqt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* mb,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* T, lapack_int const* ldt,
    float* work,
    lapack_int* info );

#define LAPACK_ztplqt LAPACK_GLOBAL(ztplqt,ZTPLQT)
void LAPACK_ztplqt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* mb,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_ctplqt2 LAPACK_GLOBAL(ctplqt2,CTPLQT2)
void LAPACK_ctplqt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_dtplqt2 LAPACK_GLOBAL(dtplqt2,DTPLQT2)
void LAPACK_dtplqt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_stplqt2 LAPACK_GLOBAL(stplqt2,STPLQT2)
void LAPACK_stplqt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_ztplqt2 LAPACK_GLOBAL(ztplqt2,ZTPLQT2)
void LAPACK_ztplqt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_ctpmlqt_base LAPACK_GLOBAL(ctpmlqt,CTPMLQT)
void LAPACK_ctpmlqt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* mb,
    lapack_complex_float const* V, lapack_int const* ldv,
    lapack_complex_float const* T, lapack_int const* ldt,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctpmlqt(...) LAPACK_ctpmlqt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctpmlqt(...) LAPACK_ctpmlqt_base(__VA_ARGS__)
#endif

#define LAPACK_dtpmlqt_base LAPACK_GLOBAL(dtpmlqt,DTPMLQT)
void LAPACK_dtpmlqt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* mb,
    double const* V, lapack_int const* ldv,
    double const* T, lapack_int const* ldt,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtpmlqt(...) LAPACK_dtpmlqt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtpmlqt(...) LAPACK_dtpmlqt_base(__VA_ARGS__)
#endif

#define LAPACK_stpmlqt_base LAPACK_GLOBAL(stpmlqt,STPMLQT)
void LAPACK_stpmlqt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* mb,
    float const* V, lapack_int const* ldv,
    float const* T, lapack_int const* ldt,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stpmlqt(...) LAPACK_stpmlqt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stpmlqt(...) LAPACK_stpmlqt_base(__VA_ARGS__)
#endif

#define LAPACK_ztpmlqt_base LAPACK_GLOBAL(ztpmlqt,ZTPMLQT)
void LAPACK_ztpmlqt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* mb,
    lapack_complex_double const* V, lapack_int const* ldv,
    lapack_complex_double const* T, lapack_int const* ldt,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztpmlqt(...) LAPACK_ztpmlqt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztpmlqt(...) LAPACK_ztpmlqt_base(__VA_ARGS__)
#endif

#define LAPACK_ctpmqrt_base LAPACK_GLOBAL(ctpmqrt,CTPMQRT)
void LAPACK_ctpmqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* nb,
    lapack_complex_float const* V, lapack_int const* ldv,
    lapack_complex_float const* T, lapack_int const* ldt,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctpmqrt(...) LAPACK_ctpmqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctpmqrt(...) LAPACK_ctpmqrt_base(__VA_ARGS__)
#endif

#define LAPACK_dtpmqrt_base LAPACK_GLOBAL(dtpmqrt,DTPMQRT)
void LAPACK_dtpmqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* nb,
    double const* V, lapack_int const* ldv,
    double const* T, lapack_int const* ldt,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtpmqrt(...) LAPACK_dtpmqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtpmqrt(...) LAPACK_dtpmqrt_base(__VA_ARGS__)
#endif

#define LAPACK_stpmqrt_base LAPACK_GLOBAL(stpmqrt,STPMQRT)
void LAPACK_stpmqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* nb,
    float const* V, lapack_int const* ldv,
    float const* T, lapack_int const* ldt,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stpmqrt(...) LAPACK_stpmqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stpmqrt(...) LAPACK_stpmqrt_base(__VA_ARGS__)
#endif

#define LAPACK_ztpmqrt_base LAPACK_GLOBAL(ztpmqrt,ZTPMQRT)
void LAPACK_ztpmqrt_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l, lapack_int const* nb,
    lapack_complex_double const* V, lapack_int const* ldv,
    lapack_complex_double const* T, lapack_int const* ldt,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztpmqrt(...) LAPACK_ztpmqrt_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztpmqrt(...) LAPACK_ztpmqrt_base(__VA_ARGS__)
#endif

#define LAPACK_ctpqrt LAPACK_GLOBAL(ctpqrt,CTPQRT)
void LAPACK_ctpqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* nb,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* work,
    lapack_int* info );

#define LAPACK_dtpqrt LAPACK_GLOBAL(dtpqrt,DTPQRT)
void LAPACK_dtpqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* nb,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* T, lapack_int const* ldt,
    double* work,
    lapack_int* info );

#define LAPACK_stpqrt LAPACK_GLOBAL(stpqrt,STPQRT)
void LAPACK_stpqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* nb,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* T, lapack_int const* ldt,
    float* work,
    lapack_int* info );

#define LAPACK_ztpqrt LAPACK_GLOBAL(ztpqrt,ZTPQRT)
void LAPACK_ztpqrt(
    lapack_int const* m, lapack_int const* n, lapack_int const* l, lapack_int const* nb,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* work,
    lapack_int* info );

#define LAPACK_ctpqrt2 LAPACK_GLOBAL(ctpqrt2,CTPQRT2)
void LAPACK_ctpqrt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_dtpqrt2 LAPACK_GLOBAL(dtpqrt2,DTPQRT2)
void LAPACK_dtpqrt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_stpqrt2 LAPACK_GLOBAL(stpqrt2,STPQRT2)
void LAPACK_stpqrt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_ztpqrt2 LAPACK_GLOBAL(ztpqrt2,ZTPQRT2)
void LAPACK_ztpqrt2(
    lapack_int const* m, lapack_int const* n, lapack_int const* l,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_int* info );

#define LAPACK_ctprfb_base LAPACK_GLOBAL(ctprfb,CTPRFB)
void LAPACK_ctprfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    lapack_complex_float const* V, lapack_int const* ldv,
    lapack_complex_float const* T, lapack_int const* ldt,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_complex_float* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctprfb(...) LAPACK_ctprfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_ctprfb(...) LAPACK_ctprfb_base(__VA_ARGS__)
#endif

#define LAPACK_dtprfb_base LAPACK_GLOBAL(dtprfb,DTPRFB)
void LAPACK_dtprfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    double const* V, lapack_int const* ldv,
    double const* T, lapack_int const* ldt,
    double* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    double* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtprfb(...) LAPACK_dtprfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_dtprfb(...) LAPACK_dtprfb_base(__VA_ARGS__)
#endif

#define LAPACK_stprfb_base LAPACK_GLOBAL(stprfb,STPRFB)
void LAPACK_stprfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    float const* V, lapack_int const* ldv,
    float const* T, lapack_int const* ldt,
    float* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    float* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stprfb(...) LAPACK_stprfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_stprfb(...) LAPACK_stprfb_base(__VA_ARGS__)
#endif

#define LAPACK_ztprfb_base LAPACK_GLOBAL(ztprfb,ZTPRFB)
void LAPACK_ztprfb_base(
    char const* side, char const* trans, char const* direct, char const* storev,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    lapack_complex_double const* V, lapack_int const* ldv,
    lapack_complex_double const* T, lapack_int const* ldt,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_complex_double* work, lapack_int const* ldwork
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztprfb(...) LAPACK_ztprfb_base(__VA_ARGS__, 1, 1, 1, 1)
#else
    #define LAPACK_ztprfb(...) LAPACK_ztprfb_base(__VA_ARGS__)
#endif

#define LAPACK_ctprfs_base LAPACK_GLOBAL(ctprfs,CTPRFS)
void LAPACK_ctprfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float const* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctprfs(...) LAPACK_ctprfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctprfs(...) LAPACK_ctprfs_base(__VA_ARGS__)
#endif

#define LAPACK_dtprfs_base LAPACK_GLOBAL(dtprfs,DTPRFS)
void LAPACK_dtprfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    double const* AP,
    double const* B, lapack_int const* ldb,
    double const* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtprfs(...) LAPACK_dtprfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtprfs(...) LAPACK_dtprfs_base(__VA_ARGS__)
#endif

#define LAPACK_stprfs_base LAPACK_GLOBAL(stprfs,STPRFS)
void LAPACK_stprfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    float const* AP,
    float const* B, lapack_int const* ldb,
    float const* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stprfs(...) LAPACK_stprfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stprfs(...) LAPACK_stprfs_base(__VA_ARGS__)
#endif

#define LAPACK_ztprfs_base LAPACK_GLOBAL(ztprfs,ZTPRFS)
void LAPACK_ztprfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double const* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztprfs(...) LAPACK_ztprfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztprfs(...) LAPACK_ztprfs_base(__VA_ARGS__)
#endif

#define LAPACK_ctptri_base LAPACK_GLOBAL(ctptri,CTPTRI)
void LAPACK_ctptri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctptri(...) LAPACK_ctptri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctptri(...) LAPACK_ctptri_base(__VA_ARGS__)
#endif

#define LAPACK_dtptri_base LAPACK_GLOBAL(dtptri,DTPTRI)
void LAPACK_dtptri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtptri(...) LAPACK_dtptri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtptri(...) LAPACK_dtptri_base(__VA_ARGS__)
#endif

#define LAPACK_stptri_base LAPACK_GLOBAL(stptri,STPTRI)
void LAPACK_stptri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stptri(...) LAPACK_stptri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stptri(...) LAPACK_stptri_base(__VA_ARGS__)
#endif

#define LAPACK_ztptri_base LAPACK_GLOBAL(ztptri,ZTPTRI)
void LAPACK_ztptri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztptri(...) LAPACK_ztptri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztptri(...) LAPACK_ztptri_base(__VA_ARGS__)
#endif

#define LAPACK_ctptrs_base LAPACK_GLOBAL(ctptrs,CTPTRS)
void LAPACK_ctptrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* AP,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctptrs(...) LAPACK_ctptrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctptrs(...) LAPACK_ctptrs_base(__VA_ARGS__)
#endif

#define LAPACK_dtptrs_base LAPACK_GLOBAL(dtptrs,DTPTRS)
void LAPACK_dtptrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    double const* AP,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtptrs(...) LAPACK_dtptrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtptrs(...) LAPACK_dtptrs_base(__VA_ARGS__)
#endif

#define LAPACK_stptrs_base LAPACK_GLOBAL(stptrs,STPTRS)
void LAPACK_stptrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    float const* AP,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stptrs(...) LAPACK_stptrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_stptrs(...) LAPACK_stptrs_base(__VA_ARGS__)
#endif

#define LAPACK_ztptrs_base LAPACK_GLOBAL(ztptrs,ZTPTRS)
void LAPACK_ztptrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* AP,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztptrs(...) LAPACK_ztptrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztptrs(...) LAPACK_ztptrs_base(__VA_ARGS__)
#endif

#define LAPACK_ctpttf_base LAPACK_GLOBAL(ctpttf,CTPTTF)
void LAPACK_ctpttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP,
    lapack_complex_float* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctpttf(...) LAPACK_ctpttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctpttf(...) LAPACK_ctpttf_base(__VA_ARGS__)
#endif

#define LAPACK_dtpttf_base LAPACK_GLOBAL(dtpttf,DTPTTF)
void LAPACK_dtpttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    double const* AP,
    double* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtpttf(...) LAPACK_dtpttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtpttf(...) LAPACK_dtpttf_base(__VA_ARGS__)
#endif

#define LAPACK_stpttf_base LAPACK_GLOBAL(stpttf,STPTTF)
void LAPACK_stpttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    float const* AP,
    float* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stpttf(...) LAPACK_stpttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_stpttf(...) LAPACK_stpttf_base(__VA_ARGS__)
#endif

#define LAPACK_ztpttf_base LAPACK_GLOBAL(ztpttf,ZTPTTF)
void LAPACK_ztpttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP,
    lapack_complex_double* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztpttf(...) LAPACK_ztpttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztpttf(...) LAPACK_ztpttf_base(__VA_ARGS__)
#endif

#define LAPACK_ctpttr_base LAPACK_GLOBAL(ctpttr,CTPTTR)
void LAPACK_ctpttr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctpttr(...) LAPACK_ctpttr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ctpttr(...) LAPACK_ctpttr_base(__VA_ARGS__)
#endif

#define LAPACK_dtpttr_base LAPACK_GLOBAL(dtpttr,DTPTTR)
void LAPACK_dtpttr_base(
    char const* uplo,
    lapack_int const* n,
    double const* AP,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtpttr(...) LAPACK_dtpttr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dtpttr(...) LAPACK_dtpttr_base(__VA_ARGS__)
#endif

#define LAPACK_stpttr_base LAPACK_GLOBAL(stpttr,STPTTR)
void LAPACK_stpttr_base(
    char const* uplo,
    lapack_int const* n,
    float const* AP,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_stpttr(...) LAPACK_stpttr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_stpttr(...) LAPACK_stpttr_base(__VA_ARGS__)
#endif

#define LAPACK_ztpttr_base LAPACK_GLOBAL(ztpttr,ZTPTTR)
void LAPACK_ztpttr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztpttr(...) LAPACK_ztpttr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ztpttr(...) LAPACK_ztpttr_base(__VA_ARGS__)
#endif

#define LAPACK_ctrcon_base LAPACK_GLOBAL(ctrcon,CTRCON)
void LAPACK_ctrcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    float* rcond,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrcon(...) LAPACK_ctrcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctrcon(...) LAPACK_ctrcon_base(__VA_ARGS__)
#endif

#define LAPACK_dtrcon_base LAPACK_GLOBAL(dtrcon,DTRCON)
void LAPACK_dtrcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* rcond,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrcon(...) LAPACK_dtrcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtrcon(...) LAPACK_dtrcon_base(__VA_ARGS__)
#endif

#define LAPACK_strcon_base LAPACK_GLOBAL(strcon,STRCON)
void LAPACK_strcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* rcond,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strcon(...) LAPACK_strcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_strcon(...) LAPACK_strcon_base(__VA_ARGS__)
#endif

#define LAPACK_ztrcon_base LAPACK_GLOBAL(ztrcon,ZTRCON)
void LAPACK_ztrcon_base(
    char const* norm, char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    double* rcond,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrcon(...) LAPACK_ztrcon_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztrcon(...) LAPACK_ztrcon_base(__VA_ARGS__)
#endif

#define LAPACK_ctrevc_base LAPACK_GLOBAL(ctrevc,CTREVC)
void LAPACK_ctrevc_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrevc(...) LAPACK_ctrevc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrevc(...) LAPACK_ctrevc_base(__VA_ARGS__)
#endif

#define LAPACK_dtrevc_base LAPACK_GLOBAL(dtrevc,DTREVC)
void LAPACK_dtrevc_base(
    char const* side, char const* howmny,
    lapack_logical* select,
    lapack_int const* n,
    double const* T, lapack_int const* ldt,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrevc(...) LAPACK_dtrevc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrevc(...) LAPACK_dtrevc_base(__VA_ARGS__)
#endif

#define LAPACK_strevc_base LAPACK_GLOBAL(strevc,STREVC)
void LAPACK_strevc_base(
    char const* side, char const* howmny,
    lapack_logical* select,
    lapack_int const* n,
    float const* T, lapack_int const* ldt,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strevc(...) LAPACK_strevc_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strevc(...) LAPACK_strevc_base(__VA_ARGS__)
#endif

#define LAPACK_ztrevc_base LAPACK_GLOBAL(ztrevc,ZTREVC)
void LAPACK_ztrevc_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrevc(...) LAPACK_ztrevc_base(__VA_ARGS__, (size_t)1, 1)
#else
    #define LAPACK_ztrevc(...) LAPACK_ztrevc_base(__VA_ARGS__)
#endif

#define LAPACK_ctrevc3_base LAPACK_GLOBAL(ctrevc3,CTREVC3)
void LAPACK_ctrevc3_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* VL, lapack_int const* ldvl,
    lapack_complex_float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrevc3(...) LAPACK_ctrevc3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrevc3(...) LAPACK_ctrevc3_base(__VA_ARGS__)
#endif

#define LAPACK_dtrevc3_base LAPACK_GLOBAL(dtrevc3,DTREVC3)
void LAPACK_dtrevc3_base(
    char const* side, char const* howmny,
    lapack_logical* select,
    lapack_int const* n,
    double const* T, lapack_int const* ldt,
    double* VL, lapack_int const* ldvl,
    double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrevc3(...) LAPACK_dtrevc3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrevc3(...) LAPACK_dtrevc3_base(__VA_ARGS__)
#endif

#define LAPACK_strevc3_base LAPACK_GLOBAL(strevc3,STREVC3)
void LAPACK_strevc3_base(
    char const* side, char const* howmny,
    lapack_logical* select,
    lapack_int const* n,
    float const* T, lapack_int const* ldt,
    float* VL, lapack_int const* ldvl,
    float* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strevc3(...) LAPACK_strevc3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strevc3(...) LAPACK_strevc3_base(__VA_ARGS__)
#endif

#define LAPACK_ztrevc3_base LAPACK_GLOBAL(ztrevc3,ZTREVC3)
void LAPACK_ztrevc3_base(
    char const* side, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* VL, lapack_int const* ldvl,
    lapack_complex_double* VR, lapack_int const* ldvr, lapack_int const* mm, lapack_int* m,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrevc3(...) LAPACK_ztrevc3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztrevc3(...) LAPACK_ztrevc3_base(__VA_ARGS__)
#endif

#define LAPACK_ctrexc_base LAPACK_GLOBAL(ctrexc,CTREXC)
void LAPACK_ctrexc_base(
    char const* compq,
    lapack_int const* n,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* Q, lapack_int const* ldq, lapack_int const* ifst, lapack_int const* ilst,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrexc(...) LAPACK_ctrexc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ctrexc(...) LAPACK_ctrexc_base(__VA_ARGS__)
#endif

#define LAPACK_dtrexc_base LAPACK_GLOBAL(dtrexc,DTREXC)
void LAPACK_dtrexc_base(
    char const* compq,
    lapack_int const* n,
    double* T, lapack_int const* ldt,
    double* Q, lapack_int const* ldq, lapack_int* ifst, lapack_int* ilst,
    double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrexc(...) LAPACK_dtrexc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dtrexc(...) LAPACK_dtrexc_base(__VA_ARGS__)
#endif

#define LAPACK_strexc_base LAPACK_GLOBAL(strexc,STREXC)
void LAPACK_strexc_base(
    char const* compq,
    lapack_int const* n,
    float* T, lapack_int const* ldt,
    float* Q, lapack_int const* ldq, lapack_int* ifst, lapack_int* ilst,
    float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strexc(...) LAPACK_strexc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_strexc(...) LAPACK_strexc_base(__VA_ARGS__)
#endif

#define LAPACK_ztrexc_base LAPACK_GLOBAL(ztrexc,ZTREXC)
void LAPACK_ztrexc_base(
    char const* compq,
    lapack_int const* n,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* Q, lapack_int const* ldq, lapack_int const* ifst, lapack_int const* ilst,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrexc(...) LAPACK_ztrexc_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ztrexc(...) LAPACK_ztrexc_base(__VA_ARGS__)
#endif

#define LAPACK_ctrrfs_base LAPACK_GLOBAL(ctrrfs,CTRRFS)
void LAPACK_ctrrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float const* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    lapack_complex_float* work,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrrfs(...) LAPACK_ctrrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctrrfs(...) LAPACK_ctrrfs_base(__VA_ARGS__)
#endif

#define LAPACK_dtrrfs_base LAPACK_GLOBAL(dtrrfs,DTRRFS)
void LAPACK_dtrrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    double const* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    double* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrrfs(...) LAPACK_dtrrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtrrfs(...) LAPACK_dtrrfs_base(__VA_ARGS__)
#endif

#define LAPACK_strrfs_base LAPACK_GLOBAL(strrfs,STRRFS)
void LAPACK_strrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float const* B, lapack_int const* ldb,
    float const* X, lapack_int const* ldx,
    float* ferr,
    float* berr,
    float* work,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strrfs(...) LAPACK_strrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_strrfs(...) LAPACK_strrfs_base(__VA_ARGS__)
#endif

#define LAPACK_ztrrfs_base LAPACK_GLOBAL(ztrrfs,ZTRRFS)
void LAPACK_ztrrfs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double const* X, lapack_int const* ldx,
    double* ferr,
    double* berr,
    lapack_complex_double* work,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrrfs(...) LAPACK_ztrrfs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztrrfs(...) LAPACK_ztrrfs_base(__VA_ARGS__)
#endif

#define LAPACK_ctrsen_base LAPACK_GLOBAL(ctrsen,CTRSEN)
void LAPACK_ctrsen_base(
    char const* job, char const* compq,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_float* T, lapack_int const* ldt,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* W, lapack_int* m,
    float* s,
    float* sep,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrsen(...) LAPACK_ctrsen_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrsen(...) LAPACK_ctrsen_base(__VA_ARGS__)
#endif

#define LAPACK_dtrsen_base LAPACK_GLOBAL(dtrsen,DTRSEN)
void LAPACK_dtrsen_base(
    char const* job, char const* compq,
    lapack_logical const* select,
    lapack_int const* n,
    double* T, lapack_int const* ldt,
    double* Q, lapack_int const* ldq,
    double* WR,
    double* WI, lapack_int* m,
    double* s,
    double* sep,
    double* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrsen(...) LAPACK_dtrsen_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrsen(...) LAPACK_dtrsen_base(__VA_ARGS__)
#endif

#define LAPACK_strsen_base LAPACK_GLOBAL(strsen,STRSEN)
void LAPACK_strsen_base(
    char const* job, char const* compq,
    lapack_logical const* select,
    lapack_int const* n,
    float* T, lapack_int const* ldt,
    float* Q, lapack_int const* ldq,
    float* WR,
    float* WI, lapack_int* m,
    float* s,
    float* sep,
    float* work, lapack_int const* lwork,
    lapack_int* iwork, lapack_int const* liwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strsen(...) LAPACK_strsen_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strsen(...) LAPACK_strsen_base(__VA_ARGS__)
#endif

#define LAPACK_ztrsen_base LAPACK_GLOBAL(ztrsen,ZTRSEN)
void LAPACK_ztrsen_base(
    char const* job, char const* compq,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_double* T, lapack_int const* ldt,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* W, lapack_int* m,
    double* s,
    double* sep,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrsen(...) LAPACK_ztrsen_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztrsen(...) LAPACK_ztrsen_base(__VA_ARGS__)
#endif

#define LAPACK_ctrsna_base LAPACK_GLOBAL(ctrsna,CTRSNA)
void LAPACK_ctrsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_float const* T, lapack_int const* ldt,
    lapack_complex_float const* VL, lapack_int const* ldvl,
    lapack_complex_float const* VR, lapack_int const* ldvr,
    float* S,
    float* SEP, lapack_int const* mm, lapack_int* m,
    lapack_complex_float* work, lapack_int const* ldwork,
    float* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrsna(...) LAPACK_ctrsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrsna(...) LAPACK_ctrsna_base(__VA_ARGS__)
#endif

#define LAPACK_dtrsna_base LAPACK_GLOBAL(dtrsna,DTRSNA)
void LAPACK_dtrsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    double const* T, lapack_int const* ldt,
    double const* VL, lapack_int const* ldvl,
    double const* VR, lapack_int const* ldvr,
    double* S,
    double* SEP, lapack_int const* mm, lapack_int* m,
    double* work, lapack_int const* ldwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrsna(...) LAPACK_dtrsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrsna(...) LAPACK_dtrsna_base(__VA_ARGS__)
#endif

#define LAPACK_strsna_base LAPACK_GLOBAL(strsna,STRSNA)
void LAPACK_strsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    float const* T, lapack_int const* ldt,
    float const* VL, lapack_int const* ldvl,
    float const* VR, lapack_int const* ldvr,
    float* S,
    float* SEP, lapack_int const* mm, lapack_int* m,
    float* work, lapack_int const* ldwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strsna(...) LAPACK_strsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strsna(...) LAPACK_strsna_base(__VA_ARGS__)
#endif

#define LAPACK_ztrsna_base LAPACK_GLOBAL(ztrsna,ZTRSNA)
void LAPACK_ztrsna_base(
    char const* job, char const* howmny,
    lapack_logical const* select,
    lapack_int const* n,
    lapack_complex_double const* T, lapack_int const* ldt,
    lapack_complex_double const* VL, lapack_int const* ldvl,
    lapack_complex_double const* VR, lapack_int const* ldvr,
    double* S,
    double* SEP, lapack_int const* mm, lapack_int* m,
    lapack_complex_double* work, lapack_int const* ldwork,
    double* rwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrsna(...) LAPACK_ztrsna_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztrsna(...) LAPACK_ztrsna_base(__VA_ARGS__)
#endif

#define LAPACK_ctrsyl_base LAPACK_GLOBAL(ctrsyl,CTRSYL)
void LAPACK_ctrsyl_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* C, lapack_int const* ldc,
    float* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrsyl(...) LAPACK_ctrsyl_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrsyl(...) LAPACK_ctrsyl_base(__VA_ARGS__)
#endif

#define LAPACK_dtrsyl_base LAPACK_GLOBAL(dtrsyl,DTRSYL)
void LAPACK_dtrsyl_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    double* C, lapack_int const* ldc,
    double* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrsyl(...) LAPACK_dtrsyl_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrsyl(...) LAPACK_dtrsyl_base(__VA_ARGS__)
#endif

#define LAPACK_strsyl_base LAPACK_GLOBAL(strsyl,STRSYL)
void LAPACK_strsyl_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* B, lapack_int const* ldb,
    float* C, lapack_int const* ldc,
    float* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strsyl(...) LAPACK_strsyl_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strsyl(...) LAPACK_strsyl_base(__VA_ARGS__)
#endif

#define LAPACK_ztrsyl_base LAPACK_GLOBAL(ztrsyl,ZTRSYL)
void LAPACK_ztrsyl_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* C, lapack_int const* ldc,
    double* scale,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrsyl(...) LAPACK_ztrsyl_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztrsyl(...) LAPACK_ztrsyl_base(__VA_ARGS__)
#endif

#define LAPACK_ctrsyl3_base LAPACK_GLOBAL(ctrsyl3,CTRSYL3)
void LAPACK_ctrsyl3_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* B, lapack_int const* ldb,
    lapack_complex_float* C, lapack_int const* ldc, float* scale,
    float* swork, lapack_int const *ldswork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrsyl3(...) LAPACK_ctrsyl3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrsyl3(...) LAPACK_ctrsyl3_base(__VA_ARGS__)
#endif

#define LAPACK_dtrsyl3_base LAPACK_GLOBAL(dtrsyl3,DTRSYL3)
void LAPACK_dtrsyl3_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    double const* A, lapack_int const* lda,
    double const* B, lapack_int const* ldb,
    double* C, lapack_int const* ldc, double* scale,
    lapack_int* iwork, lapack_int const* liwork,
    double* swork, lapack_int const *ldswork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrsyl3(...) LAPACK_dtrsyl3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrsyl3(...) LAPACK_dtrsyl3_base(__VA_ARGS__)
#endif

#define LAPACK_strsyl3_base LAPACK_GLOBAL(strsyl3,STRSYL3)
void LAPACK_strsyl3_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    float const* A, lapack_int const* lda,
    float const* B, lapack_int const* ldb,
    float* C, lapack_int const* ldc, float* scale,
    lapack_int* iwork, lapack_int const* liwork,
    float* swork, lapack_int const *ldswork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strsyl3(...) LAPACK_strsyl3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strsyl3(...) LAPACK_strsyl3_base(__VA_ARGS__)
#endif

#define LAPACK_ztrsyl3_base LAPACK_GLOBAL(ztrsyl3,ZTRSYL3)
void LAPACK_ztrsyl3_base(
    char const* trana, char const* tranb,
    lapack_int const* isgn, lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* B, lapack_int const* ldb,
    lapack_complex_double* C, lapack_int const* ldc, double* scale,
    double* swork, lapack_int const *ldswork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrsyl3(...) LAPACK_ztrsyl3_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztrsyl3(...) LAPACK_ztrsyl3_base(__VA_ARGS__)
#endif

#define LAPACK_ctrtri_base LAPACK_GLOBAL(ctrtri,CTRTRI)
lapack_int LAPACK_ctrtri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrtri(...) LAPACK_ctrtri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrtri(...) LAPACK_ctrtri_base(__VA_ARGS__)
#endif

#define LAPACK_dtrtri_base LAPACK_GLOBAL(dtrtri,DTRTRI)
lapack_int LAPACK_dtrtri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrtri(...) LAPACK_dtrtri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrtri(...) LAPACK_dtrtri_base(__VA_ARGS__)
#endif

#define LAPACK_strtri_base LAPACK_GLOBAL(strtri,STRTRI)
lapack_int LAPACK_strtri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    float* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strtri(...) LAPACK_strtri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strtri(...) LAPACK_strtri_base(__VA_ARGS__)
#endif

#define LAPACK_ztrtri_base LAPACK_GLOBAL(ztrtri,ZTRTRI)
lapack_int LAPACK_ztrtri_base(
    char const* uplo, char const* diag,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrtri(...) LAPACK_ztrtri_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztrtri(...) LAPACK_ztrtri_base(__VA_ARGS__)
#endif

#define LAPACK_ctrtrs_base LAPACK_GLOBAL(ctrtrs,CTRTRS)
void LAPACK_ctrtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrtrs(...) LAPACK_ctrtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ctrtrs(...) LAPACK_ctrtrs_base(__VA_ARGS__)
#endif

#define LAPACK_dtrtrs_base LAPACK_GLOBAL(dtrtrs,DTRTRS)
void LAPACK_dtrtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    double const* A, lapack_int const* lda,
    double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrtrs(...) LAPACK_dtrtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_dtrtrs(...) LAPACK_dtrtrs_base(__VA_ARGS__)
#endif

#define LAPACK_strtrs_base LAPACK_GLOBAL(strtrs,STRTRS)
void LAPACK_strtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    float const* A, lapack_int const* lda,
    float* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strtrs(...) LAPACK_strtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_strtrs(...) LAPACK_strtrs_base(__VA_ARGS__)
#endif

#define LAPACK_ztrtrs_base LAPACK_GLOBAL(ztrtrs,ZTRTRS)
void LAPACK_ztrtrs_base(
    char const* uplo, char const* trans, char const* diag,
    lapack_int const* n, lapack_int const* nrhs,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* B, lapack_int const* ldb,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrtrs(...) LAPACK_ztrtrs_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_ztrtrs(...) LAPACK_ztrtrs_base(__VA_ARGS__)
#endif

#define LAPACK_ctrttf_base LAPACK_GLOBAL(ctrttf,CTRTTF)
void LAPACK_ctrttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrttf(...) LAPACK_ctrttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ctrttf(...) LAPACK_ctrttf_base(__VA_ARGS__)
#endif

#define LAPACK_dtrttf_base LAPACK_GLOBAL(dtrttf,DTRTTF)
void LAPACK_dtrttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrttf(...) LAPACK_dtrttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_dtrttf(...) LAPACK_dtrttf_base(__VA_ARGS__)
#endif

#define LAPACK_strttf_base LAPACK_GLOBAL(strttf,STRTTF)
void LAPACK_strttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strttf(...) LAPACK_strttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_strttf(...) LAPACK_strttf_base(__VA_ARGS__)
#endif

#define LAPACK_ztrttf_base LAPACK_GLOBAL(ztrttf,ZTRTTF)
void LAPACK_ztrttf_base(
    char const* transr, char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* ARF,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrttf(...) LAPACK_ztrttf_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_ztrttf(...) LAPACK_ztrttf_base(__VA_ARGS__)
#endif

#define LAPACK_ctrttp_base LAPACK_GLOBAL(ctrttp,CTRTTP)
void LAPACK_ctrttp_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ctrttp(...) LAPACK_ctrttp_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ctrttp(...) LAPACK_ctrttp_base(__VA_ARGS__)
#endif

#define LAPACK_dtrttp_base LAPACK_GLOBAL(dtrttp,DTRTTP)
void LAPACK_dtrttp_base(
    char const* uplo,
    lapack_int const* n,
    double const* A, lapack_int const* lda,
    double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_dtrttp(...) LAPACK_dtrttp_base(__VA_ARGS__, 1)
#else
    #define LAPACK_dtrttp(...) LAPACK_dtrttp_base(__VA_ARGS__)
#endif

#define LAPACK_strttp_base LAPACK_GLOBAL(strttp,STRTTP)
void LAPACK_strttp_base(
    char const* uplo,
    lapack_int const* n,
    float const* A, lapack_int const* lda,
    float* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_strttp(...) LAPACK_strttp_base(__VA_ARGS__, 1)
#else
    #define LAPACK_strttp(...) LAPACK_strttp_base(__VA_ARGS__)
#endif

#define LAPACK_ztrttp_base LAPACK_GLOBAL(ztrttp,ZTRTTP)
void LAPACK_ztrttp_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double* AP,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_ztrttp(...) LAPACK_ztrttp_base(__VA_ARGS__, 1)
#else
    #define LAPACK_ztrttp(...) LAPACK_ztrttp_base(__VA_ARGS__)
#endif

#define LAPACK_ctzrzf LAPACK_GLOBAL(ctzrzf,CTZRZF)
void LAPACK_ctzrzf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_dtzrzf LAPACK_GLOBAL(dtzrzf,DTZRZF)
void LAPACK_dtzrzf(
    lapack_int const* m, lapack_int const* n,
    double* A, lapack_int const* lda,
    double* tau,
    double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_stzrzf LAPACK_GLOBAL(stzrzf,STZRZF)
void LAPACK_stzrzf(
    lapack_int const* m, lapack_int const* n,
    float* A, lapack_int const* lda,
    float* tau,
    float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_ztzrzf LAPACK_GLOBAL(ztzrzf,ZTZRZF)
void LAPACK_ztzrzf(
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cunbdb_base LAPACK_GLOBAL(cunbdb,CUNBDB)
void LAPACK_cunbdb_base(
    char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    lapack_complex_float* X11, lapack_int const* ldx11,
    lapack_complex_float* X12, lapack_int const* ldx12,
    lapack_complex_float* X21, lapack_int const* ldx21,
    lapack_complex_float* X22, lapack_int const* ldx22,
    float* theta,
    float* phi,
    lapack_complex_float* TAUP1,
    lapack_complex_float* TAUP2,
    lapack_complex_float* TAUQ1,
    lapack_complex_float* TAUQ2,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunbdb(...) LAPACK_cunbdb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cunbdb(...) LAPACK_cunbdb_base(__VA_ARGS__)
#endif

#define LAPACK_zunbdb_base LAPACK_GLOBAL(zunbdb,ZUNBDB)
void LAPACK_zunbdb_base(
    char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    lapack_complex_double* X11, lapack_int const* ldx11,
    lapack_complex_double* X12, lapack_int const* ldx12,
    lapack_complex_double* X21, lapack_int const* ldx21,
    lapack_complex_double* X22, lapack_int const* ldx22,
    double* theta,
    double* phi,
    lapack_complex_double* TAUP1,
    lapack_complex_double* TAUP2,
    lapack_complex_double* TAUQ1,
    lapack_complex_double* TAUQ2,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunbdb(...) LAPACK_zunbdb_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zunbdb(...) LAPACK_zunbdb_base(__VA_ARGS__)
#endif

#define LAPACK_cuncsd_base LAPACK_GLOBAL(cuncsd,CUNCSD)
void LAPACK_cuncsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    lapack_complex_float* X11, lapack_int const* ldx11,
    lapack_complex_float* X12, lapack_int const* ldx12,
    lapack_complex_float* X21, lapack_int const* ldx21,
    lapack_complex_float* X22, lapack_int const* ldx22,
    float* theta,
    lapack_complex_float* U1, lapack_int const* ldu1,
    lapack_complex_float* U2, lapack_int const* ldu2,
    lapack_complex_float* V1T, lapack_int const* ldv1t,
    lapack_complex_float* V2T, lapack_int const* ldv2t,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cuncsd(...) LAPACK_cuncsd_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_cuncsd(...) LAPACK_cuncsd_base(__VA_ARGS__)
#endif

#define LAPACK_zuncsd_base LAPACK_GLOBAL(zuncsd,ZUNCSD)
void LAPACK_zuncsd_base(
    char const* jobu1, char const* jobu2, char const* jobv1t, char const* jobv2t, char const* trans, char const* signs,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    lapack_complex_double* X11, lapack_int const* ldx11,
    lapack_complex_double* X12, lapack_int const* ldx12,
    lapack_complex_double* X21, lapack_int const* ldx21,
    lapack_complex_double* X22, lapack_int const* ldx22,
    double* theta,
    lapack_complex_double* U1, lapack_int const* ldu1,
    lapack_complex_double* U2, lapack_int const* ldu2,
    lapack_complex_double* V1T, lapack_int const* ldv1t,
    lapack_complex_double* V2T, lapack_int const* ldv2t,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zuncsd(...) LAPACK_zuncsd_base(__VA_ARGS__, 1, 1, 1, 1, 1, 1)
#else
    #define LAPACK_zuncsd(...) LAPACK_zuncsd_base(__VA_ARGS__)
#endif

#define LAPACK_cuncsd2by1_base LAPACK_GLOBAL(cuncsd2by1,CUNCSD2BY1)
void LAPACK_cuncsd2by1_base(
    char const* jobu1, char const* jobu2, char const* jobv1t,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    lapack_complex_float* X11, lapack_int const* ldx11,
    lapack_complex_float* X21, lapack_int const* ldx21,
    float* theta,
    lapack_complex_float* U1, lapack_int const* ldu1,
    lapack_complex_float* U2, lapack_int const* ldu2,
    lapack_complex_float* V1T, lapack_int const* ldv1t,
    lapack_complex_float* work, lapack_int const* lwork,
    float* rwork, lapack_int const* lrwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cuncsd2by1(...) LAPACK_cuncsd2by1_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cuncsd2by1(...) LAPACK_cuncsd2by1_base(__VA_ARGS__)
#endif

#define LAPACK_zuncsd2by1_base LAPACK_GLOBAL(zuncsd2by1,ZUNCSD2BY1)
void LAPACK_zuncsd2by1_base(
    char const* jobu1, char const* jobu2, char const* jobv1t,
    lapack_int const* m, lapack_int const* p, lapack_int const* q,
    lapack_complex_double* X11, lapack_int const* ldx11,
    lapack_complex_double* X21, lapack_int const* ldx21,
    double* theta,
    lapack_complex_double* U1, lapack_int const* ldu1,
    lapack_complex_double* U2, lapack_int const* ldu2,
    lapack_complex_double* V1T, lapack_int const* ldv1t,
    lapack_complex_double* work, lapack_int const* lwork,
    double* rwork, lapack_int const* lrwork,
    lapack_int* iwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zuncsd2by1(...) LAPACK_zuncsd2by1_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zuncsd2by1(...) LAPACK_zuncsd2by1_base(__VA_ARGS__)
#endif

#define LAPACK_cungbr_base LAPACK_GLOBAL(cungbr,CUNGBR)
void LAPACK_cungbr_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cungbr(...) LAPACK_cungbr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cungbr(...) LAPACK_cungbr_base(__VA_ARGS__)
#endif

#define LAPACK_zungbr_base LAPACK_GLOBAL(zungbr,ZUNGBR)
void LAPACK_zungbr_base(
    char const* vect,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zungbr(...) LAPACK_zungbr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zungbr(...) LAPACK_zungbr_base(__VA_ARGS__)
#endif

#define LAPACK_cunghr LAPACK_GLOBAL(cunghr,CUNGHR)
void LAPACK_cunghr(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zunghr LAPACK_GLOBAL(zunghr,ZUNGHR)
void LAPACK_zunghr(
    lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cunglq LAPACK_GLOBAL(cunglq,CUNGLQ)
void LAPACK_cunglq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zunglq LAPACK_GLOBAL(zunglq,ZUNGLQ)
void LAPACK_zunglq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cungql LAPACK_GLOBAL(cungql,CUNGQL)
void LAPACK_cungql(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zungql LAPACK_GLOBAL(zungql,ZUNGQL)
void LAPACK_zungql(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cungqr LAPACK_GLOBAL(cungqr,CUNGQR)
void LAPACK_cungqr(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zungqr LAPACK_GLOBAL(zungqr,ZUNGQR)
void LAPACK_zungqr(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cungrq LAPACK_GLOBAL(cungrq,CUNGRQ)
void LAPACK_cungrq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zungrq LAPACK_GLOBAL(zungrq,ZUNGRQ)
void LAPACK_zungrq(
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cungtr_base LAPACK_GLOBAL(cungtr,CUNGTR)
void LAPACK_cungtr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cungtr(...) LAPACK_cungtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cungtr(...) LAPACK_cungtr_base(__VA_ARGS__)
#endif

#define LAPACK_zungtr_base LAPACK_GLOBAL(zungtr,ZUNGTR)
void LAPACK_zungtr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zungtr(...) LAPACK_zungtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zungtr(...) LAPACK_zungtr_base(__VA_ARGS__)
#endif

#define LAPACK_cungtsqr_row LAPACK_GLOBAL(cungtsqr_row,CUNGTSQR_ROW)
void LAPACK_cungtsqr_row(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb, lapack_int const* nb,
    lapack_complex_float* A, lapack_int const* lda,
    lapack_complex_float const* T, lapack_int const* ldt,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_zungtsqr_row LAPACK_GLOBAL(zungtsqr_row,ZUNGTSQR_ROW)
void LAPACK_zungtsqr_row(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* mb, lapack_int const* nb,
    lapack_complex_double* A, lapack_int const* lda,
    lapack_complex_double const* T, lapack_int const* ldt,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info );

#define LAPACK_cunhr_col LAPACK_GLOBAL(cunhr_col,CUNHR_COL)
void LAPACK_cunhr_col(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* nb, lapack_complex_float* A,
    lapack_int const* lda, lapack_complex_float* T,
    lapack_int const* ldt, lapack_complex_float* D,
    lapack_int* info );

#define LAPACK_zunhr_col LAPACK_GLOBAL(zunhr_col,ZUNHR_COL)
void LAPACK_zunhr_col(
    lapack_int const* m, lapack_int const* n,
    lapack_int const* nb, lapack_complex_double* A,
    lapack_int const* lda, lapack_complex_double* T,
    lapack_int const* ldt, lapack_complex_double* D,
    lapack_int* info );

#define LAPACK_cunmbr_base LAPACK_GLOBAL(cunmbr,CUNMBR)
void LAPACK_cunmbr_base(
    char const* vect, char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmbr(...) LAPACK_cunmbr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cunmbr(...) LAPACK_cunmbr_base(__VA_ARGS__)
#endif

#define LAPACK_zunmbr_base LAPACK_GLOBAL(zunmbr,ZUNMBR)
void LAPACK_zunmbr_base(
    char const* vect, char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmbr(...) LAPACK_zunmbr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zunmbr(...) LAPACK_zunmbr_base(__VA_ARGS__)
#endif

#define LAPACK_cunmhr_base LAPACK_GLOBAL(cunmhr,CUNMHR)
void LAPACK_cunmhr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmhr(...) LAPACK_cunmhr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cunmhr(...) LAPACK_cunmhr_base(__VA_ARGS__)
#endif

#define LAPACK_zunmhr_base LAPACK_GLOBAL(zunmhr,ZUNMHR)
void LAPACK_zunmhr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* ilo, lapack_int const* ihi,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmhr(...) LAPACK_zunmhr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zunmhr(...) LAPACK_zunmhr_base(__VA_ARGS__)
#endif

#define LAPACK_cunmlq_base LAPACK_GLOBAL(cunmlq,CUNMLQ)
void LAPACK_cunmlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmlq(...) LAPACK_cunmlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cunmlq(...) LAPACK_cunmlq_base(__VA_ARGS__)
#endif

#define LAPACK_zunmlq_base LAPACK_GLOBAL(zunmlq,ZUNMLQ)
void LAPACK_zunmlq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmlq(...) LAPACK_zunmlq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zunmlq(...) LAPACK_zunmlq_base(__VA_ARGS__)
#endif

#define LAPACK_cunmql_base LAPACK_GLOBAL(cunmql,CUNMQL)
void LAPACK_cunmql_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmql(...) LAPACK_cunmql_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cunmql(...) LAPACK_cunmql_base(__VA_ARGS__)
#endif

#define LAPACK_zunmql_base LAPACK_GLOBAL(zunmql,ZUNMQL)
void LAPACK_zunmql_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmql(...) LAPACK_zunmql_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zunmql(...) LAPACK_zunmql_base(__VA_ARGS__)
#endif

#define LAPACK_cunmqr_base LAPACK_GLOBAL(cunmqr,CUNMQR)
void LAPACK_cunmqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmqr(...) LAPACK_cunmqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cunmqr(...) LAPACK_cunmqr_base(__VA_ARGS__)
#endif

#define LAPACK_zunmqr_base LAPACK_GLOBAL(zunmqr,ZUNMQR)
void LAPACK_zunmqr_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmqr(...) LAPACK_zunmqr_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zunmqr(...) LAPACK_zunmqr_base(__VA_ARGS__)
#endif

#define LAPACK_cunmrq_base LAPACK_GLOBAL(cunmrq,CUNMRQ)
void LAPACK_cunmrq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmrq(...) LAPACK_cunmrq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cunmrq(...) LAPACK_cunmrq_base(__VA_ARGS__)
#endif

#define LAPACK_zunmrq_base LAPACK_GLOBAL(zunmrq,ZUNMRQ)
void LAPACK_zunmrq_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmrq(...) LAPACK_zunmrq_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zunmrq(...) LAPACK_zunmrq_base(__VA_ARGS__)
#endif

#define LAPACK_cunmrz_base LAPACK_GLOBAL(cunmrz,CUNMRZ)
void LAPACK_cunmrz_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmrz(...) LAPACK_cunmrz_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_cunmrz(...) LAPACK_cunmrz_base(__VA_ARGS__)
#endif

#define LAPACK_zunmrz_base LAPACK_GLOBAL(zunmrz,ZUNMRZ)
void LAPACK_zunmrz_base(
    char const* side, char const* trans,
    lapack_int const* m, lapack_int const* n, lapack_int const* k, lapack_int const* l,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmrz(...) LAPACK_zunmrz_base(__VA_ARGS__, 1, 1)
#else
    #define LAPACK_zunmrz(...) LAPACK_zunmrz_base(__VA_ARGS__)
#endif

#define LAPACK_cunmtr_base LAPACK_GLOBAL(cunmtr,CUNMTR)
void LAPACK_cunmtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* A, lapack_int const* lda,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cunmtr(...) LAPACK_cunmtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cunmtr(...) LAPACK_cunmtr_base(__VA_ARGS__)
#endif

#define LAPACK_zunmtr_base LAPACK_GLOBAL(zunmtr,ZUNMTR)
void LAPACK_zunmtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* A, lapack_int const* lda,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work, lapack_int const* lwork,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zunmtr(...) LAPACK_zunmtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zunmtr(...) LAPACK_zunmtr_base(__VA_ARGS__)
#endif

#define LAPACK_cupgtr_base LAPACK_GLOBAL(cupgtr,CUPGTR)
void LAPACK_cupgtr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_float const* AP,
    lapack_complex_float const* tau,
    lapack_complex_float* Q, lapack_int const* ldq,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cupgtr(...) LAPACK_cupgtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_cupgtr(...) LAPACK_cupgtr_base(__VA_ARGS__)
#endif

#define LAPACK_zupgtr_base LAPACK_GLOBAL(zupgtr,ZUPGTR)
void LAPACK_zupgtr_base(
    char const* uplo,
    lapack_int const* n,
    lapack_complex_double const* AP,
    lapack_complex_double const* tau,
    lapack_complex_double* Q, lapack_int const* ldq,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zupgtr(...) LAPACK_zupgtr_base(__VA_ARGS__, 1)
#else
    #define LAPACK_zupgtr(...) LAPACK_zupgtr_base(__VA_ARGS__)
#endif

#define LAPACK_cupmtr_base LAPACK_GLOBAL(cupmtr,CUPMTR)
void LAPACK_cupmtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_float const* AP,
    lapack_complex_float const* tau,
    lapack_complex_float* C, lapack_int const* ldc,
    lapack_complex_float* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_cupmtr(...) LAPACK_cupmtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_cupmtr(...) LAPACK_cupmtr_base(__VA_ARGS__)
#endif

#define LAPACK_zupmtr_base LAPACK_GLOBAL(zupmtr,ZUPMTR)
void LAPACK_zupmtr_base(
    char const* side, char const* uplo, char const* trans,
    lapack_int const* m, lapack_int const* n,
    lapack_complex_double const* AP,
    lapack_complex_double const* tau,
    lapack_complex_double* C, lapack_int const* ldc,
    lapack_complex_double* work,
    lapack_int* info
#ifdef LAPACK_FORTRAN_STRLEN_END
    , FORTRAN_STRLEN, FORTRAN_STRLEN, FORTRAN_STRLEN
#endif
);
#ifdef LAPACK_FORTRAN_STRLEN_END
    #define LAPACK_zupmtr(...) LAPACK_zupmtr_base(__VA_ARGS__, 1, 1, 1)
#else
    #define LAPACK_zupmtr(...) LAPACK_zupmtr_base(__VA_ARGS__)
#endif

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* LAPACK_H */
