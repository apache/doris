#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <complex.h>
#ifdef complex
#undef complex
#endif
#ifdef I
#undef I
#endif

#if defined(_WIN64)
typedef long long BLASLONG;
typedef unsigned long long BLASULONG;
#else
typedef long BLASLONG;
typedef unsigned long BLASULONG;
#endif

#ifdef LAPACK_ILP64
typedef BLASLONG blasint;
#if defined(_WIN64)
#define blasabs(x) llabs(x)
#else
#define blasabs(x) labs(x)
#endif
#else
typedef int blasint;
#define blasabs(x) abs(x)
#endif

typedef blasint integer;

typedef unsigned int uinteger;
typedef char *address;
typedef short int shortint;
typedef float real;
typedef double doublereal;
typedef struct { real r, i; } complex;
typedef struct { doublereal r, i; } doublecomplex;
#ifdef _MSC_VER
static inline _Fcomplex Cf(complex *z) {_Fcomplex zz={z->r , z->i}; return zz;}
static inline _Dcomplex Cd(doublecomplex *z) {_Dcomplex zz={z->r , z->i};return zz;}
static inline _Fcomplex * _pCf(complex *z) {return (_Fcomplex*)z;}
static inline _Dcomplex * _pCd(doublecomplex *z) {return (_Dcomplex*)z;}
#else
static inline _Complex float Cf(complex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex double Cd(doublecomplex *z) {return z->r + z->i*_Complex_I;}
static inline _Complex float * _pCf(complex *z) {return (_Complex float*)z;}
static inline _Complex double * _pCd(doublecomplex *z) {return (_Complex double*)z;}
#endif
#define pCf(z) (*_pCf(z))
#define pCd(z) (*_pCd(z))
typedef blasint logical;

typedef char logical1;
typedef char integer1;

#define TRUE_ (1)
#define FALSE_ (0)

/* Extern is for use with -E */
#ifndef Extern
#define Extern extern
#endif

/* I/O stuff */

typedef int flag;
typedef int ftnlen;
typedef int ftnint;

/*external read, write*/
typedef struct
{	flag cierr;
	ftnint ciunit;
	flag ciend;
	char *cifmt;
	ftnint cirec;
} cilist;

/*internal read, write*/
typedef struct
{	flag icierr;
	char *iciunit;
	flag iciend;
	char *icifmt;
	ftnint icirlen;
	ftnint icirnum;
} icilist;

/*open*/
typedef struct
{	flag oerr;
	ftnint ounit;
	char *ofnm;
	ftnlen ofnmlen;
	char *osta;
	char *oacc;
	char *ofm;
	ftnint orl;
	char *oblnk;
} olist;

/*close*/
typedef struct
{	flag cerr;
	ftnint cunit;
	char *csta;
} cllist;

/*rewind, backspace, endfile*/
typedef struct
{	flag aerr;
	ftnint aunit;
} alist;

/* inquire */
typedef struct
{	flag inerr;
	ftnint inunit;
	char *infile;
	ftnlen infilen;
	ftnint	*inex;	/*parameters in standard's order*/
	ftnint	*inopen;
	ftnint	*innum;
	ftnint	*innamed;
	char	*inname;
	ftnlen	innamlen;
	char	*inacc;
	ftnlen	inacclen;
	char	*inseq;
	ftnlen	inseqlen;
	char 	*indir;
	ftnlen	indirlen;
	char	*infmt;
	ftnlen	infmtlen;
	char	*inform;
	ftnint	informlen;
	char	*inunf;
	ftnlen	inunflen;
	ftnint	*inrecl;
	ftnint	*innrec;
	char	*inblank;
	ftnlen	inblanklen;
} inlist;

#define VOID void

union Multitype {	/* for multiple entry points */
	integer1 g;
	shortint h;
	integer i;
	/* longint j; */
	real r;
	doublereal d;
	complex c;
	doublecomplex z;
	};

typedef union Multitype Multitype;

struct Vardesc {	/* for Namelist */
	char *name;
	char *addr;
	ftnlen *dims;
	int  type;
	};
typedef struct Vardesc Vardesc;

struct Namelist {
	char *name;
	Vardesc **vars;
	int nvars;
	};
typedef struct Namelist Namelist;

#define abs(x) ((x) >= 0 ? (x) : -(x))
#define dabs(x) (fabs(x))
#define f2cmin(a,b) ((a) <= (b) ? (a) : (b))
#define f2cmax(a,b) ((a) >= (b) ? (a) : (b))
#define dmin(a,b) (f2cmin(a,b))
#define dmax(a,b) (f2cmax(a,b))
#define bit_test(a,b)	((a) >> (b) & 1)
#define bit_clear(a,b)	((a) & ~((uinteger)1 << (b)))
#define bit_set(a,b)	((a) |  ((uinteger)1 << (b)))

#define abort_() { sig_die("Fortran abort routine called", 1); }
#define c_abs(z) (cabsf(Cf(z)))
#define c_cos(R,Z) { pCf(R)=ccos(Cf(Z)); }
#ifdef _MSC_VER
#define c_div(c, a, b) {Cf(c)._Val[0] = (Cf(a)._Val[0]/Cf(b)._Val[0]); Cf(c)._Val[1]=(Cf(a)._Val[1]/Cf(b)._Val[1]);}
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/df(b)._Val[1]);}
#else
#define c_div(c, a, b) {pCf(c) = Cf(a)/Cf(b);}
#define z_div(c, a, b) {pCd(c) = Cd(a)/Cd(b);}
#endif
#define c_exp(R, Z) {pCf(R) = cexpf(Cf(Z));}
#define c_log(R, Z) {pCf(R) = clogf(Cf(Z));}
#define c_sin(R, Z) {pCf(R) = csinf(Cf(Z));}
//#define c_sqrt(R, Z) {*(R) = csqrtf(Cf(Z));}
#define c_sqrt(R, Z) {pCf(R) = csqrtf(Cf(Z));}
#define d_abs(x) (fabs(*(x)))
#define d_acos(x) (acos(*(x)))
#define d_asin(x) (asin(*(x)))
#define d_atan(x) (atan(*(x)))
#define d_atn2(x, y) (atan2(*(x),*(y)))
#define d_cnjg(R, Z) { pCd(R) = conj(Cd(Z)); }
#define r_cnjg(R, Z) { pCf(R) = conjf(Cf(Z)); }
#define d_cos(x) (cos(*(x)))
#define d_cosh(x) (cosh(*(x)))
#define d_dim(__a, __b) ( *(__a) > *(__b) ? *(__a) - *(__b) : 0.0 )
#define d_exp(x) (exp(*(x)))
#define d_imag(z) (cimag(Cd(z)))
#define r_imag(z) (cimagf(Cf(z)))
#define d_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define r_int(__x) (*(__x)>0 ? floor(*(__x)) : -floor(- *(__x)))
#define d_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define r_lg10(x) ( 0.43429448190325182765 * log(*(x)) )
#define d_log(x) (log(*(x)))
#define d_mod(x, y) (fmod(*(x), *(y)))
#define u_nint(__x) ((__x)>=0 ? floor((__x) + .5) : -floor(.5 - (__x)))
#define d_nint(x) u_nint(*(x))
#define u_sign(__a,__b) ((__b) >= 0 ? ((__a) >= 0 ? (__a) : -(__a)) : -((__a) >= 0 ? (__a) : -(__a)))
#define d_sign(a,b) u_sign(*(a),*(b))
#define r_sign(a,b) u_sign(*(a),*(b))
#define d_sin(x) (sin(*(x)))
#define d_sinh(x) (sinh(*(x)))
#define d_sqrt(x) (sqrt(*(x)))
#define d_tan(x) (tan(*(x)))
#define d_tanh(x) (tanh(*(x)))
#define i_abs(x) abs(*(x))
#define i_dnnt(x) ((integer)u_nint(*(x)))
#define i_len(s, n) (n)
#define i_nint(x) ((integer)u_nint(*(x)))
#define i_sign(a,b) ((integer)u_sign((integer)*(a),(integer)*(b)))
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* > \brief <b> SGTSV computes the solution to system of linear equations A * X = B for GT matrices </b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SGTSV + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgtsv.f
"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgtsv.f
"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgtsv.f
"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SGTSV( N, NRHS, DL, D, DU, B, LDB, INFO ) */

/*       INTEGER            INFO, LDB, N, NRHS */
/*       REAL               B( LDB, * ), D( * ), DL( * ), DU( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SGTSV  solves the equation */
/* > */
/* >    A*X = B, */
/* > */
/* > where A is an n by n tridiagonal matrix, by Gaussian elimination with */
/* > partial pivoting. */
/* > */
/* > Note that the equation  A**T*X = B  may be solved by interchanging the */
/* > order of the arguments DU and DL. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >          The number of right hand sides, i.e., the number of columns */
/* >          of the matrix B.  NRHS >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] DL */
/* > \verbatim */
/* >          DL is REAL array, dimension (N-1) */
/* >          On entry, DL must contain the (n-1) sub-diagonal elements of */
/* >          A. */
/* > */
/* >          On exit, DL is overwritten by the (n-2) elements of the */
/* >          second super-diagonal of the upper triangular matrix U from */
/* >          the LU factorization of A, in DL(1), ..., DL(n-2). */
/* > \endverbatim */
/* > */
/* > \param[in,out] D */
/* > \verbatim */
/* >          D is REAL array, dimension (N) */
/* >          On entry, D must contain the diagonal elements of A. */
/* > */
/* >          On exit, D is overwritten by the n diagonal elements of U. */
/* > \endverbatim */
/* > */
/* > \param[in,out] DU */
/* > \verbatim */
/* >          DU is REAL array, dimension (N-1) */
/* >          On entry, DU must contain the (n-1) super-diagonal elements */
/* >          of A. */
/* > */
/* >          On exit, DU is overwritten by the (n-1) elements of the first */
/* >          super-diagonal of U. */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB,NRHS) */
/* >          On entry, the N by NRHS matrix of right hand side matrix B. */
/* >          On exit, if INFO = 0, the N by NRHS solution matrix X. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* >          > 0: if INFO = i, U(i,i) is exactly zero, and the solution */
/* >               has not been computed.  The factorization has not been */
/* >               completed unless i = N. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realGTsolve */

/*  ===================================================================== */
/* Subroutine */ void sgtsv_(integer *n, integer *nrhs, real *dl, real *d__, 
	real *du, real *b, integer *ldb, integer *info)
{
    /* System generated locals */
    integer b_dim1, b_offset, i__1, i__2;
    real r__1, r__2;

    /* Local variables */
    real fact, temp;
    integer i__, j;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    /* Parameter adjustments */
    --dl;
    --d__;
    --du;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;

    /* Function Body */
    *info = 0;
    if (*n < 0) {
	*info = -1;
    } else if (*nrhs < 0) {
	*info = -2;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -7;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SGTSV ", &i__1, (ftnlen)5);
	return;
    }

    if (*n == 0) {
	return;
    }

    if (*nrhs == 1) {
	i__1 = *n - 2;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    if ((r__1 = d__[i__], abs(r__1)) >= (r__2 = dl[i__], abs(r__2))) {

/*              No row interchange required */

		if (d__[i__] != 0.f) {
		    fact = dl[i__] / d__[i__];
		    d__[i__ + 1] -= fact * du[i__];
		    b[i__ + 1 + b_dim1] -= fact * b[i__ + b_dim1];
		} else {
		    *info = i__;
		    return;
		}
		dl[i__] = 0.f;
	    } else {

/*              Interchange rows I and I+1 */

		fact = d__[i__] / dl[i__];
		d__[i__] = dl[i__];
		temp = d__[i__ + 1];
		d__[i__ + 1] = du[i__] - fact * temp;
		dl[i__] = du[i__ + 1];
		du[i__ + 1] = -fact * dl[i__];
		du[i__] = temp;
		temp = b[i__ + b_dim1];
		b[i__ + b_dim1] = b[i__ + 1 + b_dim1];
		b[i__ + 1 + b_dim1] = temp - fact * b[i__ + 1 + b_dim1];
	    }
/* L10: */
	}
	if (*n > 1) {
	    i__ = *n - 1;
	    if ((r__1 = d__[i__], abs(r__1)) >= (r__2 = dl[i__], abs(r__2))) {
		if (d__[i__] != 0.f) {
		    fact = dl[i__] / d__[i__];
		    d__[i__ + 1] -= fact * du[i__];
		    b[i__ + 1 + b_dim1] -= fact * b[i__ + b_dim1];
		} else {
		    *info = i__;
		    return;
		}
	    } else {
		fact = d__[i__] / dl[i__];
		d__[i__] = dl[i__];
		temp = d__[i__ + 1];
		d__[i__ + 1] = du[i__] - fact * temp;
		du[i__] = temp;
		temp = b[i__ + b_dim1];
		b[i__ + b_dim1] = b[i__ + 1 + b_dim1];
		b[i__ + 1 + b_dim1] = temp - fact * b[i__ + 1 + b_dim1];
	    }
	}
	if (d__[*n] == 0.f) {
	    *info = *n;
	    return;
	}
    } else {
	i__1 = *n - 2;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    if ((r__1 = d__[i__], abs(r__1)) >= (r__2 = dl[i__], abs(r__2))) {

/*              No row interchange required */

		if (d__[i__] != 0.f) {
		    fact = dl[i__] / d__[i__];
		    d__[i__ + 1] -= fact * du[i__];
		    i__2 = *nrhs;
		    for (j = 1; j <= i__2; ++j) {
			b[i__ + 1 + j * b_dim1] -= fact * b[i__ + j * b_dim1];
/* L20: */
		    }
		} else {
		    *info = i__;
		    return;
		}
		dl[i__] = 0.f;
	    } else {

/*              Interchange rows I and I+1 */

		fact = d__[i__] / dl[i__];
		d__[i__] = dl[i__];
		temp = d__[i__ + 1];
		d__[i__ + 1] = du[i__] - fact * temp;
		dl[i__] = du[i__ + 1];
		du[i__ + 1] = -fact * dl[i__];
		du[i__] = temp;
		i__2 = *nrhs;
		for (j = 1; j <= i__2; ++j) {
		    temp = b[i__ + j * b_dim1];
		    b[i__ + j * b_dim1] = b[i__ + 1 + j * b_dim1];
		    b[i__ + 1 + j * b_dim1] = temp - fact * b[i__ + 1 + j * 
			    b_dim1];
/* L30: */
		}
	    }
/* L40: */
	}
	if (*n > 1) {
	    i__ = *n - 1;
	    if ((r__1 = d__[i__], abs(r__1)) >= (r__2 = dl[i__], abs(r__2))) {
		if (d__[i__] != 0.f) {
		    fact = dl[i__] / d__[i__];
		    d__[i__ + 1] -= fact * du[i__];
		    i__1 = *nrhs;
		    for (j = 1; j <= i__1; ++j) {
			b[i__ + 1 + j * b_dim1] -= fact * b[i__ + j * b_dim1];
/* L50: */
		    }
		} else {
		    *info = i__;
		    return;
		}
	    } else {
		fact = d__[i__] / dl[i__];
		d__[i__] = dl[i__];
		temp = d__[i__ + 1];
		d__[i__ + 1] = du[i__] - fact * temp;
		du[i__] = temp;
		i__1 = *nrhs;
		for (j = 1; j <= i__1; ++j) {
		    temp = b[i__ + j * b_dim1];
		    b[i__ + j * b_dim1] = b[i__ + 1 + j * b_dim1];
		    b[i__ + 1 + j * b_dim1] = temp - fact * b[i__ + 1 + j * 
			    b_dim1];
/* L60: */
		}
	    }
	}
	if (d__[*n] == 0.f) {
	    *info = *n;
	    return;
	}
    }

/*     Back solve with the matrix U from the factorization. */

    if (*nrhs <= 2) {
	j = 1;
L70:
	b[*n + j * b_dim1] /= d__[*n];
	if (*n > 1) {
	    b[*n - 1 + j * b_dim1] = (b[*n - 1 + j * b_dim1] - du[*n - 1] * b[
		    *n + j * b_dim1]) / d__[*n - 1];
	}
	for (i__ = *n - 2; i__ >= 1; --i__) {
	    b[i__ + j * b_dim1] = (b[i__ + j * b_dim1] - du[i__] * b[i__ + 1 
		    + j * b_dim1] - dl[i__] * b[i__ + 2 + j * b_dim1]) / d__[
		    i__];
/* L80: */
	}
	if (j < *nrhs) {
	    ++j;
	    goto L70;
	}
    } else {
	i__1 = *nrhs;
	for (j = 1; j <= i__1; ++j) {
	    b[*n + j * b_dim1] /= d__[*n];
	    if (*n > 1) {
		b[*n - 1 + j * b_dim1] = (b[*n - 1 + j * b_dim1] - du[*n - 1] 
			* b[*n + j * b_dim1]) / d__[*n - 1];
	    }
	    for (i__ = *n - 2; i__ >= 1; --i__) {
		b[i__ + j * b_dim1] = (b[i__ + j * b_dim1] - du[i__] * b[i__ 
			+ 1 + j * b_dim1] - dl[i__] * b[i__ + 2 + j * b_dim1])
			 / d__[i__];
/* L90: */
	    }
/* L100: */
	}
    }

    return;

/*     End of SGTSV */

} /* sgtsv_ */

