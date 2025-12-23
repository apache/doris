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
#define z_div(c, a, b) {Cd(c)._Val[0] = (Cd(a)._Val[0]/Cd(b)._Val[0]); Cd(c)._Val[1]=(Cd(a)._Val[1]/Cd(b)._Val[1]);}
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
#define pow_dd(ap, bp) ( pow(*(ap), *(bp)))
#define pow_si(B,E) spow_ui(*(B),*(E))
#define pow_ri(B,E) spow_ui(*(B),*(E))
#define pow_di(B,E) dpow_ui(*(B),*(E))
#define pow_zi(p, a, b) {pCd(p) = zpow_ui(Cd(a), *(b));}
#define pow_ci(p, a, b) {pCf(p) = cpow_ui(Cf(a), *(b));}
#define pow_zz(R,A,B) {pCd(R) = cpow(Cd(A),*(B));}
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
static char junk[] = "\n@(#)LIBF77 VERSION 19990503\n";
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) dmaxloc_(w,*(s),*(e),n)

/* procedure parameter types for -A and -C++ */


#ifdef __cplusplus
typedef logical (*L_fp)(...);
#else
typedef logical (*L_fp)();
#endif

static float spow_ui(float x, integer n) {
	float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static double dpow_ui(double x, integer n) {
	double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#ifdef _MSC_VER
static _Fcomplex cpow_ui(complex x, integer n) {
	complex pow={1.0,0.0}; unsigned long int u;
		if(n != 0) {
		if(n < 0) n = -n, x.r = 1/x.r, x.i=1/x.i;
		for(u = n; ; ) {
			if(u & 01) pow.r *= x.r, pow.i *= x.i;
			if(u >>= 1) x.r *= x.r, x.i *= x.i;
			else break;
		}
	}
	_Fcomplex p={pow.r, pow.i};
	return p;
}
#else
static _Complex float cpow_ui(_Complex float x, integer n) {
	_Complex float pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
#ifdef _MSC_VER
static _Dcomplex zpow_ui(_Dcomplex x, integer n) {
	_Dcomplex pow={1.0,0.0}; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x._Val[0] = 1/x._Val[0], x._Val[1] =1/x._Val[1];
		for(u = n; ; ) {
			if(u & 01) pow._Val[0] *= x._Val[0], pow._Val[1] *= x._Val[1];
			if(u >>= 1) x._Val[0] *= x._Val[0], x._Val[1] *= x._Val[1];
			else break;
		}
	}
	_Dcomplex p = {pow._Val[0], pow._Val[1]};
	return p;
}
#else
static _Complex double zpow_ui(_Complex double x, integer n) {
	_Complex double pow=1.0; unsigned long int u;
	if(n != 0) {
		if(n < 0) n = -n, x = 1/x;
		for(u = n; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
#endif
static integer pow_ii(integer x, integer n) {
	integer pow; unsigned long int u;
	if (n <= 0) {
		if (n == 0 || x == 1) pow = 1;
		else if (x != -1) pow = x == 0 ? 1/x : 0;
		else n = -n;
	}
	if ((n > 0) || !(n == 0 || x == 1 || x != -1)) {
		u = n;
		for(pow = 1; ; ) {
			if(u & 01) pow *= x;
			if(u >>= 1) x *= x;
			else break;
		}
	}
	return pow;
}
static integer dmaxloc_(double *w, integer s, integer e, integer *n)
{
	double m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static integer smaxloc_(float *w, integer s, integer e, integer *n)
{
	float m; integer i, mi;
	for(m=w[s-1], mi=s, i=s+1; i<=e; i++)
		if (w[i-1]>m) mi=i ,m=w[i-1];
	return mi-s+1;
}
static inline void cdotc_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i]))._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i]))._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conjf(Cf(&x[i*incx]))._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += conjf(Cf(&x[i*incx]))._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i])) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conjf(Cf(&x[i*incx])) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotc_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i]))._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i]))._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += conj(Cd(&x[i*incx]))._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += conj(Cd(&x[i*incx]))._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i])) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += conj(Cd(&x[i*incx])) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif	
static inline void cdotu_(complex *z, integer *n_, complex *x, integer *incx_, complex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Fcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i])._Val[0] * Cf(&y[i])._Val[0];
			zdotc._Val[1] += Cf(&x[i])._Val[1] * Cf(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cf(&x[i*incx])._Val[0] * Cf(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cf(&x[i*incx])._Val[1] * Cf(&y[i*incy])._Val[1];
		}
	}
	pCf(z) = zdotc;
}
#else
	_Complex float zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i]) * Cf(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cf(&x[i*incx]) * Cf(&y[i*incy]);
		}
	}
	pCf(z) = zdotc;
}
#endif
static inline void zdotu_(doublecomplex *z, integer *n_, doublecomplex *x, integer *incx_, doublecomplex *y, integer *incy_) {
	integer n = *n_, incx = *incx_, incy = *incy_, i;
#ifdef _MSC_VER
	_Dcomplex zdotc = {0.0, 0.0};
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i])._Val[0] * Cd(&y[i])._Val[0];
			zdotc._Val[1] += Cd(&x[i])._Val[1] * Cd(&y[i])._Val[1];
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc._Val[0] += Cd(&x[i*incx])._Val[0] * Cd(&y[i*incy])._Val[0];
			zdotc._Val[1] += Cd(&x[i*incx])._Val[1] * Cd(&y[i*incy])._Val[1];
		}
	}
	pCd(z) = zdotc;
}
#else
	_Complex double zdotc = 0.0;
	if (incx == 1 && incy == 1) {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i]) * Cd(&y[i]);
		}
	} else {
		for (i=0;i<n;i++) { /* zdotc = zdotc + dconjg(x(i))* y(i) */
			zdotc += Cd(&x[i*incx]) * Cd(&y[i*incy]);
		}
	}
	pCd(z) = zdotc;
}
#endif
/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/





/* Table of constant values */

static doublecomplex c_b1 = {1.,0.};
static integer c__1 = 1;
static integer c_n1 = -1;
static doublereal c_b32 = -1.;
static doublereal c_b33 = 1.;

/* > \brief \b ZPSTRF computes the Cholesky factorization with complete pivoting of a complex Hermitian positi
ve semidefinite matrix. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZPSTRF + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zpstrf.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zpstrf.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zpstrf.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZPSTRF( UPLO, N, A, LDA, PIV, RANK, TOL, WORK, INFO ) */

/*       DOUBLE PRECISION   TOL */
/*       INTEGER            INFO, LDA, N, RANK */
/*       CHARACTER          UPLO */
/*       COMPLEX*16         A( LDA, * ) */
/*       DOUBLE PRECISION   WORK( 2*N ) */
/*       INTEGER            PIV( N ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZPSTRF computes the Cholesky factorization with complete */
/* > pivoting of a complex Hermitian positive semidefinite matrix A. */
/* > */
/* > The factorization has the form */
/* >    P**T * A * P = U**H * U ,  if UPLO = 'U', */
/* >    P**T * A * P = L  * L**H,  if UPLO = 'L', */
/* > where U is an upper triangular matrix and L is lower triangular, and */
/* > P is stored as vector PIV. */
/* > */
/* > This algorithm does not attempt to check that A is positive */
/* > semidefinite. This version of the algorithm calls level 3 BLAS. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          Specifies whether the upper or lower triangular part of the */
/* >          symmetric matrix A is stored. */
/* >          = 'U':  Upper triangular */
/* >          = 'L':  Lower triangular */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix A.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array, dimension (LDA,N) */
/* >          On entry, the symmetric matrix A.  If UPLO = 'U', the leading */
/* >          n by n upper triangular part of A contains the upper */
/* >          triangular part of the matrix A, and the strictly lower */
/* >          triangular part of A is not referenced.  If UPLO = 'L', the */
/* >          leading n by n lower triangular part of A contains the lower */
/* >          triangular part of the matrix A, and the strictly upper */
/* >          triangular part of A is not referenced. */
/* > */
/* >          On exit, if INFO = 0, the factor U or L from the Cholesky */
/* >          factorization as above. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] PIV */
/* > \verbatim */
/* >          PIV is INTEGER array, dimension (N) */
/* >          PIV is such that the nonzero entries are P( PIV(K), K ) = 1. */
/* > \endverbatim */
/* > */
/* > \param[out] RANK */
/* > \verbatim */
/* >          RANK is INTEGER */
/* >          The rank of A given by the number of steps the algorithm */
/* >          completed. */
/* > \endverbatim */
/* > */
/* > \param[in] TOL */
/* > \verbatim */
/* >          TOL is DOUBLE PRECISION */
/* >          User defined tolerance. If TOL < 0, then N*U*MAX( A(K,K) ) */
/* >          will be used. The algorithm terminates at the (K-1)st step */
/* >          if the pivot <= TOL. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is DOUBLE PRECISION array, dimension (2*N) */
/* >          Work space. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          < 0: If INFO = -K, the K-th argument had an illegal value, */
/* >          = 0: algorithm completed successfully, and */
/* >          > 0: the matrix A is either rank deficient with computed rank */
/* >               as returned in RANK, or is not positive semidefinite. See */
/* >               Section 7 of LAPACK Working Note #161 for further */
/* >               information. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16OTHERcomputational */

/*  ===================================================================== */
/* Subroutine */ void zpstrf_(char *uplo, integer *n, doublecomplex *a, 
	integer *lda, integer *piv, integer *rank, doublereal *tol, 
	doublereal *work, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5;
    doublereal d__1;
    doublecomplex z__1, z__2;

    /* Local variables */
    
    integer i__, j, k;
    extern logical lsame_(char *, char *);
    doublereal dtemp;
    integer itemp;
    extern /* Subroutine */ void zherk_(char *, char *, integer *, integer *, 
	    doublereal *, doublecomplex *, integer *, doublereal *, 
	    doublecomplex *, integer *), zgemv_(char *, 
	    integer *, integer *, doublecomplex *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *);
    doublereal dstop;
    logical upper;
    doublecomplex ztemp;
    extern /* Subroutine */ void zswap_(integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *);
    integer jb, nb;
    extern doublereal dlamch_(char *);
    extern /* Subroutine */ void zpstf2_(char *, integer *, doublecomplex *, 
	    integer *, integer *, integer *, doublereal *, doublereal *, 
	    integer *);
    extern logical disnan_(doublereal *);
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern /* Subroutine */ void zdscal_(integer *, doublereal *, 
	    doublecomplex *, integer *), zlacgv_(integer *, doublecomplex *, 
	    integer *);
    doublereal ajj;
    integer pvt;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    --work;
    --piv;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
    *info = 0;
    upper = lsame_(uplo, "U");
    if (! upper && ! lsame_(uplo, "L")) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -4;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZPSTRF", &i__1, (ftnlen)6);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Get block size */

    nb = ilaenv_(&c__1, "ZPOTRF", uplo, n, &c_n1, &c_n1, &c_n1, (ftnlen)6, (
	    ftnlen)1);
    if (nb <= 1 || nb >= *n) {

/*        Use unblocked code */

	zpstf2_(uplo, n, &a[a_dim1 + 1], lda, &piv[1], rank, tol, &work[1], 
		info);
	goto L230;

    } else {

/*     Initialize PIV */

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    piv[i__] = i__;
/* L100: */
	}

/*     Compute stopping value */

	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = i__ + i__ * a_dim1;
	    work[i__] = a[i__2].r;
/* L110: */
	}
	pvt = mymaxloc_(&work[1], &c__1, n, &c__1);
	i__1 = pvt + pvt * a_dim1;
	ajj = a[i__1].r;
	if (ajj <= 0. || disnan_(&ajj)) {
	    *rank = 0;
	    *info = 1;
	    goto L230;
	}

/*     Compute stopping value if not supplied */

	if (*tol < 0.) {
	    dstop = *n * dlamch_("Epsilon") * ajj;
	} else {
	    dstop = *tol;
	}


	if (upper) {

/*           Compute the Cholesky factorization P**T * A * P = U**H * U */

	    i__1 = *n;
	    i__2 = nb;
	    for (k = 1; i__2 < 0 ? k >= i__1 : k <= i__1; k += i__2) {

/*              Account for last block not being NB wide */

/* Computing MIN */
		i__3 = nb, i__4 = *n - k + 1;
		jb = f2cmin(i__3,i__4);

/*              Set relevant part of first half of WORK to zero, */
/*              holds dot products */

		i__3 = *n;
		for (i__ = k; i__ <= i__3; ++i__) {
		    work[i__] = 0.;
/* L120: */
		}

		i__3 = k + jb - 1;
		for (j = k; j <= i__3; ++j) {

/*              Find pivot, test for exit, else swap rows and columns */
/*              Update dot products, compute possible pivots which are */
/*              stored in the second half of WORK */

		    i__4 = *n;
		    for (i__ = j; i__ <= i__4; ++i__) {

			if (j > k) {
			    d_cnjg(&z__2, &a[j - 1 + i__ * a_dim1]);
			    i__5 = j - 1 + i__ * a_dim1;
			    z__1.r = z__2.r * a[i__5].r - z__2.i * a[i__5].i, 
				    z__1.i = z__2.r * a[i__5].i + z__2.i * a[
				    i__5].r;
			    work[i__] += z__1.r;
			}
			i__5 = i__ + i__ * a_dim1;
			work[*n + i__] = a[i__5].r - work[i__];

/* L130: */
		    }

		    if (j > 1) {
			i__4 = *n + j;
			i__5 = *n << 1;
			itemp = mymaxloc_(&work[1], &i__4, &i__5, &c__1);
			pvt = itemp + j - 1;
			ajj = work[*n + pvt];
			if (ajj <= dstop || disnan_(&ajj)) {
			    i__4 = j + j * a_dim1;
			    a[i__4].r = ajj, a[i__4].i = 0.;
			    goto L220;
			}
		    }

		    if (j != pvt) {

/*                    Pivot OK, so can now swap pivot rows and columns */

			i__4 = pvt + pvt * a_dim1;
			i__5 = j + j * a_dim1;
			a[i__4].r = a[i__5].r, a[i__4].i = a[i__5].i;
			i__4 = j - 1;
			zswap_(&i__4, &a[j * a_dim1 + 1], &c__1, &a[pvt * 
				a_dim1 + 1], &c__1);
			if (pvt < *n) {
			    i__4 = *n - pvt;
			    zswap_(&i__4, &a[j + (pvt + 1) * a_dim1], lda, &a[
				    pvt + (pvt + 1) * a_dim1], lda);
			}
			i__4 = pvt - 1;
			for (i__ = j + 1; i__ <= i__4; ++i__) {
			    d_cnjg(&z__1, &a[j + i__ * a_dim1]);
			    ztemp.r = z__1.r, ztemp.i = z__1.i;
			    i__5 = j + i__ * a_dim1;
			    d_cnjg(&z__1, &a[i__ + pvt * a_dim1]);
			    a[i__5].r = z__1.r, a[i__5].i = z__1.i;
			    i__5 = i__ + pvt * a_dim1;
			    a[i__5].r = ztemp.r, a[i__5].i = ztemp.i;
/* L140: */
			}
			i__4 = j + pvt * a_dim1;
			d_cnjg(&z__1, &a[j + pvt * a_dim1]);
			a[i__4].r = z__1.r, a[i__4].i = z__1.i;

/*                    Swap dot products and PIV */

			dtemp = work[j];
			work[j] = work[pvt];
			work[pvt] = dtemp;
			itemp = piv[pvt];
			piv[pvt] = piv[j];
			piv[j] = itemp;
		    }

		    ajj = sqrt(ajj);
		    i__4 = j + j * a_dim1;
		    a[i__4].r = ajj, a[i__4].i = 0.;

/*                 Compute elements J+1:N of row J. */

		    if (j < *n) {
			i__4 = j - 1;
			zlacgv_(&i__4, &a[j * a_dim1 + 1], &c__1);
			i__4 = j - k;
			i__5 = *n - j;
			z__1.r = -1., z__1.i = 0.;
			zgemv_("Trans", &i__4, &i__5, &z__1, &a[k + (j + 1) * 
				a_dim1], lda, &a[k + j * a_dim1], &c__1, &
				c_b1, &a[j + (j + 1) * a_dim1], lda);
			i__4 = j - 1;
			zlacgv_(&i__4, &a[j * a_dim1 + 1], &c__1);
			i__4 = *n - j;
			d__1 = 1. / ajj;
			zdscal_(&i__4, &d__1, &a[j + (j + 1) * a_dim1], lda);
		    }

/* L150: */
		}

/*              Update trailing matrix, J already incremented */

		if (k + jb <= *n) {
		    i__3 = *n - j + 1;
		    zherk_("Upper", "Conj Trans", &i__3, &jb, &c_b32, &a[k + 
			    j * a_dim1], lda, &c_b33, &a[j + j * a_dim1], lda);
		}

/* L160: */
	    }

	} else {

/*        Compute the Cholesky factorization P**T * A * P = L * L**H */

	    i__2 = *n;
	    i__1 = nb;
	    for (k = 1; i__1 < 0 ? k >= i__2 : k <= i__2; k += i__1) {

/*              Account for last block not being NB wide */

/* Computing MIN */
		i__3 = nb, i__4 = *n - k + 1;
		jb = f2cmin(i__3,i__4);

/*              Set relevant part of first half of WORK to zero, */
/*              holds dot products */

		i__3 = *n;
		for (i__ = k; i__ <= i__3; ++i__) {
		    work[i__] = 0.;
/* L170: */
		}

		i__3 = k + jb - 1;
		for (j = k; j <= i__3; ++j) {

/*              Find pivot, test for exit, else swap rows and columns */
/*              Update dot products, compute possible pivots which are */
/*              stored in the second half of WORK */

		    i__4 = *n;
		    for (i__ = j; i__ <= i__4; ++i__) {

			if (j > k) {
			    d_cnjg(&z__2, &a[i__ + (j - 1) * a_dim1]);
			    i__5 = i__ + (j - 1) * a_dim1;
			    z__1.r = z__2.r * a[i__5].r - z__2.i * a[i__5].i, 
				    z__1.i = z__2.r * a[i__5].i + z__2.i * a[
				    i__5].r;
			    work[i__] += z__1.r;
			}
			i__5 = i__ + i__ * a_dim1;
			work[*n + i__] = a[i__5].r - work[i__];

/* L180: */
		    }

		    if (j > 1) {
			i__4 = *n + j;
			i__5 = *n << 1;
			itemp = mymaxloc_(&work[1], &i__4, &i__5, &c__1);
			pvt = itemp + j - 1;
			ajj = work[*n + pvt];
			if (ajj <= dstop || disnan_(&ajj)) {
			    i__4 = j + j * a_dim1;
			    a[i__4].r = ajj, a[i__4].i = 0.;
			    goto L220;
			}
		    }

		    if (j != pvt) {

/*                    Pivot OK, so can now swap pivot rows and columns */

			i__4 = pvt + pvt * a_dim1;
			i__5 = j + j * a_dim1;
			a[i__4].r = a[i__5].r, a[i__4].i = a[i__5].i;
			i__4 = j - 1;
			zswap_(&i__4, &a[j + a_dim1], lda, &a[pvt + a_dim1], 
				lda);
			if (pvt < *n) {
			    i__4 = *n - pvt;
			    zswap_(&i__4, &a[pvt + 1 + j * a_dim1], &c__1, &a[
				    pvt + 1 + pvt * a_dim1], &c__1);
			}
			i__4 = pvt - 1;
			for (i__ = j + 1; i__ <= i__4; ++i__) {
			    d_cnjg(&z__1, &a[i__ + j * a_dim1]);
			    ztemp.r = z__1.r, ztemp.i = z__1.i;
			    i__5 = i__ + j * a_dim1;
			    d_cnjg(&z__1, &a[pvt + i__ * a_dim1]);
			    a[i__5].r = z__1.r, a[i__5].i = z__1.i;
			    i__5 = pvt + i__ * a_dim1;
			    a[i__5].r = ztemp.r, a[i__5].i = ztemp.i;
/* L190: */
			}
			i__4 = pvt + j * a_dim1;
			d_cnjg(&z__1, &a[pvt + j * a_dim1]);
			a[i__4].r = z__1.r, a[i__4].i = z__1.i;


/*                    Swap dot products and PIV */

			dtemp = work[j];
			work[j] = work[pvt];
			work[pvt] = dtemp;
			itemp = piv[pvt];
			piv[pvt] = piv[j];
			piv[j] = itemp;
		    }

		    ajj = sqrt(ajj);
		    i__4 = j + j * a_dim1;
		    a[i__4].r = ajj, a[i__4].i = 0.;

/*                 Compute elements J+1:N of column J. */

		    if (j < *n) {
			i__4 = j - 1;
			zlacgv_(&i__4, &a[j + a_dim1], lda);
			i__4 = *n - j;
			i__5 = j - k;
			z__1.r = -1., z__1.i = 0.;
			zgemv_("No Trans", &i__4, &i__5, &z__1, &a[j + 1 + k *
				 a_dim1], lda, &a[j + k * a_dim1], lda, &c_b1,
				 &a[j + 1 + j * a_dim1], &c__1);
			i__4 = j - 1;
			zlacgv_(&i__4, &a[j + a_dim1], lda);
			i__4 = *n - j;
			d__1 = 1. / ajj;
			zdscal_(&i__4, &d__1, &a[j + 1 + j * a_dim1], &c__1);
		    }

/* L200: */
		}

/*              Update trailing matrix, J already incremented */

		if (k + jb <= *n) {
		    i__3 = *n - j + 1;
		    zherk_("Lower", "No Trans", &i__3, &jb, &c_b32, &a[j + k *
			     a_dim1], lda, &c_b33, &a[j + j * a_dim1], lda);
		}

/* L210: */
	    }

	}
    }

/*     Ran to completion, A has full rank */

    *rank = *n;

    goto L230;
L220:

/*     Rank is the number of steps completed.  Set INFO = 1 to signal */
/*     that the factorization cannot be used to solve a system. */

    *rank = j - 1;
    *info = 1;

L230:
    return;

/*     End of ZPSTRF */

} /* zpstrf_ */

