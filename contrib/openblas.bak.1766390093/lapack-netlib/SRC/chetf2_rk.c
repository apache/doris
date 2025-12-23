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
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

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

static integer c__1 = 1;

/* > \brief \b CHETF2_RK computes the factorization of a complex Hermitian indefinite matrix using the bounded
 Bunch-Kaufman (rook) diagonal pivoting method (BLAS2 unblocked algorithm). */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CHETF2_RK + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chetf2_
rk.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chetf2_
rk.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chetf2_
rk.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CHETF2_RK( UPLO, N, A, LDA, E, IPIV, INFO ) */

/*       CHARACTER          UPLO */
/*       INTEGER            INFO, LDA, N */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX            A( LDA, * ), E ( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > CHETF2_RK computes the factorization of a complex Hermitian matrix A */
/* > using the bounded Bunch-Kaufman (rook) diagonal pivoting method: */
/* > */
/* >    A = P*U*D*(U**H)*(P**T) or A = P*L*D*(L**H)*(P**T), */
/* > */
/* > where U (or L) is unit upper (or lower) triangular matrix, */
/* > U**H (or L**H) is the conjugate of U (or L), P is a permutation */
/* > matrix, P**T is the transpose of P, and D is Hermitian and block */
/* > diagonal with 1-by-1 and 2-by-2 diagonal blocks. */
/* > */
/* > This is the unblocked version of the algorithm, calling Level 2 BLAS. */
/* > For more information see Further Details section. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          Specifies whether the upper or lower triangular part of the */
/* >          Hermitian matrix A is stored: */
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
/* >          A is COMPLEX array, dimension (LDA,N) */
/* >          On entry, the Hermitian matrix A. */
/* >            If UPLO = 'U': the leading N-by-N upper triangular part */
/* >            of A contains the upper triangular part of the matrix A, */
/* >            and the strictly lower triangular part of A is not */
/* >            referenced. */
/* > */
/* >            If UPLO = 'L': the leading N-by-N lower triangular part */
/* >            of A contains the lower triangular part of the matrix A, */
/* >            and the strictly upper triangular part of A is not */
/* >            referenced. */
/* > */
/* >          On exit, contains: */
/* >            a) ONLY diagonal elements of the Hermitian block diagonal */
/* >               matrix D on the diagonal of A, i.e. D(k,k) = A(k,k); */
/* >               (superdiagonal (or subdiagonal) elements of D */
/* >                are stored on exit in array E), and */
/* >            b) If UPLO = 'U': factor U in the superdiagonal part of A. */
/* >               If UPLO = 'L': factor L in the subdiagonal part of A. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] E */
/* > \verbatim */
/* >          E is COMPLEX array, dimension (N) */
/* >          On exit, contains the superdiagonal (or subdiagonal) */
/* >          elements of the Hermitian block diagonal matrix D */
/* >          with 1-by-1 or 2-by-2 diagonal blocks, where */
/* >          If UPLO = 'U': E(i) = D(i-1,i), i=2:N, E(1) is set to 0; */
/* >          If UPLO = 'L': E(i) = D(i+1,i), i=1:N-1, E(N) is set to 0. */
/* > */
/* >          NOTE: For 1-by-1 diagonal block D(k), where */
/* >          1 <= k <= N, the element E(k) is set to 0 in both */
/* >          UPLO = 'U' or UPLO = 'L' cases. */
/* > \endverbatim */
/* > */
/* > \param[out] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >          IPIV describes the permutation matrix P in the factorization */
/* >          of matrix A as follows. The absolute value of IPIV(k) */
/* >          represents the index of row and column that were */
/* >          interchanged with the k-th row and column. The value of UPLO */
/* >          describes the order in which the interchanges were applied. */
/* >          Also, the sign of IPIV represents the block structure of */
/* >          the Hermitian block diagonal matrix D with 1-by-1 or 2-by-2 */
/* >          diagonal blocks which correspond to 1 or 2 interchanges */
/* >          at each factorization step. For more info see Further */
/* >          Details section. */
/* > */
/* >          If UPLO = 'U', */
/* >          ( in factorization order, k decreases from N to 1 ): */
/* >            a) A single positive entry IPIV(k) > 0 means: */
/* >               D(k,k) is a 1-by-1 diagonal block. */
/* >               If IPIV(k) != k, rows and columns k and IPIV(k) were */
/* >               interchanged in the matrix A(1:N,1:N); */
/* >               If IPIV(k) = k, no interchange occurred. */
/* > */
/* >            b) A pair of consecutive negative entries */
/* >               IPIV(k) < 0 and IPIV(k-1) < 0 means: */
/* >               D(k-1:k,k-1:k) is a 2-by-2 diagonal block. */
/* >               (NOTE: negative entries in IPIV appear ONLY in pairs). */
/* >               1) If -IPIV(k) != k, rows and columns */
/* >                  k and -IPIV(k) were interchanged */
/* >                  in the matrix A(1:N,1:N). */
/* >                  If -IPIV(k) = k, no interchange occurred. */
/* >               2) If -IPIV(k-1) != k-1, rows and columns */
/* >                  k-1 and -IPIV(k-1) were interchanged */
/* >                  in the matrix A(1:N,1:N). */
/* >                  If -IPIV(k-1) = k-1, no interchange occurred. */
/* > */
/* >            c) In both cases a) and b), always ABS( IPIV(k) ) <= k. */
/* > */
/* >            d) NOTE: Any entry IPIV(k) is always NONZERO on output. */
/* > */
/* >          If UPLO = 'L', */
/* >          ( in factorization order, k increases from 1 to N ): */
/* >            a) A single positive entry IPIV(k) > 0 means: */
/* >               D(k,k) is a 1-by-1 diagonal block. */
/* >               If IPIV(k) != k, rows and columns k and IPIV(k) were */
/* >               interchanged in the matrix A(1:N,1:N). */
/* >               If IPIV(k) = k, no interchange occurred. */
/* > */
/* >            b) A pair of consecutive negative entries */
/* >               IPIV(k) < 0 and IPIV(k+1) < 0 means: */
/* >               D(k:k+1,k:k+1) is a 2-by-2 diagonal block. */
/* >               (NOTE: negative entries in IPIV appear ONLY in pairs). */
/* >               1) If -IPIV(k) != k, rows and columns */
/* >                  k and -IPIV(k) were interchanged */
/* >                  in the matrix A(1:N,1:N). */
/* >                  If -IPIV(k) = k, no interchange occurred. */
/* >               2) If -IPIV(k+1) != k+1, rows and columns */
/* >                  k-1 and -IPIV(k-1) were interchanged */
/* >                  in the matrix A(1:N,1:N). */
/* >                  If -IPIV(k+1) = k+1, no interchange occurred. */
/* > */
/* >            c) In both cases a) and b), always ABS( IPIV(k) ) >= k. */
/* > */
/* >            d) NOTE: Any entry IPIV(k) is always NONZERO on output. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* > */
/* >          < 0: If INFO = -k, the k-th argument had an illegal value */
/* > */
/* >          > 0: If INFO = k, the matrix A is singular, because: */
/* >                 If UPLO = 'U': column k in the upper */
/* >                 triangular part of A contains all zeros. */
/* >                 If UPLO = 'L': column k in the lower */
/* >                 triangular part of A contains all zeros. */
/* > */
/* >               Therefore D(k,k) is exactly zero, and superdiagonal */
/* >               elements of column k of U (or subdiagonal elements of */
/* >               column k of L ) are all zeros. The factorization has */
/* >               been completed, but the block diagonal matrix D is */
/* >               exactly singular, and division by zero will occur if */
/* >               it is used to solve a system of equations. */
/* > */
/* >               NOTE: INFO only stores the first occurrence of */
/* >               a singularity, any subsequent occurrence of singularity */
/* >               is not stored in INFO even though the factorization */
/* >               always completes. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complexHEcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > TODO: put further details */
/* > \endverbatim */

/* > \par Contributors: */
/*  ================== */
/* > */
/* > \verbatim */
/* > */
/* >  December 2016,  Igor Kozachenko, */
/* >                  Computer Science Division, */
/* >                  University of California, Berkeley */
/* > */
/* >  September 2007, Sven Hammarling, Nicholas J. Higham, Craig Lucas, */
/* >                  School of Mathematics, */
/* >                  University of Manchester */
/* > */
/* >  01-01-96 - Based on modifications by */
/* >    J. Lewis, Boeing Computer Services Company */
/* >    A. Petitet, Computer Science Dept., */
/* >                Univ. of Tenn., Knoxville abd , USA */
/* > \endverbatim */

/*  ===================================================================== */
/* Subroutine */ void chetf2_rk_(char *uplo, integer *n, complex *a, integer *
	lda, complex *e, integer *ipiv, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5, i__6;
    real r__1, r__2;
    complex q__1, q__2, q__3, q__4, q__5, q__6, q__7, q__8;

    /* Local variables */
    extern /* Subroutine */ void cher_(char *, integer *, real *, complex *, 
	    integer *, complex *, integer *);
    logical done;
    integer imax, jmax;
    real d__;
    integer i__, j, k, p;
    complex t;
    real alpha;
    extern logical lsame_(char *, char *);
    real sfmin;
    extern /* Subroutine */ void cswap_(integer *, complex *, integer *, 
	    complex *, integer *);
    integer itemp, kstep;
    real stemp;
    logical upper;
    real r1, d11;
    complex d12;
    real d22;
    complex d21;
    extern real slapy2_(real *, real *);
    integer ii, kk, kp;
    real absakk;
    complex wk;
    extern integer icamax_(integer *, complex *, integer *);
    extern real slamch_(char *);
    real tt;
    extern /* Subroutine */ void csscal_(integer *, real *, complex *, integer 
	    *);
    extern int xerbla_(char *, integer *, ftnlen);
    real colmax, rowmax;
    complex wkm1, wkp1;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ====================================================================== */



/*     Test the input parameters. */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --e;
    --ipiv;

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
	xerbla_("CHETF2_RK", &i__1, (ftnlen)9);
	return;
    }

/*     Initialize ALPHA for use in choosing pivot block size. */

    alpha = (sqrt(17.f) + 1.f) / 8.f;

/*     Compute machine safe minimum */

    sfmin = slamch_("S");

    if (upper) {

/*        Factorize A as U*D*U**H using the upper triangle of A */

/*        Initialize the first entry of array E, where superdiagonal */
/*        elements of D are stored */

	e[1].r = 0.f, e[1].i = 0.f;

/*        K is the main loop index, decreasing from N to 1 in steps of */
/*        1 or 2 */

	k = *n;
L10:

/*        If K < 1, exit from loop */

	if (k < 1) {
	    goto L34;
	}
	kstep = 1;
	p = k;

/*        Determine rows and columns to be interchanged and whether */
/*        a 1-by-1 or 2-by-2 pivot block will be used */

	i__1 = k + k * a_dim1;
	absakk = (r__1 = a[i__1].r, abs(r__1));

/*        IMAX is the row-index of the largest off-diagonal element in */
/*        column K, and COLMAX is its absolute value. */
/*        Determine both COLMAX and IMAX. */

	if (k > 1) {
	    i__1 = k - 1;
	    imax = icamax_(&i__1, &a[k * a_dim1 + 1], &c__1);
	    i__1 = imax + k * a_dim1;
	    colmax = (r__1 = a[i__1].r, abs(r__1)) + (r__2 = r_imag(&a[imax + 
		    k * a_dim1]), abs(r__2));
	} else {
	    colmax = 0.f;
	}

	if (f2cmax(absakk,colmax) == 0.f) {

/*           Column K is zero or underflow: set INFO and continue */

	    if (*info == 0) {
		*info = k;
	    }
	    kp = k;
	    i__1 = k + k * a_dim1;
	    i__2 = k + k * a_dim1;
	    r__1 = a[i__2].r;
	    a[i__1].r = r__1, a[i__1].i = 0.f;

/*           Set E( K ) to zero */

	    if (k > 1) {
		i__1 = k;
		e[i__1].r = 0.f, e[i__1].i = 0.f;
	    }

	} else {

/*           ============================================================ */

/*           BEGIN pivot search */

/*           Case(1) */
/*           Equivalent to testing for ABSAKK.GE.ALPHA*COLMAX */
/*           (used to handle NaN and Inf) */

	    if (! (absakk < alpha * colmax)) {

/*              no interchange, use 1-by-1 pivot block */

		kp = k;

	    } else {

		done = FALSE_;

/*              Loop until pivot found */

L12:

/*                 BEGIN pivot search loop body */


/*                 JMAX is the column-index of the largest off-diagonal */
/*                 element in row IMAX, and ROWMAX is its absolute value. */
/*                 Determine both ROWMAX and JMAX. */

		if (imax != k) {
		    i__1 = k - imax;
		    jmax = imax + icamax_(&i__1, &a[imax + (imax + 1) * 
			    a_dim1], lda);
		    i__1 = imax + jmax * a_dim1;
		    rowmax = (r__1 = a[i__1].r, abs(r__1)) + (r__2 = r_imag(&
			    a[imax + jmax * a_dim1]), abs(r__2));
		} else {
		    rowmax = 0.f;
		}

		if (imax > 1) {
		    i__1 = imax - 1;
		    itemp = icamax_(&i__1, &a[imax * a_dim1 + 1], &c__1);
		    i__1 = itemp + imax * a_dim1;
		    stemp = (r__1 = a[i__1].r, abs(r__1)) + (r__2 = r_imag(&a[
			    itemp + imax * a_dim1]), abs(r__2));
		    if (stemp > rowmax) {
			rowmax = stemp;
			jmax = itemp;
		    }
		}

/*                 Case(2) */
/*                 Equivalent to testing for */
/*                 ABS( REAL( W( IMAX,KW-1 ) ) ).GE.ALPHA*ROWMAX */
/*                 (used to handle NaN and Inf) */

		i__1 = imax + imax * a_dim1;
		if (! ((r__1 = a[i__1].r, abs(r__1)) < alpha * rowmax)) {

/*                    interchange rows and columns K and IMAX, */
/*                    use 1-by-1 pivot block */

		    kp = imax;
		    done = TRUE_;

/*                 Case(3) */
/*                 Equivalent to testing for ROWMAX.EQ.COLMAX, */
/*                 (used to handle NaN and Inf) */

		} else if (p == jmax || rowmax <= colmax) {

/*                    interchange rows and columns K-1 and IMAX, */
/*                    use 2-by-2 pivot block */

		    kp = imax;
		    kstep = 2;
		    done = TRUE_;

/*                 Case(4) */
		} else {

/*                    Pivot not found: set params and repeat */

		    p = imax;
		    colmax = rowmax;
		    imax = jmax;
		}

/*                 END pivot search loop body */

		if (! done) {
		    goto L12;
		}

	    }

/*           END pivot search */

/*           ============================================================ */

/*           KK is the column of A where pivoting step stopped */

	    kk = k - kstep + 1;

/*           For only a 2x2 pivot, interchange rows and columns K and P */
/*           in the leading submatrix A(1:k,1:k) */

	    if (kstep == 2 && p != k) {
/*              (1) Swap columnar parts */
		if (p > 1) {
		    i__1 = p - 1;
		    cswap_(&i__1, &a[k * a_dim1 + 1], &c__1, &a[p * a_dim1 + 
			    1], &c__1);
		}
/*              (2) Swap and conjugate middle parts */
		i__1 = k - 1;
		for (j = p + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    t.r = q__1.r, t.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[p + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = p + j * a_dim1;
		    a[i__2].r = t.r, a[i__2].i = t.i;
/* L14: */
		}
/*              (3) Swap and conjugate corner elements at row-col interserction */
		i__1 = p + k * a_dim1;
		r_cnjg(&q__1, &a[p + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
/*              (4) Swap diagonal elements at row-col intersection */
		i__1 = k + k * a_dim1;
		r1 = a[i__1].r;
		i__1 = k + k * a_dim1;
		i__2 = p + p * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		i__1 = p + p * a_dim1;
		a[i__1].r = r1, a[i__1].i = 0.f;

/*              Convert upper triangle of A into U form by applying */
/*              the interchanges in columns k+1:N. */

		if (k < *n) {
		    i__1 = *n - k;
		    cswap_(&i__1, &a[k + (k + 1) * a_dim1], lda, &a[p + (k + 
			    1) * a_dim1], lda);
		}

	    }

/*           For both 1x1 and 2x2 pivots, interchange rows and */
/*           columns KK and KP in the leading submatrix A(1:k,1:k) */

	    if (kp != kk) {
/*              (1) Swap columnar parts */
		if (kp > 1) {
		    i__1 = kp - 1;
		    cswap_(&i__1, &a[kk * a_dim1 + 1], &c__1, &a[kp * a_dim1 
			    + 1], &c__1);
		}
/*              (2) Swap and conjugate middle parts */
		i__1 = kk - 1;
		for (j = kp + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + kk * a_dim1]);
		    t.r = q__1.r, t.i = q__1.i;
		    i__2 = j + kk * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = t.r, a[i__2].i = t.i;
/* L15: */
		}
/*              (3) Swap and conjugate corner elements at row-col interserction */
		i__1 = kp + kk * a_dim1;
		r_cnjg(&q__1, &a[kp + kk * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
/*              (4) Swap diagonal elements at row-col intersection */
		i__1 = kk + kk * a_dim1;
		r1 = a[i__1].r;
		i__1 = kk + kk * a_dim1;
		i__2 = kp + kp * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = r1, a[i__1].i = 0.f;

		if (kstep == 2) {
/*                 (*) Make sure that diagonal element of pivot is real */
		    i__1 = k + k * a_dim1;
		    i__2 = k + k * a_dim1;
		    r__1 = a[i__2].r;
		    a[i__1].r = r__1, a[i__1].i = 0.f;
/*                 (5) Swap row elements */
		    i__1 = k - 1 + k * a_dim1;
		    t.r = a[i__1].r, t.i = a[i__1].i;
		    i__1 = k - 1 + k * a_dim1;
		    i__2 = kp + k * a_dim1;
		    a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		    i__1 = kp + k * a_dim1;
		    a[i__1].r = t.r, a[i__1].i = t.i;
		}

/*              Convert upper triangle of A into U form by applying */
/*              the interchanges in columns k+1:N. */

		if (k < *n) {
		    i__1 = *n - k;
		    cswap_(&i__1, &a[kk + (k + 1) * a_dim1], lda, &a[kp + (k 
			    + 1) * a_dim1], lda);
		}

	    } else {
/*              (*) Make sure that diagonal element of pivot is real */
		i__1 = k + k * a_dim1;
		i__2 = k + k * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		if (kstep == 2) {
		    i__1 = k - 1 + (k - 1) * a_dim1;
		    i__2 = k - 1 + (k - 1) * a_dim1;
		    r__1 = a[i__2].r;
		    a[i__1].r = r__1, a[i__1].i = 0.f;
		}
	    }

/*           Update the leading submatrix */

	    if (kstep == 1) {

/*              1-by-1 pivot block D(k): column k now holds */

/*              W(k) = U(k)*D(k) */

/*              where U(k) is the k-th column of U */

		if (k > 1) {

/*                 Perform a rank-1 update of A(1:k-1,1:k-1) and */
/*                 store U(k) in column k */

		    i__1 = k + k * a_dim1;
		    if ((r__1 = a[i__1].r, abs(r__1)) >= sfmin) {

/*                    Perform a rank-1 update of A(1:k-1,1:k-1) as */
/*                    A := A - U(k)*D(k)*U(k)**T */
/*                       = A - W(k)*1/D(k)*W(k)**T */

			i__1 = k + k * a_dim1;
			d11 = 1.f / a[i__1].r;
			i__1 = k - 1;
			r__1 = -d11;
			cher_(uplo, &i__1, &r__1, &a[k * a_dim1 + 1], &c__1, &
				a[a_offset], lda);

/*                    Store U(k) in column k */

			i__1 = k - 1;
			csscal_(&i__1, &d11, &a[k * a_dim1 + 1], &c__1);
		    } else {

/*                    Store L(k) in column K */

			i__1 = k + k * a_dim1;
			d11 = a[i__1].r;
			i__1 = k - 1;
			for (ii = 1; ii <= i__1; ++ii) {
			    i__2 = ii + k * a_dim1;
			    i__3 = ii + k * a_dim1;
			    q__1.r = a[i__3].r / d11, q__1.i = a[i__3].i / 
				    d11;
			    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
/* L16: */
			}

/*                    Perform a rank-1 update of A(k+1:n,k+1:n) as */
/*                    A := A - U(k)*D(k)*U(k)**T */
/*                       = A - W(k)*(1/D(k))*W(k)**T */
/*                       = A - (W(k)/D(k))*(D(k))*(W(k)/D(K))**T */

			i__1 = k - 1;
			r__1 = -d11;
			cher_(uplo, &i__1, &r__1, &a[k * a_dim1 + 1], &c__1, &
				a[a_offset], lda);
		    }

/*                 Store the superdiagonal element of D in array E */

		    i__1 = k;
		    e[i__1].r = 0.f, e[i__1].i = 0.f;

		}

	    } else {

/*              2-by-2 pivot block D(k): columns k and k-1 now hold */

/*              ( W(k-1) W(k) ) = ( U(k-1) U(k) )*D(k) */

/*              where U(k) and U(k-1) are the k-th and (k-1)-th columns */
/*              of U */

/*              Perform a rank-2 update of A(1:k-2,1:k-2) as */

/*              A := A - ( U(k-1) U(k) )*D(k)*( U(k-1) U(k) )**T */
/*                 = A - ( ( A(k-1)A(k) )*inv(D(k)) ) * ( A(k-1)A(k) )**T */

/*              and store L(k) and L(k+1) in columns k and k+1 */

		if (k > 2) {
/*                 D = |A12| */
		    i__1 = k - 1 + k * a_dim1;
		    r__1 = a[i__1].r;
		    r__2 = r_imag(&a[k - 1 + k * a_dim1]);
		    d__ = slapy2_(&r__1, &r__2);
		    i__1 = k + k * a_dim1;
		    q__1.r = a[i__1].r / d__, q__1.i = a[i__1].i / d__;
		    d11 = q__1.r;
		    i__1 = k - 1 + (k - 1) * a_dim1;
		    q__1.r = a[i__1].r / d__, q__1.i = a[i__1].i / d__;
		    d22 = q__1.r;
		    i__1 = k - 1 + k * a_dim1;
		    q__1.r = a[i__1].r / d__, q__1.i = a[i__1].i / d__;
		    d12.r = q__1.r, d12.i = q__1.i;
		    tt = 1.f / (d11 * d22 - 1.f);

		    for (j = k - 2; j >= 1; --j) {

/*                    Compute  D21 * ( W(k)W(k+1) ) * inv(D(k)) for row J */

			i__1 = j + (k - 1) * a_dim1;
			q__3.r = d11 * a[i__1].r, q__3.i = d11 * a[i__1].i;
			r_cnjg(&q__5, &d12);
			i__2 = j + k * a_dim1;
			q__4.r = q__5.r * a[i__2].r - q__5.i * a[i__2].i, 
				q__4.i = q__5.r * a[i__2].i + q__5.i * a[i__2]
				.r;
			q__2.r = q__3.r - q__4.r, q__2.i = q__3.i - q__4.i;
			q__1.r = tt * q__2.r, q__1.i = tt * q__2.i;
			wkm1.r = q__1.r, wkm1.i = q__1.i;
			i__1 = j + k * a_dim1;
			q__3.r = d22 * a[i__1].r, q__3.i = d22 * a[i__1].i;
			i__2 = j + (k - 1) * a_dim1;
			q__4.r = d12.r * a[i__2].r - d12.i * a[i__2].i, 
				q__4.i = d12.r * a[i__2].i + d12.i * a[i__2]
				.r;
			q__2.r = q__3.r - q__4.r, q__2.i = q__3.i - q__4.i;
			q__1.r = tt * q__2.r, q__1.i = tt * q__2.i;
			wk.r = q__1.r, wk.i = q__1.i;

/*                    Perform a rank-2 update of A(1:k-2,1:k-2) */

			for (i__ = j; i__ >= 1; --i__) {
			    i__1 = i__ + j * a_dim1;
			    i__2 = i__ + j * a_dim1;
			    i__3 = i__ + k * a_dim1;
			    q__4.r = a[i__3].r / d__, q__4.i = a[i__3].i / 
				    d__;
			    r_cnjg(&q__5, &wk);
			    q__3.r = q__4.r * q__5.r - q__4.i * q__5.i, 
				    q__3.i = q__4.r * q__5.i + q__4.i * 
				    q__5.r;
			    q__2.r = a[i__2].r - q__3.r, q__2.i = a[i__2].i - 
				    q__3.i;
			    i__4 = i__ + (k - 1) * a_dim1;
			    q__7.r = a[i__4].r / d__, q__7.i = a[i__4].i / 
				    d__;
			    r_cnjg(&q__8, &wkm1);
			    q__6.r = q__7.r * q__8.r - q__7.i * q__8.i, 
				    q__6.i = q__7.r * q__8.i + q__7.i * 
				    q__8.r;
			    q__1.r = q__2.r - q__6.r, q__1.i = q__2.i - 
				    q__6.i;
			    a[i__1].r = q__1.r, a[i__1].i = q__1.i;
/* L20: */
			}

/*                    Store U(k) and U(k-1) in cols k and k-1 for row J */

			i__1 = j + k * a_dim1;
			q__1.r = wk.r / d__, q__1.i = wk.i / d__;
			a[i__1].r = q__1.r, a[i__1].i = q__1.i;
			i__1 = j + (k - 1) * a_dim1;
			q__1.r = wkm1.r / d__, q__1.i = wkm1.i / d__;
			a[i__1].r = q__1.r, a[i__1].i = q__1.i;
/*                    (*) Make sure that diagonal element of pivot is real */
			i__1 = j + j * a_dim1;
			i__2 = j + j * a_dim1;
			r__1 = a[i__2].r;
			q__1.r = r__1, q__1.i = 0.f;
			a[i__1].r = q__1.r, a[i__1].i = q__1.i;

/* L30: */
		    }

		}

/*              Copy superdiagonal elements of D(K) to E(K) and */
/*              ZERO out superdiagonal entry of A */

		i__1 = k;
		i__2 = k - 1 + k * a_dim1;
		e[i__1].r = a[i__2].r, e[i__1].i = a[i__2].i;
		i__1 = k - 1;
		e[i__1].r = 0.f, e[i__1].i = 0.f;
		i__1 = k - 1 + k * a_dim1;
		a[i__1].r = 0.f, a[i__1].i = 0.f;

	    }

/*           End column K is nonsingular */

	}

/*        Store details of the interchanges in IPIV */

	if (kstep == 1) {
	    ipiv[k] = kp;
	} else {
	    ipiv[k] = -p;
	    ipiv[k - 1] = -kp;
	}

/*        Decrease K and return to the start of the main loop */

	k -= kstep;
	goto L10;

L34:

	;
    } else {

/*        Factorize A as L*D*L**H using the lower triangle of A */

/*        Initialize the unused last entry of the subdiagonal array E. */

	i__1 = *n;
	e[i__1].r = 0.f, e[i__1].i = 0.f;

/*        K is the main loop index, increasing from 1 to N in steps of */
/*        1 or 2 */

	k = 1;
L40:

/*        If K > N, exit from loop */

	if (k > *n) {
	    goto L64;
	}
	kstep = 1;
	p = k;

/*        Determine rows and columns to be interchanged and whether */
/*        a 1-by-1 or 2-by-2 pivot block will be used */

	i__1 = k + k * a_dim1;
	absakk = (r__1 = a[i__1].r, abs(r__1));

/*        IMAX is the row-index of the largest off-diagonal element in */
/*        column K, and COLMAX is its absolute value. */
/*        Determine both COLMAX and IMAX. */

	if (k < *n) {
	    i__1 = *n - k;
	    imax = k + icamax_(&i__1, &a[k + 1 + k * a_dim1], &c__1);
	    i__1 = imax + k * a_dim1;
	    colmax = (r__1 = a[i__1].r, abs(r__1)) + (r__2 = r_imag(&a[imax + 
		    k * a_dim1]), abs(r__2));
	} else {
	    colmax = 0.f;
	}

	if (f2cmax(absakk,colmax) == 0.f) {

/*           Column K is zero or underflow: set INFO and continue */

	    if (*info == 0) {
		*info = k;
	    }
	    kp = k;
	    i__1 = k + k * a_dim1;
	    i__2 = k + k * a_dim1;
	    r__1 = a[i__2].r;
	    a[i__1].r = r__1, a[i__1].i = 0.f;

/*           Set E( K ) to zero */

	    if (k < *n) {
		i__1 = k;
		e[i__1].r = 0.f, e[i__1].i = 0.f;
	    }

	} else {

/*           ============================================================ */

/*           BEGIN pivot search */

/*           Case(1) */
/*           Equivalent to testing for ABSAKK.GE.ALPHA*COLMAX */
/*           (used to handle NaN and Inf) */

	    if (! (absakk < alpha * colmax)) {

/*              no interchange, use 1-by-1 pivot block */

		kp = k;

	    } else {

		done = FALSE_;

/*              Loop until pivot found */

L42:

/*                 BEGIN pivot search loop body */


/*                 JMAX is the column-index of the largest off-diagonal */
/*                 element in row IMAX, and ROWMAX is its absolute value. */
/*                 Determine both ROWMAX and JMAX. */

		if (imax != k) {
		    i__1 = imax - k;
		    jmax = k - 1 + icamax_(&i__1, &a[imax + k * a_dim1], lda);
		    i__1 = imax + jmax * a_dim1;
		    rowmax = (r__1 = a[i__1].r, abs(r__1)) + (r__2 = r_imag(&
			    a[imax + jmax * a_dim1]), abs(r__2));
		} else {
		    rowmax = 0.f;
		}

		if (imax < *n) {
		    i__1 = *n - imax;
		    itemp = imax + icamax_(&i__1, &a[imax + 1 + imax * a_dim1]
			    , &c__1);
		    i__1 = itemp + imax * a_dim1;
		    stemp = (r__1 = a[i__1].r, abs(r__1)) + (r__2 = r_imag(&a[
			    itemp + imax * a_dim1]), abs(r__2));
		    if (stemp > rowmax) {
			rowmax = stemp;
			jmax = itemp;
		    }
		}

/*                 Case(2) */
/*                 Equivalent to testing for */
/*                 ABS( REAL( W( IMAX,KW-1 ) ) ).GE.ALPHA*ROWMAX */
/*                 (used to handle NaN and Inf) */

		i__1 = imax + imax * a_dim1;
		if (! ((r__1 = a[i__1].r, abs(r__1)) < alpha * rowmax)) {

/*                    interchange rows and columns K and IMAX, */
/*                    use 1-by-1 pivot block */

		    kp = imax;
		    done = TRUE_;

/*                 Case(3) */
/*                 Equivalent to testing for ROWMAX.EQ.COLMAX, */
/*                 (used to handle NaN and Inf) */

		} else if (p == jmax || rowmax <= colmax) {

/*                    interchange rows and columns K+1 and IMAX, */
/*                    use 2-by-2 pivot block */

		    kp = imax;
		    kstep = 2;
		    done = TRUE_;

/*                 Case(4) */
		} else {

/*                    Pivot not found: set params and repeat */

		    p = imax;
		    colmax = rowmax;
		    imax = jmax;
		}


/*                 END pivot search loop body */

		if (! done) {
		    goto L42;
		}

	    }

/*           END pivot search */

/*           ============================================================ */

/*           KK is the column of A where pivoting step stopped */

	    kk = k + kstep - 1;

/*           For only a 2x2 pivot, interchange rows and columns K and P */
/*           in the trailing submatrix A(k:n,k:n) */

	    if (kstep == 2 && p != k) {
/*              (1) Swap columnar parts */
		if (p < *n) {
		    i__1 = *n - p;
		    cswap_(&i__1, &a[p + 1 + k * a_dim1], &c__1, &a[p + 1 + p 
			    * a_dim1], &c__1);
		}
/*              (2) Swap and conjugate middle parts */
		i__1 = p - 1;
		for (j = k + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    t.r = q__1.r, t.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[p + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = p + j * a_dim1;
		    a[i__2].r = t.r, a[i__2].i = t.i;
/* L44: */
		}
/*              (3) Swap and conjugate corner elements at row-col interserction */
		i__1 = p + k * a_dim1;
		r_cnjg(&q__1, &a[p + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
/*              (4) Swap diagonal elements at row-col intersection */
		i__1 = k + k * a_dim1;
		r1 = a[i__1].r;
		i__1 = k + k * a_dim1;
		i__2 = p + p * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		i__1 = p + p * a_dim1;
		a[i__1].r = r1, a[i__1].i = 0.f;

/*              Convert lower triangle of A into L form by applying */
/*              the interchanges in columns 1:k-1. */

		if (k > 1) {
		    i__1 = k - 1;
		    cswap_(&i__1, &a[k + a_dim1], lda, &a[p + a_dim1], lda);
		}

	    }

/*           For both 1x1 and 2x2 pivots, interchange rows and */
/*           columns KK and KP in the trailing submatrix A(k:n,k:n) */

	    if (kp != kk) {
/*              (1) Swap columnar parts */
		if (kp < *n) {
		    i__1 = *n - kp;
		    cswap_(&i__1, &a[kp + 1 + kk * a_dim1], &c__1, &a[kp + 1 
			    + kp * a_dim1], &c__1);
		}
/*              (2) Swap and conjugate middle parts */
		i__1 = kp - 1;
		for (j = kk + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + kk * a_dim1]);
		    t.r = q__1.r, t.i = q__1.i;
		    i__2 = j + kk * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = t.r, a[i__2].i = t.i;
/* L45: */
		}
/*              (3) Swap and conjugate corner elements at row-col interserction */
		i__1 = kp + kk * a_dim1;
		r_cnjg(&q__1, &a[kp + kk * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
/*              (4) Swap diagonal elements at row-col intersection */
		i__1 = kk + kk * a_dim1;
		r1 = a[i__1].r;
		i__1 = kk + kk * a_dim1;
		i__2 = kp + kp * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = r1, a[i__1].i = 0.f;

		if (kstep == 2) {
/*                 (*) Make sure that diagonal element of pivot is real */
		    i__1 = k + k * a_dim1;
		    i__2 = k + k * a_dim1;
		    r__1 = a[i__2].r;
		    a[i__1].r = r__1, a[i__1].i = 0.f;
/*                 (5) Swap row elements */
		    i__1 = k + 1 + k * a_dim1;
		    t.r = a[i__1].r, t.i = a[i__1].i;
		    i__1 = k + 1 + k * a_dim1;
		    i__2 = kp + k * a_dim1;
		    a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		    i__1 = kp + k * a_dim1;
		    a[i__1].r = t.r, a[i__1].i = t.i;
		}

/*              Convert lower triangle of A into L form by applying */
/*              the interchanges in columns 1:k-1. */

		if (k > 1) {
		    i__1 = k - 1;
		    cswap_(&i__1, &a[kk + a_dim1], lda, &a[kp + a_dim1], lda);
		}

	    } else {
/*              (*) Make sure that diagonal element of pivot is real */
		i__1 = k + k * a_dim1;
		i__2 = k + k * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		if (kstep == 2) {
		    i__1 = k + 1 + (k + 1) * a_dim1;
		    i__2 = k + 1 + (k + 1) * a_dim1;
		    r__1 = a[i__2].r;
		    a[i__1].r = r__1, a[i__1].i = 0.f;
		}
	    }

/*           Update the trailing submatrix */

	    if (kstep == 1) {

/*              1-by-1 pivot block D(k): column k of A now holds */

/*              W(k) = L(k)*D(k), */

/*              where L(k) is the k-th column of L */

		if (k < *n) {

/*                 Perform a rank-1 update of A(k+1:n,k+1:n) and */
/*                 store L(k) in column k */

/*                 Handle division by a small number */

		    i__1 = k + k * a_dim1;
		    if ((r__1 = a[i__1].r, abs(r__1)) >= sfmin) {

/*                    Perform a rank-1 update of A(k+1:n,k+1:n) as */
/*                    A := A - L(k)*D(k)*L(k)**T */
/*                       = A - W(k)*(1/D(k))*W(k)**T */

			i__1 = k + k * a_dim1;
			d11 = 1.f / a[i__1].r;
			i__1 = *n - k;
			r__1 = -d11;
			cher_(uplo, &i__1, &r__1, &a[k + 1 + k * a_dim1], &
				c__1, &a[k + 1 + (k + 1) * a_dim1], lda);

/*                    Store L(k) in column k */

			i__1 = *n - k;
			csscal_(&i__1, &d11, &a[k + 1 + k * a_dim1], &c__1);
		    } else {

/*                    Store L(k) in column k */

			i__1 = k + k * a_dim1;
			d11 = a[i__1].r;
			i__1 = *n;
			for (ii = k + 1; ii <= i__1; ++ii) {
			    i__2 = ii + k * a_dim1;
			    i__3 = ii + k * a_dim1;
			    q__1.r = a[i__3].r / d11, q__1.i = a[i__3].i / 
				    d11;
			    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
/* L46: */
			}

/*                    Perform a rank-1 update of A(k+1:n,k+1:n) as */
/*                    A := A - L(k)*D(k)*L(k)**T */
/*                       = A - W(k)*(1/D(k))*W(k)**T */
/*                       = A - (W(k)/D(k))*(D(k))*(W(k)/D(K))**T */

			i__1 = *n - k;
			r__1 = -d11;
			cher_(uplo, &i__1, &r__1, &a[k + 1 + k * a_dim1], &
				c__1, &a[k + 1 + (k + 1) * a_dim1], lda);
		    }

/*                 Store the subdiagonal element of D in array E */

		    i__1 = k;
		    e[i__1].r = 0.f, e[i__1].i = 0.f;

		}

	    } else {

/*              2-by-2 pivot block D(k): columns k and k+1 now hold */

/*              ( W(k) W(k+1) ) = ( L(k) L(k+1) )*D(k) */

/*              where L(k) and L(k+1) are the k-th and (k+1)-th columns */
/*              of L */


/*              Perform a rank-2 update of A(k+2:n,k+2:n) as */

/*              A := A - ( L(k) L(k+1) ) * D(k) * ( L(k) L(k+1) )**T */
/*                 = A - ( ( A(k)A(k+1) )*inv(D(k) ) * ( A(k)A(k+1) )**T */

/*              and store L(k) and L(k+1) in columns k and k+1 */

		if (k < *n - 1) {
/*                 D = |A21| */
		    i__1 = k + 1 + k * a_dim1;
		    r__1 = a[i__1].r;
		    r__2 = r_imag(&a[k + 1 + k * a_dim1]);
		    d__ = slapy2_(&r__1, &r__2);
		    i__1 = k + 1 + (k + 1) * a_dim1;
		    d11 = a[i__1].r / d__;
		    i__1 = k + k * a_dim1;
		    d22 = a[i__1].r / d__;
		    i__1 = k + 1 + k * a_dim1;
		    q__1.r = a[i__1].r / d__, q__1.i = a[i__1].i / d__;
		    d21.r = q__1.r, d21.i = q__1.i;
		    tt = 1.f / (d11 * d22 - 1.f);

		    i__1 = *n;
		    for (j = k + 2; j <= i__1; ++j) {

/*                    Compute  D21 * ( W(k)W(k+1) ) * inv(D(k)) for row J */

			i__2 = j + k * a_dim1;
			q__3.r = d11 * a[i__2].r, q__3.i = d11 * a[i__2].i;
			i__3 = j + (k + 1) * a_dim1;
			q__4.r = d21.r * a[i__3].r - d21.i * a[i__3].i, 
				q__4.i = d21.r * a[i__3].i + d21.i * a[i__3]
				.r;
			q__2.r = q__3.r - q__4.r, q__2.i = q__3.i - q__4.i;
			q__1.r = tt * q__2.r, q__1.i = tt * q__2.i;
			wk.r = q__1.r, wk.i = q__1.i;
			i__2 = j + (k + 1) * a_dim1;
			q__3.r = d22 * a[i__2].r, q__3.i = d22 * a[i__2].i;
			r_cnjg(&q__5, &d21);
			i__3 = j + k * a_dim1;
			q__4.r = q__5.r * a[i__3].r - q__5.i * a[i__3].i, 
				q__4.i = q__5.r * a[i__3].i + q__5.i * a[i__3]
				.r;
			q__2.r = q__3.r - q__4.r, q__2.i = q__3.i - q__4.i;
			q__1.r = tt * q__2.r, q__1.i = tt * q__2.i;
			wkp1.r = q__1.r, wkp1.i = q__1.i;

/*                    Perform a rank-2 update of A(k+2:n,k+2:n) */

			i__2 = *n;
			for (i__ = j; i__ <= i__2; ++i__) {
			    i__3 = i__ + j * a_dim1;
			    i__4 = i__ + j * a_dim1;
			    i__5 = i__ + k * a_dim1;
			    q__4.r = a[i__5].r / d__, q__4.i = a[i__5].i / 
				    d__;
			    r_cnjg(&q__5, &wk);
			    q__3.r = q__4.r * q__5.r - q__4.i * q__5.i, 
				    q__3.i = q__4.r * q__5.i + q__4.i * 
				    q__5.r;
			    q__2.r = a[i__4].r - q__3.r, q__2.i = a[i__4].i - 
				    q__3.i;
			    i__6 = i__ + (k + 1) * a_dim1;
			    q__7.r = a[i__6].r / d__, q__7.i = a[i__6].i / 
				    d__;
			    r_cnjg(&q__8, &wkp1);
			    q__6.r = q__7.r * q__8.r - q__7.i * q__8.i, 
				    q__6.i = q__7.r * q__8.i + q__7.i * 
				    q__8.r;
			    q__1.r = q__2.r - q__6.r, q__1.i = q__2.i - 
				    q__6.i;
			    a[i__3].r = q__1.r, a[i__3].i = q__1.i;
/* L50: */
			}

/*                    Store L(k) and L(k+1) in cols k and k+1 for row J */

			i__2 = j + k * a_dim1;
			q__1.r = wk.r / d__, q__1.i = wk.i / d__;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
			i__2 = j + (k + 1) * a_dim1;
			q__1.r = wkp1.r / d__, q__1.i = wkp1.i / d__;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
/*                    (*) Make sure that diagonal element of pivot is real */
			i__2 = j + j * a_dim1;
			i__3 = j + j * a_dim1;
			r__1 = a[i__3].r;
			q__1.r = r__1, q__1.i = 0.f;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;

/* L60: */
		    }

		}

/*              Copy subdiagonal elements of D(K) to E(K) and */
/*              ZERO out subdiagonal entry of A */

		i__1 = k;
		i__2 = k + 1 + k * a_dim1;
		e[i__1].r = a[i__2].r, e[i__1].i = a[i__2].i;
		i__1 = k + 1;
		e[i__1].r = 0.f, e[i__1].i = 0.f;
		i__1 = k + 1 + k * a_dim1;
		a[i__1].r = 0.f, a[i__1].i = 0.f;

	    }

/*           End column K is nonsingular */

	}

/*        Store details of the interchanges in IPIV */

	if (kstep == 1) {
	    ipiv[k] = kp;
	} else {
	    ipiv[k] = -p;
	    ipiv[k + 1] = -kp;
	}

/*        Increase K and return to the start of the main loop */

	k += kstep;
	goto L40;

L64:

	;
    }

    return;

/*     End of CHETF2_RK */

} /* chetf2_rk__ */

