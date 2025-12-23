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
#define mymaxloc_(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

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
static integer c_n1 = -1;
static doublecomplex c_b15 = {1.,0.};
static doublecomplex c_b19 = {-1.,0.};

/* > \brief \b ZSYTRF_AA */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZSYTRF_AA + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zsytrf_
aa.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zsytrf_
aa.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zsytrf_
aa.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZSYTRF_AA( UPLO, N, A, LDA, IPIV, WORK, LWORK, INFO ) */

/*       CHARACTER          UPLO */
/*       INTEGER            N, LDA, LWORK, INFO */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX*16         A( LDA, * ), WORK( * ) */

/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZSYTRF_AA computes the factorization of a complex symmetric matrix A */
/* > using the Aasen's algorithm.  The form of the factorization is */
/* > */
/* >    A = U**T*T*U  or  A = L*T*L**T */
/* > */
/* > where U (or L) is a product of permutation and unit upper (lower) */
/* > triangular matrices, and T is a complex symmetric tridiagonal matrix. */
/* > */
/* > This is the blocked version of the algorithm, calling Level 3 BLAS. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          = 'U':  Upper triangle of A is stored; */
/* >          = 'L':  Lower triangle of A is stored. */
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
/* >          N-by-N upper triangular part of A contains the upper */
/* >          triangular part of the matrix A, and the strictly lower */
/* >          triangular part of A is not referenced.  If UPLO = 'L', the */
/* >          leading N-by-N lower triangular part of A contains the lower */
/* >          triangular part of the matrix A, and the strictly upper */
/* >          triangular part of A is not referenced. */
/* > */
/* >          On exit, the tridiagonal matrix is stored in the diagonals */
/* >          and the subdiagonals of A just below (or above) the diagonals, */
/* >          and L is stored below (or above) the subdiaonals, when UPLO */
/* >          is 'L' (or 'U'). */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >          On exit, it contains the details of the interchanges, i.e., */
/* >          the row and column k of A were interchanged with the */
/* >          row and column IPIV(k). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The length of WORK. LWORK >=MAX(1,2*N). For optimum performance */
/* >          LWORK >= N*(1+NB), where NB is the optimal blocksize. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date November 2017 */

/* > \ingroup complex16SYcomputational */

/*  ===================================================================== */
/* Subroutine */ void zsytrf_aa_(char *uplo, integer *n, doublecomplex *a, 
	integer *lda, integer *ipiv, doublecomplex *work, integer *lwork, 
	integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4;

    /* Local variables */
    integer j;
    doublecomplex alpha;
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void zscal_(integer *, doublecomplex *, 
	    doublecomplex *, integer *), zgemm_(char *, char *, integer *, 
	    integer *, integer *, doublecomplex *, doublecomplex *, integer *,
	     doublecomplex *, integer *, doublecomplex *, doublecomplex *, 
	    integer *), zlasyf_aa_(char *, integer *, 
	    integer *, integer *, doublecomplex *, integer *, integer *, 
	    doublecomplex *, integer *, doublecomplex *), zgemv_(char 
	    *, integer *, integer *, doublecomplex *, doublecomplex *, 
	    integer *, doublecomplex *, integer *, doublecomplex *, 
	    doublecomplex *, integer *);
    logical upper;
    integer k1, k2, j1, j2, j3;
    extern /* Subroutine */ void zcopy_(integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *), zswap_(integer *, doublecomplex *, 
	    integer *, doublecomplex *, integer *);
    integer jb, nb, mj, nj;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    integer lwkopt;
    logical lquery;


/*  -- LAPACK computational routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2017 */



/*  ===================================================================== */


/*     Determine the block size */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ipiv;
    --work;

    /* Function Body */
    nb = ilaenv_(&c__1, "ZSYTRF_AA", uplo, n, &c_n1, &c_n1, &c_n1, (ftnlen)9, 
	    (ftnlen)1);

/*     Test the input parameters. */

    *info = 0;
    upper = lsame_(uplo, "U");
    lquery = *lwork == -1;
    if (! upper && ! lsame_(uplo, "L")) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -4;
    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__1 = 1, i__2 = *n << 1;
	if (*lwork < f2cmax(i__1,i__2) && ! lquery) {
	    *info = -7;
	}
    }

    if (*info == 0) {
	lwkopt = (nb + 1) * *n;
	work[1].r = (doublereal) lwkopt, work[1].i = 0.;
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZSYTRF_AA", &i__1, (ftnlen)9);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return */

    if (*n == 0) {
	return;
    }
    ipiv[1] = 1;
    if (*n == 1) {
	return;
    }

/*     Adjust block size based on the workspace size */

    if (*lwork < (nb + 1) * *n) {
	nb = (*lwork - *n) / *n;
    }

    if (upper) {

/*        ..................................................... */
/*        Factorize A as U**T*D*U using the upper triangle of A */
/*        ..................................................... */

/*        Copy first row A(1, 1:N) into H(1:n) (stored in WORK(1:N)) */

	zcopy_(n, &a[a_dim1 + 1], lda, &work[1], &c__1);

/*        J is the main loop index, increasing from 1 to N in steps of */
/*        JB, where JB is the number of columns factorized by ZLASYF; */
/*        JB is either NB, or N-J+1 for the last block */

	j = 0;
L10:
	if (j >= *n) {
	    goto L20;
	}

/*        each step of the main loop */
/*         J is the last column of the previous panel */
/*         J1 is the first column of the current panel */
/*         K1 identifies if the previous column of the panel has been */
/*          explicitly stored, e.g., K1=1 for the first panel, and */
/*          K1=0 for the rest */

	j1 = j + 1;
/* Computing MIN */
	i__1 = *n - j1 + 1;
	jb = f2cmin(i__1,nb);
	k1 = f2cmax(1,j) - j;

/*        Panel factorization */

	i__1 = 2 - k1;
	i__2 = *n - j;
	zlasyf_aa_(uplo, &i__1, &i__2, &jb, &a[f2cmax(1,j) + (j + 1) * a_dim1], 
		lda, &ipiv[j + 1], &work[1], n, &work[*n * nb + 1])
		;

/*        Adjust IPIV and apply it back (J-th step picks (J+1)-th pivot) */

/* Computing MIN */
	i__2 = *n, i__3 = j + jb + 1;
	i__1 = f2cmin(i__2,i__3);
	for (j2 = j + 2; j2 <= i__1; ++j2) {
	    ipiv[j2] += j;
	    if (j2 != ipiv[j2] && j1 - k1 > 2) {
		i__2 = j1 - k1 - 2;
		zswap_(&i__2, &a[j2 * a_dim1 + 1], &c__1, &a[ipiv[j2] * 
			a_dim1 + 1], &c__1);
	    }
	}
	j += jb;

/*        Trailing submatrix update, where */
/*         the row A(J1-1, J2-1:N) stores U(J1, J2+1:N) and */
/*         WORK stores the current block of the auxiriarly matrix H */

	if (j < *n) {

/*           If first panel and JB=1 (NB=1), then nothing to do */

	    if (j1 > 1 || jb > 1) {

/*              Merge rank-1 update with BLAS-3 update */

		i__1 = j + (j + 1) * a_dim1;
		alpha.r = a[i__1].r, alpha.i = a[i__1].i;
		i__1 = j + (j + 1) * a_dim1;
		a[i__1].r = 1., a[i__1].i = 0.;
		i__1 = *n - j;
		zcopy_(&i__1, &a[j - 1 + (j + 1) * a_dim1], lda, &work[j + 1 
			- j1 + 1 + jb * *n], &c__1);
		i__1 = *n - j;
		zscal_(&i__1, &alpha, &work[j + 1 - j1 + 1 + jb * *n], &c__1);

/*              K1 identifies if the previous column of the panel has been */
/*               explicitly stored, e.g., K1=1 and K2= 0 for the first panel, */
/*               while K1=0 and K2=1 for the rest */

		if (j1 > 1) {

/*                 Not first panel */

		    k2 = 1;
		} else {

/*                 First panel */

		    k2 = 0;

/*                 First update skips the first column */

		    --jb;
		}

		i__1 = *n;
		i__2 = nb;
		for (j2 = j + 1; i__2 < 0 ? j2 >= i__1 : j2 <= i__1; j2 += 
			i__2) {
/* Computing MIN */
		    i__3 = nb, i__4 = *n - j2 + 1;
		    nj = f2cmin(i__3,i__4);

/*                 Update (J2, J2) diagonal block with ZGEMV */

		    j3 = j2;
		    for (mj = nj - 1; mj >= 1; --mj) {
			i__3 = jb + 1;
			zgemv_("No transpose", &mj, &i__3, &c_b19, &work[j3 - 
				j1 + 1 + k1 * *n], n, &a[j1 - k2 + j3 * 
				a_dim1], &c__1, &c_b15, &a[j3 + j3 * a_dim1], 
				lda);
			++j3;
		    }

/*                 Update off-diagonal block of J2-th block row with ZGEMM */

		    i__3 = *n - j3 + 1;
		    i__4 = jb + 1;
		    zgemm_("Transpose", "Transpose", &nj, &i__3, &i__4, &
			    c_b19, &a[j1 - k2 + j2 * a_dim1], lda, &work[j3 - 
			    j1 + 1 + k1 * *n], n, &c_b15, &a[j2 + j3 * a_dim1]
			    , lda);
		}

/*              Recover T( J, J+1 ) */

		i__2 = j + (j + 1) * a_dim1;
		a[i__2].r = alpha.r, a[i__2].i = alpha.i;
	    }

/*           WORK(J+1, 1) stores H(J+1, 1) */

	    i__2 = *n - j;
	    zcopy_(&i__2, &a[j + 1 + (j + 1) * a_dim1], lda, &work[1], &c__1);
	}
	goto L10;
    } else {

/*        ..................................................... */
/*        Factorize A as L*D*L**T using the lower triangle of A */
/*        ..................................................... */

/*        copy first column A(1:N, 1) into H(1:N, 1) */
/*         (stored in WORK(1:N)) */

	zcopy_(n, &a[a_dim1 + 1], &c__1, &work[1], &c__1);

/*        J is the main loop index, increasing from 1 to N in steps of */
/*        JB, where JB is the number of columns factorized by ZLASYF; */
/*        JB is either NB, or N-J+1 for the last block */

	j = 0;
L11:
	if (j >= *n) {
	    goto L20;
	}

/*        each step of the main loop */
/*         J is the last column of the previous panel */
/*         J1 is the first column of the current panel */
/*         K1 identifies if the previous column of the panel has been */
/*          explicitly stored, e.g., K1=1 for the first panel, and */
/*          K1=0 for the rest */

	j1 = j + 1;
/* Computing MIN */
	i__2 = *n - j1 + 1;
	jb = f2cmin(i__2,nb);
	k1 = f2cmax(1,j) - j;

/*        Panel factorization */

	i__2 = 2 - k1;
	i__1 = *n - j;
	zlasyf_aa_(uplo, &i__2, &i__1, &jb, &a[j + 1 + f2cmax(1,j) * a_dim1], 
		lda, &ipiv[j + 1], &work[1], n, &work[*n * nb + 1])
		;

/*        Adjust IPIV and apply it back (J-th step picks (J+1)-th pivot) */

/* Computing MIN */
	i__1 = *n, i__3 = j + jb + 1;
	i__2 = f2cmin(i__1,i__3);
	for (j2 = j + 2; j2 <= i__2; ++j2) {
	    ipiv[j2] += j;
	    if (j2 != ipiv[j2] && j1 - k1 > 2) {
		i__1 = j1 - k1 - 2;
		zswap_(&i__1, &a[j2 + a_dim1], lda, &a[ipiv[j2] + a_dim1], 
			lda);
	    }
	}
	j += jb;

/*        Trailing submatrix update, where */
/*          A(J2+1, J1-1) stores L(J2+1, J1) and */
/*          WORK(J2+1, 1) stores H(J2+1, 1) */

	if (j < *n) {

/*           if first panel and JB=1 (NB=1), then nothing to do */

	    if (j1 > 1 || jb > 1) {

/*              Merge rank-1 update with BLAS-3 update */

		i__2 = j + 1 + j * a_dim1;
		alpha.r = a[i__2].r, alpha.i = a[i__2].i;
		i__2 = j + 1 + j * a_dim1;
		a[i__2].r = 1., a[i__2].i = 0.;
		i__2 = *n - j;
		zcopy_(&i__2, &a[j + 1 + (j - 1) * a_dim1], &c__1, &work[j + 
			1 - j1 + 1 + jb * *n], &c__1);
		i__2 = *n - j;
		zscal_(&i__2, &alpha, &work[j + 1 - j1 + 1 + jb * *n], &c__1);

/*              K1 identifies if the previous column of the panel has been */
/*               explicitly stored, e.g., K1=1 and K2= 0 for the first panel, */
/*               while K1=0 and K2=1 for the rest */

		if (j1 > 1) {

/*                 Not first panel */

		    k2 = 1;
		} else {

/*                 First panel */

		    k2 = 0;

/*                 First update skips the first column */

		    --jb;
		}

		i__2 = *n;
		i__1 = nb;
		for (j2 = j + 1; i__1 < 0 ? j2 >= i__2 : j2 <= i__2; j2 += 
			i__1) {
/* Computing MIN */
		    i__3 = nb, i__4 = *n - j2 + 1;
		    nj = f2cmin(i__3,i__4);

/*                 Update (J2, J2) diagonal block with ZGEMV */

		    j3 = j2;
		    for (mj = nj - 1; mj >= 1; --mj) {
			i__3 = jb + 1;
			zgemv_("No transpose", &mj, &i__3, &c_b19, &work[j3 - 
				j1 + 1 + k1 * *n], n, &a[j3 + (j1 - k2) * 
				a_dim1], lda, &c_b15, &a[j3 + j3 * a_dim1], &
				c__1);
			++j3;
		    }

/*                 Update off-diagonal block in J2-th block column with ZGEMM */

		    i__3 = *n - j3 + 1;
		    i__4 = jb + 1;
		    zgemm_("No transpose", "Transpose", &i__3, &nj, &i__4, &
			    c_b19, &work[j3 - j1 + 1 + k1 * *n], n, &a[j2 + (
			    j1 - k2) * a_dim1], lda, &c_b15, &a[j3 + j2 * 
			    a_dim1], lda);
		}

/*              Recover T( J+1, J ) */

		i__1 = j + 1 + j * a_dim1;
		a[i__1].r = alpha.r, a[i__1].i = alpha.i;
	    }

/*           WORK(J+1, 1) stores H(J+1, 1) */

	    i__1 = *n - j;
	    zcopy_(&i__1, &a[j + 1 + (j + 1) * a_dim1], &c__1, &work[1], &
		    c__1);
	}
	goto L11;
    }

L20:
    return;

/*     End of ZSYTRF_AA */

} /* zsytrf_aa__ */

