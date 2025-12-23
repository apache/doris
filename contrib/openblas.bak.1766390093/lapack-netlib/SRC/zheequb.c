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

/* > \brief \b ZHEEQUB */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZHEEQUB + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zheequb
.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zheequb
.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zheequb
.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZHEEQUB( UPLO, N, A, LDA, S, SCOND, AMAX, WORK, INFO ) */

/*       INTEGER            INFO, LDA, N */
/*       DOUBLE PRECISION   AMAX, SCOND */
/*       CHARACTER          UPLO */
/*       COMPLEX*16         A( LDA, * ), WORK( * ) */
/*       DOUBLE PRECISION   S( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZHEEQUB computes row and column scalings intended to equilibrate a */
/* > Hermitian matrix A (with respect to the Euclidean norm) and reduce */
/* > its condition number. The scale factors S are computed by the BIN */
/* > algorithm (see references) so that the scaled matrix B with elements */
/* > B(i,j) = S(i)*A(i,j)*S(j) has a condition number within a factor N of */
/* > the smallest possible condition number over all possible diagonal */
/* > scalings. */
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
/* >          The order of the matrix A. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] A */
/* > \verbatim */
/* >          A is COMPLEX*16 array, dimension (LDA,N) */
/* >          The N-by-N Hermitian matrix whose scaling factors are to be */
/* >          computed. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is DOUBLE PRECISION array, dimension (N) */
/* >          If INFO = 0, S contains the scale factors for A. */
/* > \endverbatim */
/* > */
/* > \param[out] SCOND */
/* > \verbatim */
/* >          SCOND is DOUBLE PRECISION */
/* >          If INFO = 0, S contains the ratio of the smallest S(i) to */
/* >          the largest S(i). If SCOND >= 0.1 and AMAX is neither too */
/* >          large nor too small, it is not worth scaling by S. */
/* > \endverbatim */
/* > */
/* > \param[out] AMAX */
/* > \verbatim */
/* >          AMAX is DOUBLE PRECISION */
/* >          Largest absolute value of any matrix element. If AMAX is */
/* >          very close to overflow or very close to underflow, the */
/* >          matrix should be scaled. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension (2*N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value */
/* >          > 0:  if INFO = i, the i-th diagonal element is nonpositive. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date April 2012 */

/* > \ingroup complex16HEcomputational */

/* > \par References: */
/*  ================ */
/* > */
/* >  Livne, O.E. and Golub, G.H., "Scaling by Binormalization", \n */
/* >  Numerical Algorithms, vol. 35, no. 1, pp. 97-120, January 2004. \n */
/* >  DOI 10.1023/B:NUMA.0000016606.32820.69 \n */
/* >  Tech report version: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.3.1679 */
/* > */
/*  ===================================================================== */
/* Subroutine */ void zheequb_(char *uplo, integer *n, doublecomplex *a, 
	integer *lda, doublereal *s, doublereal *scond, doublereal *amax, 
	doublecomplex *work, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5;
    doublereal d__1, d__2, d__3, d__4;
    doublecomplex z__1, z__2, z__3, z__4;

    /* Local variables */
    doublereal base;
    integer iter;
    doublereal smin, smax, d__;
    integer i__, j;
    doublereal t, u, scale;
    extern logical lsame_(char *, char *);
    doublereal c0, c1, c2, sumsq;
    extern doublereal dlamch_(char *);
    doublereal si;
    logical up;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    doublereal bignum, smlnum;
    extern /* Subroutine */ void zlassq_(integer *, doublecomplex *, integer *,
	     doublereal *, doublereal *);
    doublereal avg, std, tol;


/*  -- LAPACK computational routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     April 2012 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --s;
    --work;

    /* Function Body */
    *info = 0;
    if (! (lsame_(uplo, "U") || lsame_(uplo, "L"))) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -4;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("ZHEEQUB", &i__1, (ftnlen)7);
	return;
    }
    up = lsame_(uplo, "U");
    *amax = 0.;

/*     Quick return if possible. */

    if (*n == 0) {
	*scond = 1.;
	return;
    }
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	s[i__] = 0.;
    }
    *amax = 0.;
    if (up) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = j - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
/* Computing MAX */
		i__3 = i__ + j * a_dim1;
		d__3 = s[i__], d__4 = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = 
			d_imag(&a[i__ + j * a_dim1]), abs(d__2));
		s[i__] = f2cmax(d__3,d__4);
/* Computing MAX */
		i__3 = i__ + j * a_dim1;
		d__3 = s[j], d__4 = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = 
			d_imag(&a[i__ + j * a_dim1]), abs(d__2));
		s[j] = f2cmax(d__3,d__4);
/* Computing MAX */
		i__3 = i__ + j * a_dim1;
		d__3 = *amax, d__4 = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = 
			d_imag(&a[i__ + j * a_dim1]), abs(d__2));
		*amax = f2cmax(d__3,d__4);
	    }
/* Computing MAX */
	    i__2 = j + j * a_dim1;
	    d__3 = s[j], d__4 = (d__1 = a[i__2].r, abs(d__1)) + (d__2 = 
		    d_imag(&a[j + j * a_dim1]), abs(d__2));
	    s[j] = f2cmax(d__3,d__4);
/* Computing MAX */
	    i__2 = j + j * a_dim1;
	    d__3 = *amax, d__4 = (d__1 = a[i__2].r, abs(d__1)) + (d__2 = 
		    d_imag(&a[j + j * a_dim1]), abs(d__2));
	    *amax = f2cmax(d__3,d__4);
	}
    } else {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
/* Computing MAX */
	    i__2 = j + j * a_dim1;
	    d__3 = s[j], d__4 = (d__1 = a[i__2].r, abs(d__1)) + (d__2 = 
		    d_imag(&a[j + j * a_dim1]), abs(d__2));
	    s[j] = f2cmax(d__3,d__4);
/* Computing MAX */
	    i__2 = j + j * a_dim1;
	    d__3 = *amax, d__4 = (d__1 = a[i__2].r, abs(d__1)) + (d__2 = 
		    d_imag(&a[j + j * a_dim1]), abs(d__2));
	    *amax = f2cmax(d__3,d__4);
	    i__2 = *n;
	    for (i__ = j + 1; i__ <= i__2; ++i__) {
/* Computing MAX */
		i__3 = i__ + j * a_dim1;
		d__3 = s[i__], d__4 = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = 
			d_imag(&a[i__ + j * a_dim1]), abs(d__2));
		s[i__] = f2cmax(d__3,d__4);
/* Computing MAX */
		i__3 = i__ + j * a_dim1;
		d__3 = s[j], d__4 = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = 
			d_imag(&a[i__ + j * a_dim1]), abs(d__2));
		s[j] = f2cmax(d__3,d__4);
/* Computing MAX */
		i__3 = i__ + j * a_dim1;
		d__3 = *amax, d__4 = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = 
			d_imag(&a[i__ + j * a_dim1]), abs(d__2));
		*amax = f2cmax(d__3,d__4);
	    }
	}
    }
    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	s[j] = 1. / s[j];
    }
    tol = 1. / sqrt(*n * 2.);
    for (iter = 1; iter <= 100; ++iter) {
	scale = 0.;
	sumsq = 0.;
/*        beta = |A|s */
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = i__;
	    work[i__2].r = 0., work[i__2].i = 0.;
	}
	if (up) {
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		i__2 = j - 1;
		for (i__ = 1; i__ <= i__2; ++i__) {
		    i__3 = i__;
		    i__4 = i__;
		    i__5 = i__ + j * a_dim1;
		    d__3 = ((d__1 = a[i__5].r, abs(d__1)) + (d__2 = d_imag(&a[
			    i__ + j * a_dim1]), abs(d__2))) * s[j];
		    z__1.r = work[i__4].r + d__3, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		    i__3 = j;
		    i__4 = j;
		    i__5 = i__ + j * a_dim1;
		    d__3 = ((d__1 = a[i__5].r, abs(d__1)) + (d__2 = d_imag(&a[
			    i__ + j * a_dim1]), abs(d__2))) * s[i__];
		    z__1.r = work[i__4].r + d__3, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		}
		i__2 = j;
		i__3 = j;
		i__4 = j + j * a_dim1;
		d__3 = ((d__1 = a[i__4].r, abs(d__1)) + (d__2 = d_imag(&a[j + 
			j * a_dim1]), abs(d__2))) * s[j];
		z__1.r = work[i__3].r + d__3, z__1.i = work[i__3].i;
		work[i__2].r = z__1.r, work[i__2].i = z__1.i;
	    }
	} else {
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		i__2 = j;
		i__3 = j;
		i__4 = j + j * a_dim1;
		d__3 = ((d__1 = a[i__4].r, abs(d__1)) + (d__2 = d_imag(&a[j + 
			j * a_dim1]), abs(d__2))) * s[j];
		z__1.r = work[i__3].r + d__3, z__1.i = work[i__3].i;
		work[i__2].r = z__1.r, work[i__2].i = z__1.i;
		i__2 = *n;
		for (i__ = j + 1; i__ <= i__2; ++i__) {
		    i__3 = i__;
		    i__4 = i__;
		    i__5 = i__ + j * a_dim1;
		    d__3 = ((d__1 = a[i__5].r, abs(d__1)) + (d__2 = d_imag(&a[
			    i__ + j * a_dim1]), abs(d__2))) * s[j];
		    z__1.r = work[i__4].r + d__3, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		    i__3 = j;
		    i__4 = j;
		    i__5 = i__ + j * a_dim1;
		    d__3 = ((d__1 = a[i__5].r, abs(d__1)) + (d__2 = d_imag(&a[
			    i__ + j * a_dim1]), abs(d__2))) * s[i__];
		    z__1.r = work[i__4].r + d__3, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		}
	    }
	}
/*        avg = s^T beta / n */
	avg = 0.;
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = i__;
	    i__3 = i__;
	    z__2.r = s[i__2] * work[i__3].r, z__2.i = s[i__2] * work[i__3].i;
	    z__1.r = avg + z__2.r, z__1.i = z__2.i;
	    avg = z__1.r;
	}
	avg /= *n;
	std = 0.;
	i__1 = *n << 1;
	for (i__ = *n + 1; i__ <= i__1; ++i__) {
	    i__2 = i__;
	    i__3 = i__ - *n;
	    i__4 = i__ - *n;
	    z__2.r = s[i__3] * work[i__4].r, z__2.i = s[i__3] * work[i__4].i;
	    z__1.r = z__2.r - avg, z__1.i = z__2.i;
	    work[i__2].r = z__1.r, work[i__2].i = z__1.i;
	}
	zlassq_(n, &work[*n + 1], &c__1, &scale, &sumsq);
	std = scale * sqrt(sumsq / *n);
	if (std < tol * avg) {
	    goto L999;
	}
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    i__2 = i__ + i__ * a_dim1;
	    t = (d__1 = a[i__2].r, abs(d__1)) + (d__2 = d_imag(&a[i__ + i__ * 
		    a_dim1]), abs(d__2));
	    si = s[i__];
	    c2 = (*n - 1) * t;
	    i__2 = *n - 2;
	    i__3 = i__;
	    d__1 = t * si;
	    z__2.r = work[i__3].r - d__1, z__2.i = work[i__3].i;
	    d__2 = (doublereal) i__2;
	    z__1.r = d__2 * z__2.r, z__1.i = d__2 * z__2.i;
	    c1 = z__1.r;
	    d__1 = -(t * si) * si;
	    i__2 = i__;
	    d__2 = 2.;
	    z__4.r = d__2 * work[i__2].r, z__4.i = d__2 * work[i__2].i;
	    z__3.r = si * z__4.r, z__3.i = si * z__4.i;
	    z__2.r = d__1 + z__3.r, z__2.i = z__3.i;
	    d__3 = *n * avg;
	    z__1.r = z__2.r - d__3, z__1.i = z__2.i;
	    c0 = z__1.r;
	    d__ = c1 * c1 - c0 * 4 * c2;
	    if (d__ <= 0.) {
		*info = -1;
		return;
	    }
	    si = c0 * -2 / (c1 + sqrt(d__));
	    d__ = si - s[i__];
	    u = 0.;
	    if (up) {
		i__2 = i__;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = j + i__ * a_dim1;
		    t = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = d_imag(&a[j + 
			    i__ * a_dim1]), abs(d__2));
		    u += s[j] * t;
		    i__3 = j;
		    i__4 = j;
		    d__1 = d__ * t;
		    z__1.r = work[i__4].r + d__1, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		}
		i__2 = *n;
		for (j = i__ + 1; j <= i__2; ++j) {
		    i__3 = i__ + j * a_dim1;
		    t = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = d_imag(&a[i__ 
			    + j * a_dim1]), abs(d__2));
		    u += s[j] * t;
		    i__3 = j;
		    i__4 = j;
		    d__1 = d__ * t;
		    z__1.r = work[i__4].r + d__1, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		}
	    } else {
		i__2 = i__;
		for (j = 1; j <= i__2; ++j) {
		    i__3 = i__ + j * a_dim1;
		    t = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = d_imag(&a[i__ 
			    + j * a_dim1]), abs(d__2));
		    u += s[j] * t;
		    i__3 = j;
		    i__4 = j;
		    d__1 = d__ * t;
		    z__1.r = work[i__4].r + d__1, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		}
		i__2 = *n;
		for (j = i__ + 1; j <= i__2; ++j) {
		    i__3 = j + i__ * a_dim1;
		    t = (d__1 = a[i__3].r, abs(d__1)) + (d__2 = d_imag(&a[j + 
			    i__ * a_dim1]), abs(d__2));
		    u += s[j] * t;
		    i__3 = j;
		    i__4 = j;
		    d__1 = d__ * t;
		    z__1.r = work[i__4].r + d__1, z__1.i = work[i__4].i;
		    work[i__3].r = z__1.r, work[i__3].i = z__1.i;
		}
	    }
	    i__2 = i__;
	    z__4.r = u + work[i__2].r, z__4.i = work[i__2].i;
	    z__3.r = d__ * z__4.r, z__3.i = d__ * z__4.i;
	    d__1 = (doublereal) (*n);
	    z__2.r = z__3.r / d__1, z__2.i = z__3.i / d__1;
	    z__1.r = avg + z__2.r, z__1.i = z__2.i;
	    avg = z__1.r;
	    s[i__] = si;
	}
    }
L999:
    smlnum = dlamch_("SAFEMIN");
    bignum = 1. / smlnum;
    smin = bignum;
    smax = 0.;
    t = 1. / sqrt(avg);
    base = dlamch_("B");
    u = 1. / log(base);
    i__1 = *n;
    for (i__ = 1; i__ <= i__1; ++i__) {
	i__2 = (integer) (u * log(s[i__] * t));
	s[i__] = pow_di(&base, &i__2);
/* Computing MIN */
	d__1 = smin, d__2 = s[i__];
	smin = f2cmin(d__1,d__2);
/* Computing MAX */
	d__1 = smax, d__2 = s[i__];
	smax = f2cmax(d__1,d__2);
    }
    *scond = f2cmax(smin,smlnum) / f2cmin(smax,bignum);

    return;
} /* zheequb_ */

