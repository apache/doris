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

static complex c_b1 = {0.f,0.f};
static complex c_b2 = {1.f,0.f};
static integer c__1 = 1;
static integer c__0 = 0;
static real c_b41 = 1.f;
static integer c__2 = 2;

/* > \brief <b> CGESVJ </b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CGESVJ + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgesvj.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgesvj.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgesvj.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CGESVJ( JOBA, JOBU, JOBV, M, N, A, LDA, SVA, MV, V, */
/*                          LDV, CWORK, LWORK, RWORK, LRWORK, INFO ) */

/*       INTEGER            INFO, LDA, LDV, LWORK, LRWORK, M, MV, N */
/*       CHARACTER*1        JOBA, JOBU, JOBV */
/*       COMPLEX            A( LDA, * ),  V( LDV, * ), CWORK( LWORK ) */
/*       REAL               RWORK( LRWORK ),  SVA( N ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CGESVJ computes the singular value decomposition (SVD) of a complex */
/* > M-by-N matrix A, where M >= N. The SVD of A is written as */
/* >                                    [++]   [xx]   [x0]   [xx] */
/* >              A = U * SIGMA * V^*,  [++] = [xx] * [ox] * [xx] */
/* >                                    [++]   [xx] */
/* > where SIGMA is an N-by-N diagonal matrix, U is an M-by-N orthonormal */
/* > matrix, and V is an N-by-N unitary matrix. The diagonal elements */
/* > of SIGMA are the singular values of A. The columns of U and V are the */
/* > left and the right singular vectors of A, respectively. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOBA */
/* > \verbatim */
/* >          JOBA is CHARACTER*1 */
/* >          Specifies the structure of A. */
/* >          = 'L': The input matrix A is lower triangular; */
/* >          = 'U': The input matrix A is upper triangular; */
/* >          = 'G': The input matrix A is general M-by-N matrix, M >= N. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBU */
/* > \verbatim */
/* >          JOBU is CHARACTER*1 */
/* >          Specifies whether to compute the left singular vectors */
/* >          (columns of U): */
/* >          = 'U' or 'F': The left singular vectors corresponding to the nonzero */
/* >                 singular values are computed and returned in the leading */
/* >                 columns of A. See more details in the description of A. */
/* >                 The default numerical orthogonality threshold is set to */
/* >                 approximately TOL=CTOL*EPS, CTOL=SQRT(M), EPS=SLAMCH('E'). */
/* >          = 'C': Analogous to JOBU='U', except that user can control the */
/* >                 level of numerical orthogonality of the computed left */
/* >                 singular vectors. TOL can be set to TOL = CTOL*EPS, where */
/* >                 CTOL is given on input in the array WORK. */
/* >                 No CTOL smaller than ONE is allowed. CTOL greater */
/* >                 than 1 / EPS is meaningless. The option 'C' */
/* >                 can be used if M*EPS is satisfactory orthogonality */
/* >                 of the computed left singular vectors, so CTOL=M could */
/* >                 save few sweeps of Jacobi rotations. */
/* >                 See the descriptions of A and WORK(1). */
/* >          = 'N': The matrix U is not computed. However, see the */
/* >                 description of A. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBV */
/* > \verbatim */
/* >          JOBV is CHARACTER*1 */
/* >          Specifies whether to compute the right singular vectors, that */
/* >          is, the matrix V: */
/* >          = 'V' or 'J': the matrix V is computed and returned in the array V */
/* >          = 'A':  the Jacobi rotations are applied to the MV-by-N */
/* >                  array V. In other words, the right singular vector */
/* >                  matrix V is not computed explicitly; instead it is */
/* >                  applied to an MV-by-N matrix initially stored in the */
/* >                  first MV rows of V. */
/* >          = 'N':  the matrix V is not computed and the array V is not */
/* >                  referenced */
/* > \endverbatim */
/* > */
/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the input matrix A. 1/SLAMCH('E') > M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the input matrix A. */
/* >          M >= N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, */
/* >          If JOBU = 'U' .OR. JOBU = 'C': */
/* >                 If INFO = 0 : */
/* >                 RANKA orthonormal columns of U are returned in the */
/* >                 leading RANKA columns of the array A. Here RANKA <= N */
/* >                 is the number of computed singular values of A that are */
/* >                 above the underflow threshold SLAMCH('S'). The singular */
/* >                 vectors corresponding to underflowed or zero singular */
/* >                 values are not computed. The value of RANKA is returned */
/* >                 in the array RWORK as RANKA=NINT(RWORK(2)). Also see the */
/* >                 descriptions of SVA and RWORK. The computed columns of U */
/* >                 are mutually numerically orthogonal up to approximately */
/* >                 TOL=SQRT(M)*EPS (default); or TOL=CTOL*EPS (JOBU = 'C'), */
/* >                 see the description of JOBU. */
/* >                 If INFO > 0, */
/* >                 the procedure CGESVJ did not converge in the given number */
/* >                 of iterations (sweeps). In that case, the computed */
/* >                 columns of U may not be orthogonal up to TOL. The output */
/* >                 U (stored in A), SIGMA (given by the computed singular */
/* >                 values in SVA(1:N)) and V is still a decomposition of the */
/* >                 input matrix A in the sense that the residual */
/* >                 || A - SCALE * U * SIGMA * V^* ||_2 / ||A||_2 is small. */
/* >          If JOBU = 'N': */
/* >                 If INFO = 0 : */
/* >                 Note that the left singular vectors are 'for free' in the */
/* >                 one-sided Jacobi SVD algorithm. However, if only the */
/* >                 singular values are needed, the level of numerical */
/* >                 orthogonality of U is not an issue and iterations are */
/* >                 stopped when the columns of the iterated matrix are */
/* >                 numerically orthogonal up to approximately M*EPS. Thus, */
/* >                 on exit, A contains the columns of U scaled with the */
/* >                 corresponding singular values. */
/* >                 If INFO > 0 : */
/* >                 the procedure CGESVJ did not converge in the given number */
/* >                 of iterations (sweeps). */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[out] SVA */
/* > \verbatim */
/* >          SVA is REAL array, dimension (N) */
/* >          On exit, */
/* >          If INFO = 0 : */
/* >          depending on the value SCALE = RWORK(1), we have: */
/* >                 If SCALE = ONE: */
/* >                 SVA(1:N) contains the computed singular values of A. */
/* >                 During the computation SVA contains the Euclidean column */
/* >                 norms of the iterated matrices in the array A. */
/* >                 If SCALE .NE. ONE: */
/* >                 The singular values of A are SCALE*SVA(1:N), and this */
/* >                 factored representation is due to the fact that some of the */
/* >                 singular values of A might underflow or overflow. */
/* > */
/* >          If INFO > 0 : */
/* >          the procedure CGESVJ did not converge in the given number of */
/* >          iterations (sweeps) and SCALE*SVA(1:N) may not be accurate. */
/* > \endverbatim */
/* > */
/* > \param[in] MV */
/* > \verbatim */
/* >          MV is INTEGER */
/* >          If JOBV = 'A', then the product of Jacobi rotations in CGESVJ */
/* >          is applied to the first MV rows of V. See the description of JOBV. */
/* > \endverbatim */
/* > */
/* > \param[in,out] V */
/* > \verbatim */
/* >          V is COMPLEX array, dimension (LDV,N) */
/* >          If JOBV = 'V', then V contains on exit the N-by-N matrix of */
/* >                         the right singular vectors; */
/* >          If JOBV = 'A', then V contains the product of the computed right */
/* >                         singular vector matrix and the initial matrix in */
/* >                         the array V. */
/* >          If JOBV = 'N', then V is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV */
/* > \verbatim */
/* >          LDV is INTEGER */
/* >          The leading dimension of the array V, LDV >= 1. */
/* >          If JOBV = 'V', then LDV >= f2cmax(1,N). */
/* >          If JOBV = 'A', then LDV >= f2cmax(1,MV) . */
/* > \endverbatim */
/* > */
/* > \param[in,out] CWORK */
/* > \verbatim */
/* >          CWORK is COMPLEX array, dimension (f2cmax(1,LWORK)) */
/* >          Used as workspace. */
/* >          If on entry LWORK = -1, then a workspace query is assumed and */
/* >          no computation is done; CWORK(1) is set to the minial (and optimal) */
/* >          length of CWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER. */
/* >          Length of CWORK, LWORK >= M+N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (f2cmax(6,LRWORK)) */
/* >          On entry, */
/* >          If JOBU = 'C' : */
/* >          RWORK(1) = CTOL, where CTOL defines the threshold for convergence. */
/* >                    The process stops if all columns of A are mutually */
/* >                    orthogonal up to CTOL*EPS, EPS=SLAMCH('E'). */
/* >                    It is required that CTOL >= ONE, i.e. it is not */
/* >                    allowed to force the routine to obtain orthogonality */
/* >                    below EPSILON. */
/* >          On exit, */
/* >          RWORK(1) = SCALE is the scaling factor such that SCALE*SVA(1:N) */
/* >                    are the computed singular values of A. */
/* >                    (See description of SVA().) */
/* >          RWORK(2) = NINT(RWORK(2)) is the number of the computed nonzero */
/* >                    singular values. */
/* >          RWORK(3) = NINT(RWORK(3)) is the number of the computed singular */
/* >                    values that are larger than the underflow threshold. */
/* >          RWORK(4) = NINT(RWORK(4)) is the number of sweeps of Jacobi */
/* >                    rotations needed for numerical convergence. */
/* >          RWORK(5) = max_{i.NE.j} |COS(A(:,i),A(:,j))| in the last sweep. */
/* >                    This is useful information in cases when CGESVJ did */
/* >                    not converge, as it can be used to estimate whether */
/* >                    the output is still useful and for post festum analysis. */
/* >          RWORK(6) = the largest absolute value over all sines of the */
/* >                    Jacobi rotation angles in the last sweep. It can be */
/* >                    useful for a post festum analysis. */
/* >         If on entry LRWORK = -1, then a workspace query is assumed and */
/* >         no computation is done; RWORK(1) is set to the minial (and optimal) */
/* >         length of RWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LRWORK */
/* > \verbatim */
/* >         LRWORK is INTEGER */
/* >         Length of RWORK, LRWORK >= MAX(6,N). */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit. */
/* >          < 0:  if INFO = -i, then the i-th argument had an illegal value */
/* >          > 0:  CGESVJ did not converge in the maximal allowed number */
/* >                (NSWEEP=30) of sweeps. The output may still be useful. */
/* >                See the description of RWORK. */
/* > \endverbatim */
/* > */
/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup complexGEcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* > The orthogonal N-by-N matrix V is obtained as a product of Jacobi plane */
/* > rotations. In the case of underflow of the tangent of the Jacobi angle, a */
/* > modified Jacobi transformation of Drmac [3] is used. Pivot strategy uses */
/* > column interchanges of de Rijk [1]. The relative accuracy of the computed */
/* > singular values and the accuracy of the computed singular vectors (in */
/* > angle metric) is as guaranteed by the theory of Demmel and Veselic [2]. */
/* > The condition number that determines the accuracy in the full rank case */
/* > is essentially min_{D=diag} kappa(A*D), where kappa(.) is the */
/* > spectral condition number. The best performance of this Jacobi SVD */
/* > procedure is achieved if used in an  accelerated version of Drmac and */
/* > Veselic [4,5], and it is the kernel routine in the SIGMA library [6]. */
/* > Some tunning parameters (marked with [TP]) are available for the */
/* > implementer. */
/* > The computational range for the nonzero singular values is the  machine */
/* > number interval ( UNDERFLOW , OVERFLOW ). In extreme cases, even */
/* > denormalized singular values can be computed with the corresponding */
/* > gradual loss of accurate digits. */
/* > \endverbatim */

/* > \par Contributor: */
/*  ================== */
/* > */
/* > \verbatim */
/* > */
/* >  ============ */
/* > */
/* >  Zlatko Drmac (Zagreb, Croatia) */
/* > */
/* > \endverbatim */

/* > \par References: */
/*  ================ */
/* > */
/* > \verbatim */
/* > */
/* > [1] P. P. M. De Rijk: A one-sided Jacobi algorithm for computing the */
/* >    singular value decomposition on a vector computer. */
/* >    SIAM J. Sci. Stat. Comp., Vol. 10 (1998), pp. 359-371. */
/* > [2] J. Demmel and K. Veselic: Jacobi method is more accurate than QR. */
/* > [3] Z. Drmac: Implementation of Jacobi rotations for accurate singular */
/* >    value computation in floating point arithmetic. */
/* >    SIAM J. Sci. Comp., Vol. 18 (1997), pp. 1200-1222. */
/* > [4] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm I. */
/* >    SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1322-1342. */
/* >    LAPACK Working note 169. */
/* > [5] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm II. */
/* >    SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1343-1362. */
/* >    LAPACK Working note 170. */
/* > [6] Z. Drmac: SIGMA - mathematical software library for accurate SVD, PSV, */
/* >    QSVD, (H,K)-SVD computations. */
/* >    Department of Mathematics, University of Zagreb, 2008, 2015. */
/* > \endverbatim */

/* > \par Bugs, examples and comments: */
/*  ================================= */
/* > */
/* > \verbatim */
/* >  =========================== */
/* >  Please report all bugs and send interesting test examples and comments to */
/* >  drmac@math.hr. Thank you. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void cgesvj_(char *joba, char *jobu, char *jobv, integer *m, 
	integer *n, complex *a, integer *lda, real *sva, integer *mv, complex 
	*v, integer *ldv, complex *cwork, integer *lwork, real *rwork, 
	integer *lrwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, v_dim1, v_offset, i__1, i__2, i__3, i__4, i__5, 
	    i__6;
    real r__1, r__2;
    complex q__1, q__2, q__3;

    /* Local variables */
    real aapp;
    complex aapq;
    real aaqq, ctol;
    integer ierr;
    real bigtheta;
    extern /* Subroutine */ void crot_(integer *, complex *, integer *, 
	    complex *, integer *, real *, complex *);
    complex ompq;
    integer pskipped;
    real aapp0, aapq1, temp1;
    integer i__, p, q;
    real t;
    extern /* Complex */ VOID cdotc_(complex *, integer *, complex *, integer 
	    *, complex *, integer *);
    real apoaq, aqoap;
    extern logical lsame_(char *, char *);
    real theta, small, sfmin;
    logical lsvec;
    extern /* Subroutine */ void ccopy_(integer *, complex *, integer *, 
	    complex *, integer *), cswap_(integer *, complex *, integer *, 
	    complex *, integer *);
    real epsln;
    logical applv, rsvec, uctol;
    extern /* Subroutine */ void caxpy_(integer *, complex *, complex *, 
	    integer *, complex *, integer *);
    logical lower, upper, rotok;
    integer n2, n4;
    extern /* Subroutine */ void cgsvj0_(char *, integer *, integer *, complex 
	    *, integer *, complex *, real *, integer *, complex *, integer *, 
	    real *, real *, real *, integer *, complex *, integer *, integer *
	    ), cgsvj1_(char *, integer *, integer *, integer *, 
	    complex *, integer *, complex *, real *, integer *, complex *, 
	    integer *, real *, real *, real *, integer *, complex *, integer *
	    , integer *);
    real rootsfmin;
    extern real scnrm2_(integer *, complex *, integer *);
    integer n34;
    real cs, sn;
    extern /* Subroutine */ void clascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, complex *, integer *, integer *);
    extern real slamch_(char *);
    extern /* Subroutine */ void csscal_(integer *, real *, complex *, integer 
	    *), claset_(char *, integer *, integer *, complex *, complex *, 
	    complex *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    integer ijblsk, swband;
    extern integer isamax_(integer *, real *, integer *);
    extern /* Subroutine */ void slascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, real *, integer *, integer *);
    integer blskip;
    extern /* Subroutine */ void classq_(integer *, complex *, integer *, real 
	    *, real *);
    real mxaapq, thsign, mxsinj;
    integer ir1, emptsw;
    logical lquery;
    integer notrot, iswrot, jbc;
    real big;
    integer kbl, lkahead, igl, ibr, jgl, nbl;
    real skl;
    logical goscale;
    real tol;
    integer mvl;
    logical noscale;
    real rootbig, rooteps;
    integer rowskip;
    real roottol;


/*  -- LAPACK computational routine (version 3.8.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */

/*     from BLAS */
/*     from LAPACK */
/*     from BLAS */
/*     from LAPACK */

/*     Test the input arguments */

    /* Parameter adjustments */
    --sva;
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    --cwork;
    --rwork;

    /* Function Body */
    lsvec = lsame_(jobu, "U") || lsame_(jobu, "F");
    uctol = lsame_(jobu, "C");
    rsvec = lsame_(jobv, "V") || lsame_(jobv, "J");
    applv = lsame_(jobv, "A");
    upper = lsame_(joba, "U");
    lower = lsame_(joba, "L");

    lquery = *lwork == -1 || *lrwork == -1;
    if (! (upper || lower || lsame_(joba, "G"))) {
	*info = -1;
    } else if (! (lsvec || uctol || lsame_(jobu, "N"))) 
	    {
	*info = -2;
    } else if (! (rsvec || applv || lsame_(jobv, "N"))) 
	    {
	*info = -3;
    } else if (*m < 0) {
	*info = -4;
    } else if (*n < 0 || *n > *m) {
	*info = -5;
    } else if (*lda < *m) {
	*info = -7;
    } else if (*mv < 0) {
	*info = -9;
    } else if (rsvec && *ldv < *n || applv && *ldv < *mv) {
	*info = -11;
    } else if (uctol && rwork[1] <= 1.f) {
	*info = -12;
    } else if (*lwork < *m + *n && ! lquery) {
	*info = -13;
    } else if (*lrwork < f2cmax(*n,6) && ! lquery) {
	*info = -15;
    } else {
	*info = 0;
    }

/*     #:( */
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CGESVJ", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	i__1 = *m + *n;
	cwork[1].r = (real) i__1, cwork[1].i = 0.f;
	rwork[1] = (real) f2cmax(*n,6);
	return;
    }

/* #:) Quick return for void matrix */

    if (*m == 0 || *n == 0) {
	return;
    }

/*     Set numerical parameters */
/*     The stopping criterion for Jacobi rotations is */

/*     max_{i<>j}|A(:,i)^* * A(:,j)| / (||A(:,i)||*||A(:,j)||) < CTOL*EPS */

/*     where EPS is the round-off and CTOL is defined as follows: */

    if (uctol) {
/*        ... user controlled */
	ctol = rwork[1];
    } else {
/*        ... default */
	if (lsvec || rsvec || applv) {
	    ctol = sqrt((real) (*m));
	} else {
	    ctol = (real) (*m);
	}
    }
/*     ... and the machine dependent parameters are */
/* [!]  (Make sure that SLAMCH() works properly on the target machine.) */

    epsln = slamch_("Epsilon");
    rooteps = sqrt(epsln);
    sfmin = slamch_("SafeMinimum");
    rootsfmin = sqrt(sfmin);
    small = sfmin / epsln;
/*      BIG = SLAMCH( 'Overflow' ) */
    big = 1.f / sfmin;
    rootbig = 1.f / rootsfmin;
/*     LARGE = BIG / SQRT( REAL( M*N ) ) */
    bigtheta = 1.f / rooteps;

    tol = ctol * epsln;
    roottol = sqrt(tol);

    if ((real) (*m) * epsln >= 1.f) {
	*info = -4;
	i__1 = -(*info);
	xerbla_("CGESVJ", &i__1, (ftnlen)6);
	return;
    }

/*     Initialize the right singular vector matrix. */

    if (rsvec) {
	mvl = *n;
	claset_("A", &mvl, n, &c_b1, &c_b2, &v[v_offset], ldv);
    } else if (applv) {
	mvl = *mv;
    }
    rsvec = rsvec || applv;

/*     Initialize SVA( 1:N ) = ( ||A e_i||_2, i = 1:N ) */
/* (!)  If necessary, scale A to protect the largest singular value */
/*     from overflow. It is possible that saving the largest singular */
/*     value destroys the information about the small ones. */
/*     This initial scaling is almost minimal in the sense that the */
/*     goal is to make sure that no column norm overflows, and that */
/*     SQRT(N)*max_i SVA(i) does not overflow. If INFinite entries */
/*     in A are detected, the procedure returns with INFO=-6. */

    skl = 1.f / sqrt((real) (*m) * (real) (*n));
    noscale = TRUE_;
    goscale = TRUE_;

    if (lower) {
/*        the input matrix is M-by-N lower triangular (trapezoidal) */
	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
	    aapp = 0.f;
	    aaqq = 1.f;
	    i__2 = *m - p + 1;
	    classq_(&i__2, &a[p + p * a_dim1], &c__1, &aapp, &aaqq);
	    if (aapp > big) {
		*info = -6;
		i__2 = -(*info);
		xerbla_("CGESVJ", &i__2, (ftnlen)6);
		return;
	    }
	    aaqq = sqrt(aaqq);
	    if (aapp < big / aaqq && noscale) {
		sva[p] = aapp * aaqq;
	    } else {
		noscale = FALSE_;
		sva[p] = aapp * (aaqq * skl);
		if (goscale) {
		    goscale = FALSE_;
		    i__2 = p - 1;
		    for (q = 1; q <= i__2; ++q) {
			sva[q] *= skl;
/* L1873: */
		    }
		}
	    }
/* L1874: */
	}
    } else if (upper) {
/*        the input matrix is M-by-N upper triangular (trapezoidal) */
	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
	    aapp = 0.f;
	    aaqq = 1.f;
	    classq_(&p, &a[p * a_dim1 + 1], &c__1, &aapp, &aaqq);
	    if (aapp > big) {
		*info = -6;
		i__2 = -(*info);
		xerbla_("CGESVJ", &i__2, (ftnlen)6);
		return;
	    }
	    aaqq = sqrt(aaqq);
	    if (aapp < big / aaqq && noscale) {
		sva[p] = aapp * aaqq;
	    } else {
		noscale = FALSE_;
		sva[p] = aapp * (aaqq * skl);
		if (goscale) {
		    goscale = FALSE_;
		    i__2 = p - 1;
		    for (q = 1; q <= i__2; ++q) {
			sva[q] *= skl;
/* L2873: */
		    }
		}
	    }
/* L2874: */
	}
    } else {
/*        the input matrix is M-by-N general dense */
	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
	    aapp = 0.f;
	    aaqq = 1.f;
	    classq_(m, &a[p * a_dim1 + 1], &c__1, &aapp, &aaqq);
	    if (aapp > big) {
		*info = -6;
		i__2 = -(*info);
		xerbla_("CGESVJ", &i__2, (ftnlen)6);
		return;
	    }
	    aaqq = sqrt(aaqq);
	    if (aapp < big / aaqq && noscale) {
		sva[p] = aapp * aaqq;
	    } else {
		noscale = FALSE_;
		sva[p] = aapp * (aaqq * skl);
		if (goscale) {
		    goscale = FALSE_;
		    i__2 = p - 1;
		    for (q = 1; q <= i__2; ++q) {
			sva[q] *= skl;
/* L3873: */
		    }
		}
	    }
/* L3874: */
	}
    }

    if (noscale) {
	skl = 1.f;
    }

/*     Move the smaller part of the spectrum from the underflow threshold */
/* (!)  Start by determining the position of the nonzero entries of the */
/*     array SVA() relative to ( SFMIN, BIG ). */

    aapp = 0.f;
    aaqq = big;
    i__1 = *n;
    for (p = 1; p <= i__1; ++p) {
	if (sva[p] != 0.f) {
/* Computing MIN */
	    r__1 = aaqq, r__2 = sva[p];
	    aaqq = f2cmin(r__1,r__2);
	}
/* Computing MAX */
	r__1 = aapp, r__2 = sva[p];
	aapp = f2cmax(r__1,r__2);
/* L4781: */
    }

/* #:) Quick return for zero matrix */

    if (aapp == 0.f) {
	if (lsvec) {
	    claset_("G", m, n, &c_b1, &c_b2, &a[a_offset], lda);
	}
	rwork[1] = 1.f;
	rwork[2] = 0.f;
	rwork[3] = 0.f;
	rwork[4] = 0.f;
	rwork[5] = 0.f;
	rwork[6] = 0.f;
	return;
    }

/* #:) Quick return for one-column matrix */

    if (*n == 1) {
	if (lsvec) {
	    clascl_("G", &c__0, &c__0, &sva[1], &skl, m, &c__1, &a[a_dim1 + 1]
		    , lda, &ierr);
	}
	rwork[1] = 1.f / skl;
	if (sva[1] >= sfmin) {
	    rwork[2] = 1.f;
	} else {
	    rwork[2] = 0.f;
	}
	rwork[3] = 0.f;
	rwork[4] = 0.f;
	rwork[5] = 0.f;
	rwork[6] = 0.f;
	return;
    }

/*     Protect small singular values from underflow, and try to */
/*     avoid underflows/overflows in computing Jacobi rotations. */

    sn = sqrt(sfmin / epsln);
    temp1 = sqrt(big / (real) (*n));
    if (aapp <= sn || aaqq >= temp1 || sn <= aaqq && aapp <= temp1) {
/* Computing MIN */
	r__1 = big, r__2 = temp1 / aapp;
	temp1 = f2cmin(r__1,r__2);
/*         AAQQ  = AAQQ*TEMP1 */
/*         AAPP  = AAPP*TEMP1 */
    } else if (aaqq <= sn && aapp <= temp1) {
/* Computing MIN */
	r__1 = sn / aaqq, r__2 = big / (aapp * sqrt((real) (*n)));
	temp1 = f2cmin(r__1,r__2);
/*         AAQQ  = AAQQ*TEMP1 */
/*         AAPP  = AAPP*TEMP1 */
    } else if (aaqq >= sn && aapp >= temp1) {
/* Computing MAX */
	r__1 = sn / aaqq, r__2 = temp1 / aapp;
	temp1 = f2cmax(r__1,r__2);
/*         AAQQ  = AAQQ*TEMP1 */
/*         AAPP  = AAPP*TEMP1 */
    } else if (aaqq <= sn && aapp >= temp1) {
/* Computing MIN */
	r__1 = sn / aaqq, r__2 = big / (sqrt((real) (*n)) * aapp);
	temp1 = f2cmin(r__1,r__2);
/*         AAQQ  = AAQQ*TEMP1 */
/*         AAPP  = AAPP*TEMP1 */
    } else {
	temp1 = 1.f;
    }

/*     Scale, if necessary */

    if (temp1 != 1.f) {
	slascl_("G", &c__0, &c__0, &c_b41, &temp1, n, &c__1, &sva[1], n, &
		ierr);
    }
    skl = temp1 * skl;
    if (skl != 1.f) {
	clascl_(joba, &c__0, &c__0, &c_b41, &skl, m, n, &a[a_offset], lda, &
		ierr);
	skl = 1.f / skl;
    }

/*     Row-cyclic Jacobi SVD algorithm with column pivoting */

    emptsw = *n * (*n - 1) / 2;
    notrot = 0;
    i__1 = *n;
    for (q = 1; q <= i__1; ++q) {
	i__2 = q;
	cwork[i__2].r = 1.f, cwork[i__2].i = 0.f;
/* L1868: */
    }



    swband = 3;
/* [TP] SWBAND is a tuning parameter [TP]. It is meaningful and effective */
/*     if CGESVJ is used as a computational routine in the preconditioned */
/*     Jacobi SVD algorithm CGEJSV. For sweeps i=1:SWBAND the procedure */
/*     works on pivots inside a band-like region around the diagonal. */
/*     The boundaries are determined dynamically, based on the number of */
/*     pivots above a threshold. */

    kbl = f2cmin(8,*n);
/* [TP] KBL is a tuning parameter that defines the tile size in the */
/*     tiling of the p-q loops of pivot pairs. In general, an optimal */
/*     value of KBL depends on the matrix dimensions and on the */
/*     parameters of the computer's memory. */

    nbl = *n / kbl;
    if (nbl * kbl != *n) {
	++nbl;
    }

/* Computing 2nd power */
    i__1 = kbl;
    blskip = i__1 * i__1;
/* [TP] BLKSKIP is a tuning parameter that depends on SWBAND and KBL. */

    rowskip = f2cmin(5,kbl);
/* [TP] ROWSKIP is a tuning parameter. */

    lkahead = 1;
/* [TP] LKAHEAD is a tuning parameter. */

/*     Quasi block transformations, using the lower (upper) triangular */
/*     structure of the input matrix. The quasi-block-cycling usually */
/*     invokes cubic convergence. Big part of this cycle is done inside */
/*     canonical subspaces of dimensions less than M. */

/* Computing MAX */
    i__1 = 64, i__2 = kbl << 2;
    if ((lower || upper) && *n > f2cmax(i__1,i__2)) {
/* [TP] The number of partition levels and the actual partition are */
/*     tuning parameters. */
	n4 = *n / 4;
	n2 = *n / 2;
	n34 = n4 * 3;
	if (applv) {
	    q = 0;
	} else {
	    q = 1;
	}

	if (lower) {

/*     This works very well on lower triangular matrices, in particular */
/*     in the framework of the preconditioned Jacobi SVD (xGEJSV). */
/*     The idea is simple: */
/*     [+ 0 0 0]   Note that Jacobi transformations of [0 0] */
/*     [+ + 0 0]                                       [0 0] */
/*     [+ + x 0]   actually work on [x 0]              [x 0] */
/*     [+ + x x]                    [x x].             [x x] */

	    i__1 = *m - n34;
	    i__2 = *n - n34;
	    i__3 = *lwork - *n;
	    cgsvj0_(jobv, &i__1, &i__2, &a[n34 + 1 + (n34 + 1) * a_dim1], lda,
		     &cwork[n34 + 1], &sva[n34 + 1], &mvl, &v[n34 * q + 1 + (
		    n34 + 1) * v_dim1], ldv, &epsln, &sfmin, &tol, &c__2, &
		    cwork[*n + 1], &i__3, &ierr);
	    i__1 = *m - n2;
	    i__2 = n34 - n2;
	    i__3 = *lwork - *n;
	    cgsvj0_(jobv, &i__1, &i__2, &a[n2 + 1 + (n2 + 1) * a_dim1], lda, &
		    cwork[n2 + 1], &sva[n2 + 1], &mvl, &v[n2 * q + 1 + (n2 + 
		    1) * v_dim1], ldv, &epsln, &sfmin, &tol, &c__2, &cwork[*n 
		    + 1], &i__3, &ierr);
	    i__1 = *m - n2;
	    i__2 = *n - n2;
	    i__3 = *lwork - *n;
	    cgsvj1_(jobv, &i__1, &i__2, &n4, &a[n2 + 1 + (n2 + 1) * a_dim1], 
		    lda, &cwork[n2 + 1], &sva[n2 + 1], &mvl, &v[n2 * q + 1 + (
		    n2 + 1) * v_dim1], ldv, &epsln, &sfmin, &tol, &c__1, &
		    cwork[*n + 1], &i__3, &ierr);

	    i__1 = *m - n4;
	    i__2 = n2 - n4;
	    i__3 = *lwork - *n;
	    cgsvj0_(jobv, &i__1, &i__2, &a[n4 + 1 + (n4 + 1) * a_dim1], lda, &
		    cwork[n4 + 1], &sva[n4 + 1], &mvl, &v[n4 * q + 1 + (n4 + 
		    1) * v_dim1], ldv, &epsln, &sfmin, &tol, &c__1, &cwork[*n 
		    + 1], &i__3, &ierr);

	    i__1 = *lwork - *n;
	    cgsvj0_(jobv, m, &n4, &a[a_offset], lda, &cwork[1], &sva[1], &mvl,
		     &v[v_offset], ldv, &epsln, &sfmin, &tol, &c__1, &cwork[*
		    n + 1], &i__1, &ierr);

	    i__1 = *lwork - *n;
	    cgsvj1_(jobv, m, &n2, &n4, &a[a_offset], lda, &cwork[1], &sva[1], 
		    &mvl, &v[v_offset], ldv, &epsln, &sfmin, &tol, &c__1, &
		    cwork[*n + 1], &i__1, &ierr);


	} else if (upper) {


	    i__1 = *lwork - *n;
	    cgsvj0_(jobv, &n4, &n4, &a[a_offset], lda, &cwork[1], &sva[1], &
		    mvl, &v[v_offset], ldv, &epsln, &sfmin, &tol, &c__2, &
		    cwork[*n + 1], &i__1, &ierr);

	    i__1 = *lwork - *n;
	    cgsvj0_(jobv, &n2, &n4, &a[(n4 + 1) * a_dim1 + 1], lda, &cwork[n4 
		    + 1], &sva[n4 + 1], &mvl, &v[n4 * q + 1 + (n4 + 1) * 
		    v_dim1], ldv, &epsln, &sfmin, &tol, &c__1, &cwork[*n + 1],
		     &i__1, &ierr);

	    i__1 = *lwork - *n;
	    cgsvj1_(jobv, &n2, &n2, &n4, &a[a_offset], lda, &cwork[1], &sva[1]
		    , &mvl, &v[v_offset], ldv, &epsln, &sfmin, &tol, &c__1, &
		    cwork[*n + 1], &i__1, &ierr);

	    i__1 = n2 + n4;
	    i__2 = *lwork - *n;
	    cgsvj0_(jobv, &i__1, &n4, &a[(n2 + 1) * a_dim1 + 1], lda, &cwork[
		    n2 + 1], &sva[n2 + 1], &mvl, &v[n2 * q + 1 + (n2 + 1) * 
		    v_dim1], ldv, &epsln, &sfmin, &tol, &c__1, &cwork[*n + 1],
		     &i__2, &ierr);
	}

    }


    for (i__ = 1; i__ <= 30; ++i__) {


	mxaapq = 0.f;
	mxsinj = 0.f;
	iswrot = 0;

	notrot = 0;
	pskipped = 0;

/*     Each sweep is unrolled using KBL-by-KBL tiles over the pivot pairs */
/*     1 <= p < q <= N. This is the first step toward a blocked implementation */
/*     of the rotations. New implementation, based on block transformations, */
/*     is under development. */

	i__1 = nbl;
	for (ibr = 1; ibr <= i__1; ++ibr) {

	    igl = (ibr - 1) * kbl + 1;

/* Computing MIN */
	    i__3 = lkahead, i__4 = nbl - ibr;
	    i__2 = f2cmin(i__3,i__4);
	    for (ir1 = 0; ir1 <= i__2; ++ir1) {

		igl += ir1 * kbl;

/* Computing MIN */
		i__4 = igl + kbl - 1, i__5 = *n - 1;
		i__3 = f2cmin(i__4,i__5);
		for (p = igl; p <= i__3; ++p) {


		    i__4 = *n - p + 1;
		    q = isamax_(&i__4, &sva[p], &c__1) + p - 1;
		    if (p != q) {
			cswap_(m, &a[p * a_dim1 + 1], &c__1, &a[q * a_dim1 + 
				1], &c__1);
			if (rsvec) {
			    cswap_(&mvl, &v[p * v_dim1 + 1], &c__1, &v[q * 
				    v_dim1 + 1], &c__1);
			}
			temp1 = sva[p];
			sva[p] = sva[q];
			sva[q] = temp1;
			i__4 = p;
			aapq.r = cwork[i__4].r, aapq.i = cwork[i__4].i;
			i__4 = p;
			i__5 = q;
			cwork[i__4].r = cwork[i__5].r, cwork[i__4].i = cwork[
				i__5].i;
			i__4 = q;
			cwork[i__4].r = aapq.r, cwork[i__4].i = aapq.i;
		    }

		    if (ir1 == 0) {

/*        Column norms are periodically updated by explicit */
/*        norm computation. */
/* [!]     Caveat: */
/*        Unfortunately, some BLAS implementations compute SCNRM2(M,A(1,p),1) */
/*        as SQRT(S=CDOTC(M,A(1,p),1,A(1,p),1)), which may cause the result to */
/*        overflow for ||A(:,p)||_2 > SQRT(overflow_threshold), and to */
/*        underflow for ||A(:,p)||_2 < SQRT(underflow_threshold). */
/*        Hence, SCNRM2 cannot be trusted, not even in the case when */
/*        the true norm is far from the under(over)flow boundaries. */
/*        If properly implemented SCNRM2 is available, the IF-THEN-ELSE-END IF */
/*        below should be replaced with "AAPP = SCNRM2( M, A(1,p), 1 )". */

			if (sva[p] < rootbig && sva[p] > rootsfmin) {
			    sva[p] = scnrm2_(m, &a[p * a_dim1 + 1], &c__1);
			} else {
			    temp1 = 0.f;
			    aapp = 1.f;
			    classq_(m, &a[p * a_dim1 + 1], &c__1, &temp1, &
				    aapp);
			    sva[p] = temp1 * sqrt(aapp);
			}
			aapp = sva[p];
		    } else {
			aapp = sva[p];
		    }

		    if (aapp > 0.f) {

			pskipped = 0;

/* Computing MIN */
			i__5 = igl + kbl - 1;
			i__4 = f2cmin(i__5,*n);
			for (q = p + 1; q <= i__4; ++q) {

			    aaqq = sva[q];

			    if (aaqq > 0.f) {

				aapp0 = aapp;
				if (aaqq >= 1.f) {
				    rotok = small * aapp <= aaqq;
				    if (aapp < big / aaqq) {
					cdotc_(&q__3, m, &a[p * a_dim1 + 1], &
						c__1, &a[q * a_dim1 + 1], &
						c__1);
					q__2.r = q__3.r / aaqq, q__2.i = 
						q__3.i / aaqq;
					q__1.r = q__2.r / aapp, q__1.i = 
						q__2.i / aapp;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    } else {
					ccopy_(m, &a[p * a_dim1 + 1], &c__1, &
						cwork[*n + 1], &c__1);
					clascl_("G", &c__0, &c__0, &aapp, &
						c_b41, m, &c__1, &cwork[*n + 
						1], lda, &ierr);
					cdotc_(&q__2, m, &cwork[*n + 1], &
						c__1, &a[q * a_dim1 + 1], &
						c__1);
					q__1.r = q__2.r / aaqq, q__1.i = 
						q__2.i / aaqq;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    }
				} else {
				    rotok = aapp <= aaqq / small;
				    if (aapp > small / aaqq) {
					cdotc_(&q__3, m, &a[p * a_dim1 + 1], &
						c__1, &a[q * a_dim1 + 1], &
						c__1);
					q__2.r = q__3.r / aapp, q__2.i = 
						q__3.i / aapp;
					q__1.r = q__2.r / aaqq, q__1.i = 
						q__2.i / aaqq;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    } else {
					ccopy_(m, &a[q * a_dim1 + 1], &c__1, &
						cwork[*n + 1], &c__1);
					clascl_("G", &c__0, &c__0, &aaqq, &
						c_b41, m, &c__1, &cwork[*n + 
						1], lda, &ierr);
					cdotc_(&q__2, m, &a[p * a_dim1 + 1], &
						c__1, &cwork[*n + 1], &c__1);
					q__1.r = q__2.r / aapp, q__1.i = 
						q__2.i / aapp;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    }
				}

/*                           AAPQ = AAPQ * CONJG( CWORK(p) ) * CWORK(q) */
				aapq1 = -c_abs(&aapq);
/* Computing MAX */
				r__1 = mxaapq, r__2 = -aapq1;
				mxaapq = f2cmax(r__1,r__2);

/*        TO rotate or NOT to rotate, THAT is the question ... */

				if (abs(aapq1) > tol) {
				    r__1 = c_abs(&aapq);
				    q__1.r = aapq.r / r__1, q__1.i = aapq.i / 
					    r__1;
				    ompq.r = q__1.r, ompq.i = q__1.i;

/* [RTD]      ROTATED = ROTATED + ONE */

				    if (ir1 == 0) {
					notrot = 0;
					pskipped = 0;
					++iswrot;
				    }

				    if (rotok) {

					aqoap = aaqq / aapp;
					apoaq = aapp / aaqq;
					theta = (r__1 = aqoap - apoaq, abs(
						r__1)) * -.5f / aapq1;

					if (abs(theta) > bigtheta) {

					    t = .5f / theta;
					    cs = 1.f;
					    r_cnjg(&q__2, &ompq);
					    q__1.r = t * q__2.r, q__1.i = t * 
						    q__2.i;
					    crot_(m, &a[p * a_dim1 + 1], &
						    c__1, &a[q * a_dim1 + 1], 
						    &c__1, &cs, &q__1);
					    if (rsvec) {
			  r_cnjg(&q__2, &ompq);
			  q__1.r = t * q__2.r, q__1.i = t * q__2.i;
			  crot_(&mvl, &v[p * v_dim1 + 1], &c__1, &v[q * 
				  v_dim1 + 1], &c__1, &cs, &q__1);
					    }
/* Computing MAX */
					    r__1 = 0.f, r__2 = t * apoaq * 
						    aapq1 + 1.f;
					    sva[q] = aaqq * sqrt((f2cmax(r__1,
						    r__2)));
/* Computing MAX */
					    r__1 = 0.f, r__2 = 1.f - t * 
						    aqoap * aapq1;
					    aapp *= sqrt((f2cmax(r__1,r__2)));
/* Computing MAX */
					    r__1 = mxsinj, r__2 = abs(t);
					    mxsinj = f2cmax(r__1,r__2);

					} else {


					    thsign = -r_sign(&c_b41, &aapq1);
					    t = 1.f / (theta + thsign * sqrt(
						    theta * theta + 1.f));
					    cs = sqrt(1.f / (t * t + 1.f));
					    sn = t * cs;

/* Computing MAX */
					    r__1 = mxsinj, r__2 = abs(sn);
					    mxsinj = f2cmax(r__1,r__2);
/* Computing MAX */
					    r__1 = 0.f, r__2 = t * apoaq * 
						    aapq1 + 1.f;
					    sva[q] = aaqq * sqrt((f2cmax(r__1,
						    r__2)));
/* Computing MAX */
					    r__1 = 0.f, r__2 = 1.f - t * 
						    aqoap * aapq1;
					    aapp *= sqrt((f2cmax(r__1,r__2)));

					    r_cnjg(&q__2, &ompq);
					    q__1.r = sn * q__2.r, q__1.i = sn 
						    * q__2.i;
					    crot_(m, &a[p * a_dim1 + 1], &
						    c__1, &a[q * a_dim1 + 1], 
						    &c__1, &cs, &q__1);
					    if (rsvec) {
			  r_cnjg(&q__2, &ompq);
			  q__1.r = sn * q__2.r, q__1.i = sn * q__2.i;
			  crot_(&mvl, &v[p * v_dim1 + 1], &c__1, &v[q * 
				  v_dim1 + 1], &c__1, &cs, &q__1);
					    }
					}
					i__5 = p;
					i__6 = q;
					q__2.r = -cwork[i__6].r, q__2.i = 
						-cwork[i__6].i;
					q__1.r = q__2.r * ompq.r - q__2.i * 
						ompq.i, q__1.i = q__2.r * 
						ompq.i + q__2.i * ompq.r;
					cwork[i__5].r = q__1.r, cwork[i__5].i 
						= q__1.i;

				    } else {
					ccopy_(m, &a[p * a_dim1 + 1], &c__1, &
						cwork[*n + 1], &c__1);
					clascl_("G", &c__0, &c__0, &aapp, &
						c_b41, m, &c__1, &cwork[*n + 
						1], lda, &ierr);
					clascl_("G", &c__0, &c__0, &aaqq, &
						c_b41, m, &c__1, &a[q * 
						a_dim1 + 1], lda, &ierr);
					q__1.r = -aapq.r, q__1.i = -aapq.i;
					caxpy_(m, &q__1, &cwork[*n + 1], &
						c__1, &a[q * a_dim1 + 1], &
						c__1);
					clascl_("G", &c__0, &c__0, &c_b41, &
						aaqq, m, &c__1, &a[q * a_dim1 
						+ 1], lda, &ierr);
/* Computing MAX */
					r__1 = 0.f, r__2 = 1.f - aapq1 * 
						aapq1;
					sva[q] = aaqq * sqrt((f2cmax(r__1,r__2)))
						;
					mxsinj = f2cmax(mxsinj,sfmin);
				    }
/*           END IF ROTOK THEN ... ELSE */

/*           In the case of cancellation in updating SVA(q), SVA(p) */
/*           recompute SVA(q), SVA(p). */

/* Computing 2nd power */
				    r__1 = sva[q] / aaqq;
				    if (r__1 * r__1 <= rooteps) {
					if (aaqq < rootbig && aaqq > 
						rootsfmin) {
					    sva[q] = scnrm2_(m, &a[q * a_dim1 
						    + 1], &c__1);
					} else {
					    t = 0.f;
					    aaqq = 1.f;
					    classq_(m, &a[q * a_dim1 + 1], &
						    c__1, &t, &aaqq);
					    sva[q] = t * sqrt(aaqq);
					}
				    }
				    if (aapp / aapp0 <= rooteps) {
					if (aapp < rootbig && aapp > 
						rootsfmin) {
					    aapp = scnrm2_(m, &a[p * a_dim1 + 
						    1], &c__1);
					} else {
					    t = 0.f;
					    aapp = 1.f;
					    classq_(m, &a[p * a_dim1 + 1], &
						    c__1, &t, &aapp);
					    aapp = t * sqrt(aapp);
					}
					sva[p] = aapp;
				    }

				} else {
/*                             A(:,p) and A(:,q) already numerically orthogonal */
				    if (ir1 == 0) {
					++notrot;
				    }
/* [RTD]      SKIPPED  = SKIPPED + 1 */
				    ++pskipped;
				}
			    } else {
/*                          A(:,q) is zero column */
				if (ir1 == 0) {
				    ++notrot;
				}
				++pskipped;
			    }

			    if (i__ <= swband && pskipped > rowskip) {
				if (ir1 == 0) {
				    aapp = -aapp;
				}
				notrot = 0;
				goto L2103;
			    }

/* L2002: */
			}
/*     END q-LOOP */

L2103:
/*     bailed out of q-loop */

			sva[p] = aapp;

		    } else {
			sva[p] = aapp;
			if (ir1 == 0 && aapp == 0.f) {
/* Computing MIN */
			    i__4 = igl + kbl - 1;
			    notrot = notrot + f2cmin(i__4,*n) - p;
			}
		    }

/* L2001: */
		}
/*     end of the p-loop */
/*     end of doing the block ( ibr, ibr ) */
/* L1002: */
	    }
/*     end of ir1-loop */

/* ... go to the off diagonal blocks */

	    igl = (ibr - 1) * kbl + 1;

	    i__2 = nbl;
	    for (jbc = ibr + 1; jbc <= i__2; ++jbc) {

		jgl = (jbc - 1) * kbl + 1;

/*        doing the block at ( ibr, jbc ) */

		ijblsk = 0;
/* Computing MIN */
		i__4 = igl + kbl - 1;
		i__3 = f2cmin(i__4,*n);
		for (p = igl; p <= i__3; ++p) {

		    aapp = sva[p];
		    if (aapp > 0.f) {

			pskipped = 0;

/* Computing MIN */
			i__5 = jgl + kbl - 1;
			i__4 = f2cmin(i__5,*n);
			for (q = jgl; q <= i__4; ++q) {

			    aaqq = sva[q];
			    if (aaqq > 0.f) {
				aapp0 = aapp;


/*        Safe Gram matrix computation */

				if (aaqq >= 1.f) {
				    if (aapp >= aaqq) {
					rotok = small * aapp <= aaqq;
				    } else {
					rotok = small * aaqq <= aapp;
				    }
				    if (aapp < big / aaqq) {
					cdotc_(&q__3, m, &a[p * a_dim1 + 1], &
						c__1, &a[q * a_dim1 + 1], &
						c__1);
					q__2.r = q__3.r / aaqq, q__2.i = 
						q__3.i / aaqq;
					q__1.r = q__2.r / aapp, q__1.i = 
						q__2.i / aapp;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    } else {
					ccopy_(m, &a[p * a_dim1 + 1], &c__1, &
						cwork[*n + 1], &c__1);
					clascl_("G", &c__0, &c__0, &aapp, &
						c_b41, m, &c__1, &cwork[*n + 
						1], lda, &ierr);
					cdotc_(&q__2, m, &cwork[*n + 1], &
						c__1, &a[q * a_dim1 + 1], &
						c__1);
					q__1.r = q__2.r / aaqq, q__1.i = 
						q__2.i / aaqq;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    }
				} else {
				    if (aapp >= aaqq) {
					rotok = aapp <= aaqq / small;
				    } else {
					rotok = aaqq <= aapp / small;
				    }
				    if (aapp > small / aaqq) {
					cdotc_(&q__3, m, &a[p * a_dim1 + 1], &
						c__1, &a[q * a_dim1 + 1], &
						c__1);
					r__1 = f2cmax(aaqq,aapp);
					q__2.r = q__3.r / r__1, q__2.i = 
						q__3.i / r__1;
					r__2 = f2cmin(aaqq,aapp);
					q__1.r = q__2.r / r__2, q__1.i = 
						q__2.i / r__2;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    } else {
					ccopy_(m, &a[q * a_dim1 + 1], &c__1, &
						cwork[*n + 1], &c__1);
					clascl_("G", &c__0, &c__0, &aaqq, &
						c_b41, m, &c__1, &cwork[*n + 
						1], lda, &ierr);
					cdotc_(&q__2, m, &a[p * a_dim1 + 1], &
						c__1, &cwork[*n + 1], &c__1);
					q__1.r = q__2.r / aapp, q__1.i = 
						q__2.i / aapp;
					aapq.r = q__1.r, aapq.i = q__1.i;
				    }
				}

/*                           AAPQ = AAPQ * CONJG(CWORK(p))*CWORK(q) */
				aapq1 = -c_abs(&aapq);
/* Computing MAX */
				r__1 = mxaapq, r__2 = -aapq1;
				mxaapq = f2cmax(r__1,r__2);

/*        TO rotate or NOT to rotate, THAT is the question ... */

				if (abs(aapq1) > tol) {
				    r__1 = c_abs(&aapq);
				    q__1.r = aapq.r / r__1, q__1.i = aapq.i / 
					    r__1;
				    ompq.r = q__1.r, ompq.i = q__1.i;
				    notrot = 0;
/* [RTD]      ROTATED  = ROTATED + 1 */
				    pskipped = 0;
				    ++iswrot;

				    if (rotok) {

					aqoap = aaqq / aapp;
					apoaq = aapp / aaqq;
					theta = (r__1 = aqoap - apoaq, abs(
						r__1)) * -.5f / aapq1;
					if (aaqq > aapp0) {
					    theta = -theta;
					}

					if (abs(theta) > bigtheta) {
					    t = .5f / theta;
					    cs = 1.f;
					    r_cnjg(&q__2, &ompq);
					    q__1.r = t * q__2.r, q__1.i = t * 
						    q__2.i;
					    crot_(m, &a[p * a_dim1 + 1], &
						    c__1, &a[q * a_dim1 + 1], 
						    &c__1, &cs, &q__1);
					    if (rsvec) {
			  r_cnjg(&q__2, &ompq);
			  q__1.r = t * q__2.r, q__1.i = t * q__2.i;
			  crot_(&mvl, &v[p * v_dim1 + 1], &c__1, &v[q * 
				  v_dim1 + 1], &c__1, &cs, &q__1);
					    }
/* Computing MAX */
					    r__1 = 0.f, r__2 = t * apoaq * 
						    aapq1 + 1.f;
					    sva[q] = aaqq * sqrt((f2cmax(r__1,
						    r__2)));
/* Computing MAX */
					    r__1 = 0.f, r__2 = 1.f - t * 
						    aqoap * aapq1;
					    aapp *= sqrt((f2cmax(r__1,r__2)));
/* Computing MAX */
					    r__1 = mxsinj, r__2 = abs(t);
					    mxsinj = f2cmax(r__1,r__2);
					} else {


					    thsign = -r_sign(&c_b41, &aapq1);
					    if (aaqq > aapp0) {
			  thsign = -thsign;
					    }
					    t = 1.f / (theta + thsign * sqrt(
						    theta * theta + 1.f));
					    cs = sqrt(1.f / (t * t + 1.f));
					    sn = t * cs;
/* Computing MAX */
					    r__1 = mxsinj, r__2 = abs(sn);
					    mxsinj = f2cmax(r__1,r__2);
/* Computing MAX */
					    r__1 = 0.f, r__2 = t * apoaq * 
						    aapq1 + 1.f;
					    sva[q] = aaqq * sqrt((f2cmax(r__1,
						    r__2)));
/* Computing MAX */
					    r__1 = 0.f, r__2 = 1.f - t * 
						    aqoap * aapq1;
					    aapp *= sqrt((f2cmax(r__1,r__2)));

					    r_cnjg(&q__2, &ompq);
					    q__1.r = sn * q__2.r, q__1.i = sn 
						    * q__2.i;
					    crot_(m, &a[p * a_dim1 + 1], &
						    c__1, &a[q * a_dim1 + 1], 
						    &c__1, &cs, &q__1);
					    if (rsvec) {
			  r_cnjg(&q__2, &ompq);
			  q__1.r = sn * q__2.r, q__1.i = sn * q__2.i;
			  crot_(&mvl, &v[p * v_dim1 + 1], &c__1, &v[q * 
				  v_dim1 + 1], &c__1, &cs, &q__1);
					    }
					}
					i__5 = p;
					i__6 = q;
					q__2.r = -cwork[i__6].r, q__2.i = 
						-cwork[i__6].i;
					q__1.r = q__2.r * ompq.r - q__2.i * 
						ompq.i, q__1.i = q__2.r * 
						ompq.i + q__2.i * ompq.r;
					cwork[i__5].r = q__1.r, cwork[i__5].i 
						= q__1.i;

				    } else {
					if (aapp > aaqq) {
					    ccopy_(m, &a[p * a_dim1 + 1], &
						    c__1, &cwork[*n + 1], &
						    c__1);
					    clascl_("G", &c__0, &c__0, &aapp, 
						    &c_b41, m, &c__1, &cwork[*
						    n + 1], lda, &ierr);
					    clascl_("G", &c__0, &c__0, &aaqq, 
						    &c_b41, m, &c__1, &a[q * 
						    a_dim1 + 1], lda, &ierr);
					    q__1.r = -aapq.r, q__1.i = 
						    -aapq.i;
					    caxpy_(m, &q__1, &cwork[*n + 1], &
						    c__1, &a[q * a_dim1 + 1], 
						    &c__1);
					    clascl_("G", &c__0, &c__0, &c_b41,
						     &aaqq, m, &c__1, &a[q * 
						    a_dim1 + 1], lda, &ierr);
/* Computing MAX */
					    r__1 = 0.f, r__2 = 1.f - aapq1 * 
						    aapq1;
					    sva[q] = aaqq * sqrt((f2cmax(r__1,
						    r__2)));
					    mxsinj = f2cmax(mxsinj,sfmin);
					} else {
					    ccopy_(m, &a[q * a_dim1 + 1], &
						    c__1, &cwork[*n + 1], &
						    c__1);
					    clascl_("G", &c__0, &c__0, &aaqq, 
						    &c_b41, m, &c__1, &cwork[*
						    n + 1], lda, &ierr);
					    clascl_("G", &c__0, &c__0, &aapp, 
						    &c_b41, m, &c__1, &a[p * 
						    a_dim1 + 1], lda, &ierr);
					    r_cnjg(&q__2, &aapq);
					    q__1.r = -q__2.r, q__1.i = 
						    -q__2.i;
					    caxpy_(m, &q__1, &cwork[*n + 1], &
						    c__1, &a[p * a_dim1 + 1], 
						    &c__1);
					    clascl_("G", &c__0, &c__0, &c_b41,
						     &aapp, m, &c__1, &a[p * 
						    a_dim1 + 1], lda, &ierr);
/* Computing MAX */
					    r__1 = 0.f, r__2 = 1.f - aapq1 * 
						    aapq1;
					    sva[p] = aapp * sqrt((f2cmax(r__1,
						    r__2)));
					    mxsinj = f2cmax(mxsinj,sfmin);
					}
				    }
/*           END IF ROTOK THEN ... ELSE */

/*           In the case of cancellation in updating SVA(q), SVA(p) */
/* Computing 2nd power */
				    r__1 = sva[q] / aaqq;
				    if (r__1 * r__1 <= rooteps) {
					if (aaqq < rootbig && aaqq > 
						rootsfmin) {
					    sva[q] = scnrm2_(m, &a[q * a_dim1 
						    + 1], &c__1);
					} else {
					    t = 0.f;
					    aaqq = 1.f;
					    classq_(m, &a[q * a_dim1 + 1], &
						    c__1, &t, &aaqq);
					    sva[q] = t * sqrt(aaqq);
					}
				    }
/* Computing 2nd power */
				    r__1 = aapp / aapp0;
				    if (r__1 * r__1 <= rooteps) {
					if (aapp < rootbig && aapp > 
						rootsfmin) {
					    aapp = scnrm2_(m, &a[p * a_dim1 + 
						    1], &c__1);
					} else {
					    t = 0.f;
					    aapp = 1.f;
					    classq_(m, &a[p * a_dim1 + 1], &
						    c__1, &t, &aapp);
					    aapp = t * sqrt(aapp);
					}
					sva[p] = aapp;
				    }
/*              end of OK rotation */
				} else {
				    ++notrot;
/* [RTD]      SKIPPED  = SKIPPED  + 1 */
				    ++pskipped;
				    ++ijblsk;
				}
			    } else {
				++notrot;
				++pskipped;
				++ijblsk;
			    }

			    if (i__ <= swband && ijblsk >= blskip) {
				sva[p] = aapp;
				notrot = 0;
				goto L2011;
			    }
			    if (i__ <= swband && pskipped > rowskip) {
				aapp = -aapp;
				notrot = 0;
				goto L2203;
			    }

/* L2200: */
			}
/*        end of the q-loop */
L2203:

			sva[p] = aapp;

		    } else {

			if (aapp == 0.f) {
/* Computing MIN */
			    i__4 = jgl + kbl - 1;
			    notrot = notrot + f2cmin(i__4,*n) - jgl + 1;
			}
			if (aapp < 0.f) {
			    notrot = 0;
			}

		    }

/* L2100: */
		}
/*     end of the p-loop */
/* L2010: */
	    }
/*     end of the jbc-loop */
L2011:
/* 2011 bailed out of the jbc-loop */
/* Computing MIN */
	    i__3 = igl + kbl - 1;
	    i__2 = f2cmin(i__3,*n);
	    for (p = igl; p <= i__2; ++p) {
		sva[p] = (r__1 = sva[p], abs(r__1));
/* L2012: */
	    }
/* ** */
/* L2000: */
	}
/* 2000 :: end of the ibr-loop */

	if (sva[*n] < rootbig && sva[*n] > rootsfmin) {
	    sva[*n] = scnrm2_(m, &a[*n * a_dim1 + 1], &c__1);
	} else {
	    t = 0.f;
	    aapp = 1.f;
	    classq_(m, &a[*n * a_dim1 + 1], &c__1, &t, &aapp);
	    sva[*n] = t * sqrt(aapp);
	}

/*     Additional steering devices */

	if (i__ < swband && (mxaapq <= roottol || iswrot <= *n)) {
	    swband = i__;
	}

	if (i__ > swband + 1 && mxaapq < sqrt((real) (*n)) * tol && (real) (*
		n) * mxaapq * mxsinj < tol) {
	    goto L1994;
	}

	if (notrot >= emptsw) {
	    goto L1994;
	}

/* L1993: */
    }
/*     end i=1:NSWEEP loop */

/* #:( Reaching this point means that the procedure has not converged. */
    *info = 29;
    goto L1995;

L1994:
/* #:) Reaching this point means numerical convergence after the i-th */
/*     sweep. */

    *info = 0;
/* #:) INFO = 0 confirms successful iterations. */
L1995:

/*     Sort the singular values and find how many are above */
/*     the underflow threshold. */

    n2 = 0;
    n4 = 0;
    i__1 = *n - 1;
    for (p = 1; p <= i__1; ++p) {
	i__2 = *n - p + 1;
	q = isamax_(&i__2, &sva[p], &c__1) + p - 1;
	if (p != q) {
	    temp1 = sva[p];
	    sva[p] = sva[q];
	    sva[q] = temp1;
	    cswap_(m, &a[p * a_dim1 + 1], &c__1, &a[q * a_dim1 + 1], &c__1);
	    if (rsvec) {
		cswap_(&mvl, &v[p * v_dim1 + 1], &c__1, &v[q * v_dim1 + 1], &
			c__1);
	    }
	}
	if (sva[p] != 0.f) {
	    ++n4;
	    if (sva[p] * skl > sfmin) {
		++n2;
	    }
	}
/* L5991: */
    }
    if (sva[*n] != 0.f) {
	++n4;
	if (sva[*n] * skl > sfmin) {
	    ++n2;
	}
    }

/*     Normalize the left singular vectors. */

    if (lsvec || uctol) {
	i__1 = n4;
	for (p = 1; p <= i__1; ++p) {
/*           CALL CSSCAL( M, ONE / SVA( p ), A( 1, p ), 1 ) */
	    clascl_("G", &c__0, &c__0, &sva[p], &c_b41, m, &c__1, &a[p * 
		    a_dim1 + 1], m, &ierr);
/* L1998: */
	}
    }

/*     Scale the product of Jacobi rotations. */

    if (rsvec) {
	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
	    temp1 = 1.f / scnrm2_(&mvl, &v[p * v_dim1 + 1], &c__1);
	    csscal_(&mvl, &temp1, &v[p * v_dim1 + 1], &c__1);
/* L2399: */
	}
    }

/*     Undo scaling, if necessary (and possible). */
    if (skl > 1.f && sva[1] < big / skl || skl < 1.f && sva[f2cmax(n2,1)] > 
	    sfmin / skl) {
	i__1 = *n;
	for (p = 1; p <= i__1; ++p) {
	    sva[p] = skl * sva[p];
/* L2400: */
	}
	skl = 1.f;
    }

    rwork[1] = skl;
/*     The singular values of A are SKL*SVA(1:N). If SKL.NE.ONE */
/*     then some of the singular values may overflow or underflow and */
/*     the spectrum is given in this factored representation. */

    rwork[2] = (real) n4;
/*     N4 is the number of computed nonzero singular values of A. */

    rwork[3] = (real) n2;
/*     N2 is the number of singular values of A greater than SFMIN. */
/*     If N2<N, SVA(N2:N) contains ZEROS and/or denormalized numbers */
/*     that may carry some information. */

    rwork[4] = (real) i__;
/*     i is the index of the last sweep before declaring convergence. */

    rwork[5] = mxaapq;
/*     MXAAPQ is the largest absolute value of scaled pivots in the */
/*     last sweep */

    rwork[6] = mxsinj;
/*     MXSINJ is the largest absolute value of the sines of Jacobi angles */
/*     in the last sweep */

    return;
} /* cgesvj_ */

