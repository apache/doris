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

static complex c_b2 = {0.f,0.f};
static integer c__1 = 1;

/* > \brief \b CHETRI_ROOK computes the inverse of HE matrix using the factorization obtained with the bounded
 Bunch-Kaufman ("rook") diagonal pivoting method. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CHETRI_ROOK + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chetri_
rook.f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chetri_
rook.f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chetri_
rook.f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CHETRI_ROOK( UPLO, N, A, LDA, IPIV, WORK, INFO ) */

/*       CHARACTER          UPLO */
/*       INTEGER            INFO, LDA, N */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX            A( LDA, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CHETRI_ROOK computes the inverse of a complex Hermitian indefinite matrix */
/* > A using the factorization A = U*D*U**H or A = L*D*L**H computed by */
/* > CHETRF_ROOK. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPLO */
/* > \verbatim */
/* >          UPLO is CHARACTER*1 */
/* >          Specifies whether the details of the factorization are stored */
/* >          as an upper or lower triangular matrix. */
/* >          = 'U':  Upper triangular, form is A = U*D*U**H; */
/* >          = 'L':  Lower triangular, form is A = L*D*L**H. */
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
/* >          On entry, the block diagonal matrix D and the multipliers */
/* >          used to obtain the factor U or L as computed by CHETRF_ROOK. */
/* > */
/* >          On exit, if INFO = 0, the (Hermitian) inverse of the original */
/* >          matrix.  If UPLO = 'U', the upper triangular part of the */
/* >          inverse is formed and the part of A below the diagonal is not */
/* >          referenced; if UPLO = 'L' the lower triangular part of the */
/* >          inverse is formed and the part of A above the diagonal is */
/* >          not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] IPIV */
/* > \verbatim */
/* >          IPIV is INTEGER array, dimension (N) */
/* >          Details of the interchanges and the block structure of D */
/* >          as determined by CHETRF_ROOK. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* >          > 0: if INFO = i, D(i,i) = 0; the matrix is singular and its */
/* >               inverse could not be computed. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date November 2013 */

/* > \ingroup complexHEcomputational */

/* > \par Contributors: */
/*  ================== */
/* > */
/* > \verbatim */
/* > */
/* >  November 2013,  Igor Kozachenko, */
/* >                  Computer Science Division, */
/* >                  University of California, Berkeley */
/* > */
/* >  September 2007, Sven Hammarling, Nicholas J. Higham, Craig Lucas, */
/* >                  School of Mathematics, */
/* >                  University of Manchester */
/* > \endverbatim */

/*  ===================================================================== */
/* Subroutine */ void chetri_rook_(char *uplo, integer *n, complex *a, 
	integer *lda, integer *ipiv, complex *work, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3;
    real r__1;
    complex q__1, q__2;

    /* Local variables */
    complex temp, akkp1;
    real d__;
    integer j, k;
    real t;
    extern /* Complex */ VOID cdotc_(complex *, integer *, complex *, integer 
	    *, complex *, integer *);
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void chemv_(char *, integer *, complex *, complex *
	    , integer *, complex *, integer *, complex *, complex *, integer *
	    ), ccopy_(integer *, complex *, integer *, complex *, 
	    integer *), cswap_(integer *, complex *, integer *, complex *, 
	    integer *);
    integer kstep;
    logical upper;
    real ak;
    integer kp;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    real akp1;


/*  -- LAPACK computational routine (version 3.5.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2013 */


/*  ===================================================================== */


/*     Test the input parameters. */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ipiv;
    --work;

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
	xerbla_("CHETRI_ROOK", &i__1, (ftnlen)11);
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Check that the diagonal matrix D is nonsingular. */

    if (upper) {

/*        Upper triangular storage: examine D from bottom to top */

	for (*info = *n; *info >= 1; --(*info)) {
	    i__1 = *info + *info * a_dim1;
	    if (ipiv[*info] > 0 && (a[i__1].r == 0.f && a[i__1].i == 0.f)) {
		return;
	    }
/* L10: */
	}
    } else {

/*        Lower triangular storage: examine D from top to bottom. */

	i__1 = *n;
	for (*info = 1; *info <= i__1; ++(*info)) {
	    i__2 = *info + *info * a_dim1;
	    if (ipiv[*info] > 0 && (a[i__2].r == 0.f && a[i__2].i == 0.f)) {
		return;
	    }
/* L20: */
	}
    }
    *info = 0;

    if (upper) {

/*        Compute inv(A) from the factorization A = U*D*U**H. */

/*        K is the main loop index, increasing from 1 to N in steps of */
/*        1 or 2, depending on the size of the diagonal blocks. */

	k = 1;
L30:

/*        If K > N, exit from loop. */

	if (k > *n) {
	    goto L70;
	}

	if (ipiv[k] > 0) {

/*           1 x 1 diagonal block */

/*           Invert the diagonal block. */

	    i__1 = k + k * a_dim1;
	    i__2 = k + k * a_dim1;
	    r__1 = 1.f / a[i__2].r;
	    a[i__1].r = r__1, a[i__1].i = 0.f;

/*           Compute column K of the inverse. */

	    if (k > 1) {
		i__1 = k - 1;
		ccopy_(&i__1, &a[k * a_dim1 + 1], &c__1, &work[1], &c__1);
		i__1 = k - 1;
		q__1.r = -1.f, q__1.i = 0.f;
		chemv_(uplo, &i__1, &q__1, &a[a_offset], lda, &work[1], &c__1,
			 &c_b2, &a[k * a_dim1 + 1], &c__1);
		i__1 = k + k * a_dim1;
		i__2 = k + k * a_dim1;
		i__3 = k - 1;
		cdotc_(&q__2, &i__3, &work[1], &c__1, &a[k * a_dim1 + 1], &
			c__1);
		r__1 = q__2.r;
		q__1.r = a[i__2].r - r__1, q__1.i = a[i__2].i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
	    }
	    kstep = 1;
	} else {

/*           2 x 2 diagonal block */

/*           Invert the diagonal block. */

	    t = c_abs(&a[k + (k + 1) * a_dim1]);
	    i__1 = k + k * a_dim1;
	    ak = a[i__1].r / t;
	    i__1 = k + 1 + (k + 1) * a_dim1;
	    akp1 = a[i__1].r / t;
	    i__1 = k + (k + 1) * a_dim1;
	    q__1.r = a[i__1].r / t, q__1.i = a[i__1].i / t;
	    akkp1.r = q__1.r, akkp1.i = q__1.i;
	    d__ = t * (ak * akp1 - 1.f);
	    i__1 = k + k * a_dim1;
	    r__1 = akp1 / d__;
	    a[i__1].r = r__1, a[i__1].i = 0.f;
	    i__1 = k + 1 + (k + 1) * a_dim1;
	    r__1 = ak / d__;
	    a[i__1].r = r__1, a[i__1].i = 0.f;
	    i__1 = k + (k + 1) * a_dim1;
	    q__2.r = -akkp1.r, q__2.i = -akkp1.i;
	    q__1.r = q__2.r / d__, q__1.i = q__2.i / d__;
	    a[i__1].r = q__1.r, a[i__1].i = q__1.i;

/*           Compute columns K and K+1 of the inverse. */

	    if (k > 1) {
		i__1 = k - 1;
		ccopy_(&i__1, &a[k * a_dim1 + 1], &c__1, &work[1], &c__1);
		i__1 = k - 1;
		q__1.r = -1.f, q__1.i = 0.f;
		chemv_(uplo, &i__1, &q__1, &a[a_offset], lda, &work[1], &c__1,
			 &c_b2, &a[k * a_dim1 + 1], &c__1);
		i__1 = k + k * a_dim1;
		i__2 = k + k * a_dim1;
		i__3 = k - 1;
		cdotc_(&q__2, &i__3, &work[1], &c__1, &a[k * a_dim1 + 1], &
			c__1);
		r__1 = q__2.r;
		q__1.r = a[i__2].r - r__1, q__1.i = a[i__2].i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
		i__1 = k + (k + 1) * a_dim1;
		i__2 = k + (k + 1) * a_dim1;
		i__3 = k - 1;
		cdotc_(&q__2, &i__3, &a[k * a_dim1 + 1], &c__1, &a[(k + 1) * 
			a_dim1 + 1], &c__1);
		q__1.r = a[i__2].r - q__2.r, q__1.i = a[i__2].i - q__2.i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
		i__1 = k - 1;
		ccopy_(&i__1, &a[(k + 1) * a_dim1 + 1], &c__1, &work[1], &
			c__1);
		i__1 = k - 1;
		q__1.r = -1.f, q__1.i = 0.f;
		chemv_(uplo, &i__1, &q__1, &a[a_offset], lda, &work[1], &c__1,
			 &c_b2, &a[(k + 1) * a_dim1 + 1], &c__1);
		i__1 = k + 1 + (k + 1) * a_dim1;
		i__2 = k + 1 + (k + 1) * a_dim1;
		i__3 = k - 1;
		cdotc_(&q__2, &i__3, &work[1], &c__1, &a[(k + 1) * a_dim1 + 1]
			, &c__1);
		r__1 = q__2.r;
		q__1.r = a[i__2].r - r__1, q__1.i = a[i__2].i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
	    }
	    kstep = 2;
	}

	if (kstep == 1) {

/*           Interchange rows and columns K and IPIV(K) in the leading */
/*           submatrix A(1:k,1:k) */

	    kp = ipiv[k];
	    if (kp != k) {

		if (kp > 1) {
		    i__1 = kp - 1;
		    cswap_(&i__1, &a[k * a_dim1 + 1], &c__1, &a[kp * a_dim1 + 
			    1], &c__1);
		}

		i__1 = k - 1;
		for (j = kp + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    temp.r = q__1.r, temp.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = temp.r, a[i__2].i = temp.i;
/* L40: */
		}

		i__1 = kp + k * a_dim1;
		r_cnjg(&q__1, &a[kp + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;

		i__1 = k + k * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + k * a_dim1;
		i__2 = kp + kp * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;
	    }
	} else {

/*           Interchange rows and columns K and K+1 with -IPIV(K) and */
/*           -IPIV(K+1) in the leading submatrix A(k+1:n,k+1:n) */

/*           (1) Interchange rows and columns K and -IPIV(K) */

	    kp = -ipiv[k];
	    if (kp != k) {

		if (kp > 1) {
		    i__1 = kp - 1;
		    cswap_(&i__1, &a[k * a_dim1 + 1], &c__1, &a[kp * a_dim1 + 
			    1], &c__1);
		}

		i__1 = k - 1;
		for (j = kp + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    temp.r = q__1.r, temp.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = temp.r, a[i__2].i = temp.i;
/* L50: */
		}

		i__1 = kp + k * a_dim1;
		r_cnjg(&q__1, &a[kp + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;

		i__1 = k + k * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + k * a_dim1;
		i__2 = kp + kp * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;

		i__1 = k + (k + 1) * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + (k + 1) * a_dim1;
		i__2 = kp + (k + 1) * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + (k + 1) * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;
	    }

/*           (2) Interchange rows and columns K+1 and -IPIV(K+1) */

	    ++k;
	    kp = -ipiv[k];
	    if (kp != k) {

		if (kp > 1) {
		    i__1 = kp - 1;
		    cswap_(&i__1, &a[k * a_dim1 + 1], &c__1, &a[kp * a_dim1 + 
			    1], &c__1);
		}

		i__1 = k - 1;
		for (j = kp + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    temp.r = q__1.r, temp.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = temp.r, a[i__2].i = temp.i;
/* L60: */
		}

		i__1 = kp + k * a_dim1;
		r_cnjg(&q__1, &a[kp + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;

		i__1 = k + k * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + k * a_dim1;
		i__2 = kp + kp * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;
	    }
	}

	++k;
	goto L30;
L70:

	;
    } else {

/*        Compute inv(A) from the factorization A = L*D*L**H. */

/*        K is the main loop index, decreasing from N to 1 in steps of */
/*        1 or 2, depending on the size of the diagonal blocks. */

	k = *n;
L80:

/*        If K < 1, exit from loop. */

	if (k < 1) {
	    goto L120;
	}

	if (ipiv[k] > 0) {

/*           1 x 1 diagonal block */

/*           Invert the diagonal block. */

	    i__1 = k + k * a_dim1;
	    i__2 = k + k * a_dim1;
	    r__1 = 1.f / a[i__2].r;
	    a[i__1].r = r__1, a[i__1].i = 0.f;

/*           Compute column K of the inverse. */

	    if (k < *n) {
		i__1 = *n - k;
		ccopy_(&i__1, &a[k + 1 + k * a_dim1], &c__1, &work[1], &c__1);
		i__1 = *n - k;
		q__1.r = -1.f, q__1.i = 0.f;
		chemv_(uplo, &i__1, &q__1, &a[k + 1 + (k + 1) * a_dim1], lda, 
			&work[1], &c__1, &c_b2, &a[k + 1 + k * a_dim1], &c__1);
		i__1 = k + k * a_dim1;
		i__2 = k + k * a_dim1;
		i__3 = *n - k;
		cdotc_(&q__2, &i__3, &work[1], &c__1, &a[k + 1 + k * a_dim1], 
			&c__1);
		r__1 = q__2.r;
		q__1.r = a[i__2].r - r__1, q__1.i = a[i__2].i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
	    }
	    kstep = 1;
	} else {

/*           2 x 2 diagonal block */

/*           Invert the diagonal block. */

	    t = c_abs(&a[k + (k - 1) * a_dim1]);
	    i__1 = k - 1 + (k - 1) * a_dim1;
	    ak = a[i__1].r / t;
	    i__1 = k + k * a_dim1;
	    akp1 = a[i__1].r / t;
	    i__1 = k + (k - 1) * a_dim1;
	    q__1.r = a[i__1].r / t, q__1.i = a[i__1].i / t;
	    akkp1.r = q__1.r, akkp1.i = q__1.i;
	    d__ = t * (ak * akp1 - 1.f);
	    i__1 = k - 1 + (k - 1) * a_dim1;
	    r__1 = akp1 / d__;
	    a[i__1].r = r__1, a[i__1].i = 0.f;
	    i__1 = k + k * a_dim1;
	    r__1 = ak / d__;
	    a[i__1].r = r__1, a[i__1].i = 0.f;
	    i__1 = k + (k - 1) * a_dim1;
	    q__2.r = -akkp1.r, q__2.i = -akkp1.i;
	    q__1.r = q__2.r / d__, q__1.i = q__2.i / d__;
	    a[i__1].r = q__1.r, a[i__1].i = q__1.i;

/*           Compute columns K-1 and K of the inverse. */

	    if (k < *n) {
		i__1 = *n - k;
		ccopy_(&i__1, &a[k + 1 + k * a_dim1], &c__1, &work[1], &c__1);
		i__1 = *n - k;
		q__1.r = -1.f, q__1.i = 0.f;
		chemv_(uplo, &i__1, &q__1, &a[k + 1 + (k + 1) * a_dim1], lda, 
			&work[1], &c__1, &c_b2, &a[k + 1 + k * a_dim1], &c__1);
		i__1 = k + k * a_dim1;
		i__2 = k + k * a_dim1;
		i__3 = *n - k;
		cdotc_(&q__2, &i__3, &work[1], &c__1, &a[k + 1 + k * a_dim1], 
			&c__1);
		r__1 = q__2.r;
		q__1.r = a[i__2].r - r__1, q__1.i = a[i__2].i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
		i__1 = k + (k - 1) * a_dim1;
		i__2 = k + (k - 1) * a_dim1;
		i__3 = *n - k;
		cdotc_(&q__2, &i__3, &a[k + 1 + k * a_dim1], &c__1, &a[k + 1 
			+ (k - 1) * a_dim1], &c__1);
		q__1.r = a[i__2].r - q__2.r, q__1.i = a[i__2].i - q__2.i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
		i__1 = *n - k;
		ccopy_(&i__1, &a[k + 1 + (k - 1) * a_dim1], &c__1, &work[1], &
			c__1);
		i__1 = *n - k;
		q__1.r = -1.f, q__1.i = 0.f;
		chemv_(uplo, &i__1, &q__1, &a[k + 1 + (k + 1) * a_dim1], lda, 
			&work[1], &c__1, &c_b2, &a[k + 1 + (k - 1) * a_dim1], 
			&c__1);
		i__1 = k - 1 + (k - 1) * a_dim1;
		i__2 = k - 1 + (k - 1) * a_dim1;
		i__3 = *n - k;
		cdotc_(&q__2, &i__3, &work[1], &c__1, &a[k + 1 + (k - 1) * 
			a_dim1], &c__1);
		r__1 = q__2.r;
		q__1.r = a[i__2].r - r__1, q__1.i = a[i__2].i;
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;
	    }
	    kstep = 2;
	}

	if (kstep == 1) {

/*           Interchange rows and columns K and IPIV(K) in the trailing */
/*           submatrix A(k:n,k:n) */

	    kp = ipiv[k];
	    if (kp != k) {

		if (kp < *n) {
		    i__1 = *n - kp;
		    cswap_(&i__1, &a[kp + 1 + k * a_dim1], &c__1, &a[kp + 1 + 
			    kp * a_dim1], &c__1);
		}

		i__1 = kp - 1;
		for (j = k + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    temp.r = q__1.r, temp.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = temp.r, a[i__2].i = temp.i;
/* L90: */
		}

		i__1 = kp + k * a_dim1;
		r_cnjg(&q__1, &a[kp + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;

		i__1 = k + k * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + k * a_dim1;
		i__2 = kp + kp * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;
	    }
	} else {

/*           Interchange rows and columns K and K-1 with -IPIV(K) and */
/*           -IPIV(K-1) in the trailing submatrix A(k-1:n,k-1:n) */

/*           (1) Interchange rows and columns K and -IPIV(K) */

	    kp = -ipiv[k];
	    if (kp != k) {

		if (kp < *n) {
		    i__1 = *n - kp;
		    cswap_(&i__1, &a[kp + 1 + k * a_dim1], &c__1, &a[kp + 1 + 
			    kp * a_dim1], &c__1);
		}

		i__1 = kp - 1;
		for (j = k + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    temp.r = q__1.r, temp.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = temp.r, a[i__2].i = temp.i;
/* L100: */
		}

		i__1 = kp + k * a_dim1;
		r_cnjg(&q__1, &a[kp + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;

		i__1 = k + k * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + k * a_dim1;
		i__2 = kp + kp * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;

		i__1 = k + (k - 1) * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + (k - 1) * a_dim1;
		i__2 = kp + (k - 1) * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + (k - 1) * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;
	    }

/*           (2) Interchange rows and columns K-1 and -IPIV(K-1) */

	    --k;
	    kp = -ipiv[k];
	    if (kp != k) {

		if (kp < *n) {
		    i__1 = *n - kp;
		    cswap_(&i__1, &a[kp + 1 + k * a_dim1], &c__1, &a[kp + 1 + 
			    kp * a_dim1], &c__1);
		}

		i__1 = kp - 1;
		for (j = k + 1; j <= i__1; ++j) {
		    r_cnjg(&q__1, &a[j + k * a_dim1]);
		    temp.r = q__1.r, temp.i = q__1.i;
		    i__2 = j + k * a_dim1;
		    r_cnjg(&q__1, &a[kp + j * a_dim1]);
		    a[i__2].r = q__1.r, a[i__2].i = q__1.i;
		    i__2 = kp + j * a_dim1;
		    a[i__2].r = temp.r, a[i__2].i = temp.i;
/* L110: */
		}

		i__1 = kp + k * a_dim1;
		r_cnjg(&q__1, &a[kp + k * a_dim1]);
		a[i__1].r = q__1.r, a[i__1].i = q__1.i;

		i__1 = k + k * a_dim1;
		temp.r = a[i__1].r, temp.i = a[i__1].i;
		i__1 = k + k * a_dim1;
		i__2 = kp + kp * a_dim1;
		a[i__1].r = a[i__2].r, a[i__1].i = a[i__2].i;
		i__1 = kp + kp * a_dim1;
		a[i__1].r = temp.r, a[i__1].i = temp.i;
	    }
	}

	--k;
	goto L80;
L120:
	;
    }

    return;

/*     End of CHETRI_ROOK */

} /* chetri_rook__ */

