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

static complex c_b1 = {1.f,0.f};
static integer c__1 = 1;

/* > \brief \b CLAHEF computes a partial factorization of a complex Hermitian indefinite matrix using the Bunc
h-Kaufman diagonal pivoting method (blocked algorithm, calling Level 3 BLAS). */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CLAHEF + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clahef.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clahef.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clahef.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CLAHEF( UPLO, N, NB, KB, A, LDA, IPIV, W, LDW, INFO ) */

/*       CHARACTER          UPLO */
/*       INTEGER            INFO, KB, LDA, LDW, N, NB */
/*       INTEGER            IPIV( * ) */
/*       COMPLEX            A( LDA, * ), W( LDW, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CLAHEF computes a partial factorization of a complex Hermitian */
/* > matrix A using the Bunch-Kaufman diagonal pivoting method. The */
/* > partial factorization has the form: */
/* > */
/* > A  =  ( I  U12 ) ( A11  0  ) (  I      0     )  if UPLO = 'U', or: */
/* >       ( 0  U22 ) (  0   D  ) ( U12**H U22**H ) */
/* > */
/* > A  =  ( L11  0 ) (  D   0  ) ( L11**H L21**H )  if UPLO = 'L' */
/* >       ( L21  I ) (  0  A22 ) (  0      I     ) */
/* > */
/* > where the order of D is at most NB. The actual order is returned in */
/* > the argument KB, and is either NB or NB-1, or N if N <= NB. */
/* > Note that U**H denotes the conjugate transpose of U. */
/* > */
/* > CLAHEF is an auxiliary routine called by CHETRF. It uses blocked code */
/* > (calling Level 3 BLAS) to update the submatrix A11 (if UPLO = 'U') or */
/* > A22 (if UPLO = 'L'). */
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
/* > \param[in] NB */
/* > \verbatim */
/* >          NB is INTEGER */
/* >          The maximum number of columns of the matrix A that should be */
/* >          factored.  NB should be at least 2 to allow for 2-by-2 pivot */
/* >          blocks. */
/* > \endverbatim */
/* > */
/* > \param[out] KB */
/* > \verbatim */
/* >          KB is INTEGER */
/* >          The number of columns of A that were actually factored. */
/* >          KB is either NB-1 or NB, or N if N <= NB. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA,N) */
/* >          On entry, the Hermitian matrix A.  If UPLO = 'U', the leading */
/* >          n-by-n upper triangular part of A contains the upper */
/* >          triangular part of the matrix A, and the strictly lower */
/* >          triangular part of A is not referenced.  If UPLO = 'L', the */
/* >          leading n-by-n lower triangular part of A contains the lower */
/* >          triangular part of the matrix A, and the strictly upper */
/* >          triangular part of A is not referenced. */
/* >          On exit, A contains details of the partial factorization. */
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
/* >          Details of the interchanges and the block structure of D. */
/* > */
/* >          If UPLO = 'U': */
/* >             Only the last KB elements of IPIV are set. */
/* > */
/* >             If IPIV(k) > 0, then rows and columns k and IPIV(k) were */
/* >             interchanged and D(k,k) is a 1-by-1 diagonal block. */
/* > */
/* >             If IPIV(k) = IPIV(k-1) < 0, then rows and columns */
/* >             k-1 and -IPIV(k) were interchanged and D(k-1:k,k-1:k) */
/* >             is a 2-by-2 diagonal block. */
/* > */
/* >          If UPLO = 'L': */
/* >             Only the first KB elements of IPIV are set. */
/* > */
/* >             If IPIV(k) > 0, then rows and columns k and IPIV(k) were */
/* >             interchanged and D(k,k) is a 1-by-1 diagonal block. */
/* > */
/* >             If IPIV(k) = IPIV(k+1) < 0, then rows and columns */
/* >             k+1 and -IPIV(k) were interchanged and D(k:k+1,k:k+1) */
/* >             is a 2-by-2 diagonal block. */
/* > \endverbatim */
/* > */
/* > \param[out] W */
/* > \verbatim */
/* >          W is COMPLEX array, dimension (LDW,NB) */
/* > \endverbatim */
/* > */
/* > \param[in] LDW */
/* > \verbatim */
/* >          LDW is INTEGER */
/* >          The leading dimension of the array W.  LDW >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          > 0: if INFO = k, D(k,k) is exactly zero.  The factorization */
/* >               has been completed, but the block diagonal matrix D is */
/* >               exactly singular. */
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
/* > \endverbatim */

/*  ===================================================================== */
/* Subroutine */ void clahef_(char *uplo, integer *n, integer *nb, integer *kb,
	 complex *a, integer *lda, integer *ipiv, complex *w, integer *ldw, 
	integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, w_dim1, w_offset, i__1, i__2, i__3, i__4, i__5;
    real r__1, r__2, r__3, r__4;
    complex q__1, q__2, q__3, q__4;

    /* Local variables */
    integer imax, jmax, j, k;
    real t, alpha;
    extern /* Subroutine */ void cgemm_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *);
    extern logical lsame_(char *, char *);
    extern /* Subroutine */ void cgemv_(char *, integer *, integer *, complex *
	    , complex *, integer *, complex *, integer *, complex *, complex *
	    , integer *), ccopy_(integer *, complex *, integer *, 
	    complex *, integer *), cswap_(integer *, complex *, integer *, 
	    complex *, integer *);
    integer kstep;
    real r1;
    complex d11, d21, d22;
    integer jb, jj, kk, jp, kp;
    real absakk;
    extern /* Subroutine */ void clacgv_(integer *, complex *, integer *);
    integer kw;
    extern integer icamax_(integer *, complex *, integer *);
    extern /* Subroutine */ void csscal_(integer *, real *, complex *, integer 
	    *);
    real colmax, rowmax;
    integer kkw;


/*  -- LAPACK computational routine (version 3.5.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     November 2013 */


/*  ===================================================================== */


    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ipiv;
    w_dim1 = *ldw;
    w_offset = 1 + w_dim1 * 1;
    w -= w_offset;

    /* Function Body */
    *info = 0;

/*     Initialize ALPHA for use in choosing pivot block size. */

    alpha = (sqrt(17.f) + 1.f) / 8.f;

    if (lsame_(uplo, "U")) {

/*        Factorize the trailing columns of A using the upper triangle */
/*        of A and working backwards, and compute the matrix W = U12*D */
/*        for use in updating A11 (note that conjg(W) is actually stored) */

/*        K is the main loop index, decreasing from N in steps of 1 or 2 */

	k = *n;
L10:

/*        KW is the column of W which corresponds to column K of A */

	kw = *nb + k - *n;

/*        Exit from loop */

	if (k <= *n - *nb + 1 && *nb < *n || k < 1) {
	    goto L30;
	}

	kstep = 1;

/*        Copy column K of A to column KW of W and update it */

	i__1 = k - 1;
	ccopy_(&i__1, &a[k * a_dim1 + 1], &c__1, &w[kw * w_dim1 + 1], &c__1);
	i__1 = k + kw * w_dim1;
	i__2 = k + k * a_dim1;
	r__1 = a[i__2].r;
	w[i__1].r = r__1, w[i__1].i = 0.f;
	if (k < *n) {
	    i__1 = *n - k;
	    q__1.r = -1.f, q__1.i = 0.f;
	    cgemv_("No transpose", &k, &i__1, &q__1, &a[(k + 1) * a_dim1 + 1],
		     lda, &w[k + (kw + 1) * w_dim1], ldw, &c_b1, &w[kw * 
		    w_dim1 + 1], &c__1);
	    i__1 = k + kw * w_dim1;
	    i__2 = k + kw * w_dim1;
	    r__1 = w[i__2].r;
	    w[i__1].r = r__1, w[i__1].i = 0.f;
	}

/*        Determine rows and columns to be interchanged and whether */
/*        a 1-by-1 or 2-by-2 pivot block will be used */

	i__1 = k + kw * w_dim1;
	absakk = (r__1 = w[i__1].r, abs(r__1));

/*        IMAX is the row-index of the largest off-diagonal element in */
/*        column K, and COLMAX is its absolute value. */
/*        Determine both COLMAX and IMAX. */

	if (k > 1) {
	    i__1 = k - 1;
	    imax = icamax_(&i__1, &w[kw * w_dim1 + 1], &c__1);
	    i__1 = imax + kw * w_dim1;
	    colmax = (r__1 = w[i__1].r, abs(r__1)) + (r__2 = r_imag(&w[imax + 
		    kw * w_dim1]), abs(r__2));
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
	} else {

/*           ============================================================ */

/*           BEGIN pivot search */

/*           Case(1) */
	    if (absakk >= alpha * colmax) {

/*              no interchange, use 1-by-1 pivot block */

		kp = k;
	    } else {

/*              BEGIN pivot search along IMAX row */


/*              Copy column IMAX to column KW-1 of W and update it */

		i__1 = imax - 1;
		ccopy_(&i__1, &a[imax * a_dim1 + 1], &c__1, &w[(kw - 1) * 
			w_dim1 + 1], &c__1);
		i__1 = imax + (kw - 1) * w_dim1;
		i__2 = imax + imax * a_dim1;
		r__1 = a[i__2].r;
		w[i__1].r = r__1, w[i__1].i = 0.f;
		i__1 = k - imax;
		ccopy_(&i__1, &a[imax + (imax + 1) * a_dim1], lda, &w[imax + 
			1 + (kw - 1) * w_dim1], &c__1);
		i__1 = k - imax;
		clacgv_(&i__1, &w[imax + 1 + (kw - 1) * w_dim1], &c__1);
		if (k < *n) {
		    i__1 = *n - k;
		    q__1.r = -1.f, q__1.i = 0.f;
		    cgemv_("No transpose", &k, &i__1, &q__1, &a[(k + 1) * 
			    a_dim1 + 1], lda, &w[imax + (kw + 1) * w_dim1], 
			    ldw, &c_b1, &w[(kw - 1) * w_dim1 + 1], &c__1);
		    i__1 = imax + (kw - 1) * w_dim1;
		    i__2 = imax + (kw - 1) * w_dim1;
		    r__1 = w[i__2].r;
		    w[i__1].r = r__1, w[i__1].i = 0.f;
		}

/*              JMAX is the column-index of the largest off-diagonal */
/*              element in row IMAX, and ROWMAX is its absolute value. */
/*              Determine only ROWMAX. */

		i__1 = k - imax;
		jmax = imax + icamax_(&i__1, &w[imax + 1 + (kw - 1) * w_dim1],
			 &c__1);
		i__1 = jmax + (kw - 1) * w_dim1;
		rowmax = (r__1 = w[i__1].r, abs(r__1)) + (r__2 = r_imag(&w[
			jmax + (kw - 1) * w_dim1]), abs(r__2));
		if (imax > 1) {
		    i__1 = imax - 1;
		    jmax = icamax_(&i__1, &w[(kw - 1) * w_dim1 + 1], &c__1);
/* Computing MAX */
		    i__1 = jmax + (kw - 1) * w_dim1;
		    r__3 = rowmax, r__4 = (r__1 = w[i__1].r, abs(r__1)) + (
			    r__2 = r_imag(&w[jmax + (kw - 1) * w_dim1]), abs(
			    r__2));
		    rowmax = f2cmax(r__3,r__4);
		}

/*              Case(2) */
		if (absakk >= alpha * colmax * (colmax / rowmax)) {

/*                 no interchange, use 1-by-1 pivot block */

		    kp = k;

/*              Case(3) */
		} else /* if(complicated condition) */ {
		    i__1 = imax + (kw - 1) * w_dim1;
		    if ((r__1 = w[i__1].r, abs(r__1)) >= alpha * rowmax) {

/*                 interchange rows and columns K and IMAX, use 1-by-1 */
/*                 pivot block */

			kp = imax;

/*                 copy column KW-1 of W to column KW of W */

			ccopy_(&k, &w[(kw - 1) * w_dim1 + 1], &c__1, &w[kw * 
				w_dim1 + 1], &c__1);

/*              Case(4) */
		    } else {

/*                 interchange rows and columns K-1 and IMAX, use 2-by-2 */
/*                 pivot block */

			kp = imax;
			kstep = 2;
		    }
		}


/*              END pivot search along IMAX row */

	    }

/*           END pivot search */

/*           ============================================================ */

/*           KK is the column of A where pivoting step stopped */

	    kk = k - kstep + 1;

/*           KKW is the column of W which corresponds to column KK of A */

	    kkw = *nb + kk - *n;

/*           Interchange rows and columns KP and KK. */
/*           Updated column KP is already stored in column KKW of W. */

	    if (kp != kk) {

/*              Copy non-updated column KK to column KP of submatrix A */
/*              at step K. No need to copy element into column K */
/*              (or K and K-1 for 2-by-2 pivot) of A, since these columns */
/*              will be later overwritten. */

		i__1 = kp + kp * a_dim1;
		i__2 = kk + kk * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		i__1 = kk - 1 - kp;
		ccopy_(&i__1, &a[kp + 1 + kk * a_dim1], &c__1, &a[kp + (kp + 
			1) * a_dim1], lda);
		i__1 = kk - 1 - kp;
		clacgv_(&i__1, &a[kp + (kp + 1) * a_dim1], lda);
		if (kp > 1) {
		    i__1 = kp - 1;
		    ccopy_(&i__1, &a[kk * a_dim1 + 1], &c__1, &a[kp * a_dim1 
			    + 1], &c__1);
		}

/*              Interchange rows KK and KP in last K+1 to N columns of A */
/*              (columns K (or K and K-1 for 2-by-2 pivot) of A will be */
/*              later overwritten). Interchange rows KK and KP */
/*              in last KKW to NB columns of W. */

		if (k < *n) {
		    i__1 = *n - k;
		    cswap_(&i__1, &a[kk + (k + 1) * a_dim1], lda, &a[kp + (k 
			    + 1) * a_dim1], lda);
		}
		i__1 = *n - kk + 1;
		cswap_(&i__1, &w[kk + kkw * w_dim1], ldw, &w[kp + kkw * 
			w_dim1], ldw);
	    }

	    if (kstep == 1) {

/*              1-by-1 pivot block D(k): column kw of W now holds */

/*              W(kw) = U(k)*D(k), */

/*              where U(k) is the k-th column of U */

/*              (1) Store subdiag. elements of column U(k) */
/*              and 1-by-1 block D(k) in column k of A. */
/*              (NOTE: Diagonal element U(k,k) is a UNIT element */
/*              and not stored) */
/*                 A(k,k) := D(k,k) = W(k,kw) */
/*                 A(1:k-1,k) := U(1:k-1,k) = W(1:k-1,kw)/D(k,k) */

/*              (NOTE: No need to use for Hermitian matrix */
/*              A( K, K ) = DBLE( W( K, K) ) to separately copy diagonal */
/*              element D(k,k) from W (potentially saves only one load)) */
		ccopy_(&k, &w[kw * w_dim1 + 1], &c__1, &a[k * a_dim1 + 1], &
			c__1);
		if (k > 1) {

/*                 (NOTE: No need to check if A(k,k) is NOT ZERO, */
/*                  since that was ensured earlier in pivot search: */
/*                  case A(k,k) = 0 falls into 2x2 pivot case(4)) */

		    i__1 = k + k * a_dim1;
		    r1 = 1.f / a[i__1].r;
		    i__1 = k - 1;
		    csscal_(&i__1, &r1, &a[k * a_dim1 + 1], &c__1);

/*                 (2) Conjugate column W(kw) */

		    i__1 = k - 1;
		    clacgv_(&i__1, &w[kw * w_dim1 + 1], &c__1);
		}

	    } else {

/*              2-by-2 pivot block D(k): columns kw and kw-1 of W now hold */

/*              ( W(kw-1) W(kw) ) = ( U(k-1) U(k) )*D(k) */

/*              where U(k) and U(k-1) are the k-th and (k-1)-th columns */
/*              of U */

/*              (1) Store U(1:k-2,k-1) and U(1:k-2,k) and 2-by-2 */
/*              block D(k-1:k,k-1:k) in columns k-1 and k of A. */
/*              (NOTE: 2-by-2 diagonal block U(k-1:k,k-1:k) is a UNIT */
/*              block and not stored) */
/*                 A(k-1:k,k-1:k) := D(k-1:k,k-1:k) = W(k-1:k,kw-1:kw) */
/*                 A(1:k-2,k-1:k) := U(1:k-2,k:k-1:k) = */
/*                 = W(1:k-2,kw-1:kw) * ( D(k-1:k,k-1:k)**(-1) ) */

		if (k > 2) {

/*                 Factor out the columns of the inverse of 2-by-2 pivot */
/*                 block D, so that each column contains 1, to reduce the */
/*                 number of FLOPS when we multiply panel */
/*                 ( W(kw-1) W(kw) ) by this inverse, i.e. by D**(-1). */

/*                 D**(-1) = ( d11 cj(d21) )**(-1) = */
/*                           ( d21    d22 ) */

/*                 = 1/(d11*d22-|d21|**2) * ( ( d22) (-cj(d21) ) ) = */
/*                                          ( (-d21) (     d11 ) ) */

/*                 = 1/(|d21|**2) * 1/((d11/cj(d21))*(d22/d21)-1) * */

/*                   * ( d21*( d22/d21 ) conj(d21)*(           - 1 ) ) = */
/*                     (     (      -1 )           ( d11/conj(d21) ) ) */

/*                 = 1/(|d21|**2) * 1/(D22*D11-1) * */

/*                   * ( d21*( D11 ) conj(d21)*(  -1 ) ) = */
/*                     (     (  -1 )           ( D22 ) ) */

/*                 = (1/|d21|**2) * T * ( d21*( D11 ) conj(d21)*(  -1 ) ) = */
/*                                      (     (  -1 )           ( D22 ) ) */

/*                 = ( (T/conj(d21))*( D11 ) (T/d21)*(  -1 ) ) = */
/*                   (               (  -1 )         ( D22 ) ) */

/*                 = ( conj(D21)*( D11 ) D21*(  -1 ) ) */
/*                   (           (  -1 )     ( D22 ) ), */

/*                 where D11 = d22/d21, */
/*                       D22 = d11/conj(d21), */
/*                       D21 = T/d21, */
/*                       T = 1/(D22*D11-1). */

/*                 (NOTE: No need to check for division by ZERO, */
/*                  since that was ensured earlier in pivot search: */
/*                  (a) d21 != 0, since in 2x2 pivot case(4) */
/*                      |d21| should be larger than |d11| and |d22|; */
/*                  (b) (D22*D11 - 1) != 0, since from (a), */
/*                      both |D11| < 1, |D22| < 1, hence |D22*D11| << 1.) */

		    i__1 = k - 1 + kw * w_dim1;
		    d21.r = w[i__1].r, d21.i = w[i__1].i;
		    r_cnjg(&q__2, &d21);
		    c_div(&q__1, &w[k + kw * w_dim1], &q__2);
		    d11.r = q__1.r, d11.i = q__1.i;
		    c_div(&q__1, &w[k - 1 + (kw - 1) * w_dim1], &d21);
		    d22.r = q__1.r, d22.i = q__1.i;
		    q__1.r = d11.r * d22.r - d11.i * d22.i, q__1.i = d11.r * 
			    d22.i + d11.i * d22.r;
		    t = 1.f / (q__1.r - 1.f);
		    q__2.r = t, q__2.i = 0.f;
		    c_div(&q__1, &q__2, &d21);
		    d21.r = q__1.r, d21.i = q__1.i;

/*                 Update elements in columns A(k-1) and A(k) as */
/*                 dot products of rows of ( W(kw-1) W(kw) ) and columns */
/*                 of D**(-1) */

		    i__1 = k - 2;
		    for (j = 1; j <= i__1; ++j) {
			i__2 = j + (k - 1) * a_dim1;
			i__3 = j + (kw - 1) * w_dim1;
			q__3.r = d11.r * w[i__3].r - d11.i * w[i__3].i, 
				q__3.i = d11.r * w[i__3].i + d11.i * w[i__3]
				.r;
			i__4 = j + kw * w_dim1;
			q__2.r = q__3.r - w[i__4].r, q__2.i = q__3.i - w[i__4]
				.i;
			q__1.r = d21.r * q__2.r - d21.i * q__2.i, q__1.i = 
				d21.r * q__2.i + d21.i * q__2.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
			i__2 = j + k * a_dim1;
			r_cnjg(&q__2, &d21);
			i__3 = j + kw * w_dim1;
			q__4.r = d22.r * w[i__3].r - d22.i * w[i__3].i, 
				q__4.i = d22.r * w[i__3].i + d22.i * w[i__3]
				.r;
			i__4 = j + (kw - 1) * w_dim1;
			q__3.r = q__4.r - w[i__4].r, q__3.i = q__4.i - w[i__4]
				.i;
			q__1.r = q__2.r * q__3.r - q__2.i * q__3.i, q__1.i = 
				q__2.r * q__3.i + q__2.i * q__3.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
/* L20: */
		    }
		}

/*              Copy D(k) to A */

		i__1 = k - 1 + (k - 1) * a_dim1;
		i__2 = k - 1 + (kw - 1) * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k - 1 + k * a_dim1;
		i__2 = k - 1 + kw * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k + k * a_dim1;
		i__2 = k + kw * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;

/*              (2) Conjugate columns W(kw) and W(kw-1) */

		i__1 = k - 1;
		clacgv_(&i__1, &w[kw * w_dim1 + 1], &c__1);
		i__1 = k - 2;
		clacgv_(&i__1, &w[(kw - 1) * w_dim1 + 1], &c__1);

	    }

	}

/*        Store details of the interchanges in IPIV */

	if (kstep == 1) {
	    ipiv[k] = kp;
	} else {
	    ipiv[k] = -kp;
	    ipiv[k - 1] = -kp;
	}

/*        Decrease K and return to the start of the main loop */

	k -= kstep;
	goto L10;

L30:

/*        Update the upper triangle of A11 (= A(1:k,1:k)) as */

/*        A11 := A11 - U12*D*U12**H = A11 - U12*W**H */

/*        computing blocks of NB columns at a time (note that conjg(W) is */
/*        actually stored) */

	i__1 = -(*nb);
	for (j = (k - 1) / *nb * *nb + 1; i__1 < 0 ? j >= 1 : j <= 1; j += 
		i__1) {
/* Computing MIN */
	    i__2 = *nb, i__3 = k - j + 1;
	    jb = f2cmin(i__2,i__3);

/*           Update the upper triangle of the diagonal block */

	    i__2 = j + jb - 1;
	    for (jj = j; jj <= i__2; ++jj) {
		i__3 = jj + jj * a_dim1;
		i__4 = jj + jj * a_dim1;
		r__1 = a[i__4].r;
		a[i__3].r = r__1, a[i__3].i = 0.f;
		i__3 = jj - j + 1;
		i__4 = *n - k;
		q__1.r = -1.f, q__1.i = 0.f;
		cgemv_("No transpose", &i__3, &i__4, &q__1, &a[j + (k + 1) * 
			a_dim1], lda, &w[jj + (kw + 1) * w_dim1], ldw, &c_b1, 
			&a[j + jj * a_dim1], &c__1);
		i__3 = jj + jj * a_dim1;
		i__4 = jj + jj * a_dim1;
		r__1 = a[i__4].r;
		a[i__3].r = r__1, a[i__3].i = 0.f;
/* L40: */
	    }

/*           Update the rectangular superdiagonal block */

	    i__2 = j - 1;
	    i__3 = *n - k;
	    q__1.r = -1.f, q__1.i = 0.f;
	    cgemm_("No transpose", "Transpose", &i__2, &jb, &i__3, &q__1, &a[(
		    k + 1) * a_dim1 + 1], lda, &w[j + (kw + 1) * w_dim1], ldw,
		     &c_b1, &a[j * a_dim1 + 1], lda);
/* L50: */
	}

/*        Put U12 in standard form by partially undoing the interchanges */
/*        in of rows in columns k+1:n looping backwards from k+1 to n */

	j = k + 1;
L60:

/*           Undo the interchanges (if any) of rows J and JP */
/*           at each step J */

/*           (Here, J is a diagonal index) */
	jj = j;
	jp = ipiv[j];
	if (jp < 0) {
	    jp = -jp;
/*              (Here, J is a diagonal index) */
	    ++j;
	}
/*           (NOTE: Here, J is used to determine row length. Length N-J+1 */
/*           of the rows to swap back doesn't include diagonal element) */
	++j;
	if (jp != jj && j <= *n) {
	    i__1 = *n - j + 1;
	    cswap_(&i__1, &a[jp + j * a_dim1], lda, &a[jj + j * a_dim1], lda);
	}
	if (j <= *n) {
	    goto L60;
	}

/*        Set KB to the number of columns factorized */

	*kb = *n - k;

    } else {

/*        Factorize the leading columns of A using the lower triangle */
/*        of A and working forwards, and compute the matrix W = L21*D */
/*        for use in updating A22 (note that conjg(W) is actually stored) */

/*        K is the main loop index, increasing from 1 in steps of 1 or 2 */

	k = 1;
L70:

/*        Exit from loop */

	if (k >= *nb && *nb < *n || k > *n) {
	    goto L90;
	}

	kstep = 1;

/*        Copy column K of A to column K of W and update it */

	i__1 = k + k * w_dim1;
	i__2 = k + k * a_dim1;
	r__1 = a[i__2].r;
	w[i__1].r = r__1, w[i__1].i = 0.f;
	if (k < *n) {
	    i__1 = *n - k;
	    ccopy_(&i__1, &a[k + 1 + k * a_dim1], &c__1, &w[k + 1 + k * 
		    w_dim1], &c__1);
	}
	i__1 = *n - k + 1;
	i__2 = k - 1;
	q__1.r = -1.f, q__1.i = 0.f;
	cgemv_("No transpose", &i__1, &i__2, &q__1, &a[k + a_dim1], lda, &w[k 
		+ w_dim1], ldw, &c_b1, &w[k + k * w_dim1], &c__1);
	i__1 = k + k * w_dim1;
	i__2 = k + k * w_dim1;
	r__1 = w[i__2].r;
	w[i__1].r = r__1, w[i__1].i = 0.f;

/*        Determine rows and columns to be interchanged and whether */
/*        a 1-by-1 or 2-by-2 pivot block will be used */

	i__1 = k + k * w_dim1;
	absakk = (r__1 = w[i__1].r, abs(r__1));

/*        IMAX is the row-index of the largest off-diagonal element in */
/*        column K, and COLMAX is its absolute value. */
/*        Determine both COLMAX and IMAX. */

	if (k < *n) {
	    i__1 = *n - k;
	    imax = k + icamax_(&i__1, &w[k + 1 + k * w_dim1], &c__1);
	    i__1 = imax + k * w_dim1;
	    colmax = (r__1 = w[i__1].r, abs(r__1)) + (r__2 = r_imag(&w[imax + 
		    k * w_dim1]), abs(r__2));
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
	} else {

/*           ============================================================ */

/*           BEGIN pivot search */

/*           Case(1) */
	    if (absakk >= alpha * colmax) {

/*              no interchange, use 1-by-1 pivot block */

		kp = k;
	    } else {

/*              BEGIN pivot search along IMAX row */


/*              Copy column IMAX to column K+1 of W and update it */

		i__1 = imax - k;
		ccopy_(&i__1, &a[imax + k * a_dim1], lda, &w[k + (k + 1) * 
			w_dim1], &c__1);
		i__1 = imax - k;
		clacgv_(&i__1, &w[k + (k + 1) * w_dim1], &c__1);
		i__1 = imax + (k + 1) * w_dim1;
		i__2 = imax + imax * a_dim1;
		r__1 = a[i__2].r;
		w[i__1].r = r__1, w[i__1].i = 0.f;
		if (imax < *n) {
		    i__1 = *n - imax;
		    ccopy_(&i__1, &a[imax + 1 + imax * a_dim1], &c__1, &w[
			    imax + 1 + (k + 1) * w_dim1], &c__1);
		}
		i__1 = *n - k + 1;
		i__2 = k - 1;
		q__1.r = -1.f, q__1.i = 0.f;
		cgemv_("No transpose", &i__1, &i__2, &q__1, &a[k + a_dim1], 
			lda, &w[imax + w_dim1], ldw, &c_b1, &w[k + (k + 1) * 
			w_dim1], &c__1);
		i__1 = imax + (k + 1) * w_dim1;
		i__2 = imax + (k + 1) * w_dim1;
		r__1 = w[i__2].r;
		w[i__1].r = r__1, w[i__1].i = 0.f;

/*              JMAX is the column-index of the largest off-diagonal */
/*              element in row IMAX, and ROWMAX is its absolute value. */
/*              Determine only ROWMAX. */

		i__1 = imax - k;
		jmax = k - 1 + icamax_(&i__1, &w[k + (k + 1) * w_dim1], &c__1)
			;
		i__1 = jmax + (k + 1) * w_dim1;
		rowmax = (r__1 = w[i__1].r, abs(r__1)) + (r__2 = r_imag(&w[
			jmax + (k + 1) * w_dim1]), abs(r__2));
		if (imax < *n) {
		    i__1 = *n - imax;
		    jmax = imax + icamax_(&i__1, &w[imax + 1 + (k + 1) * 
			    w_dim1], &c__1);
/* Computing MAX */
		    i__1 = jmax + (k + 1) * w_dim1;
		    r__3 = rowmax, r__4 = (r__1 = w[i__1].r, abs(r__1)) + (
			    r__2 = r_imag(&w[jmax + (k + 1) * w_dim1]), abs(
			    r__2));
		    rowmax = f2cmax(r__3,r__4);
		}

/*              Case(2) */
		if (absakk >= alpha * colmax * (colmax / rowmax)) {

/*                 no interchange, use 1-by-1 pivot block */

		    kp = k;

/*              Case(3) */
		} else /* if(complicated condition) */ {
		    i__1 = imax + (k + 1) * w_dim1;
		    if ((r__1 = w[i__1].r, abs(r__1)) >= alpha * rowmax) {

/*                 interchange rows and columns K and IMAX, use 1-by-1 */
/*                 pivot block */

			kp = imax;

/*                 copy column K+1 of W to column K of W */

			i__1 = *n - k + 1;
			ccopy_(&i__1, &w[k + (k + 1) * w_dim1], &c__1, &w[k + 
				k * w_dim1], &c__1);

/*              Case(4) */
		    } else {

/*                 interchange rows and columns K+1 and IMAX, use 2-by-2 */
/*                 pivot block */

			kp = imax;
			kstep = 2;
		    }
		}


/*              END pivot search along IMAX row */

	    }

/*           END pivot search */

/*           ============================================================ */

/*           KK is the column of A where pivoting step stopped */

	    kk = k + kstep - 1;

/*           Interchange rows and columns KP and KK. */
/*           Updated column KP is already stored in column KK of W. */

	    if (kp != kk) {

/*              Copy non-updated column KK to column KP of submatrix A */
/*              at step K. No need to copy element into column K */
/*              (or K and K+1 for 2-by-2 pivot) of A, since these columns */
/*              will be later overwritten. */

		i__1 = kp + kp * a_dim1;
		i__2 = kk + kk * a_dim1;
		r__1 = a[i__2].r;
		a[i__1].r = r__1, a[i__1].i = 0.f;
		i__1 = kp - kk - 1;
		ccopy_(&i__1, &a[kk + 1 + kk * a_dim1], &c__1, &a[kp + (kk + 
			1) * a_dim1], lda);
		i__1 = kp - kk - 1;
		clacgv_(&i__1, &a[kp + (kk + 1) * a_dim1], lda);
		if (kp < *n) {
		    i__1 = *n - kp;
		    ccopy_(&i__1, &a[kp + 1 + kk * a_dim1], &c__1, &a[kp + 1 
			    + kp * a_dim1], &c__1);
		}

/*              Interchange rows KK and KP in first K-1 columns of A */
/*              (columns K (or K and K+1 for 2-by-2 pivot) of A will be */
/*              later overwritten). Interchange rows KK and KP */
/*              in first KK columns of W. */

		if (k > 1) {
		    i__1 = k - 1;
		    cswap_(&i__1, &a[kk + a_dim1], lda, &a[kp + a_dim1], lda);
		}
		cswap_(&kk, &w[kk + w_dim1], ldw, &w[kp + w_dim1], ldw);
	    }

	    if (kstep == 1) {

/*              1-by-1 pivot block D(k): column k of W now holds */

/*              W(k) = L(k)*D(k), */

/*              where L(k) is the k-th column of L */

/*              (1) Store subdiag. elements of column L(k) */
/*              and 1-by-1 block D(k) in column k of A. */
/*              (NOTE: Diagonal element L(k,k) is a UNIT element */
/*              and not stored) */
/*                 A(k,k) := D(k,k) = W(k,k) */
/*                 A(k+1:N,k) := L(k+1:N,k) = W(k+1:N,k)/D(k,k) */

/*              (NOTE: No need to use for Hermitian matrix */
/*              A( K, K ) = DBLE( W( K, K) ) to separately copy diagonal */
/*              element D(k,k) from W (potentially saves only one load)) */
		i__1 = *n - k + 1;
		ccopy_(&i__1, &w[k + k * w_dim1], &c__1, &a[k + k * a_dim1], &
			c__1);
		if (k < *n) {

/*                 (NOTE: No need to check if A(k,k) is NOT ZERO, */
/*                  since that was ensured earlier in pivot search: */
/*                  case A(k,k) = 0 falls into 2x2 pivot case(4)) */

		    i__1 = k + k * a_dim1;
		    r1 = 1.f / a[i__1].r;
		    i__1 = *n - k;
		    csscal_(&i__1, &r1, &a[k + 1 + k * a_dim1], &c__1);

/*                 (2) Conjugate column W(k) */

		    i__1 = *n - k;
		    clacgv_(&i__1, &w[k + 1 + k * w_dim1], &c__1);
		}

	    } else {

/*              2-by-2 pivot block D(k): columns k and k+1 of W now hold */

/*              ( W(k) W(k+1) ) = ( L(k) L(k+1) )*D(k) */

/*              where L(k) and L(k+1) are the k-th and (k+1)-th columns */
/*              of L */

/*              (1) Store L(k+2:N,k) and L(k+2:N,k+1) and 2-by-2 */
/*              block D(k:k+1,k:k+1) in columns k and k+1 of A. */
/*              (NOTE: 2-by-2 diagonal block L(k:k+1,k:k+1) is a UNIT */
/*              block and not stored) */
/*                 A(k:k+1,k:k+1) := D(k:k+1,k:k+1) = W(k:k+1,k:k+1) */
/*                 A(k+2:N,k:k+1) := L(k+2:N,k:k+1) = */
/*                 = W(k+2:N,k:k+1) * ( D(k:k+1,k:k+1)**(-1) ) */

		if (k < *n - 1) {

/*                 Factor out the columns of the inverse of 2-by-2 pivot */
/*                 block D, so that each column contains 1, to reduce the */
/*                 number of FLOPS when we multiply panel */
/*                 ( W(kw-1) W(kw) ) by this inverse, i.e. by D**(-1). */

/*                 D**(-1) = ( d11 cj(d21) )**(-1) = */
/*                           ( d21    d22 ) */

/*                 = 1/(d11*d22-|d21|**2) * ( ( d22) (-cj(d21) ) ) = */
/*                                          ( (-d21) (     d11 ) ) */

/*                 = 1/(|d21|**2) * 1/((d11/cj(d21))*(d22/d21)-1) * */

/*                   * ( d21*( d22/d21 ) conj(d21)*(           - 1 ) ) = */
/*                     (     (      -1 )           ( d11/conj(d21) ) ) */

/*                 = 1/(|d21|**2) * 1/(D22*D11-1) * */

/*                   * ( d21*( D11 ) conj(d21)*(  -1 ) ) = */
/*                     (     (  -1 )           ( D22 ) ) */

/*                 = (1/|d21|**2) * T * ( d21*( D11 ) conj(d21)*(  -1 ) ) = */
/*                                      (     (  -1 )           ( D22 ) ) */

/*                 = ( (T/conj(d21))*( D11 ) (T/d21)*(  -1 ) ) = */
/*                   (               (  -1 )         ( D22 ) ) */

/*                 = ( conj(D21)*( D11 ) D21*(  -1 ) ) */
/*                   (           (  -1 )     ( D22 ) ) */

/*                 where D11 = d22/d21, */
/*                       D22 = d11/conj(d21), */
/*                       D21 = T/d21, */
/*                       T = 1/(D22*D11-1). */

/*                 (NOTE: No need to check for division by ZERO, */
/*                  since that was ensured earlier in pivot search: */
/*                  (a) d21 != 0, since in 2x2 pivot case(4) */
/*                      |d21| should be larger than |d11| and |d22|; */
/*                  (b) (D22*D11 - 1) != 0, since from (a), */
/*                      both |D11| < 1, |D22| < 1, hence |D22*D11| << 1.) */

		    i__1 = k + 1 + k * w_dim1;
		    d21.r = w[i__1].r, d21.i = w[i__1].i;
		    c_div(&q__1, &w[k + 1 + (k + 1) * w_dim1], &d21);
		    d11.r = q__1.r, d11.i = q__1.i;
		    r_cnjg(&q__2, &d21);
		    c_div(&q__1, &w[k + k * w_dim1], &q__2);
		    d22.r = q__1.r, d22.i = q__1.i;
		    q__1.r = d11.r * d22.r - d11.i * d22.i, q__1.i = d11.r * 
			    d22.i + d11.i * d22.r;
		    t = 1.f / (q__1.r - 1.f);
		    q__2.r = t, q__2.i = 0.f;
		    c_div(&q__1, &q__2, &d21);
		    d21.r = q__1.r, d21.i = q__1.i;

/*                 Update elements in columns A(k) and A(k+1) as */
/*                 dot products of rows of ( W(k) W(k+1) ) and columns */
/*                 of D**(-1) */

		    i__1 = *n;
		    for (j = k + 2; j <= i__1; ++j) {
			i__2 = j + k * a_dim1;
			r_cnjg(&q__2, &d21);
			i__3 = j + k * w_dim1;
			q__4.r = d11.r * w[i__3].r - d11.i * w[i__3].i, 
				q__4.i = d11.r * w[i__3].i + d11.i * w[i__3]
				.r;
			i__4 = j + (k + 1) * w_dim1;
			q__3.r = q__4.r - w[i__4].r, q__3.i = q__4.i - w[i__4]
				.i;
			q__1.r = q__2.r * q__3.r - q__2.i * q__3.i, q__1.i = 
				q__2.r * q__3.i + q__2.i * q__3.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
			i__2 = j + (k + 1) * a_dim1;
			i__3 = j + (k + 1) * w_dim1;
			q__3.r = d22.r * w[i__3].r - d22.i * w[i__3].i, 
				q__3.i = d22.r * w[i__3].i + d22.i * w[i__3]
				.r;
			i__4 = j + k * w_dim1;
			q__2.r = q__3.r - w[i__4].r, q__2.i = q__3.i - w[i__4]
				.i;
			q__1.r = d21.r * q__2.r - d21.i * q__2.i, q__1.i = 
				d21.r * q__2.i + d21.i * q__2.r;
			a[i__2].r = q__1.r, a[i__2].i = q__1.i;
/* L80: */
		    }
		}

/*              Copy D(k) to A */

		i__1 = k + k * a_dim1;
		i__2 = k + k * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k + 1 + k * a_dim1;
		i__2 = k + 1 + k * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;
		i__1 = k + 1 + (k + 1) * a_dim1;
		i__2 = k + 1 + (k + 1) * w_dim1;
		a[i__1].r = w[i__2].r, a[i__1].i = w[i__2].i;

/*              (2) Conjugate columns W(k) and W(k+1) */

		i__1 = *n - k;
		clacgv_(&i__1, &w[k + 1 + k * w_dim1], &c__1);
		i__1 = *n - k - 1;
		clacgv_(&i__1, &w[k + 2 + (k + 1) * w_dim1], &c__1);

	    }

	}

/*        Store details of the interchanges in IPIV */

	if (kstep == 1) {
	    ipiv[k] = kp;
	} else {
	    ipiv[k] = -kp;
	    ipiv[k + 1] = -kp;
	}

/*        Increase K and return to the start of the main loop */

	k += kstep;
	goto L70;

L90:

/*        Update the lower triangle of A22 (= A(k:n,k:n)) as */

/*        A22 := A22 - L21*D*L21**H = A22 - L21*W**H */

/*        computing blocks of NB columns at a time (note that conjg(W) is */
/*        actually stored) */

	i__1 = *n;
	i__2 = *nb;
	for (j = k; i__2 < 0 ? j >= i__1 : j <= i__1; j += i__2) {
/* Computing MIN */
	    i__3 = *nb, i__4 = *n - j + 1;
	    jb = f2cmin(i__3,i__4);

/*           Update the lower triangle of the diagonal block */

	    i__3 = j + jb - 1;
	    for (jj = j; jj <= i__3; ++jj) {
		i__4 = jj + jj * a_dim1;
		i__5 = jj + jj * a_dim1;
		r__1 = a[i__5].r;
		a[i__4].r = r__1, a[i__4].i = 0.f;
		i__4 = j + jb - jj;
		i__5 = k - 1;
		q__1.r = -1.f, q__1.i = 0.f;
		cgemv_("No transpose", &i__4, &i__5, &q__1, &a[jj + a_dim1], 
			lda, &w[jj + w_dim1], ldw, &c_b1, &a[jj + jj * a_dim1]
			, &c__1);
		i__4 = jj + jj * a_dim1;
		i__5 = jj + jj * a_dim1;
		r__1 = a[i__5].r;
		a[i__4].r = r__1, a[i__4].i = 0.f;
/* L100: */
	    }

/*           Update the rectangular subdiagonal block */

	    if (j + jb <= *n) {
		i__3 = *n - j - jb + 1;
		i__4 = k - 1;
		q__1.r = -1.f, q__1.i = 0.f;
		cgemm_("No transpose", "Transpose", &i__3, &jb, &i__4, &q__1, 
			&a[j + jb + a_dim1], lda, &w[j + w_dim1], ldw, &c_b1, 
			&a[j + jb + j * a_dim1], lda);
	    }
/* L110: */
	}

/*        Put L21 in standard form by partially undoing the interchanges */
/*        of rows in columns 1:k-1 looping backwards from k-1 to 1 */

	j = k - 1;
L120:

/*           Undo the interchanges (if any) of rows J and JP */
/*           at each step J */

/*           (Here, J is a diagonal index) */
	jj = j;
	jp = ipiv[j];
	if (jp < 0) {
	    jp = -jp;
/*              (Here, J is a diagonal index) */
	    --j;
	}
/*           (NOTE: Here, J is used to determine row length. Length J */
/*           of the rows to swap back doesn't include diagonal element) */
	--j;
	if (jp != jj && j >= 1) {
	    cswap_(&j, &a[jp + a_dim1], lda, &a[jj + a_dim1], lda);
	}
	if (j >= 1) {
	    goto L120;
	}

/*        Set KB to the number of columns factorized */

	*kb = k - 1;

    }
    return;

/*     End of CLAHEF */

} /* clahef_ */

