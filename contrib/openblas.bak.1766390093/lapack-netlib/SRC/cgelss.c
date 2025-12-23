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
static integer c__6 = 6;
static integer c_n1 = -1;
static integer c__1 = 1;
static integer c__0 = 0;
static real c_b59 = 0.f;

/* > \brief <b> CGELSS solves overdetermined or underdetermined systems for GE matrices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CGELSS + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cgelss.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cgelss.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cgelss.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CGELSS( M, N, NRHS, A, LDA, B, LDB, S, RCOND, RANK, */
/*                          WORK, LWORK, RWORK, INFO ) */

/*       INTEGER            INFO, LDA, LDB, LWORK, M, N, NRHS, RANK */
/*       REAL               RCOND */
/*       REAL               RWORK( * ), S( * ) */
/*       COMPLEX            A( LDA, * ), B( LDB, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CGELSS computes the minimum norm solution to a complex linear */
/* > least squares problem: */
/* > */
/* > Minimize 2-norm(| b - A*x |). */
/* > */
/* > using the singular value decomposition (SVD) of A. A is an M-by-N */
/* > matrix which may be rank-deficient. */
/* > */
/* > Several right hand side vectors b and solution vectors x can be */
/* > handled in a single call; they are stored as the columns of the */
/* > M-by-NRHS right hand side matrix B and the N-by-NRHS solution matrix */
/* > X. */
/* > */
/* > The effective rank of A is determined by treating as zero those */
/* > singular values which are less than RCOND times the largest singular */
/* > value. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] M */
/* > \verbatim */
/* >          M is INTEGER */
/* >          The number of rows of the matrix A. M >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The number of columns of the matrix A. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] NRHS */
/* > \verbatim */
/* >          NRHS is INTEGER */
/* >          The number of right hand sides, i.e., the number of columns */
/* >          of the matrices B and X. NRHS >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is COMPLEX array, dimension (LDA,N) */
/* >          On entry, the M-by-N matrix A. */
/* >          On exit, the first f2cmin(m,n) rows of A are overwritten with */
/* >          its right singular vectors, stored rowwise. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of the array A. LDA >= f2cmax(1,M). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is COMPLEX array, dimension (LDB,NRHS) */
/* >          On entry, the M-by-NRHS right hand side matrix B. */
/* >          On exit, B is overwritten by the N-by-NRHS solution matrix X. */
/* >          If m >= n and RANK = n, the residual sum-of-squares for */
/* >          the solution in the i-th column is given by the sum of */
/* >          squares of the modulus of elements n+1:m in that column. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of the array B.  LDB >= f2cmax(1,M,N). */
/* > \endverbatim */
/* > */
/* > \param[out] S */
/* > \verbatim */
/* >          S is REAL array, dimension (f2cmin(M,N)) */
/* >          The singular values of A in decreasing order. */
/* >          The condition number of A in the 2-norm = S(1)/S(f2cmin(m,n)). */
/* > \endverbatim */
/* > */
/* > \param[in] RCOND */
/* > \verbatim */
/* >          RCOND is REAL */
/* >          RCOND is used to determine the effective rank of A. */
/* >          Singular values S(i) <= RCOND*S(1) are treated as zero. */
/* >          If RCOND < 0, machine precision is used instead. */
/* > \endverbatim */
/* > */
/* > \param[out] RANK */
/* > \verbatim */
/* >          RANK is INTEGER */
/* >          The effective rank of A, i.e., the number of singular values */
/* >          which are greater than RCOND*S(1). */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK >= 1, and also: */
/* >          LWORK >=  2*f2cmin(M,N) + f2cmax(M,N,NRHS) */
/* >          For good performance, LWORK should generally be larger. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] RWORK */
/* > \verbatim */
/* >          RWORK is REAL array, dimension (5*f2cmin(M,N)) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          > 0:  the algorithm for computing the SVD failed to converge; */
/* >                if INFO = i, i off-diagonal elements of an intermediate */
/* >                bidiagonal form did not converge to zero. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup complexGEsolve */

/*  ===================================================================== */
/* Subroutine */ void cgelss_(integer *m, integer *n, integer *nrhs, complex *
	a, integer *lda, complex *b, integer *ldb, real *s, real *rcond, 
	integer *rank, complex *work, integer *lwork, real *rwork, integer *
	info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, i__1, i__2, i__3;
    real r__1;

    /* Local variables */
    real anrm, bnrm;
    integer itau, lwork_cgebrd__, lwork_cgelqf__, lwork_cgeqrf__, 
	    lwork_cungbr__, lwork_cunmbr__, i__, lwork_cunmlq__, 
	    lwork_cunmqr__;
    extern /* Subroutine */ void cgemm_(char *, char *, integer *, integer *, 
	    integer *, complex *, complex *, integer *, complex *, integer *, 
	    complex *, complex *, integer *);
    integer iascl, ibscl;
    extern /* Subroutine */ void cgemv_(char *, integer *, integer *, complex *
	    , complex *, integer *, complex *, integer *, complex *, complex *
	    , integer *);
    integer chunk;
    real sfmin;
    extern /* Subroutine */ void ccopy_(integer *, complex *, integer *, 
	    complex *, integer *);
    integer minmn, maxmn, itaup, itauq, mnthr, iwork, bl, ie, il;
    extern /* Subroutine */ void cgebrd_(integer *, integer *, complex *, 
	    integer *, real *, real *, complex *, complex *, complex *, 
	    integer *, integer *), slabad_(real *, real *);
    extern real clange_(char *, integer *, integer *, complex *, integer *, 
	    real *);
    integer mm;
    extern /* Subroutine */ void cgelqf_(integer *, integer *, complex *, 
	    integer *, complex *, complex *, integer *, integer *), clascl_(
	    char *, integer *, integer *, real *, real *, integer *, integer *
	    , complex *, integer *, integer *), cgeqrf_(integer *, 
	    integer *, complex *, integer *, complex *, complex *, integer *, 
	    integer *);
    extern real slamch_(char *);
    extern /* Subroutine */ void clacpy_(char *, integer *, integer *, complex 
	    *, integer *, complex *, integer *), claset_(char *, 
	    integer *, integer *, complex *, complex *, complex *, integer *);
    extern int xerbla_(char *, integer *, ftnlen);
    extern void cbdsqr_(char *, 
	    integer *, integer *, integer *, integer *, real *, real *, 
	    complex *, integer *, complex *, integer *, complex *, integer *, 
	    real *, integer *);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    real bignum;
    extern /* Subroutine */ void cungbr_(char *, integer *, integer *, integer 
	    *, complex *, integer *, complex *, complex *, integer *, integer 
	    *), slascl_(char *, integer *, integer *, real *, real *, 
	    integer *, integer *, real *, integer *, integer *), 
	    cunmbr_(char *, char *, char *, integer *, integer *, integer *, 
	    complex *, integer *, complex *, complex *, integer *, complex *, 
	    integer *, integer *), csrscl_(integer *, 
	    real *, complex *, integer *), slaset_(char *, integer *, integer 
	    *, real *, real *, real *, integer *), cunmlq_(char *, 
	    char *, integer *, integer *, integer *, complex *, integer *, 
	    complex *, complex *, integer *, complex *, integer *, integer *);
    integer ldwork;
    extern /* Subroutine */ void cunmqr_(char *, char *, integer *, integer *, 
	    integer *, complex *, integer *, complex *, complex *, integer *, 
	    complex *, integer *, integer *);
    integer minwrk, maxwrk;
    real smlnum;
    integer irwork;
    logical lquery;
    complex dum[1];
    real eps, thr;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */


/*     Test the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --s;
    --work;
    --rwork;

    /* Function Body */
    *info = 0;
    minmn = f2cmin(*m,*n);
    maxmn = f2cmax(*m,*n);
    lquery = *lwork == -1;
    if (*m < 0) {
	*info = -1;
    } else if (*n < 0) {
	*info = -2;
    } else if (*nrhs < 0) {
	*info = -3;
    } else if (*lda < f2cmax(1,*m)) {
	*info = -5;
    } else if (*ldb < f2cmax(1,maxmn)) {
	*info = -7;
    }

/*     Compute workspace */
/*      (Note: Comments in the code beginning "Workspace:" describe the */
/*       minimal amount of workspace needed at that point in the code, */
/*       as well as the preferred amount for good performance. */
/*       CWorkspace refers to complex workspace, and RWorkspace refers */
/*       to real workspace. NB refers to the optimal block size for the */
/*       immediately following subroutine, as returned by ILAENV.) */

    if (*info == 0) {
	minwrk = 1;
	maxwrk = 1;
	if (minmn > 0) {
	    mm = *m;
	    mnthr = ilaenv_(&c__6, "CGELSS", " ", m, n, nrhs, &c_n1, (ftnlen)
		    6, (ftnlen)1);
	    if (*m >= *n && *m >= mnthr) {

/*              Path 1a - overdetermined, with many more rows than */
/*                        columns */

/*              Compute space needed for CGEQRF */
		cgeqrf_(m, n, &a[a_offset], lda, dum, dum, &c_n1, info);
		lwork_cgeqrf__ = dum[0].r;
/*              Compute space needed for CUNMQR */
		cunmqr_("L", "C", m, nrhs, n, &a[a_offset], lda, dum, &b[
			b_offset], ldb, dum, &c_n1, info);
		lwork_cunmqr__ = dum[0].r;
		mm = *n;
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n + *n * ilaenv_(&c__1, "CGEQRF", 
			" ", m, n, &c_n1, &c_n1, (ftnlen)6, (ftnlen)1);
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n + *nrhs * ilaenv_(&c__1, "CUNMQR", 
			"LC", m, nrhs, n, &c_n1, (ftnlen)6, (ftnlen)2);
		maxwrk = f2cmax(i__1,i__2);
	    }
	    if (*m >= *n) {

/*              Path 1 - overdetermined or exactly determined */

/*              Compute space needed for CGEBRD */
		cgebrd_(&mm, n, &a[a_offset], lda, &s[1], &s[1], dum, dum, 
			dum, &c_n1, info);
		lwork_cgebrd__ = dum[0].r;
/*              Compute space needed for CUNMBR */
		cunmbr_("Q", "L", "C", &mm, nrhs, n, &a[a_offset], lda, dum, &
			b[b_offset], ldb, dum, &c_n1, info);
		lwork_cunmbr__ = dum[0].r;
/*              Compute space needed for CUNGBR */
		cungbr_("P", n, n, n, &a[a_offset], lda, dum, dum, &c_n1, 
			info);
		lwork_cungbr__ = dum[0].r;
/*              Compute total workspace needed */
/* Computing MAX */
		i__1 = maxwrk, i__2 = (*n << 1) + lwork_cgebrd__;
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = maxwrk, i__2 = (*n << 1) + lwork_cunmbr__;
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = maxwrk, i__2 = (*n << 1) + lwork_cungbr__;
		maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n * *nrhs;
		maxwrk = f2cmax(i__1,i__2);
		minwrk = (*n << 1) + f2cmax(*nrhs,*m);
	    }
	    if (*n > *m) {
		minwrk = (*m << 1) + f2cmax(*nrhs,*n);
		if (*n >= mnthr) {

/*                 Path 2a - underdetermined, with many more columns */
/*                 than rows */

/*                 Compute space needed for CGELQF */
		    cgelqf_(m, n, &a[a_offset], lda, dum, dum, &c_n1, info);
		    lwork_cgelqf__ = dum[0].r;
/*                 Compute space needed for CGEBRD */
		    cgebrd_(m, m, &a[a_offset], lda, &s[1], &s[1], dum, dum, 
			    dum, &c_n1, info);
		    lwork_cgebrd__ = dum[0].r;
/*                 Compute space needed for CUNMBR */
		    cunmbr_("Q", "L", "C", m, nrhs, n, &a[a_offset], lda, dum,
			     &b[b_offset], ldb, dum, &c_n1, info);
		    lwork_cunmbr__ = dum[0].r;
/*                 Compute space needed for CUNGBR */
		    cungbr_("P", m, m, m, &a[a_offset], lda, dum, dum, &c_n1, 
			    info);
		    lwork_cungbr__ = dum[0].r;
/*                 Compute space needed for CUNMLQ */
		    cunmlq_("L", "C", n, nrhs, m, &a[a_offset], lda, dum, &b[
			    b_offset], ldb, dum, &c_n1, info);
		    lwork_cunmlq__ = dum[0].r;
/*                 Compute total workspace needed */
		    maxwrk = *m + lwork_cgelqf__;
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * 3 + *m * *m + lwork_cgebrd__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * 3 + *m * *m + lwork_cunmbr__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m * 3 + *m * *m + lwork_cungbr__;
		    maxwrk = f2cmax(i__1,i__2);
		    if (*nrhs > 1) {
/* Computing MAX */
			i__1 = maxwrk, i__2 = *m * *m + *m + *m * *nrhs;
			maxwrk = f2cmax(i__1,i__2);
		    } else {
/* Computing MAX */
			i__1 = maxwrk, i__2 = *m * *m + (*m << 1);
			maxwrk = f2cmax(i__1,i__2);
		    }
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *m + lwork_cunmlq__;
		    maxwrk = f2cmax(i__1,i__2);
		} else {

/*                 Path 2 - underdetermined */

/*                 Compute space needed for CGEBRD */
		    cgebrd_(m, n, &a[a_offset], lda, &s[1], &s[1], dum, dum, 
			    dum, &c_n1, info);
		    lwork_cgebrd__ = dum[0].r;
/*                 Compute space needed for CUNMBR */
		    cunmbr_("Q", "L", "C", m, nrhs, m, &a[a_offset], lda, dum,
			     &b[b_offset], ldb, dum, &c_n1, info);
		    lwork_cunmbr__ = dum[0].r;
/*                 Compute space needed for CUNGBR */
		    cungbr_("P", m, n, m, &a[a_offset], lda, dum, dum, &c_n1, 
			    info);
		    lwork_cungbr__ = dum[0].r;
		    maxwrk = (*m << 1) + lwork_cgebrd__;
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cunmbr__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = (*m << 1) + lwork_cungbr__;
		    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
		    i__1 = maxwrk, i__2 = *n * *nrhs;
		    maxwrk = f2cmax(i__1,i__2);
		}
	    }
	    maxwrk = f2cmax(minwrk,maxwrk);
	}
	work[1].r = (real) maxwrk, work[1].i = 0.f;

	if (*lwork < minwrk && ! lquery) {
	    *info = -12;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("CGELSS", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*m == 0 || *n == 0) {
	*rank = 0;
	return;
    }

/*     Get machine parameters */

    eps = slamch_("P");
    sfmin = slamch_("S");
    smlnum = sfmin / eps;
    bignum = 1.f / smlnum;
    slabad_(&smlnum, &bignum);

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = clange_("M", m, n, &a[a_offset], lda, &rwork[1]);
    iascl = 0;
    if (anrm > 0.f && anrm < smlnum) {

/*        Scale matrix norm up to SMLNUM */

	clascl_("G", &c__0, &c__0, &anrm, &smlnum, m, n, &a[a_offset], lda, 
		info);
	iascl = 1;
    } else if (anrm > bignum) {

/*        Scale matrix norm down to BIGNUM */

	clascl_("G", &c__0, &c__0, &anrm, &bignum, m, n, &a[a_offset], lda, 
		info);
	iascl = 2;
    } else if (anrm == 0.f) {

/*        Matrix all zero. Return zero solution. */

	i__1 = f2cmax(*m,*n);
	claset_("F", &i__1, nrhs, &c_b1, &c_b1, &b[b_offset], ldb);
	slaset_("F", &minmn, &c__1, &c_b59, &c_b59, &s[1], &minmn);
	*rank = 0;
	goto L70;
    }

/*     Scale B if f2cmax element outside range [SMLNUM,BIGNUM] */

    bnrm = clange_("M", m, nrhs, &b[b_offset], ldb, &rwork[1]);
    ibscl = 0;
    if (bnrm > 0.f && bnrm < smlnum) {

/*        Scale matrix norm up to SMLNUM */

	clascl_("G", &c__0, &c__0, &bnrm, &smlnum, m, nrhs, &b[b_offset], ldb,
		 info);
	ibscl = 1;
    } else if (bnrm > bignum) {

/*        Scale matrix norm down to BIGNUM */

	clascl_("G", &c__0, &c__0, &bnrm, &bignum, m, nrhs, &b[b_offset], ldb,
		 info);
	ibscl = 2;
    }

/*     Overdetermined case */

    if (*m >= *n) {

/*        Path 1 - overdetermined or exactly determined */

	mm = *m;
	if (*m >= mnthr) {

/*           Path 1a - overdetermined, with many more rows than columns */

	    mm = *n;
	    itau = 1;
	    iwork = itau + *n;

/*           Compute A=Q*R */
/*           (CWorkspace: need 2*N, prefer N+N*NB) */
/*           (RWorkspace: none) */

	    i__1 = *lwork - iwork + 1;
	    cgeqrf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &i__1,
		     info);

/*           Multiply B by transpose(Q) */
/*           (CWorkspace: need N+NRHS, prefer N+NRHS*NB) */
/*           (RWorkspace: none) */

	    i__1 = *lwork - iwork + 1;
	    cunmqr_("L", "C", m, nrhs, n, &a[a_offset], lda, &work[itau], &b[
		    b_offset], ldb, &work[iwork], &i__1, info);

/*           Zero out below R */

	    if (*n > 1) {
		i__1 = *n - 1;
		i__2 = *n - 1;
		claset_("L", &i__1, &i__2, &c_b1, &c_b1, &a[a_dim1 + 2], lda);
	    }
	}

	ie = 1;
	itauq = 1;
	itaup = itauq + *n;
	iwork = itaup + *n;

/*        Bidiagonalize R in A */
/*        (CWorkspace: need 2*N+MM, prefer 2*N+(MM+N)*NB) */
/*        (RWorkspace: need N) */

	i__1 = *lwork - iwork + 1;
	cgebrd_(&mm, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], &
		work[itaup], &work[iwork], &i__1, info);

/*        Multiply B by transpose of left bidiagonalizing vectors of R */
/*        (CWorkspace: need 2*N+NRHS, prefer 2*N+NRHS*NB) */
/*        (RWorkspace: none) */

	i__1 = *lwork - iwork + 1;
	cunmbr_("Q", "L", "C", &mm, nrhs, n, &a[a_offset], lda, &work[itauq], 
		&b[b_offset], ldb, &work[iwork], &i__1, info);

/*        Generate right bidiagonalizing vectors of R in A */
/*        (CWorkspace: need 3*N-1, prefer 2*N+(N-1)*NB) */
/*        (RWorkspace: none) */

	i__1 = *lwork - iwork + 1;
	cungbr_("P", n, n, n, &a[a_offset], lda, &work[itaup], &work[iwork], &
		i__1, info);
	irwork = ie + *n;

/*        Perform bidiagonal QR iteration */
/*          multiply B by transpose of left singular vectors */
/*          compute right singular vectors in A */
/*        (CWorkspace: none) */
/*        (RWorkspace: need BDSPAC) */

	cbdsqr_("U", n, n, &c__0, nrhs, &s[1], &rwork[ie], &a[a_offset], lda, 
		dum, &c__1, &b[b_offset], ldb, &rwork[irwork], info);
	if (*info != 0) {
	    goto L70;
	}

/*        Multiply B by reciprocals of singular values */

/* Computing MAX */
	r__1 = *rcond * s[1];
	thr = f2cmax(r__1,sfmin);
	if (*rcond < 0.f) {
/* Computing MAX */
	    r__1 = eps * s[1];
	    thr = f2cmax(r__1,sfmin);
	}
	*rank = 0;
	i__1 = *n;
	for (i__ = 1; i__ <= i__1; ++i__) {
	    if (s[i__] > thr) {
		csrscl_(nrhs, &s[i__], &b[i__ + b_dim1], ldb);
		++(*rank);
	    } else {
		claset_("F", &c__1, nrhs, &c_b1, &c_b1, &b[i__ + b_dim1], ldb);
	    }
/* L10: */
	}

/*        Multiply B by right singular vectors */
/*        (CWorkspace: need N, prefer N*NRHS) */
/*        (RWorkspace: none) */

	if (*lwork >= *ldb * *nrhs && *nrhs > 1) {
	    cgemm_("C", "N", n, nrhs, n, &c_b2, &a[a_offset], lda, &b[
		    b_offset], ldb, &c_b1, &work[1], ldb);
	    clacpy_("G", n, nrhs, &work[1], ldb, &b[b_offset], ldb)
		    ;
	} else if (*nrhs > 1) {
	    chunk = *lwork / *n;
	    i__1 = *nrhs;
	    i__2 = chunk;
	    for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += i__2) {
/* Computing MIN */
		i__3 = *nrhs - i__ + 1;
		bl = f2cmin(i__3,chunk);
		cgemm_("C", "N", n, &bl, n, &c_b2, &a[a_offset], lda, &b[i__ *
			 b_dim1 + 1], ldb, &c_b1, &work[1], n);
		clacpy_("G", n, &bl, &work[1], n, &b[i__ * b_dim1 + 1], ldb);
/* L20: */
	    }
	} else {
	    cgemv_("C", n, n, &c_b2, &a[a_offset], lda, &b[b_offset], &c__1, &
		    c_b1, &work[1], &c__1);
	    ccopy_(n, &work[1], &c__1, &b[b_offset], &c__1);
	}

    } else /* if(complicated condition) */ {
/* Computing MAX */
	i__2 = f2cmax(*m,*nrhs), i__1 = *n - (*m << 1);
	if (*n >= mnthr && *lwork >= *m * 3 + *m * *m + f2cmax(i__2,i__1)) {

/*        Underdetermined case, M much less than N */

/*        Path 2a - underdetermined, with many more columns than rows */
/*        and sufficient workspace for an efficient algorithm */

	    ldwork = *m;
/* Computing MAX */
	    i__2 = f2cmax(*m,*nrhs), i__1 = *n - (*m << 1);
	    if (*lwork >= *m * 3 + *m * *lda + f2cmax(i__2,i__1)) {
		ldwork = *lda;
	    }
	    itau = 1;
	    iwork = *m + 1;

/*        Compute A=L*Q */
/*        (CWorkspace: need 2*M, prefer M+M*NB) */
/*        (RWorkspace: none) */

	    i__2 = *lwork - iwork + 1;
	    cgelqf_(m, n, &a[a_offset], lda, &work[itau], &work[iwork], &i__2,
		     info);
	    il = iwork;

/*        Copy L to WORK(IL), zeroing out above it */

	    clacpy_("L", m, m, &a[a_offset], lda, &work[il], &ldwork);
	    i__2 = *m - 1;
	    i__1 = *m - 1;
	    claset_("U", &i__2, &i__1, &c_b1, &c_b1, &work[il + ldwork], &
		    ldwork);
	    ie = 1;
	    itauq = il + ldwork * *m;
	    itaup = itauq + *m;
	    iwork = itaup + *m;

/*        Bidiagonalize L in WORK(IL) */
/*        (CWorkspace: need M*M+4*M, prefer M*M+3*M+2*M*NB) */
/*        (RWorkspace: need M) */

	    i__2 = *lwork - iwork + 1;
	    cgebrd_(m, m, &work[il], &ldwork, &s[1], &rwork[ie], &work[itauq],
		     &work[itaup], &work[iwork], &i__2, info);

/*        Multiply B by transpose of left bidiagonalizing vectors of L */
/*        (CWorkspace: need M*M+3*M+NRHS, prefer M*M+3*M+NRHS*NB) */
/*        (RWorkspace: none) */

	    i__2 = *lwork - iwork + 1;
	    cunmbr_("Q", "L", "C", m, nrhs, m, &work[il], &ldwork, &work[
		    itauq], &b[b_offset], ldb, &work[iwork], &i__2, info);

/*        Generate right bidiagonalizing vectors of R in WORK(IL) */
/*        (CWorkspace: need M*M+4*M-1, prefer M*M+3*M+(M-1)*NB) */
/*        (RWorkspace: none) */

	    i__2 = *lwork - iwork + 1;
	    cungbr_("P", m, m, m, &work[il], &ldwork, &work[itaup], &work[
		    iwork], &i__2, info);
	    irwork = ie + *m;

/*        Perform bidiagonal QR iteration, computing right singular */
/*        vectors of L in WORK(IL) and multiplying B by transpose of */
/*        left singular vectors */
/*        (CWorkspace: need M*M) */
/*        (RWorkspace: need BDSPAC) */

	    cbdsqr_("U", m, m, &c__0, nrhs, &s[1], &rwork[ie], &work[il], &
		    ldwork, &a[a_offset], lda, &b[b_offset], ldb, &rwork[
		    irwork], info);
	    if (*info != 0) {
		goto L70;
	    }

/*        Multiply B by reciprocals of singular values */

/* Computing MAX */
	    r__1 = *rcond * s[1];
	    thr = f2cmax(r__1,sfmin);
	    if (*rcond < 0.f) {
/* Computing MAX */
		r__1 = eps * s[1];
		thr = f2cmax(r__1,sfmin);
	    }
	    *rank = 0;
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		if (s[i__] > thr) {
		    csrscl_(nrhs, &s[i__], &b[i__ + b_dim1], ldb);
		    ++(*rank);
		} else {
		    claset_("F", &c__1, nrhs, &c_b1, &c_b1, &b[i__ + b_dim1], 
			    ldb);
		}
/* L30: */
	    }
	    iwork = il + *m * ldwork;

/*        Multiply B by right singular vectors of L in WORK(IL) */
/*        (CWorkspace: need M*M+2*M, prefer M*M+M+M*NRHS) */
/*        (RWorkspace: none) */

	    if (*lwork >= *ldb * *nrhs + iwork - 1 && *nrhs > 1) {
		cgemm_("C", "N", m, nrhs, m, &c_b2, &work[il], &ldwork, &b[
			b_offset], ldb, &c_b1, &work[iwork], ldb);
		clacpy_("G", m, nrhs, &work[iwork], ldb, &b[b_offset], ldb);
	    } else if (*nrhs > 1) {
		chunk = (*lwork - iwork + 1) / *m;
		i__2 = *nrhs;
		i__1 = chunk;
		for (i__ = 1; i__1 < 0 ? i__ >= i__2 : i__ <= i__2; i__ += 
			i__1) {
/* Computing MIN */
		    i__3 = *nrhs - i__ + 1;
		    bl = f2cmin(i__3,chunk);
		    cgemm_("C", "N", m, &bl, m, &c_b2, &work[il], &ldwork, &b[
			    i__ * b_dim1 + 1], ldb, &c_b1, &work[iwork], m);
		    clacpy_("G", m, &bl, &work[iwork], m, &b[i__ * b_dim1 + 1]
			    , ldb);
/* L40: */
		}
	    } else {
		cgemv_("C", m, m, &c_b2, &work[il], &ldwork, &b[b_dim1 + 1], &
			c__1, &c_b1, &work[iwork], &c__1);
		ccopy_(m, &work[iwork], &c__1, &b[b_dim1 + 1], &c__1);
	    }

/*        Zero out below first M rows of B */

	    i__1 = *n - *m;
	    claset_("F", &i__1, nrhs, &c_b1, &c_b1, &b[*m + 1 + b_dim1], ldb);
	    iwork = itau + *m;

/*        Multiply transpose(Q) by B */
/*        (CWorkspace: need M+NRHS, prefer M+NHRS*NB) */
/*        (RWorkspace: none) */

	    i__1 = *lwork - iwork + 1;
	    cunmlq_("L", "C", n, nrhs, m, &a[a_offset], lda, &work[itau], &b[
		    b_offset], ldb, &work[iwork], &i__1, info);

	} else {

/*        Path 2 - remaining underdetermined cases */

	    ie = 1;
	    itauq = 1;
	    itaup = itauq + *m;
	    iwork = itaup + *m;

/*        Bidiagonalize A */
/*        (CWorkspace: need 3*M, prefer 2*M+(M+N)*NB) */
/*        (RWorkspace: need N) */

	    i__1 = *lwork - iwork + 1;
	    cgebrd_(m, n, &a[a_offset], lda, &s[1], &rwork[ie], &work[itauq], 
		    &work[itaup], &work[iwork], &i__1, info);

/*        Multiply B by transpose of left bidiagonalizing vectors */
/*        (CWorkspace: need 2*M+NRHS, prefer 2*M+NRHS*NB) */
/*        (RWorkspace: none) */

	    i__1 = *lwork - iwork + 1;
	    cunmbr_("Q", "L", "C", m, nrhs, n, &a[a_offset], lda, &work[itauq]
		    , &b[b_offset], ldb, &work[iwork], &i__1, info);

/*        Generate right bidiagonalizing vectors in A */
/*        (CWorkspace: need 3*M, prefer 2*M+M*NB) */
/*        (RWorkspace: none) */

	    i__1 = *lwork - iwork + 1;
	    cungbr_("P", m, n, m, &a[a_offset], lda, &work[itaup], &work[
		    iwork], &i__1, info);
	    irwork = ie + *m;

/*        Perform bidiagonal QR iteration, */
/*           computing right singular vectors of A in A and */
/*           multiplying B by transpose of left singular vectors */
/*        (CWorkspace: none) */
/*        (RWorkspace: need BDSPAC) */

	    cbdsqr_("L", m, n, &c__0, nrhs, &s[1], &rwork[ie], &a[a_offset], 
		    lda, dum, &c__1, &b[b_offset], ldb, &rwork[irwork], info);
	    if (*info != 0) {
		goto L70;
	    }

/*        Multiply B by reciprocals of singular values */

/* Computing MAX */
	    r__1 = *rcond * s[1];
	    thr = f2cmax(r__1,sfmin);
	    if (*rcond < 0.f) {
/* Computing MAX */
		r__1 = eps * s[1];
		thr = f2cmax(r__1,sfmin);
	    }
	    *rank = 0;
	    i__1 = *m;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		if (s[i__] > thr) {
		    csrscl_(nrhs, &s[i__], &b[i__ + b_dim1], ldb);
		    ++(*rank);
		} else {
		    claset_("F", &c__1, nrhs, &c_b1, &c_b1, &b[i__ + b_dim1], 
			    ldb);
		}
/* L50: */
	    }

/*        Multiply B by right singular vectors of A */
/*        (CWorkspace: need N, prefer N*NRHS) */
/*        (RWorkspace: none) */

	    if (*lwork >= *ldb * *nrhs && *nrhs > 1) {
		cgemm_("C", "N", n, nrhs, m, &c_b2, &a[a_offset], lda, &b[
			b_offset], ldb, &c_b1, &work[1], ldb);
		clacpy_("G", n, nrhs, &work[1], ldb, &b[b_offset], ldb);
	    } else if (*nrhs > 1) {
		chunk = *lwork / *n;
		i__1 = *nrhs;
		i__2 = chunk;
		for (i__ = 1; i__2 < 0 ? i__ >= i__1 : i__ <= i__1; i__ += 
			i__2) {
/* Computing MIN */
		    i__3 = *nrhs - i__ + 1;
		    bl = f2cmin(i__3,chunk);
		    cgemm_("C", "N", n, &bl, m, &c_b2, &a[a_offset], lda, &b[
			    i__ * b_dim1 + 1], ldb, &c_b1, &work[1], n);
		    clacpy_("F", n, &bl, &work[1], n, &b[i__ * b_dim1 + 1], 
			    ldb);
/* L60: */
		}
	    } else {
		cgemv_("C", m, n, &c_b2, &a[a_offset], lda, &b[b_offset], &
			c__1, &c_b1, &work[1], &c__1);
		ccopy_(n, &work[1], &c__1, &b[b_offset], &c__1);
	    }
	}
    }

/*     Undo scaling */

    if (iascl == 1) {
	clascl_("G", &c__0, &c__0, &anrm, &smlnum, n, nrhs, &b[b_offset], ldb,
		 info);
	slascl_("G", &c__0, &c__0, &smlnum, &anrm, &minmn, &c__1, &s[1], &
		minmn, info);
    } else if (iascl == 2) {
	clascl_("G", &c__0, &c__0, &anrm, &bignum, n, nrhs, &b[b_offset], ldb,
		 info);
	slascl_("G", &c__0, &c__0, &bignum, &anrm, &minmn, &c__1, &s[1], &
		minmn, info);
    }
    if (ibscl == 1) {
	clascl_("G", &c__0, &c__0, &smlnum, &bnrm, n, nrhs, &b[b_offset], ldb,
		 info);
    } else if (ibscl == 2) {
	clascl_("G", &c__0, &c__0, &bignum, &bnrm, n, nrhs, &b[b_offset], ldb,
		 info);
    }
L70:
    work[1].r = (real) maxwrk, work[1].i = 0.f;
    return;

/*     End of CGELSS */

} /* cgelss_ */

