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
static logical c_false = FALSE_;
static integer c__2 = 2;
static real c_b21 = 1.f;
static real c_b25 = 0.f;
static logical c_true = TRUE_;

/* > \brief \b SLAQTR solves a real quasi-triangular system of equations, or a complex quasi-triangular system
 of special form, in real arithmetic. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SLAQTR + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaqtr.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaqtr.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaqtr.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SLAQTR( LTRAN, LREAL, N, T, LDT, B, W, SCALE, X, WORK, */
/*                          INFO ) */

/*       LOGICAL            LREAL, LTRAN */
/*       INTEGER            INFO, LDT, N */
/*       REAL               SCALE, W */
/*       REAL               B( * ), T( LDT, * ), WORK( * ), X( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SLAQTR solves the real quasi-triangular system */
/* > */
/* >              op(T)*p = scale*c,               if LREAL = .TRUE. */
/* > */
/* > or the complex quasi-triangular systems */
/* > */
/* >            op(T + iB)*(p+iq) = scale*(c+id),  if LREAL = .FALSE. */
/* > */
/* > in real arithmetic, where T is upper quasi-triangular. */
/* > If LREAL = .FALSE., then the first diagonal block of T must be */
/* > 1 by 1, B is the specially structured matrix */
/* > */
/* >                B = [ b(1) b(2) ... b(n) ] */
/* >                    [       w            ] */
/* >                    [           w        ] */
/* >                    [              .     ] */
/* >                    [                 w  ] */
/* > */
/* > op(A) = A or A**T, A**T denotes the transpose of */
/* > matrix A. */
/* > */
/* > On input, X = [ c ].  On output, X = [ p ]. */
/* >               [ d ]                  [ q ] */
/* > */
/* > This subroutine is designed for the condition number estimation */
/* > in routine STRSNA. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] LTRAN */
/* > \verbatim */
/* >          LTRAN is LOGICAL */
/* >          On entry, LTRAN specifies the option of conjugate transpose: */
/* >             = .FALSE.,    op(T+i*B) = T+i*B, */
/* >             = .TRUE.,     op(T+i*B) = (T+i*B)**T. */
/* > \endverbatim */
/* > */
/* > \param[in] LREAL */
/* > \verbatim */
/* >          LREAL is LOGICAL */
/* >          On entry, LREAL specifies the input matrix structure: */
/* >             = .FALSE.,    the input is complex */
/* >             = .TRUE.,     the input is real */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          On entry, N specifies the order of T+i*B. N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] T */
/* > \verbatim */
/* >          T is REAL array, dimension (LDT,N) */
/* >          On entry, T contains a matrix in Schur canonical form. */
/* >          If LREAL = .FALSE., then the first diagonal block of T must */
/* >          be 1 by 1. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the matrix T. LDT >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in] B */
/* > \verbatim */
/* >          B is REAL array, dimension (N) */
/* >          On entry, B contains the elements to form the matrix */
/* >          B as described above. */
/* >          If LREAL = .TRUE., B is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] W */
/* > \verbatim */
/* >          W is REAL */
/* >          On entry, W is the diagonal element of the matrix B. */
/* >          If LREAL = .TRUE., W is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] SCALE */
/* > \verbatim */
/* >          SCALE is REAL */
/* >          On exit, SCALE is the scale factor. */
/* > \endverbatim */
/* > */
/* > \param[in,out] X */
/* > \verbatim */
/* >          X is REAL array, dimension (2*N) */
/* >          On entry, X contains the right hand side of the system. */
/* >          On exit, X is overwritten by the solution. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          On exit, INFO is set to */
/* >             0: successful exit. */
/* >               1: the some diagonal 1 by 1 block has been perturbed by */
/* >                  a small number SMIN to keep nonsingularity. */
/* >               2: the some diagonal 2 by 2 block has been perturbed by */
/* >                  a small number in SLALN2 to keep nonsingularity. */
/* >          NOTE: In the interests of speed, this routine does not */
/* >                check the inputs for errors. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup realOTHERauxiliary */

/*  ===================================================================== */
/* Subroutine */ void slaqtr_(logical *ltran, logical *lreal, integer *n, real 
	*t, integer *ldt, real *b, real *w, real *scale, real *x, real *work, 
	integer *info)
{
    /* System generated locals */
    integer t_dim1, t_offset, i__1, i__2;
    real r__1, r__2, r__3, r__4, r__5, r__6;

    /* Local variables */
    integer ierr;
    real smin;
    extern real sdot_(integer *, real *, integer *, real *, integer *);
    real xmax, d__[4]	/* was [2][2] */;
    integer i__, j, k;
    real v[4]	/* was [2][2] */, z__;
    extern /* Subroutine */ void sscal_(integer *, real *, real *, integer *);
    integer jnext;
    extern real sasum_(integer *, real *, integer *);
    integer j1, j2;
    real sminw;
    integer n1, n2;
    real xnorm;
    extern /* Subroutine */ void saxpy_(integer *, real *, real *, integer *, 
	    real *, integer *), slaln2_(logical *, integer *, integer *, real 
	    *, real *, real *, integer *, real *, real *, real *, integer *, 
	    real *, real *, real *, integer *, real *, real *, integer *);
    real si, xj, scaloc, sr;
    extern real slamch_(char *), slange_(char *, integer *, integer *,
	     real *, integer *, real *);
    real bignum;
    extern integer isamax_(integer *, real *, integer *);
    extern /* Subroutine */ void sladiv_(real *, real *, real *, real *, real *
	    , real *);
    logical notran;
    real smlnum, rec, eps, tjj, tmp;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/* ===================================================================== */


/*     Do not test the input parameters for errors */

    /* Parameter adjustments */
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    --b;
    --x;
    --work;

    /* Function Body */
    notran = ! (*ltran);
    *info = 0;

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }

/*     Set constants to control overflow */

    eps = slamch_("P");
    smlnum = slamch_("S") / eps;
    bignum = 1.f / smlnum;

    xnorm = slange_("M", n, n, &t[t_offset], ldt, d__);
    if (! (*lreal)) {
/* Computing MAX */
	r__1 = xnorm, r__2 = abs(*w), r__1 = f2cmax(r__1,r__2), r__2 = slange_(
		"M", n, &c__1, &b[1], n, d__);
	xnorm = f2cmax(r__1,r__2);
    }
/* Computing MAX */
    r__1 = smlnum, r__2 = eps * xnorm;
    smin = f2cmax(r__1,r__2);

/*     Compute 1-norm of each column of strictly upper triangular */
/*     part of T to control overflow in triangular solver. */

    work[1] = 0.f;
    i__1 = *n;
    for (j = 2; j <= i__1; ++j) {
	i__2 = j - 1;
	work[j] = sasum_(&i__2, &t[j * t_dim1 + 1], &c__1);
/* L10: */
    }

    if (! (*lreal)) {
	i__1 = *n;
	for (i__ = 2; i__ <= i__1; ++i__) {
	    work[i__] += (r__1 = b[i__], abs(r__1));
/* L20: */
	}
    }

    n2 = *n << 1;
    n1 = *n;
    if (! (*lreal)) {
	n1 = n2;
    }
    k = isamax_(&n1, &x[1], &c__1);
    xmax = (r__1 = x[k], abs(r__1));
    *scale = 1.f;

    if (xmax > bignum) {
	*scale = bignum / xmax;
	sscal_(&n1, scale, &x[1], &c__1);
	xmax = bignum;
    }

    if (*lreal) {

	if (notran) {

/*           Solve T*p = scale*c */

	    jnext = *n;
	    for (j = *n; j >= 1; --j) {
		if (j > jnext) {
		    goto L30;
		}
		j1 = j;
		j2 = j;
		jnext = j - 1;
		if (j > 1) {
		    if (t[j + (j - 1) * t_dim1] != 0.f) {
			j1 = j - 1;
			jnext = j - 2;
		    }
		}

		if (j1 == j2) {

/*                 Meet 1 by 1 diagonal block */

/*                 Scale to avoid overflow when computing */
/*                     x(j) = b(j)/T(j,j) */

		    xj = (r__1 = x[j1], abs(r__1));
		    tjj = (r__1 = t[j1 + j1 * t_dim1], abs(r__1));
		    tmp = t[j1 + j1 * t_dim1];
		    if (tjj < smin) {
			tmp = smin;
			tjj = smin;
			*info = 1;
		    }

		    if (xj == 0.f) {
			goto L30;
		    }

		    if (tjj < 1.f) {
			if (xj > bignum * tjj) {
			    rec = 1.f / xj;
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }
		    x[j1] /= tmp;
		    xj = (r__1 = x[j1], abs(r__1));

/*                 Scale x if necessary to avoid overflow when adding a */
/*                 multiple of column j1 of T. */

		    if (xj > 1.f) {
			rec = 1.f / xj;
			if (work[j1] > (bignum - xmax) * rec) {
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			}
		    }
		    if (j1 > 1) {
			i__1 = j1 - 1;
			r__1 = -x[j1];
			saxpy_(&i__1, &r__1, &t[j1 * t_dim1 + 1], &c__1, &x[1]
				, &c__1);
			i__1 = j1 - 1;
			k = isamax_(&i__1, &x[1], &c__1);
			xmax = (r__1 = x[k], abs(r__1));
		    }

		} else {

/*                 Meet 2 by 2 diagonal block */

/*                 Call 2 by 2 linear system solve, to take */
/*                 care of possible overflow by scaling factor. */

		    d__[0] = x[j1];
		    d__[1] = x[j2];
		    slaln2_(&c_false, &c__2, &c__1, &smin, &c_b21, &t[j1 + j1 
			    * t_dim1], ldt, &c_b21, &c_b21, d__, &c__2, &
			    c_b25, &c_b25, v, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 2;
		    }

		    if (scaloc != 1.f) {
			sscal_(n, &scaloc, &x[1], &c__1);
			*scale *= scaloc;
		    }
		    x[j1] = v[0];
		    x[j2] = v[1];

/*                 Scale V(1,1) (= X(J1)) and/or V(2,1) (=X(J2)) */
/*                 to avoid overflow in updating right-hand side. */

/* Computing MAX */
		    r__1 = abs(v[0]), r__2 = abs(v[1]);
		    xj = f2cmax(r__1,r__2);
		    if (xj > 1.f) {
			rec = 1.f / xj;
/* Computing MAX */
			r__1 = work[j1], r__2 = work[j2];
			if (f2cmax(r__1,r__2) > (bignum - xmax) * rec) {
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			}
		    }

/*                 Update right-hand side */

		    if (j1 > 1) {
			i__1 = j1 - 1;
			r__1 = -x[j1];
			saxpy_(&i__1, &r__1, &t[j1 * t_dim1 + 1], &c__1, &x[1]
				, &c__1);
			i__1 = j1 - 1;
			r__1 = -x[j2];
			saxpy_(&i__1, &r__1, &t[j2 * t_dim1 + 1], &c__1, &x[1]
				, &c__1);
			i__1 = j1 - 1;
			k = isamax_(&i__1, &x[1], &c__1);
			xmax = (r__1 = x[k], abs(r__1));
		    }

		}

L30:
		;
	    }

	} else {

/*           Solve T**T*p = scale*c */

	    jnext = 1;
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		if (j < jnext) {
		    goto L40;
		}
		j1 = j;
		j2 = j;
		jnext = j + 1;
		if (j < *n) {
		    if (t[j + 1 + j * t_dim1] != 0.f) {
			j2 = j + 1;
			jnext = j + 2;
		    }
		}

		if (j1 == j2) {

/*                 1 by 1 diagonal block */

/*                 Scale if necessary to avoid overflow in forming the */
/*                 right-hand side element by inner product. */

		    xj = (r__1 = x[j1], abs(r__1));
		    if (xmax > 1.f) {
			rec = 1.f / xmax;
			if (work[j1] > (bignum - xj) * rec) {
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }

		    i__2 = j1 - 1;
		    x[j1] -= sdot_(&i__2, &t[j1 * t_dim1 + 1], &c__1, &x[1], &
			    c__1);

		    xj = (r__1 = x[j1], abs(r__1));
		    tjj = (r__1 = t[j1 + j1 * t_dim1], abs(r__1));
		    tmp = t[j1 + j1 * t_dim1];
		    if (tjj < smin) {
			tmp = smin;
			tjj = smin;
			*info = 1;
		    }

		    if (tjj < 1.f) {
			if (xj > bignum * tjj) {
			    rec = 1.f / xj;
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }
		    x[j1] /= tmp;
/* Computing MAX */
		    r__2 = xmax, r__3 = (r__1 = x[j1], abs(r__1));
		    xmax = f2cmax(r__2,r__3);

		} else {

/*                 2 by 2 diagonal block */

/*                 Scale if necessary to avoid overflow in forming the */
/*                 right-hand side elements by inner product. */

/* Computing MAX */
		    r__3 = (r__1 = x[j1], abs(r__1)), r__4 = (r__2 = x[j2], 
			    abs(r__2));
		    xj = f2cmax(r__3,r__4);
		    if (xmax > 1.f) {
			rec = 1.f / xmax;
/* Computing MAX */
			r__1 = work[j2], r__2 = work[j1];
			if (f2cmax(r__1,r__2) > (bignum - xj) * rec) {
			    sscal_(n, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }

		    i__2 = j1 - 1;
		    d__[0] = x[j1] - sdot_(&i__2, &t[j1 * t_dim1 + 1], &c__1, 
			    &x[1], &c__1);
		    i__2 = j1 - 1;
		    d__[1] = x[j2] - sdot_(&i__2, &t[j2 * t_dim1 + 1], &c__1, 
			    &x[1], &c__1);

		    slaln2_(&c_true, &c__2, &c__1, &smin, &c_b21, &t[j1 + j1 *
			     t_dim1], ldt, &c_b21, &c_b21, d__, &c__2, &c_b25,
			     &c_b25, v, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 2;
		    }

		    if (scaloc != 1.f) {
			sscal_(n, &scaloc, &x[1], &c__1);
			*scale *= scaloc;
		    }
		    x[j1] = v[0];
		    x[j2] = v[1];
/* Computing MAX */
		    r__3 = (r__1 = x[j1], abs(r__1)), r__4 = (r__2 = x[j2], 
			    abs(r__2)), r__3 = f2cmax(r__3,r__4);
		    xmax = f2cmax(r__3,xmax);

		}
L40:
		;
	    }
	}

    } else {

/* Computing MAX */
	r__1 = eps * abs(*w);
	sminw = f2cmax(r__1,smin);
	if (notran) {

/*           Solve (T + iB)*(p+iq) = c+id */

	    jnext = *n;
	    for (j = *n; j >= 1; --j) {
		if (j > jnext) {
		    goto L70;
		}
		j1 = j;
		j2 = j;
		jnext = j - 1;
		if (j > 1) {
		    if (t[j + (j - 1) * t_dim1] != 0.f) {
			j1 = j - 1;
			jnext = j - 2;
		    }
		}

		if (j1 == j2) {

/*                 1 by 1 diagonal block */

/*                 Scale if necessary to avoid overflow in division */

		    z__ = *w;
		    if (j1 == 1) {
			z__ = b[1];
		    }
		    xj = (r__1 = x[j1], abs(r__1)) + (r__2 = x[*n + j1], abs(
			    r__2));
		    tjj = (r__1 = t[j1 + j1 * t_dim1], abs(r__1)) + abs(z__);
		    tmp = t[j1 + j1 * t_dim1];
		    if (tjj < sminw) {
			tmp = sminw;
			tjj = sminw;
			*info = 1;
		    }

		    if (xj == 0.f) {
			goto L70;
		    }

		    if (tjj < 1.f) {
			if (xj > bignum * tjj) {
			    rec = 1.f / xj;
			    sscal_(&n2, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }
		    sladiv_(&x[j1], &x[*n + j1], &tmp, &z__, &sr, &si);
		    x[j1] = sr;
		    x[*n + j1] = si;
		    xj = (r__1 = x[j1], abs(r__1)) + (r__2 = x[*n + j1], abs(
			    r__2));

/*                 Scale x if necessary to avoid overflow when adding a */
/*                 multiple of column j1 of T. */

		    if (xj > 1.f) {
			rec = 1.f / xj;
			if (work[j1] > (bignum - xmax) * rec) {
			    sscal_(&n2, &rec, &x[1], &c__1);
			    *scale *= rec;
			}
		    }

		    if (j1 > 1) {
			i__1 = j1 - 1;
			r__1 = -x[j1];
			saxpy_(&i__1, &r__1, &t[j1 * t_dim1 + 1], &c__1, &x[1]
				, &c__1);
			i__1 = j1 - 1;
			r__1 = -x[*n + j1];
			saxpy_(&i__1, &r__1, &t[j1 * t_dim1 + 1], &c__1, &x[*
				n + 1], &c__1);

			x[1] += b[j1] * x[*n + j1];
			x[*n + 1] -= b[j1] * x[j1];

			xmax = 0.f;
			i__1 = j1 - 1;
			for (k = 1; k <= i__1; ++k) {
/* Computing MAX */
			    r__3 = xmax, r__4 = (r__1 = x[k], abs(r__1)) + (
				    r__2 = x[k + *n], abs(r__2));
			    xmax = f2cmax(r__3,r__4);
/* L50: */
			}
		    }

		} else {

/*                 Meet 2 by 2 diagonal block */

		    d__[0] = x[j1];
		    d__[1] = x[j2];
		    d__[2] = x[*n + j1];
		    d__[3] = x[*n + j2];
		    r__1 = -(*w);
		    slaln2_(&c_false, &c__2, &c__2, &sminw, &c_b21, &t[j1 + 
			    j1 * t_dim1], ldt, &c_b21, &c_b21, d__, &c__2, &
			    c_b25, &r__1, v, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 2;
		    }

		    if (scaloc != 1.f) {
			i__1 = *n << 1;
			sscal_(&i__1, &scaloc, &x[1], &c__1);
			*scale = scaloc * *scale;
		    }
		    x[j1] = v[0];
		    x[j2] = v[1];
		    x[*n + j1] = v[2];
		    x[*n + j2] = v[3];

/*                 Scale X(J1), .... to avoid overflow in */
/*                 updating right hand side. */

/* Computing MAX */
		    r__1 = abs(v[0]) + abs(v[2]), r__2 = abs(v[1]) + abs(v[3])
			    ;
		    xj = f2cmax(r__1,r__2);
		    if (xj > 1.f) {
			rec = 1.f / xj;
/* Computing MAX */
			r__1 = work[j1], r__2 = work[j2];
			if (f2cmax(r__1,r__2) > (bignum - xmax) * rec) {
			    sscal_(&n2, &rec, &x[1], &c__1);
			    *scale *= rec;
			}
		    }

/*                 Update the right-hand side. */

		    if (j1 > 1) {
			i__1 = j1 - 1;
			r__1 = -x[j1];
			saxpy_(&i__1, &r__1, &t[j1 * t_dim1 + 1], &c__1, &x[1]
				, &c__1);
			i__1 = j1 - 1;
			r__1 = -x[j2];
			saxpy_(&i__1, &r__1, &t[j2 * t_dim1 + 1], &c__1, &x[1]
				, &c__1);

			i__1 = j1 - 1;
			r__1 = -x[*n + j1];
			saxpy_(&i__1, &r__1, &t[j1 * t_dim1 + 1], &c__1, &x[*
				n + 1], &c__1);
			i__1 = j1 - 1;
			r__1 = -x[*n + j2];
			saxpy_(&i__1, &r__1, &t[j2 * t_dim1 + 1], &c__1, &x[*
				n + 1], &c__1);

			x[1] = x[1] + b[j1] * x[*n + j1] + b[j2] * x[*n + j2];
			x[*n + 1] = x[*n + 1] - b[j1] * x[j1] - b[j2] * x[j2];

			xmax = 0.f;
			i__1 = j1 - 1;
			for (k = 1; k <= i__1; ++k) {
/* Computing MAX */
			    r__3 = (r__1 = x[k], abs(r__1)) + (r__2 = x[k + *
				    n], abs(r__2));
			    xmax = f2cmax(r__3,xmax);
/* L60: */
			}
		    }

		}
L70:
		;
	    }

	} else {

/*           Solve (T + iB)**T*(p+iq) = c+id */

	    jnext = 1;
	    i__1 = *n;
	    for (j = 1; j <= i__1; ++j) {
		if (j < jnext) {
		    goto L80;
		}
		j1 = j;
		j2 = j;
		jnext = j + 1;
		if (j < *n) {
		    if (t[j + 1 + j * t_dim1] != 0.f) {
			j2 = j + 1;
			jnext = j + 2;
		    }
		}

		if (j1 == j2) {

/*                 1 by 1 diagonal block */

/*                 Scale if necessary to avoid overflow in forming the */
/*                 right-hand side element by inner product. */

		    xj = (r__1 = x[j1], abs(r__1)) + (r__2 = x[j1 + *n], abs(
			    r__2));
		    if (xmax > 1.f) {
			rec = 1.f / xmax;
			if (work[j1] > (bignum - xj) * rec) {
			    sscal_(&n2, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }

		    i__2 = j1 - 1;
		    x[j1] -= sdot_(&i__2, &t[j1 * t_dim1 + 1], &c__1, &x[1], &
			    c__1);
		    i__2 = j1 - 1;
		    x[*n + j1] -= sdot_(&i__2, &t[j1 * t_dim1 + 1], &c__1, &x[
			    *n + 1], &c__1);
		    if (j1 > 1) {
			x[j1] -= b[j1] * x[*n + 1];
			x[*n + j1] += b[j1] * x[1];
		    }
		    xj = (r__1 = x[j1], abs(r__1)) + (r__2 = x[j1 + *n], abs(
			    r__2));

		    z__ = *w;
		    if (j1 == 1) {
			z__ = b[1];
		    }

/*                 Scale if necessary to avoid overflow in */
/*                 complex division */

		    tjj = (r__1 = t[j1 + j1 * t_dim1], abs(r__1)) + abs(z__);
		    tmp = t[j1 + j1 * t_dim1];
		    if (tjj < sminw) {
			tmp = sminw;
			tjj = sminw;
			*info = 1;
		    }

		    if (tjj < 1.f) {
			if (xj > bignum * tjj) {
			    rec = 1.f / xj;
			    sscal_(&n2, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }
		    r__1 = -z__;
		    sladiv_(&x[j1], &x[*n + j1], &tmp, &r__1, &sr, &si);
		    x[j1] = sr;
		    x[j1 + *n] = si;
/* Computing MAX */
		    r__3 = (r__1 = x[j1], abs(r__1)) + (r__2 = x[j1 + *n], 
			    abs(r__2));
		    xmax = f2cmax(r__3,xmax);

		} else {

/*                 2 by 2 diagonal block */

/*                 Scale if necessary to avoid overflow in forming the */
/*                 right-hand side element by inner product. */

/* Computing MAX */
		    r__5 = (r__1 = x[j1], abs(r__1)) + (r__2 = x[*n + j1], 
			    abs(r__2)), r__6 = (r__3 = x[j2], abs(r__3)) + (
			    r__4 = x[*n + j2], abs(r__4));
		    xj = f2cmax(r__5,r__6);
		    if (xmax > 1.f) {
			rec = 1.f / xmax;
/* Computing MAX */
			r__1 = work[j1], r__2 = work[j2];
			if (f2cmax(r__1,r__2) > (bignum - xj) / xmax) {
			    sscal_(&n2, &rec, &x[1], &c__1);
			    *scale *= rec;
			    xmax *= rec;
			}
		    }

		    i__2 = j1 - 1;
		    d__[0] = x[j1] - sdot_(&i__2, &t[j1 * t_dim1 + 1], &c__1, 
			    &x[1], &c__1);
		    i__2 = j1 - 1;
		    d__[1] = x[j2] - sdot_(&i__2, &t[j2 * t_dim1 + 1], &c__1, 
			    &x[1], &c__1);
		    i__2 = j1 - 1;
		    d__[2] = x[*n + j1] - sdot_(&i__2, &t[j1 * t_dim1 + 1], &
			    c__1, &x[*n + 1], &c__1);
		    i__2 = j1 - 1;
		    d__[3] = x[*n + j2] - sdot_(&i__2, &t[j2 * t_dim1 + 1], &
			    c__1, &x[*n + 1], &c__1);
		    d__[0] -= b[j1] * x[*n + 1];
		    d__[1] -= b[j2] * x[*n + 1];
		    d__[2] += b[j1] * x[1];
		    d__[3] += b[j2] * x[1];

		    slaln2_(&c_true, &c__2, &c__2, &sminw, &c_b21, &t[j1 + j1 
			    * t_dim1], ldt, &c_b21, &c_b21, d__, &c__2, &
			    c_b25, w, v, &c__2, &scaloc, &xnorm, &ierr);
		    if (ierr != 0) {
			*info = 2;
		    }

		    if (scaloc != 1.f) {
			sscal_(&n2, &scaloc, &x[1], &c__1);
			*scale = scaloc * *scale;
		    }
		    x[j1] = v[0];
		    x[j2] = v[1];
		    x[*n + j1] = v[2];
		    x[*n + j2] = v[3];
/* Computing MAX */
		    r__5 = (r__1 = x[j1], abs(r__1)) + (r__2 = x[*n + j1], 
			    abs(r__2)), r__6 = (r__3 = x[j2], abs(r__3)) + (
			    r__4 = x[*n + j2], abs(r__4)), r__5 = f2cmax(r__5,
			    r__6);
		    xmax = f2cmax(r__5,xmax);

		}

L80:
		;
	    }

	}

    }

    return;

/*     End of SLAQTR */

} /* slaqtr_ */

