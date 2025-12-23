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
static integer c_n1 = -1;
static logical c_true = TRUE_;

/* > \brief \b CLAQR2 performs the unitary similarity transformation of a Hessenberg matrix to detect and defl
ate fully converged eigenvalues from a trailing principal submatrix (aggressive early deflation). */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CLAQR2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/claqr2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/claqr2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/claqr2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CLAQR2( WANTT, WANTZ, N, KTOP, KBOT, NW, H, LDH, ILOZ, */
/*                          IHIZ, Z, LDZ, NS, ND, SH, V, LDV, NH, T, LDT, */
/*                          NV, WV, LDWV, WORK, LWORK ) */

/*       INTEGER            IHIZ, ILOZ, KBOT, KTOP, LDH, LDT, LDV, LDWV, */
/*      $                   LDZ, LWORK, N, ND, NH, NS, NV, NW */
/*       LOGICAL            WANTT, WANTZ */
/*       COMPLEX            H( LDH, * ), SH( * ), T( LDT, * ), V( LDV, * ), */
/*      $                   WORK( * ), WV( LDWV, * ), Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    CLAQR2 is identical to CLAQR3 except that it avoids */
/* >    recursion by calling CLAHQR instead of CLAQR4. */
/* > */
/* >    Aggressive early deflation: */
/* > */
/* >    This subroutine accepts as input an upper Hessenberg matrix */
/* >    H and performs an unitary similarity transformation */
/* >    designed to detect and deflate fully converged eigenvalues from */
/* >    a trailing principal submatrix.  On output H has been over- */
/* >    written by a new Hessenberg matrix that is a perturbation of */
/* >    an unitary similarity transformation of H.  It is to be */
/* >    hoped that the final version of H has many zero subdiagonal */
/* >    entries. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] WANTT */
/* > \verbatim */
/* >          WANTT is LOGICAL */
/* >          If .TRUE., then the Hessenberg matrix H is fully updated */
/* >          so that the triangular Schur factor may be */
/* >          computed (in cooperation with the calling subroutine). */
/* >          If .FALSE., then only enough of H is updated to preserve */
/* >          the eigenvalues. */
/* > \endverbatim */
/* > */
/* > \param[in] WANTZ */
/* > \verbatim */
/* >          WANTZ is LOGICAL */
/* >          If .TRUE., then the unitary matrix Z is updated so */
/* >          so that the unitary Schur factor may be computed */
/* >          (in cooperation with the calling subroutine). */
/* >          If .FALSE., then Z is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrix H and (if WANTZ is .TRUE.) the */
/* >          order of the unitary matrix Z. */
/* > \endverbatim */
/* > */
/* > \param[in] KTOP */
/* > \verbatim */
/* >          KTOP is INTEGER */
/* >          It is assumed that either KTOP = 1 or H(KTOP,KTOP-1)=0. */
/* >          KBOT and KTOP together determine an isolated block */
/* >          along the diagonal of the Hessenberg matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] KBOT */
/* > \verbatim */
/* >          KBOT is INTEGER */
/* >          It is assumed without a check that either */
/* >          KBOT = N or H(KBOT+1,KBOT)=0.  KBOT and KTOP together */
/* >          determine an isolated block along the diagonal of the */
/* >          Hessenberg matrix. */
/* > \endverbatim */
/* > */
/* > \param[in] NW */
/* > \verbatim */
/* >          NW is INTEGER */
/* >          Deflation window size.  1 <= NW <= (KBOT-KTOP+1). */
/* > \endverbatim */
/* > */
/* > \param[in,out] H */
/* > \verbatim */
/* >          H is COMPLEX array, dimension (LDH,N) */
/* >          On input the initial N-by-N section of H stores the */
/* >          Hessenberg matrix undergoing aggressive early deflation. */
/* >          On output H has been transformed by a unitary */
/* >          similarity transformation, perturbed, and the returned */
/* >          to Hessenberg form that (it is to be hoped) has some */
/* >          zero subdiagonal entries. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >          Leading dimension of H just as declared in the calling */
/* >          subroutine.  N <= LDH */
/* > \endverbatim */
/* > */
/* > \param[in] ILOZ */
/* > \verbatim */
/* >          ILOZ is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] IHIZ */
/* > \verbatim */
/* >          IHIZ is INTEGER */
/* >          Specify the rows of Z to which transformations must be */
/* >          applied if WANTZ is .TRUE.. 1 <= ILOZ <= IHIZ <= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is COMPLEX array, dimension (LDZ,N) */
/* >          IF WANTZ is .TRUE., then on output, the unitary */
/* >          similarity transformation mentioned above has been */
/* >          accumulated into Z(ILOZ:IHIZ,ILOZ:IHIZ) from the right. */
/* >          If WANTZ is .FALSE., then Z is unreferenced. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of Z just as declared in the */
/* >          calling subroutine.  1 <= LDZ. */
/* > \endverbatim */
/* > */
/* > \param[out] NS */
/* > \verbatim */
/* >          NS is INTEGER */
/* >          The number of unconverged (ie approximate) eigenvalues */
/* >          returned in SR and SI that may be used as shifts by the */
/* >          calling subroutine. */
/* > \endverbatim */
/* > */
/* > \param[out] ND */
/* > \verbatim */
/* >          ND is INTEGER */
/* >          The number of converged eigenvalues uncovered by this */
/* >          subroutine. */
/* > \endverbatim */
/* > */
/* > \param[out] SH */
/* > \verbatim */
/* >          SH is COMPLEX array, dimension (KBOT) */
/* >          On output, approximate eigenvalues that may */
/* >          be used for shifts are stored in SH(KBOT-ND-NS+1) */
/* >          through SR(KBOT-ND).  Converged eigenvalues are */
/* >          stored in SH(KBOT-ND+1) through SH(KBOT). */
/* > \endverbatim */
/* > */
/* > \param[out] V */
/* > \verbatim */
/* >          V is COMPLEX array, dimension (LDV,NW) */
/* >          An NW-by-NW work array. */
/* > \endverbatim */
/* > */
/* > \param[in] LDV */
/* > \verbatim */
/* >          LDV is INTEGER */
/* >          The leading dimension of V just as declared in the */
/* >          calling subroutine.  NW <= LDV */
/* > \endverbatim */
/* > */
/* > \param[in] NH */
/* > \verbatim */
/* >          NH is INTEGER */
/* >          The number of columns of T.  NH >= NW. */
/* > \endverbatim */
/* > */
/* > \param[out] T */
/* > \verbatim */
/* >          T is COMPLEX array, dimension (LDT,NW) */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of T just as declared in the */
/* >          calling subroutine.  NW <= LDT */
/* > \endverbatim */
/* > */
/* > \param[in] NV */
/* > \verbatim */
/* >          NV is INTEGER */
/* >          The number of rows of work array WV available for */
/* >          workspace.  NV >= NW. */
/* > \endverbatim */
/* > */
/* > \param[out] WV */
/* > \verbatim */
/* >          WV is COMPLEX array, dimension (LDWV,NW) */
/* > \endverbatim */
/* > */
/* > \param[in] LDWV */
/* > \verbatim */
/* >          LDWV is INTEGER */
/* >          The leading dimension of W just as declared in the */
/* >          calling subroutine.  NW <= LDV */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX array, dimension (LWORK) */
/* >          On exit, WORK(1) is set to an estimate of the optimal value */
/* >          of LWORK for the given values of N, NW, KTOP and KBOT. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the work array WORK.  LWORK = 2*NW */
/* >          suffices, but greater efficiency may result from larger */
/* >          values of LWORK. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; CLAQR2 */
/* >          only estimates the optimal workspace size for the given */
/* >          values of N, NW, KTOP and KBOT.  The estimate is returned */
/* >          in WORK(1).  No error message related to LWORK is issued */
/* >          by XERBLA.  Neither H nor Z are accessed. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2017 */

/* > \ingroup complexOTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >       Karen Braman and Ralph Byers, Department of Mathematics, */
/* >       University of Kansas, USA */
/* > */
/*  ===================================================================== */
/* Subroutine */ void claqr2_(logical *wantt, logical *wantz, integer *n, 
	integer *ktop, integer *kbot, integer *nw, complex *h__, integer *ldh,
	 integer *iloz, integer *ihiz, complex *z__, integer *ldz, integer *
	ns, integer *nd, complex *sh, complex *v, integer *ldv, integer *nh, 
	complex *t, integer *ldt, integer *nv, complex *wv, integer *ldwv, 
	complex *work, integer *lwork)
{
    /* System generated locals */
    integer h_dim1, h_offset, t_dim1, t_offset, v_dim1, v_offset, wv_dim1, 
	    wv_offset, z_dim1, z_offset, i__1, i__2, i__3, i__4;
    real r__1, r__2, r__3, r__4, r__5, r__6;
    complex q__1, q__2;

    /* Local variables */
    complex beta;
    integer kcol, info, ifst, ilst, ltop, krow, i__, j;
    complex s;
    extern /* Subroutine */ void clarf_(char *, integer *, integer *, complex *
	    , integer *, complex *, complex *, integer *, complex *), 
	    cgemm_(char *, char *, integer *, integer *, integer *, complex *,
	     complex *, integer *, complex *, integer *, complex *, complex *,
	     integer *), ccopy_(integer *, complex *, integer 
	    *, complex *, integer *);
    integer infqr, kwtop;
    extern /* Subroutine */ void slabad_(real *, real *), cgehrd_(integer *, 
	    integer *, integer *, complex *, integer *, complex *, complex *, 
	    integer *, integer *), clarfg_(integer *, complex *, complex *, 
	    integer *, complex *);
    integer jw;
    extern real slamch_(char *);
    extern /* Subroutine */ void clahqr_(logical *, logical *, integer *, 
	    integer *, integer *, complex *, integer *, complex *, integer *, 
	    integer *, complex *, integer *, integer *), clacpy_(char *, 
	    integer *, integer *, complex *, integer *, complex *, integer *), claset_(char *, integer *, integer *, complex *, complex 
	    *, complex *, integer *);
    real safmin, safmax;
    extern /* Subroutine */ void ctrexc_(char *, integer *, complex *, integer 
	    *, complex *, integer *, integer *, integer *, integer *),
	     cunmhr_(char *, char *, integer *, integer *, integer *, integer 
	    *, complex *, integer *, complex *, complex *, integer *, complex 
	    *, integer *, integer *);
    real smlnum;
    integer lwkopt;
    real foo;
    integer kln;
    complex tau;
    integer knt;
    real ulp;
    integer lwk1, lwk2;


/*  -- LAPACK auxiliary routine (version 3.7.1) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2017 */


/*  ================================================================ */


/*     ==== Estimate optimal workspace. ==== */

    /* Parameter adjustments */
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --sh;
    v_dim1 = *ldv;
    v_offset = 1 + v_dim1 * 1;
    v -= v_offset;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    wv_dim1 = *ldwv;
    wv_offset = 1 + wv_dim1 * 1;
    wv -= wv_offset;
    --work;

    /* Function Body */
/* Computing MIN */
    i__1 = *nw, i__2 = *kbot - *ktop + 1;
    jw = f2cmin(i__1,i__2);
    if (jw <= 2) {
	lwkopt = 1;
    } else {

/*        ==== Workspace query call to CGEHRD ==== */

	i__1 = jw - 1;
	cgehrd_(&jw, &c__1, &i__1, &t[t_offset], ldt, &work[1], &work[1], &
		c_n1, &info);
	lwk1 = (integer) work[1].r;

/*        ==== Workspace query call to CUNMHR ==== */

	i__1 = jw - 1;
	cunmhr_("R", "N", &jw, &jw, &c__1, &i__1, &t[t_offset], ldt, &work[1],
		 &v[v_offset], ldv, &work[1], &c_n1, &info);
	lwk2 = (integer) work[1].r;

/*        ==== Optimal workspace ==== */

	lwkopt = jw + f2cmax(lwk1,lwk2);
    }

/*     ==== Quick return in case of workspace query. ==== */

    if (*lwork == -1) {
	r__1 = (real) lwkopt;
	q__1.r = r__1, q__1.i = 0.f;
	work[1].r = q__1.r, work[1].i = q__1.i;
	return;
    }

/*     ==== Nothing to do ... */
/*     ... for an empty active block ... ==== */
    *ns = 0;
    *nd = 0;
    work[1].r = 1.f, work[1].i = 0.f;
    if (*ktop > *kbot) {
	return;
    }
/*     ... nor for an empty deflation window. ==== */
    if (*nw < 1) {
	return;
    }

/*     ==== Machine constants ==== */

    safmin = slamch_("SAFE MINIMUM");
    safmax = 1.f / safmin;
    slabad_(&safmin, &safmax);
    ulp = slamch_("PRECISION");
    smlnum = safmin * ((real) (*n) / ulp);

/*     ==== Setup deflation window ==== */

/* Computing MIN */
    i__1 = *nw, i__2 = *kbot - *ktop + 1;
    jw = f2cmin(i__1,i__2);
    kwtop = *kbot - jw + 1;
    if (kwtop == *ktop) {
	s.r = 0.f, s.i = 0.f;
    } else {
	i__1 = kwtop + (kwtop - 1) * h_dim1;
	s.r = h__[i__1].r, s.i = h__[i__1].i;
    }

    if (*kbot == kwtop) {

/*        ==== 1-by-1 deflation window: not much to do ==== */

	i__1 = kwtop;
	i__2 = kwtop + kwtop * h_dim1;
	sh[i__1].r = h__[i__2].r, sh[i__1].i = h__[i__2].i;
	*ns = 1;
	*nd = 0;
/* Computing MAX */
	i__1 = kwtop + kwtop * h_dim1;
	r__5 = smlnum, r__6 = ulp * ((r__1 = h__[i__1].r, abs(r__1)) + (r__2 =
		 r_imag(&h__[kwtop + kwtop * h_dim1]), abs(r__2)));
	if ((r__3 = s.r, abs(r__3)) + (r__4 = r_imag(&s), abs(r__4)) <= f2cmax(
		r__5,r__6)) {
	    *ns = 0;
	    *nd = 1;
	    if (kwtop > *ktop) {
		i__1 = kwtop + (kwtop - 1) * h_dim1;
		h__[i__1].r = 0.f, h__[i__1].i = 0.f;
	    }
	}
	work[1].r = 1.f, work[1].i = 0.f;
	return;
    }

/*     ==== Convert to spike-triangular form.  (In case of a */
/*     .    rare QR failure, this routine continues to do */
/*     .    aggressive early deflation using that part of */
/*     .    the deflation window that converged using INFQR */
/*     .    here and there to keep track.) ==== */

    clacpy_("U", &jw, &jw, &h__[kwtop + kwtop * h_dim1], ldh, &t[t_offset], 
	    ldt);
    i__1 = jw - 1;
    i__2 = *ldh + 1;
    i__3 = *ldt + 1;
    ccopy_(&i__1, &h__[kwtop + 1 + kwtop * h_dim1], &i__2, &t[t_dim1 + 2], &
	    i__3);

    claset_("A", &jw, &jw, &c_b1, &c_b2, &v[v_offset], ldv);
    clahqr_(&c_true, &c_true, &jw, &c__1, &jw, &t[t_offset], ldt, &sh[kwtop], 
	    &c__1, &jw, &v[v_offset], ldv, &infqr);

/*     ==== Deflation detection loop ==== */

    *ns = jw;
    ilst = infqr + 1;
    i__1 = jw;
    for (knt = infqr + 1; knt <= i__1; ++knt) {

/*        ==== Small spike tip deflation test ==== */

	i__2 = *ns + *ns * t_dim1;
	foo = (r__1 = t[i__2].r, abs(r__1)) + (r__2 = r_imag(&t[*ns + *ns * 
		t_dim1]), abs(r__2));
	if (foo == 0.f) {
	    foo = (r__1 = s.r, abs(r__1)) + (r__2 = r_imag(&s), abs(r__2));
	}
	i__2 = *ns * v_dim1 + 1;
/* Computing MAX */
	r__5 = smlnum, r__6 = ulp * foo;
	if (((r__1 = s.r, abs(r__1)) + (r__2 = r_imag(&s), abs(r__2))) * ((
		r__3 = v[i__2].r, abs(r__3)) + (r__4 = r_imag(&v[*ns * v_dim1 
		+ 1]), abs(r__4))) <= f2cmax(r__5,r__6)) {

/*           ==== One more converged eigenvalue ==== */

	    --(*ns);
	} else {

/*           ==== One undeflatable eigenvalue.  Move it up out of the */
/*           .    way.   (CTREXC can not fail in this case.) ==== */

	    ifst = *ns;
	    ctrexc_("V", &jw, &t[t_offset], ldt, &v[v_offset], ldv, &ifst, &
		    ilst, &info);
	    ++ilst;
	}
/* L10: */
    }

/*        ==== Return to Hessenberg form ==== */

    if (*ns == 0) {
	s.r = 0.f, s.i = 0.f;
    }

    if (*ns < jw) {

/*        ==== sorting the diagonal of T improves accuracy for */
/*        .    graded matrices.  ==== */

	i__1 = *ns;
	for (i__ = infqr + 1; i__ <= i__1; ++i__) {
	    ifst = i__;
	    i__2 = *ns;
	    for (j = i__ + 1; j <= i__2; ++j) {
		i__3 = j + j * t_dim1;
		i__4 = ifst + ifst * t_dim1;
		if ((r__1 = t[i__3].r, abs(r__1)) + (r__2 = r_imag(&t[j + j * 
			t_dim1]), abs(r__2)) > (r__3 = t[i__4].r, abs(r__3)) 
			+ (r__4 = r_imag(&t[ifst + ifst * t_dim1]), abs(r__4))
			) {
		    ifst = j;
		}
/* L20: */
	    }
	    ilst = i__;
	    if (ifst != ilst) {
		ctrexc_("V", &jw, &t[t_offset], ldt, &v[v_offset], ldv, &ifst,
			 &ilst, &info);
	    }
/* L30: */
	}
    }

/*     ==== Restore shift/eigenvalue array from T ==== */

    i__1 = jw;
    for (i__ = infqr + 1; i__ <= i__1; ++i__) {
	i__2 = kwtop + i__ - 1;
	i__3 = i__ + i__ * t_dim1;
	sh[i__2].r = t[i__3].r, sh[i__2].i = t[i__3].i;
/* L40: */
    }


    if (*ns < jw || s.r == 0.f && s.i == 0.f) {
	if (*ns > 1 && (s.r != 0.f || s.i != 0.f)) {

/*           ==== Reflect spike back into lower triangle ==== */

	    ccopy_(ns, &v[v_offset], ldv, &work[1], &c__1);
	    i__1 = *ns;
	    for (i__ = 1; i__ <= i__1; ++i__) {
		i__2 = i__;
		r_cnjg(&q__1, &work[i__]);
		work[i__2].r = q__1.r, work[i__2].i = q__1.i;
/* L50: */
	    }
	    beta.r = work[1].r, beta.i = work[1].i;
	    clarfg_(ns, &beta, &work[2], &c__1, &tau);
	    work[1].r = 1.f, work[1].i = 0.f;

	    i__1 = jw - 2;
	    i__2 = jw - 2;
	    claset_("L", &i__1, &i__2, &c_b1, &c_b1, &t[t_dim1 + 3], ldt);

	    r_cnjg(&q__1, &tau);
	    clarf_("L", ns, &jw, &work[1], &c__1, &q__1, &t[t_offset], ldt, &
		    work[jw + 1]);
	    clarf_("R", ns, ns, &work[1], &c__1, &tau, &t[t_offset], ldt, &
		    work[jw + 1]);
	    clarf_("R", &jw, ns, &work[1], &c__1, &tau, &v[v_offset], ldv, &
		    work[jw + 1]);

	    i__1 = *lwork - jw;
	    cgehrd_(&jw, &c__1, ns, &t[t_offset], ldt, &work[1], &work[jw + 1]
		    , &i__1, &info);
	}

/*        ==== Copy updated reduced window into place ==== */

	if (kwtop > 1) {
	    i__1 = kwtop + (kwtop - 1) * h_dim1;
	    r_cnjg(&q__2, &v[v_dim1 + 1]);
	    q__1.r = s.r * q__2.r - s.i * q__2.i, q__1.i = s.r * q__2.i + s.i 
		    * q__2.r;
	    h__[i__1].r = q__1.r, h__[i__1].i = q__1.i;
	}
	clacpy_("U", &jw, &jw, &t[t_offset], ldt, &h__[kwtop + kwtop * h_dim1]
		, ldh);
	i__1 = jw - 1;
	i__2 = *ldt + 1;
	i__3 = *ldh + 1;
	ccopy_(&i__1, &t[t_dim1 + 2], &i__2, &h__[kwtop + 1 + kwtop * h_dim1],
		 &i__3);

/*        ==== Accumulate orthogonal matrix in order update */
/*        .    H and Z, if requested.  ==== */

	if (*ns > 1 && (s.r != 0.f || s.i != 0.f)) {
	    i__1 = *lwork - jw;
	    cunmhr_("R", "N", &jw, ns, &c__1, ns, &t[t_offset], ldt, &work[1],
		     &v[v_offset], ldv, &work[jw + 1], &i__1, &info);
	}

/*        ==== Update vertical slab in H ==== */

	if (*wantt) {
	    ltop = 1;
	} else {
	    ltop = *ktop;
	}
	i__1 = kwtop - 1;
	i__2 = *nv;
	for (krow = ltop; i__2 < 0 ? krow >= i__1 : krow <= i__1; krow += 
		i__2) {
/* Computing MIN */
	    i__3 = *nv, i__4 = kwtop - krow;
	    kln = f2cmin(i__3,i__4);
	    cgemm_("N", "N", &kln, &jw, &jw, &c_b2, &h__[krow + kwtop * 
		    h_dim1], ldh, &v[v_offset], ldv, &c_b1, &wv[wv_offset], 
		    ldwv);
	    clacpy_("A", &kln, &jw, &wv[wv_offset], ldwv, &h__[krow + kwtop * 
		    h_dim1], ldh);
/* L60: */
	}

/*        ==== Update horizontal slab in H ==== */

	if (*wantt) {
	    i__2 = *n;
	    i__1 = *nh;
	    for (kcol = *kbot + 1; i__1 < 0 ? kcol >= i__2 : kcol <= i__2; 
		    kcol += i__1) {
/* Computing MIN */
		i__3 = *nh, i__4 = *n - kcol + 1;
		kln = f2cmin(i__3,i__4);
		cgemm_("C", "N", &jw, &kln, &jw, &c_b2, &v[v_offset], ldv, &
			h__[kwtop + kcol * h_dim1], ldh, &c_b1, &t[t_offset], 
			ldt);
		clacpy_("A", &jw, &kln, &t[t_offset], ldt, &h__[kwtop + kcol *
			 h_dim1], ldh);
/* L70: */
	    }
	}

/*        ==== Update vertical slab in Z ==== */

	if (*wantz) {
	    i__1 = *ihiz;
	    i__2 = *nv;
	    for (krow = *iloz; i__2 < 0 ? krow >= i__1 : krow <= i__1; krow +=
		     i__2) {
/* Computing MIN */
		i__3 = *nv, i__4 = *ihiz - krow + 1;
		kln = f2cmin(i__3,i__4);
		cgemm_("N", "N", &kln, &jw, &jw, &c_b2, &z__[krow + kwtop * 
			z_dim1], ldz, &v[v_offset], ldv, &c_b1, &wv[wv_offset]
			, ldwv);
		clacpy_("A", &kln, &jw, &wv[wv_offset], ldwv, &z__[krow + 
			kwtop * z_dim1], ldz);
/* L80: */
	    }
	}
    }

/*     ==== Return the number of deflations ... ==== */

    *nd = jw - *ns;

/*     ==== ... and the number of shifts. (Subtracting */
/*     .    INFQR from the spike length takes care */
/*     .    of the case of a rare QR failure while */
/*     .    calculating eigenvalues of the deflation */
/*     .    window.)  ==== */

    *ns -= infqr;

/*      ==== Return optimal workspace. ==== */

    r__1 = (real) lwkopt;
    q__1.r = r__1, q__1.i = 0.f;
    work[1].r = q__1.r, work[1].i = q__1.i;

/*     ==== End of CLAQR2 ==== */

    return;
} /* claqr2_ */

