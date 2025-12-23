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

static integer c__13 = 13;
static integer c__15 = 15;
static integer c_n1 = -1;
static integer c__12 = 12;
static integer c__14 = 14;
static integer c__16 = 16;
static logical c_false = FALSE_;
static integer c__1 = 1;
static integer c__3 = 3;

/* > \brief \b ZLAQR0 computes the eigenvalues of a Hessenberg matrix, and optionally the matrices from the Sc
hur decomposition. */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZLAQR0 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlaqr0.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlaqr0.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlaqr0.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLAQR0( WANTT, WANTZ, N, ILO, IHI, H, LDH, W, ILOZ, */
/*                          IHIZ, Z, LDZ, WORK, LWORK, INFO ) */

/*       INTEGER            IHI, IHIZ, ILO, ILOZ, INFO, LDH, LDZ, LWORK, N */
/*       LOGICAL            WANTT, WANTZ */
/*       COMPLEX*16         H( LDH, * ), W( * ), WORK( * ), Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* >    ZLAQR0 computes the eigenvalues of a Hessenberg matrix H */
/* >    and, optionally, the matrices T and Z from the Schur decomposition */
/* >    H = Z T Z**H, where T is an upper triangular matrix (the */
/* >    Schur form), and Z is the unitary matrix of Schur vectors. */
/* > */
/* >    Optionally Z may be postmultiplied into an input unitary */
/* >    matrix Q so that this routine can give the Schur factorization */
/* >    of a matrix A which has been reduced to the Hessenberg form H */
/* >    by the unitary matrix Q:  A = Q*H*Q**H = (QZ)*H*(QZ)**H. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] WANTT */
/* > \verbatim */
/* >          WANTT is LOGICAL */
/* >          = .TRUE. : the full Schur form T is required; */
/* >          = .FALSE.: only eigenvalues are required. */
/* > \endverbatim */
/* > */
/* > \param[in] WANTZ */
/* > \verbatim */
/* >          WANTZ is LOGICAL */
/* >          = .TRUE. : the matrix of Schur vectors Z is required; */
/* >          = .FALSE.: Schur vectors are not required. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >           The order of the matrix H.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in] ILO */
/* > \verbatim */
/* >          ILO is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[in] IHI */
/* > \verbatim */
/* >          IHI is INTEGER */
/* > */
/* >           It is assumed that H is already upper triangular in rows */
/* >           and columns 1:ILO-1 and IHI+1:N and, if ILO > 1, */
/* >           H(ILO,ILO-1) is zero. ILO and IHI are normally set by a */
/* >           previous call to ZGEBAL, and then passed to ZGEHRD when the */
/* >           matrix output by ZGEBAL is reduced to Hessenberg form. */
/* >           Otherwise, ILO and IHI should be set to 1 and N, */
/* >           respectively.  If N > 0, then 1 <= ILO <= IHI <= N. */
/* >           If N = 0, then ILO = 1 and IHI = 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] H */
/* > \verbatim */
/* >          H is COMPLEX*16 array, dimension (LDH,N) */
/* >           On entry, the upper Hessenberg matrix H. */
/* >           On exit, if INFO = 0 and WANTT is .TRUE., then H */
/* >           contains the upper triangular matrix T from the Schur */
/* >           decomposition (the Schur form). If INFO = 0 and WANT is */
/* >           .FALSE., then the contents of H are unspecified on exit. */
/* >           (The output value of H when INFO > 0 is given under the */
/* >           description of INFO below.) */
/* > */
/* >           This subroutine may explicitly set H(i,j) = 0 for i > j and */
/* >           j = 1, 2, ... ILO-1 or j = IHI+1, IHI+2, ... N. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >           The leading dimension of the array H. LDH >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] W */
/* > \verbatim */
/* >          W is COMPLEX*16 array, dimension (N) */
/* >           The computed eigenvalues of H(ILO:IHI,ILO:IHI) are stored */
/* >           in W(ILO:IHI). If WANTT is .TRUE., then the eigenvalues are */
/* >           stored in the same order as on the diagonal of the Schur */
/* >           form returned in H, with W(i) = H(i,i). */
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
/* >           Specify the rows of Z to which transformations must be */
/* >           applied if WANTZ is .TRUE.. */
/* >           1 <= ILOZ <= ILO; IHI <= IHIZ <= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is COMPLEX*16 array, dimension (LDZ,IHI) */
/* >           If WANTZ is .FALSE., then Z is not referenced. */
/* >           If WANTZ is .TRUE., then Z(ILO:IHI,ILOZ:IHIZ) is */
/* >           replaced by Z(ILO:IHI,ILOZ:IHIZ)*U where U is the */
/* >           orthogonal Schur factor of H(ILO:IHI,ILO:IHI). */
/* >           (The output value of Z when INFO > 0 is given under */
/* >           the description of INFO below.) */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >           The leading dimension of the array Z.  if WANTZ is .TRUE. */
/* >           then LDZ >= MAX(1,IHIZ).  Otherwise, LDZ >= 1. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is COMPLEX*16 array, dimension LWORK */
/* >           On exit, if LWORK = -1, WORK(1) returns an estimate of */
/* >           the optimal value for LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >           The dimension of the array WORK.  LWORK >= f2cmax(1,N) */
/* >           is sufficient, but LWORK typically as large as 6*N may */
/* >           be required for optimal performance.  A workspace query */
/* >           to determine the optimal workspace size is recommended. */
/* > */
/* >           If LWORK = -1, then ZLAQR0 does a workspace query. */
/* >           In this case, ZLAQR0 checks the input parameters and */
/* >           estimates the optimal workspace size for the given */
/* >           values of N, ILO and IHI.  The estimate is returned */
/* >           in WORK(1).  No error message related to LWORK is */
/* >           issued by XERBLA.  Neither H nor Z are accessed. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >             = 0:  successful exit */
/* >             > 0:  if INFO = i, ZLAQR0 failed to compute all of */
/* >                the eigenvalues.  Elements 1:ilo-1 and i+1:n of WR */
/* >                and WI contain those eigenvalues which have been */
/* >                successfully computed.  (Failures are rare.) */
/* > */
/* >                If INFO > 0 and WANT is .FALSE., then on exit, */
/* >                the remaining unconverged eigenvalues are the eigen- */
/* >                values of the upper Hessenberg matrix rows and */
/* >                columns ILO through INFO of the final, output */
/* >                value of H. */
/* > */
/* >                If INFO > 0 and WANTT is .TRUE., then on exit */
/* > */
/* >           (*)  (initial value of H)*U  = U*(final value of H) */
/* > */
/* >                where U is a unitary matrix.  The final */
/* >                value of  H is upper Hessenberg and triangular in */
/* >                rows and columns INFO+1 through IHI. */
/* > */
/* >                If INFO > 0 and WANTZ is .TRUE., then on exit */
/* > */
/* >                  (final value of Z(ILO:IHI,ILOZ:IHIZ) */
/* >                   =  (initial value of Z(ILO:IHI,ILOZ:IHIZ)*U */
/* > */
/* >                where U is the unitary matrix in (*) (regard- */
/* >                less of the value of WANTT.) */
/* > */
/* >                If INFO > 0 and WANTZ is .FALSE., then Z is not */
/* >                accessed. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16OTHERauxiliary */

/* > \par Contributors: */
/*  ================== */
/* > */
/* >       Karen Braman and Ralph Byers, Department of Mathematics, */
/* >       University of Kansas, USA */

/* > \par References: */
/*  ================ */
/* > */
/* >       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR */
/* >       Algorithm Part I: Maintaining Well Focused Shifts, and Level 3 */
/* >       Performance, SIAM Journal of Matrix Analysis, volume 23, pages */
/* >       929--947, 2002. */
/* > \n */
/* >       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR */
/* >       Algorithm Part II: Aggressive Early Deflation, SIAM Journal */
/* >       of Matrix Analysis, volume 23, pages 948--973, 2002. */
/* > */
/*  ===================================================================== */
/* Subroutine */ void zlaqr0_(logical *wantt, logical *wantz, integer *n, 
	integer *ilo, integer *ihi, doublecomplex *h__, integer *ldh, 
	doublecomplex *w, integer *iloz, integer *ihiz, doublecomplex *z__, 
	integer *ldz, doublecomplex *work, integer *lwork, integer *info)
{
    /* System generated locals */
    integer h_dim1, h_offset, z_dim1, z_offset, i__1, i__2, i__3, i__4, i__5;
    doublereal d__1, d__2, d__3, d__4, d__5, d__6, d__7, d__8;
    doublecomplex z__1, z__2, z__3, z__4, z__5;

    /* Local variables */
    integer ndec, ndfl, kbot, nmin;
    doublecomplex swap;
    integer ktop;
    doublecomplex zdum[1]	/* was [1][1] */;
    integer kacc22, i__, k;
    doublereal s;
    integer itmax, nsmax, nwmax, kwtop;
    doublecomplex aa, bb, cc, dd;
    extern /* Subroutine */ void zlaqr3_(logical *, logical *, integer *, 
	    integer *, integer *, integer *, doublecomplex *, integer *, 
	    integer *, integer *, doublecomplex *, integer *, integer *, 
	    integer *, doublecomplex *, doublecomplex *, integer *, integer *,
	     doublecomplex *, integer *, integer *, doublecomplex *, integer *
	    , doublecomplex *, integer *), zlaqr4_(logical *, logical *, 
	    integer *, integer *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, integer *, doublecomplex *, integer *,
	     doublecomplex *, integer *, integer *), zlaqr5_(logical *, 
	    logical *, integer *, integer *, integer *, integer *, integer *, 
	    doublecomplex *, doublecomplex *, integer *, integer *, integer *,
	     doublecomplex *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *, integer *, doublecomplex *, integer *,
	     integer *, doublecomplex *, integer *);
    integer ld, nh, nibble, it, ks, kt, ku, kv, ls, ns, nw;
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    char jbcmpz[2];
    doublecomplex rtdisc;
    integer nwupbd;
    logical sorted;
    extern /* Subroutine */ void zlahqr_(logical *, logical *, integer *, 
	    integer *, integer *, doublecomplex *, integer *, doublecomplex *,
	     integer *, integer *, doublecomplex *, integer *, integer *), 
	    zlacpy_(char *, integer *, integer *, doublecomplex *, integer *, 
	    doublecomplex *, integer *);
    integer lwkopt;
    doublecomplex tr2, det;
    integer inf, kdu, nho, nve, kwh, nsr, nwr, kwv;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ================================================================ */


/*     ==== Matrices of order NTINY or smaller must be processed by */
/*     .    ZLAHQR because of insufficient subdiagonal scratch space. */
/*     .    (This is a hard limit.) ==== */

/*     ==== Exceptional deflation windows:  try to cure rare */
/*     .    slow convergence by varying the size of the */
/*     .    deflation window after KEXNW iterations. ==== */

/*     ==== Exceptional shifts: try to cure rare slow convergence */
/*     .    with ad-hoc exceptional shifts every KEXSH iterations. */
/*     .    ==== */

/*     ==== The constant WILK1 is used to form the exceptional */
/*     .    shifts. ==== */
    /* Parameter adjustments */
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    --w;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --work;

    /* Function Body */
    *info = 0;

/*     ==== Quick return for N = 0: nothing to do. ==== */

    if (*n == 0) {
	work[1].r = 1., work[1].i = 0.;
	return;
    }

    if (*n <= 15) {

/*        ==== Tiny matrices must use ZLAHQR. ==== */

	lwkopt = 1;
	if (*lwork != -1) {
	    zlahqr_(wantt, wantz, n, ilo, ihi, &h__[h_offset], ldh, &w[1], 
		    iloz, ihiz, &z__[z_offset], ldz, info);
	}
    } else {

/*        ==== Use small bulge multi-shift QR with aggressive early */
/*        .    deflation on larger-than-tiny matrices. ==== */

/*        ==== Hope for the best. ==== */

	*info = 0;

/*        ==== Set up job flags for ILAENV. ==== */

	if (*wantt) {
	    *(unsigned char *)jbcmpz = 'S';
	} else {
	    *(unsigned char *)jbcmpz = 'E';
	}
	if (*wantz) {
	    *(unsigned char *)&jbcmpz[1] = 'V';
	} else {
	    *(unsigned char *)&jbcmpz[1] = 'N';
	}

/*        ==== NWR = recommended deflation window size.  At this */
/*        .    point,  N .GT. NTINY = 15, so there is enough */
/*        .    subdiagonal workspace for NWR.GE.2 as required. */
/*        .    (In fact, there is enough subdiagonal space for */
/*        .    NWR.GE.4.) ==== */

	nwr = ilaenv_(&c__13, "ZLAQR0", jbcmpz, n, ilo, ihi, lwork, (ftnlen)6,
		 (ftnlen)2);
	nwr = f2cmax(2,nwr);
/* Computing MIN */
	i__1 = *ihi - *ilo + 1, i__2 = (*n - 1) / 3, i__1 = f2cmin(i__1,i__2);
	nwr = f2cmin(i__1,nwr);

/*        ==== NSR = recommended number of simultaneous shifts. */
/*        .    At this point N .GT. NTINY = 15, so there is at */
/*        .    enough subdiagonal workspace for NSR to be even */
/*        .    and greater than or equal to two as required. ==== */

	nsr = ilaenv_(&c__15, "ZLAQR0", jbcmpz, n, ilo, ihi, lwork, (ftnlen)6,
		 (ftnlen)2);
/* Computing MIN */
	i__1 = nsr, i__2 = (*n - 3) / 6, i__1 = f2cmin(i__1,i__2), i__2 = *ihi - 
		*ilo;
	nsr = f2cmin(i__1,i__2);
/* Computing MAX */
	i__1 = 2, i__2 = nsr - nsr % 2;
	nsr = f2cmax(i__1,i__2);

/*        ==== Estimate optimal workspace ==== */

/*        ==== Workspace query call to ZLAQR3 ==== */

	i__1 = nwr + 1;
	zlaqr3_(wantt, wantz, n, ilo, ihi, &i__1, &h__[h_offset], ldh, iloz, 
		ihiz, &z__[z_offset], ldz, &ls, &ld, &w[1], &h__[h_offset], 
		ldh, n, &h__[h_offset], ldh, n, &h__[h_offset], ldh, &work[1],
		 &c_n1);

/*        ==== Optimal workspace = MAX(ZLAQR5, ZLAQR3) ==== */

/* Computing MAX */
	i__1 = nsr * 3 / 2, i__2 = (integer) work[1].r;
	lwkopt = f2cmax(i__1,i__2);

/*        ==== Quick return in case of workspace query. ==== */

	if (*lwork == -1) {
	    d__1 = (doublereal) lwkopt;
	    z__1.r = d__1, z__1.i = 0.;
	    work[1].r = z__1.r, work[1].i = z__1.i;
	    return;
	}

/*        ==== ZLAHQR/ZLAQR0 crossover point ==== */

	nmin = ilaenv_(&c__12, "ZLAQR0", jbcmpz, n, ilo, ihi, lwork, (ftnlen)
		6, (ftnlen)2);
	nmin = f2cmax(15,nmin);

/*        ==== Nibble crossover point ==== */

	nibble = ilaenv_(&c__14, "ZLAQR0", jbcmpz, n, ilo, ihi, lwork, (
		ftnlen)6, (ftnlen)2);
	nibble = f2cmax(0,nibble);

/*        ==== Accumulate reflections during ttswp?  Use block */
/*        .    2-by-2 structure during matrix-matrix multiply? ==== */

	kacc22 = ilaenv_(&c__16, "ZLAQR0", jbcmpz, n, ilo, ihi, lwork, (
		ftnlen)6, (ftnlen)2);
	kacc22 = f2cmax(0,kacc22);
	kacc22 = f2cmin(2,kacc22);

/*        ==== NWMAX = the largest possible deflation window for */
/*        .    which there is sufficient workspace. ==== */

/* Computing MIN */
	i__1 = (*n - 1) / 3, i__2 = *lwork / 2;
	nwmax = f2cmin(i__1,i__2);
	nw = nwmax;

/*        ==== NSMAX = the Largest number of simultaneous shifts */
/*        .    for which there is sufficient workspace. ==== */

/* Computing MIN */
	i__1 = (*n - 3) / 6, i__2 = (*lwork << 1) / 3;
	nsmax = f2cmin(i__1,i__2);
	nsmax -= nsmax % 2;

/*        ==== NDFL: an iteration count restarted at deflation. ==== */

	ndfl = 1;

/*        ==== ITMAX = iteration limit ==== */

/* Computing MAX */
	i__1 = 10, i__2 = *ihi - *ilo + 1;
	itmax = 30 * f2cmax(i__1,i__2);

/*        ==== Last row and column in the active block ==== */

	kbot = *ihi;

/*        ==== Main Loop ==== */

	i__1 = itmax;
	for (it = 1; it <= i__1; ++it) {

/*           ==== Done when KBOT falls below ILO ==== */

	    if (kbot < *ilo) {
		goto L80;
	    }

/*           ==== Locate active block ==== */

	    i__2 = *ilo + 1;
	    for (k = kbot; k >= i__2; --k) {
		i__3 = k + (k - 1) * h_dim1;
		if (h__[i__3].r == 0. && h__[i__3].i == 0.) {
		    goto L20;
		}
/* L10: */
	    }
	    k = *ilo;
L20:
	    ktop = k;

/*           ==== Select deflation window size: */
/*           .    Typical Case: */
/*           .      If possible and advisable, nibble the entire */
/*           .      active block.  If not, use size MIN(NWR,NWMAX) */
/*           .      or MIN(NWR+1,NWMAX) depending upon which has */
/*           .      the smaller corresponding subdiagonal entry */
/*           .      (a heuristic). */
/*           . */
/*           .    Exceptional Case: */
/*           .      If there have been no deflations in KEXNW or */
/*           .      more iterations, then vary the deflation window */
/*           .      size.   At first, because, larger windows are, */
/*           .      in general, more powerful than smaller ones, */
/*           .      rapidly increase the window to the maximum possible. */
/*           .      Then, gradually reduce the window size. ==== */

	    nh = kbot - ktop + 1;
	    nwupbd = f2cmin(nh,nwmax);
	    if (ndfl < 5) {
		nw = f2cmin(nwupbd,nwr);
	    } else {
/* Computing MIN */
		i__2 = nwupbd, i__3 = nw << 1;
		nw = f2cmin(i__2,i__3);
	    }
	    if (nw < nwmax) {
		if (nw >= nh - 1) {
		    nw = nh;
		} else {
		    kwtop = kbot - nw + 1;
		    i__2 = kwtop + (kwtop - 1) * h_dim1;
		    i__3 = kwtop - 1 + (kwtop - 2) * h_dim1;
		    if ((d__1 = h__[i__2].r, abs(d__1)) + (d__2 = d_imag(&h__[
			    kwtop + (kwtop - 1) * h_dim1]), abs(d__2)) > (
			    d__3 = h__[i__3].r, abs(d__3)) + (d__4 = d_imag(&
			    h__[kwtop - 1 + (kwtop - 2) * h_dim1]), abs(d__4))
			    ) {
			++nw;
		    }
		}
	    }
	    if (ndfl < 5) {
		ndec = -1;
	    } else if (ndec >= 0 || nw >= nwupbd) {
		++ndec;
		if (nw - ndec < 2) {
		    ndec = 0;
		}
		nw -= ndec;
	    }

/*           ==== Aggressive early deflation: */
/*           .    split workspace under the subdiagonal into */
/*           .      - an nw-by-nw work array V in the lower */
/*           .        left-hand-corner, */
/*           .      - an NW-by-at-least-NW-but-more-is-better */
/*           .        (NW-by-NHO) horizontal work array along */
/*           .        the bottom edge, */
/*           .      - an at-least-NW-but-more-is-better (NHV-by-NW) */
/*           .        vertical work array along the left-hand-edge. */
/*           .        ==== */

	    kv = *n - nw + 1;
	    kt = nw + 1;
	    nho = *n - nw - 1 - kt + 1;
	    kwv = nw + 2;
	    nve = *n - nw - kwv + 1;

/*           ==== Aggressive early deflation ==== */

	    zlaqr3_(wantt, wantz, n, &ktop, &kbot, &nw, &h__[h_offset], ldh, 
		    iloz, ihiz, &z__[z_offset], ldz, &ls, &ld, &w[1], &h__[kv 
		    + h_dim1], ldh, &nho, &h__[kv + kt * h_dim1], ldh, &nve, &
		    h__[kwv + h_dim1], ldh, &work[1], lwork);

/*           ==== Adjust KBOT accounting for new deflations. ==== */

	    kbot -= ld;

/*           ==== KS points to the shifts. ==== */

	    ks = kbot - ls + 1;

/*           ==== Skip an expensive QR sweep if there is a (partly */
/*           .    heuristic) reason to expect that many eigenvalues */
/*           .    will deflate without it.  Here, the QR sweep is */
/*           .    skipped if many eigenvalues have just been deflated */
/*           .    or if the remaining active block is small. */

	    if (ld == 0 || ld * 100 <= nw * nibble && kbot - ktop + 1 > f2cmin(
		    nmin,nwmax)) {

/*              ==== NS = nominal number of simultaneous shifts. */
/*              .    This may be lowered (slightly) if ZLAQR3 */
/*              .    did not provide that many shifts. ==== */

/* Computing MIN */
/* Computing MAX */
		i__4 = 2, i__5 = kbot - ktop;
		i__2 = f2cmin(nsmax,nsr), i__3 = f2cmax(i__4,i__5);
		ns = f2cmin(i__2,i__3);
		ns -= ns % 2;

/*              ==== If there have been no deflations */
/*              .    in a multiple of KEXSH iterations, */
/*              .    then try exceptional shifts. */
/*              .    Otherwise use shifts provided by */
/*              .    ZLAQR3 above or from the eigenvalues */
/*              .    of a trailing principal submatrix. ==== */

		if (ndfl % 6 == 0) {
		    ks = kbot - ns + 1;
		    i__2 = ks + 1;
		    for (i__ = kbot; i__ >= i__2; i__ += -2) {
			i__3 = i__;
			i__4 = i__ + i__ * h_dim1;
			i__5 = i__ + (i__ - 1) * h_dim1;
			d__3 = ((d__1 = h__[i__5].r, abs(d__1)) + (d__2 = 
				d_imag(&h__[i__ + (i__ - 1) * h_dim1]), abs(
				d__2))) * .75;
			z__1.r = h__[i__4].r + d__3, z__1.i = h__[i__4].i;
			w[i__3].r = z__1.r, w[i__3].i = z__1.i;
			i__3 = i__ - 1;
			i__4 = i__;
			w[i__3].r = w[i__4].r, w[i__3].i = w[i__4].i;
/* L30: */
		    }
		} else {

/*                 ==== Got NS/2 or fewer shifts? Use ZLAQR4 or */
/*                 .    ZLAHQR on a trailing principal submatrix to */
/*                 .    get more. (Since NS.LE.NSMAX.LE.(N-3)/6, */
/*                 .    there is enough space below the subdiagonal */
/*                 .    to fit an NS-by-NS scratch array.) ==== */

		    if (kbot - ks + 1 <= ns / 2) {
			ks = kbot - ns + 1;
			kt = *n - ns + 1;
			zlacpy_("A", &ns, &ns, &h__[ks + ks * h_dim1], ldh, &
				h__[kt + h_dim1], ldh);
			if (ns > nmin) {
			    zlaqr4_(&c_false, &c_false, &ns, &c__1, &ns, &h__[
				    kt + h_dim1], ldh, &w[ks], &c__1, &c__1, 
				    zdum, &c__1, &work[1], lwork, &inf);
			} else {
			    zlahqr_(&c_false, &c_false, &ns, &c__1, &ns, &h__[
				    kt + h_dim1], ldh, &w[ks], &c__1, &c__1, 
				    zdum, &c__1, &inf);
			}
			ks += inf;

/*                    ==== In case of a rare QR failure use */
/*                    .    eigenvalues of the trailing 2-by-2 */
/*                    .    principal submatrix.  Scale to avoid */
/*                    .    overflows, underflows and subnormals. */
/*                    .    (The scale factor S can not be zero, */
/*                    .    because H(KBOT,KBOT-1) is nonzero.) ==== */

			if (ks >= kbot) {
			    i__2 = kbot - 1 + (kbot - 1) * h_dim1;
			    i__3 = kbot + (kbot - 1) * h_dim1;
			    i__4 = kbot - 1 + kbot * h_dim1;
			    i__5 = kbot + kbot * h_dim1;
			    s = (d__1 = h__[i__2].r, abs(d__1)) + (d__2 = 
				    d_imag(&h__[kbot - 1 + (kbot - 1) * 
				    h_dim1]), abs(d__2)) + ((d__3 = h__[i__3]
				    .r, abs(d__3)) + (d__4 = d_imag(&h__[kbot 
				    + (kbot - 1) * h_dim1]), abs(d__4))) + ((
				    d__5 = h__[i__4].r, abs(d__5)) + (d__6 = 
				    d_imag(&h__[kbot - 1 + kbot * h_dim1]), 
				    abs(d__6))) + ((d__7 = h__[i__5].r, abs(
				    d__7)) + (d__8 = d_imag(&h__[kbot + kbot *
				     h_dim1]), abs(d__8)));
			    i__2 = kbot - 1 + (kbot - 1) * h_dim1;
			    z__1.r = h__[i__2].r / s, z__1.i = h__[i__2].i / 
				    s;
			    aa.r = z__1.r, aa.i = z__1.i;
			    i__2 = kbot + (kbot - 1) * h_dim1;
			    z__1.r = h__[i__2].r / s, z__1.i = h__[i__2].i / 
				    s;
			    cc.r = z__1.r, cc.i = z__1.i;
			    i__2 = kbot - 1 + kbot * h_dim1;
			    z__1.r = h__[i__2].r / s, z__1.i = h__[i__2].i / 
				    s;
			    bb.r = z__1.r, bb.i = z__1.i;
			    i__2 = kbot + kbot * h_dim1;
			    z__1.r = h__[i__2].r / s, z__1.i = h__[i__2].i / 
				    s;
			    dd.r = z__1.r, dd.i = z__1.i;
			    z__2.r = aa.r + dd.r, z__2.i = aa.i + dd.i;
			    z__1.r = z__2.r / 2., z__1.i = z__2.i / 2.;
			    tr2.r = z__1.r, tr2.i = z__1.i;
			    z__3.r = aa.r - tr2.r, z__3.i = aa.i - tr2.i;
			    z__4.r = dd.r - tr2.r, z__4.i = dd.i - tr2.i;
			    z__2.r = z__3.r * z__4.r - z__3.i * z__4.i, 
				    z__2.i = z__3.r * z__4.i + z__3.i * 
				    z__4.r;
			    z__5.r = bb.r * cc.r - bb.i * cc.i, z__5.i = bb.r 
				    * cc.i + bb.i * cc.r;
			    z__1.r = z__2.r - z__5.r, z__1.i = z__2.i - 
				    z__5.i;
			    det.r = z__1.r, det.i = z__1.i;
			    z__2.r = -det.r, z__2.i = -det.i;
			    z_sqrt(&z__1, &z__2);
			    rtdisc.r = z__1.r, rtdisc.i = z__1.i;
			    i__2 = kbot - 1;
			    z__2.r = tr2.r + rtdisc.r, z__2.i = tr2.i + 
				    rtdisc.i;
			    z__1.r = s * z__2.r, z__1.i = s * z__2.i;
			    w[i__2].r = z__1.r, w[i__2].i = z__1.i;
			    i__2 = kbot;
			    z__2.r = tr2.r - rtdisc.r, z__2.i = tr2.i - 
				    rtdisc.i;
			    z__1.r = s * z__2.r, z__1.i = s * z__2.i;
			    w[i__2].r = z__1.r, w[i__2].i = z__1.i;

			    ks = kbot - 1;
			}
		    }

		    if (kbot - ks + 1 > ns) {

/*                    ==== Sort the shifts (Helps a little) ==== */

			sorted = FALSE_;
			i__2 = ks + 1;
			for (k = kbot; k >= i__2; --k) {
			    if (sorted) {
				goto L60;
			    }
			    sorted = TRUE_;
			    i__3 = k - 1;
			    for (i__ = ks; i__ <= i__3; ++i__) {
				i__4 = i__;
				i__5 = i__ + 1;
				if ((d__1 = w[i__4].r, abs(d__1)) + (d__2 = 
					d_imag(&w[i__]), abs(d__2)) < (d__3 = 
					w[i__5].r, abs(d__3)) + (d__4 = 
					d_imag(&w[i__ + 1]), abs(d__4))) {
				    sorted = FALSE_;
				    i__4 = i__;
				    swap.r = w[i__4].r, swap.i = w[i__4].i;
				    i__4 = i__;
				    i__5 = i__ + 1;
				    w[i__4].r = w[i__5].r, w[i__4].i = w[i__5]
					    .i;
				    i__4 = i__ + 1;
				    w[i__4].r = swap.r, w[i__4].i = swap.i;
				}
/* L40: */
			    }
/* L50: */
			}
L60:
			;
		    }
		}

/*              ==== If there are only two shifts, then use */
/*              .    only one.  ==== */

		if (kbot - ks + 1 == 2) {
		    i__2 = kbot;
		    i__3 = kbot + kbot * h_dim1;
		    z__2.r = w[i__2].r - h__[i__3].r, z__2.i = w[i__2].i - 
			    h__[i__3].i;
		    z__1.r = z__2.r, z__1.i = z__2.i;
		    i__4 = kbot - 1;
		    i__5 = kbot + kbot * h_dim1;
		    z__4.r = w[i__4].r - h__[i__5].r, z__4.i = w[i__4].i - 
			    h__[i__5].i;
		    z__3.r = z__4.r, z__3.i = z__4.i;
		    if ((d__1 = z__1.r, abs(d__1)) + (d__2 = d_imag(&z__1), 
			    abs(d__2)) < (d__3 = z__3.r, abs(d__3)) + (d__4 = 
			    d_imag(&z__3), abs(d__4))) {
			i__2 = kbot - 1;
			i__3 = kbot;
			w[i__2].r = w[i__3].r, w[i__2].i = w[i__3].i;
		    } else {
			i__2 = kbot;
			i__3 = kbot - 1;
			w[i__2].r = w[i__3].r, w[i__2].i = w[i__3].i;
		    }
		}

/*              ==== Use up to NS of the the smallest magnitude */
/*              .    shifts.  If there aren't NS shifts available, */
/*              .    then use them all, possibly dropping one to */
/*              .    make the number of shifts even. ==== */

/* Computing MIN */
		i__2 = ns, i__3 = kbot - ks + 1;
		ns = f2cmin(i__2,i__3);
		ns -= ns % 2;
		ks = kbot - ns + 1;

/*              ==== Small-bulge multi-shift QR sweep: */
/*              .    split workspace under the subdiagonal into */
/*              .    - a KDU-by-KDU work array U in the lower */
/*              .      left-hand-corner, */
/*              .    - a KDU-by-at-least-KDU-but-more-is-better */
/*              .      (KDU-by-NHo) horizontal work array WH along */
/*              .      the bottom edge, */
/*              .    - and an at-least-KDU-but-more-is-better-by-KDU */
/*              .      (NVE-by-KDU) vertical work WV arrow along */
/*              .      the left-hand-edge. ==== */

		kdu = ns << 1;
		ku = *n - kdu + 1;
		kwh = kdu + 1;
		nho = *n - kdu - 3 - (kdu + 1) + 1;
		kwv = kdu + 4;
		nve = *n - kdu - kwv + 1;

/*              ==== Small-bulge multi-shift QR sweep ==== */

		zlaqr5_(wantt, wantz, &kacc22, n, &ktop, &kbot, &ns, &w[ks], &
			h__[h_offset], ldh, iloz, ihiz, &z__[z_offset], ldz, &
			work[1], &c__3, &h__[ku + h_dim1], ldh, &nve, &h__[
			kwv + h_dim1], ldh, &nho, &h__[ku + kwh * h_dim1], 
			ldh);
	    }

/*           ==== Note progress (or the lack of it). ==== */

	    if (ld > 0) {
		ndfl = 1;
	    } else {
		++ndfl;
	    }

/*           ==== End of main loop ==== */
/* L70: */
	}

/*        ==== Iteration limit exceeded.  Set INFO to show where */
/*        .    the problem occurred and exit. ==== */

	*info = kbot;
L80:
	;
    }

/*     ==== Return the optimal value of LWORK. ==== */

    d__1 = (doublereal) lwkopt;
    z__1.r = d__1, z__1.i = 0.;
    work[1].r = z__1.r, work[1].i = z__1.i;

/*     ==== End of ZLAQR0 ==== */

    return;
} /* zlaqr0_ */

