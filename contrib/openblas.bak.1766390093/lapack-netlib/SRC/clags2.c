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




/* > \brief \b CLAGS2 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download CLAGS2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clags2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clags2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clags2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE CLAGS2( UPPER, A1, A2, A3, B1, B2, B3, CSU, SNU, CSV, */
/*                          SNV, CSQ, SNQ ) */

/*       LOGICAL            UPPER */
/*       REAL               A1, A3, B1, B3, CSQ, CSU, CSV */
/*       COMPLEX            A2, B2, SNQ, SNU, SNV */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > CLAGS2 computes 2-by-2 unitary matrices U, V and Q, such */
/* > that if ( UPPER ) then */
/* > */
/* >           U**H *A*Q = U**H *( A1 A2 )*Q = ( x  0  ) */
/* >                             ( 0  A3 )     ( x  x  ) */
/* > and */
/* >           V**H*B*Q = V**H *( B1 B2 )*Q = ( x  0  ) */
/* >                            ( 0  B3 )     ( x  x  ) */
/* > */
/* > or if ( .NOT.UPPER ) then */
/* > */
/* >           U**H *A*Q = U**H *( A1 0  )*Q = ( x  x  ) */
/* >                             ( A2 A3 )     ( 0  x  ) */
/* > and */
/* >           V**H *B*Q = V**H *( B1 0  )*Q = ( x  x  ) */
/* >                             ( B2 B3 )     ( 0  x  ) */
/* > where */
/* > */
/* >   U = (   CSU    SNU ), V = (  CSV    SNV ), */
/* >       ( -SNU**H  CSU )      ( -SNV**H CSV ) */
/* > */
/* >   Q = (   CSQ    SNQ ) */
/* >       ( -SNQ**H  CSQ ) */
/* > */
/* > The rows of the transformed A and B are parallel. Moreover, if the */
/* > input 2-by-2 matrix A is not zero, then the transformed (1,1) entry */
/* > of A is not zero. If the input matrices A and B are both not zero, */
/* > then the transformed (2,2) element of B is not zero, except when the */
/* > first rows of input A and B are parallel and the second rows are */
/* > zero. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] UPPER */
/* > \verbatim */
/* >          UPPER is LOGICAL */
/* >          = .TRUE.: the input matrices A and B are upper triangular. */
/* >          = .FALSE.: the input matrices A and B are lower triangular. */
/* > \endverbatim */
/* > */
/* > \param[in] A1 */
/* > \verbatim */
/* >          A1 is REAL */
/* > \endverbatim */
/* > */
/* > \param[in] A2 */
/* > \verbatim */
/* >          A2 is COMPLEX */
/* > \endverbatim */
/* > */
/* > \param[in] A3 */
/* > \verbatim */
/* >          A3 is REAL */
/* >          On entry, A1, A2 and A3 are elements of the input 2-by-2 */
/* >          upper (lower) triangular matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] B1 */
/* > \verbatim */
/* >          B1 is REAL */
/* > \endverbatim */
/* > */
/* > \param[in] B2 */
/* > \verbatim */
/* >          B2 is COMPLEX */
/* > \endverbatim */
/* > */
/* > \param[in] B3 */
/* > \verbatim */
/* >          B3 is REAL */
/* >          On entry, B1, B2 and B3 are elements of the input 2-by-2 */
/* >          upper (lower) triangular matrix B. */
/* > \endverbatim */
/* > */
/* > \param[out] CSU */
/* > \verbatim */
/* >          CSU is REAL */
/* > \endverbatim */
/* > */
/* > \param[out] SNU */
/* > \verbatim */
/* >          SNU is COMPLEX */
/* >          The desired unitary matrix U. */
/* > \endverbatim */
/* > */
/* > \param[out] CSV */
/* > \verbatim */
/* >          CSV is REAL */
/* > \endverbatim */
/* > */
/* > \param[out] SNV */
/* > \verbatim */
/* >          SNV is COMPLEX */
/* >          The desired unitary matrix V. */
/* > \endverbatim */
/* > */
/* > \param[out] CSQ */
/* > \verbatim */
/* >          CSQ is REAL */
/* > \endverbatim */
/* > */
/* > \param[out] SNQ */
/* > \verbatim */
/* >          SNQ is COMPLEX */
/* >          The desired unitary matrix Q. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complexOTHERauxiliary */

/*  ===================================================================== */
/* Subroutine */ void clags2_(logical *upper, real *a1, complex *a2, real *a3, 
	real *b1, complex *b2, real *b3, real *csu, complex *snu, real *csv, 
	complex *snv, real *csq, complex *snq)
{
    /* System generated locals */
    real r__1, r__2, r__3, r__4, r__5, r__6, r__7, r__8;
    complex q__1, q__2, q__3, q__4, q__5;

    /* Local variables */
    real aua11, aua12, aua21, aua22, avb11, avb12, avb21, avb22, ua11r, ua22r,
	     vb11r, vb22r, a;
    complex b, c__;
    real d__;
    complex r__, d1;
    real s1, s2, fb, fc;
    extern /* Subroutine */ void slasv2_(real *, real *, real *, real *, real *
	    , real *, real *, real *, real *), clartg_(complex *, complex *, 
	    real *, complex *, complex *);
    complex ua11, ua12, ua21, ua22, vb11, vb12, vb21, vb22;
    real csl, csr, snl, snr;


/*  -- LAPACK auxiliary routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     December 2016 */


/*  ===================================================================== */


    if (*upper) {

/*        Input matrices A and B are upper triangular matrices */

/*        Form matrix C = A*adj(B) = ( a b ) */
/*                                   ( 0 d ) */

	a = *a1 * *b3;
	d__ = *a3 * *b1;
	q__2.r = *b1 * a2->r, q__2.i = *b1 * a2->i;
	q__3.r = *a1 * b2->r, q__3.i = *a1 * b2->i;
	q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
	b.r = q__1.r, b.i = q__1.i;
	fb = c_abs(&b);

/*        Transform complex 2-by-2 matrix C to real matrix by unitary */
/*        diagonal matrix diag(1,D1). */

	d1.r = 1.f, d1.i = 0.f;
	if (fb != 0.f) {
	    q__1.r = b.r / fb, q__1.i = b.i / fb;
	    d1.r = q__1.r, d1.i = q__1.i;
	}

/*        The SVD of real 2 by 2 triangular C */

/*         ( CSL -SNL )*( A B )*(  CSR  SNR ) = ( R 0 ) */
/*         ( SNL  CSL ) ( 0 D ) ( -SNR  CSR )   ( 0 T ) */

	slasv2_(&a, &fb, &d__, &s1, &s2, &snr, &csr, &snl, &csl);

	if (abs(csl) >= abs(snl) || abs(csr) >= abs(snr)) {

/*           Compute the (1,1) and (1,2) elements of U**H *A and V**H *B, */
/*           and (1,2) element of |U|**H *|A| and |V|**H *|B|. */

	    ua11r = csl * *a1;
	    q__2.r = csl * a2->r, q__2.i = csl * a2->i;
	    q__4.r = snl * d1.r, q__4.i = snl * d1.i;
	    q__3.r = *a3 * q__4.r, q__3.i = *a3 * q__4.i;
	    q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
	    ua12.r = q__1.r, ua12.i = q__1.i;

	    vb11r = csr * *b1;
	    q__2.r = csr * b2->r, q__2.i = csr * b2->i;
	    q__4.r = snr * d1.r, q__4.i = snr * d1.i;
	    q__3.r = *b3 * q__4.r, q__3.i = *b3 * q__4.i;
	    q__1.r = q__2.r + q__3.r, q__1.i = q__2.i + q__3.i;
	    vb12.r = q__1.r, vb12.i = q__1.i;

	    aua12 = abs(csl) * ((r__1 = a2->r, abs(r__1)) + (r__2 = r_imag(a2)
		    , abs(r__2))) + abs(snl) * abs(*a3);
	    avb12 = abs(csr) * ((r__1 = b2->r, abs(r__1)) + (r__2 = r_imag(b2)
		    , abs(r__2))) + abs(snr) * abs(*b3);

/*           zero (1,2) elements of U**H *A and V**H *B */

	    if (abs(ua11r) + ((r__1 = ua12.r, abs(r__1)) + (r__2 = r_imag(&
		    ua12), abs(r__2))) == 0.f) {
		q__2.r = vb11r, q__2.i = 0.f;
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &vb12);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    } else if (abs(vb11r) + ((r__1 = vb12.r, abs(r__1)) + (r__2 = 
		    r_imag(&vb12), abs(r__2))) == 0.f) {
		q__2.r = ua11r, q__2.i = 0.f;
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &ua12);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    } else if (aua12 / (abs(ua11r) + ((r__1 = ua12.r, abs(r__1)) + (
		    r__2 = r_imag(&ua12), abs(r__2)))) <= avb12 / (abs(vb11r) 
		    + ((r__3 = vb12.r, abs(r__3)) + (r__4 = r_imag(&vb12), 
		    abs(r__4))))) {
		q__2.r = ua11r, q__2.i = 0.f;
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &ua12);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    } else {
		q__2.r = vb11r, q__2.i = 0.f;
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &vb12);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    }

	    *csu = csl;
	    q__2.r = -d1.r, q__2.i = -d1.i;
	    q__1.r = snl * q__2.r, q__1.i = snl * q__2.i;
	    snu->r = q__1.r, snu->i = q__1.i;
	    *csv = csr;
	    q__2.r = -d1.r, q__2.i = -d1.i;
	    q__1.r = snr * q__2.r, q__1.i = snr * q__2.i;
	    snv->r = q__1.r, snv->i = q__1.i;

	} else {

/*           Compute the (2,1) and (2,2) elements of U**H *A and V**H *B, */
/*           and (2,2) element of |U|**H *|A| and |V|**H *|B|. */

	    r_cnjg(&q__4, &d1);
	    q__3.r = -q__4.r, q__3.i = -q__4.i;
	    q__2.r = snl * q__3.r, q__2.i = snl * q__3.i;
	    q__1.r = *a1 * q__2.r, q__1.i = *a1 * q__2.i;
	    ua21.r = q__1.r, ua21.i = q__1.i;
	    r_cnjg(&q__5, &d1);
	    q__4.r = -q__5.r, q__4.i = -q__5.i;
	    q__3.r = snl * q__4.r, q__3.i = snl * q__4.i;
	    q__2.r = q__3.r * a2->r - q__3.i * a2->i, q__2.i = q__3.r * a2->i 
		    + q__3.i * a2->r;
	    r__1 = csl * *a3;
	    q__1.r = q__2.r + r__1, q__1.i = q__2.i;
	    ua22.r = q__1.r, ua22.i = q__1.i;

	    r_cnjg(&q__4, &d1);
	    q__3.r = -q__4.r, q__3.i = -q__4.i;
	    q__2.r = snr * q__3.r, q__2.i = snr * q__3.i;
	    q__1.r = *b1 * q__2.r, q__1.i = *b1 * q__2.i;
	    vb21.r = q__1.r, vb21.i = q__1.i;
	    r_cnjg(&q__5, &d1);
	    q__4.r = -q__5.r, q__4.i = -q__5.i;
	    q__3.r = snr * q__4.r, q__3.i = snr * q__4.i;
	    q__2.r = q__3.r * b2->r - q__3.i * b2->i, q__2.i = q__3.r * b2->i 
		    + q__3.i * b2->r;
	    r__1 = csr * *b3;
	    q__1.r = q__2.r + r__1, q__1.i = q__2.i;
	    vb22.r = q__1.r, vb22.i = q__1.i;

	    aua22 = abs(snl) * ((r__1 = a2->r, abs(r__1)) + (r__2 = r_imag(a2)
		    , abs(r__2))) + abs(csl) * abs(*a3);
	    avb22 = abs(snr) * ((r__1 = b2->r, abs(r__1)) + (r__2 = r_imag(b2)
		    , abs(r__2))) + abs(csr) * abs(*b3);

/*           zero (2,2) elements of U**H *A and V**H *B, and then swap. */

	    if ((r__1 = ua21.r, abs(r__1)) + (r__2 = r_imag(&ua21), abs(r__2))
		     + ((r__3 = ua22.r, abs(r__3)) + (r__4 = r_imag(&ua22), 
		    abs(r__4))) == 0.f) {
		r_cnjg(&q__2, &vb21);
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &vb22);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    } else if ((r__1 = vb21.r, abs(r__1)) + (r__2 = r_imag(&vb21), 
		    abs(r__2)) + c_abs(&vb22) == 0.f) {
		r_cnjg(&q__2, &ua21);
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &ua22);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    } else if (aua22 / ((r__1 = ua21.r, abs(r__1)) + (r__2 = r_imag(&
		    ua21), abs(r__2)) + ((r__3 = ua22.r, abs(r__3)) + (r__4 = 
		    r_imag(&ua22), abs(r__4)))) <= avb22 / ((r__5 = vb21.r, 
		    abs(r__5)) + (r__6 = r_imag(&vb21), abs(r__6)) + ((r__7 = 
		    vb22.r, abs(r__7)) + (r__8 = r_imag(&vb22), abs(r__8))))) 
		    {
		r_cnjg(&q__2, &ua21);
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &ua22);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    } else {
		r_cnjg(&q__2, &vb21);
		q__1.r = -q__2.r, q__1.i = -q__2.i;
		r_cnjg(&q__3, &vb22);
		clartg_(&q__1, &q__3, csq, snq, &r__);
	    }

	    *csu = snl;
	    q__1.r = csl * d1.r, q__1.i = csl * d1.i;
	    snu->r = q__1.r, snu->i = q__1.i;
	    *csv = snr;
	    q__1.r = csr * d1.r, q__1.i = csr * d1.i;
	    snv->r = q__1.r, snv->i = q__1.i;

	}

    } else {

/*        Input matrices A and B are lower triangular matrices */

/*        Form matrix C = A*adj(B) = ( a 0 ) */
/*                                   ( c d ) */

	a = *a1 * *b3;
	d__ = *a3 * *b1;
	q__2.r = *b3 * a2->r, q__2.i = *b3 * a2->i;
	q__3.r = *a3 * b2->r, q__3.i = *a3 * b2->i;
	q__1.r = q__2.r - q__3.r, q__1.i = q__2.i - q__3.i;
	c__.r = q__1.r, c__.i = q__1.i;
	fc = c_abs(&c__);

/*        Transform complex 2-by-2 matrix C to real matrix by unitary */
/*        diagonal matrix diag(d1,1). */

	d1.r = 1.f, d1.i = 0.f;
	if (fc != 0.f) {
	    q__1.r = c__.r / fc, q__1.i = c__.i / fc;
	    d1.r = q__1.r, d1.i = q__1.i;
	}

/*        The SVD of real 2 by 2 triangular C */

/*         ( CSL -SNL )*( A 0 )*(  CSR  SNR ) = ( R 0 ) */
/*         ( SNL  CSL ) ( C D ) ( -SNR  CSR )   ( 0 T ) */

	slasv2_(&a, &fc, &d__, &s1, &s2, &snr, &csr, &snl, &csl);

	if (abs(csr) >= abs(snr) || abs(csl) >= abs(snl)) {

/*           Compute the (2,1) and (2,2) elements of U**H *A and V**H *B, */
/*           and (2,1) element of |U|**H *|A| and |V|**H *|B|. */

	    q__4.r = -d1.r, q__4.i = -d1.i;
	    q__3.r = snr * q__4.r, q__3.i = snr * q__4.i;
	    q__2.r = *a1 * q__3.r, q__2.i = *a1 * q__3.i;
	    q__5.r = csr * a2->r, q__5.i = csr * a2->i;
	    q__1.r = q__2.r + q__5.r, q__1.i = q__2.i + q__5.i;
	    ua21.r = q__1.r, ua21.i = q__1.i;
	    ua22r = csr * *a3;

	    q__4.r = -d1.r, q__4.i = -d1.i;
	    q__3.r = snl * q__4.r, q__3.i = snl * q__4.i;
	    q__2.r = *b1 * q__3.r, q__2.i = *b1 * q__3.i;
	    q__5.r = csl * b2->r, q__5.i = csl * b2->i;
	    q__1.r = q__2.r + q__5.r, q__1.i = q__2.i + q__5.i;
	    vb21.r = q__1.r, vb21.i = q__1.i;
	    vb22r = csl * *b3;

	    aua21 = abs(snr) * abs(*a1) + abs(csr) * ((r__1 = a2->r, abs(r__1)
		    ) + (r__2 = r_imag(a2), abs(r__2)));
	    avb21 = abs(snl) * abs(*b1) + abs(csl) * ((r__1 = b2->r, abs(r__1)
		    ) + (r__2 = r_imag(b2), abs(r__2)));

/*           zero (2,1) elements of U**H *A and V**H *B. */

	    if ((r__1 = ua21.r, abs(r__1)) + (r__2 = r_imag(&ua21), abs(r__2))
		     + abs(ua22r) == 0.f) {
		q__1.r = vb22r, q__1.i = 0.f;
		clartg_(&q__1, &vb21, csq, snq, &r__);
	    } else if ((r__1 = vb21.r, abs(r__1)) + (r__2 = r_imag(&vb21), 
		    abs(r__2)) + abs(vb22r) == 0.f) {
		q__1.r = ua22r, q__1.i = 0.f;
		clartg_(&q__1, &ua21, csq, snq, &r__);
	    } else if (aua21 / ((r__1 = ua21.r, abs(r__1)) + (r__2 = r_imag(&
		    ua21), abs(r__2)) + abs(ua22r)) <= avb21 / ((r__3 = 
		    vb21.r, abs(r__3)) + (r__4 = r_imag(&vb21), abs(r__4)) + 
		    abs(vb22r))) {
		q__1.r = ua22r, q__1.i = 0.f;
		clartg_(&q__1, &ua21, csq, snq, &r__);
	    } else {
		q__1.r = vb22r, q__1.i = 0.f;
		clartg_(&q__1, &vb21, csq, snq, &r__);
	    }

	    *csu = csr;
	    r_cnjg(&q__3, &d1);
	    q__2.r = -q__3.r, q__2.i = -q__3.i;
	    q__1.r = snr * q__2.r, q__1.i = snr * q__2.i;
	    snu->r = q__1.r, snu->i = q__1.i;
	    *csv = csl;
	    r_cnjg(&q__3, &d1);
	    q__2.r = -q__3.r, q__2.i = -q__3.i;
	    q__1.r = snl * q__2.r, q__1.i = snl * q__2.i;
	    snv->r = q__1.r, snv->i = q__1.i;

	} else {

/*           Compute the (1,1) and (1,2) elements of U**H *A and V**H *B, */
/*           and (1,1) element of |U|**H *|A| and |V|**H *|B|. */

	    r__1 = csr * *a1;
	    r_cnjg(&q__4, &d1);
	    q__3.r = snr * q__4.r, q__3.i = snr * q__4.i;
	    q__2.r = q__3.r * a2->r - q__3.i * a2->i, q__2.i = q__3.r * a2->i 
		    + q__3.i * a2->r;
	    q__1.r = r__1 + q__2.r, q__1.i = q__2.i;
	    ua11.r = q__1.r, ua11.i = q__1.i;
	    r_cnjg(&q__3, &d1);
	    q__2.r = snr * q__3.r, q__2.i = snr * q__3.i;
	    q__1.r = *a3 * q__2.r, q__1.i = *a3 * q__2.i;
	    ua12.r = q__1.r, ua12.i = q__1.i;

	    r__1 = csl * *b1;
	    r_cnjg(&q__4, &d1);
	    q__3.r = snl * q__4.r, q__3.i = snl * q__4.i;
	    q__2.r = q__3.r * b2->r - q__3.i * b2->i, q__2.i = q__3.r * b2->i 
		    + q__3.i * b2->r;
	    q__1.r = r__1 + q__2.r, q__1.i = q__2.i;
	    vb11.r = q__1.r, vb11.i = q__1.i;
	    r_cnjg(&q__3, &d1);
	    q__2.r = snl * q__3.r, q__2.i = snl * q__3.i;
	    q__1.r = *b3 * q__2.r, q__1.i = *b3 * q__2.i;
	    vb12.r = q__1.r, vb12.i = q__1.i;

	    aua11 = abs(csr) * abs(*a1) + abs(snr) * ((r__1 = a2->r, abs(r__1)
		    ) + (r__2 = r_imag(a2), abs(r__2)));
	    avb11 = abs(csl) * abs(*b1) + abs(snl) * ((r__1 = b2->r, abs(r__1)
		    ) + (r__2 = r_imag(b2), abs(r__2)));

/*           zero (1,1) elements of U**H *A and V**H *B, and then swap. */

	    if ((r__1 = ua11.r, abs(r__1)) + (r__2 = r_imag(&ua11), abs(r__2))
		     + ((r__3 = ua12.r, abs(r__3)) + (r__4 = r_imag(&ua12), 
		    abs(r__4))) == 0.f) {
		clartg_(&vb12, &vb11, csq, snq, &r__);
	    } else if ((r__1 = vb11.r, abs(r__1)) + (r__2 = r_imag(&vb11), 
		    abs(r__2)) + ((r__3 = vb12.r, abs(r__3)) + (r__4 = r_imag(
		    &vb12), abs(r__4))) == 0.f) {
		clartg_(&ua12, &ua11, csq, snq, &r__);
	    } else if (aua11 / ((r__1 = ua11.r, abs(r__1)) + (r__2 = r_imag(&
		    ua11), abs(r__2)) + ((r__3 = ua12.r, abs(r__3)) + (r__4 = 
		    r_imag(&ua12), abs(r__4)))) <= avb11 / ((r__5 = vb11.r, 
		    abs(r__5)) + (r__6 = r_imag(&vb11), abs(r__6)) + ((r__7 = 
		    vb12.r, abs(r__7)) + (r__8 = r_imag(&vb12), abs(r__8))))) 
		    {
		clartg_(&ua12, &ua11, csq, snq, &r__);
	    } else {
		clartg_(&vb12, &vb11, csq, snq, &r__);
	    }

	    *csu = snr;
	    r_cnjg(&q__2, &d1);
	    q__1.r = csr * q__2.r, q__1.i = csr * q__2.i;
	    snu->r = q__1.r, snu->i = q__1.i;
	    *csv = snl;
	    r_cnjg(&q__2, &d1);
	    q__1.r = csl * q__2.r, q__1.i = csl * q__2.i;
	    snv->r = q__1.r, snv->i = q__1.i;

	}

    }

    return;

/*     End of CLAGS2 */

} /* clags2_ */

