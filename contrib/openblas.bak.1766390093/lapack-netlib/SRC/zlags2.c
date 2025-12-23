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




/* > \brief \b ZLAGS2 */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download ZLAGS2 + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlags2.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlags2.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlags2.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE ZLAGS2( UPPER, A1, A2, A3, B1, B2, B3, CSU, SNU, CSV, */
/*                          SNV, CSQ, SNQ ) */

/*       LOGICAL            UPPER */
/*       DOUBLE PRECISION   A1, A3, B1, B3, CSQ, CSU, CSV */
/*       COMPLEX*16         A2, B2, SNQ, SNU, SNV */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > ZLAGS2 computes 2-by-2 unitary matrices U, V and Q, such */
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
/* >          A1 is DOUBLE PRECISION */
/* > \endverbatim */
/* > */
/* > \param[in] A2 */
/* > \verbatim */
/* >          A2 is COMPLEX*16 */
/* > \endverbatim */
/* > */
/* > \param[in] A3 */
/* > \verbatim */
/* >          A3 is DOUBLE PRECISION */
/* >          On entry, A1, A2 and A3 are elements of the input 2-by-2 */
/* >          upper (lower) triangular matrix A. */
/* > \endverbatim */
/* > */
/* > \param[in] B1 */
/* > \verbatim */
/* >          B1 is DOUBLE PRECISION */
/* > \endverbatim */
/* > */
/* > \param[in] B2 */
/* > \verbatim */
/* >          B2 is COMPLEX*16 */
/* > \endverbatim */
/* > */
/* > \param[in] B3 */
/* > \verbatim */
/* >          B3 is DOUBLE PRECISION */
/* >          On entry, B1, B2 and B3 are elements of the input 2-by-2 */
/* >          upper (lower) triangular matrix B. */
/* > \endverbatim */
/* > */
/* > \param[out] CSU */
/* > \verbatim */
/* >          CSU is DOUBLE PRECISION */
/* > \endverbatim */
/* > */
/* > \param[out] SNU */
/* > \verbatim */
/* >          SNU is COMPLEX*16 */
/* >          The desired unitary matrix U. */
/* > \endverbatim */
/* > */
/* > \param[out] CSV */
/* > \verbatim */
/* >          CSV is DOUBLE PRECISION */
/* > \endverbatim */
/* > */
/* > \param[out] SNV */
/* > \verbatim */
/* >          SNV is COMPLEX*16 */
/* >          The desired unitary matrix V. */
/* > \endverbatim */
/* > */
/* > \param[out] CSQ */
/* > \verbatim */
/* >          CSQ is DOUBLE PRECISION */
/* > \endverbatim */
/* > */
/* > \param[out] SNQ */
/* > \verbatim */
/* >          SNQ is COMPLEX*16 */
/* >          The desired unitary matrix Q. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date December 2016 */

/* > \ingroup complex16OTHERauxiliary */

/*  ===================================================================== */
/* Subroutine */ void zlags2_(logical *upper, doublereal *a1, doublecomplex *
	a2, doublereal *a3, doublereal *b1, doublecomplex *b2, doublereal *b3,
	 doublereal *csu, doublecomplex *snu, doublereal *csv, doublecomplex *
	snv, doublereal *csq, doublecomplex *snq)
{
    /* System generated locals */
    doublereal d__1, d__2, d__3, d__4, d__5, d__6, d__7, d__8;
    doublecomplex z__1, z__2, z__3, z__4, z__5;

    /* Local variables */
    doublereal aua11, aua12, aua21, aua22, avb12, avb11, avb21, avb22, ua11r, 
	    ua22r, vb11r, vb22r, a;
    doublecomplex b, c__;
    doublereal d__;
    doublecomplex r__, d1;
    doublereal s1, s2;
    extern /* Subroutine */ void dlasv2_(doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *, doublereal *, 
	    doublereal *, doublereal *, doublereal *);
    doublereal fb, fc;
    extern /* Subroutine */ void zlartg_(doublecomplex *, doublecomplex *, 
	    doublereal *, doublecomplex *, doublecomplex *);
    doublecomplex ua11, ua12, ua21, ua22, vb11, vb12, vb21, vb22;
    doublereal csl, csr, snl, snr;


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
	z__2.r = *b1 * a2->r, z__2.i = *b1 * a2->i;
	z__3.r = *a1 * b2->r, z__3.i = *a1 * b2->i;
	z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
	b.r = z__1.r, b.i = z__1.i;
	fb = z_abs(&b);

/*        Transform complex 2-by-2 matrix C to real matrix by unitary */
/*        diagonal matrix diag(1,D1). */

	d1.r = 1., d1.i = 0.;
	if (fb != 0.) {
	    z__1.r = b.r / fb, z__1.i = b.i / fb;
	    d1.r = z__1.r, d1.i = z__1.i;
	}

/*        The SVD of real 2 by 2 triangular C */

/*         ( CSL -SNL )*( A B )*(  CSR  SNR ) = ( R 0 ) */
/*         ( SNL  CSL ) ( 0 D ) ( -SNR  CSR )   ( 0 T ) */

	dlasv2_(&a, &fb, &d__, &s1, &s2, &snr, &csr, &snl, &csl);

	if (abs(csl) >= abs(snl) || abs(csr) >= abs(snr)) {

/*           Compute the (1,1) and (1,2) elements of U**H *A and V**H *B, */
/*           and (1,2) element of |U|**H *|A| and |V|**H *|B|. */

	    ua11r = csl * *a1;
	    z__2.r = csl * a2->r, z__2.i = csl * a2->i;
	    z__4.r = snl * d1.r, z__4.i = snl * d1.i;
	    z__3.r = *a3 * z__4.r, z__3.i = *a3 * z__4.i;
	    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
	    ua12.r = z__1.r, ua12.i = z__1.i;

	    vb11r = csr * *b1;
	    z__2.r = csr * b2->r, z__2.i = csr * b2->i;
	    z__4.r = snr * d1.r, z__4.i = snr * d1.i;
	    z__3.r = *b3 * z__4.r, z__3.i = *b3 * z__4.i;
	    z__1.r = z__2.r + z__3.r, z__1.i = z__2.i + z__3.i;
	    vb12.r = z__1.r, vb12.i = z__1.i;

	    aua12 = abs(csl) * ((d__1 = a2->r, abs(d__1)) + (d__2 = d_imag(a2)
		    , abs(d__2))) + abs(snl) * abs(*a3);
	    avb12 = abs(csr) * ((d__1 = b2->r, abs(d__1)) + (d__2 = d_imag(b2)
		    , abs(d__2))) + abs(snr) * abs(*b3);

/*           zero (1,2) elements of U**H *A and V**H *B */

	    if (abs(ua11r) + ((d__1 = ua12.r, abs(d__1)) + (d__2 = d_imag(&
		    ua12), abs(d__2))) == 0.) {
		z__2.r = vb11r, z__2.i = 0.;
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &vb12);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    } else if (abs(vb11r) + ((d__1 = vb12.r, abs(d__1)) + (d__2 = 
		    d_imag(&vb12), abs(d__2))) == 0.) {
		z__2.r = ua11r, z__2.i = 0.;
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &ua12);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    } else if (aua12 / (abs(ua11r) + ((d__1 = ua12.r, abs(d__1)) + (
		    d__2 = d_imag(&ua12), abs(d__2)))) <= avb12 / (abs(vb11r) 
		    + ((d__3 = vb12.r, abs(d__3)) + (d__4 = d_imag(&vb12), 
		    abs(d__4))))) {
		z__2.r = ua11r, z__2.i = 0.;
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &ua12);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    } else {
		z__2.r = vb11r, z__2.i = 0.;
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &vb12);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    }

	    *csu = csl;
	    z__2.r = -d1.r, z__2.i = -d1.i;
	    z__1.r = snl * z__2.r, z__1.i = snl * z__2.i;
	    snu->r = z__1.r, snu->i = z__1.i;
	    *csv = csr;
	    z__2.r = -d1.r, z__2.i = -d1.i;
	    z__1.r = snr * z__2.r, z__1.i = snr * z__2.i;
	    snv->r = z__1.r, snv->i = z__1.i;

	} else {

/*           Compute the (2,1) and (2,2) elements of U**H *A and V**H *B, */
/*           and (2,2) element of |U|**H *|A| and |V|**H *|B|. */

	    d_cnjg(&z__4, &d1);
	    z__3.r = -z__4.r, z__3.i = -z__4.i;
	    z__2.r = snl * z__3.r, z__2.i = snl * z__3.i;
	    z__1.r = *a1 * z__2.r, z__1.i = *a1 * z__2.i;
	    ua21.r = z__1.r, ua21.i = z__1.i;
	    d_cnjg(&z__5, &d1);
	    z__4.r = -z__5.r, z__4.i = -z__5.i;
	    z__3.r = snl * z__4.r, z__3.i = snl * z__4.i;
	    z__2.r = z__3.r * a2->r - z__3.i * a2->i, z__2.i = z__3.r * a2->i 
		    + z__3.i * a2->r;
	    d__1 = csl * *a3;
	    z__1.r = z__2.r + d__1, z__1.i = z__2.i;
	    ua22.r = z__1.r, ua22.i = z__1.i;

	    d_cnjg(&z__4, &d1);
	    z__3.r = -z__4.r, z__3.i = -z__4.i;
	    z__2.r = snr * z__3.r, z__2.i = snr * z__3.i;
	    z__1.r = *b1 * z__2.r, z__1.i = *b1 * z__2.i;
	    vb21.r = z__1.r, vb21.i = z__1.i;
	    d_cnjg(&z__5, &d1);
	    z__4.r = -z__5.r, z__4.i = -z__5.i;
	    z__3.r = snr * z__4.r, z__3.i = snr * z__4.i;
	    z__2.r = z__3.r * b2->r - z__3.i * b2->i, z__2.i = z__3.r * b2->i 
		    + z__3.i * b2->r;
	    d__1 = csr * *b3;
	    z__1.r = z__2.r + d__1, z__1.i = z__2.i;
	    vb22.r = z__1.r, vb22.i = z__1.i;

	    aua22 = abs(snl) * ((d__1 = a2->r, abs(d__1)) + (d__2 = d_imag(a2)
		    , abs(d__2))) + abs(csl) * abs(*a3);
	    avb22 = abs(snr) * ((d__1 = b2->r, abs(d__1)) + (d__2 = d_imag(b2)
		    , abs(d__2))) + abs(csr) * abs(*b3);

/*           zero (2,2) elements of U**H *A and V**H *B, and then swap. */

	    if ((d__1 = ua21.r, abs(d__1)) + (d__2 = d_imag(&ua21), abs(d__2))
		     + ((d__3 = ua22.r, abs(d__3)) + (d__4 = d_imag(&ua22), 
		    abs(d__4))) == 0.) {
		d_cnjg(&z__2, &vb21);
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &vb22);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    } else if ((d__1 = vb21.r, abs(d__1)) + (d__2 = d_imag(&vb21), 
		    abs(d__2)) + z_abs(&vb22) == 0.) {
		d_cnjg(&z__2, &ua21);
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &ua22);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    } else if (aua22 / ((d__1 = ua21.r, abs(d__1)) + (d__2 = d_imag(&
		    ua21), abs(d__2)) + ((d__3 = ua22.r, abs(d__3)) + (d__4 = 
		    d_imag(&ua22), abs(d__4)))) <= avb22 / ((d__5 = vb21.r, 
		    abs(d__5)) + (d__6 = d_imag(&vb21), abs(d__6)) + ((d__7 = 
		    vb22.r, abs(d__7)) + (d__8 = d_imag(&vb22), abs(d__8))))) 
		    {
		d_cnjg(&z__2, &ua21);
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &ua22);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    } else {
		d_cnjg(&z__2, &vb21);
		z__1.r = -z__2.r, z__1.i = -z__2.i;
		d_cnjg(&z__3, &vb22);
		zlartg_(&z__1, &z__3, csq, snq, &r__);
	    }

	    *csu = snl;
	    z__1.r = csl * d1.r, z__1.i = csl * d1.i;
	    snu->r = z__1.r, snu->i = z__1.i;
	    *csv = snr;
	    z__1.r = csr * d1.r, z__1.i = csr * d1.i;
	    snv->r = z__1.r, snv->i = z__1.i;

	}

    } else {

/*        Input matrices A and B are lower triangular matrices */

/*        Form matrix C = A*adj(B) = ( a 0 ) */
/*                                   ( c d ) */

	a = *a1 * *b3;
	d__ = *a3 * *b1;
	z__2.r = *b3 * a2->r, z__2.i = *b3 * a2->i;
	z__3.r = *a3 * b2->r, z__3.i = *a3 * b2->i;
	z__1.r = z__2.r - z__3.r, z__1.i = z__2.i - z__3.i;
	c__.r = z__1.r, c__.i = z__1.i;
	fc = z_abs(&c__);

/*        Transform complex 2-by-2 matrix C to real matrix by unitary */
/*        diagonal matrix diag(d1,1). */

	d1.r = 1., d1.i = 0.;
	if (fc != 0.) {
	    z__1.r = c__.r / fc, z__1.i = c__.i / fc;
	    d1.r = z__1.r, d1.i = z__1.i;
	}

/*        The SVD of real 2 by 2 triangular C */

/*         ( CSL -SNL )*( A 0 )*(  CSR  SNR ) = ( R 0 ) */
/*         ( SNL  CSL ) ( C D ) ( -SNR  CSR )   ( 0 T ) */

	dlasv2_(&a, &fc, &d__, &s1, &s2, &snr, &csr, &snl, &csl);

	if (abs(csr) >= abs(snr) || abs(csl) >= abs(snl)) {

/*           Compute the (2,1) and (2,2) elements of U**H *A and V**H *B, */
/*           and (2,1) element of |U|**H *|A| and |V|**H *|B|. */

	    z__4.r = -d1.r, z__4.i = -d1.i;
	    z__3.r = snr * z__4.r, z__3.i = snr * z__4.i;
	    z__2.r = *a1 * z__3.r, z__2.i = *a1 * z__3.i;
	    z__5.r = csr * a2->r, z__5.i = csr * a2->i;
	    z__1.r = z__2.r + z__5.r, z__1.i = z__2.i + z__5.i;
	    ua21.r = z__1.r, ua21.i = z__1.i;
	    ua22r = csr * *a3;

	    z__4.r = -d1.r, z__4.i = -d1.i;
	    z__3.r = snl * z__4.r, z__3.i = snl * z__4.i;
	    z__2.r = *b1 * z__3.r, z__2.i = *b1 * z__3.i;
	    z__5.r = csl * b2->r, z__5.i = csl * b2->i;
	    z__1.r = z__2.r + z__5.r, z__1.i = z__2.i + z__5.i;
	    vb21.r = z__1.r, vb21.i = z__1.i;
	    vb22r = csl * *b3;

	    aua21 = abs(snr) * abs(*a1) + abs(csr) * ((d__1 = a2->r, abs(d__1)
		    ) + (d__2 = d_imag(a2), abs(d__2)));
	    avb21 = abs(snl) * abs(*b1) + abs(csl) * ((d__1 = b2->r, abs(d__1)
		    ) + (d__2 = d_imag(b2), abs(d__2)));

/*           zero (2,1) elements of U**H *A and V**H *B. */

	    if ((d__1 = ua21.r, abs(d__1)) + (d__2 = d_imag(&ua21), abs(d__2))
		     + abs(ua22r) == 0.) {
		z__1.r = vb22r, z__1.i = 0.;
		zlartg_(&z__1, &vb21, csq, snq, &r__);
	    } else if ((d__1 = vb21.r, abs(d__1)) + (d__2 = d_imag(&vb21), 
		    abs(d__2)) + abs(vb22r) == 0.) {
		z__1.r = ua22r, z__1.i = 0.;
		zlartg_(&z__1, &ua21, csq, snq, &r__);
	    } else if (aua21 / ((d__1 = ua21.r, abs(d__1)) + (d__2 = d_imag(&
		    ua21), abs(d__2)) + abs(ua22r)) <= avb21 / ((d__3 = 
		    vb21.r, abs(d__3)) + (d__4 = d_imag(&vb21), abs(d__4)) + 
		    abs(vb22r))) {
		z__1.r = ua22r, z__1.i = 0.;
		zlartg_(&z__1, &ua21, csq, snq, &r__);
	    } else {
		z__1.r = vb22r, z__1.i = 0.;
		zlartg_(&z__1, &vb21, csq, snq, &r__);
	    }

	    *csu = csr;
	    d_cnjg(&z__3, &d1);
	    z__2.r = -z__3.r, z__2.i = -z__3.i;
	    z__1.r = snr * z__2.r, z__1.i = snr * z__2.i;
	    snu->r = z__1.r, snu->i = z__1.i;
	    *csv = csl;
	    d_cnjg(&z__3, &d1);
	    z__2.r = -z__3.r, z__2.i = -z__3.i;
	    z__1.r = snl * z__2.r, z__1.i = snl * z__2.i;
	    snv->r = z__1.r, snv->i = z__1.i;

	} else {

/*           Compute the (1,1) and (1,2) elements of U**H *A and V**H *B, */
/*           and (1,1) element of |U|**H *|A| and |V|**H *|B|. */

	    d__1 = csr * *a1;
	    d_cnjg(&z__4, &d1);
	    z__3.r = snr * z__4.r, z__3.i = snr * z__4.i;
	    z__2.r = z__3.r * a2->r - z__3.i * a2->i, z__2.i = z__3.r * a2->i 
		    + z__3.i * a2->r;
	    z__1.r = d__1 + z__2.r, z__1.i = z__2.i;
	    ua11.r = z__1.r, ua11.i = z__1.i;
	    d_cnjg(&z__3, &d1);
	    z__2.r = snr * z__3.r, z__2.i = snr * z__3.i;
	    z__1.r = *a3 * z__2.r, z__1.i = *a3 * z__2.i;
	    ua12.r = z__1.r, ua12.i = z__1.i;

	    d__1 = csl * *b1;
	    d_cnjg(&z__4, &d1);
	    z__3.r = snl * z__4.r, z__3.i = snl * z__4.i;
	    z__2.r = z__3.r * b2->r - z__3.i * b2->i, z__2.i = z__3.r * b2->i 
		    + z__3.i * b2->r;
	    z__1.r = d__1 + z__2.r, z__1.i = z__2.i;
	    vb11.r = z__1.r, vb11.i = z__1.i;
	    d_cnjg(&z__3, &d1);
	    z__2.r = snl * z__3.r, z__2.i = snl * z__3.i;
	    z__1.r = *b3 * z__2.r, z__1.i = *b3 * z__2.i;
	    vb12.r = z__1.r, vb12.i = z__1.i;

	    aua11 = abs(csr) * abs(*a1) + abs(snr) * ((d__1 = a2->r, abs(d__1)
		    ) + (d__2 = d_imag(a2), abs(d__2)));
	    avb11 = abs(csl) * abs(*b1) + abs(snl) * ((d__1 = b2->r, abs(d__1)
		    ) + (d__2 = d_imag(b2), abs(d__2)));

/*           zero (1,1) elements of U**H *A and V**H *B, and then swap. */

	    if ((d__1 = ua11.r, abs(d__1)) + (d__2 = d_imag(&ua11), abs(d__2))
		     + ((d__3 = ua12.r, abs(d__3)) + (d__4 = d_imag(&ua12), 
		    abs(d__4))) == 0.) {
		zlartg_(&vb12, &vb11, csq, snq, &r__);
	    } else if ((d__1 = vb11.r, abs(d__1)) + (d__2 = d_imag(&vb11), 
		    abs(d__2)) + ((d__3 = vb12.r, abs(d__3)) + (d__4 = d_imag(
		    &vb12), abs(d__4))) == 0.) {
		zlartg_(&ua12, &ua11, csq, snq, &r__);
	    } else if (aua11 / ((d__1 = ua11.r, abs(d__1)) + (d__2 = d_imag(&
		    ua11), abs(d__2)) + ((d__3 = ua12.r, abs(d__3)) + (d__4 = 
		    d_imag(&ua12), abs(d__4)))) <= avb11 / ((d__5 = vb11.r, 
		    abs(d__5)) + (d__6 = d_imag(&vb11), abs(d__6)) + ((d__7 = 
		    vb12.r, abs(d__7)) + (d__8 = d_imag(&vb12), abs(d__8))))) 
		    {
		zlartg_(&ua12, &ua11, csq, snq, &r__);
	    } else {
		zlartg_(&vb12, &vb11, csq, snq, &r__);
	    }

	    *csu = snr;
	    d_cnjg(&z__2, &d1);
	    z__1.r = csr * z__2.r, z__1.i = csr * z__2.i;
	    snu->r = z__1.r, snu->i = z__1.i;
	    *csv = snl;
	    d_cnjg(&z__2, &d1);
	    z__1.r = csl * z__2.r, z__1.i = csl * z__2.i;
	    snv->r = z__1.r, snv->i = z__1.i;

	}

    }

    return;

/*     End of ZLAGS2 */

} /* zlags2_ */

