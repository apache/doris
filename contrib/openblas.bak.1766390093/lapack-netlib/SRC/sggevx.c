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
#define s_cat(lpp, rpp, rnp, np, llp) { 	ftnlen i, nc, ll; char *f__rp, *lp; 	ll = (llp); lp = (lpp); 	for(i=0; i < (int)*(np); ++i) {         	nc = ll; 	        if((rnp)[i] < nc) nc = (rnp)[i]; 	        ll -= nc;         	f__rp = (rpp)[i]; 	        while(--nc >= 0) *lp++ = *(f__rp)++;         } 	while(--ll >= 0) *lp++ = ' '; }
#define s_cmp(a,b,c,d) ((integer)strncmp((a),(b),f2cmin((c),(d))))
#define s_copy(A,B,C,D) { int __i,__m; for (__i=0, __m=f2cmin((C),(D)); __i<__m && (B)[__i] != 0; ++__i) (A)[__i] = (B)[__i]; }
#define sig_die(s, kill) { exit(1); }
#define s_stop(s, n) {exit(0);}
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle() continue;
#define myceiling(w) {ceil(w)}
#define myhuge(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc(w,s,e,n) {dmaxloc_(w,*(s),*(e),n)}

/*  -- translated by f2c (version 20000121).
   You must link the resulting object file with the libraries:
	-lf2c -lm   (in that order)
*/




/* Table of constant values */

static integer c__1 = 1;
static integer c__0 = 0;
static real c_b57 = 0.f;
static real c_b58 = 1.f;

/* > \brief <b> SGGEVX computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE mat
rices</b> */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SGGEVX + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sggevx.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sggevx.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sggevx.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SGGEVX( BALANC, JOBVL, JOBVR, SENSE, N, A, LDA, B, LDB, */
/*                          ALPHAR, ALPHAI, BETA, VL, LDVL, VR, LDVR, ILO, */
/*                          IHI, LSCALE, RSCALE, ABNRM, BBNRM, RCONDE, */
/*                          RCONDV, WORK, LWORK, IWORK, BWORK, INFO ) */

/*       CHARACTER          BALANC, JOBVL, JOBVR, SENSE */
/*       INTEGER            IHI, ILO, INFO, LDA, LDB, LDVL, LDVR, LWORK, N */
/*       REAL               ABNRM, BBNRM */
/*       LOGICAL            BWORK( * ) */
/*       INTEGER            IWORK( * ) */
/*       REAL               A( LDA, * ), ALPHAI( * ), ALPHAR( * ), */
/*      $                   B( LDB, * ), BETA( * ), LSCALE( * ), */
/*      $                   RCONDE( * ), RCONDV( * ), RSCALE( * ), */
/*      $                   VL( LDVL, * ), VR( LDVR, * ), WORK( * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SGGEVX computes for a pair of N-by-N real nonsymmetric matrices (A,B) */
/* > the generalized eigenvalues, and optionally, the left and/or right */
/* > generalized eigenvectors. */
/* > */
/* > Optionally also, it computes a balancing transformation to improve */
/* > the conditioning of the eigenvalues and eigenvectors (ILO, IHI, */
/* > LSCALE, RSCALE, ABNRM, and BBNRM), reciprocal condition numbers for */
/* > the eigenvalues (RCONDE), and reciprocal condition numbers for the */
/* > right eigenvectors (RCONDV). */
/* > */
/* > A generalized eigenvalue for a pair of matrices (A,B) is a scalar */
/* > lambda or a ratio alpha/beta = lambda, such that A - lambda*B is */
/* > singular. It is usually represented as the pair (alpha,beta), as */
/* > there is a reasonable interpretation for beta=0, and even for both */
/* > being zero. */
/* > */
/* > The right eigenvector v(j) corresponding to the eigenvalue lambda(j) */
/* > of (A,B) satisfies */
/* > */
/* >                  A * v(j) = lambda(j) * B * v(j) . */
/* > */
/* > The left eigenvector u(j) corresponding to the eigenvalue lambda(j) */
/* > of (A,B) satisfies */
/* > */
/* >                  u(j)**H * A  = lambda(j) * u(j)**H * B. */
/* > */
/* > where u(j)**H is the conjugate-transpose of u(j). */
/* > */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] BALANC */
/* > \verbatim */
/* >          BALANC is CHARACTER*1 */
/* >          Specifies the balance option to be performed. */
/* >          = 'N':  do not diagonally scale or permute; */
/* >          = 'P':  permute only; */
/* >          = 'S':  scale only; */
/* >          = 'B':  both permute and scale. */
/* >          Computed reciprocal condition numbers will be for the */
/* >          matrices after permuting and/or balancing. Permuting does */
/* >          not change condition numbers (in exact arithmetic), but */
/* >          balancing does. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBVL */
/* > \verbatim */
/* >          JOBVL is CHARACTER*1 */
/* >          = 'N':  do not compute the left generalized eigenvectors; */
/* >          = 'V':  compute the left generalized eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] JOBVR */
/* > \verbatim */
/* >          JOBVR is CHARACTER*1 */
/* >          = 'N':  do not compute the right generalized eigenvectors; */
/* >          = 'V':  compute the right generalized eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] SENSE */
/* > \verbatim */
/* >          SENSE is CHARACTER*1 */
/* >          Determines which reciprocal condition numbers are computed. */
/* >          = 'N': none are computed; */
/* >          = 'E': computed for eigenvalues only; */
/* >          = 'V': computed for eigenvectors only; */
/* >          = 'B': computed for eigenvalues and eigenvectors. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices A, B, VL, and VR.  N >= 0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] A */
/* > \verbatim */
/* >          A is REAL array, dimension (LDA, N) */
/* >          On entry, the matrix A in the pair (A,B). */
/* >          On exit, A has been overwritten. If JOBVL='V' or JOBVR='V' */
/* >          or both, then A contains the first part of the real Schur */
/* >          form of the "balanced" versions of the input A and B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDA */
/* > \verbatim */
/* >          LDA is INTEGER */
/* >          The leading dimension of A.  LDA >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[in,out] B */
/* > \verbatim */
/* >          B is REAL array, dimension (LDB, N) */
/* >          On entry, the matrix B in the pair (A,B). */
/* >          On exit, B has been overwritten. If JOBVL='V' or JOBVR='V' */
/* >          or both, then B contains the second part of the real Schur */
/* >          form of the "balanced" versions of the input A and B. */
/* > \endverbatim */
/* > */
/* > \param[in] LDB */
/* > \verbatim */
/* >          LDB is INTEGER */
/* >          The leading dimension of B.  LDB >= f2cmax(1,N). */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHAR */
/* > \verbatim */
/* >          ALPHAR is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHAI */
/* > \verbatim */
/* >          ALPHAI is REAL array, dimension (N) */
/* > \endverbatim */
/* > */
/* > \param[out] BETA */
/* > \verbatim */
/* >          BETA is REAL array, dimension (N) */
/* >          On exit, (ALPHAR(j) + ALPHAI(j)*i)/BETA(j), j=1,...,N, will */
/* >          be the generalized eigenvalues.  If ALPHAI(j) is zero, then */
/* >          the j-th eigenvalue is real; if positive, then the j-th and */
/* >          (j+1)-st eigenvalues are a complex conjugate pair, with */
/* >          ALPHAI(j+1) negative. */
/* > */
/* >          Note: the quotients ALPHAR(j)/BETA(j) and ALPHAI(j)/BETA(j) */
/* >          may easily over- or underflow, and BETA(j) may even be zero. */
/* >          Thus, the user should avoid naively computing the ratio */
/* >          ALPHA/BETA. However, ALPHAR and ALPHAI will be always less */
/* >          than and usually comparable with norm(A) in magnitude, and */
/* >          BETA always less than and usually comparable with norm(B). */
/* > \endverbatim */
/* > */
/* > \param[out] VL */
/* > \verbatim */
/* >          VL is REAL array, dimension (LDVL,N) */
/* >          If JOBVL = 'V', the left eigenvectors u(j) are stored one */
/* >          after another in the columns of VL, in the same order as */
/* >          their eigenvalues. If the j-th eigenvalue is real, then */
/* >          u(j) = VL(:,j), the j-th column of VL. If the j-th and */
/* >          (j+1)-th eigenvalues form a complex conjugate pair, then */
/* >          u(j) = VL(:,j)+i*VL(:,j+1) and u(j+1) = VL(:,j)-i*VL(:,j+1). */
/* >          Each eigenvector will be scaled so the largest component have */
/* >          abs(real part) + abs(imag. part) = 1. */
/* >          Not referenced if JOBVL = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVL */
/* > \verbatim */
/* >          LDVL is INTEGER */
/* >          The leading dimension of the matrix VL. LDVL >= 1, and */
/* >          if JOBVL = 'V', LDVL >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] VR */
/* > \verbatim */
/* >          VR is REAL array, dimension (LDVR,N) */
/* >          If JOBVR = 'V', the right eigenvectors v(j) are stored one */
/* >          after another in the columns of VR, in the same order as */
/* >          their eigenvalues. If the j-th eigenvalue is real, then */
/* >          v(j) = VR(:,j), the j-th column of VR. If the j-th and */
/* >          (j+1)-th eigenvalues form a complex conjugate pair, then */
/* >          v(j) = VR(:,j)+i*VR(:,j+1) and v(j+1) = VR(:,j)-i*VR(:,j+1). */
/* >          Each eigenvector will be scaled so the largest component have */
/* >          abs(real part) + abs(imag. part) = 1. */
/* >          Not referenced if JOBVR = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDVR */
/* > \verbatim */
/* >          LDVR is INTEGER */
/* >          The leading dimension of the matrix VR. LDVR >= 1, and */
/* >          if JOBVR = 'V', LDVR >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] ILO */
/* > \verbatim */
/* >          ILO is INTEGER */
/* > \endverbatim */
/* > */
/* > \param[out] IHI */
/* > \verbatim */
/* >          IHI is INTEGER */
/* >          ILO and IHI are integer values such that on exit */
/* >          A(i,j) = 0 and B(i,j) = 0 if i > j and */
/* >          j = 1,...,ILO-1 or i = IHI+1,...,N. */
/* >          If BALANC = 'N' or 'S', ILO = 1 and IHI = N. */
/* > \endverbatim */
/* > */
/* > \param[out] LSCALE */
/* > \verbatim */
/* >          LSCALE is REAL array, dimension (N) */
/* >          Details of the permutations and scaling factors applied */
/* >          to the left side of A and B.  If PL(j) is the index of the */
/* >          row interchanged with row j, and DL(j) is the scaling */
/* >          factor applied to row j, then */
/* >            LSCALE(j) = PL(j)  for j = 1,...,ILO-1 */
/* >                      = DL(j)  for j = ILO,...,IHI */
/* >                      = PL(j)  for j = IHI+1,...,N. */
/* >          The order in which the interchanges are made is N to IHI+1, */
/* >          then 1 to ILO-1. */
/* > \endverbatim */
/* > */
/* > \param[out] RSCALE */
/* > \verbatim */
/* >          RSCALE is REAL array, dimension (N) */
/* >          Details of the permutations and scaling factors applied */
/* >          to the right side of A and B.  If PR(j) is the index of the */
/* >          column interchanged with column j, and DR(j) is the scaling */
/* >          factor applied to column j, then */
/* >            RSCALE(j) = PR(j)  for j = 1,...,ILO-1 */
/* >                      = DR(j)  for j = ILO,...,IHI */
/* >                      = PR(j)  for j = IHI+1,...,N */
/* >          The order in which the interchanges are made is N to IHI+1, */
/* >          then 1 to ILO-1. */
/* > \endverbatim */
/* > */
/* > \param[out] ABNRM */
/* > \verbatim */
/* >          ABNRM is REAL */
/* >          The one-norm of the balanced matrix A. */
/* > \endverbatim */
/* > */
/* > \param[out] BBNRM */
/* > \verbatim */
/* >          BBNRM is REAL */
/* >          The one-norm of the balanced matrix B. */
/* > \endverbatim */
/* > */
/* > \param[out] RCONDE */
/* > \verbatim */
/* >          RCONDE is REAL array, dimension (N) */
/* >          If SENSE = 'E' or 'B', the reciprocal condition numbers of */
/* >          the eigenvalues, stored in consecutive elements of the array. */
/* >          For a complex conjugate pair of eigenvalues two consecutive */
/* >          elements of RCONDE are set to the same value. Thus RCONDE(j), */
/* >          RCONDV(j), and the j-th columns of VL and VR all correspond */
/* >          to the j-th eigenpair. */
/* >          If SENSE = 'N' or 'V', RCONDE is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] RCONDV */
/* > \verbatim */
/* >          RCONDV is REAL array, dimension (N) */
/* >          If SENSE = 'V' or 'B', the estimated reciprocal condition */
/* >          numbers of the eigenvectors, stored in consecutive elements */
/* >          of the array. For a complex eigenvector two consecutive */
/* >          elements of RCONDV are set to the same value. If the */
/* >          eigenvalues cannot be reordered to compute RCONDV(j), */
/* >          RCONDV(j) is set to 0; this can only occur when the true */
/* >          value would be very small anyway. */
/* >          If SENSE = 'N' or 'E', RCONDV is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO = 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK. LWORK >= f2cmax(1,2*N). */
/* >          If BALANC = 'S' or 'B', or JOBVL = 'V', or JOBVR = 'V', */
/* >          LWORK >= f2cmax(1,6*N). */
/* >          If SENSE = 'E', LWORK >= f2cmax(1,10*N). */
/* >          If SENSE = 'V' or 'B', LWORK >= 2*N*N+8*N+16. */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] IWORK */
/* > \verbatim */
/* >          IWORK is INTEGER array, dimension (N+6) */
/* >          If SENSE = 'E', IWORK is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] BWORK */
/* > \verbatim */
/* >          BWORK is LOGICAL array, dimension (N) */
/* >          If SENSE = 'N', BWORK is not referenced. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0:  successful exit */
/* >          < 0:  if INFO = -i, the i-th argument had an illegal value. */
/* >          = 1,...,N: */
/* >                The QZ iteration failed.  No eigenvectors have been */
/* >                calculated, but ALPHAR(j), ALPHAI(j), and BETA(j) */
/* >                should be correct for j=INFO+1,...,N. */
/* >          > N:  =N+1: other than QZ iteration failed in SHGEQZ. */
/* >                =N+2: error return from STGEVC. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date April 2012 */

/* > \ingroup realGEeigen */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Balancing a matrix pair (A,B) includes, first, permuting rows and */
/* >  columns to isolate eigenvalues, second, applying diagonal similarity */
/* >  transformation to the rows and columns to make the rows and columns */
/* >  as close in norm as possible. The computed reciprocal condition */
/* >  numbers correspond to the balanced matrix. Permuting rows and columns */
/* >  will not change the condition numbers (in exact arithmetic) but */
/* >  diagonal scaling will.  For further explanation of balancing, see */
/* >  section 4.11.1.2 of LAPACK Users' Guide. */
/* > */
/* >  An approximate error bound on the chordal distance between the i-th */
/* >  computed generalized eigenvalue w and the corresponding exact */
/* >  eigenvalue lambda is */
/* > */
/* >       chord(w, lambda) <= EPS * norm(ABNRM, BBNRM) / RCONDE(I) */
/* > */
/* >  An approximate error bound for the angle between the i-th computed */
/* >  eigenvector VL(i) or VR(i) is given by */
/* > */
/* >       EPS * norm(ABNRM, BBNRM) / DIF(i). */
/* > */
/* >  For further explanation of the reciprocal condition numbers RCONDE */
/* >  and RCONDV, see section 4.11 of LAPACK User's Guide. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void sggevx_(char *balanc, char *jobvl, char *jobvr, char *
	sense, integer *n, real *a, integer *lda, real *b, integer *ldb, real 
	*alphar, real *alphai, real *beta, real *vl, integer *ldvl, real *vr, 
	integer *ldvr, integer *ilo, integer *ihi, real *lscale, real *rscale,
	 real *abnrm, real *bbnrm, real *rconde, real *rcondv, real *work, 
	integer *lwork, integer *iwork, logical *bwork, integer *info)
{
    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, vl_dim1, vl_offset, vr_dim1, 
	    vr_offset, i__1, i__2;
    real r__1, r__2, r__3, r__4;

    /* Local variables */
    logical pair;
    real anrm, bnrm;
    integer ierr, itau;
    real temp;
    logical ilvl, ilvr;
    integer iwrk, iwrk1, i__, j, m;
    extern logical lsame_(char *, char *);
    integer icols;
    logical noscl;
    integer irows, jc;
    extern /* Subroutine */ void slabad_(real *, real *);
    integer in, mm, jr;
    extern /* Subroutine */ void sggbak_(char *, char *, integer *, integer *, 
	    integer *, real *, real *, integer *, real *, integer *, integer *
	    ), sggbal_(char *, integer *, real *, integer *, 
	    real *, integer *, integer *, integer *, real *, real *, real *, 
	    integer *);
    logical ilascl, ilbscl;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    extern void sgghrd_(
	    char *, char *, integer *, integer *, integer *, real *, integer *
	    , real *, integer *, real *, integer *, real *, integer *, 
	    integer *);
    logical ldumma[1];
    char chtemp[1];
    real bignum;
    extern /* Subroutine */ void slascl_(char *, integer *, integer *, real *, 
	    real *, integer *, integer *, real *, integer *, integer *);
    extern integer ilaenv_(integer *, char *, char *, integer *, integer *, 
	    integer *, integer *, ftnlen, ftnlen);
    extern real slamch_(char *);
    integer ijobvl;
    extern real slange_(char *, integer *, integer *, real *, integer *, real 
	    *);
    extern /* Subroutine */ void sgeqrf_(integer *, integer *, real *, integer 
	    *, real *, real *, integer *, integer *);
    integer ijobvr;
    extern /* Subroutine */ void slacpy_(char *, integer *, integer *, real *, 
	    integer *, real *, integer *);
    logical wantsb;
    extern /* Subroutine */ void slaset_(char *, integer *, integer *, real *, 
	    real *, real *, integer *);
    real anrmto;
    logical wantse;
    real bnrmto;
    extern /* Subroutine */ void shgeqz_(char *, char *, char *, integer *, 
	    integer *, integer *, real *, integer *, real *, integer *, real *
	    , real *, real *, real *, integer *, real *, integer *, real *, 
	    integer *, integer *), stgevc_(char *, 
	    char *, logical *, integer *, real *, integer *, real *, integer *
	    , real *, integer *, real *, integer *, integer *, integer *, 
	    real *, integer *), stgsna_(char *, char *, 
	    logical *, integer *, real *, integer *, real *, integer *, real *
	    , integer *, real *, integer *, real *, real *, integer *, 
	    integer *, real *, integer *, integer *, integer *);
    integer minwrk, maxwrk;
    logical wantsn;
    real smlnum;
    extern /* Subroutine */ void sorgqr_(integer *, integer *, integer *, real 
	    *, integer *, real *, real *, integer *, integer *);
    logical lquery, wantsv;
    extern /* Subroutine */ void sormqr_(char *, char *, integer *, integer *, 
	    integer *, real *, integer *, real *, real *, integer *, real *, 
	    integer *, integer *);
    real eps;
    logical ilv;


/*  -- LAPACK driver routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     April 2012 */


/*  ===================================================================== */


/*     Decode the input arguments */

    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --alphar;
    --alphai;
    --beta;
    vl_dim1 = *ldvl;
    vl_offset = 1 + vl_dim1 * 1;
    vl -= vl_offset;
    vr_dim1 = *ldvr;
    vr_offset = 1 + vr_dim1 * 1;
    vr -= vr_offset;
    --lscale;
    --rscale;
    --rconde;
    --rcondv;
    --work;
    --iwork;
    --bwork;

    /* Function Body */
    if (lsame_(jobvl, "N")) {
	ijobvl = 1;
	ilvl = FALSE_;
    } else if (lsame_(jobvl, "V")) {
	ijobvl = 2;
	ilvl = TRUE_;
    } else {
	ijobvl = -1;
	ilvl = FALSE_;
    }

    if (lsame_(jobvr, "N")) {
	ijobvr = 1;
	ilvr = FALSE_;
    } else if (lsame_(jobvr, "V")) {
	ijobvr = 2;
	ilvr = TRUE_;
    } else {
	ijobvr = -1;
	ilvr = FALSE_;
    }
    ilv = ilvl || ilvr;

    noscl = lsame_(balanc, "N") || lsame_(balanc, "P");
    wantsn = lsame_(sense, "N");
    wantse = lsame_(sense, "E");
    wantsv = lsame_(sense, "V");
    wantsb = lsame_(sense, "B");

/*     Test the input arguments */

    *info = 0;
    lquery = *lwork == -1;
    if (! (noscl || lsame_(balanc, "S") || lsame_(
	    balanc, "B"))) {
	*info = -1;
    } else if (ijobvl <= 0) {
	*info = -2;
    } else if (ijobvr <= 0) {
	*info = -3;
    } else if (! (wantsn || wantse || wantsb || wantsv)) {
	*info = -4;
    } else if (*n < 0) {
	*info = -5;
    } else if (*lda < f2cmax(1,*n)) {
	*info = -7;
    } else if (*ldb < f2cmax(1,*n)) {
	*info = -9;
    } else if (*ldvl < 1 || ilvl && *ldvl < *n) {
	*info = -14;
    } else if (*ldvr < 1 || ilvr && *ldvr < *n) {
	*info = -16;
    }

/*     Compute workspace */
/*      (Note: Comments in the code beginning "Workspace:" describe the */
/*       minimal amount of workspace needed at that point in the code, */
/*       as well as the preferred amount for good performance. */
/*       NB refers to the optimal block size for the immediately */
/*       following subroutine, as returned by ILAENV. The workspace is */
/*       computed assuming ILO = 1 and IHI = N, the worst case.) */

    if (*info == 0) {
	if (*n == 0) {
	    minwrk = 1;
	    maxwrk = 1;
	} else {
	    if (noscl && ! ilv) {
		minwrk = *n << 1;
	    } else {
		minwrk = *n * 6;
	    }
	    if (wantse) {
		minwrk = *n * 10;
	    } else if (wantsv || wantsb) {
		minwrk = (*n << 1) * (*n + 4) + 16;
	    }
	    maxwrk = minwrk;
/* Computing MAX */
	    i__1 = maxwrk, i__2 = *n + *n * ilaenv_(&c__1, "SGEQRF", " ", n, &
		    c__1, n, &c__0, (ftnlen)6, (ftnlen)1);
	    maxwrk = f2cmax(i__1,i__2);
/* Computing MAX */
	    i__1 = maxwrk, i__2 = *n + *n * ilaenv_(&c__1, "SORMQR", " ", n, &
		    c__1, n, &c__0, (ftnlen)6, (ftnlen)1);
	    maxwrk = f2cmax(i__1,i__2);
	    if (ilvl) {
/* Computing MAX */
		i__1 = maxwrk, i__2 = *n + *n * ilaenv_(&c__1, "SORGQR", 
			" ", n, &c__1, n, &c__0, (ftnlen)6, (ftnlen)1);
		maxwrk = f2cmax(i__1,i__2);
	    }
	}
	work[1] = (real) maxwrk;

	if (*lwork < minwrk && ! lquery) {
	    *info = -26;
	}
    }

    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SGGEVX", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*n == 0) {
	return;
    }


/*     Get machine constants */

    eps = slamch_("P");
    smlnum = slamch_("S");
    bignum = 1.f / smlnum;
    slabad_(&smlnum, &bignum);
    smlnum = sqrt(smlnum) / eps;
    bignum = 1.f / smlnum;

/*     Scale A if f2cmax element outside range [SMLNUM,BIGNUM] */

    anrm = slange_("M", n, n, &a[a_offset], lda, &work[1]);
    ilascl = FALSE_;
    if (anrm > 0.f && anrm < smlnum) {
	anrmto = smlnum;
	ilascl = TRUE_;
    } else if (anrm > bignum) {
	anrmto = bignum;
	ilascl = TRUE_;
    }
    if (ilascl) {
	slascl_("G", &c__0, &c__0, &anrm, &anrmto, n, n, &a[a_offset], lda, &
		ierr);
    }

/*     Scale B if f2cmax element outside range [SMLNUM,BIGNUM] */

    bnrm = slange_("M", n, n, &b[b_offset], ldb, &work[1]);
    ilbscl = FALSE_;
    if (bnrm > 0.f && bnrm < smlnum) {
	bnrmto = smlnum;
	ilbscl = TRUE_;
    } else if (bnrm > bignum) {
	bnrmto = bignum;
	ilbscl = TRUE_;
    }
    if (ilbscl) {
	slascl_("G", &c__0, &c__0, &bnrm, &bnrmto, n, n, &b[b_offset], ldb, &
		ierr);
    }

/*     Permute and/or balance the matrix pair (A,B) */
/*     (Workspace: need 6*N if BALANC = 'S' or 'B', 1 otherwise) */

    sggbal_(balanc, n, &a[a_offset], lda, &b[b_offset], ldb, ilo, ihi, &
	    lscale[1], &rscale[1], &work[1], &ierr);

/*     Compute ABNRM and BBNRM */

    *abnrm = slange_("1", n, n, &a[a_offset], lda, &work[1]);
    if (ilascl) {
	work[1] = *abnrm;
	slascl_("G", &c__0, &c__0, &anrmto, &anrm, &c__1, &c__1, &work[1], &
		c__1, &ierr);
	*abnrm = work[1];
    }

    *bbnrm = slange_("1", n, n, &b[b_offset], ldb, &work[1]);
    if (ilbscl) {
	work[1] = *bbnrm;
	slascl_("G", &c__0, &c__0, &bnrmto, &bnrm, &c__1, &c__1, &work[1], &
		c__1, &ierr);
	*bbnrm = work[1];
    }

/*     Reduce B to triangular form (QR decomposition of B) */
/*     (Workspace: need N, prefer N*NB ) */

    irows = *ihi + 1 - *ilo;
    if (ilv || ! wantsn) {
	icols = *n + 1 - *ilo;
    } else {
	icols = irows;
    }
    itau = 1;
    iwrk = itau + irows;
    i__1 = *lwork + 1 - iwrk;
    sgeqrf_(&irows, &icols, &b[*ilo + *ilo * b_dim1], ldb, &work[itau], &work[
	    iwrk], &i__1, &ierr);

/*     Apply the orthogonal transformation to A */
/*     (Workspace: need N, prefer N*NB) */

    i__1 = *lwork + 1 - iwrk;
    sormqr_("L", "T", &irows, &icols, &irows, &b[*ilo + *ilo * b_dim1], ldb, &
	    work[itau], &a[*ilo + *ilo * a_dim1], lda, &work[iwrk], &i__1, &
	    ierr);

/*     Initialize VL and/or VR */
/*     (Workspace: need N, prefer N*NB) */

    if (ilvl) {
	slaset_("Full", n, n, &c_b57, &c_b58, &vl[vl_offset], ldvl)
		;
	if (irows > 1) {
	    i__1 = irows - 1;
	    i__2 = irows - 1;
	    slacpy_("L", &i__1, &i__2, &b[*ilo + 1 + *ilo * b_dim1], ldb, &vl[
		    *ilo + 1 + *ilo * vl_dim1], ldvl);
	}
	i__1 = *lwork + 1 - iwrk;
	sorgqr_(&irows, &irows, &irows, &vl[*ilo + *ilo * vl_dim1], ldvl, &
		work[itau], &work[iwrk], &i__1, &ierr);
    }

    if (ilvr) {
	slaset_("Full", n, n, &c_b57, &c_b58, &vr[vr_offset], ldvr)
		;
    }

/*     Reduce to generalized Hessenberg form */
/*     (Workspace: none needed) */

    if (ilv || ! wantsn) {

/*        Eigenvectors requested -- work on whole matrix. */

	sgghrd_(jobvl, jobvr, n, ilo, ihi, &a[a_offset], lda, &b[b_offset], 
		ldb, &vl[vl_offset], ldvl, &vr[vr_offset], ldvr, &ierr);
    } else {
	sgghrd_("N", "N", &irows, &c__1, &irows, &a[*ilo + *ilo * a_dim1], 
		lda, &b[*ilo + *ilo * b_dim1], ldb, &vl[vl_offset], ldvl, &vr[
		vr_offset], ldvr, &ierr);
    }

/*     Perform QZ algorithm (Compute eigenvalues, and optionally, the */
/*     Schur forms and Schur vectors) */
/*     (Workspace: need N) */

    if (ilv || ! wantsn) {
	*(unsigned char *)chtemp = 'S';
    } else {
	*(unsigned char *)chtemp = 'E';
    }

    shgeqz_(chtemp, jobvl, jobvr, n, ilo, ihi, &a[a_offset], lda, &b[b_offset]
	    , ldb, &alphar[1], &alphai[1], &beta[1], &vl[vl_offset], ldvl, &
	    vr[vr_offset], ldvr, &work[1], lwork, &ierr);
    if (ierr != 0) {
	if (ierr > 0 && ierr <= *n) {
	    *info = ierr;
	} else if (ierr > *n && ierr <= *n << 1) {
	    *info = ierr - *n;
	} else {
	    *info = *n + 1;
	}
	goto L130;
    }

/*     Compute Eigenvectors and estimate condition numbers if desired */
/*     (Workspace: STGEVC: need 6*N */
/*                 STGSNA: need 2*N*(N+2)+16 if SENSE = 'V' or 'B', */
/*                         need N otherwise ) */

    if (ilv || ! wantsn) {
	if (ilv) {
	    if (ilvl) {
		if (ilvr) {
		    *(unsigned char *)chtemp = 'B';
		} else {
		    *(unsigned char *)chtemp = 'L';
		}
	    } else {
		*(unsigned char *)chtemp = 'R';
	    }

	    stgevc_(chtemp, "B", ldumma, n, &a[a_offset], lda, &b[b_offset], 
		    ldb, &vl[vl_offset], ldvl, &vr[vr_offset], ldvr, n, &in, &
		    work[1], &ierr);
	    if (ierr != 0) {
		*info = *n + 2;
		goto L130;
	    }
	}

	if (! wantsn) {

/*           compute eigenvectors (STGEVC) and estimate condition */
/*           numbers (STGSNA). Note that the definition of the condition */
/*           number is not invariant under transformation (u,v) to */
/*           (Q*u, Z*v), where (u,v) are eigenvectors of the generalized */
/*           Schur form (S,T), Q and Z are orthogonal matrices. In order */
/*           to avoid using extra 2*N*N workspace, we have to recalculate */
/*           eigenvectors and estimate one condition numbers at a time. */

	    pair = FALSE_;
	    i__1 = *n;
	    for (i__ = 1; i__ <= i__1; ++i__) {

		if (pair) {
		    pair = FALSE_;
		    goto L20;
		}
		mm = 1;
		if (i__ < *n) {
		    if (a[i__ + 1 + i__ * a_dim1] != 0.f) {
			pair = TRUE_;
			mm = 2;
		    }
		}

		i__2 = *n;
		for (j = 1; j <= i__2; ++j) {
		    bwork[j] = FALSE_;
/* L10: */
		}
		if (mm == 1) {
		    bwork[i__] = TRUE_;
		} else if (mm == 2) {
		    bwork[i__] = TRUE_;
		    bwork[i__ + 1] = TRUE_;
		}

		iwrk = mm * *n + 1;
		iwrk1 = iwrk + mm * *n;

/*              Compute a pair of left and right eigenvectors. */
/*              (compute workspace: need up to 4*N + 6*N) */

		if (wantse || wantsb) {
		    stgevc_("B", "S", &bwork[1], n, &a[a_offset], lda, &b[
			    b_offset], ldb, &work[1], n, &work[iwrk], n, &mm, 
			    &m, &work[iwrk1], &ierr);
		    if (ierr != 0) {
			*info = *n + 2;
			goto L130;
		    }
		}

		i__2 = *lwork - iwrk1 + 1;
		stgsna_(sense, "S", &bwork[1], n, &a[a_offset], lda, &b[
			b_offset], ldb, &work[1], n, &work[iwrk], n, &rconde[
			i__], &rcondv[i__], &mm, &m, &work[iwrk1], &i__2, &
			iwork[1], &ierr);

L20:
		;
	    }
	}
    }

/*     Undo balancing on VL and VR and normalization */
/*     (Workspace: none needed) */

    if (ilvl) {
	sggbak_(balanc, "L", n, ilo, ihi, &lscale[1], &rscale[1], n, &vl[
		vl_offset], ldvl, &ierr);

	i__1 = *n;
	for (jc = 1; jc <= i__1; ++jc) {
	    if (alphai[jc] < 0.f) {
		goto L70;
	    }
	    temp = 0.f;
	    if (alphai[jc] == 0.f) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
/* Computing MAX */
		    r__2 = temp, r__3 = (r__1 = vl[jr + jc * vl_dim1], abs(
			    r__1));
		    temp = f2cmax(r__2,r__3);
/* L30: */
		}
	    } else {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
/* Computing MAX */
		    r__3 = temp, r__4 = (r__1 = vl[jr + jc * vl_dim1], abs(
			    r__1)) + (r__2 = vl[jr + (jc + 1) * vl_dim1], abs(
			    r__2));
		    temp = f2cmax(r__3,r__4);
/* L40: */
		}
	    }
	    if (temp < smlnum) {
		goto L70;
	    }
	    temp = 1.f / temp;
	    if (alphai[jc] == 0.f) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    vl[jr + jc * vl_dim1] *= temp;
/* L50: */
		}
	    } else {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    vl[jr + jc * vl_dim1] *= temp;
		    vl[jr + (jc + 1) * vl_dim1] *= temp;
/* L60: */
		}
	    }
L70:
	    ;
	}
    }
    if (ilvr) {
	sggbak_(balanc, "R", n, ilo, ihi, &lscale[1], &rscale[1], n, &vr[
		vr_offset], ldvr, &ierr);
	i__1 = *n;
	for (jc = 1; jc <= i__1; ++jc) {
	    if (alphai[jc] < 0.f) {
		goto L120;
	    }
	    temp = 0.f;
	    if (alphai[jc] == 0.f) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
/* Computing MAX */
		    r__2 = temp, r__3 = (r__1 = vr[jr + jc * vr_dim1], abs(
			    r__1));
		    temp = f2cmax(r__2,r__3);
/* L80: */
		}
	    } else {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
/* Computing MAX */
		    r__3 = temp, r__4 = (r__1 = vr[jr + jc * vr_dim1], abs(
			    r__1)) + (r__2 = vr[jr + (jc + 1) * vr_dim1], abs(
			    r__2));
		    temp = f2cmax(r__3,r__4);
/* L90: */
		}
	    }
	    if (temp < smlnum) {
		goto L120;
	    }
	    temp = 1.f / temp;
	    if (alphai[jc] == 0.f) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    vr[jr + jc * vr_dim1] *= temp;
/* L100: */
		}
	    } else {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    vr[jr + jc * vr_dim1] *= temp;
		    vr[jr + (jc + 1) * vr_dim1] *= temp;
/* L110: */
		}
	    }
L120:
	    ;
	}
    }

/*     Undo scaling if necessary */

L130:

    if (ilascl) {
	slascl_("G", &c__0, &c__0, &anrmto, &anrm, n, &c__1, &alphar[1], n, &
		ierr);
	slascl_("G", &c__0, &c__0, &anrmto, &anrm, n, &c__1, &alphai[1], n, &
		ierr);
    }

    if (ilbscl) {
	slascl_("G", &c__0, &c__0, &bnrmto, &bnrm, n, &c__1, &beta[1], n, &
		ierr);
    }

    work[1] = (real) maxwrk;
    return;

/*     End of SGGEVX */

} /* sggevx_ */

