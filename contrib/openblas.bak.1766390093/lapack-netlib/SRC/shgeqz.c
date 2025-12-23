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

static real c_b12 = 0.f;
static real c_b13 = 1.f;
static integer c__1 = 1;
static integer c__3 = 3;

/* > \brief \b SHGEQZ */

/*  =========== DOCUMENTATION =========== */

/* Online html documentation available at */
/*            http://www.netlib.org/lapack/explore-html/ */

/* > \htmlonly */
/* > Download SHGEQZ + dependencies */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/shgeqz.
f"> */
/* > [TGZ]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/shgeqz.
f"> */
/* > [ZIP]</a> */
/* > <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/shgeqz.
f"> */
/* > [TXT]</a> */
/* > \endhtmlonly */

/*  Definition: */
/*  =========== */

/*       SUBROUTINE SHGEQZ( JOB, COMPQ, COMPZ, N, ILO, IHI, H, LDH, T, LDT, */
/*                          ALPHAR, ALPHAI, BETA, Q, LDQ, Z, LDZ, WORK, */
/*                          LWORK, INFO ) */

/*       CHARACTER          COMPQ, COMPZ, JOB */
/*       INTEGER            IHI, ILO, INFO, LDH, LDQ, LDT, LDZ, LWORK, N */
/*       REAL               ALPHAI( * ), ALPHAR( * ), BETA( * ), */
/*      $                   H( LDH, * ), Q( LDQ, * ), T( LDT, * ), */
/*      $                   WORK( * ), Z( LDZ, * ) */


/* > \par Purpose: */
/*  ============= */
/* > */
/* > \verbatim */
/* > */
/* > SHGEQZ computes the eigenvalues of a real matrix pair (H,T), */
/* > where H is an upper Hessenberg matrix and T is upper triangular, */
/* > using the double-shift QZ method. */
/* > Matrix pairs of this type are produced by the reduction to */
/* > generalized upper Hessenberg form of a real matrix pair (A,B): */
/* > */
/* >    A = Q1*H*Z1**T,  B = Q1*T*Z1**T, */
/* > */
/* > as computed by SGGHRD. */
/* > */
/* > If JOB='S', then the Hessenberg-triangular pair (H,T) is */
/* > also reduced to generalized Schur form, */
/* > */
/* >    H = Q*S*Z**T,  T = Q*P*Z**T, */
/* > */
/* > where Q and Z are orthogonal matrices, P is an upper triangular */
/* > matrix, and S is a quasi-triangular matrix with 1-by-1 and 2-by-2 */
/* > diagonal blocks. */
/* > */
/* > The 1-by-1 blocks correspond to real eigenvalues of the matrix pair */
/* > (H,T) and the 2-by-2 blocks correspond to complex conjugate pairs of */
/* > eigenvalues. */
/* > */
/* > Additionally, the 2-by-2 upper triangular diagonal blocks of P */
/* > corresponding to 2-by-2 blocks of S are reduced to positive diagonal */
/* > form, i.e., if S(j+1,j) is non-zero, then P(j+1,j) = P(j,j+1) = 0, */
/* > P(j,j) > 0, and P(j+1,j+1) > 0. */
/* > */
/* > Optionally, the orthogonal matrix Q from the generalized Schur */
/* > factorization may be postmultiplied into an input matrix Q1, and the */
/* > orthogonal matrix Z may be postmultiplied into an input matrix Z1. */
/* > If Q1 and Z1 are the orthogonal matrices from SGGHRD that reduced */
/* > the matrix pair (A,B) to generalized upper Hessenberg form, then the */
/* > output matrices Q1*Q and Z1*Z are the orthogonal factors from the */
/* > generalized Schur factorization of (A,B): */
/* > */
/* >    A = (Q1*Q)*S*(Z1*Z)**T,  B = (Q1*Q)*P*(Z1*Z)**T. */
/* > */
/* > To avoid overflow, eigenvalues of the matrix pair (H,T) (equivalently, */
/* > of (A,B)) are computed as a pair of values (alpha,beta), where alpha is */
/* > complex and beta real. */
/* > If beta is nonzero, lambda = alpha / beta is an eigenvalue of the */
/* > generalized nonsymmetric eigenvalue problem (GNEP) */
/* >    A*x = lambda*B*x */
/* > and if alpha is nonzero, mu = beta / alpha is an eigenvalue of the */
/* > alternate form of the GNEP */
/* >    mu*A*y = B*y. */
/* > Real eigenvalues can be read directly from the generalized Schur */
/* > form: */
/* >   alpha = S(i,i), beta = P(i,i). */
/* > */
/* > Ref: C.B. Moler & G.W. Stewart, "An Algorithm for Generalized Matrix */
/* >      Eigenvalue Problems", SIAM J. Numer. Anal., 10(1973), */
/* >      pp. 241--256. */
/* > \endverbatim */

/*  Arguments: */
/*  ========== */

/* > \param[in] JOB */
/* > \verbatim */
/* >          JOB is CHARACTER*1 */
/* >          = 'E': Compute eigenvalues only; */
/* >          = 'S': Compute eigenvalues and the Schur form. */
/* > \endverbatim */
/* > */
/* > \param[in] COMPQ */
/* > \verbatim */
/* >          COMPQ is CHARACTER*1 */
/* >          = 'N': Left Schur vectors (Q) are not computed; */
/* >          = 'I': Q is initialized to the unit matrix and the matrix Q */
/* >                 of left Schur vectors of (H,T) is returned; */
/* >          = 'V': Q must contain an orthogonal matrix Q1 on entry and */
/* >                 the product Q1*Q is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] COMPZ */
/* > \verbatim */
/* >          COMPZ is CHARACTER*1 */
/* >          = 'N': Right Schur vectors (Z) are not computed; */
/* >          = 'I': Z is initialized to the unit matrix and the matrix Z */
/* >                 of right Schur vectors of (H,T) is returned; */
/* >          = 'V': Z must contain an orthogonal matrix Z1 on entry and */
/* >                 the product Z1*Z is returned. */
/* > \endverbatim */
/* > */
/* > \param[in] N */
/* > \verbatim */
/* >          N is INTEGER */
/* >          The order of the matrices H, T, Q, and Z.  N >= 0. */
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
/* >          ILO and IHI mark the rows and columns of H which are in */
/* >          Hessenberg form.  It is assumed that A is already upper */
/* >          triangular in rows and columns 1:ILO-1 and IHI+1:N. */
/* >          If N > 0, 1 <= ILO <= IHI <= N; if N = 0, ILO=1 and IHI=0. */
/* > \endverbatim */
/* > */
/* > \param[in,out] H */
/* > \verbatim */
/* >          H is REAL array, dimension (LDH, N) */
/* >          On entry, the N-by-N upper Hessenberg matrix H. */
/* >          On exit, if JOB = 'S', H contains the upper quasi-triangular */
/* >          matrix S from the generalized Schur factorization. */
/* >          If JOB = 'E', the diagonal blocks of H match those of S, but */
/* >          the rest of H is unspecified. */
/* > \endverbatim */
/* > */
/* > \param[in] LDH */
/* > \verbatim */
/* >          LDH is INTEGER */
/* >          The leading dimension of the array H.  LDH >= f2cmax( 1, N ). */
/* > \endverbatim */
/* > */
/* > \param[in,out] T */
/* > \verbatim */
/* >          T is REAL array, dimension (LDT, N) */
/* >          On entry, the N-by-N upper triangular matrix T. */
/* >          On exit, if JOB = 'S', T contains the upper triangular */
/* >          matrix P from the generalized Schur factorization; */
/* >          2-by-2 diagonal blocks of P corresponding to 2-by-2 blocks of S */
/* >          are reduced to positive diagonal form, i.e., if H(j+1,j) is */
/* >          non-zero, then T(j+1,j) = T(j,j+1) = 0, T(j,j) > 0, and */
/* >          T(j+1,j+1) > 0. */
/* >          If JOB = 'E', the diagonal blocks of T match those of P, but */
/* >          the rest of T is unspecified. */
/* > \endverbatim */
/* > */
/* > \param[in] LDT */
/* > \verbatim */
/* >          LDT is INTEGER */
/* >          The leading dimension of the array T.  LDT >= f2cmax( 1, N ). */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHAR */
/* > \verbatim */
/* >          ALPHAR is REAL array, dimension (N) */
/* >          The real parts of each scalar alpha defining an eigenvalue */
/* >          of GNEP. */
/* > \endverbatim */
/* > */
/* > \param[out] ALPHAI */
/* > \verbatim */
/* >          ALPHAI is REAL array, dimension (N) */
/* >          The imaginary parts of each scalar alpha defining an */
/* >          eigenvalue of GNEP. */
/* >          If ALPHAI(j) is zero, then the j-th eigenvalue is real; if */
/* >          positive, then the j-th and (j+1)-st eigenvalues are a */
/* >          complex conjugate pair, with ALPHAI(j+1) = -ALPHAI(j). */
/* > \endverbatim */
/* > */
/* > \param[out] BETA */
/* > \verbatim */
/* >          BETA is REAL array, dimension (N) */
/* >          The scalars beta that define the eigenvalues of GNEP. */
/* >          Together, the quantities alpha = (ALPHAR(j),ALPHAI(j)) and */
/* >          beta = BETA(j) represent the j-th eigenvalue of the matrix */
/* >          pair (A,B), in one of the forms lambda = alpha/beta or */
/* >          mu = beta/alpha.  Since either lambda or mu may overflow, */
/* >          they should not, in general, be computed. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Q */
/* > \verbatim */
/* >          Q is REAL array, dimension (LDQ, N) */
/* >          On entry, if COMPQ = 'V', the orthogonal matrix Q1 used in */
/* >          the reduction of (A,B) to generalized Hessenberg form. */
/* >          On exit, if COMPQ = 'I', the orthogonal matrix of left Schur */
/* >          vectors of (H,T), and if COMPQ = 'V', the orthogonal matrix */
/* >          of left Schur vectors of (A,B). */
/* >          Not referenced if COMPQ = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDQ */
/* > \verbatim */
/* >          LDQ is INTEGER */
/* >          The leading dimension of the array Q.  LDQ >= 1. */
/* >          If COMPQ='V' or 'I', then LDQ >= N. */
/* > \endverbatim */
/* > */
/* > \param[in,out] Z */
/* > \verbatim */
/* >          Z is REAL array, dimension (LDZ, N) */
/* >          On entry, if COMPZ = 'V', the orthogonal matrix Z1 used in */
/* >          the reduction of (A,B) to generalized Hessenberg form. */
/* >          On exit, if COMPZ = 'I', the orthogonal matrix of */
/* >          right Schur vectors of (H,T), and if COMPZ = 'V', the */
/* >          orthogonal matrix of right Schur vectors of (A,B). */
/* >          Not referenced if COMPZ = 'N'. */
/* > \endverbatim */
/* > */
/* > \param[in] LDZ */
/* > \verbatim */
/* >          LDZ is INTEGER */
/* >          The leading dimension of the array Z.  LDZ >= 1. */
/* >          If COMPZ='V' or 'I', then LDZ >= N. */
/* > \endverbatim */
/* > */
/* > \param[out] WORK */
/* > \verbatim */
/* >          WORK is REAL array, dimension (MAX(1,LWORK)) */
/* >          On exit, if INFO >= 0, WORK(1) returns the optimal LWORK. */
/* > \endverbatim */
/* > */
/* > \param[in] LWORK */
/* > \verbatim */
/* >          LWORK is INTEGER */
/* >          The dimension of the array WORK.  LWORK >= f2cmax(1,N). */
/* > */
/* >          If LWORK = -1, then a workspace query is assumed; the routine */
/* >          only calculates the optimal size of the WORK array, returns */
/* >          this value as the first entry of the WORK array, and no error */
/* >          message related to LWORK is issued by XERBLA. */
/* > \endverbatim */
/* > */
/* > \param[out] INFO */
/* > \verbatim */
/* >          INFO is INTEGER */
/* >          = 0: successful exit */
/* >          < 0: if INFO = -i, the i-th argument had an illegal value */
/* >          = 1,...,N: the QZ iteration did not converge.  (H,T) is not */
/* >                     in Schur form, but ALPHAR(i), ALPHAI(i), and */
/* >                     BETA(i), i=INFO+1,...,N should be correct. */
/* >          = N+1,...,2*N: the shift calculation failed.  (H,T) is not */
/* >                     in Schur form, but ALPHAR(i), ALPHAI(i), and */
/* >                     BETA(i), i=INFO-N+1,...,N should be correct. */
/* > \endverbatim */

/*  Authors: */
/*  ======== */

/* > \author Univ. of Tennessee */
/* > \author Univ. of California Berkeley */
/* > \author Univ. of Colorado Denver */
/* > \author NAG Ltd. */

/* > \date June 2016 */

/* > \ingroup realGEcomputational */

/* > \par Further Details: */
/*  ===================== */
/* > */
/* > \verbatim */
/* > */
/* >  Iteration counters: */
/* > */
/* >  JITER  -- counts iterations. */
/* >  IITER  -- counts iterations run since ILAST was last */
/* >            changed.  This is therefore reset only when a 1-by-1 or */
/* >            2-by-2 block deflates off the bottom. */
/* > \endverbatim */
/* > */
/*  ===================================================================== */
/* Subroutine */ void shgeqz_(char *job, char *compq, char *compz, integer *n, 
	integer *ilo, integer *ihi, real *h__, integer *ldh, real *t, integer 
	*ldt, real *alphar, real *alphai, real *beta, real *q, integer *ldq, 
	real *z__, integer *ldz, real *work, integer *lwork, integer *info)
{
    /* System generated locals */
    integer h_dim1, h_offset, q_dim1, q_offset, t_dim1, t_offset, z_dim1, 
	    z_offset, i__1, i__2, i__3, i__4;
    real r__1, r__2, r__3, r__4;

    /* Local variables */
    real ad11l, ad12l, ad21l, ad22l, ad32l, wabs, atol, btol, temp;
    extern /* Subroutine */ void srot_(integer *, real *, integer *, real *, 
	    integer *, real *, real *), slag2_(real *, integer *, real *, 
	    integer *, real *, real *, real *, real *, real *, real *);
    real temp2, s1inv, c__;
    integer j;
    real s, v[3], scale;
    extern logical lsame_(char *, char *);
    integer iiter, ilast, jiter;
    real anorm, bnorm;
    integer maxit;
    real tempi, tempr, s1, s2, t1, u1, u2;
    logical ilazr2;
    real a11, a12, a21, a22, b11, b22, c12, c21;
    extern real slapy2_(real *, real *);
    integer jc;
    extern real slapy3_(real *, real *, real *);
    real an, bn, cl;
    extern /* Subroutine */ void slasv2_(real *, real *, real *, real *, real *
	    , real *, real *, real *, real *);
    real cq, cr;
    integer in;
    real ascale, bscale, u12, w11;
    integer jr;
    real cz, sl, w12, w21, w22, wi, sr;
    extern real slamch_(char *);
    real vs, wr, safmin;
    extern /* Subroutine */ void slarfg_(integer *, real *, real *, integer *, 
	    real *);
    real safmax;
    extern /* Subroutine */ int xerbla_(char *, integer *, ftnlen);
    real eshift;
    logical ilschr;
    real b1a, b2a;
    integer icompq, ilastm;
    extern real slanhs_(char *, integer *, real *, integer *, real *);
    real a1i;
    integer ischur;
    real a2i, b1i;
    logical ilazro;
    integer icompz, ifirst, ifrstm;
    real a1r;
    integer istart;
    logical ilpivt;
    real a2r, b1r, b2i, b2r;
    extern /* Subroutine */ void slartg_(real *, real *, real *, real *, real *
	    ), slaset_(char *, integer *, integer *, real *, real *, real *, 
	    integer *);
    logical lquery;
    real wr2, ad11, ad12, ad21, ad22, c11i, c22i;
    integer jch;
    real c11r, c22r;
    logical ilq;
    real u12l, tau, sqi;
    logical ilz;
    real ulp, sqr, szi, szr;


/*  -- LAPACK computational routine (version 3.7.0) -- */
/*  -- LAPACK is a software package provided by Univ. of Tennessee,    -- */
/*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..-- */
/*     June 2016 */


/*  ===================================================================== */

/*    $                     SAFETY = 1.0E+0 ) */

/*     Decode JOB, COMPQ, COMPZ */

    /* Parameter adjustments */
    h_dim1 = *ldh;
    h_offset = 1 + h_dim1 * 1;
    h__ -= h_offset;
    t_dim1 = *ldt;
    t_offset = 1 + t_dim1 * 1;
    t -= t_offset;
    --alphar;
    --alphai;
    --beta;
    q_dim1 = *ldq;
    q_offset = 1 + q_dim1 * 1;
    q -= q_offset;
    z_dim1 = *ldz;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --work;

    /* Function Body */
    if (lsame_(job, "E")) {
	ilschr = FALSE_;
	ischur = 1;
    } else if (lsame_(job, "S")) {
	ilschr = TRUE_;
	ischur = 2;
    } else {
	ischur = 0;
    }

    if (lsame_(compq, "N")) {
	ilq = FALSE_;
	icompq = 1;
    } else if (lsame_(compq, "V")) {
	ilq = TRUE_;
	icompq = 2;
    } else if (lsame_(compq, "I")) {
	ilq = TRUE_;
	icompq = 3;
    } else {
	icompq = 0;
    }

    if (lsame_(compz, "N")) {
	ilz = FALSE_;
	icompz = 1;
    } else if (lsame_(compz, "V")) {
	ilz = TRUE_;
	icompz = 2;
    } else if (lsame_(compz, "I")) {
	ilz = TRUE_;
	icompz = 3;
    } else {
	icompz = 0;
    }

/*     Check Argument Values */

    *info = 0;
    work[1] = (real) f2cmax(1,*n);
    lquery = *lwork == -1;
    if (ischur == 0) {
	*info = -1;
    } else if (icompq == 0) {
	*info = -2;
    } else if (icompz == 0) {
	*info = -3;
    } else if (*n < 0) {
	*info = -4;
    } else if (*ilo < 1) {
	*info = -5;
    } else if (*ihi > *n || *ihi < *ilo - 1) {
	*info = -6;
    } else if (*ldh < *n) {
	*info = -8;
    } else if (*ldt < *n) {
	*info = -10;
    } else if (*ldq < 1 || ilq && *ldq < *n) {
	*info = -15;
    } else if (*ldz < 1 || ilz && *ldz < *n) {
	*info = -17;
    } else if (*lwork < f2cmax(1,*n) && ! lquery) {
	*info = -19;
    }
    if (*info != 0) {
	i__1 = -(*info);
	xerbla_("SHGEQZ", &i__1, (ftnlen)6);
	return;
    } else if (lquery) {
	return;
    }

/*     Quick return if possible */

    if (*n <= 0) {
	work[1] = 1.f;
	return;
    }

/*     Initialize Q and Z */

    if (icompq == 3) {
	slaset_("Full", n, n, &c_b12, &c_b13, &q[q_offset], ldq);
    }
    if (icompz == 3) {
	slaset_("Full", n, n, &c_b12, &c_b13, &z__[z_offset], ldz);
    }

/*     Machine Constants */

    in = *ihi + 1 - *ilo;
    safmin = slamch_("S");
    safmax = 1.f / safmin;
    ulp = slamch_("E") * slamch_("B");
    anorm = slanhs_("F", &in, &h__[*ilo + *ilo * h_dim1], ldh, &work[1]);
    bnorm = slanhs_("F", &in, &t[*ilo + *ilo * t_dim1], ldt, &work[1]);
/* Computing MAX */
    r__1 = safmin, r__2 = ulp * anorm;
    atol = f2cmax(r__1,r__2);
/* Computing MAX */
    r__1 = safmin, r__2 = ulp * bnorm;
    btol = f2cmax(r__1,r__2);
    ascale = 1.f / f2cmax(safmin,anorm);
    bscale = 1.f / f2cmax(safmin,bnorm);

/*     Set Eigenvalues IHI+1:N */

    i__1 = *n;
    for (j = *ihi + 1; j <= i__1; ++j) {
	if (t[j + j * t_dim1] < 0.f) {
	    if (ilschr) {
		i__2 = j;
		for (jr = 1; jr <= i__2; ++jr) {
		    h__[jr + j * h_dim1] = -h__[jr + j * h_dim1];
		    t[jr + j * t_dim1] = -t[jr + j * t_dim1];
/* L10: */
		}
	    } else {
		h__[j + j * h_dim1] = -h__[j + j * h_dim1];
		t[j + j * t_dim1] = -t[j + j * t_dim1];
	    }
	    if (ilz) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    z__[jr + j * z_dim1] = -z__[jr + j * z_dim1];
/* L20: */
		}
	    }
	}
	alphar[j] = h__[j + j * h_dim1];
	alphai[j] = 0.f;
	beta[j] = t[j + j * t_dim1];
/* L30: */
    }

/*     If IHI < ILO, skip QZ steps */

    if (*ihi < *ilo) {
	goto L380;
    }

/*     MAIN QZ ITERATION LOOP */

/*     Initialize dynamic indices */

/*     Eigenvalues ILAST+1:N have been found. */
/*        Column operations modify rows IFRSTM:whatever. */
/*        Row operations modify columns whatever:ILASTM. */

/*     If only eigenvalues are being computed, then */
/*        IFRSTM is the row of the last splitting row above row ILAST; */
/*        this is always at least ILO. */
/*     IITER counts iterations since the last eigenvalue was found, */
/*        to tell when to use an extraordinary shift. */
/*     MAXIT is the maximum number of QZ sweeps allowed. */

    ilast = *ihi;
    if (ilschr) {
	ifrstm = 1;
	ilastm = *n;
    } else {
	ifrstm = *ilo;
	ilastm = *ihi;
    }
    iiter = 0;
    eshift = 0.f;
    maxit = (*ihi - *ilo + 1) * 30;

    i__1 = maxit;
    for (jiter = 1; jiter <= i__1; ++jiter) {

/*        Split the matrix if possible. */

/*        Two tests: */
/*           1: H(j,j-1)=0  or  j=ILO */
/*           2: T(j,j)=0 */

	if (ilast == *ilo) {

/*           Special case: j=ILAST */

	    goto L80;
	} else {
	    if ((r__1 = h__[ilast + (ilast - 1) * h_dim1], abs(r__1)) <= atol)
		     {
		h__[ilast + (ilast - 1) * h_dim1] = 0.f;
		goto L80;
	    }
	}

	if ((r__1 = t[ilast + ilast * t_dim1], abs(r__1)) <= btol) {
	    t[ilast + ilast * t_dim1] = 0.f;
	    goto L70;
	}

/*        General case: j<ILAST */

	i__2 = *ilo;
	for (j = ilast - 1; j >= i__2; --j) {

/*           Test 1: for H(j,j-1)=0 or j=ILO */

	    if (j == *ilo) {
		ilazro = TRUE_;
	    } else {
		if ((r__1 = h__[j + (j - 1) * h_dim1], abs(r__1)) <= atol) {
		    h__[j + (j - 1) * h_dim1] = 0.f;
		    ilazro = TRUE_;
		} else {
		    ilazro = FALSE_;
		}
	    }

/*           Test 2: for T(j,j)=0 */

	    if ((r__1 = t[j + j * t_dim1], abs(r__1)) < btol) {
		t[j + j * t_dim1] = 0.f;

/*              Test 1a: Check for 2 consecutive small subdiagonals in A */

		ilazr2 = FALSE_;
		if (! ilazro) {
		    temp = (r__1 = h__[j + (j - 1) * h_dim1], abs(r__1));
		    temp2 = (r__1 = h__[j + j * h_dim1], abs(r__1));
		    tempr = f2cmax(temp,temp2);
		    if (tempr < 1.f && tempr != 0.f) {
			temp /= tempr;
			temp2 /= tempr;
		    }
		    if (temp * (ascale * (r__1 = h__[j + 1 + j * h_dim1], abs(
			    r__1))) <= temp2 * (ascale * atol)) {
			ilazr2 = TRUE_;
		    }
		}

/*              If both tests pass (1 & 2), i.e., the leading diagonal */
/*              element of B in the block is zero, split a 1x1 block off */
/*              at the top. (I.e., at the J-th row/column) The leading */
/*              diagonal element of the remainder can also be zero, so */
/*              this may have to be done repeatedly. */

		if (ilazro || ilazr2) {
		    i__3 = ilast - 1;
		    for (jch = j; jch <= i__3; ++jch) {
			temp = h__[jch + jch * h_dim1];
			slartg_(&temp, &h__[jch + 1 + jch * h_dim1], &c__, &s,
				 &h__[jch + jch * h_dim1]);
			h__[jch + 1 + jch * h_dim1] = 0.f;
			i__4 = ilastm - jch;
			srot_(&i__4, &h__[jch + (jch + 1) * h_dim1], ldh, &
				h__[jch + 1 + (jch + 1) * h_dim1], ldh, &c__, 
				&s);
			i__4 = ilastm - jch;
			srot_(&i__4, &t[jch + (jch + 1) * t_dim1], ldt, &t[
				jch + 1 + (jch + 1) * t_dim1], ldt, &c__, &s);
			if (ilq) {
			    srot_(n, &q[jch * q_dim1 + 1], &c__1, &q[(jch + 1)
				     * q_dim1 + 1], &c__1, &c__, &s);
			}
			if (ilazr2) {
			    h__[jch + (jch - 1) * h_dim1] *= c__;
			}
			ilazr2 = FALSE_;
			if ((r__1 = t[jch + 1 + (jch + 1) * t_dim1], abs(r__1)
				) >= btol) {
			    if (jch + 1 >= ilast) {
				goto L80;
			    } else {
				ifirst = jch + 1;
				goto L110;
			    }
			}
			t[jch + 1 + (jch + 1) * t_dim1] = 0.f;
/* L40: */
		    }
		    goto L70;
		} else {

/*                 Only test 2 passed -- chase the zero to T(ILAST,ILAST) */
/*                 Then process as in the case T(ILAST,ILAST)=0 */

		    i__3 = ilast - 1;
		    for (jch = j; jch <= i__3; ++jch) {
			temp = t[jch + (jch + 1) * t_dim1];
			slartg_(&temp, &t[jch + 1 + (jch + 1) * t_dim1], &c__,
				 &s, &t[jch + (jch + 1) * t_dim1]);
			t[jch + 1 + (jch + 1) * t_dim1] = 0.f;
			if (jch < ilastm - 1) {
			    i__4 = ilastm - jch - 1;
			    srot_(&i__4, &t[jch + (jch + 2) * t_dim1], ldt, &
				    t[jch + 1 + (jch + 2) * t_dim1], ldt, &
				    c__, &s);
			}
			i__4 = ilastm - jch + 2;
			srot_(&i__4, &h__[jch + (jch - 1) * h_dim1], ldh, &
				h__[jch + 1 + (jch - 1) * h_dim1], ldh, &c__, 
				&s);
			if (ilq) {
			    srot_(n, &q[jch * q_dim1 + 1], &c__1, &q[(jch + 1)
				     * q_dim1 + 1], &c__1, &c__, &s);
			}
			temp = h__[jch + 1 + jch * h_dim1];
			slartg_(&temp, &h__[jch + 1 + (jch - 1) * h_dim1], &
				c__, &s, &h__[jch + 1 + jch * h_dim1]);
			h__[jch + 1 + (jch - 1) * h_dim1] = 0.f;
			i__4 = jch + 1 - ifrstm;
			srot_(&i__4, &h__[ifrstm + jch * h_dim1], &c__1, &h__[
				ifrstm + (jch - 1) * h_dim1], &c__1, &c__, &s)
				;
			i__4 = jch - ifrstm;
			srot_(&i__4, &t[ifrstm + jch * t_dim1], &c__1, &t[
				ifrstm + (jch - 1) * t_dim1], &c__1, &c__, &s)
				;
			if (ilz) {
			    srot_(n, &z__[jch * z_dim1 + 1], &c__1, &z__[(jch 
				    - 1) * z_dim1 + 1], &c__1, &c__, &s);
			}
/* L50: */
		    }
		    goto L70;
		}
	    } else if (ilazro) {

/*              Only test 1 passed -- work on J:ILAST */

		ifirst = j;
		goto L110;
	    }

/*           Neither test passed -- try next J */

/* L60: */
	}

/*        (Drop-through is "impossible") */

	*info = *n + 1;
	goto L420;

/*        T(ILAST,ILAST)=0 -- clear H(ILAST,ILAST-1) to split off a */
/*        1x1 block. */

L70:
	temp = h__[ilast + ilast * h_dim1];
	slartg_(&temp, &h__[ilast + (ilast - 1) * h_dim1], &c__, &s, &h__[
		ilast + ilast * h_dim1]);
	h__[ilast + (ilast - 1) * h_dim1] = 0.f;
	i__2 = ilast - ifrstm;
	srot_(&i__2, &h__[ifrstm + ilast * h_dim1], &c__1, &h__[ifrstm + (
		ilast - 1) * h_dim1], &c__1, &c__, &s);
	i__2 = ilast - ifrstm;
	srot_(&i__2, &t[ifrstm + ilast * t_dim1], &c__1, &t[ifrstm + (ilast - 
		1) * t_dim1], &c__1, &c__, &s);
	if (ilz) {
	    srot_(n, &z__[ilast * z_dim1 + 1], &c__1, &z__[(ilast - 1) * 
		    z_dim1 + 1], &c__1, &c__, &s);
	}

/*        H(ILAST,ILAST-1)=0 -- Standardize B, set ALPHAR, ALPHAI, */
/*                              and BETA */

L80:
	if (t[ilast + ilast * t_dim1] < 0.f) {
	    if (ilschr) {
		i__2 = ilast;
		for (j = ifrstm; j <= i__2; ++j) {
		    h__[j + ilast * h_dim1] = -h__[j + ilast * h_dim1];
		    t[j + ilast * t_dim1] = -t[j + ilast * t_dim1];
/* L90: */
		}
	    } else {
		h__[ilast + ilast * h_dim1] = -h__[ilast + ilast * h_dim1];
		t[ilast + ilast * t_dim1] = -t[ilast + ilast * t_dim1];
	    }
	    if (ilz) {
		i__2 = *n;
		for (j = 1; j <= i__2; ++j) {
		    z__[j + ilast * z_dim1] = -z__[j + ilast * z_dim1];
/* L100: */
		}
	    }
	}
	alphar[ilast] = h__[ilast + ilast * h_dim1];
	alphai[ilast] = 0.f;
	beta[ilast] = t[ilast + ilast * t_dim1];

/*        Go to next block -- exit if finished. */

	--ilast;
	if (ilast < *ilo) {
	    goto L380;
	}

/*        Reset counters */

	iiter = 0;
	eshift = 0.f;
	if (! ilschr) {
	    ilastm = ilast;
	    if (ifrstm > ilast) {
		ifrstm = *ilo;
	    }
	}
	goto L350;

/*        QZ step */

/*        This iteration only involves rows/columns IFIRST:ILAST. We */
/*        assume IFIRST < ILAST, and that the diagonal of B is non-zero. */

L110:
	++iiter;
	if (! ilschr) {
	    ifrstm = ifirst;
	}

/*        Compute single shifts. */

/*        At this point, IFIRST < ILAST, and the diagonal elements of */
/*        T(IFIRST:ILAST,IFIRST,ILAST) are larger than BTOL (in */
/*        magnitude) */

	if (iiter / 10 * 10 == iiter) {

/*           Exceptional shift.  Chosen for no particularly good reason. */
/*           (Single shift only.) */

	    if ((real) maxit * safmin * (r__1 = h__[ilast + (ilast - 1) * 
		    h_dim1], abs(r__1)) < (r__2 = t[ilast - 1 + (ilast - 1) * 
		    t_dim1], abs(r__2))) {
		eshift = h__[ilast + (ilast - 1) * h_dim1] / t[ilast - 1 + (
			ilast - 1) * t_dim1];
	    } else {
		eshift += 1.f / (safmin * (real) maxit);
	    }
	    s1 = 1.f;
	    wr = eshift;

	} else {

/*           Shifts based on the generalized eigenvalues of the */
/*           bottom-right 2x2 block of A and B. The first eigenvalue */
/*           returned by SLAG2 is the Wilkinson shift (AEP p.512), */

	    r__1 = safmin * 100.f;
	    slag2_(&h__[ilast - 1 + (ilast - 1) * h_dim1], ldh, &t[ilast - 1 
		    + (ilast - 1) * t_dim1], ldt, &r__1, &s1, &s2, &wr, &wr2, 
		    &wi);

	    if ((r__1 = wr / s1 * t[ilast + ilast * t_dim1] - h__[ilast + 
		    ilast * h_dim1], abs(r__1)) > (r__2 = wr2 / s2 * t[ilast 
		    + ilast * t_dim1] - h__[ilast + ilast * h_dim1], abs(r__2)
		    )) {
		temp = wr;
		wr = wr2;
		wr2 = temp;
		temp = s1;
		s1 = s2;
		s2 = temp;
	    }
/* Computing MAX */
/* Computing MAX */
	    r__3 = 1.f, r__4 = abs(wr), r__3 = f2cmax(r__3,r__4), r__4 = abs(wi);
	    r__1 = s1, r__2 = safmin * f2cmax(r__3,r__4);
	    temp = f2cmax(r__1,r__2);
	    if (wi != 0.f) {
		goto L200;
	    }
	}

/*        Fiddle with shift to avoid overflow */

	temp = f2cmin(ascale,1.f) * (safmax * .5f);
	if (s1 > temp) {
	    scale = temp / s1;
	} else {
	    scale = 1.f;
	}

	temp = f2cmin(bscale,1.f) * (safmax * .5f);
	if (abs(wr) > temp) {
/* Computing MIN */
	    r__1 = scale, r__2 = temp / abs(wr);
	    scale = f2cmin(r__1,r__2);
	}
	s1 = scale * s1;
	wr = scale * wr;

/*        Now check for two consecutive small subdiagonals. */

	i__2 = ifirst + 1;
	for (j = ilast - 1; j >= i__2; --j) {
	    istart = j;
	    temp = (r__1 = s1 * h__[j + (j - 1) * h_dim1], abs(r__1));
	    temp2 = (r__1 = s1 * h__[j + j * h_dim1] - wr * t[j + j * t_dim1],
		     abs(r__1));
	    tempr = f2cmax(temp,temp2);
	    if (tempr < 1.f && tempr != 0.f) {
		temp /= tempr;
		temp2 /= tempr;
	    }
	    if ((r__1 = ascale * h__[j + 1 + j * h_dim1] * temp, abs(r__1)) <=
		     ascale * atol * temp2) {
		goto L130;
	    }
/* L120: */
	}

	istart = ifirst;
L130:

/*        Do an implicit single-shift QZ sweep. */

/*        Initial Q */

	temp = s1 * h__[istart + istart * h_dim1] - wr * t[istart + istart * 
		t_dim1];
	temp2 = s1 * h__[istart + 1 + istart * h_dim1];
	slartg_(&temp, &temp2, &c__, &s, &tempr);

/*        Sweep */

	i__2 = ilast - 1;
	for (j = istart; j <= i__2; ++j) {
	    if (j > istart) {
		temp = h__[j + (j - 1) * h_dim1];
		slartg_(&temp, &h__[j + 1 + (j - 1) * h_dim1], &c__, &s, &h__[
			j + (j - 1) * h_dim1]);
		h__[j + 1 + (j - 1) * h_dim1] = 0.f;
	    }

	    i__3 = ilastm;
	    for (jc = j; jc <= i__3; ++jc) {
		temp = c__ * h__[j + jc * h_dim1] + s * h__[j + 1 + jc * 
			h_dim1];
		h__[j + 1 + jc * h_dim1] = -s * h__[j + jc * h_dim1] + c__ * 
			h__[j + 1 + jc * h_dim1];
		h__[j + jc * h_dim1] = temp;
		temp2 = c__ * t[j + jc * t_dim1] + s * t[j + 1 + jc * t_dim1];
		t[j + 1 + jc * t_dim1] = -s * t[j + jc * t_dim1] + c__ * t[j 
			+ 1 + jc * t_dim1];
		t[j + jc * t_dim1] = temp2;
/* L140: */
	    }
	    if (ilq) {
		i__3 = *n;
		for (jr = 1; jr <= i__3; ++jr) {
		    temp = c__ * q[jr + j * q_dim1] + s * q[jr + (j + 1) * 
			    q_dim1];
		    q[jr + (j + 1) * q_dim1] = -s * q[jr + j * q_dim1] + c__ *
			     q[jr + (j + 1) * q_dim1];
		    q[jr + j * q_dim1] = temp;
/* L150: */
		}
	    }

	    temp = t[j + 1 + (j + 1) * t_dim1];
	    slartg_(&temp, &t[j + 1 + j * t_dim1], &c__, &s, &t[j + 1 + (j + 
		    1) * t_dim1]);
	    t[j + 1 + j * t_dim1] = 0.f;

/* Computing MIN */
	    i__4 = j + 2;
	    i__3 = f2cmin(i__4,ilast);
	    for (jr = ifrstm; jr <= i__3; ++jr) {
		temp = c__ * h__[jr + (j + 1) * h_dim1] + s * h__[jr + j * 
			h_dim1];
		h__[jr + j * h_dim1] = -s * h__[jr + (j + 1) * h_dim1] + c__ *
			 h__[jr + j * h_dim1];
		h__[jr + (j + 1) * h_dim1] = temp;
/* L160: */
	    }
	    i__3 = j;
	    for (jr = ifrstm; jr <= i__3; ++jr) {
		temp = c__ * t[jr + (j + 1) * t_dim1] + s * t[jr + j * t_dim1]
			;
		t[jr + j * t_dim1] = -s * t[jr + (j + 1) * t_dim1] + c__ * t[
			jr + j * t_dim1];
		t[jr + (j + 1) * t_dim1] = temp;
/* L170: */
	    }
	    if (ilz) {
		i__3 = *n;
		for (jr = 1; jr <= i__3; ++jr) {
		    temp = c__ * z__[jr + (j + 1) * z_dim1] + s * z__[jr + j *
			     z_dim1];
		    z__[jr + j * z_dim1] = -s * z__[jr + (j + 1) * z_dim1] + 
			    c__ * z__[jr + j * z_dim1];
		    z__[jr + (j + 1) * z_dim1] = temp;
/* L180: */
		}
	    }
/* L190: */
	}

	goto L350;

/*        Use Francis double-shift */

/*        Note: the Francis double-shift should work with real shifts, */
/*              but only if the block is at least 3x3. */
/*              This code may break if this point is reached with */
/*              a 2x2 block with real eigenvalues. */

L200:
	if (ifirst + 1 == ilast) {

/*           Special case -- 2x2 block with complex eigenvectors */

/*           Step 1: Standardize, that is, rotate so that */

/*                       ( B11  0  ) */
/*                   B = (         )  with B11 non-negative. */
/*                       (  0  B22 ) */

	    slasv2_(&t[ilast - 1 + (ilast - 1) * t_dim1], &t[ilast - 1 + 
		    ilast * t_dim1], &t[ilast + ilast * t_dim1], &b22, &b11, &
		    sr, &cr, &sl, &cl);

	    if (b11 < 0.f) {
		cr = -cr;
		sr = -sr;
		b11 = -b11;
		b22 = -b22;
	    }

	    i__2 = ilastm + 1 - ifirst;
	    srot_(&i__2, &h__[ilast - 1 + (ilast - 1) * h_dim1], ldh, &h__[
		    ilast + (ilast - 1) * h_dim1], ldh, &cl, &sl);
	    i__2 = ilast + 1 - ifrstm;
	    srot_(&i__2, &h__[ifrstm + (ilast - 1) * h_dim1], &c__1, &h__[
		    ifrstm + ilast * h_dim1], &c__1, &cr, &sr);

	    if (ilast < ilastm) {
		i__2 = ilastm - ilast;
		srot_(&i__2, &t[ilast - 1 + (ilast + 1) * t_dim1], ldt, &t[
			ilast + (ilast + 1) * t_dim1], ldt, &cl, &sl);
	    }
	    if (ifrstm < ilast - 1) {
		i__2 = ifirst - ifrstm;
		srot_(&i__2, &t[ifrstm + (ilast - 1) * t_dim1], &c__1, &t[
			ifrstm + ilast * t_dim1], &c__1, &cr, &sr);
	    }

	    if (ilq) {
		srot_(n, &q[(ilast - 1) * q_dim1 + 1], &c__1, &q[ilast * 
			q_dim1 + 1], &c__1, &cl, &sl);
	    }
	    if (ilz) {
		srot_(n, &z__[(ilast - 1) * z_dim1 + 1], &c__1, &z__[ilast * 
			z_dim1 + 1], &c__1, &cr, &sr);
	    }

	    t[ilast - 1 + (ilast - 1) * t_dim1] = b11;
	    t[ilast - 1 + ilast * t_dim1] = 0.f;
	    t[ilast + (ilast - 1) * t_dim1] = 0.f;
	    t[ilast + ilast * t_dim1] = b22;

/*           If B22 is negative, negate column ILAST */

	    if (b22 < 0.f) {
		i__2 = ilast;
		for (j = ifrstm; j <= i__2; ++j) {
		    h__[j + ilast * h_dim1] = -h__[j + ilast * h_dim1];
		    t[j + ilast * t_dim1] = -t[j + ilast * t_dim1];
/* L210: */
		}

		if (ilz) {
		    i__2 = *n;
		    for (j = 1; j <= i__2; ++j) {
			z__[j + ilast * z_dim1] = -z__[j + ilast * z_dim1];
/* L220: */
		    }
		}
		b22 = -b22;
	    }

/*           Step 2: Compute ALPHAR, ALPHAI, and BETA (see refs.) */

/*           Recompute shift */

	    r__1 = safmin * 100.f;
	    slag2_(&h__[ilast - 1 + (ilast - 1) * h_dim1], ldh, &t[ilast - 1 
		    + (ilast - 1) * t_dim1], ldt, &r__1, &s1, &temp, &wr, &
		    temp2, &wi);

/*           If standardization has perturbed the shift onto real line, */
/*           do another (real single-shift) QR step. */

	    if (wi == 0.f) {
		goto L350;
	    }
	    s1inv = 1.f / s1;

/*           Do EISPACK (QZVAL) computation of alpha and beta */

	    a11 = h__[ilast - 1 + (ilast - 1) * h_dim1];
	    a21 = h__[ilast + (ilast - 1) * h_dim1];
	    a12 = h__[ilast - 1 + ilast * h_dim1];
	    a22 = h__[ilast + ilast * h_dim1];

/*           Compute complex Givens rotation on right */
/*           (Assume some element of C = (sA - wB) > unfl ) */
/*                            __ */
/*           (sA - wB) ( CZ   -SZ ) */
/*                     ( SZ    CZ ) */

	    c11r = s1 * a11 - wr * b11;
	    c11i = -wi * b11;
	    c12 = s1 * a12;
	    c21 = s1 * a21;
	    c22r = s1 * a22 - wr * b22;
	    c22i = -wi * b22;

	    if (abs(c11r) + abs(c11i) + abs(c12) > abs(c21) + abs(c22r) + abs(
		    c22i)) {
		t1 = slapy3_(&c12, &c11r, &c11i);
		cz = c12 / t1;
		szr = -c11r / t1;
		szi = -c11i / t1;
	    } else {
		cz = slapy2_(&c22r, &c22i);
		if (cz <= safmin) {
		    cz = 0.f;
		    szr = 1.f;
		    szi = 0.f;
		} else {
		    tempr = c22r / cz;
		    tempi = c22i / cz;
		    t1 = slapy2_(&cz, &c21);
		    cz /= t1;
		    szr = -c21 * tempr / t1;
		    szi = c21 * tempi / t1;
		}
	    }

/*           Compute Givens rotation on left */

/*           (  CQ   SQ ) */
/*           (  __      )  A or B */
/*           ( -SQ   CQ ) */

	    an = abs(a11) + abs(a12) + abs(a21) + abs(a22);
	    bn = abs(b11) + abs(b22);
	    wabs = abs(wr) + abs(wi);
	    if (s1 * an > wabs * bn) {
		cq = cz * b11;
		sqr = szr * b22;
		sqi = -szi * b22;
	    } else {
		a1r = cz * a11 + szr * a12;
		a1i = szi * a12;
		a2r = cz * a21 + szr * a22;
		a2i = szi * a22;
		cq = slapy2_(&a1r, &a1i);
		if (cq <= safmin) {
		    cq = 0.f;
		    sqr = 1.f;
		    sqi = 0.f;
		} else {
		    tempr = a1r / cq;
		    tempi = a1i / cq;
		    sqr = tempr * a2r + tempi * a2i;
		    sqi = tempi * a2r - tempr * a2i;
		}
	    }
	    t1 = slapy3_(&cq, &sqr, &sqi);
	    cq /= t1;
	    sqr /= t1;
	    sqi /= t1;

/*           Compute diagonal elements of QBZ */

	    tempr = sqr * szr - sqi * szi;
	    tempi = sqr * szi + sqi * szr;
	    b1r = cq * cz * b11 + tempr * b22;
	    b1i = tempi * b22;
	    b1a = slapy2_(&b1r, &b1i);
	    b2r = cq * cz * b22 + tempr * b11;
	    b2i = -tempi * b11;
	    b2a = slapy2_(&b2r, &b2i);

/*           Normalize so beta > 0, and Im( alpha1 ) > 0 */

	    beta[ilast - 1] = b1a;
	    beta[ilast] = b2a;
	    alphar[ilast - 1] = wr * b1a * s1inv;
	    alphai[ilast - 1] = wi * b1a * s1inv;
	    alphar[ilast] = wr * b2a * s1inv;
	    alphai[ilast] = -(wi * b2a) * s1inv;

/*           Step 3: Go to next block -- exit if finished. */

	    ilast = ifirst - 1;
	    if (ilast < *ilo) {
		goto L380;
	    }

/*           Reset counters */

	    iiter = 0;
	    eshift = 0.f;
	    if (! ilschr) {
		ilastm = ilast;
		if (ifrstm > ilast) {
		    ifrstm = *ilo;
		}
	    }
	    goto L350;
	} else {

/*           Usual case: 3x3 or larger block, using Francis implicit */
/*                       double-shift */

/*                                    2 */
/*           Eigenvalue equation is  w  - c w + d = 0, */

/*                                         -1 2        -1 */
/*           so compute 1st column of  (A B  )  - c A B   + d */
/*           using the formula in QZIT (from EISPACK) */

/*           We assume that the block is at least 3x3 */

	    ad11 = ascale * h__[ilast - 1 + (ilast - 1) * h_dim1] / (bscale * 
		    t[ilast - 1 + (ilast - 1) * t_dim1]);
	    ad21 = ascale * h__[ilast + (ilast - 1) * h_dim1] / (bscale * t[
		    ilast - 1 + (ilast - 1) * t_dim1]);
	    ad12 = ascale * h__[ilast - 1 + ilast * h_dim1] / (bscale * t[
		    ilast + ilast * t_dim1]);
	    ad22 = ascale * h__[ilast + ilast * h_dim1] / (bscale * t[ilast + 
		    ilast * t_dim1]);
	    u12 = t[ilast - 1 + ilast * t_dim1] / t[ilast + ilast * t_dim1];
	    ad11l = ascale * h__[ifirst + ifirst * h_dim1] / (bscale * t[
		    ifirst + ifirst * t_dim1]);
	    ad21l = ascale * h__[ifirst + 1 + ifirst * h_dim1] / (bscale * t[
		    ifirst + ifirst * t_dim1]);
	    ad12l = ascale * h__[ifirst + (ifirst + 1) * h_dim1] / (bscale * 
		    t[ifirst + 1 + (ifirst + 1) * t_dim1]);
	    ad22l = ascale * h__[ifirst + 1 + (ifirst + 1) * h_dim1] / (
		    bscale * t[ifirst + 1 + (ifirst + 1) * t_dim1]);
	    ad32l = ascale * h__[ifirst + 2 + (ifirst + 1) * h_dim1] / (
		    bscale * t[ifirst + 1 + (ifirst + 1) * t_dim1]);
	    u12l = t[ifirst + (ifirst + 1) * t_dim1] / t[ifirst + 1 + (ifirst 
		    + 1) * t_dim1];

	    v[0] = (ad11 - ad11l) * (ad22 - ad11l) - ad12 * ad21 + ad21 * u12 
		    * ad11l + (ad12l - ad11l * u12l) * ad21l;
	    v[1] = (ad22l - ad11l - ad21l * u12l - (ad11 - ad11l) - (ad22 - 
		    ad11l) + ad21 * u12) * ad21l;
	    v[2] = ad32l * ad21l;

	    istart = ifirst;

	    slarfg_(&c__3, v, &v[1], &c__1, &tau);
	    v[0] = 1.f;

/*           Sweep */

	    i__2 = ilast - 2;
	    for (j = istart; j <= i__2; ++j) {

/*              All but last elements: use 3x3 Householder transforms. */

/*              Zero (j-1)st column of A */

		if (j > istart) {
		    v[0] = h__[j + (j - 1) * h_dim1];
		    v[1] = h__[j + 1 + (j - 1) * h_dim1];
		    v[2] = h__[j + 2 + (j - 1) * h_dim1];

		    slarfg_(&c__3, &h__[j + (j - 1) * h_dim1], &v[1], &c__1, &
			    tau);
		    v[0] = 1.f;
		    h__[j + 1 + (j - 1) * h_dim1] = 0.f;
		    h__[j + 2 + (j - 1) * h_dim1] = 0.f;
		}

		i__3 = ilastm;
		for (jc = j; jc <= i__3; ++jc) {
		    temp = tau * (h__[j + jc * h_dim1] + v[1] * h__[j + 1 + 
			    jc * h_dim1] + v[2] * h__[j + 2 + jc * h_dim1]);
		    h__[j + jc * h_dim1] -= temp;
		    h__[j + 1 + jc * h_dim1] -= temp * v[1];
		    h__[j + 2 + jc * h_dim1] -= temp * v[2];
		    temp2 = tau * (t[j + jc * t_dim1] + v[1] * t[j + 1 + jc * 
			    t_dim1] + v[2] * t[j + 2 + jc * t_dim1]);
		    t[j + jc * t_dim1] -= temp2;
		    t[j + 1 + jc * t_dim1] -= temp2 * v[1];
		    t[j + 2 + jc * t_dim1] -= temp2 * v[2];
/* L230: */
		}
		if (ilq) {
		    i__3 = *n;
		    for (jr = 1; jr <= i__3; ++jr) {
			temp = tau * (q[jr + j * q_dim1] + v[1] * q[jr + (j + 
				1) * q_dim1] + v[2] * q[jr + (j + 2) * q_dim1]
				);
			q[jr + j * q_dim1] -= temp;
			q[jr + (j + 1) * q_dim1] -= temp * v[1];
			q[jr + (j + 2) * q_dim1] -= temp * v[2];
/* L240: */
		    }
		}

/*              Zero j-th column of B (see SLAGBC for details) */

/*              Swap rows to pivot */

		ilpivt = FALSE_;
/* Computing MAX */
		r__3 = (r__1 = t[j + 1 + (j + 1) * t_dim1], abs(r__1)), r__4 =
			 (r__2 = t[j + 1 + (j + 2) * t_dim1], abs(r__2));
		temp = f2cmax(r__3,r__4);
/* Computing MAX */
		r__3 = (r__1 = t[j + 2 + (j + 1) * t_dim1], abs(r__1)), r__4 =
			 (r__2 = t[j + 2 + (j + 2) * t_dim1], abs(r__2));
		temp2 = f2cmax(r__3,r__4);
		if (f2cmax(temp,temp2) < safmin) {
		    scale = 0.f;
		    u1 = 1.f;
		    u2 = 0.f;
		    goto L250;
		} else if (temp >= temp2) {
		    w11 = t[j + 1 + (j + 1) * t_dim1];
		    w21 = t[j + 2 + (j + 1) * t_dim1];
		    w12 = t[j + 1 + (j + 2) * t_dim1];
		    w22 = t[j + 2 + (j + 2) * t_dim1];
		    u1 = t[j + 1 + j * t_dim1];
		    u2 = t[j + 2 + j * t_dim1];
		} else {
		    w21 = t[j + 1 + (j + 1) * t_dim1];
		    w11 = t[j + 2 + (j + 1) * t_dim1];
		    w22 = t[j + 1 + (j + 2) * t_dim1];
		    w12 = t[j + 2 + (j + 2) * t_dim1];
		    u2 = t[j + 1 + j * t_dim1];
		    u1 = t[j + 2 + j * t_dim1];
		}

/*              Swap columns if nec. */

		if (abs(w12) > abs(w11)) {
		    ilpivt = TRUE_;
		    temp = w12;
		    temp2 = w22;
		    w12 = w11;
		    w22 = w21;
		    w11 = temp;
		    w21 = temp2;
		}

/*              LU-factor */

		temp = w21 / w11;
		u2 -= temp * u1;
		w22 -= temp * w12;
		w21 = 0.f;

/*              Compute SCALE */

		scale = 1.f;
		if (abs(w22) < safmin) {
		    scale = 0.f;
		    u2 = 1.f;
		    u1 = -w12 / w11;
		    goto L250;
		}
		if (abs(w22) < abs(u2)) {
		    scale = (r__1 = w22 / u2, abs(r__1));
		}
		if (abs(w11) < abs(u1)) {
/* Computing MIN */
		    r__2 = scale, r__3 = (r__1 = w11 / u1, abs(r__1));
		    scale = f2cmin(r__2,r__3);
		}

/*              Solve */

		u2 = scale * u2 / w22;
		u1 = (scale * u1 - w12 * u2) / w11;

L250:
		if (ilpivt) {
		    temp = u2;
		    u2 = u1;
		    u1 = temp;
		}

/*              Compute Householder Vector */

/* Computing 2nd power */
		r__1 = scale;
/* Computing 2nd power */
		r__2 = u1;
/* Computing 2nd power */
		r__3 = u2;
		t1 = sqrt(r__1 * r__1 + r__2 * r__2 + r__3 * r__3);
		tau = scale / t1 + 1.f;
		vs = -1.f / (scale + t1);
		v[0] = 1.f;
		v[1] = vs * u1;
		v[2] = vs * u2;

/*              Apply transformations from the right. */

/* Computing MIN */
		i__4 = j + 3;
		i__3 = f2cmin(i__4,ilast);
		for (jr = ifrstm; jr <= i__3; ++jr) {
		    temp = tau * (h__[jr + j * h_dim1] + v[1] * h__[jr + (j + 
			    1) * h_dim1] + v[2] * h__[jr + (j + 2) * h_dim1]);
		    h__[jr + j * h_dim1] -= temp;
		    h__[jr + (j + 1) * h_dim1] -= temp * v[1];
		    h__[jr + (j + 2) * h_dim1] -= temp * v[2];
/* L260: */
		}
		i__3 = j + 2;
		for (jr = ifrstm; jr <= i__3; ++jr) {
		    temp = tau * (t[jr + j * t_dim1] + v[1] * t[jr + (j + 1) *
			     t_dim1] + v[2] * t[jr + (j + 2) * t_dim1]);
		    t[jr + j * t_dim1] -= temp;
		    t[jr + (j + 1) * t_dim1] -= temp * v[1];
		    t[jr + (j + 2) * t_dim1] -= temp * v[2];
/* L270: */
		}
		if (ilz) {
		    i__3 = *n;
		    for (jr = 1; jr <= i__3; ++jr) {
			temp = tau * (z__[jr + j * z_dim1] + v[1] * z__[jr + (
				j + 1) * z_dim1] + v[2] * z__[jr + (j + 2) * 
				z_dim1]);
			z__[jr + j * z_dim1] -= temp;
			z__[jr + (j + 1) * z_dim1] -= temp * v[1];
			z__[jr + (j + 2) * z_dim1] -= temp * v[2];
/* L280: */
		    }
		}
		t[j + 1 + j * t_dim1] = 0.f;
		t[j + 2 + j * t_dim1] = 0.f;
/* L290: */
	    }

/*           Last elements: Use Givens rotations */

/*           Rotations from the left */

	    j = ilast - 1;
	    temp = h__[j + (j - 1) * h_dim1];
	    slartg_(&temp, &h__[j + 1 + (j - 1) * h_dim1], &c__, &s, &h__[j + 
		    (j - 1) * h_dim1]);
	    h__[j + 1 + (j - 1) * h_dim1] = 0.f;

	    i__2 = ilastm;
	    for (jc = j; jc <= i__2; ++jc) {
		temp = c__ * h__[j + jc * h_dim1] + s * h__[j + 1 + jc * 
			h_dim1];
		h__[j + 1 + jc * h_dim1] = -s * h__[j + jc * h_dim1] + c__ * 
			h__[j + 1 + jc * h_dim1];
		h__[j + jc * h_dim1] = temp;
		temp2 = c__ * t[j + jc * t_dim1] + s * t[j + 1 + jc * t_dim1];
		t[j + 1 + jc * t_dim1] = -s * t[j + jc * t_dim1] + c__ * t[j 
			+ 1 + jc * t_dim1];
		t[j + jc * t_dim1] = temp2;
/* L300: */
	    }
	    if (ilq) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    temp = c__ * q[jr + j * q_dim1] + s * q[jr + (j + 1) * 
			    q_dim1];
		    q[jr + (j + 1) * q_dim1] = -s * q[jr + j * q_dim1] + c__ *
			     q[jr + (j + 1) * q_dim1];
		    q[jr + j * q_dim1] = temp;
/* L310: */
		}
	    }

/*           Rotations from the right. */

	    temp = t[j + 1 + (j + 1) * t_dim1];
	    slartg_(&temp, &t[j + 1 + j * t_dim1], &c__, &s, &t[j + 1 + (j + 
		    1) * t_dim1]);
	    t[j + 1 + j * t_dim1] = 0.f;

	    i__2 = ilast;
	    for (jr = ifrstm; jr <= i__2; ++jr) {
		temp = c__ * h__[jr + (j + 1) * h_dim1] + s * h__[jr + j * 
			h_dim1];
		h__[jr + j * h_dim1] = -s * h__[jr + (j + 1) * h_dim1] + c__ *
			 h__[jr + j * h_dim1];
		h__[jr + (j + 1) * h_dim1] = temp;
/* L320: */
	    }
	    i__2 = ilast - 1;
	    for (jr = ifrstm; jr <= i__2; ++jr) {
		temp = c__ * t[jr + (j + 1) * t_dim1] + s * t[jr + j * t_dim1]
			;
		t[jr + j * t_dim1] = -s * t[jr + (j + 1) * t_dim1] + c__ * t[
			jr + j * t_dim1];
		t[jr + (j + 1) * t_dim1] = temp;
/* L330: */
	    }
	    if (ilz) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    temp = c__ * z__[jr + (j + 1) * z_dim1] + s * z__[jr + j *
			     z_dim1];
		    z__[jr + j * z_dim1] = -s * z__[jr + (j + 1) * z_dim1] + 
			    c__ * z__[jr + j * z_dim1];
		    z__[jr + (j + 1) * z_dim1] = temp;
/* L340: */
		}
	    }

/*           End of Double-Shift code */

	}

	goto L350;

/*        End of iteration loop */

L350:
/* L360: */
	;
    }

/*     Drop-through = non-convergence */

    *info = ilast;
    goto L420;

/*     Successful completion of all QZ steps */

L380:

/*     Set Eigenvalues 1:ILO-1 */

    i__1 = *ilo - 1;
    for (j = 1; j <= i__1; ++j) {
	if (t[j + j * t_dim1] < 0.f) {
	    if (ilschr) {
		i__2 = j;
		for (jr = 1; jr <= i__2; ++jr) {
		    h__[jr + j * h_dim1] = -h__[jr + j * h_dim1];
		    t[jr + j * t_dim1] = -t[jr + j * t_dim1];
/* L390: */
		}
	    } else {
		h__[j + j * h_dim1] = -h__[j + j * h_dim1];
		t[j + j * t_dim1] = -t[j + j * t_dim1];
	    }
	    if (ilz) {
		i__2 = *n;
		for (jr = 1; jr <= i__2; ++jr) {
		    z__[jr + j * z_dim1] = -z__[jr + j * z_dim1];
/* L400: */
		}
	    }
	}
	alphar[j] = h__[j + j * h_dim1];
	alphai[j] = 0.f;
	beta[j] = t[j + j * t_dim1];
/* L410: */
    }

/*     Normal Termination */

    *info = 0;

/*     Exit (other than argument error) -- return optimal workspace size */

L420:
    work[1] = (real) (*n);
    return;

/*     End of SHGEQZ */

} /* shgeqz_ */

