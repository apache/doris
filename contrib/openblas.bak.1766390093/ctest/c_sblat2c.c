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

#include "common.h"

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
typedef int logical;
typedef short int shortlogical;
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
#define z_abs(z) (cabs(Cd(z)))
#define z_exp(R, Z) {pCd(R) = cexp(Cd(Z));}
#define z_sqrt(R, Z) {pCd(R) = csqrt(Cd(Z));}
#define myexit_() break;
#define mycycle_() continue;
#define myceiling_(w) {ceil(w)}
#define myhuge_(w) {HUGE_VAL}
//#define mymaxloc_(w,s,e,n) {if (sizeof(*(w)) == sizeof(double)) dmaxloc_((w),*(s),*(e),n); else dmaxloc_((w),*(s),*(e),n);}
#define mymaxloc_(w,s,e,n) dmaxloc_(w,*(s),*(e),n)

/* procedure parameter types for -A and -C++ */

#define F2C_proc_par_types 1


/* Common Block Declarations */

struct {
    integer infot, noutc;
    logical ok;
} infoc_;

#define infoc_1 infoc_

struct {
    char srnamt[12];
} srnamc_;

#define srnamc_1 srnamc_

/* Table of constant values */

static integer c__1 = 1;
static integer c__65 = 65;
static integer c__2 = 2;
static real c_b123 = (float)1.;
static real c_b135 = (float)0.;
static integer c__6 = 6;
static logical c_true = TRUE_;
static integer c_n1 = -1;
static integer c__0 = 0;
static logical c_false = FALSE_;

/* Main program */ int main(void)
{
    /* Initialized data */

    static char snames[16][13] = { "cblas_sgemv ", "cblas_sgbmv ", "cblas_ssymv ",
    "cblas_ssbmv ", "cblas_sspmv ", "cblas_strmv ", "cblas_stbmv ", "cblas_stpmv ",
    "cblas_strsv ", "cblas_stbsv ", "cblas_stpsv ", "cblas_sger  ", "cblas_ssyr  ",
    "cblas_sspr  ", "cblas_ssyr2 ", "cblas_sspr2 "};

    /* System generated locals */
    integer i__1, i__2, i__3;
    real r__1;

    /* Local variables */
    static integer nalf, idim[9];
    static logical same;
    static integer ninc, nbet, ntra;
    static logical rewi;
extern /* Subroutine */ int schk1_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
extern /* Subroutine */ int schk2_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
extern /* Subroutine */ int schk3_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, integer*, integer*, integer*, integer*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
extern /* Subroutine */ int schk4_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, real*, integer*, integer*, integer*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
extern /* Subroutine */ int schk5_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, real*, integer*, integer*, integer*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
extern /* Subroutine */ int schk6_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* ninc, integer* inc, integer* nmax, integer* incmax, real* a, real* aa, real* as, real* x, real* xx, real* xs, real* y, real* yy, real* ys, real* yt, real* g, real* z__, integer* iorder, ftnlen sname_len);
    static real a[4225]	/* was [65][65] */, g[65];
    static integer i__, j, n;
    static logical fatal;
    static real x[65], y[65], z__[130];
    extern doublereal sdiff_(real*, real*);
    static logical trace;
    static integer nidim;
    extern /* Subroutine */ int smvch_(char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, real*, real*, logical*, integer*, logical*, ftnlen);
    static char snaps[32], trans[1];
    static integer isnum;
    static logical ltest[16];
    static real aa[4225];
    static integer kb[7];
    static real as[4225];
    static logical sfatal;
    static real xs[130], ys[130];
    static logical corder;
    static real xx[130], yt[65], yy[130];
    static char snamet[12];
    static real thresh;
    static logical rorder;
    static integer layout;
    static logical ltestt;
    extern /* Subroutine */ int cs2chke_(char*, ftnlen);
    static logical tsterr;
    static real alf[7];
    static integer inc[7], nkb;
    static real bet[7];
    extern logical lse_(real*, real*, integer*);
    static real eps, err;
    char   tmpchar;
    
/*  Test program for the REAL             Level 2 Blas. */

/*  The program must be driven by a short data file. The first 17 records */
/*  of the file are read using list-directed input, the last 16 records */
/*  are read using the format ( A12, L2 ). An annotated example of a data */
/*  file can be obtained by deleting the first 3 characters from the */
/*  following 33 lines: */
/*  'SBLAT2.SNAP'     NAME OF SNAPSHOT OUTPUT FILE */
/*  -1                UNIT NUMBER OF SNAPSHOT FILE (NOT USED IF .LT. 0) */
/*  F        LOGICAL FLAG, T TO REWIND SNAPSHOT FILE AFTER EACH RECORD. */
/*  F        LOGICAL FLAG, T TO STOP ON FAILURES. */
/*  T        LOGICAL FLAG, T TO TEST ERROR EXITS. */
/*  2        0 TO TEST COLUMN-MAJOR, 1 TO TEST ROW-MAJOR, 2 TO TEST BOTH */
/*  16.0     THRESHOLD VALUE OF TEST RATIO */
/*  6                 NUMBER OF VALUES OF N */
/*  0 1 2 3 5 9       VALUES OF N */
/*  4                 NUMBER OF VALUES OF K */
/*  0 1 2 4           VALUES OF K */
/*  4                 NUMBER OF VALUES OF INCX AND INCY */
/*  1 2 -1 -2         VALUES OF INCX AND INCY */
/*  3                 NUMBER OF VALUES OF ALPHA */
/*  0.0 1.0 0.7       VALUES OF ALPHA */
/*  3                 NUMBER OF VALUES OF BETA */
/*  0.0 1.0 0.9       VALUES OF BETA */
/*  cblas_sgemv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_sgbmv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ssymv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ssbmv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_sspmv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_strmv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_stbmv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_stpmv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_strsv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_stbsv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_stpsv  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_sger   T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ssyr   T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_sspr   T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ssyr2  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_sspr2  T PUT F FOR NO TEST. SAME COLUMNS. */

/*     See: */

/*        Dongarra J. J., Du Croz J. J., Hammarling S.  and Hanson R. J.. */
/*        An  extended  set of Fortran  Basic Linear Algebra Subprograms. */

/*        Technical  Memoranda  Nos. 41 (revision 3) and 81,  Mathematics */
/*        and  Computer Science  Division,  Argonne  National Laboratory, */
/*        9700 South Cass Avenue, Argonne, Illinois 60439, US. */

/*        Or */

/*        NAG  Technical Reports TR3/87 and TR4/87,  Numerical Algorithms */
/*        Group  Ltd.,  NAG  Central  Office,  256  Banbury  Road, Oxford */
/*        OX2 7DE, UK,  and  Numerical Algorithms Group Inc.,  1101  31st */
/*        Street,  Suite 100,  Downers Grove,  Illinois 60515-1263,  USA. */


/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
/*     .. Executable Statements .. */

    infoc_1.noutc = 6;

/*     Read name and unit number for snapshot output file and open file. */

    char line[80];
    
    fgets(line,80,stdin);
    sscanf(line,"'%s'",snaps);
    fgets(line,80,stdin);
#ifdef USE64BITINT
    sscanf(line,"%ld",&ntra);
#else
    sscanf(line,"%d",&ntra);
#endif
    trace = ntra >= 0;

    if (trace) {
/*	o__1.oerr = 0;
	o__1.ounit = ntra;
	o__1.ofnmlen = 32;
	o__1.ofnm = snaps;
	o__1.orl = 0;
	o__1.osta = 0;
	o__1.oacc = 0;
	o__1.ofm = 0;
	o__1.oblnk = 0;
	f_open(&o__1);*/
    }
/*     Read the flag that directs rewinding of the snapshot file. */
   fgets(line,80,stdin);
   sscanf(line,"%d",&rewi);
   rewi = rewi && trace;
/*     Read the flag that directs stopping on any failure. */
   fgets(line,80,stdin);
   sscanf(line,"%c",&tmpchar);
/*     Read the flag that indicates whether error exits are to be tested. */
   sfatal=FALSE_;
   if (tmpchar=='T')sfatal=TRUE_;
   fgets(line,80,stdin);
   sscanf(line,"%c",&tmpchar);
/*     Read the flag that indicates whether error exits are to be tested. */
   tsterr=FALSE_;
   if (tmpchar=='T')tsterr=TRUE_;
/*     Read the flag that indicates whether row-major data layout to be tested. */
   fgets(line,80,stdin);
   sscanf(line,"%d",&layout);
/*     Read the threshold value of the test ratio */
   fgets(line,80,stdin);
   sscanf(line,"%f",&thresh);

/*     Read and check the parameter values for the tests. */

/*     Values of N */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nidim);
#else
   sscanf(line,"%d",&nidim);
#endif
    if (nidim < 1 || nidim > 9) {
        fprintf(stderr,"NUMBER OF VALUES OF N IS LESS THAN 1 OR GREATER THAN 9");
        goto L220;
    }
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld %ld %ld %ld %ld %ld %ld %ld %ld",&idim[0],&idim[1],&idim[2],
    &idim[3],&idim[4],&idim[5],&idim[6],&idim[7],&idim[8]);
#else
   sscanf(line,"%d %d %d %d %d %d %d %d %d",&idim[0],&idim[1],&idim[2],
    &idim[3],&idim[4],&idim[5],&idim[6],&idim[7],&idim[8]);
#endif
    i__1 = nidim;
    for (i__ = 1; i__ <= i__1; ++i__) {
        if (idim[i__ - 1] < 0 || idim[i__ - 1] > 65) {
        fprintf(stderr,"VALUE OF N IS LESS THAN 0 OR GREATER THAN 65\n");
            goto L220;
        }
/* L10: */
    }
/*     Values of K */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nkb);
#else
   sscanf(line,"%d",&nkb);
#endif
    if (nkb < 1 || nkb > 7) {
        fprintf(stderr,"NUMBER OF VALUES OF K IS LESS THAN 1 OR GREATER THAN 7");
        goto L220;
    }
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld %ld %ld %ld %ld %ld %ld",&kb[0],&kb[1],&kb[2],&kb[3],&kb[4],&kb[5],&kb[6]);
#else
   sscanf(line,"%d %d %d %d %d %d %d",&kb[0],&kb[1],&kb[2],&kb[3],&kb[4],&kb[5],&kb[6]);
#endif
    i__1 = nkb;
    for (i__ = 1; i__ <= i__1; ++i__) {
        if (kb[i__ - 1] < 0 ) {
        fprintf(stderr,"VALUE OF K IS LESS THAN 0\n");
            goto L230;
        }
/* L20: */
    }
/*     Values of INCX and INCY */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&ninc);
#else
   sscanf(line,"%d",&ninc);
#endif
    if (ninc < 1 || ninc > 7) {
        fprintf(stderr,"NUMBER OF VALUES OF INCX AND INCY IS LESS THAN 1 OR GREATER THAN 7");
        goto L230;
    }

   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld %ld %ld %ld %ld %ld %ld",&inc[0],&inc[1],&inc[2],&inc[3],&inc[4],&inc[5],&inc[6]);
#else
   sscanf(line,"%d %d %d %d %d %d %d",&inc[0],&inc[1],&inc[2],&inc[3],&inc[4],&inc[5],&inc[6]);
#endif
    i__1 = ninc;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (inc[i__ - 1] == 0 || (i__2 = inc[i__ - 1], abs(i__2)) > 2) {
    	    fprintf (stderr,"ABSOLUTE VALUE OF INCX OR INCY IS 0 OR GREATER THAN 2\n");
	    goto L230;
	}
/* L30: */
    }
/*     Values of ALPHA */
   fgets(line,80,stdin);
   sscanf(line,"%d",&nalf);
    if (nalf < 1 || nalf > 7) {
        fprintf(stderr,"VALUE OF ALPHA IS LESS THAN 0 OR GREATER THAN 7\n");
        goto L230;
    }
   fgets(line,80,stdin);
   sscanf(line,"%f %f %f %f %f %f %f",&alf[0],&alf[1],&alf[2],&alf[3],&alf[4],&alf[5],&alf[6]);

/*     Values of BETA */
   fgets(line,80,stdin);
   sscanf(line,"%d",&nbet);
    if (nbet < 1 || nbet > 7) {
        fprintf(stderr,"VALUE OF BETA IS LESS THAN 0 OR GREATER THAN 7\n");
        goto L230;
    }
   fgets(line,80,stdin);
   sscanf(line,"%f %f %f %f %f %f %f",&bet[0],&bet[1],&bet[2],&bet[3],&bet[4],&bet[5],&bet[6]);

/*     Report values of parameters. */
    printf("TESTS OF THE REAL      LEVEL 2 BLAS\nTHE FOLLOWING PARAMETER VALUES WILL BE USED:\n");
    printf(" FOR N");
    for (i__ =1; i__ <=nidim;++i__) printf(" %d",idim[i__-1]);
    printf("\n");

    printf(" FOR K");
    for (i__ =1; i__ <=nkb;++i__) printf(" %d",kb[i__-1]);
    printf("\n");

    printf(" FOR INCX AND INCY");
    for (i__ =1; i__ <=ninc;++i__) printf(" %d",inc[i__-1]);
    printf("\n");

    printf(" FOR ALPHA");
    for (i__ =1; i__ <=nalf;++i__) printf(" %f",alf[i__-1]);
    printf("\n");    
    printf(" FOR BETA");
    for (i__ =1; i__ <=nbet;++i__) printf(" %f",bet[i__-1]);
    printf("\n");    

    if (! tsterr) {
      printf(" ERROR-EXITS WILL NOT BE TESTED\n"); 
    }
    printf("ROUTINES PASS COMPUTATIONAL TESTS IF TEST RATIO IS LESS THAN %f\n",thresh);
    
    rorder = FALSE_;
    corder = FALSE_;
    if (layout == 2) {
	rorder = TRUE_;
	corder = TRUE_;
        printf("COLUMN-MAJOR AND ROW-MAJOR DATA LAYOUTS ARE TESTED\n");
    } else if (layout == 1) {
	rorder = TRUE_;
        printf("ROW-MAJOR DATA LAYOUT IS TESTED\n");
    } else if (layout == 0) {
	corder = TRUE_;
        printf("COLUMN-MAJOR DATA LAYOUT IS TESTED\n");
    }

/*     Read names of subroutines and flags which indicate */
/*     whether they are to be tested. */

    for (i__ = 1; i__ <= 16; ++i__) {
	ltest[i__ - 1] = FALSE_;
/* L40: */
    }
L50:
    if (! fgets(line,80,stdin)) {
	goto L80;
    }
    i__1 = sscanf(line,"%12c %c",snamet,&tmpchar);
   ltestt=FALSE_;
   if (tmpchar=='T')ltestt=TRUE_;
    if (i__1 < 2) {
        goto L80;
    }

    for (i__ = 1; i__ <= 16; ++i__) {
	if (s_cmp(snamet, snames[i__ - 1], (ftnlen)12, (ftnlen)12) == 
		0) {
	    goto L70;
	}
/* L60: */
    }
    printf("SUBPROGRAM NAME %s NOT RECOGNIZED\n****** TESTS ABANDONED ******\n",snamet);
    exit(1);
L70:
    ltest[i__ - 1] = ltestt;
    goto L50;

L80:
/*    cl__1.cerr = 0;
    cl__1.cunit = 5;
    cl__1.csta = 0;
    f_clos(&cl__1);*/

/*     Compute EPS (the machine precision). */

    eps = (float)1.;
L90:
    r__1 = eps + (float)1.;
    if (sdiff_(&r__1, &c_b123) == (float)0.) {
	goto L100;
    }
    eps *= (float).5;
    goto L90;
L100:
    eps += eps;
    printf("RELATIVE MACHINE PRECISION IS TAKEN TO BE %9.1g\n",eps);

/*     Check the reliability of SMVCH using exact data. */

    n = 32;
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = n;
	for (i__ = 1; i__ <= i__2; ++i__) {
/* Computing MAX */
	    i__3 = i__ - j + 1;
	    a[i__ + j * 65 - 66] = (real) f2cmax(i__3,0);
/* L110: */
	}
	x[j - 1] = (real) j;
	y[j - 1] = (float)0.;
/* L120: */
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	yy[j - 1] = (real) (j * ((j + 1) * j) / 2 - (j + 1) * j * (j - 1) / 3)
		;
/* L130: */
    }
/*     YY holds the exact result. On exit from SMVCH YT holds */
/*     the result computed by SMVCH. */
    *(unsigned char *)trans = 'N';
    smvch_(trans, &n, &n, &c_b123, a, &c__65, x, &c__1, &c_b135, y, &c__1, yt,
	     g, yy, &eps, &err, &fatal, &c__6, &c_true, (ftnlen)1);
    same = lse_(yy, yt, &n);
    if (! same || err != (float)0.) {
      printf("ERROR IN SMVCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("SMVCH WAS CALLED WITH TRANS = %s ", trans);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    *(unsigned char *)trans = 'T';
    smvch_(trans, &n, &n, &c_b123, a, &c__65, x, &c_n1, &c_b135, y, &c_n1, yt,
	     g, yy, &eps, &err, &fatal, &c__6, &c_true, (ftnlen)1);
    same = lse_(yy, yt, &n);
    if (! same || err != (float)0.) {
      printf("ERROR IN SMVCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("SMVCH WAS CALLED WITH TRANS = %s ", trans);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }

/*     Test each subroutine in turn. */

    for (isnum = 1; isnum <= 16; ++isnum) {
	if (! ltest[isnum - 1]) {
/*           Subprogram is not to be tested. */
           printf("%12s WAS NOT TESTED\n",snames[isnum-1]);
	} else {
	    s_copy(srnamc_1.srnamt, snames[isnum - 1], (ftnlen)12, (
		    ftnlen)12);
/*           Test error exits. */
	    if (tsterr) {
		cs2chke_(snames[isnum - 1], (ftnlen)12);
	    }
/*           Test computations. */
	    infoc_1.infot = 0;
	    infoc_1.ok = TRUE_;
	    fatal = FALSE_;
	    switch ((int)isnum) {
		case 1:  goto L140;
		case 2:  goto L140;
		case 3:  goto L150;
		case 4:  goto L150;
		case 5:  goto L150;
		case 6:  goto L160;
		case 7:  goto L160;
		case 8:  goto L160;
		case 9:  goto L160;
		case 10:  goto L160;
		case 11:  goto L160;
		case 12:  goto L170;
		case 13:  goto L180;
		case 14:  goto L180;
		case 15:  goto L190;
		case 16:  goto L190;
	    }
/*           Test SGEMV, 01, and SGBMV, 02. */
L140:
	    if (corder) {
		schk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nkb, kb, &nalf,
			 alf, &nbet, bet, &ninc, inc, &c__65, &c__2, a, aa, 
			as, x, xx, xs, y, yy, ys, yt, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nkb, kb, &nalf,
			 alf, &nbet, bet, &ninc, inc, &c__65, &c__2, a, aa, 
			as, x, xx, xs, y, yy, ys, yt, g, &c__1, (ftnlen)12);
	    }
	    goto L200;
/*           Test SSYMV, 03, SSBMV, 04, and SSPMV, 05. */
L150:
	    if (corder) {
		schk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nkb, kb, &nalf,
			 alf, &nbet, bet, &ninc, inc, &c__65, &c__2, a, aa, 
			as, x, xx, xs, y, yy, ys, yt, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nkb, kb, &nalf,
			 alf, &nbet, bet, &ninc, inc, &c__65, &c__2, a, aa, 
			as, x, xx, xs, y, yy, ys, yt, g, &c__1, (ftnlen)12);
	    }
	    goto L200;
/*           Test STRMV, 06, STBMV, 07, STPMV, 08, */
/*           STRSV, 09, STBSV, 10, and STPSV, 11. */
L160:
	    if (corder) {
		schk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nkb, kb, &ninc,
			 inc, &c__65, &c__2, a, aa, as, y, yy, ys, yt, g, z__,
			 &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nkb, kb, &ninc,
			 inc, &c__65, &c__2, a, aa, as, y, yy, ys, yt, g, z__,
			 &c__1, (ftnlen)12);
	    }
	    goto L200;
/*           Test SGER, 12. */
L170:
	    if (corder) {
		schk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			ninc, inc, &c__65, &c__2, a, aa, as, x, xx, xs, y, yy,
			 ys, yt, g, z__, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			ninc, inc, &c__65, &c__2, a, aa, as, x, xx, xs, y, yy,
			 ys, yt, g, z__, &c__1, (ftnlen)12);
	    }
	    goto L200;
/*           Test SSYR, 13, and SSPR, 14. */
L180:
	    if (corder) {
		schk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			ninc, inc, &c__65, &c__2, a, aa, as, x, xx, xs, y, yy,
			 ys, yt, g, z__, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			ninc, inc, &c__65, &c__2, a, aa, as, x, xx, xs, y, yy,
			 ys, yt, g, z__, &c__1, (ftnlen)12);
	    }
	    goto L200;
/*           Test SSYR2, 15, and SSPR2, 16. */
L190:
	    if (corder) {
		schk6_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			ninc, inc, &c__65, &c__2, a, aa, as, x, xx, xs, y, yy,
			 ys, yt, g, z__, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk6_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			ninc, inc, &c__65, &c__2, a, aa, as, x, xx, xs, y, yy,
			 ys, yt, g, z__, &c__1, (ftnlen)12);
	    }

L200:
	    if (fatal && sfatal) {
		goto L220;
	    }
	}
/* L210: */
    }
    printf("\nEND OF TESTS\n");
    goto L240;

L220:
    printf("\n****** FATAL ERROR - TESTS ABANDONED ******\n");
    goto L240;

L230:
    printf("AMEND DATA FILE OR INCREASE ARRAY SIZES IN PROGRAM\n");
    printf("****** TESTS ABANDONED ******\n");

L240:
    if (trace) {
/*	cl__1.cerr = 0;
	cl__1.cunit = ntra;
	cl__1.csta = 0;
	f_clos(&cl__1);*/
    }
/*    cl__1.cerr = 0;
    cl__1.cunit = 6;
    cl__1.csta = 0;
    f_clos(&cl__1);
    s_stop("", (ftnlen)0);*/
    exit(0);

/*     End of SBLAT2. */

} /* MAIN__ */

/* Subroutine */ int schk1_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nkb, integer* kb, integer* nalf, real* alf, integer* nbet, real* bet, integer* ninc, integer* inc, integer* nmax, integer* incmax, real* a, real* aa, real* as, real* x, real* xx, real* xs, real* y, real* yy, real* ys, real* yt, real* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ich[3+1] = "NTC";

    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8;

    /* Local variables */
    static real beta;
    static integer ldas;
    static logical same;
    static integer incx, incy;
    static logical full, tran, null;
    static integer i__, m, n;
    static real alpha;
    static logical isame[13];
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int smvch_(char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, real*, real*, logical*, integer*, logical*, ftnlen);
    static logical reset;
    static integer incxs, incys;
    static char trans[1];
    static integer ia, ib, ic;
    static logical banded;
    static integer nc, nd, im, in, kl, ml, nk, nl, ku, ix, iy, ms, lx, ly, ns;
    extern /* Subroutine */ void csgbmv_(integer*, char*, integer*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, ftnlen);
    extern /* Subroutine */ void csgemv_(integer*, char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, ftnlen);
    static char ctrans[14];
    static real errmax;
    extern logical lseres_(char* type__, char* uplo, integer* m, integer* n, real* aa, real* as, integer* lda, ftnlen ltype_len, ftnlen uplo_len);
    static real transl;
    static char transs[1];
    static integer laa, lda;
    static real als, bls;
    extern logical lse_(real*, real*, integer*);
    static real err;
    static integer iku, kls, kus;

/*  Tests SGEMV and SGBMV. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --kb;
    --alf;
    --bet;
    --inc;
    --g;
    --yt;
    --y;
    --x;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ys;
    --yy;
    --xs;
    --xx;

    /* Function Body */
/*     .. Executable Statements .. */
    full = *(unsigned char *)&sname[8] == 'e';
    banded = *(unsigned char *)&sname[8] == 'b';
/*     Define the number of arguments. */
    if (full) {
	nargs = 11;
    } else if (banded) {
	nargs = 13;
    }

    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
	nd = n / 2 + 1;

	for (im = 1; im <= 2; ++im) {
	    if (im == 1) {
/* Computing MAX */
		i__2 = n - nd;
		m = f2cmax(i__2,0);
	    }
	    if (im == 2) {
/* Computing MIN */
		i__2 = n + nd;
		m = f2cmin(i__2,*nmax);
	    }

	    if (banded) {
		nk = *nkb;
	    } else {
		nk = 1;
	    }
	    i__2 = nk;
	    for (iku = 1; iku <= i__2; ++iku) {
		if (banded) {
		    ku = kb[iku];
/* Computing MAX */
		    i__3 = ku - 1;
		    kl = f2cmax(i__3,0);
		} else {
		    ku = n - 1;
		    kl = m - 1;
		}
/*              Set LDA to 1 more than minimum value if room. */
		if (banded) {
		    lda = kl + ku + 1;
		} else {
		    lda = m;
		}
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L100;
		}
		laa = lda * n;
		null = n <= 0 || m <= 0;

/*              Generate the matrix A. */

		transl = (float)0.;
		smake_(sname + 7, " ", " ", &m, &n, &a[a_offset], nmax, &aa[1]
			, &lda, &kl, &ku, &reset, &transl, (ftnlen)2, (ftnlen)
			1, (ftnlen)1);

		for (ic = 1; ic <= 3; ++ic) {
		    *(unsigned char *)trans = *(unsigned char *)&ich[ic - 1];
		    if (*(unsigned char *)trans == 'N') {
			s_copy(ctrans, "  CblasNoTrans", (ftnlen)14, (ftnlen)
				14);
		    } else if (*(unsigned char *)trans == 'T') {
			s_copy(ctrans, "    CblasTrans", (ftnlen)14, (ftnlen)
				14);
		    } else {
			s_copy(ctrans, "CblasConjTrans", (ftnlen)14, (ftnlen)
				14);
		    }
		    tran = *(unsigned char *)trans == 'T' || *(unsigned char *
			    )trans == 'C';

		    if (tran) {
			ml = n;
			nl = m;
		    } else {
			ml = m;
			nl = n;
		    }

		    i__3 = *ninc;
		    for (ix = 1; ix <= i__3; ++ix) {
			incx = inc[ix];
			lx = abs(incx) * nl;

/*                    Generate the vector X. */

			transl = (float).5;
			i__4 = abs(incx);
			i__5 = nl - 1;
			smake_("ge", " ", " ", &c__1, &nl, &x[1], &c__1, &xx[
				1], &i__4, &c__0, &i__5, &reset, &transl, (
				ftnlen)2, (ftnlen)1, (ftnlen)1);
			if (nl > 1) {
			    x[nl / 2] = (float)0.;
			    xx[abs(incx) * (nl / 2 - 1) + 1] = (float)0.;
			}

			i__4 = *ninc;
			for (iy = 1; iy <= i__4; ++iy) {
			    incy = inc[iy];
			    ly = abs(incy) * ml;

			    i__5 = *nalf;
			    for (ia = 1; ia <= i__5; ++ia) {
				alpha = alf[ia];

				i__6 = *nbet;
				for (ib = 1; ib <= i__6; ++ib) {
				    beta = bet[ib];

/*                             Generate the vector Y. */

				    transl = (float)0.;
				    i__7 = abs(incy);
				    i__8 = ml - 1;
				    smake_("ge", " ", " ", &c__1, &ml, &y[1], 
					    &c__1, &yy[1], &i__7, &c__0, &
					    i__8, &reset, &transl, (ftnlen)2, 
					    (ftnlen)1, (ftnlen)1);

				    ++nc;

/*                             Save every datum before calling the */
/*                             subroutine. */

				    *(unsigned char *)transs = *(unsigned 
					    char *)trans;
				    ms = m;
				    ns = n;
				    kls = kl;
				    kus = ku;
				    als = alpha;
				    i__7 = laa;
				    for (i__ = 1; i__ <= i__7; ++i__) {
					as[i__] = aa[i__];
/* L10: */
				    }
				    ldas = lda;
				    i__7 = lx;
				    for (i__ = 1; i__ <= i__7; ++i__) {
					xs[i__] = xx[i__];
/* L20: */
				    }
				    incxs = incx;
				    bls = beta;
				    i__7 = ly;
				    for (i__ = 1; i__ <= i__7; ++i__) {
					ys[i__] = yy[i__];
/* L30: */
				    }
				    incys = incy;

/*                             Call the subroutine. */

				    if (full) {
					if (*trace) {
/*
    					    sprintf(ntra,"%6d: %12s  %14s %3d %3d %4.1f A %3d  X  %2d %4.1f  Y  %2d .\n",
    					    nc,sname,ctrans,m,n,alpha,lda,incx,beta,incy);
*/
					}
					if (*rewi) {
/*					    al__1.aerr = 0;
					    al__1.aunit = *ntra;
					    f_rew(&al__1);*/
					}
					csgemv_(iorder, trans, &m, &n, &alpha,
						 &aa[1], &lda, &xx[1], &incx, 
						&beta, &yy[1], &incy, (ftnlen)
						1);
				    } else if (banded) {
					if (*trace) {
/*
					    sprintf(ntra,"%6d: %12s %14s %3d %3d %3d %3d %4.1f A %3d            %2d  %4.1f  Y %2d\n",
    					    nc,sname,ctrans,m,n,kl,ku,alpha,lda,incx,beta,incy);
*/
					}
					if (*rewi) {
/*					    al__1.aerr = 0;
					    al__1.aunit = *ntra;
					    f_rew(&al__1);*/
					}
					csgbmv_(iorder, trans, &m, &n, &kl, &
						ku, &alpha, &aa[1], &lda, &xx[
						1], &incx, &beta, &yy[1], &
						incy, (ftnlen)1);
				    }

/*                             Check if error-exit was taken incorrectly. */

				    if (! infoc_1.ok) {
				    	printf(" ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *******n");
					*fatal = TRUE_;
					goto L130;
				    }

/*                             See what data changed inside subroutines. */

				    isame[0] = *(unsigned char *)trans == *(
					    unsigned char *)transs;
				    isame[1] = ms == m;
				    isame[2] = ns == n;
				    if (full) {
					isame[3] = als == alpha;
					isame[4] = lse_(&as[1], &aa[1], &laa);
					isame[5] = ldas == lda;
					isame[6] = lse_(&xs[1], &xx[1], &lx);
					isame[7] = incxs == incx;
					isame[8] = bls == beta;
					if (null) {
					    isame[9] = lse_(&ys[1], &yy[1], &
						    ly);
					} else {
					    i__7 = abs(incy);
					    isame[9] = lseres_("ge", " ", &
						    c__1, &ml, &ys[1], &yy[1],
						     &i__7, (ftnlen)2, (
						    ftnlen)1);
					}
					isame[10] = incys == incy;
				    } else if (banded) {
					isame[3] = kls == kl;
					isame[4] = kus == ku;
					isame[5] = als == alpha;
					isame[6] = lse_(&as[1], &aa[1], &laa);
					isame[7] = ldas == lda;
					isame[8] = lse_(&xs[1], &xx[1], &lx);
					isame[9] = incxs == incx;
					isame[10] = bls == beta;
					if (null) {
					    isame[11] = lse_(&ys[1], &yy[1], &
						    ly);
					} else {
					    i__7 = abs(incy);
					    isame[11] = lseres_("ge", " ", &
						    c__1, &ml, &ys[1], &yy[1],
						     &i__7, (ftnlen)2, (
						    ftnlen)1);
					}
					isame[12] = incys == incy;
				    }

/*                             If data was incorrectly changed, report */
/*                             and return. */

				    same = TRUE_;
				    i__7 = nargs;
				    for (i__ = 1; i__ <= i__7; ++i__) {
					same = same && isame[i__ - 1];
					if (! isame[i__ - 1]) {
					    printf(" ******* FATAL ERROR - PARAMETER NUMBER %2d WAS CHANGED INCORRECTLY *******\n",i__);
					}
/* L40: */
				    }
				    if (! same) {
					*fatal = TRUE_;
					goto L130;
				    }

				    if (! null) {

/*                                Check the result. */

					smvch_(trans, &m, &n, &alpha, &a[
						a_offset], nmax, &x[1], &incx,
						 &beta, &y[1], &incy, &yt[1], 
						&g[1], &yy[1], eps, &err, 
						fatal, nout, &c_true, (ftnlen)
						1);
					errmax = dmax(errmax,err);
/*                                If got really bad answer, report and */
/*                                return. */
					if (*fatal) {
					    goto L130;
					}
				    } else {
/*                                Avoid repeating tests with M.le.0 or */
/*                                N.le.0. */
					goto L110;
				    }

/* L50: */
				}

/* L60: */
			    }

/* L70: */
			}

/* L80: */
		    }

/* L90: */
		}

L100:
		;
	    }

L110:
	    ;
	}

/* L120: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
	    printf("%12s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
	    printf("%12s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
	    printf("%12s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
	if (*iorder == 1) {
	    printf("%12s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
    }
    goto L140;

L130:
    printf("******* %12s FAILED ON CALL NUMBER:",sname);
    if (full) {
	printf("%6d: %12s  %14s %3d %3d %4.1f A %3d  X  %2d %4.1f  Y  %2d .\n",
		nc,sname,ctrans,m,n,alpha,lda,incx,beta,incy);
    } else if (banded) {
	printf("%6d: %12s %14s %3d %3d %3d %3d %4.1f A %3d            %2d  %4.1f  Y %2d\n",
		nc,sname,ctrans,m,n,kl,ku,alpha,lda,incx,beta,incy);
    }

L140:
    return 0;

/* 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C', */
/*     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2, */
/*     $      ' - SUSPECT *******' ) */

/*     End of SCHK1. */

} /* schk1_ */

/* Subroutine */ int schk2_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nkb, integer* kb, integer* nalf, real* alf, integer* nbet, real* bet, integer* ninc, integer* inc, integer* nmax, integer* incmax, real* a, real* aa, real* as, real* x, real* xx, real* xs, real* y, real* yy, real* ys, real* yt, real* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ich[2+1] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8;

    /* Local variables */
    static real beta;
    static integer ldas;
    static logical same;
    static integer incx, incy;
    static logical full, null;
    static char uplo[1];
    static integer i__, k, n;
    static real alpha;
    static logical isame[13];
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int smvch_(char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, real*, real*, logical*, integer*, logical*, ftnlen);
    static logical reset;
    static char cuplo[14];
    static integer incxs, incys;
    static char uplos[1];
    static integer ia, ib, ic;
    static logical banded;
    static integer nc, ik, in;
    static logical packed;
    static integer nk, ks, ix, iy, ns, lx, ly;
    static real errmax;
    extern logical lseres_(char* , char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern /* Subroutine */ void cssbmv_(integer*, char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, ftnlen);
    static real transl;
    extern /* Subroutine */ void csspmv_(integer*, char*, integer*, real*, real*, real*, integer*, real*, real*, integer*, ftnlen);
    extern /* Subroutine */ void cssymv_(integer*, char*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, ftnlen);
    static integer laa, lda;
    static real als, bls;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests SSYMV, SSBMV and SSPMV. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --kb;
    --alf;
    --bet;
    --inc;
    --g;
    --yt;
    --y;
    --x;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ys;
    --yy;
    --xs;
    --xx;

    /* Function Body */
/*     .. Executable Statements .. */
    full = *(unsigned char *)&sname[8] == 'y';
    banded = *(unsigned char *)&sname[8] == 'b';
    packed = *(unsigned char *)&sname[8] == 'p';
/*     Define the number of arguments. */
    if (full) {
	nargs = 10;
    } else if (banded) {
	nargs = 11;
    } else if (packed) {
	nargs = 9;
    }

    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];

	if (banded) {
	    nk = *nkb;
	} else {
	    nk = 1;
	}
	i__2 = nk;
	for (ik = 1; ik <= i__2; ++ik) {
	    if (banded) {
		k = kb[ik];
	    } else {
		k = n - 1;
	    }
/*           Set LDA to 1 more than minimum value if room. */
	    if (banded) {
		lda = k + 1;
	    } else {
		lda = n;
	    }
	    if (lda < *nmax) {
		++lda;
	    }
/*           Skip tests if not enough room. */
	    if (lda > *nmax) {
		goto L100;
	    }
	    if (packed) {
		laa = n * (n + 1) / 2;
	    } else {
		laa = lda * n;
	    }
	    null = n <= 0;

	    for (ic = 1; ic <= 2; ++ic) {
		*(unsigned char *)uplo = *(unsigned char *)&ich[ic - 1];
		if (*(unsigned char *)uplo == 'U') {
		    s_copy(cuplo, "    CblasUpper", (ftnlen)14, (ftnlen)14);
		} else {
		    s_copy(cuplo, "    CblasLower", (ftnlen)14, (ftnlen)14);
		}

/*              Generate the matrix A. */

		transl = (float)0.;
		smake_(sname + 7, uplo, " ", &n, &n, &a[a_offset], nmax, &aa[
			1], &lda, &k, &k, &reset, &transl, (ftnlen)2, (ftnlen)
			1, (ftnlen)1);

		i__3 = *ninc;
		for (ix = 1; ix <= i__3; ++ix) {
		    incx = inc[ix];
		    lx = abs(incx) * n;

/*                 Generate the vector X. */

		    transl = (float).5;
		    i__4 = abs(incx);
		    i__5 = n - 1;
		    smake_("ge", " ", " ", &c__1, &n, &x[1], &c__1, &xx[1], &
			    i__4, &c__0, &i__5, &reset, &transl, (ftnlen)2, (
			    ftnlen)1, (ftnlen)1);
		    if (n > 1) {
			x[n / 2] = (float)0.;
			xx[abs(incx) * (n / 2 - 1) + 1] = (float)0.;
		    }

		    i__4 = *ninc;
		    for (iy = 1; iy <= i__4; ++iy) {
			incy = inc[iy];
			ly = abs(incy) * n;

			i__5 = *nalf;
			for (ia = 1; ia <= i__5; ++ia) {
			    alpha = alf[ia];

			    i__6 = *nbet;
			    for (ib = 1; ib <= i__6; ++ib) {
				beta = bet[ib];

/*                          Generate the vector Y. */

				transl = (float)0.;
				i__7 = abs(incy);
				i__8 = n - 1;
				smake_("ge", " ", " ", &c__1, &n, &y[1], &
					c__1, &yy[1], &i__7, &c__0, &i__8, &
					reset, &transl, (ftnlen)2, (ftnlen)1, 
					(ftnlen)1);

				++nc;

/*                          Save every datum before calling the */
/*                          subroutine. */

				*(unsigned char *)uplos = *(unsigned char *)
					uplo;
				ns = n;
				ks = k;
				als = alpha;
				i__7 = laa;
				for (i__ = 1; i__ <= i__7; ++i__) {
				    as[i__] = aa[i__];
/* L10: */
				}
				ldas = lda;
				i__7 = lx;
				for (i__ = 1; i__ <= i__7; ++i__) {
				    xs[i__] = xx[i__];
/* L20: */
				}
				incxs = incx;
				bls = beta;
				i__7 = ly;
				for (i__ = 1; i__ <= i__7; ++i__) {
				    ys[i__] = yy[i__];
/* L30: */
				}
				incys = incy;

/*                          Call the subroutine. */

				if (full) {
				    if (*trace) {
/*
					sprintf(ntra,"%6d: %12s (%14s, %3d, %4.1f, A %3d, X %2d, %4.1f Y %2d )..\n",
					nc,sname,cuplo,n,alpha,lda,incx,beta,incy);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cssymv_(iorder, uplo, &n, &alpha, &aa[1], 
					    &lda, &xx[1], &incx, &beta, &yy[1]
					    , &incy, (ftnlen)1);
				} else if (banded) {
				    if (*trace) {
/*
					sprintf(ntra,"%6d: %12s (%14s %3d, %3d, %4.1f, A %3d, X %2d, %4.1f, Y, %2d ).\n",
					nc,sname,cuplo,n,k,alpha,lda,incx,beta,incy);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cssbmv_(iorder, uplo, &n, &k, &alpha, &aa[
					    1], &lda, &xx[1], &incx, &beta, &
					    yy[1], &incy, (ftnlen)1);
				} else if (packed) {
				    if (*trace) {
/*
					sprintf(ntra,"%6d: %12s ( %14s %3d, %4.1f, AP  X %2d, %4.1f, Y, %2d ).\n",
					nc,sname,cuplo,n,alpha,incx,beta,incy);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    csspmv_(iorder, uplo, &n, &alpha, &aa[1], 
					    &xx[1], &incx, &beta, &yy[1], &
					    incy, (ftnlen)1);
				}

/*                          Check if error-exit was taken incorrectly. */

				if (! infoc_1.ok) {
				    printf(" ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *******n");
				    *fatal = TRUE_;
				    goto L120;
				}

/*                          See what data changed inside subroutines. */

				isame[0] = *(unsigned char *)uplo == *(
					unsigned char *)uplos;
				isame[1] = ns == n;
				if (full) {
				    isame[2] = als == alpha;
				    isame[3] = lse_(&as[1], &aa[1], &laa);
				    isame[4] = ldas == lda;
				    isame[5] = lse_(&xs[1], &xx[1], &lx);
				    isame[6] = incxs == incx;
				    isame[7] = bls == beta;
				    if (null) {
					isame[8] = lse_(&ys[1], &yy[1], &ly);
				    } else {
					i__7 = abs(incy);
					isame[8] = lseres_("ge", " ", &c__1, &
						n, &ys[1], &yy[1], &i__7, (
						ftnlen)2, (ftnlen)1);
				    }
				    isame[9] = incys == incy;
				} else if (banded) {
				    isame[2] = ks == k;
				    isame[3] = als == alpha;
				    isame[4] = lse_(&as[1], &aa[1], &laa);
				    isame[5] = ldas == lda;
				    isame[6] = lse_(&xs[1], &xx[1], &lx);
				    isame[7] = incxs == incx;
				    isame[8] = bls == beta;
				    if (null) {
					isame[9] = lse_(&ys[1], &yy[1], &ly);
				    } else {
					i__7 = abs(incy);
					isame[9] = lseres_("ge", " ", &c__1, &
						n, &ys[1], &yy[1], &i__7, (
						ftnlen)2, (ftnlen)1);
				    }
				    isame[10] = incys == incy;
				} else if (packed) {
				    isame[2] = als == alpha;
				    isame[3] = lse_(&as[1], &aa[1], &laa);
				    isame[4] = lse_(&xs[1], &xx[1], &lx);
				    isame[5] = incxs == incx;
				    isame[6] = bls == beta;
				    if (null) {
					isame[7] = lse_(&ys[1], &yy[1], &ly);
				    } else {
					i__7 = abs(incy);
					isame[7] = lseres_("ge", " ", &c__1, &
						n, &ys[1], &yy[1], &i__7, (
						ftnlen)2, (ftnlen)1);
				    }
				    isame[8] = incys == incy;
				}

/*                          If data was incorrectly changed, report and */
/*                          return. */

				same = TRUE_;
				i__7 = nargs;
				for (i__ = 1; i__ <= i__7; ++i__) {
				    same = same && isame[i__ - 1];
				    if (! isame[i__ - 1]) {
					printf(" ******* FATAL ERROR - PARAMETER NUMBER %2d WAS CHANGED INCORRECTLY *******\n",i__);
				    }
/* L40: */
				}
				if (! same) {
				    *fatal = TRUE_;
				    goto L120;
				}

				if (! null) {

/*                             Check the result. */

				    smvch_("N", &n, &n, &alpha, &a[a_offset], 
					    nmax, &x[1], &incx, &beta, &y[1], 
					    &incy, &yt[1], &g[1], &yy[1], eps,
					     &err, fatal, nout, &c_true, (
					    ftnlen)1);
				    errmax = dmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L120;
				    }
				} else {
/*                             Avoid repeating tests with N.le.0 */
				    goto L110;
				}

/* L50: */
			    }

/* L60: */
			}

/* L70: */
		    }

/* L80: */
		}

/* L90: */
	    }

L100:
	    ;
	}

L110:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
	    printf("%12s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
	    printf("%12s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
	    printf("%12s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
	if (*iorder == 1) {
	    printf("%12s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
    }
    goto L130;

L120:
    printf("******* %12s FAILED ON CALL NUMBER:",sname);

    if (full) {
	printf("%6d: %12s (%14s, %3d, %4.1f, A %3d, X %2d, %4.1f Y %2d )..\n",
		nc,sname,cuplo,n,alpha,lda,incx,beta,incy);
    } else if (banded) {

	printf("%6d: %12s (%14s %3d, %3d, %4.1f, A %3d, X %2d, %4.1f, Y, %2d ).\n",
		nc,sname,cuplo,n,k,alpha,lda,incx,beta,incy);
    } else if (packed) {
	printf("%6d: %12s ( %14s %3d, %4.1f, AP  X %2d, %4.1f, Y, %2d ).\n",
		nc,sname,cuplo,n,alpha,incx,beta,incy);
    }

L130:
    return 0;

/* 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C', */
/*     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2, */
/*     $      ' - SUSPECT *******' ) */

/*     End of SCHK2. */

} /* schk2_ */

/* Subroutine */ int schk3_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nkb, integer* kb, integer* ninc, integer* inc, integer* nmax, integer* incmax, real* a, real* aa, real* as, real* x, real* xx, real* xs, real* xt, real* g, real* z__, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ichu[2+1] = "UL";
    static char icht[3+1] = "NTC";
    static char ichd[2+1] = "UN";

    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5;

    /* Local variables */
    static char diag[1];
    static integer ldas;
    static logical same;
    static integer incx;
    static logical full, null;
    static char uplo[1], cdiag[14];
    static integer i__, k, n;
    static char diags[1];
    static logical isame[13];
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int smvch_(char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, real*, real*, logical*, integer*, logical*, ftnlen);
    static logical reset;
    static char cuplo[14];
    static integer incxs;
    static char trans[1], uplos[1];
    static logical banded;
    static integer nc, ik, in;
    static logical packed;
    static integer nk, ks, ix, ns, lx;
    static char ctrans[14];
    static real errmax;
    extern logical lseres_(char*, char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern /* Subroutine */ void cstbmv_(integer*, char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    static real transl;
    extern /* Subroutine */ void cstbsv_(integer*, char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    static char transs[1];
    extern /* Subroutine */ void cstpmv_(integer*, char*, char*, char*, integer*, real*, real*, integer*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ void cstrmv_(integer*, char*, char*, char*, integer*, real*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ void cstpsv_(integer*, char*, char*, char*, integer*, real*, real*, integer*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ void cstrsv_(integer*, char*, char*, char*, integer*, real*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    static integer laa, icd, lda, ict, icu;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests STRMV, STBMV, STPMV, STRSV, STBSV and STPSV. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --kb;
    --inc;
    --z__;
    --g;
    --xt;
    --x;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --xs;
    --xx;

    /* Function Body */
/*     .. Executable Statements .. */
    full = *(unsigned char *)&sname[8] == 'r';
    banded = *(unsigned char *)&sname[8] == 'b';
    packed = *(unsigned char *)&sname[8] == 'p';
/*     Define the number of arguments. */
    if (full) {
	nargs = 8;
    } else if (banded) {
	nargs = 9;
    } else if (packed) {
	nargs = 7;
    }

    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;
/*     Set up zero vector for SMVCH. */
    i__1 = *nmax;
    for (i__ = 1; i__ <= i__1; ++i__) {
	z__[i__] = (float)0.;
/* L10: */
    }

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];

	if (banded) {
	    nk = *nkb;
	} else {
	    nk = 1;
	}
	i__2 = nk;
	for (ik = 1; ik <= i__2; ++ik) {
	    if (banded) {
		k = kb[ik];
	    } else {
		k = n - 1;
	    }
/*           Set LDA to 1 more than minimum value if room. */
	    if (banded) {
		lda = k + 1;
	    } else {
		lda = n;
	    }
	    if (lda < *nmax) {
		++lda;
	    }
/*           Skip tests if not enough room. */
	    if (lda > *nmax) {
		goto L100;
	    }
	    if (packed) {
		laa = n * (n + 1) / 2;
	    } else {
		laa = lda * n;
	    }
	    null = n <= 0;

	    for (icu = 1; icu <= 2; ++icu) {
		*(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];
		if (*(unsigned char *)uplo == 'U') {
		    s_copy(cuplo, "    CblasUpper", (ftnlen)14, (ftnlen)14);
		} else {
		    s_copy(cuplo, "    CblasLower", (ftnlen)14, (ftnlen)14);
		}

		for (ict = 1; ict <= 3; ++ict) {
		    *(unsigned char *)trans = *(unsigned char *)&icht[ict - 1]
			    ;
		    if (*(unsigned char *)trans == 'N') {
			s_copy(ctrans, "  CblasNoTrans", (ftnlen)14, (ftnlen)
				14);
		    } else if (*(unsigned char *)trans == 'T') {
			s_copy(ctrans, "    CblasTrans", (ftnlen)14, (ftnlen)
				14);
		    } else {
			s_copy(ctrans, "CblasConjTrans", (ftnlen)14, (ftnlen)
				14);
		    }

		    for (icd = 1; icd <= 2; ++icd) {
			*(unsigned char *)diag = *(unsigned char *)&ichd[icd 
				- 1];
			if (*(unsigned char *)diag == 'N') {
			    s_copy(cdiag, "  CblasNonUnit", (ftnlen)14, (
				    ftnlen)14);
			} else {
			    s_copy(cdiag, "     CblasUnit", (ftnlen)14, (
				    ftnlen)14);
			}

/*                    Generate the matrix A. */

			transl = (float)0.;
			smake_(sname + 7, uplo, diag, &n, &n, &a[a_offset], 
				nmax, &aa[1], &lda, &k, &k, &reset, &transl, (
				ftnlen)2, (ftnlen)1, (ftnlen)1);

			i__3 = *ninc;
			for (ix = 1; ix <= i__3; ++ix) {
			    incx = inc[ix];
			    lx = abs(incx) * n;

/*                       Generate the vector X. */

			    transl = (float).5;
			    i__4 = abs(incx);
			    i__5 = n - 1;
			    smake_("ge", " ", " ", &c__1, &n, &x[1], &c__1, &
				    xx[1], &i__4, &c__0, &i__5, &reset, &
				    transl, (ftnlen)2, (ftnlen)1, (ftnlen)1);
			    if (n > 1) {
				x[n / 2] = (float)0.;
				xx[abs(incx) * (n / 2 - 1) + 1] = (float)0.;
			    }

			    ++nc;

/*                       Save every datum before calling the subroutine. */

			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    *(unsigned char *)transs = *(unsigned char *)
				    trans;
			    *(unsigned char *)diags = *(unsigned char *)diag;
			    ns = n;
			    ks = k;
			    i__4 = laa;
			    for (i__ = 1; i__ <= i__4; ++i__) {
				as[i__] = aa[i__];
/* L20: */
			    }
			    ldas = lda;
			    i__4 = lx;
			    for (i__ = 1; i__ <= i__4; ++i__) {
				xs[i__] = xx[i__];
/* L30: */
			    }
			    incxs = incx;

/*                       Call the subroutine. */

			    if (s_cmp(sname + 9, "mv", (ftnlen)2, (ftnlen)2) 
				    == 0) {
				if (full) {
				    if (*trace) {
/*
				    sprintf(ntra,"%6d: %12s (%14s,%14s,%14s     %3d  ,A,   %3d, X, %2d ).\n",
				    nc,sname,cuplo,ctrans,cdiag,n,lda,incx);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cstrmv_(iorder, uplo, trans, diag, &n, &
					    aa[1], &lda, &xx[1], &incx, (
					    ftnlen)1, (ftnlen)1, (ftnlen)1);
				} else if (banded) {
				    if (*trace) {
/*
				    sprintf(ntra,"%6d: %12s (%14s,%14s,%14s     %3d, %3d  ,A,   %3d, X, %2d ).\n",
				    nc,sname,cuplo,ctrans,cdiag,n,k,lda,incx);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cstbmv_(iorder, uplo, trans, diag, &n, &k,
					     &aa[1], &lda, &xx[1], &incx, (
					    ftnlen)1, (ftnlen)1, (ftnlen)1);
				} else if (packed) {
				    if (*trace) {
/*
				    sprintf(ntra,"%6d: %12s (%14s,%14s,%14s     %3d  ,AP,  X, %2d ).\n",
				    nc,sname,cuplo,ctrans,cdiag,n,incx);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cstpmv_(iorder, uplo, trans, diag, &n, &
					    aa[1], &xx[1], &incx, (ftnlen)1, (
					    ftnlen)1, (ftnlen)1);
				}
			    } else if (s_cmp(sname + 9, "sv", (ftnlen)2, (
				    ftnlen)2) == 0) {
				if (full) {
				    if (*trace) {
/*
				    sprintf(ntra,"%6d: %12s (%14s,%14s,%14s     %3d  ,A,   %3d, X, %2d ).\n",
				    nc,sname,cuplo,ctrans,cdiag,n,lda,incx);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cstrsv_(iorder, uplo, trans, diag, &n, &
					    aa[1], &lda, &xx[1], &incx, (
					    ftnlen)1, (ftnlen)1, (ftnlen)1);
				} else if (banded) {
				    if (*trace) {
/*
				    sprintf(ntra,"%6d: %12s (%14s,%14s,%14s     %3d, %3d  ,A,   %3d, X, %2d ).\n",
				    nc,sname,cuplo,ctrans,cdiag,n,k,lda,incx);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cstbsv_(iorder, uplo, trans, diag, &n, &k,
					     &aa[1], &lda, &xx[1], &incx, (
					    ftnlen)1, (ftnlen)1, (ftnlen)1);
				} else if (packed) {
				    if (*trace) {
/*
				    sprintf(ntra,"%6d: %12s (%14s,%14s,%14s     %3d  ,AP,  X, %2d ).\n",
				    nc,sname,cuplo,ctrans,cdiag,n,incx);
*/
				    }
				    if (*rewi) {
/*					al__1.aerr = 0;
					al__1.aunit = *ntra;
					f_rew(&al__1);*/
				    }
				    cstpsv_(iorder, uplo, trans, diag, &n, &
					    aa[1], &xx[1], &incx, (ftnlen)1, (
					    ftnlen)1, (ftnlen)1);
				}
			    }

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
				printf(" ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *******n");
				*fatal = TRUE_;
				goto L120;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)uplo == *(unsigned 
				    char *)uplos;
			    isame[1] = *(unsigned char *)trans == *(unsigned 
				    char *)transs;
			    isame[2] = *(unsigned char *)diag == *(unsigned 
				    char *)diags;
			    isame[3] = ns == n;
			    if (full) {
				isame[4] = lse_(&as[1], &aa[1], &laa);
				isame[5] = ldas == lda;
				if (null) {
				    isame[6] = lse_(&xs[1], &xx[1], &lx);
				} else {
				    i__4 = abs(incx);
				    isame[6] = lseres_("ge", " ", &c__1, &n, &
					    xs[1], &xx[1], &i__4, (ftnlen)2, (
					    ftnlen)1);
				}
				isame[7] = incxs == incx;
			    } else if (banded) {
				isame[4] = ks == k;
				isame[5] = lse_(&as[1], &aa[1], &laa);
				isame[6] = ldas == lda;
				if (null) {
				    isame[7] = lse_(&xs[1], &xx[1], &lx);
				} else {
				    i__4 = abs(incx);
				    isame[7] = lseres_("ge", " ", &c__1, &n, &
					    xs[1], &xx[1], &i__4, (ftnlen)2, (
					    ftnlen)1);
				}
				isame[8] = incxs == incx;
			    } else if (packed) {
				isame[4] = lse_(&as[1], &aa[1], &laa);
				if (null) {
				    isame[5] = lse_(&xs[1], &xx[1], &lx);
				} else {
				    i__4 = abs(incx);
				    isame[5] = lseres_("ge", " ", &c__1, &n, &
					    xs[1], &xx[1], &i__4, (ftnlen)2, (
					    ftnlen)1);
				}
				isame[6] = incxs == incx;
			    }

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__4 = nargs;
			    for (i__ = 1; i__ <= i__4; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
				    printf(" ******* FATAL ERROR - PARAMETER NUMBER %2d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L40: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L120;
			    }

			    if (! null) {
				if (s_cmp(sname + 9, "mv", (ftnlen)2, (ftnlen)
					2) == 0) {

/*                             Check the result. */

				    smvch_(trans, &n, &n, &c_b123, &a[
					    a_offset], nmax, &x[1], &incx, &
					    c_b135, &z__[1], &incx, &xt[1], &
					    g[1], &xx[1], eps, &err, fatal, 
					    nout, &c_true, (ftnlen)1);
				} else if (s_cmp(sname + 9, "sv", (ftnlen)2, (
					ftnlen)2) == 0) {

/*                             Compute approximation to original vector. */

				    i__4 = n;
				    for (i__ = 1; i__ <= i__4; ++i__) {
					z__[i__] = xx[(i__ - 1) * abs(incx) + 
						1];
					xx[(i__ - 1) * abs(incx) + 1] = x[i__]
						;
/* L50: */
				    }
				    smvch_(trans, &n, &n, &c_b123, &a[
					    a_offset], nmax, &z__[1], &incx, &
					    c_b135, &x[1], &incx, &xt[1], &g[
					    1], &xx[1], eps, &err, fatal, 
					    nout, &c_false, (ftnlen)1);
				}
				errmax = dmax(errmax,err);
/*                          If got really bad answer, report and return. */
				if (*fatal) {
				    goto L120;
				}
			    } else {
/*                          Avoid repeating tests with N.le.0. */
				goto L110;
			    }

/* L60: */
			}

/* L70: */
		    }

/* L80: */
		}

/* L90: */
	    }

L100:
	    ;
	}

L110:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
	    printf("%12s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
	    printf("%12s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
	    printf("%12s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
	if (*iorder == 1) {
	    printf("%12s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
    }
    goto L130;

L120:
    printf("******* %12s FAILED ON CALL NUMBER:",sname);
    if (full) {
	printf("%6d: %12s (%14s,%14s,%14s     %3d  ,A,   %3d, X, %2d ).\n",
	nc,sname,cuplo,ctrans,cdiag,n,lda,incx);
    } else if (banded) {
	printf("%6d: %12s (%14s,%14s,%14s     %3d, %3d  ,A,   %3d, X, %2d ).\n",
	nc,sname,cuplo,ctrans,cdiag,n,k,lda,incx);
    } else if (packed) {
	printf("%6d: %12s (%14s,%14s,%14s     %3d  ,AP,  X, %2d ).\n",
	nc,sname,cuplo,ctrans,cdiag,n,incx);
    }

L130:
    return 0;

/* 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C', */
/*     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2, */
/*     $      ' - SUSPECT *******' ) */

/*     End of SCHK3. */

} /* schk3_ */

/* Subroutine */ int schk4_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* ninc, integer* inc, integer* nmax, integer* incmax, real* a, real* aa, real* as, real* x, real* xx, real* xs, real* y, real* yy, real* ys, real* yt, real* g, real* z__, integer* iorder, ftnlen sname_len)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5, i__6;

    /* Local variables */
    static integer ldas;
    static logical same;
    static integer incx, incy;
    static logical null;
    static integer i__, j, m, n;
    static real alpha, w[1];
    static logical isame[13];
/* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ void csger_(integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, integer*);
    static integer nargs;
    extern /* Subroutine */ int smvch_(char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, real*, real*, logical*, integer*, logical*, ftnlen);
    static logical reset;
    static integer incxs, incys, ia, nc, nd, im, in, ms, ix, iy, ns, lx, ly;
    static real errmax;
    extern logical lseres_(char* , char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    static real transl;
    static integer laa, lda;
    static real als;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests SGER. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Executable Statements .. */
/*     Define the number of arguments. */
    /* Parameter adjustments */
    --idim;
    --alf;
    --inc;
    --z__;
    --g;
    --yt;
    --y;
    --x;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ys;
    --yy;
    --xs;
    --xx;

    /* Function Body */
    nargs = 9;

    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
	nd = n / 2 + 1;

	for (im = 1; im <= 2; ++im) {
	    if (im == 1) {
/* Computing MAX */
		i__2 = n - nd;
		m = f2cmax(i__2,0);
	    }
	    if (im == 2) {
/* Computing MIN */
		i__2 = n + nd;
		m = f2cmin(i__2,*nmax);
	    }

/*           Set LDA to 1 more than minimum value if room. */
	    lda = m;
	    if (lda < *nmax) {
		++lda;
	    }
/*           Skip tests if not enough room. */
	    if (lda > *nmax) {
		goto L110;
	    }
	    laa = lda * n;
	    null = n <= 0 || m <= 0;

	    i__2 = *ninc;
	    for (ix = 1; ix <= i__2; ++ix) {
		incx = inc[ix];
		lx = abs(incx) * m;

/*              Generate the vector X. */

		transl = (float).5;
		i__3 = abs(incx);
		i__4 = m - 1;
		smake_("ge", " ", " ", &c__1, &m, &x[1], &c__1, &xx[1], &i__3,
			 &c__0, &i__4, &reset, &transl, (ftnlen)2, (ftnlen)1, 
			(ftnlen)1);
		if (m > 1) {
		    x[m / 2] = (float)0.;
		    xx[abs(incx) * (m / 2 - 1) + 1] = (float)0.;
		}

		i__3 = *ninc;
		for (iy = 1; iy <= i__3; ++iy) {
		    incy = inc[iy];
		    ly = abs(incy) * n;

/*                 Generate the vector Y. */

		    transl = (float)0.;
		    i__4 = abs(incy);
		    i__5 = n - 1;
		    smake_("ge", " ", " ", &c__1, &n, &y[1], &c__1, &yy[1], &
			    i__4, &c__0, &i__5, &reset, &transl, (ftnlen)2, (
			    ftnlen)1, (ftnlen)1);
		    if (n > 1) {
			y[n / 2] = (float)0.;
			yy[abs(incy) * (n / 2 - 1) + 1] = (float)0.;
		    }

		    i__4 = *nalf;
		    for (ia = 1; ia <= i__4; ++ia) {
			alpha = alf[ia];

/*                    Generate the matrix A. */

			transl = (float)0.;
			i__5 = m - 1;
			i__6 = n - 1;
			smake_(sname + 7, " ", " ", &m, &n, &a[a_offset], 
				nmax, &aa[1], &lda, &i__5, &i__6, &reset, &
				transl, (ftnlen)2, (ftnlen)1, (ftnlen)1);

			++nc;

/*                    Save every datum before calling the subroutine. */

			ms = m;
			ns = n;
			als = alpha;
			i__5 = laa;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    as[i__] = aa[i__];
/* L10: */
			}
			ldas = lda;
			i__5 = lx;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    xs[i__] = xx[i__];
/* L20: */
			}
			incxs = incx;
			i__5 = ly;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    ys[i__] = yy[i__];
/* L30: */
			}
			incys = incy;

/*                    Call the subroutine. */

			if (*trace) {
/*
			    sprintf(ntra,"%6d: %12s (%3d, %3d) %4.1f, X, %2d, Y, %2d, A, %3d).\n",
			    nc,sname,m,n,alpha,incx,incy,lda);
*/			
			}
			if (*rewi) {
/*			    al__1.aerr = 0;
			    al__1.aunit = *ntra;
			    f_rew(&al__1);*/
			}
			csger_(iorder, &m, &n, &alpha, &xx[1], &incx, &yy[1], 
				&incy, &aa[1], &lda);

/*                    Check if error-exit was taken incorrectly. */

			if (! infoc_1.ok) {
			    printf(" ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *******n");
			    *fatal = TRUE_;
			    goto L140;
			}

/*                    See what data changed inside subroutine. */

			isame[0] = ms == m;
			isame[1] = ns == n;
			isame[2] = als == alpha;
			isame[3] = lse_(&xs[1], &xx[1], &lx);
			isame[4] = incxs == incx;
			isame[5] = lse_(&ys[1], &yy[1], &ly);
			isame[6] = incys == incy;
			if (null) {
			    isame[7] = lse_(&as[1], &aa[1], &laa);
			} else {
			    isame[7] = lseres_("ge", " ", &m, &n, &as[1], &aa[
				    1], &lda, (ftnlen)2, (ftnlen)1);
			}
			isame[8] = ldas == lda;

/*                    If data was incorrectly changed, report and return. */

			same = TRUE_;
			i__5 = nargs;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    same = same && isame[i__ - 1];
			    if (! isame[i__ - 1]) {
				printf(" ******* FATAL ERROR - PARAMETER NUMBER %2d WAS CHANGED INCORRECTLY *******\n",i__);
			    }
/* L40: */
			}
			if (! same) {
			    *fatal = TRUE_;
			    goto L140;
			}

			if (! null) {

/*                       Check the result column by column. */

			    if (incx > 0) {
				i__5 = m;
				for (i__ = 1; i__ <= i__5; ++i__) {
				    z__[i__] = x[i__];
/* L50: */
				}
			    } else {
				i__5 = m;
				for (i__ = 1; i__ <= i__5; ++i__) {
				    z__[i__] = x[m - i__ + 1];
/* L60: */
				}
			    }
			    i__5 = n;
			    for (j = 1; j <= i__5; ++j) {
				if (incy > 0) {
				    w[0] = y[j];
				} else {
				    w[0] = y[n - j + 1];
				}
				smvch_("N", &m, &c__1, &alpha, &z__[1], nmax, 
					w, &c__1, &c_b123, &a[j * a_dim1 + 1],
					 &c__1, &yt[1], &g[1], &aa[(j - 1) * 
					lda + 1], eps, &err, fatal, nout, &
					c_true, (ftnlen)1);
				errmax = dmax(errmax,err);
/*                          If got really bad answer, report and return. */
				if (*fatal) {
				    goto L130;
				}
/* L70: */
			    }
			} else {
/*                       Avoid repeating tests with M.le.0 or N.le.0. */
			    goto L110;
			}

/* L80: */
		    }

/* L90: */
		}

/* L100: */
	    }

L110:
	    ;
	}

/* L120: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
	    printf("%12s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
	    printf("%12s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
	    printf("%12s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
	if (*iorder == 1) {
	    printf("%12s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
    }
    goto L150;

L130:
    printf("      THESE ARE THE RESULTS FOR COLUMN %3d:\n",j);

L140:
    printf("******* %12s FAILED ON CALL NUMBER:\n",sname);
    printf("%6d: %12s (%3d, %3d) %4.1f, X, %2d, Y, %2d, A, %3d).\n",
			    nc,sname,m,n,alpha,incx,incy,lda);

L150:
    return 0;

/* 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C', */
/*     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2, */
/*     $      ' - SUSPECT *******' ) */

/*     End of SCHK4. */

} /* schk4_ */

/* Subroutine */ int schk5_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* ninc, integer* inc, integer* nmax, integer* incmax, real* a, real* aa, real* as, real* x, real* xx, real* xs, real* y, real* yy, real* ys, real* yt, real* g, real* z__, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ich[2+1] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4, i__5;

    /* Builtin functions */

    /* Local variables */
    static integer ldas;
    static logical same;
    static integer incx;
    static logical full, null;
    static char uplo[1];
    static integer i__, j, n;
    static real alpha, w[1];
    static logical isame[13];
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int smvch_(char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, real*, real*, logical*, integer*, logical*, ftnlen);
    static logical reset;
    static char cuplo[14];
    static integer incxs;
    extern /* Subroutine */ void csspr_(integer*, char*, integer*, real*, real*, integer*, real*, ftnlen);
    static logical upper;
    static char uplos[1];
    extern /* Subroutine */ void cssyr_(integer*, char*, integer*, real*, real*, integer*, real*, integer*, ftnlen);
    static integer ia, ja, ic, nc, jj, lj, in;
    static logical packed;
    static integer ix, ns, lx;
    static real errmax;
    extern logical lseres_(char*, char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    static real transl;
    static integer laa, lda;
    static real als;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests SSYR and SSPR. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --alf;
    --inc;
    --z__;
    --g;
    --yt;
    --y;
    --x;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ys;
    --yy;
    --xs;
    --xx;

    /* Function Body */
/*     .. Executable Statements .. */
    full = *(unsigned char *)&sname[8] == 'y';
    packed = *(unsigned char *)&sname[8] == 'p';
/*     Define the number of arguments. */
    if (full) {
	nargs = 7;
    } else if (packed) {
	nargs = 6;
    }

    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDA to 1 more than minimum value if room. */
	lda = n;
	if (lda < *nmax) {
	    ++lda;
	}
/*        Skip tests if not enough room. */
	if (lda > *nmax) {
	    goto L100;
	}
	if (packed) {
	    laa = n * (n + 1) / 2;
	} else {
	    laa = lda * n;
	}

	for (ic = 1; ic <= 2; ++ic) {
	    *(unsigned char *)uplo = *(unsigned char *)&ich[ic - 1];
	    if (*(unsigned char *)uplo == 'U') {
		s_copy(cuplo, "    CblasUpper", (ftnlen)14, (ftnlen)14);
	    } else {
		s_copy(cuplo, "    CblasLower", (ftnlen)14, (ftnlen)14);
	    }
	    upper = *(unsigned char *)uplo == 'U';

	    i__2 = *ninc;
	    for (ix = 1; ix <= i__2; ++ix) {
		incx = inc[ix];
		lx = abs(incx) * n;

/*              Generate the vector X. */

		transl = (float).5;
		i__3 = abs(incx);
		i__4 = n - 1;
		smake_("ge", " ", " ", &c__1, &n, &x[1], &c__1, &xx[1], &i__3,
			 &c__0, &i__4, &reset, &transl, (ftnlen)2, (ftnlen)1, 
			(ftnlen)1);
		if (n > 1) {
		    x[n / 2] = (float)0.;
		    xx[abs(incx) * (n / 2 - 1) + 1] = (float)0.;
		}

		i__3 = *nalf;
		for (ia = 1; ia <= i__3; ++ia) {
		    alpha = alf[ia];
		    null = n <= 0 || alpha == (float)0.;

/*                 Generate the matrix A. */

		    transl = (float)0.;
		    i__4 = n - 1;
		    i__5 = n - 1;
		    smake_(sname + 7, uplo, " ", &n, &n, &a[a_offset], nmax, &
			    aa[1], &lda, &i__4, &i__5, &reset, &transl, (
			    ftnlen)2, (ftnlen)1, (ftnlen)1);

		    ++nc;

/*                 Save every datum before calling the subroutine. */

		    *(unsigned char *)uplos = *(unsigned char *)uplo;
		    ns = n;
		    als = alpha;
		    i__4 = laa;
		    for (i__ = 1; i__ <= i__4; ++i__) {
			as[i__] = aa[i__];
/* L10: */
		    }
		    ldas = lda;
		    i__4 = lx;
		    for (i__ = 1; i__ <= i__4; ++i__) {
			xs[i__] = xx[i__];
/* L20: */
		    }
		    incxs = incx;

/*                 Call the subroutine. */

		    if (full) {
			if (*trace) {
/*
			    sprintf(ntra,"%6d: %12s (%14s, %3d, %4.1f, X, %2d, A, %3d).\n",
			    nc,sname,cuplo,alpha,incx,lda);
*/
			}
			if (*rewi) {
/*			    al__1.aerr = 0;
			    al__1.aunit = *ntra;
			    f_rew(&al__1);*/
			}
			cssyr_(iorder, uplo, &n, &alpha, &xx[1], &incx, &aa[1]
				, &lda, (ftnlen)1);
		    } else if (packed) {
			if (*trace) {
/*
			    sprintf(ntra,"%6d: %12s (%14s, %3d, %4.1f, X, %2d, AP).\n",
			    nc,sname,cuplo,n,alpha,incx);
*/
			}
			if (*rewi) {
/*			    al__1.aerr = 0;
			    al__1.aunit = *ntra;
			    f_rew(&al__1);*/
			}
			csspr_(iorder, uplo, &n, &alpha, &xx[1], &incx, &aa[1]
				, (ftnlen)1);
		    }

/*                 Check if error-exit was taken incorrectly. */

		    if (! infoc_1.ok) {
			printf(" ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *******n");
			*fatal = TRUE_;
			goto L120;
		    }

/*                 See what data changed inside subroutines. */

		    isame[0] = *(unsigned char *)uplo == *(unsigned char *)
			    uplos;
		    isame[1] = ns == n;
		    isame[2] = als == alpha;
		    isame[3] = lse_(&xs[1], &xx[1], &lx);
		    isame[4] = incxs == incx;
		    if (null) {
			isame[5] = lse_(&as[1], &aa[1], &laa);
		    } else {
			isame[5] = lseres_(sname + 7, uplo, &n, &n, &as[1], &
				aa[1], &lda, (ftnlen)2, (ftnlen)1);
		    }
		    if (! packed) {
			isame[6] = ldas == lda;
		    }

/*                 If data was incorrectly changed, report and return. */

		    same = TRUE_;
		    i__4 = nargs;
		    for (i__ = 1; i__ <= i__4; ++i__) {
			same = same && isame[i__ - 1];
			if (! isame[i__ - 1]) {
			    printf(" ******* FATAL ERROR - PARAMETER NUMBER %2d WAS CHANGED INCORRECTLY *******\n",i__);
			}
/* L30: */
		    }
		    if (! same) {
			*fatal = TRUE_;
			goto L120;
		    }

		    if (! null) {

/*                    Check the result column by column. */

			if (incx > 0) {
			    i__4 = n;
			    for (i__ = 1; i__ <= i__4; ++i__) {
				z__[i__] = x[i__];
/* L40: */
			    }
			} else {
			    i__4 = n;
			    for (i__ = 1; i__ <= i__4; ++i__) {
				z__[i__] = x[n - i__ + 1];
/* L50: */
			    }
			}
			ja = 1;
			i__4 = n;
			for (j = 1; j <= i__4; ++j) {
			    w[0] = z__[j];
			    if (upper) {
				jj = 1;
				lj = j;
			    } else {
				jj = j;
				lj = n - j + 1;
			    }
			    smvch_("N", &lj, &c__1, &alpha, &z__[jj], &lj, w, 
				    &c__1, &c_b123, &a[jj + j * a_dim1], &
				    c__1, &yt[1], &g[1], &aa[ja], eps, &err, 
				    fatal, nout, &c_true, (ftnlen)1);
			    if (full) {
				if (upper) {
				    ja += lda;
				} else {
				    ja = ja + lda + 1;
				}
			    } else {
				ja += lj;
			    }
			    errmax = dmax(errmax,err);
/*                       If got really bad answer, report and return. */
			    if (*fatal) {
				goto L110;
			    }
/* L60: */
			}
		    } else {
/*                    Avoid repeating tests if N.le.0. */
			if (n <= 0) {
			    goto L100;
			}
		    }

/* L70: */
		}

/* L80: */
	    }

/* L90: */
	}

L100:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
	    printf("%12s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
	    printf("%12s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
	    printf("%12s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
	if (*iorder == 1) {
	    printf("%12s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
    }
    goto L130;

L110:
    printf("      THESE ARE THE RESULTS FOR COLUMN %3d:\n",j);

L120:
    printf("******* %12s FAILED ON CALL NUMBER:\n",sname);
    if (full) {
	printf("%6d: %12s (%14s, %3d, %4.1f, X, %2d, A, %3d).\n",
			    nc,sname,cuplo,n,alpha,incx,lda);
    } else if (packed) {
	printf("%6d: %12s (%14s, %3d, %4.1f, X, %2d, AP).\n",
			    nc,sname,cuplo,n,alpha,incx);
    }

L130:
    return 0;

/* 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C', */
/*     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2, */
/*     $      ' - SUSPECT *******' ) */

/*     End of SCHK5. */

} /* schk5_ */

/* Subroutine */ int schk6_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* ninc, integer* inc, integer* nmax, integer* incmax, real* a, real* aa, real* as, real* x, real* xx, real* xs, real* y, real* yy, real* ys, real* yt, real* g, real* z__, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ich[2+1] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, z_dim1, z_offset, i__1, i__2, i__3, i__4, i__5, 
	    i__6;

    /* Local variables */
    static integer ldas;
    static logical same;
    static integer incx, incy;
    static logical full, null;
    static char uplo[1];
    static integer i__, j, n;
    static real alpha, w[2];
    static logical isame[13];
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, integer*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    static integer nargs;
    extern /* Subroutine */ int smvch_(char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, real*, real*, logical*, integer*, logical*, ftnlen);
    static logical reset;
    static char cuplo[14];
    static integer incxs, incys;
    static logical upper;
    static char uplos[1];
    static integer ia, ja, ic;
    extern /* Subroutine */ void csspr2_(integer*, char*, integer*, real*, real*, integer*, real*, integer*, real*, ftnlen);
    static integer nc, jj, lj, in;
    static logical packed;
    extern /* Subroutine */ void cssyr2_(integer*, char*, integer*, real*, real*, integer*, real*, integer*, real*, integer*, ftnlen);
    static integer ix, iy, ns, lx, ly;
    static real errmax;
    extern logical lseres_(char* type__, char* uplo, integer* m, integer* n, real* aa, real* as, integer* lda, ftnlen ltype_len, ftnlen uplo_len);
    static real transl;
    static integer laa, lda;
    static real als;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests SSYR2 and SSPR2. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Local Arrays .. */
/*     .. External Functions .. */
/*     .. External Subroutines .. */
/*     .. Intrinsic Functions .. */
/*     .. Scalars in Common .. */
/*     .. Common blocks .. */
/*     .. Data statements .. */
    /* Parameter adjustments */
    --idim;
    --alf;
    --inc;
    z_dim1 = *nmax;
    z_offset = 1 + z_dim1 * 1;
    z__ -= z_offset;
    --g;
    --yt;
    --y;
    --x;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --ys;
    --yy;
    --xs;
    --xx;

    /* Function Body */
/*     .. Executable Statements .. */
    full = *(unsigned char *)&sname[8] == 'y';
    packed = *(unsigned char *)&sname[8] == 'p';
/*     Define the number of arguments. */
    if (full) {
	nargs = 9;
    } else if (packed) {
	nargs = 8;
    }

    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDA to 1 more than minimum value if room. */
	lda = n;
	if (lda < *nmax) {
	    ++lda;
	}
/*        Skip tests if not enough room. */
	if (lda > *nmax) {
	    goto L140;
	}
	if (packed) {
	    laa = n * (n + 1) / 2;
	} else {
	    laa = lda * n;
	}

	for (ic = 1; ic <= 2; ++ic) {
	    *(unsigned char *)uplo = *(unsigned char *)&ich[ic - 1];
	    if (*(unsigned char *)uplo == 'U') {
		s_copy(cuplo, "    CblasUpper", (ftnlen)14, (ftnlen)14);
	    } else {
		s_copy(cuplo, "    CblasLower", (ftnlen)14, (ftnlen)14);
	    }
	    upper = *(unsigned char *)uplo == 'U';

	    i__2 = *ninc;
	    for (ix = 1; ix <= i__2; ++ix) {
		incx = inc[ix];
		lx = abs(incx) * n;

/*              Generate the vector X. */

		transl = (float).5;
		i__3 = abs(incx);
		i__4 = n - 1;
		smake_("ge", " ", " ", &c__1, &n, &x[1], &c__1, &xx[1], &i__3,
			 &c__0, &i__4, &reset, &transl, (ftnlen)2, (ftnlen)1, 
			(ftnlen)1);
		if (n > 1) {
		    x[n / 2] = (float)0.;
		    xx[abs(incx) * (n / 2 - 1) + 1] = (float)0.;
		}

		i__3 = *ninc;
		for (iy = 1; iy <= i__3; ++iy) {
		    incy = inc[iy];
		    ly = abs(incy) * n;

/*                 Generate the vector Y. */

		    transl = (float)0.;
		    i__4 = abs(incy);
		    i__5 = n - 1;
		    smake_("ge", " ", " ", &c__1, &n, &y[1], &c__1, &yy[1], &
			    i__4, &c__0, &i__5, &reset, &transl, (ftnlen)2, (
			    ftnlen)1, (ftnlen)1);
		    if (n > 1) {
			y[n / 2] = (float)0.;
			yy[abs(incy) * (n / 2 - 1) + 1] = (float)0.;
		    }

		    i__4 = *nalf;
		    for (ia = 1; ia <= i__4; ++ia) {
			alpha = alf[ia];
			null = n <= 0 || alpha == (float)0.;

/*                    Generate the matrix A. */

			transl = (float)0.;
			i__5 = n - 1;
			i__6 = n - 1;
			smake_(sname + 7, uplo, " ", &n, &n, &a[a_offset], 
				nmax, &aa[1], &lda, &i__5, &i__6, &reset, &
				transl, (ftnlen)2, (ftnlen)1, (ftnlen)1);

			++nc;

/*                    Save every datum before calling the subroutine. */

			*(unsigned char *)uplos = *(unsigned char *)uplo;
			ns = n;
			als = alpha;
			i__5 = laa;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    as[i__] = aa[i__];
/* L10: */
			}
			ldas = lda;
			i__5 = lx;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    xs[i__] = xx[i__];
/* L20: */
			}
			incxs = incx;
			i__5 = ly;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    ys[i__] = yy[i__];
/* L30: */
			}
			incys = incy;

/*                    Call the subroutine. */

			if (full) {
			    if (*trace) {
/*
				sprintf(ntra,"%6d: %12s (%14s, %3d, %4.1f, X, %2d, Y, %2d, A, %3d).\n",
				nc,sname,cuplo,n,alpha,incx,incy,lda);
*/
			    }
			    if (*rewi) {
/*				al__1.aerr = 0;
				al__1.aunit = *ntra;
				f_rew(&al__1);*/
			    }
			    cssyr2_(iorder, uplo, &n, &alpha, &xx[1], &incx, &
				    yy[1], &incy, &aa[1], &lda, (ftnlen)1);
			} else if (packed) {
			    if (*trace) {
/*
				sprintf(ntra,"%6d: %12s (%14s, %3d, %4.1f, X, %2d, Y, %2d, AP).\n",
				nc,sname,cuplo,n,alpha,incx,incy);
*/
			    }
			    if (*rewi) {
/*				al__1.aerr = 0;
				al__1.aunit = *ntra;
				f_rew(&al__1);*/
			    }
			    csspr2_(iorder, uplo, &n, &alpha, &xx[1], &incx, &
				    yy[1], &incy, &aa[1], (ftnlen)1);
			}

/*                    Check if error-exit was taken incorrectly. */

			if (! infoc_1.ok) {
			    printf(" ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *******n");
			    *fatal = TRUE_;
			    goto L160;
			}

/*                    See what data changed inside subroutines. */

			isame[0] = *(unsigned char *)uplo == *(unsigned char *
				)uplos;
			isame[1] = ns == n;
			isame[2] = als == alpha;
			isame[3] = lse_(&xs[1], &xx[1], &lx);
			isame[4] = incxs == incx;
			isame[5] = lse_(&ys[1], &yy[1], &ly);
			isame[6] = incys == incy;
			if (null) {
			    isame[7] = lse_(&as[1], &aa[1], &laa);
			} else {
			    isame[7] = lseres_(sname + 7, uplo, &n, &n, &as[1]
				    , &aa[1], &lda, (ftnlen)2, (ftnlen)1);
			}
			if (! packed) {
			    isame[8] = ldas == lda;
			}

/*                    If data was incorrectly changed, report and return. */

			same = TRUE_;
			i__5 = nargs;
			for (i__ = 1; i__ <= i__5; ++i__) {
			    same = same && isame[i__ - 1];
			    if (! isame[i__ - 1]) {
			        printf(" ******* FATAL ERROR - PARAMETER NUMBER %2d WAS CHANGED INCORRECTLY *******\n",i__);
			    }
/* L40: */
			}
			if (! same) {
			    *fatal = TRUE_;
			    goto L160;
			}

			if (! null) {

/*                       Check the result column by column. */

			    if (incx > 0) {
				i__5 = n;
				for (i__ = 1; i__ <= i__5; ++i__) {
				    z__[i__ + z_dim1] = x[i__];
/* L50: */
				}
			    } else {
				i__5 = n;
				for (i__ = 1; i__ <= i__5; ++i__) {
				    z__[i__ + z_dim1] = x[n - i__ + 1];
/* L60: */
				}
			    }
			    if (incy > 0) {
				i__5 = n;
				for (i__ = 1; i__ <= i__5; ++i__) {
				    z__[i__ + (z_dim1 << 1)] = y[i__];
/* L70: */
				}
			    } else {
				i__5 = n;
				for (i__ = 1; i__ <= i__5; ++i__) {
				    z__[i__ + (z_dim1 << 1)] = y[n - i__ + 1];
/* L80: */
				}
			    }
			    ja = 1;
			    i__5 = n;
			    for (j = 1; j <= i__5; ++j) {
				w[0] = z__[j + (z_dim1 << 1)];
				w[1] = z__[j + z_dim1];
				if (upper) {
				    jj = 1;
				    lj = j;
				} else {
				    jj = j;
				    lj = n - j + 1;
				}
				smvch_("N", &lj, &c__2, &alpha, &z__[jj + 
					z_dim1], nmax, w, &c__1, &c_b123, &a[
					jj + j * a_dim1], &c__1, &yt[1], &g[1]
					, &aa[ja], eps, &err, fatal, nout, &
					c_true, (ftnlen)1);
				if (full) {
				    if (upper) {
					ja += lda;
				    } else {
					ja = ja + lda + 1;
				    }
				} else {
				    ja += lj;
				}
				errmax = dmax(errmax,err);
/*                          If got really bad answer, report and return. */
				if (*fatal) {
				    goto L150;
				}
/* L90: */
			    }
			} else {
/*                       Avoid repeating tests with N.le.0. */
			    if (n <= 0) {
				goto L140;
			    }
			}

/* L100: */
		    }

/* L110: */
		}

/* L120: */
	    }

/* L130: */
	}

L140:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
	    printf("%12s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
	    printf("%12s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
	    printf("%12s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
	if (*iorder == 1) {
	    printf("%12s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%6d CALLS) ******* BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******\n",sname,nc,errmax);;
	}
    }
    goto L170;

L150:
    printf("      THESE ARE THE RESULTS FOR COLUMN %3d:\n",j);

L160:
    printf("******* %12s FAILED ON CALL NUMBER:\n",sname);
    if (full) {
	printf("%6d: %12s (%14s, %3d, %4.1f, X, %2d, Y, %2d, A, %3d).\n",
				nc,sname,cuplo,n,alpha,incx,incy,lda);
    } else if (packed) {
	printf("%6d: %12s (%14s, %3d, %4.1f, X, %2d, Y, %2d, AP).\n",
				nc,sname,cuplo,n,alpha,incx,incy);
    }

L170:
    return 0;

/* 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C', */
/*     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2, */
/*     $      ' - SUSPECT *******' ) */

/*     End of SCHK6. */

} /* schk6_ */

/* Subroutine */ int smake_(char* type__, char* uplo, char* diag, integer* m, integer* n, real* a, integer* nmax, real* aa, integer* lda, integer* kl, integer* ku, logical* reset, real* transl, ftnlen type_len, ftnlen uplo_len, ftnlen diag_len)
 {
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2, i__3, i__4;

    /* Local variables */
    static integer ibeg, iend;
    extern doublereal sbeg_(logical*);
    static integer ioff;
    static logical unit;
    static integer i__, j;
    static logical lower;
    static integer i1, i2, i3;
    static logical upper;
    static integer kk;
    static logical gen, tri, sym;


/*  Generates values for an M by N matrix A within the bandwidth */
/*  defined by KL and KU. */
/*  Stores the values in the array AA in the data structure required */
/*  by the routine, with unwanted elements set to rogue value. */

/*  TYPE is 'ge', 'gb', 'sy', 'sb', 'sp', 'tr', 'tb' OR 'tp'. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. External Functions .. */
/*     .. Intrinsic Functions .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --aa;

    /* Function Body */
    gen = *(unsigned char *)type__ == 'g';
    sym = *(unsigned char *)type__ == 's';
    tri = *(unsigned char *)type__ == 't';
    upper = (sym || tri) && *(unsigned char *)uplo == 'U';
    lower = (sym || tri) && *(unsigned char *)uplo == 'L';
    unit = tri && *(unsigned char *)diag == 'U';

/*     Generate data in array A. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    if (gen || (upper && i__ <= j) || (lower && i__ >= j)) {
		if (i__ <= j && (j - i__ <= *ku || i__ >= j && i__ - j <= *kl)) 
			{
		    a[i__ + j * a_dim1] = sbeg_(reset) + *transl;
		} else {
		    a[i__ + j * a_dim1] = (float)0.;
		}
		if (i__ != j) {
		    if (sym) {
			a[j + i__ * a_dim1] = a[i__ + j * a_dim1];
		    } else if (tri) {
			a[j + i__ * a_dim1] = (float)0.;
		    }
		}
	    }
/* L10: */
	}
	if (tri) {
	    a[j + j * a_dim1] += (float)1.;
	}
	if (unit) {
	    a[j + j * a_dim1] = (float)1.;
	}
/* L20: */
    }

/*     Store elements in array AS in data structure required by routine. */

    if (s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *m;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = a[i__ + j * a_dim1];
/* L30: */
	    }
	    i__2 = *lda;
	    for (i__ = *m + 1; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = (float)-1e10;
/* L40: */
	    }
/* L50: */
	}
    } else if (s_cmp(type__, "gb", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *ku + 1 - j;
	    for (i1 = 1; i1 <= i__2; ++i1) {
		aa[i1 + (j - 1) * *lda] = (float)-1e10;
/* L60: */
	    }
/* Computing MIN */
	    i__3 = *kl + *ku + 1, i__4 = *ku + 1 + *m - j;
	    i__2 = f2cmin(i__3,i__4);
	    for (i2 = i1; i2 <= i__2; ++i2) {
		aa[i2 + (j - 1) * *lda] = a[i2 + j - *ku - 1 + j * a_dim1];
/* L70: */
	    }
	    i__2 = *lda;
	    for (i3 = i2; i3 <= i__2; ++i3) {
		aa[i3 + (j - 1) * *lda] = (float)-1e10;
/* L80: */
	    }
/* L90: */
	}
    } else if (s_cmp(type__, "sy", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "tr", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		ibeg = 1;
		if (unit) {
		    iend = j - 1;
		} else {
		    iend = j;
		}
	    } else {
		if (unit) {
		    ibeg = j + 1;
		} else {
		    ibeg = j;
		}
		iend = *n;
	    }
	    i__2 = ibeg - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = (float)-1e10;
/* L100: */
	    }
	    i__2 = iend;
	    for (i__ = ibeg; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = a[i__ + j * a_dim1];
/* L110: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = (float)-1e10;
/* L120: */
	    }
/* L130: */
	}
    } else if (s_cmp(type__, "sb", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "tb", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		kk = *kl + 1;
/* Computing MAX */
		i__2 = 1, i__3 = *kl + 2 - j;
		ibeg = f2cmax(i__2,i__3);
		if (unit) {
		    iend = *kl;
		} else {
		    iend = *kl + 1;
		}
	    } else {
		kk = 1;
		if (unit) {
		    ibeg = 2;
		} else {
		    ibeg = 1;
		}
/* Computing MIN */
		i__2 = *kl + 1, i__3 = *m + 1 - j;
		iend = f2cmin(i__2,i__3);
	    }
	    i__2 = ibeg - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = (float)-1e10;
/* L140: */
	    }
	    i__2 = iend;
	    for (i__ = ibeg; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = a[i__ + j - kk + j * a_dim1];
/* L150: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = (float)-1e10;
/* L160: */
	    }
/* L170: */
	}
    } else if (s_cmp(type__, "sp", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "tp", (ftnlen)2, (ftnlen)2) == 0) {
	ioff = 0;
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		ibeg = 1;
		iend = j;
	    } else {
		ibeg = j;
		iend = *n;
	    }
	    i__2 = iend;
	    for (i__ = ibeg; i__ <= i__2; ++i__) {
		++ioff;
		aa[ioff] = a[i__ + j * a_dim1];
		if (i__ == j) {
		    if (unit) {
			aa[ioff] = (float)-1e10;
		    }
		}
/* L180: */
	    }
/* L190: */
	}
    }
    return 0;

/*     End of SMAKE. */

} /* smake_ */

/* Subroutine */ int smvch_(char* trans, integer* m, integer* n, real* alpha, real* a, integer* nmax, real* x, integer* incx, real* beta, real* y, integer* incy, real* yt, real* g, real* yy, real* eps, real* err, logical* fatal, integer* nout, logical* mv, ftnlen trans_len)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2;
    real r__1;

    /* Builtin functions */
    double sqrt(double);

    /* Local variables */
    static real erri;
    static logical tran;
    static integer i__, j, incxl, incyl, ml, nl, iy, jx, kx, ky;

/*  Checks the results of the computational tests. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Intrinsic Functions .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --x;
    --y;
    --yt;
    --g;
    --yy;

    /* Function Body */
    tran = *(unsigned char *)trans == 'T' || *(unsigned char *)trans == 'C';
    if (tran) {
	ml = *n;
	nl = *m;
    } else {
	ml = *m;
	nl = *n;
    }
    if (*incx < 0) {
	kx = nl;
	incxl = -1;
    } else {
	kx = 1;
	incxl = 1;
    }
    if (*incy < 0) {
	ky = ml;
	incyl = -1;
    } else {
	ky = 1;
	incyl = 1;
    }

/*     Compute expected result in YT using data in A, X and Y. */
/*     Compute gauges in G. */

    iy = ky;
    i__1 = ml;
    for (i__ = 1; i__ <= i__1; ++i__) {
	yt[iy] = (float)0.;
	g[iy] = (float)0.;
	jx = kx;
	if (tran) {
	    i__2 = nl;
	    for (j = 1; j <= i__2; ++j) {
		yt[iy] += a[j + i__ * a_dim1] * x[jx];
		g[iy] += (r__1 = a[j + i__ * a_dim1] * x[jx], dabs(r__1));
		jx += incxl;
/* L10: */
	    }
	} else {
	    i__2 = nl;
	    for (j = 1; j <= i__2; ++j) {
		yt[iy] += a[i__ + j * a_dim1] * x[jx];
		g[iy] += (r__1 = a[i__ + j * a_dim1] * x[jx], dabs(r__1));
		jx += incxl;
/* L20: */
	    }
	}
	yt[iy] = *alpha * yt[iy] + *beta * y[iy];
	g[iy] = dabs(*alpha) * g[iy] + (r__1 = *beta * y[iy], dabs(r__1));
	iy += incyl;
/* L30: */
    }

/*     Compute the error ratio for this result. */

    *err = (float)0.;
    i__1 = ml;
    for (i__ = 1; i__ <= i__1; ++i__) {
	erri = (r__1 = yt[i__] - yy[(i__ - 1) * abs(*incy) + 1], dabs(r__1)) /
		 *eps;
	if (g[i__] != (float)0.) {
	    erri /= g[i__];
	}
	*err = dmax(*err,erri);
	if (*err * sqrt(*eps) >= (float)1.) {
	    goto L50;
	}
/* L40: */
    }
/*     If the loop completes, all results are at least half accurate. */
    goto L70;

/*     Report fatal error. */

L50:
    *fatal = TRUE_;
    printf(" ******* FATAL ERROR - COMPUTED RESULT IS LESS THAN HALF ACCURATE *******\n           EXPECTED RESULT   COMPUTED RESULT\n");
    i__1 = ml;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (*mv) {
	    printf("%7d %18.6g %18.6g\n",i__,yt[i__],yy[(i__ - 1) * abs(*incy) + 1]);
	} else {
	    printf("%7d %18.6g %18.6g\n",i__,yy[(i__ - 1) * abs(*incy) + 1], yt[i__]);
	}
/* L60: */
    }

L70:
    return 0;


/*     End of SMVCH. */

} /* smvch_ */

logical lse_(real* ri, real* rj, integer* lr)
{
    /* System generated locals */
    integer i__1;
    logical ret_val;

    /* Local variables */
    static integer i__;


/*  Tests if two arrays are identical. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    --rj;
    --ri;

    /* Function Body */
    i__1 = *lr;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (ri[i__] != rj[i__]) {
	    goto L20;
	}
/* L10: */
    }
    ret_val = TRUE_;
    goto L30;
L20:
    ret_val = FALSE_;
L30:
    return ret_val;

/*     End of LSE. */

} /* lse_ */

logical lseres_(char* type__, char* uplo, integer* m, integer* n, real* aa, real* as, integer* lda, ftnlen ltype_len, ftnlen uplo_len)
{
    /* System generated locals */
    integer aa_dim1, aa_offset, as_dim1, as_offset, i__1, i__2;
    logical ret_val;

    /* Local variables */
    static integer ibeg, iend, i__, j;
    static logical upper;


/*  Tests if selected elements in two arrays are equal. */

/*  TYPE is 'ge', 'sy' or 'sp'. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    as_dim1 = *lda;
    as_offset = 1 + as_dim1 * 1;
    as -= as_offset;
    aa_dim1 = *lda;
    aa_offset = 1 + aa_dim1 * 1;
    aa -= aa_offset;

    /* Function Body */
    upper = *(unsigned char *)uplo == 'U';
    if (s_cmp(type__, "ge", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    i__2 = *lda;
	    for (i__ = *m + 1; i__ <= i__2; ++i__) {
		if (aa[i__ + j * aa_dim1] != as[i__ + j * as_dim1]) {
		    goto L70;
		}
/* L10: */
	    }
/* L20: */
	}
    } else if (s_cmp(type__, "sy", (ftnlen)2, (ftnlen)2) == 0) {
	i__1 = *n;
	for (j = 1; j <= i__1; ++j) {
	    if (upper) {
		ibeg = 1;
		iend = j;
	    } else {
		ibeg = j;
		iend = *n;
	    }
	    i__2 = ibeg - 1;
	    for (i__ = 1; i__ <= i__2; ++i__) {
		if (aa[i__ + j * aa_dim1] != as[i__ + j * as_dim1]) {
		    goto L70;
		}
/* L30: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		if (aa[i__ + j * aa_dim1] != as[i__ + j * as_dim1]) {
		    goto L70;
		}
/* L40: */
	    }
/* L50: */
	}
    }

/*   60 CONTINUE */
    ret_val = TRUE_;
    goto L80;
L70:
    ret_val = FALSE_;
L80:
    return ret_val;

/*     End of LSERES. */

} /* lseres_ */

doublereal sbeg_(logical* reset)
{
    /* System generated locals */
    real ret_val;

    /* Local variables */
    static integer i__, ic, mi;


/*  Generates random numbers uniformly distributed between -0.5 and 0.5. */

/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */
/*     Jeremy Du Croz, NAG Central Office. */

/*     .. Scalar Arguments .. */
/*     .. Local Scalars .. */
/*     .. Save statement .. */
/*     .. Intrinsic Functions .. */
/*     .. Executable Statements .. */
    if (*reset) {
/*        Initialize local variables. */
	mi = 891;
	i__ = 7;
	ic = 0;
	*reset = FALSE_;
    }

/*     The sequence of values of I is bounded between 1 and 999. */
/*     If initial I = 1,2,3,6,7 or 9, the period will be 50. */
/*     If initial I = 4 or 8, the period will be 25. */
/*     If initial I = 5, the period will be 10. */
/*     IC is used to break up the period by skipping 1 value of I in 6. */

    ++ic;
L10:
    i__ *= mi;
    i__ -= i__ / 1000 * 1000;
    if (ic >= 5) {
	ic = 0;
	goto L10;
    }
    ret_val = (real) (i__ - 500) / (float)1001.;
    return ret_val;

/*     End of SBEG. */

} /* sbeg_ */

doublereal sdiff_(real* x, real* y)
{
    /* System generated locals */
    real ret_val;


/*  Auxiliary routine for test program for Level 2 Blas. */

/*  -- Written on 10-August-1987. */
/*     Richard Hanson, Sandia National Labs. */

/*     .. Scalar Arguments .. */
/*     .. Executable Statements .. */
    ret_val = *x - *y;
    return ret_val;

/*     End of SDIFF. */

} /* sdiff_ */

