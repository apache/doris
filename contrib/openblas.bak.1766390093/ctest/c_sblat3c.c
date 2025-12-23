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
static real c_b89 = (float)1.;
static real c_b103 = (float)0.;
static integer c__6 = 6;
static logical c_true = TRUE_;
static integer c__0 = 0;
static logical c_false = FALSE_;

/* Main program  MAIN__() */ int main(void)
{
    /* Initialized data */

    static char snames[6][13] = {"cblas_sgemm ", "cblas_ssymm ", "cblas_strmm ", "cblas_strsm ", "cblas_ssyrk ", "cblas_ssyr2k"};

    /* System generated locals */
    integer i__1, i__2, i__3;
    real r__1;

    /* Local variables */
    static integer nalf, idim[9];
    static logical same;
    static integer nbet, ntra;
    static logical rewi;
    extern /* Subroutine */ int schk1_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, real*, integer*, real*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
    extern /* Subroutine */ int schk2_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, real*, integer*, real*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
    extern /* Subroutine */ int schk3_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, real*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
    extern /* Subroutine */ int schk4_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, real*, integer*, real*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
    extern /* Subroutine */ int schk5_(char*, real*, real*, integer*, integer*, logical*, logical*, logical*, integer*, integer*, integer*, real*, integer*, real*, integer*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, real*, integer*, ftnlen);
    static real c__[4225]	/* was [65][65] */, g[65];
    static integer i__, j, n;
    static logical fatal;
    static real w[130];
    extern doublereal sdiff_(real*, real*);
    static logical trace;
    static integer nidim;
    extern /* Subroutine */ int smmch_(char*, char*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, integer*, real*, real*, logical*, integer*, logical*, ftnlen, ftnlen);
    static char snaps[32];
    static integer isnum;
    static logical ltest[6];
    static real aa[4225], ab[8450]	/* was [65][130] */, bb[4225], cc[
	    4225], as[4225], bs[4225], cs[4225], ct[65];
    static logical sfatal, corder;
    static char snamet[12], transa[1], transb[1];
    static real thresh;
    static logical rorder;
    static integer layout;
    static logical ltestt, tsterr;
    extern /* Subroutine */ void cs3chke_(char*, ftnlen);
    static real alf[7], bet[7];
    extern logical lse_(real*, real*, integer*);
    static real eps, err;
    char tmpchar;

/*  Test program for the REAL             Level 3 Blas. */

/*  The program must be driven by a short data file. The first 13 records */
/*  of the file are read using list-directed input, the last 6 records */
/*  are read using the format ( A12, L2 ). An annotated example of a data */
/*  file can be obtained by deleting the first 3 characters from the */
/*  following 19 lines: */
/*  'SBLAT3.SNAP'     NAME OF SNAPSHOT OUTPUT FILE */
/*  -1                UNIT NUMBER OF SNAPSHOT FILE (NOT USED IF .LT. 0) */
/*  F        LOGICAL FLAG, T TO REWIND SNAPSHOT FILE AFTER EACH RECORD. */
/*  F        LOGICAL FLAG, T TO STOP ON FAILURES. */
/*  T        LOGICAL FLAG, T TO TEST ERROR EXITS. */
/*  2        0 TO TEST COLUMN-MAJOR, 1 TO TEST ROW-MAJOR, 2 TO TEST BOTH */
/*  16.0     THRESHOLD VALUE OF TEST RATIO */
/*  6                 NUMBER OF VALUES OF N */
/*  0 1 2 3 5 9       VALUES OF N */
/*  3                 NUMBER OF VALUES OF ALPHA */
/*  0.0 1.0 0.7       VALUES OF ALPHA */
/*  3                 NUMBER OF VALUES OF BETA */
/*  0.0 1.0 1.3       VALUES OF BETA */
/*  cblas_sgemm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ssymm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_strmm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_strsm  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ssyrk  T PUT F FOR NO TEST. SAME COLUMNS. */
/*  cblas_ssyr2k T PUT F FOR NO TEST. SAME COLUMNS. */

/*  See: */

/*     Dongarra J. J., Du Croz J. J., Duff I. S. and Hammarling S. */
/*     A Set of Level 3 Basic Linear Algebra Subprograms. */

/*     Technical Memorandum No.88 (Revision 1), Mathematics and */
/*     Computer Science Division, Argonne National Laboratory, 9700 */
/*     South Cass Avenue, Argonne, Illinois 60439, US. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
/*     Read name and unit number for summary output file and open file. */

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
/*         OPEN( NTRA, FILE = SNAPS, STATUS = 'NEW' ) */
/*	o__1.ounit = ntra;
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
/*     Values of ALPHA */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nalf);
#else
   sscanf(line,"%d",&nalf);
#endif
    if (nalf < 1 || nalf > 7) {
        fprintf(stderr,"VALUE OF ALPHA IS LESS THAN 0 OR GREATER THAN 7\n");
        goto L220;
    }
   fgets(line,80,stdin);
   sscanf(line,"%f %f %f %f %f %f %f",&alf[0],&alf[1],&alf[2],&alf[3],&alf[4],&alf[5],&alf[6]);

/*     Values of BETA */
   fgets(line,80,stdin);
#ifdef USE64BITINT
   sscanf(line,"%ld",&nbet);
#else
   sscanf(line,"%d",&nbet);
#endif
    if (nalf < 1 || nbet > 7) {
        fprintf(stderr,"VALUE OF BETA IS LESS THAN 0 OR GREATER THAN 7\n");
        goto L220;
    }
   fgets(line,80,stdin);
   sscanf(line,"%f %f %f %f %f %f %f",&bet[0],&bet[1],&bet[2],&bet[3],&bet[4],&bet[5],&bet[6]);

/*     Report values of parameters. */
    printf("TESTS OF THE REAL      LEVEL 3 BLAS\nTHE FOLLOWING PARAMETER VALUES WILL BE USED:\n");
    printf(" FOR N");
    for (i__ =1; i__ <=nidim;++i__) printf(" %d",idim[i__-1]);
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

    for (i__ = 1; i__ <= 6; ++i__) {
	ltest[i__ - 1] = FALSE_;
/* L20: */
    }
L30:
   if (! fgets(line,80,stdin)) {
        goto L60;
    }
   i__1 = sscanf(line,"%12c %c",snamet,&tmpchar);
   ltestt=FALSE_;
   if (tmpchar=='T')ltestt=TRUE_;
    if (i__1 < 2) {
        goto L60;
    }
    for (i__ = 1; i__ <= 9; ++i__) {
        if (s_cmp(snamet, snames[i__ - 1] , (ftnlen)12, (ftnlen)12) == 
                0) {
            goto L50;
        }
/* L40: */
    }
    printf("SUBPROGRAM NAME %s NOT RECOGNIZED\n****** TESTS ABANDONED ******\n",snamet);
    exit(1);

L50:
    ltest[i__ - 1] = ltestt;
    goto L30;

L60:
//    f_clos(&cl__1);

/*     Compute EPS (the machine precision). */

    eps = (float)1.;
L70:
    r__1 = eps + (float)1.;
    if (sdiff_(&r__1, &c_b89) == (float)0.) {
	goto L80;
    }
    eps *= (float).5;
    goto L70;
L80:
    eps += eps;
    printf("RELATIVE MACHINE PRECISION IS TAKEN TO BE %9.1g\n",eps);

/*     Check the reliability of SMMCH using exact data. */

    n = 32;
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = n;
	for (i__ = 1; i__ <= i__2; ++i__) {
/* Computing MAX */
	    i__3 = i__ - j + 1;
	    ab[i__ + j * 65 - 66] = (real) f2cmax(i__3,0);
/* L90: */
	}
	ab[j + 4224] = (real) j;
	ab[(j + 65) * 65 - 65] = (real) j;
	c__[j - 1] = (float)0.;
/* L100: */
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	cc[j - 1] = (real) (j * ((j + 1) * j) / 2 - (j + 1) * j * (j - 1) / 3)
		;
/* L110: */
    }
/*     CC holds the exact result. On exit from SMMCH CT holds */
/*     the result computed by SMMCH. */
    *(unsigned char *)transa = 'N';
    *(unsigned char *)transb = 'N';
    smmch_(transa, transb, &n, &c__1, &n, &c_b89, ab, &c__65, &ab[4225], &
	    c__65, &c_b103, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &
	    fatal, &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lse_(cc, ct, &n);
    if (! same || err != (float)0.) {
      printf("ERROR IN SMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("SMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    *(unsigned char *)transb = 'T';
    smmch_(transa, transb, &n, &c__1, &n, &c_b89, ab, &c__65, &ab[4225], &
	    c__65, &c_b103, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &
	    fatal, &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lse_(cc, ct, &n);
    if (! same || err != (float)0.) {
      printf("ERROR IN SMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("SMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	ab[j + 4224] = (real) (n - j + 1);
	ab[(j + 65) * 65 - 65] = (real) (n - j + 1);
/* L120: */
    }
    i__1 = n;
    for (j = 1; j <= i__1; ++j) {
	cc[n - j] = (real) (j * ((j + 1) * j) / 2 - (j + 1) * j * (j - 1) / 3)
		;
/* L130: */
    }
    *(unsigned char *)transa = 'T';
    *(unsigned char *)transb = 'N';
    smmch_(transa, transb, &n, &c__1, &n, &c_b89, ab, &c__65, &ab[4225], &
	    c__65, &c_b103, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &
	    fatal, &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lse_(cc, ct, &n);
    if (! same || err != (float)0.) {
      printf("ERROR IN SMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("SMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }
    *(unsigned char *)transb = 'T';
    smmch_(transa, transb, &n, &c__1, &n, &c_b89, ab, &c__65, &ab[4225], &
	    c__65, &c_b103, c__, &c__65, ct, g, cc, &c__65, &eps, &err, &
	    fatal, &c__6, &c_true, (ftnlen)1, (ftnlen)1);
    same = lse_(cc, ct, &n);
    if (! same || err != (float)0.) {
      printf("ERROR IN SMMCH - IN-LINE DOT PRODUCTS ARE BEING EVALUATED WRONGLY\n");
      printf("SMMCH WAS CALLED WITH TRANSA = %s AND TRANSB = %s\n", transa,transb);
      printf("AND RETURNED SAME = %c AND ERR = %12.3f.\n",(same==FALSE_? 'F':'T'),err);
      printf("THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.\n");
      printf("****** TESTS ABANDONED ******\n");
      exit(1);
    }

/*     Test each subroutine in turn. */

    for (isnum = 1; isnum <= 6; ++isnum) {
	if (! ltest[isnum - 1]) {
/*           Subprogram is not to be tested. */
           printf("%12s WAS NOT TESTED\n",snames[isnum-1]);
	} else {
	    s_copy(srnamc_1.srnamt, snames[isnum - 1], (ftnlen)12, (
		    ftnlen)12);
/*           Test error exits. */
	    if (tsterr) {
		cs3chke_(snames[isnum - 1], (ftnlen)12);
	    }
/*           Test computations. */
	    infoc_1.infot = 0;
	    infoc_1.ok = TRUE_;
	    fatal = FALSE_;
	    switch ((int)isnum) {
		case 1:  goto L140;
		case 2:  goto L150;
		case 3:  goto L160;
		case 4:  goto L160;
		case 5:  goto L170;
		case 6:  goto L180;
	    }
/*           Test SGEMM, 01. */
L140:
	    if (corder) {
		schk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk1_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test SSYMM, 02. */
L150:
	    if (corder) {
		schk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk2_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test STRMM, 03, STRSM, 04. */
L160:
	    if (corder) {
		schk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			c__65, ab, aa, as, &ab[4225], bb, bs, ct, g, c__, &
			c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk3_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			c__65, ab, aa, as, &ab[4225], bb, bs, ct, g, c__, &
			c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test SSYRK, 05. */
L170:
	    if (corder) {
		schk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk4_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, &ab[4225], bb, bs, c__,
			 cc, cs, ct, g, &c__1, (ftnlen)12);
	    }
	    goto L190;
/*           Test SSYR2K, 06. */
L180:
	    if (corder) {
		schk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, bb, bs, c__, cc, cs, 
			ct, g, w, &c__0, (ftnlen)12);
	    }
	    if (rorder) {
		schk5_(snames[isnum - 1], &eps, &thresh, &c__6, &ntra,
			 &trace, &rewi, &fatal, &nidim, idim, &nalf, alf, &
			nbet, bet, &c__65, ab, aa, as, bb, bs, c__, cc, cs, 
			ct, g, w, &c__1, (ftnlen)12);
	    }
	    goto L190;

L190:
	    if (fatal && sfatal) {
		goto L210;
	    }
	}
/* L200: */
    }
    printf("\nEND OF TESTS\n");
    goto L230;

L210:
    printf("\n****** FATAL ERROR - TESTS ABANDONED ******\n");
    goto L230;

L220:
    printf("AMEND DATA FILE OR INCREASE ARRAY SIZES IN PROGRAM\n");
    printf("****** TESTS ABANDONED ******\n");

L230:
    if (trace) {
//	f_clos(&cl__1);
    }
//    f_clos(&cl__1);
     exit(0);

/*     End of SBLAT3. */

} /* MAIN__ */

/* Subroutine */ int schk1_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* nbet, real* bet, integer* nmax, real* a, real* aa, real* as, real* b, real* bb, real* bs, real* c__, real* cc, real* cs, real* ct, real* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ich[3+1] = "NTC";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5, i__6;


    /* Local variables */
    static real beta;
    static integer ldas, ldbs, ldcs;
    static logical same, null;
    static integer i__, k, m, n;
    static real alpha;
    static logical isame[13];
    static logical trana, tranb;
    static integer nargs;
    static logical reset;
    extern /* Subroutine */ void sprcn1_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, integer*, real*, integer*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int smmch_(char*, char*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, integer*, real*, real*, logical*, integer*, logical*, ftnlen, ftnlen);
    static integer ia, ib, ma, mb, na, nb, nc, ik, im, in, ks, ms, ns;
    extern /* Subroutine */ void csgemm_(integer*, char*, char*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    static char tranas[1], tranbs[1], transa[1], transb[1];
    static real errmax;
    extern logical lseres_(char*, char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern logical lse_(real*, real*, integer*);
    static integer ica, icb, laa, lbb, lda, lcc, ldb, ldc;
    static real als, bls;
    static real err;

/*  Tests SGEMM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */

    nargs = 13;
    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDC to 1 more than minimum value if room. */
	    ldc = m;
	    if (ldc < *nmax) {
		++ldc;
	    }
/*           Skip tests if not enough room. */
	    if (ldc > *nmax) {
		goto L100;
	    }
	    lcc = ldc * n;
	    null = n <= 0 || m <= 0;

	    i__3 = *nidim;
	    for (ik = 1; ik <= i__3; ++ik) {
		k = idim[ik];

		for (ica = 1; ica <= 3; ++ica) {
		    *(unsigned char *)transa = *(unsigned char *)&ich[ica - 1]
			    ;
		    trana = *(unsigned char *)transa == 'T' || *(unsigned 
			    char *)transa == 'C';

		    if (trana) {
			ma = k;
			na = m;
		    } else {
			ma = m;
			na = k;
		    }
/*                 Set LDA to 1 more than minimum value if room. */
		    lda = ma;
		    if (lda < *nmax) {
			++lda;
		    }
/*                 Skip tests if not enough room. */
		    if (lda > *nmax) {
			goto L80;
		    }
		    laa = lda * na;

/*                 Generate the matrix A. */

		    smake_("GE", " ", " ", &ma, &na, &a[a_offset], nmax, &aa[
			    1], &lda, &reset, &c_b103, (ftnlen)2, (ftnlen)1, (
			    ftnlen)1);

		    for (icb = 1; icb <= 3; ++icb) {
			*(unsigned char *)transb = *(unsigned char *)&ich[icb 
				- 1];
			tranb = *(unsigned char *)transb == 'T' || *(unsigned 
				char *)transb == 'C';

			if (tranb) {
			    mb = n;
			    nb = k;
			} else {
			    mb = k;
			    nb = n;
			}
/*                    Set LDB to 1 more than minimum value if room. */
			ldb = mb;
			if (ldb < *nmax) {
			    ++ldb;
			}
/*                    Skip tests if not enough room. */
			if (ldb > *nmax) {
			    goto L70;
			}
			lbb = ldb * nb;

/*                    Generate the matrix B. */

			smake_("GE", " ", " ", &mb, &nb, &b[b_offset], nmax, &
				bb[1], &ldb, &reset, &c_b103, (ftnlen)2, (
				ftnlen)1, (ftnlen)1);

			i__4 = *nalf;
			for (ia = 1; ia <= i__4; ++ia) {
			    alpha = alf[ia];

			    i__5 = *nbet;
			    for (ib = 1; ib <= i__5; ++ib) {
				beta = bet[ib];

/*                          Generate the matrix C. */

				smake_("GE", " ", " ", &m, &n, &c__[c_offset],
					 nmax, &cc[1], &ldc, &reset, &c_b103, 
					(ftnlen)2, (ftnlen)1, (ftnlen)1);

				++nc;

/*                          Save every datum before calling the */
/*                          subroutine. */

				*(unsigned char *)tranas = *(unsigned char *)
					transa;
				*(unsigned char *)tranbs = *(unsigned char *)
					transb;
				ms = m;
				ns = n;
				ks = k;
				als = alpha;
				i__6 = laa;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    as[i__] = aa[i__];
/* L10: */
				}
				ldas = lda;
				i__6 = lbb;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    bs[i__] = bb[i__];
/* L20: */
				}
				ldbs = ldb;
				bls = beta;
				i__6 = lcc;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    cs[i__] = cc[i__];
/* L30: */
				}
				ldcs = ldc;

/*                          Call the subroutine. */

				if (*trace) {
				    sprcn1_(ntra, &nc, sname, iorder, transa, 
					    transb, &m, &n, &k, &alpha, &lda, 
					    &ldb, &beta, &ldc, (ftnlen)12, (
					    ftnlen)1, (ftnlen)1);
				}
				if (*rewi) {
//				    f_rew(&al__1);
				}
				csgemm_(iorder, transa, transb, &m, &n, &k, &
					alpha, &aa[1], &lda, &bb[1], &ldb, &
					beta, &cc[1], &ldc, (ftnlen)1, (
					ftnlen)1);

/*                          Check if error-exit was taken incorrectly. */

				if (! infoc_1.ok) {
                                    printf(" *** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				    *fatal = TRUE_;
				    goto L120;
				}

/*                          See what data changed inside subroutines. */

				isame[0] = *(unsigned char *)transa == *(
					unsigned char *)tranas;
				isame[1] = *(unsigned char *)transb == *(
					unsigned char *)tranbs;
				isame[2] = ms == m;
				isame[3] = ns == n;
				isame[4] = ks == k;
				isame[5] = als == alpha;
				isame[6] = lse_(&as[1], &aa[1], &laa);
				isame[7] = ldas == lda;
				isame[8] = lse_(&bs[1], &bb[1], &lbb);
				isame[9] = ldbs == ldb;
				isame[10] = bls == beta;
				if (null) {
				    isame[11] = lse_(&cs[1], &cc[1], &lcc);
				} else {
				    isame[11] = lseres_("GE", " ", &m, &n, &
					    cs[1], &cc[1], &ldc, (ftnlen)2, (
					    ftnlen)1);
				}
				isame[12] = ldcs == ldc;

/*                          If data was incorrectly changed, report */
/*                          and return. */

				same = TRUE_;
				i__6 = nargs;
				for (i__ = 1; i__ <= i__6; ++i__) {
				    same = same && isame[i__ - 1];
				    if (! isame[i__ - 1]) {
	                                printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				    }
/* L40: */
				}
				if (! same) {
				    *fatal = TRUE_;
				    goto L120;
				}

				if (! null) {

/*                             Check the result. */

				    smmch_(transa, transb, &m, &n, &k, &alpha,
					     &a[a_offset], nmax, &b[b_offset],
					     nmax, &beta, &c__[c_offset], 
					    nmax, &ct[1], &g[1], &cc[1], &ldc,
					     eps, &err, fatal, nout, &c_true, 
					    (ftnlen)1, (ftnlen)1);
				    errmax = dmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L120;
				    }
				}

/* L50: */
			    }

/* L60: */
			}

L70:
			;
		    }

L80:
		    ;
		}

/* L90: */
	    }

L100:
	    ;
	}

/* L110: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L130;

L120:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    sprcn1_(nout, &nc, sname, iorder, transa, transb, &m, &n, &k, &alpha, &
	    lda, &ldb, &beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);

L130:
    return 0;

/* 9995 FORMAT( 1X, I6, ': ', A12,'(''', A1, ''',''', A1, ''',', */
/*     $      3( I3, ',' ), F4.1, ', A,', I3, ', B,', I3, ',', F4.1, ', ', */
/*     $      'C,', I3, ').' ) */

/*     End of SCHK1. */

} /* schk1_ */




/* Subroutine */ void sprcn1_(integer* nout, integer* nc, char* sname, integer* iorder, char* transa, char* transb, integer* m, integer* n, integer* k, real* alpha, integer* lda, integer* ldb, real* beta, integer* ldc, ftnlen sname_len, ftnlen transa_len, ftnlen transb_len)
{

    /* Local variables */
    static char crc[14], cta[14], ctb[14];

    if (*(unsigned char *)transa == 'N') {
	s_copy(cta, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(cta, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cta, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transb == 'N') {
	s_copy(ctb, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transb == 'T') {
	s_copy(ctb, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ctb, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cta,ctb);
    printf("%d %d %d %4.1f A, %d, B, %d, %4.1f, C, %d.\n",*m,*n,*k,*alpha,*lda,*ldb,*beta,*ldc);

} /* sprcn1_ */


/* Subroutine */ int schk2_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* nbet, real* bet, integer* nmax, real* a, real* aa, real* as, real* b, real* bb, real* bs, real* c__, real* cc, real* cs, real* ct, real* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ichs[2+1] = "LR";
    static char ichu[2+1] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5;


    /* Local variables */
    static real beta;
    static integer ldas, ldbs, ldcs;
    static logical same;
    static char side[1];
    static logical left, null;
    static char uplo[1];
    static integer i__, m, n;
    static real alpha;
    static logical isame[13];
    static char sides[1];
    static integer nargs;
    static logical reset;
    static char uplos[1];
    static integer ia, ib, na, nc, im, in, ms, ns;
    static real errmax;
    extern logical lseres_(char*, char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern /* Subroutine */ void cssymm_(integer*, char*, char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern void sprcn2_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, real*, integer*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int smmch_(char*, char*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, integer*, real*, real*, logical*, integer*, logical*, ftnlen, ftnlen);
    static integer laa, lbb, lda, lcc, ldb, ldc, ics;
    static real als, bls;
    static integer icu;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests SSYMM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */

    nargs = 12;
    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDC to 1 more than minimum value if room. */
	    ldc = m;
	    if (ldc < *nmax) {
		++ldc;
	    }
/*           Skip tests if not enough room. */
	    if (ldc > *nmax) {
		goto L90;
	    }
	    lcc = ldc * n;
	    null = n <= 0 || m <= 0;

/*           Set LDB to 1 more than minimum value if room. */
	    ldb = m;
	    if (ldb < *nmax) {
		++ldb;
	    }
/*           Skip tests if not enough room. */
	    if (ldb > *nmax) {
		goto L90;
	    }
	    lbb = ldb * n;

/*           Generate the matrix B. */

	    smake_("GE", " ", " ", &m, &n, &b[b_offset], nmax, &bb[1], &ldb, &
		    reset, &c_b103, (ftnlen)2, (ftnlen)1, (ftnlen)1);

	    for (ics = 1; ics <= 2; ++ics) {
		*(unsigned char *)side = *(unsigned char *)&ichs[ics - 1];
		left = *(unsigned char *)side == 'L';

		if (left) {
		    na = m;
		} else {
		    na = n;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = na;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L80;
		}
		laa = lda * na;

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];

/*                 Generate the symmetric matrix A. */

		    smake_("SY", uplo, " ", &na, &na, &a[a_offset], nmax, &aa[
			    1], &lda, &reset, &c_b103, (ftnlen)2, (ftnlen)1, (
			    ftnlen)1);

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			alpha = alf[ia];

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    beta = bet[ib];

/*                       Generate the matrix C. */

			    smake_("GE", " ", " ", &m, &n, &c__[c_offset], 
				    nmax, &cc[1], &ldc, &reset, &c_b103, (
				    ftnlen)2, (ftnlen)1, (ftnlen)1);

			    ++nc;

/*                       Save every datum before calling the */
/*                       subroutine. */

			    *(unsigned char *)sides = *(unsigned char *)side;
			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    ms = m;
			    ns = n;
			    als = alpha;
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				as[i__] = aa[i__];
/* L10: */
			    }
			    ldas = lda;
			    i__5 = lbb;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				bs[i__] = bb[i__];
/* L20: */
			    }
			    ldbs = ldb;
			    bls = beta;
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				cs[i__] = cc[i__];
/* L30: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (*trace) {
				sprcn2_(ntra, &nc, sname, iorder, side, uplo, 
					&m, &n, &alpha, &lda, &ldb, &beta, &
					ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1)
					;
			    }
			    if (*rewi) {
//				f_rew(&al__1);
			    }
			    cssymm_(iorder, side, uplo, &m, &n, &alpha, &aa[1]
				    , &lda, &bb[1], &ldb, &beta, &cc[1], &ldc,
				     (ftnlen)1, (ftnlen)1);

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
			        printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L110;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)sides == *(unsigned 
				    char *)side;
			    isame[1] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[2] = ms == m;
			    isame[3] = ns == n;
			    isame[4] = als == alpha;
			    isame[5] = lse_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    isame[7] = lse_(&bs[1], &bb[1], &lbb);
			    isame[8] = ldbs == ldb;
			    isame[9] = bls == beta;
			    if (null) {
				isame[10] = lse_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[10] = lseres_("GE", " ", &m, &n, &cs[1],
					 &cc[1], &ldc, (ftnlen)2, (ftnlen)1);
			    }
			    isame[11] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
                                printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L40: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L110;
			    }

			    if (! null) {

/*                          Check the result. */

				if (left) {
				    smmch_("N", "N", &m, &n, &m, &alpha, &a[
					    a_offset], nmax, &b[b_offset], 
					    nmax, &beta, &c__[c_offset], nmax,
					     &ct[1], &g[1], &cc[1], &ldc, eps,
					     &err, fatal, nout, &c_true, (
					    ftnlen)1, (ftnlen)1);
				} else {
				    smmch_("N", "N", &m, &n, &n, &alpha, &b[
					    b_offset], nmax, &a[a_offset], 
					    nmax, &beta, &c__[c_offset], nmax,
					     &ct[1], &g[1], &cc[1], &ldc, eps,
					     &err, fatal, nout, &c_true, (
					    ftnlen)1, (ftnlen)1);
				}
				errmax = dmax(errmax,err);
/*                          If got really bad answer, report and */
/*                          return. */
				if (*fatal) {
				    goto L110;
				}
			    }

/* L50: */
			}

/* L60: */
		    }

/* L70: */
		}

L80:
		;
	    }

L90:
	    ;
	}

/* L100: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
        }
    }
    goto L120;

L110:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    sprcn2_(nout, &nc, sname, iorder, side, uplo, &m, &n, &alpha, &lda, &ldb, 
	    &beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);

L120:
    return 0;

/* 9995 FORMAT( 1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      F4.1, ', A,', I3, ', B,', I3, ',', F4.1, ', C,', I3, ')   ', */
/*     $      ' .' ) */

/*     End of SCHK2. */

} /* schk2_ */


/* Subroutine */ void sprcn2_(integer* nout, integer* nc, char* sname, integer* iorder, char* side, char* uplo, integer* m, integer* n, real* alpha, integer* lda, integer* ldb, real* beta, integer* ldc, ftnlen sname_len, ftnlen side_len, ftnlen uplo_len)
{

    /* Local variables */
    static char cs[14], cu[14], crc[14];

    if (*(unsigned char *)side == 'L') {
	s_copy(cs, "     CblasLeft", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cs, "    CblasRight", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cs,cu);
    printf("%d %d %4.1f A, %d, B, %d, %4.1f C, %d.\n",*m,*n,*alpha,*lda,*ldb,*beta,*ldc);
} /* sprcn2_ */


/* Subroutine */ int schk3_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* nmax, real* a, real* aa, real* as, real* b, real* bb, real* bs, real* ct, real* g, real* c__, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char ichu[2+1] = "UL";
    static char icht[3+1] = "NTC";
    static char ichd[2+1] = "UN";
    static char ichs[2+1] = "LR";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5;


    /* Local variables */
    static char diag[1];
    static integer ldas, ldbs;
    static logical same;
    static char side[1];
    static logical left, null;
    static char uplo[1];
    static integer i__, j, m, n;
    static real alpha;
    static char diags[1];
    static logical isame[13];
    static char sides[1];
    static integer nargs;
    static logical reset;
    static char uplos[1];
    extern /* Subroutine */ void sprcn3_(integer*, integer*, char*, integer*, char*, char*, char*, char*, integer*, integer*, real*, integer*, integer*, ftnlen , ftnlen, ftnlen, ftnlen, ftnlen);
    static integer ia, na, nc, im, in, ms, ns;
    static char tranas[1], transa[1];
    static real errmax;
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int smmch_(char*, char*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, integer*, real*, real*, logical*, integer*, logical*, ftnlen, ftnlen);
    extern logical lseres_(char*, char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern /* Subroutine */ void cstrmm_(integer*, char*, char*, char*, char*, integer*, integer*, real*, real*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ void cstrsm_(integer*, char*, char*, char*, char*, integer*, integer*, real*, real*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen, ftnlen);
    static integer laa, icd, lbb, lda, ldb, ics;
    static real als;
    static integer ict, icu;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests STRMM and STRSM. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --g;
    --ct;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */

    nargs = 11;
    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;
/*     Set up zero matrix for SMMCH. */
    i__1 = *nmax;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *nmax;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    c__[i__ + j * c_dim1] = (float)0.;
/* L10: */
	}
/* L20: */
    }

    i__1 = *nidim;
    for (im = 1; im <= i__1; ++im) {
	m = idim[im];

	i__2 = *nidim;
	for (in = 1; in <= i__2; ++in) {
	    n = idim[in];
/*           Set LDB to 1 more than minimum value if room. */
	    ldb = m;
	    if (ldb < *nmax) {
		++ldb;
	    }
/*           Skip tests if not enough room. */
	    if (ldb > *nmax) {
		goto L130;
	    }
	    lbb = ldb * n;
	    null = m <= 0 || n <= 0;

	    for (ics = 1; ics <= 2; ++ics) {
		*(unsigned char *)side = *(unsigned char *)&ichs[ics - 1];
		left = *(unsigned char *)side == 'L';
		if (left) {
		    na = m;
		} else {
		    na = n;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = na;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L130;
		}
		laa = lda * na;

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];

		    for (ict = 1; ict <= 3; ++ict) {
			*(unsigned char *)transa = *(unsigned char *)&icht[
				ict - 1];

			for (icd = 1; icd <= 2; ++icd) {
			    *(unsigned char *)diag = *(unsigned char *)&ichd[
				    icd - 1];

			    i__3 = *nalf;
			    for (ia = 1; ia <= i__3; ++ia) {
				alpha = alf[ia];

/*                          Generate the matrix A. */

				smake_("TR", uplo, diag, &na, &na, &a[
					a_offset], nmax, &aa[1], &lda, &reset,
					 &c_b103, (ftnlen)2, (ftnlen)1, (
					ftnlen)1);

/*                          Generate the matrix B. */

				smake_("GE", " ", " ", &m, &n, &b[b_offset], 
					nmax, &bb[1], &ldb, &reset, &c_b103, (
					ftnlen)2, (ftnlen)1, (ftnlen)1);

				++nc;

/*                          Save every datum before calling the */
/*                          subroutine. */

				*(unsigned char *)sides = *(unsigned char *)
					side;
				*(unsigned char *)uplos = *(unsigned char *)
					uplo;
				*(unsigned char *)tranas = *(unsigned char *)
					transa;
				*(unsigned char *)diags = *(unsigned char *)
					diag;
				ms = m;
				ns = n;
				als = alpha;
				i__4 = laa;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    as[i__] = aa[i__];
/* L30: */
				}
				ldas = lda;
				i__4 = lbb;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    bs[i__] = bb[i__];
/* L40: */
				}
				ldbs = ldb;

/*                          Call the subroutine. */

				if (s_cmp(sname + 9, "mm", (ftnlen)2, (ftnlen)
					2) == 0) {
				    if (*trace) {
					sprcn3_(ntra, &nc, sname, iorder, 
						side, uplo, transa, diag, &m, 
						&n, &alpha, &lda, &ldb, (
						ftnlen)12, (ftnlen)1, (ftnlen)
						1, (ftnlen)1, (ftnlen)1);
				    }
				    if (*rewi) {
//					f_rew(&al__1);
				    }
				    cstrmm_(iorder, side, uplo, transa, diag, 
					    &m, &n, &alpha, &aa[1], &lda, &bb[
					    1], &ldb, (ftnlen)1, (ftnlen)1, (
					    ftnlen)1, (ftnlen)1);
				} else if (s_cmp(sname + 9, "sm", (ftnlen)2, (
					ftnlen)2) == 0) {
				    if (*trace) {
					sprcn3_(ntra, &nc, sname, iorder, 
						side, uplo, transa, diag, &m, 
						&n, &alpha, &lda, &ldb, (
						ftnlen)12, (ftnlen)1, (ftnlen)
						1, (ftnlen)1, (ftnlen)1);
				    }
				    if (*rewi) {
//					f_rew(&al__1);
				    }
				    cstrsm_(iorder, side, uplo, transa, diag, 
					    &m, &n, &alpha, &aa[1], &lda, &bb[
					    1], &ldb, (ftnlen)1, (ftnlen)1, (
					    ftnlen)1, (ftnlen)1);
				}

/*                          Check if error-exit was taken incorrectly. */

				if (! infoc_1.ok) {
                                    printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				    *fatal = TRUE_;
				    goto L150;
				}

/*                          See what data changed inside subroutines. */

				isame[0] = *(unsigned char *)sides == *(
					unsigned char *)side;
				isame[1] = *(unsigned char *)uplos == *(
					unsigned char *)uplo;
				isame[2] = *(unsigned char *)tranas == *(
					unsigned char *)transa;
				isame[3] = *(unsigned char *)diags == *(
					unsigned char *)diag;
				isame[4] = ms == m;
				isame[5] = ns == n;
				isame[6] = als == alpha;
				isame[7] = lse_(&as[1], &aa[1], &laa);
				isame[8] = ldas == lda;
				if (null) {
				    isame[9] = lse_(&bs[1], &bb[1], &lbb);
				} else {
				    isame[9] = lseres_("GE", " ", &m, &n, &bs[
					    1], &bb[1], &ldb, (ftnlen)2, (
					    ftnlen)1);
				}
				isame[10] = ldbs == ldb;

/*                          If data was incorrectly changed, report and */
/*                          return. */

				same = TRUE_;
				i__4 = nargs;
				for (i__ = 1; i__ <= i__4; ++i__) {
				    same = same && isame[i__ - 1];
				    if (! isame[i__ - 1]) {
                                        printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				    }
/* L50: */
				}
				if (! same) {
				    *fatal = TRUE_;
				    goto L150;
				}

				if (! null) {
				    if (s_cmp(sname + 9, "mm", (ftnlen)2, (
					    ftnlen)2) == 0) {

/*                                Check the result. */

					if (left) {
					    smmch_(transa, "N", &m, &n, &m, &
						    alpha, &a[a_offset], nmax,
						     &b[b_offset], nmax, &
						    c_b103, &c__[c_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_true, (
						    ftnlen)1, (ftnlen)1);
					} else {
					    smmch_("N", transa, &m, &n, &n, &
						    alpha, &b[b_offset], nmax,
						     &a[a_offset], nmax, &
						    c_b103, &c__[c_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_true, (
						    ftnlen)1, (ftnlen)1);
					}
				    } else if (s_cmp(sname + 9, "sm", (ftnlen)
					    2, (ftnlen)2) == 0) {

/*                                Compute approximation to original */
/*                                matrix. */

					i__4 = n;
					for (j = 1; j <= i__4; ++j) {
					    i__5 = m;
					    for (i__ = 1; i__ <= i__5; ++i__) 
						    {
			  c__[i__ + j * c_dim1] = bb[i__ + (j - 1) * ldb];
			  bb[i__ + (j - 1) * ldb] = alpha * b[i__ + j * 
				  b_dim1];
/* L60: */
					    }
/* L70: */
					}

					if (left) {
					    smmch_(transa, "N", &m, &n, &m, &
						    c_b89, &a[a_offset], nmax,
						     &c__[c_offset], nmax, &
						    c_b103, &b[b_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_false, (
						    ftnlen)1, (ftnlen)1);
					} else {
					    smmch_("N", transa, &m, &n, &n, &
						    c_b89, &c__[c_offset], 
						    nmax, &a[a_offset], nmax, 
						    &c_b103, &b[b_offset], 
						    nmax, &ct[1], &g[1], &bb[
						    1], &ldb, eps, &err, 
						    fatal, nout, &c_false, (
						    ftnlen)1, (ftnlen)1);
					}
				    }
				    errmax = dmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L150;
				    }
				}

/* L80: */
			    }

/* L90: */
			}

/* L100: */
		    }

/* L110: */
		}

/* L120: */
	    }

L130:
	    ;
	}

/* L140: */
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L160;

L150:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    if (*trace) {
	sprcn3_(ntra, &nc, sname, iorder, side, uplo, transa, diag, &m, &n, &
		alpha, &lda, &ldb, (ftnlen)12, (ftnlen)1, (ftnlen)1, (ftnlen)
		1, (ftnlen)1);
    }

L160:
    return 0;

/* 9995 FORMAT( 1X, I6, ': ', A12,'(', 4( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      F4.1, ', A,', I3, ', B,', I3, ')        .' ) */

/*     End of SCHK3. */

} /* schk3_ */


/* Subroutine */ void sprcn3_(integer* nout, integer* nc, char* sname, integer* iorder, char* side, char* uplo, char* transa, char* diag, integer* m, integer* n, real* alpha, integer* lda, integer* ldb, ftnlen sname_len, ftnlen side_len, ftnlen uplo_len, ftnlen transa_len, ftnlen diag_len)
{

    /* Local variables */
    static char ca[14], cd[14], cs[14], cu[14], crc[14];

    if (*(unsigned char *)side == 'L') {
	s_copy(cs, "     CblasLeft", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cs, "    CblasRight", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)diag == 'N') {
	s_copy(cd, "  CblasNonUnit", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cd, "     CblasUnit", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, "CblasRowMajor", (ftnlen)14, (ftnlen)13);
    } else {
	s_copy(crc, "CblasColMajor", (ftnlen)14, (ftnlen)13);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cs,cu);
    printf("         %s %s %d %d %4.1f A %d B %d\n",ca,cd,*m,*n,*alpha,*lda,*ldb);

} /* sprcn3_ */


/* Subroutine */ int schk4_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* nbet, real* bet, integer* nmax, real* a, real* aa, real* as, real* b, real* bb, real* bs, real* c__, real* cc, real* cs, real* ct, real* g, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char icht[3+1] = "NTC";
    static char ichu[2+1] = "UL";

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, i__1, i__2, 
	    i__3, i__4, i__5;


    /* Local variables */
    static real beta;
    static integer ldas, ldcs;
    static logical same;
    static real bets;
    static logical tran, null;
    static char uplo[1];
    static integer i__, j, k, n;
    static real alpha;
    static logical isame[13];
    static integer nargs;
    static logical reset;
    static char trans[1];
    static logical upper;
    static char uplos[1];
    extern /* Subroutine */ void sprcn4_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    extern /* Subroutine */ int smmch_(char*, char*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, integer*, real*, real*, logical*, integer*, logical*, ftnlen, ftnlen);
    static integer ia, ib, jc, ma, na, nc, ik, in, jj, lj, ks, ns;
    static real errmax;
    extern logical lseres_(char*, char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    static char transs[1];
    extern /* Subroutine */ void cssyrk_(integer*, char*, char*, integer*, integer*, real*, real*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    static integer laa, lda, lcc, ldc;
    static real als;
    static integer ict, icu;
    extern logical lse_(real*, real*, integer*);
    static real err;

/*  Tests SSYRK. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
    --bet;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    b_dim1 = *nmax;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    --as;
    --aa;
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;

    /* Function Body */
/*     .. Executable Statements .. */

    nargs = 10;
    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDC to 1 more than minimum value if room. */
	ldc = n;
	if (ldc < *nmax) {
	    ++ldc;
	}
/*        Skip tests if not enough room. */
	if (ldc > *nmax) {
	    goto L100;
	}
	lcc = ldc * n;
	null = n <= 0;

	i__2 = *nidim;
	for (ik = 1; ik <= i__2; ++ik) {
	    k = idim[ik];

	    for (ict = 1; ict <= 3; ++ict) {
		*(unsigned char *)trans = *(unsigned char *)&icht[ict - 1];
		tran = *(unsigned char *)trans == 'T' || *(unsigned char *)
			trans == 'C';
		if (tran) {
		    ma = k;
		    na = n;
		} else {
		    ma = n;
		    na = k;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = ma;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L80;
		}
		laa = lda * na;

/*              Generate the matrix A. */

		smake_("GE", " ", " ", &ma, &na, &a[a_offset], nmax, &aa[1], &
			lda, &reset, &c_b103, (ftnlen)2, (ftnlen)1, (ftnlen)1)
			;

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];
		    upper = *(unsigned char *)uplo == 'U';

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			alpha = alf[ia];

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    beta = bet[ib];

/*                       Generate the matrix C. */

			    smake_("SY", uplo, " ", &n, &n, &c__[c_offset], 
				    nmax, &cc[1], &ldc, &reset, &c_b103, (
				    ftnlen)2, (ftnlen)1, (ftnlen)1);

			    ++nc;

/*                       Save every datum before calling the subroutine. */

			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    *(unsigned char *)transs = *(unsigned char *)
				    trans;
			    ns = n;
			    ks = k;
			    als = alpha;
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				as[i__] = aa[i__];
/* L10: */
			    }
			    ldas = lda;
			    bets = beta;
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				cs[i__] = cc[i__];
/* L20: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (*trace) {
				sprcn4_(ntra, &nc, sname, iorder, uplo, trans,
					 &n, &k, &alpha, &lda, &beta, &ldc, (
					ftnlen)12, (ftnlen)1, (ftnlen)1);
			    }
			    if (*rewi) {
//				f_rew(&al__1);
			    }
			    cssyrk_(iorder, uplo, trans, &n, &k, &alpha, &aa[
				    1], &lda, &beta, &cc[1], &ldc, (ftnlen)1, 
				    (ftnlen)1);

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
                                printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L120;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[1] = *(unsigned char *)transs == *(unsigned 
				    char *)trans;
			    isame[2] = ns == n;
			    isame[3] = ks == k;
			    isame[4] = als == alpha;
			    isame[5] = lse_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    isame[7] = bets == beta;
			    if (null) {
				isame[8] = lse_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[8] = lseres_("SY", uplo, &n, &n, &cs[1],
					 &cc[1], &ldc, (ftnlen)2, (ftnlen)1);
			    }
			    isame[9] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
                                    printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L30: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L120;
			    }

			    if (! null) {

/*                          Check the result column by column. */

				jc = 1;
				i__5 = n;
				for (j = 1; j <= i__5; ++j) {
				    if (upper) {
					jj = 1;
					lj = j;
				    } else {
					jj = j;
					lj = n - j + 1;
				    }
				    if (tran) {
					smmch_("T", "N", &lj, &c__1, &k, &
						alpha, &a[jj * a_dim1 + 1], 
						nmax, &a[j * a_dim1 + 1], 
						nmax, &beta, &c__[jj + j * 
						c_dim1], nmax, &ct[1], &g[1], 
						&cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true, (ftnlen)
						1, (ftnlen)1);
				    } else {
					smmch_("N", "T", &lj, &c__1, &k, &
						alpha, &a[jj + a_dim1], nmax, 
						&a[j + a_dim1], nmax, &beta, &
						c__[jj + j * c_dim1], nmax, &
						ct[1], &g[1], &cc[jc], &ldc, 
						eps, &err, fatal, nout, &
						c_true, (ftnlen)1, (ftnlen)1);
				    }
				    if (upper) {
					jc += ldc;
				    } else {
					jc = jc + ldc + 1;
				    }
				    errmax = dmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L110;
				    }
/* L40: */
				}
			    }

/* L50: */
			}

/* L60: */
		    }

/* L70: */
		}

L80:
		;
	    }

/* L90: */
	}

L100:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
	    printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L130;

L110:
    if (n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d:\n",j);
    }

L120:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    sprcn4_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &
	    beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);

L130:
    return 0;

/* 9994 FORMAT( 1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      F4.1, ', A,', I3, ',', F4.1, ', C,', I3, ')           .' ) */

/*     End of SCHK4. */

} /* schk4_ */


/* Subroutine */ void sprcn4_(integer* nout, integer* nc, char* sname, integer* iorder, char* uplo, char* transa, integer* n, integer* k, real* alpha, integer* lda, real* beta, integer* ldc, ftnlen sname_len, ftnlen uplo_len, ftnlen transa_len)
{

    /* Local variables */
    static char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("(          %d %d %4.1f A %d %4.1f C %d\n",*n,*k,*alpha,*lda,*beta,*ldc);

} /* sprcn4_ */


/* Subroutine */ int schk5_(char* sname, real* eps, real* thresh, integer* nout, integer* ntra, logical* trace, logical* rewi, logical* fatal, integer* nidim, integer* idim, integer* nalf, real* alf, integer* nbet, real* bet, integer* nmax, real* ab, real* aa, real* as, real* bb, real* bs, real* c__, real* cc, real* cs, real* ct, real* g, real* w, integer* iorder, ftnlen sname_len)
{
    /* Initialized data */

    static char icht[3+1] = "NTC";
    static char ichu[2+1] = "UL";

    /* System generated locals */
    integer c_dim1, c_offset, i__1, i__2, i__3, i__4, i__5, i__6, i__7, i__8;


    /* Local variables */
    static integer jjab;
    static real beta;
    static integer ldas, ldbs, ldcs;
    static logical same;
    static real bets;
    static logical tran, null;
    static char uplo[1];
    static integer i__, j, k, n;
    static real alpha;
    static logical isame[13];
    static integer nargs;
    static logical reset;
    static char trans[1];
    static logical upper;
    static char uplos[1];
    static integer ia, ib;
    extern /* Subroutine */ void sprcn5_(integer*, integer*, char*, integer*, char*, char*, integer*, integer*, real*, integer*, integer*, real*, integer*, ftnlen, ftnlen, ftnlen);
    static integer jc, ma, na, nc, ik, in, jj, lj, ks, ns;
    static real errmax;
    extern logical lseres_(char*, char*, integer*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern /* Subroutine */ int smake_(char*, char*, char*, integer*, integer*, real*, integer*, real*, integer*, logical*, real*, ftnlen, ftnlen, ftnlen);
    static char transs[1];
    static integer laa, lbb, lda, lcc, ldb, ldc;
    static real als;
    static integer ict, icu;
    extern /* Subroutine */ void cssyr2k_(integer*, char*, char*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, ftnlen, ftnlen);
    extern logical lse_(real*, real*, integer*);
    extern /* Subroutine */ int smmch_(char*, char*, integer*, integer*, integer*, real*, real*, integer*, real*, integer*, real*, real*, integer*, real*, real*, real*, integer*, real*, real*, logical*, integer*, logical*, ftnlen, ftnlen);
    static real err;

/*  Tests SSYR2K. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
    --bet;
    --w;
    --g;
    --ct;
    --cs;
    --cc;
    c_dim1 = *nmax;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --bs;
    --bb;
    --as;
    --aa;
    --ab;

    /* Function Body */
/*     .. Executable Statements .. */

    nargs = 12;
    nc = 0;
    reset = TRUE_;
    errmax = (float)0.;

    i__1 = *nidim;
    for (in = 1; in <= i__1; ++in) {
	n = idim[in];
/*        Set LDC to 1 more than minimum value if room. */
	ldc = n;
	if (ldc < *nmax) {
	    ++ldc;
	}
/*        Skip tests if not enough room. */
	if (ldc > *nmax) {
	    goto L130;
	}
	lcc = ldc * n;
	null = n <= 0;

	i__2 = *nidim;
	for (ik = 1; ik <= i__2; ++ik) {
	    k = idim[ik];

	    for (ict = 1; ict <= 3; ++ict) {
		*(unsigned char *)trans = *(unsigned char *)&icht[ict - 1];
		tran = *(unsigned char *)trans == 'T' || *(unsigned char *)
			trans == 'C';
		if (tran) {
		    ma = k;
		    na = n;
		} else {
		    ma = n;
		    na = k;
		}
/*              Set LDA to 1 more than minimum value if room. */
		lda = ma;
		if (lda < *nmax) {
		    ++lda;
		}
/*              Skip tests if not enough room. */
		if (lda > *nmax) {
		    goto L110;
		}
		laa = lda * na;

/*              Generate the matrix A. */

		if (tran) {
		    i__3 = *nmax << 1;
		    smake_("GE", " ", " ", &ma, &na, &ab[1], &i__3, &aa[1], &
			    lda, &reset, &c_b103, (ftnlen)2, (ftnlen)1, (
			    ftnlen)1);
		} else {
		    smake_("GE", " ", " ", &ma, &na, &ab[1], nmax, &aa[1], &
			    lda, &reset, &c_b103, (ftnlen)2, (ftnlen)1, (
			    ftnlen)1);
		}

/*              Generate the matrix B. */

		ldb = lda;
		lbb = laa;
		if (tran) {
		    i__3 = *nmax << 1;
		    smake_("GE", " ", " ", &ma, &na, &ab[k + 1], &i__3, &bb[1]
			    , &ldb, &reset, &c_b103, (ftnlen)2, (ftnlen)1, (
			    ftnlen)1);
		} else {
		    smake_("GE", " ", " ", &ma, &na, &ab[k * *nmax + 1], nmax,
			     &bb[1], &ldb, &reset, &c_b103, (ftnlen)2, (
			    ftnlen)1, (ftnlen)1);
		}

		for (icu = 1; icu <= 2; ++icu) {
		    *(unsigned char *)uplo = *(unsigned char *)&ichu[icu - 1];
		    upper = *(unsigned char *)uplo == 'U';

		    i__3 = *nalf;
		    for (ia = 1; ia <= i__3; ++ia) {
			alpha = alf[ia];

			i__4 = *nbet;
			for (ib = 1; ib <= i__4; ++ib) {
			    beta = bet[ib];

/*                       Generate the matrix C. */

			    smake_("SY", uplo, " ", &n, &n, &c__[c_offset], 
				    nmax, &cc[1], &ldc, &reset, &c_b103, (
				    ftnlen)2, (ftnlen)1, (ftnlen)1);

			    ++nc;

/*                       Save every datum before calling the subroutine. */

			    *(unsigned char *)uplos = *(unsigned char *)uplo;
			    *(unsigned char *)transs = *(unsigned char *)
				    trans;
			    ns = n;
			    ks = k;
			    als = alpha;
			    i__5 = laa;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				as[i__] = aa[i__];
/* L10: */
			    }
			    ldas = lda;
			    i__5 = lbb;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				bs[i__] = bb[i__];
/* L20: */
			    }
			    ldbs = ldb;
			    bets = beta;
			    i__5 = lcc;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				cs[i__] = cc[i__];
/* L30: */
			    }
			    ldcs = ldc;

/*                       Call the subroutine. */

			    if (*trace) {
				sprcn5_(ntra, &nc, sname, iorder, uplo, trans,
					 &n, &k, &alpha, &lda, &ldb, &beta, &
					ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1)
					;
			    }
			    if (*rewi) {
//				f_rew(&al__1);
			    }
			    cssyr2k_(iorder, uplo, trans, &n, &k, &alpha, &aa[
				    1], &lda, &bb[1], &ldb, &beta, &cc[1], &
				    ldc, (ftnlen)1, (ftnlen)1);

/*                       Check if error-exit was taken incorrectly. */

			    if (! infoc_1.ok) {
                                printf("*** FATAL ERROR - ERROR-CALL MYEXIT TAKEN ON VALID CALL\n");
				*fatal = TRUE_;
				goto L150;
			    }

/*                       See what data changed inside subroutines. */

			    isame[0] = *(unsigned char *)uplos == *(unsigned 
				    char *)uplo;
			    isame[1] = *(unsigned char *)transs == *(unsigned 
				    char *)trans;
			    isame[2] = ns == n;
			    isame[3] = ks == k;
			    isame[4] = als == alpha;
			    isame[5] = lse_(&as[1], &aa[1], &laa);
			    isame[6] = ldas == lda;
			    isame[7] = lse_(&bs[1], &bb[1], &lbb);
			    isame[8] = ldbs == ldb;
			    isame[9] = bets == beta;
			    if (null) {
				isame[10] = lse_(&cs[1], &cc[1], &lcc);
			    } else {
				isame[10] = lseres_("SY", uplo, &n, &n, &cs[1]
					, &cc[1], &ldc, (ftnlen)2, (ftnlen)1);
			    }
			    isame[11] = ldcs == ldc;

/*                       If data was incorrectly changed, report and */
/*                       return. */

			    same = TRUE_;
			    i__5 = nargs;
			    for (i__ = 1; i__ <= i__5; ++i__) {
				same = same && isame[i__ - 1];
				if (! isame[i__ - 1]) {
                                    printf(" ******* FATAL ERROR - PARAMETER NUMBER %d WAS CHANGED INCORRECTLY *******\n",i__);
				}
/* L40: */
			    }
			    if (! same) {
				*fatal = TRUE_;
				goto L150;
			    }

			    if (! null) {

/*                          Check the result column by column. */

				jjab = 1;
				jc = 1;
				i__5 = n;
				for (j = 1; j <= i__5; ++j) {
				    if (upper) {
					jj = 1;
					lj = j;
				    } else {
					jj = j;
					lj = n - j + 1;
				    }
				    if (tran) {
					i__6 = k;
					for (i__ = 1; i__ <= i__6; ++i__) {
					    w[i__] = ab[((j - 1) << 1) * *nmax 
						    + k + i__];
					    w[k + i__] = ab[((j - 1) << 1) * *
						    nmax + i__];
/* L50: */
					}
					i__6 = k << 1;
					i__7 = *nmax << 1;
					i__8 = *nmax << 1;
					smmch_("T", "N", &lj, &c__1, &i__6, &
						alpha, &ab[jjab], &i__7, &w[1]
						, &i__8, &beta, &c__[jj + j * 
						c_dim1], nmax, &ct[1], &g[1], 
						&cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true, (ftnlen)
						1, (ftnlen)1);
				    } else {
					i__6 = k;
					for (i__ = 1; i__ <= i__6; ++i__) {
					    w[i__] = ab[(k + i__ - 1) * *nmax 
						    + j];
					    w[k + i__] = ab[(i__ - 1) * *nmax 
						    + j];
/* L60: */
					}
					i__6 = k << 1;
					i__7 = *nmax << 1;
					smmch_("N", "N", &lj, &c__1, &i__6, &
						alpha, &ab[jj], nmax, &w[1], &
						i__7, &beta, &c__[jj + j * 
						c_dim1], nmax, &ct[1], &g[1], 
						&cc[jc], &ldc, eps, &err, 
						fatal, nout, &c_true, (ftnlen)
						1, (ftnlen)1);
				    }
				    if (upper) {
					jc += ldc;
				    } else {
					jc = jc + ldc + 1;
					if (tran) {
					    jjab += *nmax << 1;
					}
				    }
				    errmax = dmax(errmax,err);
/*                             If got really bad answer, report and */
/*                             return. */
				    if (*fatal) {
					goto L140;
				    }
/* L70: */
				}
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

L130:
	;
    }

/*     Report result. */

    if (errmax < *thresh) {
	if (*iorder == 0) {
            printf("%s PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
	if (*iorder == 1) {
            printf("%s PASSED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)\n",sname,nc);
	}
    } else {
	if (*iorder == 0) {
            printf("%s COMPLETED THE COLUMN-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
	if (*iorder == 1) {
            printf("%s COMPLETED THE ROW-MAJOR COMPUTATIONAL TESTS (%d CALLS)/n",sname,nc);
            printf("***** BUT WITH MAXIMUM TEST RATIO %8.2f - SUSPECT *******/n",errmax);
	}
    }
    goto L160;

L140:
    if (n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d:\n",j);
    }

L150:
    printf(" ******* %s FAILED ON CALL NUMBER:\n",sname);
    sprcn5_(nout, &nc, sname, iorder, uplo, trans, &n, &k, &alpha, &lda, &ldb,
	     &beta, &ldc, (ftnlen)12, (ftnlen)1, (ftnlen)1);

L160:
    return 0;

/* 9994 FORMAT( 1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ), */
/*     $      F4.1, ', A,', I3, ', B,', I3, ',', F4.1, ', C,', I3, ')   ', */
/*     $      ' .' ) */

/*     End of SCHK5. */

} /* schk5_ */


/* Subroutine */ void sprcn5_(integer* nout, integer* nc, char* sname, integer* iorder, char* uplo, char* transa, integer* n, integer* k, real* alpha, integer* lda, integer* ldb, real* beta, integer* ldc, ftnlen sname_len, ftnlen uplo_len, ftnlen transa_len)
{

    /* Local variables */
    static char ca[14], cu[14], crc[14];

    if (*(unsigned char *)uplo == 'U') {
	s_copy(cu, "    CblasUpper", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(cu, "    CblasLower", (ftnlen)14, (ftnlen)14);
    }
    if (*(unsigned char *)transa == 'N') {
	s_copy(ca, "  CblasNoTrans", (ftnlen)14, (ftnlen)14);
    } else if (*(unsigned char *)transa == 'T') {
	s_copy(ca, "    CblasTrans", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(ca, "CblasConjTrans", (ftnlen)14, (ftnlen)14);
    }
    if (*iorder == 1) {
	s_copy(crc, " CblasRowMajor", (ftnlen)14, (ftnlen)14);
    } else {
	s_copy(crc, " CblasColMajor", (ftnlen)14, (ftnlen)14);
    }
    printf("%6d: %s %s %s %s\n",*nc,sname,crc,cu,ca);
    printf("%d %d %4.1f , A, %d, B, %d, %4.1f , C, %d.\n",*n,*k,*alpha,*lda,*ldb,*beta,*ldc);

} /* sprcn5_ */


/* Subroutine */ int smake_(char* type__, char* uplo, char* diag, integer* m, integer* n, real* a, integer* nmax, real* aa, integer* lda, logical* reset, real* transl, ftnlen type_len, ftnlen uplo_len, ftnlen diag_len)
{
    /* System generated locals */
    integer a_dim1, a_offset, i__1, i__2;

    /* Builtin functions */

    /* Local variables */
    static integer ibeg, iend;
    extern doublereal sbeg_(logical*);
    static logical unit;
    static integer i__, j;
    static logical lower, upper, gen, tri, sym;


/*  Generates values for an M by N matrix A. */
/*  Stores the values in the array AA in the data structure required */
/*  by the routine, with unwanted elements set to rogue value. */

/*  TYPE is 'GE', 'SY' or 'TR'. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. External Functions .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    a_dim1 = *nmax;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    --aa;

    /* Function Body */
    gen = s_cmp(type__, "GE", (ftnlen)2, (ftnlen)2) == 0;
    sym = s_cmp(type__, "SY", (ftnlen)2, (ftnlen)2) == 0;
    tri = s_cmp(type__, "TR", (ftnlen)2, (ftnlen)2) == 0;
    upper = (sym || tri) && *(unsigned char *)uplo == 'U';
    lower = (sym || tri) && *(unsigned char *)uplo == 'L';
    unit = tri && *(unsigned char *)diag == 'U';

/*     Generate data in array A. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    if (gen || (upper && i__ <= j) || (lower && i__ >= j)) {
		a[i__ + j * a_dim1] = sbeg_(reset) + *transl;
		if (i__ != j) {
/*                 Set some elements to zero */
		    if (*n > 3 && j == *n / 2) {
			a[i__ + j * a_dim1] = (float)0.;
		    }
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

    if (s_cmp(type__, "GE", (ftnlen)2, (ftnlen)2) == 0) {
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
    } else if (s_cmp(type__, "SY", (ftnlen)2, (ftnlen)2) == 0 || s_cmp(type__,
	     "TR", (ftnlen)2, (ftnlen)2) == 0) {
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
/* L60: */
	    }
	    i__2 = iend;
	    for (i__ = ibeg; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = a[i__ + j * a_dim1];
/* L70: */
	    }
	    i__2 = *lda;
	    for (i__ = iend + 1; i__ <= i__2; ++i__) {
		aa[i__ + (j - 1) * *lda] = (float)-1e10;
/* L80: */
	    }
/* L90: */
	}
    }
    return 0;

/*     End of SMAKE. */

} /* smake_ */

/* Subroutine */ int smmch_(char* transa, char* transb, integer* m, integer* n, integer* kk, real* alpha, real* a, integer* lda, real* b, integer* ldb, real* beta, real* c__, integer* ldc, real* ct, real* g, real* cc, integer* ldcc, real* eps, real* err, logical* fatal, integer* nout, logical* mv, ftnlen transa_len, ftnlen transb_len)
{

    /* System generated locals */
    integer a_dim1, a_offset, b_dim1, b_offset, c_dim1, c_offset, cc_dim1, 
	    cc_offset, i__1, i__2, i__3;
    real r__1, r__2;

    /* Builtin functions */
    double sqrt(double);

    /* Local variables */
    static real erri;
    static integer i__, j, k;
    static logical trana, tranb;

/*  Checks the results of the computational tests. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Parameters .. */
/*     .. Scalar Arguments .. */
/*     .. Array Arguments .. */
/*     .. Local Scalars .. */
/*     .. Intrinsic Functions .. */
/*     .. Executable Statements .. */
    /* Parameter adjustments */
    a_dim1 = *lda;
    a_offset = 1 + a_dim1 * 1;
    a -= a_offset;
    b_dim1 = *ldb;
    b_offset = 1 + b_dim1 * 1;
    b -= b_offset;
    c_dim1 = *ldc;
    c_offset = 1 + c_dim1 * 1;
    c__ -= c_offset;
    --ct;
    --g;
    cc_dim1 = *ldcc;
    cc_offset = 1 + cc_dim1 * 1;
    cc -= cc_offset;

    /* Function Body */
    trana = *(unsigned char *)transa == 'T' || *(unsigned char *)transa == 
	    'C';
    tranb = *(unsigned char *)transb == 'T' || *(unsigned char *)transb == 
	    'C';

/*     Compute expected result, one column at a time, in CT using data */
/*     in A, B and C. */
/*     Compute gauges in G. */

    i__1 = *n;
    for (j = 1; j <= i__1; ++j) {

	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    ct[i__] = (float)0.;
	    g[i__] = (float)0.;
/* L10: */
	}
	if (! trana && ! tranb) {
	    i__2 = *kk;
	    for (k = 1; k <= i__2; ++k) {
		i__3 = *m;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    ct[i__] += a[i__ + k * a_dim1] * b[k + j * b_dim1];
		    g[i__] += (r__1 = a[i__ + k * a_dim1], dabs(r__1)) * (
			    r__2 = b[k + j * b_dim1], dabs(r__2));
/* L20: */
		}
/* L30: */
	    }
	} else if (trana && ! tranb) {
	    i__2 = *kk;
	    for (k = 1; k <= i__2; ++k) {
		i__3 = *m;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    ct[i__] += a[k + i__ * a_dim1] * b[k + j * b_dim1];
		    g[i__] += (r__1 = a[k + i__ * a_dim1], dabs(r__1)) * (
			    r__2 = b[k + j * b_dim1], dabs(r__2));
/* L40: */
		}
/* L50: */
	    }
	} else if (! trana && tranb) {
	    i__2 = *kk;
	    for (k = 1; k <= i__2; ++k) {
		i__3 = *m;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    ct[i__] += a[i__ + k * a_dim1] * b[j + k * b_dim1];
		    g[i__] += (r__1 = a[i__ + k * a_dim1], dabs(r__1)) * (
			    r__2 = b[j + k * b_dim1], dabs(r__2));
/* L60: */
		}
/* L70: */
	    }
	} else if (trana && tranb) {
	    i__2 = *kk;
	    for (k = 1; k <= i__2; ++k) {
		i__3 = *m;
		for (i__ = 1; i__ <= i__3; ++i__) {
		    ct[i__] += a[k + i__ * a_dim1] * b[j + k * b_dim1];
		    g[i__] += (r__1 = a[k + i__ * a_dim1], dabs(r__1)) * (
			    r__2 = b[j + k * b_dim1], dabs(r__2));
/* L80: */
		}
/* L90: */
	    }
	}
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    ct[i__] = *alpha * ct[i__] + *beta * c__[i__ + j * c_dim1];
	    g[i__] = dabs(*alpha) * g[i__] + dabs(*beta) * (r__1 = c__[i__ + 
		    j * c_dim1], dabs(r__1));
/* L100: */
	}

/*        Compute the error ratio for this result. */

	*err = (float)0.;
	i__2 = *m;
	for (i__ = 1; i__ <= i__2; ++i__) {
	    erri = (r__1 = ct[i__] - cc[i__ + j * cc_dim1], dabs(r__1)) / *
		    eps;
	    if (g[i__] != (float)0.) {
		erri /= g[i__];
	    }
	    *err = dmax(*err,erri);
	    if (*err * sqrt(*eps) >= (float)1.) {
		goto L130;
	    }
/* L110: */
	}

/* L120: */
    }

/*     If the loop completes, all results are at least half accurate. */
    goto L150;

/*     Report fatal error. */

L130:
    *fatal = TRUE_;
    printf(" ******* FATAL ERROR - COMPUTED RESULT IS LESS THAN HALF ACCURATE *******\n");
    printf("         EXPECTED RESULT                    COMPUTED RESULT\n");
    i__1 = *m;
    for (i__ = 1; i__ <= i__1; ++i__) {
	if (*mv) {
            printf("%7d %15.6g %15.6g\n",i__,ct[i__],cc[i__+j*cc_dim1]);
	} else {
            printf("%7d %15.6g %15.6g\n",i__,cc[i__+j*cc_dim1],ct[i__]);
	}
/* L140: */
    }
    if (*n > 1) {
        printf("      THESE ARE THE RESULTS FOR COLUMN %d\n",j);
    }

L150:
    return 0;


/*     End of SMMCH. */

} /* smmch_ */

logical lse_(real* ri, real* rj, integer* lr)
{
    /* System generated locals */
    integer i__1;
    logical ret_val;

    /* Local variables */
    static integer i__;


/*  Tests if two arrays are identical. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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

logical lseres_(char* type__, char* uplo, integer* m, integer* n, real* aa, real* as, integer* lda, ftnlen type_len, ftnlen uplo_len)
{
    /* System generated locals */
    integer aa_dim1, aa_offset, as_dim1, as_offset, i__1, i__2;
    logical ret_val;

    /* Builtin functions */

    /* Local variables */
    static integer ibeg, iend, i__, j;
    static logical upper;


/*  Tests if selected elements in two arrays are equal. */

/*  TYPE is 'GE' or 'SY'. */

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

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
    if (s_cmp(type__, "GE", (ftnlen)2, (ftnlen)2) == 0) {
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
    } else if (s_cmp(type__, "SY", (ftnlen)2, (ftnlen)2) == 0) {
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

/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Scalar Arguments .. */
/*     .. Local Scalars .. */
/*     .. Save statement .. */
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
    ret_val = (i__ - 500) / (float)1001.;
    return ret_val;

/*     End of SBEG. */

} /* sbeg_ */

doublereal sdiff_(real* x, real* y)
{
    /* System generated locals */
    real ret_val;


/*  Auxiliary routine for test program for Level 3 Blas. */

/*  -- Written on 8-February-1989. */
/*     Jack Dongarra, Argonne National Laboratory. */
/*     Iain Duff, AERE Harwell. */
/*     Jeremy Du Croz, Numerical Algorithms Group Ltd. */
/*     Sven Hammarling, Numerical Algorithms Group Ltd. */

/*     .. Scalar Arguments .. */
/*     .. Executable Statements .. */
    ret_val = *x - *y;
    return ret_val;

/*     End of SDIFF. */

} /* sdiff_ */

/* Main program alias */ /*int sblat3_ () { MAIN__ (); }*/
