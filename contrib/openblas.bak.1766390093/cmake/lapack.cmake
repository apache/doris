# Sources for compiling lapack-netlib. Can't use CMakeLists.txt because lapack-netlib already has its own cmake files.
if (NOT C_LAPACK)
	message (STATUS "fortran lapack")
set(ALLAUX ilaenv.f ilaenv2stage.f ieeeck.f lsamen.f iparmq.f iparam2stage.F
   ilaprec.f ilatrans.f ilauplo.f iladiag.f chla_transtype.f dlaset.f la_xisnan.F90
   ../INSTALL/ilaver.f xerbla_array.f
   ../INSTALL/slamch.f)

set(SCLAUX
	scombssq.f sbdsvdx.f sstevx.f sstein.f
   la_constants.f90
   sbdsdc.f
   sbdsqr.f sdisna.f slabad.f slacpy.f sladiv.f slae2.f  slaebz.f
   slaed0.f slaed1.f slaed2.f slaed3.f slaed4.f slaed5.f slaed6.f
   slaed7.f slaed8.f slaed9.f slaeda.f slaev2.f slagtf.f
   slagts.f slamrg.f slanst.f
   slapy2.f slapy3.f slarnv.f
   slarra.f slarrb.f slarrc.f slarrd.f slarre.f slarrf.f slarrj.f
   slarrk.f slarrr.f slaneg.f
   slartg.f90 slaruv.f slas2.f  slascl.f
   slasd0.f slasd1.f slasd2.f slasd3.f slasd4.f slasd5.f slasd6.f
   slasd7.f slasd8.f slasda.f slasdq.f slasdt.f
   slaset.f slasq1.f slasq2.f slasq3.f slasq4.f slasq5.f slasq6.f
   slasr.f  slasrt.f slassq.f90 slasv2.f spttrf.f sstebz.f sstedc.f
   ssteqr.f ssterf.f slaisnan.f sisnan.f slarmm.f
   slartgp.f slartgs.f ../INSTALL/sroundup_lwork.f
   ../INSTALL/second_${TIMER}.f)

set(DZLAUX
   la_constants.f90
   dbdsdc.f
   dbdsvdx.f dstevx.f dstein.f
   dbdsqr.f ddisna.f dlabad.f dlacpy.f dladiv.f dlae2.f  dlaebz.f
   dlaed0.f dlaed1.f dlaed2.f dlaed3.f dlaed4.f dlaed5.f dlaed6.f
   dlaed7.f dlaed8.f dlaed9.f dlaeda.f dlaev2.f dlagtf.f
   dlagts.f dlamrg.f dlanst.f
   dlapy2.f dlapy3.f dlarnv.f
   dlarra.f dlarrb.f dlarrc.f dlarrd.f dlarre.f dlarrf.f dlarrj.f
   dlarrk.f dlarrr.f dlaneg.f
   dlartg.f90 dlaruv.f dlas2.f  dlascl.f
   dlasd0.f dlasd1.f dlasd2.f dlasd3.f dlasd4.f dlasd5.f dlasd6.f
   dlasd7.f dlasd8.f dlasda.f dlasdq.f dlasdt.f
   dlasq1.f dlasq2.f dlasq3.f dlasq4.f dlasq5.f dlasq6.f
   dlasr.f  dlasrt.f dlassq.f90 dlasv2.f dpttrf.f dstebz.f dstedc.f
   dsteqr.f dsterf.f dlaisnan.f disnan.f dlarmm.f
   dlartgp.f dlartgs.f ../INSTALL/droundup_lwork.f
   ../INSTALL/dlamch.f ../INSTALL/dsecnd_${TIMER}.f)

set(SLASRC
   sgbbrd.f sgbcon.f sgbequ.f sgbrfs.f sgbsv.f
   sgbsvx.f sgbtf2.f sgbtrf.f sgbtrs.f sgebak.f sgebal.f sgebd2.f
   sgebrd.f sgecon.f sgeequ.f sgees.f  sgeesx.f sgeev.f  sgeevx.f
   sgehd2.f sgehrd.f sgelq2.f sgelqf.f
   sgels.f  sgelsd.f sgelss.f sgelsy.f sgeql2.f sgeqlf.f
   sgeqp3.f sgeqp3rk.f sgeqr2.f sgeqr2p.f sgeqrf.f sgeqrfp.f sgerfs.f sgerq2.f sgerqf.f
   sgesc2.f sgesdd.f sgesvd.f sgesvdx.f sgesvx.f sgetc2.f
   sgetrf2.f sgetri.f
   sggbak.f sggbal.f
   sgges.f  sgges3.f sggesx.f sggev.f  sggev3.f sggevx.f
   sggglm.f sgghrd.f sgghd3.f sgglse.f sggqrf.f
   sggrqf.f sggsvd3.f sggsvp3.f sgtcon.f sgtrfs.f sgtsv.f
   sgtsvx.f sgttrf.f sgttrs.f sgtts2.f shgeqz.f
   shsein.f shseqr.f slabrd.f slacon.f slacn2.f
   slaqz0.f slaqz1.f slaqz2.f slaqz3.f slaqz4.f
   slaein.f slaexc.f slag2.f  slags2.f slagtm.f slagv2.f slahqr.f
   slahr2.f slaic1.f slaln2.f slals0.f slalsa.f slalsd.f
   slangb.f slange.f slangt.f slanhs.f slansb.f slansp.f
   slansy.f slantb.f slantp.f slantr.f slanv2.f
   slapll.f slapmt.f
   slaqgb.f slaqge.f slaqp2.f slaqps.f slaqp2rk.f slaqp3rk.f slaqsb.f slaqsp.f slaqsy.f
   slaqr0.f slaqr1.f slaqr2.f slaqr3.f slaqr4.f slaqr5.f
   slaqtr.f slar1v.f slar2v.f ilaslr.f ilaslc.f
   slarf.f  slarfb.f slarfb_gett.f slarfg.f slarfgp.f slarft.f slarfx.f slarfy.f slargv.f
   slarrv.f slartv.f
   slarz.f  slarzb.f slarzt.f slasy2.f
   slasyf.f slasyf_rook.f slasyf_rk.f slasyf_aa.f
   slatbs.f slatdf.f slatps.f slatrd.f slatrs.f slatrz.f
   sopgtr.f sopmtr.f sorg2l.f sorg2r.f
   sorgbr.f sorghr.f sorgl2.f sorglq.f sorgql.f sorgqr.f sorgr2.f
   sorgrq.f sorgtr.f sorm2l.f sorm2r.f sorm22.f
   sormbr.f sormhr.f sorml2.f sormlq.f sormql.f sormqr.f sormr2.f
   sormr3.f sormrq.f sormrz.f sormtr.f spbcon.f spbequ.f spbrfs.f
   spbstf.f spbsv.f  spbsvx.f
   spbtf2.f spbtrf.f spbtrs.f spocon.f spoequ.f sporfs.f sposv.f
   sposvx.f spotrf2.f spotri.f spstrf.f spstf2.f
   sppcon.f sppequ.f
   spprfs.f sppsv.f  sppsvx.f spptrf.f spptri.f spptrs.f sptcon.f
   spteqr.f sptrfs.f sptsv.f  sptsvx.f spttrs.f sptts2.f srscl.f
   ssbev.f  ssbevd.f ssbevx.f ssbgst.f ssbgv.f  ssbgvd.f ssbgvx.f
   ssbtrd.f sspcon.f sspev.f  sspevd.f sspevx.f sspgst.f
   sspgv.f  sspgvd.f sspgvx.f ssprfs.f sspsv.f  sspsvx.f ssptrd.f
   ssptrf.f ssptri.f ssptrs.f sstegr.f sstev.f  sstevd.f sstevr.f
   ssycon.f ssyev.f  ssyevd.f ssyevr.f ssyevx.f ssygs2.f
   ssygst.f ssygv.f  ssygvd.f ssygvx.f ssyrfs.f ssysv.f  ssysvx.f
   ssytd2.f ssytf2.f ssytrd.f ssytrf.f ssytri.f ssytri2.f ssytri2x.f
   ssyswapr.f ssytrs.f ssytrs2.f
   ssyconv.f ssyconvf.f ssyconvf_rook.f
   ssysv_aa.f ssysv_aa_2stage.f ssytrf_aa.f ssytrf_aa_2stage.f ssytrs_aa.f ssytrs_aa_2stage.f
   ssytf2_rook.f ssytrf_rook.f ssytrs_rook.f
   ssytri_rook.f ssycon_rook.f ssysv_rook.f
   ssytf2_rk.f ssytrf_rk.f ssytrs_3.f
   ssytri_3.f ssytri_3x.f ssycon_3.f ssysv_rk.f
   ssysv_aa.f ssytrf_aa.f ssytrs_aa.f
   stbcon.f
   stbrfs.f stbtrs.f stgevc.f stgex2.f stgexc.f stgsen.f
   stgsja.f stgsna.f stgsy2.f stgsyl.f stpcon.f stprfs.f stptri.f
   stptrs.f
   strcon.f strevc.f strevc3.f strexc.f strrfs.f strsen.f strsna.f strsyl.f
   strtrs.f stzrzf.f sstemr.f
   slansf.f spftrf.f spftri.f spftrs.f ssfrk.f stfsm.f stftri.f stfttp.f
   stfttr.f stpttf.f stpttr.f strttf.f strttp.f
   sgejsv.f sgesvj.f sgsvj0.f sgsvj1.f
   sgeequb.f ssyequb.f spoequb.f sgbequb.f
   sbbcsd.f slapmr.f sorbdb.f sorbdb1.f sorbdb2.f sorbdb3.f sorbdb4.f
   sorbdb5.f sorbdb6.f sorcsd.f sorcsd2by1.f
   sgeqrt.f sgeqrt2.f sgeqrt3.f sgemqrt.f
   stpqrt.f stpqrt2.f stpmqrt.f stprfb.f
   sgelqt.f sgelqt3.f sgemlqt.f
   sgetsls.f sgetsqrhrt.f sgeqr.f slatsqr.f slamtsqr.f sgemqr.f
   sgelq.f slaswlq.f slamswlq.f sgemlq.f
   stplqt.f stplqt2.f stpmlqt.f
   ssytrd_2stage.f ssytrd_sy2sb.f ssytrd_sb2st.F ssb2st_kernels.f
   ssyevd_2stage.f ssyev_2stage.f ssyevx_2stage.f ssyevr_2stage.f
   ssbev_2stage.f ssbevx_2stage.f ssbevd_2stage.f ssygv_2stage.f
   sgesvdq.f slaorhr_col_getrfnp.f
   slaorhr_col_getrfnp2.f sorgtsqr.f sorgtsqr_row.f sorhr_col.f 
   slatrs3.f strsyl3.f sgelst.f sgedmd.f90 sgedmdq.f90)

set(SXLASRC sgesvxx.f sgerfsx.f sla_gerfsx_extended.f sla_geamv.f
   sla_gercond.f sla_gerpvgrw.f ssysvxx.f ssyrfsx.f
   sla_syrfsx_extended.f sla_syamv.f sla_syrcond.f sla_syrpvgrw.f
   sposvxx.f sporfsx.f sla_porfsx_extended.f sla_porcond.f
   sla_porpvgrw.f sgbsvxx.f sgbrfsx.f sla_gbrfsx_extended.f
   sla_gbamv.f sla_gbrcond.f sla_gbrpvgrw.f sla_lin_berr.f slarscl2.f
   slascl2.f sla_wwaddw.f)

set(CLASRC
   cbdsqr.f cgbbrd.f cgbcon.f cgbequ.f cgbrfs.f cgbsv.f  cgbsvx.f
   cgbtf2.f cgbtrf.f cgbtrs.f cgebak.f cgebal.f cgebd2.f cgebrd.f
   cgecon.f cgeequ.f cgees.f  cgeesx.f cgeev.f  cgeevx.f
   cgehd2.f cgehrd.f cgelq2.f cgelqf.f
   cgels.f  cgelsd.f cgelss.f cgelsy.f cgeql2.f cgeqlf.f cgeqp3.f cgeqp3rk.f
   cgeqr2.f cgeqr2p.f cgeqrf.f cgeqrfp.f cgerfs.f cgerq2.f cgerqf.f
   cgesc2.f cgesdd.f cgesvd.f cgesvdx.f
   cgesvj.f cgejsv.f cgsvj0.f cgsvj1.f
   cgesvx.f cgetc2.f cgetrf2.f
   cgetri.f
   cggbak.f cggbal.f
   cgges.f  cgges3.f cggesx.f cggev.f  cggev3.f cggevx.f
   cggglm.f cgghrd.f cgghd3.f cgglse.f cggqrf.f cggrqf.f
   cggsvd3.f cggsvp3.f
   cgtcon.f cgtrfs.f cgtsv.f  cgtsvx.f cgttrf.f cgttrs.f cgtts2.f chbev.f
   chbevd.f chbevx.f chbgst.f chbgv.f  chbgvd.f chbgvx.f chbtrd.f
   checon.f cheev.f  cheevd.f cheevr.f cheevx.f chegs2.f chegst.f
   chegv.f  chegvd.f chegvx.f cherfs.f chesv.f  chesvx.f chetd2.f
   chetf2.f chetrd.f
   chetrf.f chetri.f chetri2.f chetri2x.f cheswapr.f
   chetrs.f chetrs2.f
   chetf2_rook.f chetrf_rook.f chetri_rook.f
   chetrs_rook.f checon_rook.f chesv_rook.f
   chetf2_rk.f chetrf_rk.f chetri_3.f chetri_3x.f
   chetrs_3.f checon_3.f chesv_rk.f
   chesv_aa.f chesv_aa_2stage.f chetrf_aa.f chetrf_aa_2stage.f chetrs_aa.f chetrs_aa_2stage.f
   chgeqz.f chpcon.f chpev.f  chpevd.f
   chpevx.f chpgst.f chpgv.f  chpgvd.f chpgvx.f chprfs.f chpsv.f
   chpsvx.f
   chptrd.f chptrf.f chptri.f chptrs.f chsein.f chseqr.f clabrd.f
   clacgv.f clacon.f clacn2.f clacp2.f clacpy.f clacrm.f clacrt.f cladiv.f
   claed0.f claed7.f claed8.f
   claein.f claesy.f claev2.f clags2.f clagtm.f
   clahef.f clahef_rook.f clahef_rk.f clahef_aa.f clahqr.f
   clahr2.f claic1.f clals0.f clalsa.f clalsd.f clangb.f clange.f clangt.f
   clanhb.f clanhe.f
   clanhp.f clanhs.f clanht.f clansb.f clansp.f clansy.f clantb.f
   clantp.f clantr.f clapll.f clapmt.f clarcm.f claqgb.f claqge.f
   claqhb.f claqhe.f claqhp.f claqp2.f claqps.f claqp2rk.f claqp3rk.f claqsb.f
   claqr0.f claqr1.f claqr2.f claqr3.f claqr4.f claqr5.f
   claqz0.f claqz1.f claqz2.f claqz3.f
   claqsp.f claqsy.f clar1v.f clar2v.f ilaclr.f ilaclc.f
   clarf.f  clarfb.f clarfb_gett.f clarfg.f clarfgp.f clarft.f
   clarfx.f clarfy.f clargv.f clarnv.f clarrv.f clartg.f90 clartv.f
   clarz.f  clarzb.f clarzt.f clascl.f claset.f clasr.f  classq.f90
   clasyf.f clasyf_rook.f clasyf_rk.f clasyf_aa.f
   clatbs.f clatdf.f clatps.f clatrd.f clatrs.f clatrz.f
   cpbcon.f cpbequ.f cpbrfs.f cpbstf.f cpbsv.f
   cpbsvx.f cpbtf2.f cpbtrf.f cpbtrs.f cpocon.f cpoequ.f cporfs.f
   cposv.f  cposvx.f cpotrf2.f cpotri.f cpstrf.f cpstf2.f
   cppcon.f cppequ.f cpprfs.f cppsv.f  cppsvx.f cpptrf.f cpptri.f cpptrs.f
   cptcon.f cpteqr.f cptrfs.f cptsv.f  cptsvx.f cpttrf.f cpttrs.f cptts2.f
   crot.f crscl.f cspcon.f csprfs.f cspsv.f
   cspsvx.f csptrf.f csptri.f csptrs.f csrscl.f cstedc.f
   cstegr.f cstein.f csteqr.f csycon.f
   csyrfs.f csysv.f  csysvx.f csytf2.f csytrf.f csytri.f
   csytri2.f csytri2x.f csyswapr.f
   csytrs.f csytrs2.f
   csyconv.f csyconvf.f csyconvf_rook.f
   csytf2_rook.f csytrf_rook.f csytrs_rook.f
   csytri_rook.f csycon_rook.f csysv_rook.f
   csytf2_rk.f csytrf_rk.f csytrf_aa.f csytrf_aa_2stage.f csytrs_3.f csytrs_aa.f csytrs_aa_2stage.f
   csytri_3.f csytri_3x.f csycon_3.f csysv_rk.f csysv_aa.f csysv_aa_2stage.f
   ctbcon.f ctbrfs.f ctbtrs.f ctgevc.f ctgex2.f
   ctgexc.f ctgsen.f ctgsja.f ctgsna.f ctgsy2.f ctgsyl.f ctpcon.f
   ctprfs.f ctptri.f
   ctptrs.f ctrcon.f ctrevc.f ctrevc3.f ctrexc.f ctrrfs.f ctrsen.f ctrsna.f
   ctrsyl.f ctrtrs.f ctzrzf.f cung2l.f cung2r.f
   cungbr.f cunghr.f cungl2.f cunglq.f cungql.f cungqr.f cungr2.f
   cungrq.f cungtr.f cunm2l.f cunm2r.f cunmbr.f cunmhr.f cunml2.f cunm22.f
   cunmlq.f cunmql.f cunmqr.f cunmr2.f cunmr3.f cunmrq.f cunmrz.f
   cunmtr.f cupgtr.f cupmtr.f icmax1.f scsum1.f cstemr.f
   chfrk.f ctfttp.f clanhf.f cpftrf.f cpftri.f cpftrs.f ctfsm.f ctftri.f
   ctfttr.f ctpttf.f ctpttr.f ctrttf.f ctrttp.f
   cgeequb.f cgbequb.f csyequb.f cpoequb.f cheequb.f
   cbbcsd.f clapmr.f cunbdb.f cunbdb1.f cunbdb2.f cunbdb3.f cunbdb4.f
   cunbdb5.f cunbdb6.f cuncsd.f cuncsd2by1.f
   cgeqrt.f cgeqrt2.f cgeqrt3.f cgemqrt.f
   ctpqrt.f ctpqrt2.f ctpmqrt.f ctprfb.f
   cgelqt.f cgelqt3.f cgemlqt.f
   cgetsls.f cgetsqrhrt.f cgeqr.f clatsqr.f clamtsqr.f cgemqr.f
   cgelq.f claswlq.f clamswlq.f cgemlq.f
   ctplqt.f ctplqt2.f ctpmlqt.f
   chetrd_2stage.f chetrd_he2hb.f chetrd_hb2st.F chb2st_kernels.f
   cheevd_2stage.f cheev_2stage.f cheevx_2stage.f cheevr_2stage.f
   chbev_2stage.f chbevx_2stage.f chbevd_2stage.f chegv_2stage.f
   cgesvdq.f claunhr_col_getrfnp.f claunhr_col_getrfnp2.f 
   cungtsqr.f cungtsqr_row.f cunhr_col.f 
   clatrs3.f ctrsyl3.f cgelst.f cgedmd.f90 cgedmdq.f90)

set(CXLASRC cgesvxx.f cgerfsx.f cla_gerfsx_extended.f cla_geamv.f
   cla_gercond_c.f cla_gercond_x.f cla_gerpvgrw.f
   csysvxx.f csyrfsx.f cla_syrfsx_extended.f cla_syamv.f
   cla_syrcond_c.f cla_syrcond_x.f cla_syrpvgrw.f
   cposvxx.f cporfsx.f cla_porfsx_extended.f
   cla_porcond_c.f cla_porcond_x.f cla_porpvgrw.f
   cgbsvxx.f cgbrfsx.f cla_gbrfsx_extended.f cla_gbamv.f
   cla_gbrcond_c.f cla_gbrcond_x.f cla_gbrpvgrw.f
   chesvxx.f cherfsx.f cla_herfsx_extended.f cla_heamv.f
   cla_hercond_c.f cla_hercond_x.f cla_herpvgrw.f
   cla_lin_berr.f clarscl2.f clascl2.f cla_wwaddw.f)

set(DLASRC
   dgbbrd.f dgbcon.f dgbequ.f dgbrfs.f dgbsv.f
   dgbsvx.f dgbtf2.f dgbtrf.f dgbtrs.f dgebak.f dgebal.f dgebd2.f
   dgebrd.f dgecon.f dgeequ.f dgees.f  dgeesx.f dgeev.f  dgeevx.f
   dgehd2.f dgehrd.f dgelq2.f dgelqf.f
   dgels.f  dgelsd.f dgelss.f dgelsy.f dgeql2.f dgeqlf.f
   dgeqp3.f dgeqp3rk.f dgeqr2.f dgeqr2p.f dgeqrf.f dgeqrfp.f dgerfs.f dgerq2.f dgerqf.f
   dgesc2.f dgesdd.f dgesvd.f dgesvdx.f dgesvx.f dgetc2.f
   dgetrf2.f dgetri.f
   dggbak.f dggbal.f
   dgges.f  dgges3.f dggesx.f dggev.f  dggev3.f dggevx.f
   dggglm.f dgghrd.f dgghd3.f dgglse.f dggqrf.f
   dggrqf.f dggsvd3.f dggsvp3.f dgtcon.f dgtrfs.f dgtsv.f
   dgtsvx.f dgttrf.f dgttrs.f dgtts2.f dhgeqz.f
   dlaqz0.f dlaqz1.f dlaqz2.f dlaqz3.f dlaqz4.f
   dhsein.f dhseqr.f dlabrd.f dlacon.f dlacn2.f
   dlaein.f dlaexc.f dlag2.f  dlags2.f dlagtm.f dlagv2.f dlahqr.f
   dlahr2.f dlaic1.f dlaln2.f dlals0.f dlalsa.f dlalsd.f
   dlangb.f dlange.f dlangt.f dlanhs.f dlansb.f dlansp.f
   dlansy.f dlantb.f dlantp.f dlantr.f dlanv2.f
   dlapll.f dlapmt.f
   dlaqgb.f dlaqge.f dlaqp2.f dlaqp2rk.f dlaqp3rk.f dlaqps.f dlaqsb.f dlaqsp.f dlaqsy.f
   dlaqr0.f dlaqr1.f dlaqr2.f dlaqr3.f dlaqr4.f dlaqr5.f
   dlaqtr.f dlar1v.f dlar2v.f iladlr.f iladlc.f
   dlarf.f  dlarfb.f dlarfb_gett.f dlarfg.f dlarfgp.f dlarft.f dlarfx.f dlarfy.f
   dlargv.f dlarrv.f dlartv.f
   dlarz.f  dlarzb.f dlarzt.f dlasy2.f
   dlasyf.f dlasyf_rook.f dlasyf_rk.f dlasyf_aa.f
   dlatbs.f dlatdf.f dlatps.f dlatrd.f dlatrs.f dlatrz.f
   dopgtr.f dopmtr.f dorg2l.f dorg2r.f
   dorgbr.f dorghr.f dorgl2.f dorglq.f dorgql.f dorgqr.f dorgr2.f
   dorgrq.f dorgtr.f dorm2l.f dorm2r.f dorm22.f
   dormbr.f dormhr.f dorml2.f dormlq.f dormql.f dormqr.f dormr2.f
   dormr3.f dormrq.f dormrz.f dormtr.f dpbcon.f dpbequ.f dpbrfs.f
   dpbstf.f dpbsv.f  dpbsvx.f
   dpbtf2.f dpbtrf.f dpbtrs.f dpocon.f dpoequ.f dporfs.f dposv.f
   dposvx.f dpotrf2.f dpotri.f dpotrs.f dpstrf.f dpstf2.f
   dppcon.f dppequ.f
   dpprfs.f dppsv.f  dppsvx.f dpptrf.f dpptri.f dpptrs.f dptcon.f
   dpteqr.f dptrfs.f dptsv.f  dptsvx.f dpttrs.f dptts2.f drscl.f
   dsbev.f  dsbevd.f dsbevx.f dsbgst.f dsbgv.f  dsbgvd.f dsbgvx.f
   dsbtrd.f dspcon.f dspev.f  dspevd.f dspevx.f dspgst.f
   dspgv.f  dspgvd.f dspgvx.f dsprfs.f dspsv.f  dspsvx.f dsptrd.f
   dsptrf.f dsptri.f dsptrs.f dstegr.f dstev.f  dstevd.f dstevr.f
   dsycon.f dsyev.f  dsyevd.f dsyevr.f
   dsyevx.f dsygs2.f dsygst.f dsygv.f  dsygvd.f dsygvx.f dsyrfs.f
   dsysv.f  dsysvx.f
   dsytd2.f dsytf2.f dsytrd.f dsytrf.f dsytri.f dsytrs.f dsytrs2.f
   dsytri2.f dsytri2x.f dsyswapr.f
   dsyconv.f dsyconvf.f dsyconvf_rook.f
   dsytf2_rook.f dsytrf_rook.f dsytrs_rook.f
   dsytri_rook.f dsycon_rook.f dsysv_rook.f
   dsytf2_rk.f dsytrf_rk.f dsytrs_3.f
   dsytri_3.f dsytri_3x.f dsycon_3.f dsysv_rk.f
   dsysv_aa.f dsysv_aa_2stage.f dsytrf_aa.f dsytrf_aa_2stage.f dsytrs_aa.f dsytrs_aa_2stage.f
   dtbcon.f
   dtbrfs.f dtbtrs.f dtgevc.f dtgex2.f dtgexc.f dtgsen.f
   dtgsja.f dtgsna.f dtgsy2.f dtgsyl.f dtpcon.f dtprfs.f dtptri.f
   dtptrs.f
   dtrcon.f dtrevc.f dtrevc3.f dtrexc.f dtrrfs.f dtrsen.f dtrsna.f dtrsyl.f
   dtrtrs.f dtzrzf.f dstemr.f
   dsgesv.f dsposv.f dlag2s.f slag2d.f dlat2s.f
   dlansf.f dpftrf.f dpftri.f dpftrs.f dsfrk.f dtfsm.f dtftri.f dtfttp.f
   dtfttr.f dtpttf.f dtpttr.f dtrttf.f dtrttp.f
   dgejsv.f dgesvj.f dgsvj0.f dgsvj1.f
   dgeequb.f dsyequb.f dpoequb.f dgbequb.f
   dbbcsd.f dlapmr.f dorbdb.f dorbdb1.f dorbdb2.f dorbdb3.f dorbdb4.f
   dorbdb5.f dorbdb6.f dorcsd.f dorcsd2by1.f
   dgeqrt.f dgeqrt2.f dgeqrt3.f dgemqrt.f
   dtpqrt.f dtpqrt2.f dtpmqrt.f dtprfb.f
   dgelqt.f dgelqt3.f dgemlqt.f
   dgetsls.f dgetsqrhrt.f dgeqr.f dlatsqr.f dlamtsqr.f dgemqr.f
   dgelq.f dlaswlq.f dlamswlq.f dgemlq.f
   dtplqt.f dtplqt2.f dtpmlqt.f
   dsytrd_2stage.f dsytrd_sy2sb.f dsytrd_sb2st.F dsb2st_kernels.f
   dsyevd_2stage.f dsyev_2stage.f dsyevx_2stage.f dsyevr_2stage.f
   dsbev_2stage.f dsbevx_2stage.f dsbevd_2stage.f dsygv_2stage.f
   dcombssq.f dgesvdq.f dlaorhr_col_getrfnp.f
   dlaorhr_col_getrfnp2.f dorgtsqr.f dorgtsqr_row.f dorhr_col.f 
   dlatrs3.f dtrsyl3.f dgelst.f dgedmd.f90 dgedmdq.f90)

set(DXLASRC dgesvxx.f dgerfsx.f dla_gerfsx_extended.f dla_geamv.f
   dla_gercond.f dla_gerpvgrw.f dsysvxx.f dsyrfsx.f
   dla_syrfsx_extended.f dla_syamv.f dla_syrcond.f dla_syrpvgrw.f
   dposvxx.f dporfsx.f dla_porfsx_extended.f dla_porcond.f
   dla_porpvgrw.f dgbsvxx.f dgbrfsx.f dla_gbrfsx_extended.f
   dla_gbamv.f dla_gbrcond.f dla_gbrpvgrw.f dla_lin_berr.f dlarscl2.f
   dlascl2.f dla_wwaddw.f)

set(ZLASRC
   zbdsqr.f zgbbrd.f zgbcon.f zgbequ.f zgbrfs.f zgbsv.f  zgbsvx.f
   zgbtf2.f zgbtrf.f zgbtrs.f zgebak.f zgebal.f zgebd2.f zgebrd.f
   zgecon.f zgeequ.f zgees.f  zgeesx.f zgeev.f  zgeevx.f
   zgehd2.f zgehrd.f zgelq2.f zgelqf.f
   zgels.f  zgelsd.f zgelss.f zgelsy.f zgeql2.f zgeqlf.f zgeqp3.f zgeqp3rk.f
   zgeqr2.f zgeqr2p.f zgeqrf.f zgeqrfp.f zgerfs.f zgerq2.f zgerqf.f
   zgesc2.f zgesdd.f zgesvd.f zgesvdx.f zgesvx.f
   zgesvj.f zgejsv.f zgsvj0.f zgsvj1.f
   zgetc2.f zgetrf2.f
   zgetri.f
   zggbak.f zggbal.f
   zgges.f  zgges3.f zggesx.f zggev.f  zggev3.f zggevx.f
   zggglm.f zgghrd.f zgghd3.f zgglse.f zggqrf.f zggrqf.f
   zggsvd3.f zggsvp3.f
   zgtcon.f zgtrfs.f zgtsv.f  zgtsvx.f zgttrf.f zgttrs.f zgtts2.f zhbev.f
   zhbevd.f zhbevx.f zhbgst.f zhbgv.f  zhbgvd.f zhbgvx.f zhbtrd.f
   zhecon.f zheev.f  zheevd.f zheevr.f zheevx.f zhegs2.f zhegst.f
   zhegv.f  zhegvd.f zhegvx.f zherfs.f zhesv.f  zhesvx.f zhetd2.f
   zhetf2.f zhetrd.f
   zhetrf.f zhetri.f zhetri2.f zhetri2x.f zheswapr.f
   zhetrs.f zhetrs2.f
   zhetf2_rook.f zhetrf_rook.f zhetri_rook.f
   zhetrs_rook.f zhecon_rook.f zhesv_rook.f
   zhetf2_rk.f zhetrf_rk.f zhetri_3.f zhetri_3x.f
   zhetrs_3.f zhecon_3.f zhesv_rk.f
   zhesv_aa.f zhesv_aa_2stage.f zhetrf_aa.f zhetrf_aa_2stage.f zhetrs_aa.f zhetrs_aa_2stage.f
   zhgeqz.f zhpcon.f zhpev.f  zhpevd.f
   zlaqz0.f zlaqz1.f zlaqz2.f zlaqz3.f
   zhpevx.f zhpgst.f zhpgv.f  zhpgvd.f zhpgvx.f zhprfs.f zhpsv.f
   zhpsvx.f
   zhptrd.f zhptrf.f zhptri.f zhptrs.f zhsein.f zhseqr.f zlabrd.f
   zlacgv.f zlacon.f zlacn2.f zlacp2.f zlacpy.f zlacrm.f zlacrt.f zladiv.f
   zlaed0.f zlaed7.f zlaed8.f
   zlaein.f zlaesy.f zlaev2.f zlags2.f zlagtm.f
   zlahef.f zlahef_rook.f zlahef_rk.f zlahef_aa.f zlahqr.f
   zlahr2.f zlaic1.f zlals0.f zlalsa.f zlalsd.f zlangb.f zlange.f
   zlangt.f zlanhb.f
   zlanhe.f
   zlanhp.f zlanhs.f zlanht.f zlansb.f zlansp.f zlansy.f zlantb.f
   zlantp.f zlantr.f zlapll.f zlapmt.f zlaqgb.f zlaqge.f
   zlaqhb.f zlaqhe.f zlaqhp.f zlaqp2.f zlaqp2rk.f zlaqp3rk.f zlaqps.f zlaqsb.f
   zlaqr0.f zlaqr1.f zlaqr2.f zlaqr3.f zlaqr4.f zlaqr5.f
   zlaqsp.f zlaqsy.f zlar1v.f zlar2v.f ilazlr.f ilazlc.f
   zlarcm.f zlarf.f  zlarfb.f zlarfb_gett.f
   zlarfg.f zlarfgp.f zlarft.f
   zlarfx.f zlarfy.f zlargv.f zlarnv.f zlarrv.f zlartg.f90 zlartv.f
   zlarz.f  zlarzb.f zlarzt.f zlascl.f zlaset.f zlasr.f
   zlassq.f90 zlasyf.f zlasyf_rook.f zlasyf_rk.f zlasyf_aa.f
   zlatbs.f zlatdf.f zlatps.f zlatrd.f zlatrs.f zlatrz.f
   zpbcon.f zpbequ.f zpbrfs.f zpbstf.f zpbsv.f
   zpbsvx.f zpbtf2.f zpbtrf.f zpbtrs.f zpocon.f zpoequ.f zporfs.f
   zposv.f  zposvx.f zpotrf2.f zpotri.f zpotrs.f zpstrf.f zpstf2.f
   zppcon.f zppequ.f zpprfs.f zppsv.f  zppsvx.f zpptrf.f zpptri.f zpptrs.f
   zptcon.f zpteqr.f zptrfs.f zptsv.f  zptsvx.f zpttrf.f zpttrs.f zptts2.f
   zrot.f zrscl.f zspcon.f zsprfs.f zspsv.f
   zspsvx.f zsptrf.f zsptri.f zsptrs.f zdrscl.f zstedc.f
   zstegr.f zstein.f zsteqr.f zsycon.f
   zsyrfs.f zsysv.f  zsysvx.f zsytf2.f zsytrf.f zsytri.f
   zsytri2.f zsytri2x.f zsyswapr.f
   zsytrs.f zsytrs2.f
   zsyconv.f zsyconvf.f zsyconvf_rook.f
   zsytf2_rook.f zsytrf_rook.f zsytrs_rook.f zsytrs_aa.f zsytrs_aa_2stage.f
   zsytri_rook.f zsycon_rook.f zsysv_rook.f
   zsytf2_rk.f zsytrf_rk.f zsytrf_aa.f zsytrf_aa_2stage.f zsytrs_3.f
   zsytri_3.f zsytri_3x.f zsycon_3.f zsysv_rk.f zsysv_aa.f zsysv_aa_2stage.f
   ztbcon.f ztbrfs.f ztbtrs.f ztgevc.f ztgex2.f
   ztgexc.f ztgsen.f ztgsja.f ztgsna.f ztgsy2.f ztgsyl.f ztpcon.f
   ztprfs.f ztptri.f
   ztptrs.f ztrcon.f ztrevc.f ztrevc3.f ztrexc.f ztrrfs.f ztrsen.f ztrsna.f
   ztrsyl.f ztrtrs.f ztzrzf.f zung2l.f
   zung2r.f zungbr.f zunghr.f zungl2.f zunglq.f zungql.f zungqr.f zungr2.f
   zungrq.f zungtr.f zunm2l.f zunm2r.f zunmbr.f zunmhr.f zunml2.f zunm22.f
   zunmlq.f zunmql.f zunmqr.f zunmr2.f zunmr3.f zunmrq.f zunmrz.f
   zunmtr.f zupgtr.f
   zupmtr.f izmax1.f dzsum1.f zstemr.f
   zcgesv.f zcposv.f zlag2c.f clag2z.f zlat2c.f
   zhfrk.f ztfttp.f zlanhf.f zpftrf.f zpftri.f zpftrs.f ztfsm.f ztftri.f
   ztfttr.f ztpttf.f ztpttr.f ztrttf.f ztrttp.f
   zgeequb.f zgbequb.f zsyequb.f zpoequb.f zheequb.f
   zbbcsd.f zlapmr.f zunbdb.f zunbdb1.f zunbdb2.f zunbdb3.f zunbdb4.f
   zunbdb5.f zunbdb6.f zuncsd.f zuncsd2by1.f
   zgeqrt.f zgeqrt2.f zgeqrt3.f zgemqrt.f
   ztpqrt.f ztpqrt2.f ztpmqrt.f ztprfb.f
   ztplqt.f ztplqt2.f ztpmlqt.f
   zgelqt.f zgelqt3.f zgemlqt.f
   zgetsls.f zgetsqrhrt.f zgeqr.f zlatsqr.f zlamtsqr.f zgemqr.f
   zgelq.f zlaswlq.f zlamswlq.f zgemlq.f
   zhetrd_2stage.f zhetrd_he2hb.f zhetrd_hb2st.F zhb2st_kernels.f
   zheevd_2stage.f zheev_2stage.f zheevx_2stage.f zheevr_2stage.f
   zhbev_2stage.f zhbevx_2stage.f zhbevd_2stage.f zhegv_2stage.f
   zgesvdq.f zlaunhr_col_getrfnp.f zlaunhr_col_getrfnp2.f
   zungtsqr.f zungtsqr_row.f zunhr_col.f
   zlatrs3.f ztrsyl3.f zgelst.f zgedmd.f90 zgedmdq.f90)

set(ZXLASRC zgesvxx.f zgerfsx.f zla_gerfsx_extended.f zla_geamv.f
   zla_gercond_c.f zla_gercond_x.f zla_gerpvgrw.f zsysvxx.f zsyrfsx.f
   zla_syrfsx_extended.f zla_syamv.f zla_syrcond_c.f zla_syrcond_x.f
   zla_syrpvgrw.f zposvxx.f zporfsx.f zla_porfsx_extended.f
   zla_porcond_c.f zla_porcond_x.f zla_porpvgrw.f zgbsvxx.f zgbrfsx.f
   zla_gbrfsx_extended.f zla_gbamv.f zla_gbrcond_c.f zla_gbrcond_x.f
   zla_gbrpvgrw.f zhesvxx.f zherfsx.f zla_herfsx_extended.f
   zla_heamv.f zla_hercond_c.f zla_hercond_x.f zla_herpvgrw.f
   zla_lin_berr.f zlarscl2.f zlascl2.f zla_wwaddw.f)


if(USE_XBLAS)
  set(ALLXOBJ ${SXLASRC} ${DXLASRC} ${CXLASRC} ${ZXLASRC})
endif()

if(BUILD_LAPACK_DEPRECATED)
list(APPEND SLASRC DEPRECATED/sgegs.f DEPRECATED/sgegv.f
  DEPRECATED/sgelqs.f DEPRECATED/sgeqrs.f
  DEPRECATED/sgeqpf.f DEPRECATED/sgelsx.f DEPRECATED/sggsvd.f
  DEPRECATED/sggsvp.f DEPRECATED/slahrd.f DEPRECATED/slatzm.f DEPRECATED/stzrqf.f)
list(APPEND DLASRC DEPRECATED/dgegs.f DEPRECATED/dgegv.f
  DEPRECATED/dgelqs.f DEPRECATED/dgeqrs.f
  DEPRECATED/dgeqpf.f DEPRECATED/dgelsx.f DEPRECATED/dggsvd.f
  DEPRECATED/dggsvp.f DEPRECATED/dlahrd.f DEPRECATED/dlatzm.f DEPRECATED/dtzrqf.f)
list(APPEND CLASRC DEPRECATED/cgegs.f DEPRECATED/cgegv.f
  DEPRECATED/cgelqs.f DEPRECATED/cgeqrs.f
  DEPRECATED/cgeqpf.f DEPRECATED/cgelsx.f DEPRECATED/cggsvd.f
  DEPRECATED/cggsvp.f DEPRECATED/clahrd.f DEPRECATED/clatzm.f DEPRECATED/ctzrqf.f)
list(APPEND ZLASRC DEPRECATED/zgegs.f DEPRECATED/zgegv.f
  DEPRECATED/zgelqs.f DEPRECATED/zgeqrs.f
  DEPRECATED/zgeqpf.f DEPRECATED/zgelsx.f DEPRECATED/zggsvd.f
  DEPRECATED/zggsvp.f DEPRECATED/zlahrd.f DEPRECATED/zlatzm.f DEPRECATED/ztzrqf.f)
message(STATUS "Building deprecated routines")
endif()

set(DSLASRC spotrs.f)

set(ZCLASRC cpotrs.f)

set(SCATGEN slatm1.f slaran.f slarnd.f)

set(SMATGEN slatms.f slatme.f slatmr.f slatmt.f
   slagge.f slagsy.f slakf2.f slarge.f slaror.f slarot.f slatm2.f
   slatm3.f slatm5.f slatm6.f slatm7.f slahilb.f)

set(CMATGEN clatms.f clatme.f clatmr.f clatmt.f
   clagge.f claghe.f clagsy.f clakf2.f clarge.f claror.f clarot.f
   clatm1.f clarnd.f clatm2.f clatm3.f clatm5.f clatm6.f clahilb.f slatm7.f)

set(DZATGEN dlatm1.f dlaran.f dlarnd.f)

set(DMATGEN dlatms.f dlatme.f dlatmr.f dlatmt.f
   dlagge.f dlagsy.f dlakf2.f dlarge.f dlaror.f dlarot.f dlatm2.f
   dlatm3.f dlatm5.f dlatm6.f dlatm7.f dlahilb.f)

set(ZMATGEN zlatms.f zlatme.f zlatmr.f zlatmt.f
  zlagge.f zlaghe.f zlagsy.f zlakf2.f zlarge.f zlaror.f zlarot.f
  zlatm1.f zlarnd.f zlatm2.f zlatm3.f zlatm5.f zlatm6.f zlahilb.f dlatm7.f)

if(BUILD_SINGLE)
  set(LA_REL_SRC ${SLASRC} ${DSLASRC} ${ALLAUX} ${SCLAUX})
  set(LA_GEN_SRC ${SMATGEN} ${SCATGEN})
  message(STATUS "Building Single Precision")
endif()
if(BUILD_DOUBLE)
  set(LA_REL_SRC ${LA_REL_SRC} ${DLASRC} ${DSLASRC} ${ALLAUX} ${DZLAUX})
  set(LA_GEN_SRC ${LA_GEN_SRC} ${DMATGEN} ${DZATGEN})
  message(STATUS "Building Double Precision")
endif()
if(BUILD_COMPLEX)
  set(LA_REL_SRC ${LA_REL_SRC} ${CLASRC} ${ZCLASRC} ${ALLAUX} ${SCLAUX})
  SET(LA_GEN_SRC ${LA_GEN_SRC} ${CMATGEN} ${SCATGEN})
  message(STATUS "Building Single Precision Complex")
endif()
if(BUILD_COMPLEX16)
  set(LA_REL_SRC ${LA_REL_SRC} ${ZLASRC} ${ZCLASRC} ${ALLAUX} ${DZLAUX})
  SET(LA_GEN_SRC ${LA_GEN_SRC} ${ZMATGEN} ${DZATGEN})
# for zlange/zlanhe
  if (NOT BUILD_DOUBLE)
    set (LA_REL_SRC ${LA_REL_SRC} dcombssq.f)
  endif	()  
  message(STATUS "Building Double Precision Complex")
endif()

else ()

	message (STATUS "c lapack")
set(ALLAUX ilaenv.c ilaenv2stage.c ieeeck.c lsamen.c iparmq.c iparam2stage.c
   ilaprec.c ilatrans.c ilauplo.c iladiag.c chla_transtype.c dlaset.c
   ../INSTALL/ilaver.c xerbla_array.c
   ../INSTALL/slamch.c)

set(SCLAUX
	scombssq.c sbdsvdx.c sstevx.c sstein.c
   sbdsdc.c
   sbdsqr.c sdisna.c slabad.c slacpy.c sladiv.c slae2.c  slaebz.c
   slaed0.c slaed1.c slaed2.c slaed3.c slaed4.c slaed5.c slaed6.c
   slaed7.c slaed8.c slaed9.c slaeda.c slaev2.c slagtf.c
   slagts.c slamrg.c slanst.c
   slapy2.c slapy3.c slarnv.c
   slarra.c slarrb.c slarrc.c slarrd.c slarre.c slarrf.c slarrj.c
   slarrk.c slarrr.c slaneg.c
   slartg.c slaruv.c slas2.c  slascl.c
   slasd0.c slasd1.c slasd2.c slasd3.c slasd4.c slasd5.c slasd6.c
   slasd7.c slasd8.c slasda.c slasdq.c slasdt.c
   slaset.c slasq1.c slasq2.c slasq3.c slasq4.c slasq5.c slasq6.c
   slasr.c  slasrt.c slassq.c slasv2.c spttrf.c sstebz.c sstedc.c
   ssteqr.c ssterf.c slaisnan.c sisnan.c
   slartgp.c slartgs.c slarmm.c
   ../INSTALL/second_${TIMER}.c)

set(DZLAUX
   dbdsdc.c
   dbdsvdx.c dstevx.c dstein.c
   dbdsqr.c ddisna.c dlabad.c dlacpy.c dladiv.c dlae2.c  dlaebz.c
   dlaed0.c dlaed1.c dlaed2.c dlaed3.c dlaed4.c dlaed5.c dlaed6.c
   dlaed7.c dlaed8.c dlaed9.c dlaeda.c dlaev2.c dlagtf.c
   dlagts.c dlamrg.c dlanst.c
   dlapy2.c dlapy3.c dlarnv.c
   dlarra.c dlarrb.c dlarrc.c dlarrd.c dlarre.c dlarrf.c dlarrj.c
   dlarrk.c dlarrr.c dlaneg.c
   dlartg.c dlaruv.c dlas2.c  dlascl.c
   dlasd0.c dlasd1.c dlasd2.c dlasd3.c dlasd4.c dlasd5.c dlasd6.c
   dlasd7.c dlasd8.c dlasda.c dlasdq.c dlasdt.c
   dlasq1.c dlasq2.c dlasq3.c dlasq4.c dlasq5.c dlasq6.c
   dlasr.c  dlasrt.c dlassq.c dlasv2.c dpttrf.c dstebz.c dstedc.c
   dsteqr.c dsterf.c dlaisnan.c disnan.c
   dlartgp.c dlartgs.c dlarmm.c
   ../INSTALL/dlamch.c ../INSTALL/dsecnd_${TIMER}.c)

set(SLASRC
   sgbbrd.c sgbcon.c sgbequ.c sgbrfs.c sgbsv.c
   sgbsvx.c sgbtf2.c sgbtrf.c sgbtrs.c sgebak.c sgebal.c sgebd2.c
   sgebrd.c sgecon.c sgeequ.c sgees.c  sgeesx.c sgeev.c  sgeevx.c
   sgehd2.c sgehrd.c sgelq2.c sgelqf.c
   sgels.c  sgelsd.c sgelss.c sgelsy.c sgeql2.c sgeqlf.c
   sgeqp3.c sgeqp3rk.c sgeqr2.c sgeqr2p.c sgeqrf.c sgeqrfp.c sgerfs.c sgerq2.c sgerqf.c
   sgesc2.c sgesdd.c sgesvd.c sgesvdx.c sgesvx.c sgetc2.c
   sgetrf2.c sgetri.c
   sggbak.c sggbal.c
   sgges.c  sgges3.c sggesx.c sggev.c  sggev3.c sggevx.c
   sggglm.c sgghrd.c sgghd3.c sgglse.c sggqrf.c
   sggrqf.c sggsvd3.c sggsvp3.c sgtcon.c sgtrfs.c sgtsv.c
   sgtsvx.c sgttrf.c sgttrs.c sgtts2.c shgeqz.c
   shsein.c shseqr.c slabrd.c slacon.c slacn2.c
   slaein.c slaexc.c slag2.c  slags2.c slagtm.c slagv2.c slahqr.c
   slahr2.c slaic1.c slaln2.c slals0.c slalsa.c slalsd.c
   slangb.c slange.c slangt.c slanhs.c slansb.c slansp.c
   slansy.c slantb.c slantp.c slantr.c slanv2.c
   slapll.c slapmt.c
   slaqgb.c slaqge.c slaqp2.c slaqp2rk.c slaqp3rk.c slaqps.c slaqsb.c slaqsp.c slaqsy.c
   slaqr0.c slaqr1.c slaqr2.c slaqr3.c slaqr4.c slaqr5.c
   slaqtr.c slar1v.c slar2v.c ilaslr.c ilaslc.c
   slarf.c  slarfb.c slarfb_gett.c slarfg.c slarfgp.c slarft.c slarfx.c slarfy.c slargv.c
   slarrv.c slartv.c
   slarz.c  slarzb.c slarzt.c slasy2.c
   slasyf.c slasyf_rook.c slasyf_rk.c slasyf_aa.c
   slatbs.c slatdf.c slatps.c slatrd.c slatrs.c slatrz.c
   sopgtr.c sopmtr.c sorg2l.c sorg2r.c
   sorgbr.c sorghr.c sorgl2.c sorglq.c sorgql.c sorgqr.c sorgr2.c
   sorgrq.c sorgtr.c sorm2l.c sorm2r.c sorm22.c
   sormbr.c sormhr.c sorml2.c sormlq.c sormql.c sormqr.c sormr2.c
   sormr3.c sormrq.c sormrz.c sormtr.c spbcon.c spbequ.c spbrfs.c
   spbstf.c spbsv.c  spbsvx.c
   spbtf2.c spbtrf.c spbtrs.c spocon.c spoequ.c sporfs.c sposv.c
   sposvx.c spotrf2.c spotri.c spstrf.c spstf2.c
   sppcon.c sppequ.c
   spprfs.c sppsv.c  sppsvx.c spptrf.c spptri.c spptrs.c sptcon.c
   spteqr.c sptrfs.c sptsv.c  sptsvx.c spttrs.c sptts2.c srscl.c
   ssbev.c  ssbevd.c ssbevx.c ssbgst.c ssbgv.c  ssbgvd.c ssbgvx.c
   ssbtrd.c sspcon.c sspev.c  sspevd.c sspevx.c sspgst.c
   sspgv.c  sspgvd.c sspgvx.c ssprfs.c sspsv.c  sspsvx.c ssptrd.c
   ssptrf.c ssptri.c ssptrs.c sstegr.c sstev.c  sstevd.c sstevr.c
   ssycon.c ssyev.c  ssyevd.c ssyevr.c ssyevx.c ssygs2.c
   ssygst.c ssygv.c  ssygvd.c ssygvx.c ssyrfs.c ssysv.c  ssysvx.c
   ssytd2.c ssytf2.c ssytrd.c ssytrf.c ssytri.c ssytri2.c ssytri2x.c
   ssyswapr.c ssytrs.c ssytrs2.c
   ssyconv.c ssyconvf.c ssyconvf_rook.c
   ssysv_aa.c ssysv_aa_2stage.c ssytrf_aa.c ssytrf_aa_2stage.c ssytrs_aa.c ssytrs_aa_2stage.c
   ssytf2_rook.c ssytrf_rook.c ssytrs_rook.c
   ssytri_rook.c ssycon_rook.c ssysv_rook.c
   ssytf2_rk.c ssytrf_rk.c ssytrs_3.c
   ssytri_3.c ssytri_3x.c ssycon_3.c ssysv_rk.c
   ssysv_aa.c ssytrf_aa.c ssytrs_aa.c
   stbcon.c
   stbrfs.c stbtrs.c stgevc.c stgex2.c stgexc.c stgsen.c
   stgsja.c stgsna.c stgsy2.c stgsyl.c stpcon.c stprfs.c stptri.c
   stptrs.c
   strcon.c strevc.c strevc3.c strexc.c strrfs.c strsen.c strsna.c strsyl.c
   strtrs.c stzrzf.c sstemr.c
   slansf.c spftrf.c spftri.c spftrs.c ssfrk.c stfsm.c stftri.c stfttp.c
   stfttr.c stpttf.c stpttr.c strttf.c strttp.c
   sgejsv.c sgesvj.c sgsvj0.c sgsvj1.c
   sgeequb.c ssyequb.c spoequb.c sgbequb.c
   sbbcsd.c slapmr.c sorbdb.c sorbdb1.c sorbdb2.c sorbdb3.c sorbdb4.c
   sorbdb5.c sorbdb6.c sorcsd.c sorcsd2by1.c
   sgeqrt.c sgeqrt2.c sgeqrt3.c sgemqrt.c
   stpqrt.c stpqrt2.c stpmqrt.c stprfb.c
   sgelqt.c sgelqt3.c sgemlqt.c
   sgetsls.c sgetsqrhrt.c sgeqr.c slatsqr.c slamtsqr.c sgemqr.c
   sgelq.c slaswlq.c slamswlq.c sgemlq.c
   stplqt.c stplqt2.c stpmlqt.c
   ssytrd_2stage.c ssytrd_sy2sb.c ssytrd_sb2st.c ssb2st_kernels.c
   ssyevd_2stage.c ssyev_2stage.c ssyevx_2stage.c ssyevr_2stage.c
   ssbev_2stage.c ssbevx_2stage.c ssbevd_2stage.c ssygv_2stage.c
   sgesvdq.c slaorhr_col_getrfnp.c
   slaorhr_col_getrfnp2.c sorgtsqr.c sorgtsqr_row.c sorhr_col.c 
   slatrs3.c strsyl3.c sgelst.c sgedmd.c sgedmdq.c)

set(SXLASRC sgesvxx.c sgerfsx.c sla_gerfsx_extended.c sla_geamv.c
   sla_gercond.c sla_gerpvgrw.c ssysvxx.c ssyrfsx.c
   sla_syrfsx_extended.c sla_syamv.c sla_syrcond.c sla_syrpvgrw.c
   sposvxx.c sporfsx.c sla_porfsx_extended.c sla_porcond.c
   sla_porpvgrw.c sgbsvxx.c sgbrfsx.c sla_gbrfsx_extended.c
   sla_gbamv.c sla_gbrcond.c sla_gbrpvgrw.c sla_lin_berr.c slarscl2.c
   slascl2.c sla_wwaddw.c)

set(CLASRC
   cbdsqr.c cgbbrd.c cgbcon.c cgbequ.c cgbrfs.c cgbsv.c  cgbsvx.c
   cgbtf2.c cgbtrf.c cgbtrs.c cgebak.c cgebal.c cgebd2.c cgebrd.c
   cgecon.c cgeequ.c cgees.c  cgeesx.c cgeev.c  cgeevx.c
   cgehd2.c cgehrd.c cgelq2.c cgelqf.c
   cgels.c  cgelsd.c cgelss.c cgelsy.c cgeql2.c cgeqlf.c cgeqp3.c cgeqp3rk.c
   cgeqr2.c cgeqr2p.c cgeqrf.c cgeqrfp.c cgerfs.c cgerq2.c cgerqf.c
   cgesc2.c cgesdd.c cgesvd.c cgesvdx.c
   cgesvj.c cgejsv.c cgsvj0.c cgsvj1.c
   cgesvx.c cgetc2.c cgetrf2.c
   cgetri.c
   cggbak.c cggbal.c
   cgges.c  cgges3.c cggesx.c cggev.c  cggev3.c cggevx.c
   cggglm.c cgghrd.c cgghd3.c cgglse.c cggqrf.c cggrqf.c
   cggsvd3.c cggsvp3.c
   cgtcon.c cgtrfs.c cgtsv.c  cgtsvx.c cgttrf.c cgttrs.c cgtts2.c chbev.c
   chbevd.c chbevx.c chbgst.c chbgv.c  chbgvd.c chbgvx.c chbtrd.c
   checon.c cheev.c  cheevd.c cheevr.c cheevx.c chegs2.c chegst.c
   chegv.c  chegvd.c chegvx.c cherfs.c chesv.c  chesvx.c chetd2.c
   chetf2.c chetrd.c
   chetrf.c chetri.c chetri2.c chetri2x.c cheswapr.c
   chetrs.c chetrs2.c
   chetf2_rook.c chetrf_rook.c chetri_rook.c
   chetrs_rook.c checon_rook.c chesv_rook.c
   chetf2_rk.c chetrf_rk.c chetri_3.c chetri_3x.c
   chetrs_3.c checon_3.c chesv_rk.c
   chesv_aa.c chesv_aa_2stage.c chetrf_aa.c chetrf_aa_2stage.c chetrs_aa.c chetrs_aa_2stage.c
   chgeqz.c chpcon.c chpev.c  chpevd.c
   chpevx.c chpgst.c chpgv.c  chpgvd.c chpgvx.c chprfs.c chpsv.c
   chpsvx.c
   chptrd.c chptrf.c chptri.c chptrs.c chsein.c chseqr.c clabrd.c
   clacgv.c clacon.c clacn2.c clacp2.c clacpy.c clacrm.c clacrt.c cladiv.c
   claed0.c claed7.c claed8.c
   claein.c claesy.c claev2.c clags2.c clagtm.c
   clahef.c clahef_rook.c clahef_rk.c clahef_aa.c clahqr.c
   clahr2.c claic1.c clals0.c clalsa.c clalsd.c clangb.c clange.c clangt.c
   clanhb.c clanhe.c
   clanhp.c clanhs.c clanht.c clansb.c clansp.c clansy.c clantb.c
   clantp.c clantr.c clapll.c clapmt.c clarcm.c claqgb.c claqge.c
   claqhb.c claqhe.c claqhp.c claqp2.c claqp2rk.c claqp3rk.c claqps.c claqsb.c
   claqr0.c claqr1.c claqr2.c claqr3.c claqr4.c claqr5.c
   claqsp.c claqsy.c clar1v.c clar2v.c ilaclr.c ilaclc.c
   clarf.c  clarfb.c clarfb_gett.c clarfg.c clarfgp.c clarft.c
   clarfx.c clarfy.c clargv.c clarnv.c clarrv.c clartg.c clartv.c
   clarz.c  clarzb.c clarzt.c clascl.c claset.c clasr.c  classq.c
   clasyf.c clasyf_rook.c clasyf_rk.c clasyf_aa.c
   clatbs.c clatdf.c clatps.c clatrd.c clatrs.c clatrz.c
   cpbcon.c cpbequ.c cpbrfs.c cpbstf.c cpbsv.c
   cpbsvx.c cpbtf2.c cpbtrf.c cpbtrs.c cpocon.c cpoequ.c cporfs.c
   cposv.c  cposvx.c cpotrf2.c cpotri.c cpstrf.c cpstf2.c
   cppcon.c cppequ.c cpprfs.c cppsv.c  cppsvx.c cpptrf.c cpptri.c cpptrs.c
   cptcon.c cpteqr.c cptrfs.c cptsv.c  cptsvx.c cpttrf.c cpttrs.c cptts2.c
   crot.c crscl.c  cspcon.c csprfs.c cspsv.c
   cspsvx.c csptrf.c csptri.c csptrs.c csrscl.c cstedc.c
   cstegr.c cstein.c csteqr.c csycon.c
   csyrfs.c csysv.c  csysvx.c csytf2.c csytrf.c csytri.c
   csytri2.c csytri2x.c csyswapr.c
   csytrs.c csytrs2.c
   csyconv.c csyconvf.c csyconvf_rook.c
   csytf2_rook.c csytrf_rook.c csytrs_rook.c
   csytri_rook.c csycon_rook.c csysv_rook.c
   csytf2_rk.c csytrf_rk.c csytrf_aa.c csytrf_aa_2stage.c csytrs_3.c csytrs_aa.c csytrs_aa_2stage.c
   csytri_3.c csytri_3x.c csycon_3.c csysv_rk.c csysv_aa.c csysv_aa_2stage.c
   ctbcon.c ctbrfs.c ctbtrs.c ctgevc.c ctgex2.c
   ctgexc.c ctgsen.c ctgsja.c ctgsna.c ctgsy2.c ctgsyl.c ctpcon.c
   ctprfs.c ctptri.c
   ctptrs.c ctrcon.c ctrevc.c ctrevc3.c ctrexc.c ctrrfs.c ctrsen.c ctrsna.c
   ctrsyl.c ctrtrs.c ctzrzf.c cung2l.c cung2r.c
   cungbr.c cunghr.c cungl2.c cunglq.c cungql.c cungqr.c cungr2.c
   cungrq.c cungtr.c cunm2l.c cunm2r.c cunmbr.c cunmhr.c cunml2.c cunm22.c
   cunmlq.c cunmql.c cunmqr.c cunmr2.c cunmr3.c cunmrq.c cunmrz.c
   cunmtr.c cupgtr.c cupmtr.c icmax1.c scsum1.c cstemr.c
   chfrk.c ctfttp.c clanhf.c cpftrf.c cpftri.c cpftrs.c ctfsm.c ctftri.c
   ctfttr.c ctpttf.c ctpttr.c ctrttf.c ctrttp.c
   cgeequb.c cgbequb.c csyequb.c cpoequb.c cheequb.c
   cbbcsd.c clapmr.c cunbdb.c cunbdb1.c cunbdb2.c cunbdb3.c cunbdb4.c
   cunbdb5.c cunbdb6.c cuncsd.c cuncsd2by1.c
   cgeqrt.c cgeqrt2.c cgeqrt3.c cgemqrt.c
   ctpqrt.c ctpqrt2.c ctpmqrt.c ctprfb.c
   cgelqt.c cgelqt3.c cgemlqt.c
   cgetsls.c cgetsqrhrt.c cgeqr.c clatsqr.c clamtsqr.c cgemqr.c
   cgelq.c claswlq.c clamswlq.c cgemlq.c
   ctplqt.c ctplqt2.c ctpmlqt.c
   chetrd_2stage.c chetrd_he2hb.c chetrd_hb2st.c chb2st_kernels.c
   cheevd_2stage.c cheev_2stage.c cheevx_2stage.c cheevr_2stage.c
   chbev_2stage.c chbevx_2stage.c chbevd_2stage.c chegv_2stage.c
   cgesvdq.c claunhr_col_getrfnp.c claunhr_col_getrfnp2.c 
   cungtsqr.c cungtsqr_row.c cunhr_col.c 
   clatrs3.c ctrsyl3.c cgelst.c cgedmd.c cgedmdq.c)

set(CXLASRC cgesvxx.c cgerfsx.c cla_gerfsx_extended.c cla_geamv.c
   cla_gercond_c.c cla_gercond_x.c cla_gerpvgrw.c
   csysvxx.c csyrfsx.c cla_syrfsx_extended.c cla_syamv.c
   cla_syrcond_c.c cla_syrcond_x.c cla_syrpvgrw.c
   cposvxx.c cporfsx.c cla_porfsx_extended.c
   cla_porcond_c.c cla_porcond_x.c cla_porpvgrw.c
   cgbsvxx.c cgbrfsx.c cla_gbrfsx_extended.c cla_gbamv.c
   cla_gbrcond_c.c cla_gbrcond_x.c cla_gbrpvgrw.c
   chesvxx.c cherfsx.c cla_herfsx_extended.c cla_heamv.c
   cla_hercond_c.c cla_hercond_x.c cla_herpvgrw.c
   cla_lin_berr.c clarscl2.c clascl2.c cla_wwaddw.c)

set(DLASRC
   dgbbrd.c dgbcon.c dgbequ.c dgbrfs.c dgbsv.c
   dgbsvx.c dgbtf2.c dgbtrf.c dgbtrs.c dgebak.c dgebal.c dgebd2.c
   dgebrd.c dgecon.c dgeequ.c dgees.c  dgeesx.c dgeev.c  dgeevx.c
   dgehd2.c dgehrd.c dgelq2.c dgelqf.c
   dgels.c  dgelsd.c dgelss.c dgelsy.c dgeql2.c dgeqlf.c
   dgeqp3.c dgeqp3rk.c dgeqr2.c dgeqr2p.c dgeqrf.c dgeqrfp.c dgerfs.c dgerq2.c dgerqf.c
   dgesc2.c dgesdd.c dgesvd.c dgesvdx.c dgesvx.c dgetc2.c
   dgetrf2.c dgetri.c
   dggbak.c dggbal.c
   dgges.c  dgges3.c dggesx.c dggev.c  dggev3.c dggevx.c
   dggglm.c dgghrd.c dgghd3.c dgglse.c dggqrf.c
   dggrqf.c dggsvd3.c dggsvp3.c dgtcon.c dgtrfs.c dgtsv.c
   dgtsvx.c dgttrf.c dgttrs.c dgtts2.c dhgeqz.c
   dhsein.c dhseqr.c dlabrd.c dlacon.c dlacn2.c
   dlaein.c dlaexc.c dlag2.c  dlags2.c dlagtm.c dlagv2.c dlahqr.c
   dlahr2.c dlaic1.c dlaln2.c dlals0.c dlalsa.c dlalsd.c
   dlangb.c dlange.c dlangt.c dlanhs.c dlansb.c dlansp.c
   dlansy.c dlantb.c dlantp.c dlantr.c dlanv2.c
   dlapll.c dlapmt.c
   dlaqgb.c dlaqge.c dlaqp2.c dlaqp2rk.c dlaqp3rk.c dlaqps.c dlaqsb.c dlaqsp.c dlaqsy.c
   dlaqr0.c dlaqr1.c dlaqr2.c dlaqr3.c dlaqr4.c dlaqr5.c
   dlaqtr.c dlar1v.c dlar2v.c iladlr.c iladlc.c
   dlarf.c  dlarfb.c dlarfb_gett.c dlarfg.c dlarfgp.c dlarft.c dlarfx.c dlarfy.c
   dlargv.c dlarrv.c dlartv.c
   dlarz.c  dlarzb.c dlarzt.c dlasy2.c
   dlasyf.c dlasyf_rook.c dlasyf_rk.c dlasyf_aa.c
   dlatbs.c dlatdf.c dlatps.c dlatrd.c dlatrs.c dlatrz.c
   dopgtr.c dopmtr.c dorg2l.c dorg2r.c
   dorgbr.c dorghr.c dorgl2.c dorglq.c dorgql.c dorgqr.c dorgr2.c
   dorgrq.c dorgtr.c dorm2l.c dorm2r.c dorm22.c
   dormbr.c dormhr.c dorml2.c dormlq.c dormql.c dormqr.c dormr2.c
   dormr3.c dormrq.c dormrz.c dormtr.c dpbcon.c dpbequ.c dpbrfs.c
   dpbstf.c dpbsv.c  dpbsvx.c
   dpbtf2.c dpbtrf.c dpbtrs.c dpocon.c dpoequ.c dporfs.c dposv.c
   dposvx.c dpotrf2.c dpotri.c dpotrs.c dpstrf.c dpstf2.c
   dppcon.c dppequ.c
   dpprfs.c dppsv.c  dppsvx.c dpptrf.c dpptri.c dpptrs.c dptcon.c
   dpteqr.c dptrfs.c dptsv.c  dptsvx.c dpttrs.c dptts2.c drscl.c
   dsbev.c  dsbevd.c dsbevx.c dsbgst.c dsbgv.c  dsbgvd.c dsbgvx.c
   dsbtrd.c dspcon.c dspev.c  dspevd.c dspevx.c dspgst.c
   dspgv.c  dspgvd.c dspgvx.c dsprfs.c dspsv.c  dspsvx.c dsptrd.c
   dsptrf.c dsptri.c dsptrs.c dstegr.c dstev.c  dstevd.c dstevr.c
   dsycon.c dsyev.c  dsyevd.c dsyevr.c
   dsyevx.c dsygs2.c dsygst.c dsygv.c  dsygvd.c dsygvx.c dsyrfs.c
   dsysv.c  dsysvx.c
   dsytd2.c dsytf2.c dsytrd.c dsytrf.c dsytri.c dsytrs.c dsytrs2.c
   dsytri2.c dsytri2x.c dsyswapr.c
   dsyconv.c dsyconvf.c dsyconvf_rook.c
   dsytf2_rook.c dsytrf_rook.c dsytrs_rook.c
   dsytri_rook.c dsycon_rook.c dsysv_rook.c
   dsytf2_rk.c dsytrf_rk.c dsytrs_3.c
   dsytri_3.c dsytri_3x.c dsycon_3.c dsysv_rk.c
   dsysv_aa.c dsysv_aa_2stage.c dsytrf_aa.c dsytrf_aa_2stage.c dsytrs_aa.c dsytrs_aa_2stage.c
   dtbcon.c
   dtbrfs.c dtbtrs.c dtgevc.c dtgex2.c dtgexc.c dtgsen.c
   dtgsja.c dtgsna.c dtgsy2.c dtgsyl.c dtpcon.c dtprfs.c dtptri.c
   dtptrs.c
   dtrcon.c dtrevc.c dtrevc3.c dtrexc.c dtrrfs.c dtrsen.c dtrsna.c dtrsyl.c
   dtrtrs.c dtzrzf.c dstemr.c
   dsgesv.c dsposv.c dlag2s.c slag2d.c dlat2s.c
   dlansf.c dpftrf.c dpftri.c dpftrs.c dsfrk.c dtfsm.c dtftri.c dtfttp.c
   dtfttr.c dtpttf.c dtpttr.c dtrttf.c dtrttp.c
   dgejsv.c dgesvj.c dgsvj0.c dgsvj1.c
   dgeequb.c dsyequb.c dpoequb.c dgbequb.c
   dbbcsd.c dlapmr.c dorbdb.c dorbdb1.c dorbdb2.c dorbdb3.c dorbdb4.c
   dorbdb5.c dorbdb6.c dorcsd.c dorcsd2by1.c
   dgeqrt.c dgeqrt2.c dgeqrt3.c dgemqrt.c
   dtpqrt.c dtpqrt2.c dtpmqrt.c dtprfb.c
   dgelqt.c dgelqt3.c dgemlqt.c
   dgetsls.c dgetsqrhrt.c dgeqr.c dlatsqr.c dlamtsqr.c dgemqr.c
   dgelq.c dlaswlq.c dlamswlq.c dgemlq.c
   dtplqt.c dtplqt2.c dtpmlqt.c
   dsytrd_2stage.c dsytrd_sy2sb.c dsytrd_sb2st.c dsb2st_kernels.c
   dsyevd_2stage.c dsyev_2stage.c dsyevx_2stage.c dsyevr_2stage.c
   dsbev_2stage.c dsbevx_2stage.c dsbevd_2stage.c dsygv_2stage.c
   dcombssq.c dgesvdq.c dlaorhr_col_getrfnp.c
   dlaorhr_col_getrfnp2.c dorgtsqr.c dorgtsqr_row.c dorhr_col.c 
   dlatrs3.c dtrsyl3.c dgelst.c dgedmd.c dgedmdq.c)

set(DXLASRC dgesvxx.c dgerfsx.c dla_gerfsx_extended.c dla_geamv.c
   dla_gercond.c dla_gerpvgrw.c dsysvxx.c dsyrfsx.c
   dla_syrfsx_extended.c dla_syamv.c dla_syrcond.c dla_syrpvgrw.c
   dposvxx.c dporfsx.c dla_porfsx_extended.c dla_porcond.c
   dla_porpvgrw.c dgbsvxx.c dgbrfsx.c dla_gbrfsx_extended.c
   dla_gbamv.c dla_gbrcond.c dla_gbrpvgrw.c dla_lin_berr.c dlarscl2.c
   dlascl2.c dla_wwaddw.c)

set(ZLASRC
   zbdsqr.c zgbbrd.c zgbcon.c zgbequ.c zgbrfs.c zgbsv.c  zgbsvx.c
   zgbtf2.c zgbtrf.c zgbtrs.c zgebak.c zgebal.c zgebd2.c zgebrd.c
   zgecon.c zgeequ.c zgees.c  zgeesx.c zgeev.c  zgeevx.c
   zgehd2.c zgehrd.c zgelq2.c zgelqf.c
   zgels.c  zgelsd.c zgelss.c zgelsy.c zgeql2.c zgeqlf.c zgeqp3.c zgeqp3rk.c
   zgeqr2.c zgeqr2p.c zgeqrf.c zgeqrfp.c zgerfs.c zgerq2.c zgerqf.c
   zgesc2.c zgesdd.c zgesvd.c zgesvdx.c zgesvx.c
   zgesvj.c zgejsv.c zgsvj0.c zgsvj1.c
   zgetc2.c zgetrf2.c
   zgetri.c
   zggbak.c zggbal.c
   zgges.c  zgges3.c zggesx.c zggev.c  zggev3.c zggevx.c
   zggglm.c zgghrd.c zgghd3.c zgglse.c zggqrf.c zggrqf.c
   zggsvd3.c zggsvp3.c
   zgtcon.c zgtrfs.c zgtsv.c  zgtsvx.c zgttrf.c zgttrs.c zgtts2.c zhbev.c
   zhbevd.c zhbevx.c zhbgst.c zhbgv.c  zhbgvd.c zhbgvx.c zhbtrd.c
   zhecon.c zheev.c  zheevd.c zheevr.c zheevx.c zhegs2.c zhegst.c
   zhegv.c  zhegvd.c zhegvx.c zherfs.c zhesv.c  zhesvx.c zhetd2.c
   zhetf2.c zhetrd.c
   zhetrf.c zhetri.c zhetri2.c zhetri2x.c zheswapr.c
   zhetrs.c zhetrs2.c
   zhetf2_rook.c zhetrf_rook.c zhetri_rook.c
   zhetrs_rook.c zhecon_rook.c zhesv_rook.c
   zhetf2_rk.c zhetrf_rk.c zhetri_3.c zhetri_3x.c
   zhetrs_3.c zhecon_3.c zhesv_rk.c
   zhesv_aa.c zhesv_aa_2stage.c zhetrf_aa.c zhetrf_aa_2stage.c zhetrs_aa.c zhetrs_aa_2stage.c
   zhgeqz.c zhpcon.c zhpev.c  zhpevd.c
   zhpevx.c zhpgst.c zhpgv.c  zhpgvd.c zhpgvx.c zhprfs.c zhpsv.c
   zhpsvx.c
   zhptrd.c zhptrf.c zhptri.c zhptrs.c zhsein.c zhseqr.c zlabrd.c
   zlacgv.c zlacon.c zlacn2.c zlacp2.c zlacpy.c zlacrm.c zlacrt.c zladiv.c
   zlaed0.c zlaed7.c zlaed8.c
   zlaein.c zlaesy.c zlaev2.c zlags2.c zlagtm.c
   zlahef.c zlahef_rook.c zlahef_rk.c zlahef_aa.c zlahqr.c
   zlahr2.c zlaic1.c zlals0.c zlalsa.c zlalsd.c zlangb.c zlange.c
   zlangt.c zlanhb.c
   zlanhe.c
   zlanhp.c zlanhs.c zlanht.c zlansb.c zlansp.c zlansy.c zlantb.c
   zlantp.c zlantr.c zlapll.c zlapmt.c zlaqgb.c zlaqge.c
   zlaqhb.c zlaqhe.c zlaqhp.c zlaqp2.c zlaqp2rk.c zlaqp3rk.c zlaqps.c zlaqsb.c
   zlaqr0.c zlaqr1.c zlaqr2.c zlaqr3.c zlaqr4.c zlaqr5.c
   zlaqsp.c zlaqsy.c zlar1v.c zlar2v.c ilazlr.c ilazlc.c
   zlarcm.c zlarf.c  zlarfb.c zlarfb_gett.c
   zlarfg.c zlarfgp.c zlarft.c
   zlarfx.c zlarfy.c zlargv.c zlarnv.c zlarrv.c zlartg.c zlartv.c
   zlarz.c  zlarzb.c zlarzt.c zlascl.c zlaset.c zlasr.c
   zlassq.c zlasyf.c zlasyf_rook.c zlasyf_rk.c zlasyf_aa.c
   zlatbs.c zlatdf.c zlatps.c zlatrd.c zlatrs.c zlatrz.c
   zpbcon.c zpbequ.c zpbrfs.c zpbstf.c zpbsv.c
   zpbsvx.c zpbtf2.c zpbtrf.c zpbtrs.c zpocon.c zpoequ.c zporfs.c
   zposv.c  zposvx.c zpotrf2.c zpotri.c zpotrs.c zpstrf.c zpstf2.c
   zppcon.c zppequ.c zpprfs.c zppsv.c  zppsvx.c zpptrf.c zpptri.c zpptrs.c
   zptcon.c zpteqr.c zptrfs.c zptsv.c  zptsvx.c zpttrf.c zpttrs.c zptts2.c
   zrot.c zrscl.c  zspcon.c zsprfs.c zspsv.c
   zspsvx.c zsptrf.c zsptri.c zsptrs.c zdrscl.c zstedc.c
   zstegr.c zstein.c zsteqr.c zsycon.c
   zsyrfs.c zsysv.c  zsysvx.c zsytf2.c zsytrf.c zsytri.c
   zsytri2.c zsytri2x.c zsyswapr.c
   zsytrs.c zsytrs2.c
   zsyconv.c zsyconvf.c zsyconvf_rook.c
   zsytf2_rook.c zsytrf_rook.c zsytrs_rook.c zsytrs_aa.c zsytrs_aa_2stage.c
   zsytri_rook.c zsycon_rook.c zsysv_rook.c
   zsytf2_rk.c zsytrf_rk.c zsytrf_aa.c zsytrf_aa_2stage.c zsytrs_3.c
   zsytri_3.c zsytri_3x.c zsycon_3.c zsysv_rk.c zsysv_aa.c zsysv_aa_2stage.c
   ztbcon.c ztbrfs.c ztbtrs.c ztgevc.c ztgex2.c
   ztgexc.c ztgsen.c ztgsja.c ztgsna.c ztgsy2.c ztgsyl.c ztpcon.c
   ztprfs.c ztptri.c
   ztptrs.c ztrcon.c ztrevc.c ztrevc3.c ztrexc.c ztrrfs.c ztrsen.c ztrsna.c
   ztrsyl.c ztrtrs.c ztzrzf.c zung2l.c
   zung2r.c zungbr.c zunghr.c zungl2.c zunglq.c zungql.c zungqr.c zungr2.c
   zungrq.c zungtr.c zunm2l.c zunm2r.c zunmbr.c zunmhr.c zunml2.c zunm22.c
   zunmlq.c zunmql.c zunmqr.c zunmr2.c zunmr3.c zunmrq.c zunmrz.c
   zunmtr.c zupgtr.c
   zupmtr.c izmax1.c dzsum1.c zstemr.c
   zcgesv.c zcposv.c zlag2c.c clag2z.c zlat2c.c
   zhfrk.c ztfttp.c zlanhf.c zpftrf.c zpftri.c zpftrs.c ztfsm.c ztftri.c
   ztfttr.c ztpttf.c ztpttr.c ztrttf.c ztrttp.c
   zgeequb.c zgbequb.c zsyequb.c zpoequb.c zheequb.c
   zbbcsd.c zlapmr.c zunbdb.c zunbdb1.c zunbdb2.c zunbdb3.c zunbdb4.c
   zunbdb5.c zunbdb6.c zuncsd.c zuncsd2by1.c
   zgeqrt.c zgeqrt2.c zgeqrt3.c zgemqrt.c
   ztpqrt.c ztpqrt2.c ztpmqrt.c ztprfb.c
   ztplqt.c ztplqt2.c ztpmlqt.c
   zgelqt.c zgelqt3.c zgemlqt.c
   zgetsls.c zgetsqrhrt.c zgeqr.c zlatsqr.c zlamtsqr.c zgemqr.c
   zgelq.c zlaswlq.c zlamswlq.c zgemlq.c
   zhetrd_2stage.c zhetrd_he2hb.c zhetrd_hb2st.c zhb2st_kernels.c
   zheevd_2stage.c zheev_2stage.c zheevx_2stage.c zheevr_2stage.c
   zhbev_2stage.c zhbevx_2stage.c zhbevd_2stage.c zhegv_2stage.c
   zgesvdq.c zlaunhr_col_getrfnp.c zlaunhr_col_getrfnp2.c
   zungtsqr.c zungtsqr_row.c zunhr_col.c zlatrs3.c ztrsyl3.c zgelst.c
   zgedmd.c zgedmdq.c)

set(ZXLASRC zgesvxx.c zgerfsx.c zla_gerfsx_extended.c zla_geamv.c
   zla_gercond_c.c zla_gercond_x.c zla_gerpvgrw.c zsysvxx.c zsyrfsx.c
   zla_syrfsx_extended.c zla_syamv.c zla_syrcond_c.c zla_syrcond_x.c
   zla_syrpvgrw.c zposvxx.c zporfsx.c zla_porfsx_extended.c
   zla_porcond_c.c zla_porcond_x.c zla_porpvgrw.c zgbsvxx.c zgbrfsx.c
   zla_gbrfsx_extended.c zla_gbamv.c zla_gbrcond_c.c zla_gbrcond_x.c
   zla_gbrpvgrw.c zhesvxx.c zherfsx.c zla_herfsx_extended.c
   zla_heamv.c zla_hercond_c.c zla_hercond_x.c zla_herpvgrw.c
   zla_lin_berr.c zlarscl2.c zlascl2.c zla_wwaddw.c)


if(USE_XBLAS)
  set(ALLXOBJ ${SXLASRC} ${DXLASRC} ${CXLASRC} ${ZXLASRC})
endif()

if(BUILD_LAPACK_DEPRECATED)
list(APPEND SLASRC DEPRECATED/sgegs.c DEPRECATED/sgegv.c
  DEPRECATED/sgelqs.c DEPRECATED/sgeqrs.c
  DEPRECATED/sgeqpf.c DEPRECATED/sgelsx.c DEPRECATED/sggsvd.c
  DEPRECATED/sggsvp.c DEPRECATED/slahrd.c DEPRECATED/slatzm.c DEPRECATED/stzrqf.c)
list(APPEND DLASRC DEPRECATED/dgegs.c DEPRECATED/dgegv.c
  DEPRECATED/dgelqs.c DEPRECATED/dgeqrs.c
  DEPRECATED/dgeqpf.c DEPRECATED/dgelsx.c DEPRECATED/dggsvd.c
  DEPRECATED/dggsvp.c DEPRECATED/dlahrd.c DEPRECATED/dlatzm.c DEPRECATED/dtzrqf.c)
list(APPEND CLASRC DEPRECATED/cgegs.c DEPRECATED/cgegv.c
  DEPRECATED/cgelqs.c DEPRECATED/cgeqrs.c
  DEPRECATED/cgeqpf.c DEPRECATED/cgelsx.c DEPRECATED/cggsvd.c
  DEPRECATED/cggsvp.c DEPRECATED/clahrd.c DEPRECATED/clatzm.c DEPRECATED/ctzrqf.c)
list(APPEND ZLASRC DEPRECATED/zgegs.c DEPRECATED/zgegv.c
  DEPRECATED/zgelqs.c DEPRECATED/zgeqrs.c
  DEPRECATED/zgeqpf.c DEPRECATED/zgelsx.c DEPRECATED/zggsvd.c
  DEPRECATED/zggsvp.c DEPRECATED/zlahrd.c DEPRECATED/zlatzm.c DEPRECATED/ztzrqf.c)
message(STATUS "Building deprecated routines")
endif()

set(DSLASRC spotrs.c)

set(ZCLASRC cpotrs.c)

set(SCATGEN slatm1.c slaran.c slarnd.c)

set(SMATGEN slatms.c slatme.c slatmr.c slatmt.c
   slagge.c slagsy.c slakf2.c slarge.c slaror.c slarot.c slatm2.c
   slatm3.c slatm5.c slatm6.c slatm7.c slahilb.c)

set(CMATGEN clatms.c clatme.c clatmr.c clatmt.c
   clagge.c claghe.c clagsy.c clakf2.c clarge.c claror.c clarot.c
   clatm1.c clarnd.c clatm2.c clatm3.c clatm5.c clatm6.c clahilb.c slatm7.c)

set(DZATGEN dlatm1.c dlaran.c dlarnd.c)

set(DMATGEN dlatms.c dlatme.c dlatmr.c dlatmt.c
   dlagge.c dlagsy.c dlakf2.c dlarge.c dlaror.c dlarot.c dlatm2.c
   dlatm3.c dlatm5.c dlatm6.c dlatm7.c dlahilb.c)

set(ZMATGEN zlatms.c zlatme.c zlatmr.c zlatmt.c
  zlagge.c zlaghe.c zlagsy.c zlakf2.c zlarge.c zlaror.c zlarot.c
  zlatm1.c zlarnd.c zlatm2.c zlatm3.c zlatm5.c zlatm6.c zlahilb.c dlatm7.c)

if(BUILD_SINGLE)
  set(LA_REL_SRC ${SLASRC} ${DSLASRC} ${ALLAUX} ${SCLAUX})
  set(LA_GEN_SRC ${SMATGEN} ${SCATGEN})
  message(STATUS "Building Single Precision")
endif()
if(BUILD_DOUBLE)
  set(LA_REL_SRC ${LA_REL_SRC} ${DLASRC} ${DSLASRC} ${ALLAUX} ${DZLAUX})
  set(LA_GEN_SRC ${LA_GEN_SRC} ${DMATGEN} ${DZATGEN})
  message(STATUS "Building Double Precision")
endif()
if(BUILD_COMPLEX)
  set(LA_REL_SRC ${LA_REL_SRC} ${CLASRC} ${ZCLASRC} ${ALLAUX} ${SCLAUX})
  SET(LA_GEN_SRC ${LA_GEN_SRC} ${CMATGEN} ${SCATGEN})
  message(STATUS "Building Single Precision Complex")
endif()
if(BUILD_COMPLEX16)
  set(LA_REL_SRC ${LA_REL_SRC} ${ZLASRC} ${ZCLASRC} ${ALLAUX} ${DZLAUX})
  SET(LA_GEN_SRC ${LA_GEN_SRC} ${ZMATGEN} ${DZATGEN})
# for zlange/zlanhe
  if (NOT BUILD_DOUBLE)
    set (LA_REL_SRC ${LA_REL_SRC} dcombssq.c)
  endif	()  
  message(STATUS "Building Double Precision Complex")
endif()

endif()

# add lapack-netlib folder to the sources
set(LA_SOURCES "")
foreach (LA_FILE ${LA_REL_SRC})
  list(APPEND LA_SOURCES "${NETLIB_LAPACK_DIR}/SRC/${LA_FILE}")
endforeach ()
foreach (LA_FILE ${LA_GEN_SRC})
  list(APPEND LA_SOURCES "${NETLIB_LAPACK_DIR}/TESTING/MATGEN/${LA_FILE}")
endforeach ()

if (NOT C_LAPACK)
  # The below line is duplicating Fortran flags but NAG has a few flags
  # that cannot be specified twice. It's possible this is not needed for
  # any compiler, but for safety, we only turn off for NAG
  if (NOT ${F_COMPILER} STREQUAL "NAGFOR")
    set_source_files_properties(${LA_SOURCES} PROPERTIES COMPILE_FLAGS "${LAPACK_FFLAGS}")
  endif ()
  if (${F_COMPILER} STREQUAL "GFORTRAN")
    set_source_files_properties(${LA_SOURCES} PROPERTIES COMPILE_FLAGS "${LAPACK_FFLAGS} -fno-tree-vectorize")
  endif()
else ()
  set_source_files_properties(${LA_SOURCES} PROPERTIES COMPILE_FLAGS "${LAPACK_CFLAGS}")
endif ()
