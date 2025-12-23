#define ASSEMBLER
#include "common.h"

/*
	.file	"caxpy.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl caxpy_k
	.type	caxpy_k, @function
*/

	PROLOGUE

#ifdef CONJ
caxpyc_k:
#else
caxpy_k:
#endif
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
#ifdef CONJ
	.localentry	caxpyc_k,.-caxpyc_k
#else
	.localentry	caxpy_k,.-caxpy_k
#endif
	mr. 7,3
	ble 0,.L33
	cmpdi 7,9,1
	beq 7,.L37
.L3:
	mtctr 7
	ld 7,96(1)
	sldi 9,9,3
	sldi 7,7,3
	.p2align 4,,15
.L14:
	lfs 10,4(8)
	lfs 11,0(8)
	lfs 12,0(10)
	lfs 0,4(10)
	fmuls 10,2,10
#ifdef CONJ	
	fmadds 11,11,1,10
#else
	fmsubs 11,11,1,10
#endif
	fadds 12,12,11
	stfs 12,0(10)
	lfs 11,0(8)
	lfs 12,4(8)
	add 8,8,9
	fmuls 11,2,11
#ifdef CONJ
	fmsubs 12,12,1,11
	fsubs 0,0,12
#else
	fmadds 12,12,1,11
	fadds 0,0,12
#endif
	stfs 0,4(10)
	add 10,10,7
	bdnz .L14
.L33:
	li 3,0
	blr
	.p2align 4,,15
.L37:
	ld 6,96(1)
	cmpdi 7,6,1
	bne 7,.L3
	rldicr. 4,7,0,59
	li 11,0
	bne 0,.L38
.L4:
	addi 6,11,8
	subf 0,4,7
	sldi 6,6,2
	addi 9,6,-32
	add 5,10,6
	add 6,8,6
	add 3,8,9
	add 9,10,9
	subfc 5,5,3
	subfe 5,5,5
	subfc 6,6,9
	subfe 12,12,12
	addi 6,5,1
	addi 5,12,1
	or 6,6,5
	rlwinm 6,6,0,0xff
	cmpwi 7,6,0
	beq 7,.L7
	sradi 6,4,63
	srdi 5,7,63
	subfc 12,7,4
	adde 6,5,6
	subfic 12,0,4
	subfe 12,12,12
	xori 6,6,0x1
	neg 12,12
	and 6,6,12
	rlwinm 6,6,0,0xff
	cmpwi 7,6,0
	beq 7,.L7
	cmpd 7,4,7
	li 6,1
	blt 7,.L39
.L9:
	addi 0,7,-1
	subf 0,4,0
	subfic 0,0,3
	subfe 12,12,12
	addi 0,12,1
	rlwinm 0,0,0,0xff
	cmpwi 7,0,0
	bne 7,.L10
	sradi 0,4,63
	subfc 12,7,4
	adde 5,5,0
	rlwinm 5,5,0,0xff
	cmpwi 7,5,0
	bne 7,.L10
	xscvdpspn 0,1
	xscvdpspn 12,2
	addi 0,6,-1
	std 31,-8(1)
	addis 12,2,.LC2@toc@ha
	addis 6,2,.LC3@toc@ha
	li 5,16
	srdi. 31,0,2
	addi 6,6,.LC3@toc@l
	addi 12,12,.LC2@toc@l
	mtctr 31
	lxv 41,0(6)
	lxv 42,0(12)
	li 6,0
	xxspltw 0,0,0
	xxspltw 12,12,0
	beq 0,.L40
	.p2align 4,,15
.L11:
#ifdef CONJ
	lxvx 33,3,5
	lxvx 44,3,6
	lxvx 43,9,6
	lxvx 32,9,5
	vperm 13,1,12,10
	vperm 12,1,12,9
	vperm 8,0,11,10
	vperm 0,0,11,9
	xvmulsp 33,12,44
	xvmulsp 11,12,45
	xvmaddasp 33,0,45
	xvmsubmsp 44,0,11
	xvaddsp 33,33,40
	xvsubsp 32,32,44
#else
	lxvx 33,3,6
	lxvx 32,3,5
	lxvx 43,9,6
	lxvx 44,9,5
	vperm 13,0,1,10
	vperm 0,0,1,9
	vperm 8,12,11,10
	vperm 12,12,11,9
	xvmulsp 33,12,32
	xvmulsp 11,12,45
	xvmsubasp 33,0,45
	xvmaddmsp 32,0,11
	xvaddsp 33,33,40
	xvaddsp 32,32,44
#endif
	vmrglw 13,0,1
	vmrghw 0,0,1
	stxvx 45,9,6
	stxvx 32,9,5
	addi 6,6,32
	addi 5,5,32
	bdnz .L11
	rldicr 0,0,0,61
	ld 31,-8(1)
	sldi 9,0,1
	add 4,4,0
	add 11,11,9
.L10:
	sldi 5,11,2
	addi 6,4,1
	addi 9,11,2
	addi 3,5,4
	lfsx 12,8,5
	cmpd 7,7,6
	lfsx 0,10,5
	lfsx 11,8,3
	fmuls 11,2,11
#ifdef CONJ
	fmadds 12,12,1,11
#else
	fmsubs 12,12,1,11
#endif
	fadds 0,0,12
	stfsx 0,10,5
	lfsx 11,8,5
	lfsx 12,8,3
	lfsx 0,10,3
	fmuls 11,2,11
#ifdef CONJ
	fmsubs 12,12,1,11
	fsubs 0,0,12
#else
	fmadds 12,12,1,11
	fadds 0,0,12
#endif
	stfsx 0,10,3
	ble 7,.L33
	sldi 9,9,2
	addi 5,4,2
	addi 6,11,4
	addi 3,9,4
	lfsx 12,8,9
	cmpd 7,7,5
	lfsx 0,10,9
	lfsx 11,8,3
	fmuls 11,2,11
#ifdef CONJ
	fmadds 12,1,12,11
#else
	fmsubs 12,1,12,11
#endif
	fadds 0,0,12
	stfsx 0,10,9
	lfsx 11,8,9
	lfsx 12,8,3
	lfsx 0,10,3
	fmuls 11,2,11
#ifdef CONJ
	fmsubs 12,1,12,11
	fsubs 0,0,12
#else
	fmadds 12,1,12,11
	fadds 0,0,12
#endif
	stfsx 0,10,3
	ble 7,.L33
	sldi 6,6,2
	addi 4,4,3
	addi 9,11,6
	addi 5,6,4
	lfsx 12,8,6
	cmpd 7,7,4
	lfsx 0,10,6
	lfsx 11,8,5
	fmuls 11,2,11
#ifdef CONJ
	fmadds 12,1,12,11
#else
	fmsubs 12,1,12,11
#endif
	fadds 0,0,12
	stfsx 0,10,6
	lfsx 11,8,6
	lfsx 12,8,5
	lfsx 0,10,5
	fmuls 11,2,11
#ifdef CONJ
	fmsubs 12,1,12,11
	fsubs 0,0,12
#else
	fmadds 12,1,12,11
	fadds 0,0,12
#endif
	stfsx 0,10,5
	ble 7,.L33
	sldi 9,9,2
	addi 7,9,4
	lfsx 12,8,9
	lfsx 0,10,9
	lfsx 11,8,7
	fmuls 11,2,11
#ifdef CONJ
	fmadds 12,1,12,11
#else
	fmsubs 12,1,12,11
#endif
	fadds 0,0,12
	stfsx 0,10,9
	lfsx 11,8,9
	lfsx 12,8,7
	lfsx 0,10,7
	fmuls 2,2,11
#ifdef CONJ
	fmsubs 1,1,12,2
	fsubs 1,0,1
#else
	fmadds 1,1,12,2
	fadds 1,0,1
#endif
	stfsx 1,10,7
	b .L33
.L39:
	mr 6,0
	b .L9
.L38:
#ifdef CONJ
	fneg 0,1
	xxpermdi 45,1,1,0
	xscvdpspn 12,2
	addis 9,2,.LANCHOR0@toc@ha
	sradi. 3,4,1
	xxpermdi 44,0,0,0
	addi 9,9,.LANCHOR0@toc@l
	xvcvdpsp 45,45
	lxv 33,0(9)
	xvcvdpsp 32,44
	xxspltw 12,12,0
#else
	fneg 12,2
	xxpermdi 32,2,2,0
	xscvdpspn 0,1
	addis 9,2,.LANCHOR0@toc@ha
	sradi. 3,4,1
	xxpermdi 45,12,12,0
	addi 9,9,.LANCHOR0@toc@l
	xvcvdpsp 32,32
	lxv 33,0(9)
	xvcvdpsp 45,45
	xxspltw 0,0,0
#endif
	vmrgew 0,0,13
	beq 0,.L5
	mr 6,8
	mr 9,10
	li 5,0
	.p2align 4,,15
.L6:
	lxv 38,16(6)
	lxv 11,16(9)
	addi 5,5,8
	addi 6,6,128
	addi 9,9,128
	lxv 39,-96(6)
	lxv 40,-80(6)
	lxv 41,-64(6)
	lxv 42,-48(6)
	cmpd 7,3,5
	lxv 43,-32(6)
	lxv 45,-128(6)
	lxv 44,-16(6)
#ifdef CONJ
	lxv 0,-128(9)
	vpermr 17,6,6,1
	xvmaddmsp 38,32,11
	lxv 11,-96(9)
	vpermr 18,7,7,1
	vpermr 19,8,8,1
	vpermr 2,9,9,1
	vpermr 3,10,10,1
	vpermr 4,11,11,1
	xvmaddasp 0,32,45
	vpermr 5,12,12,1
	xvmaddmsp 39,32,11
	lxv 11,-80(9)
	vpermr 13,13,13,1
	xvmaddasp 38,12,49
	xvmaddmsp 40,32,11
	lxv 11,-64(9)
	xvmaddmsp 45,12,0
	xvmaddasp 39,12,50
	stxv 38,-112(9)
	xvmaddmsp 41,32,11
	lxv 11,-48(9)
	xvmaddasp 40,12,51
	stxv 45,-128(9)
	stxv 39,-96(9)
	xvmaddmsp 42,32,11
	lxv 11,-32(9)
	xvmaddasp 41,12,34
	stxv 40,-80(9)
	xvmaddmsp 43,32,11
	lxv 11,-16(9)
	xvmaddasp 42,12,35
	stxv 41,-64(9)
	xvmaddmsp 44,32,11
	xvmaddasp 43,12,36
	stxv 42,-48(9)
	xvmaddasp 44,12,37
#else
	lxv 12,-128(9)
	vpermr 17,6,6,1
	xvmaddmsp 38,0,11
	lxv 11,-96(9)
	vpermr 18,7,7,1
	vpermr 19,8,8,1
	vpermr 2,9,9,1
	vpermr 3,10,10,1
	vpermr 4,11,11,1
	xvmaddasp 12,0,45
	vpermr 5,12,12,1
	xvmaddmsp 39,0,11
	lxv 11,-80(9)
	vpermr 13,13,13,1
	xvmaddasp 38,32,49
	xvmaddmsp 40,0,11
	lxv 11,-64(9)
	xvmaddmsp 45,32,12
	xvmaddasp 39,32,50
	stxv 38,-112(9)
	xvmaddmsp 41,0,11
	lxv 11,-48(9)
	xvmaddasp 40,32,51
	stxv 45,-128(9)
	stxv 39,-96(9)
	xvmaddmsp 42,0,11
	lxv 11,-32(9)
	xvmaddasp 41,32,34
	stxv 40,-80(9)
	xvmaddmsp 43,0,11
	lxv 11,-16(9)
	xvmaddasp 42,32,35
	stxv 41,-64(9)
	xvmaddmsp 44,0,11
	xvmaddasp 43,32,36
	stxv 42,-48(9)
	xvmaddasp 44,32,37
#endif
	stxv 43,-32(9)
	stxv 44,-16(9)
	bgt 7,.L6
.L5:
	cmpd 7,7,4
	ble 7,.L33
	sldi 11,4,1
	b .L4
.L7:
	addi 10,4,1
	subf 8,4,7
	cmpd 7,10,7
	mtctr 8
	bgt 7,.L26
	li 10,-1
	rldicr 10,10,0,0
	cmpd 7,7,10
	beq 7,.L26
	.p2align 4,,15
.L13:
	lfs 10,4(3)
	lfs 11,0(3)
	lfs 12,0(9)
	lfs 0,4(9)
	addi 3,3,8
	addi 9,9,8
	fmuls 10,2,10
#ifdef CONJ
	fmadds 11,1,11,10
#else
	fmsubs 11,1,11,10
#endif
	fadds 12,12,11
	stfs 12,-8(9)
	lfs 11,-8(3)
	lfs 12,-4(3)
	fmuls 11,2,11
#ifdef CONJ
	fmsubs 12,1,12,11
	fsubs 0,0,12
#else
	fmadds 12,1,12,11
	fadds 0,0,12
#endif
	stfs 0,-4(9)
	bdnz .L13
	b .L33
.L40:
	li 31,1
	mtctr 31
	b .L11
.L26:
	li 10,1
	mtctr 10
	b .L13
	.long 0
	.byte 0,0,0,0,0,1,0,0
#ifdef CONJ
	.size	caxpyc_k,.-caxpyc_k
#else
	.size	caxpy_k,.-caxpy_k
#endif
	.section	.rodata
	.align 4
	.set	.LANCHOR0,. + 0
	.type	swap_mask_arr, @object
	.size	swap_mask_arr, 16
swap_mask_arr:
	.byte	4
	.byte	5
	.byte	6
	.byte	7
	.byte	0
	.byte	1
	.byte	2
	.byte	3
	.byte	12
	.byte	13
	.byte	14
	.byte	15
	.byte	8
	.byte	9
	.byte	10
	.byte	11
	.section	.rodata.cst16,"aM",@progbits,16
	.align 4
.LC2:
	.byte	31
	.byte	30
	.byte	29
	.byte	28
	.byte	23
	.byte	22
	.byte	21
	.byte	20
	.byte	15
	.byte	14
	.byte	13
	.byte	12
	.byte	7
	.byte	6
	.byte	5
	.byte	4
.LC3:
	.byte	27
	.byte	26
	.byte	25
	.byte	24
	.byte	19
	.byte	18
	.byte	17
	.byte	16
	.byte	11
	.byte	10
	.byte	9
	.byte	8
	.byte	3
	.byte	2
	.byte	1
	.byte	0
	.ident	"GCC: (SUSE Linux) 7.3.1 20180323 [gcc-7-branch revision 258812]"
	.gnu_attribute 4, 1
	.section	.note.GNU-stack,"",@progbits
