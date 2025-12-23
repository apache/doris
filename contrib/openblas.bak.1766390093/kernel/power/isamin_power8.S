/*	.file	"isamin.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl isamin_k
	.type	isamin_k, @function
*/
#define ASSEMBLER
#include "common.h"

	PROLOGUE

#if _CALL_ELF ==2
isamin_k:
#endif
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
#if _CALL_ELF ==2
	.localentry	isamin_k,.-isamin_k
#endif
	mr. 11,3
	ble 0,.L36
	cmpdi 7,5,0
	li 3,0
	blelr 7
	lfs 0,0(4)
	li 0,-48
	cmpdi 7,5,1
	stvx 30,1,0
	li 0,-32
	stvx 31,1,0
	fabs 0,0
	beq 7,.L62
	rldicr. 6,11,0,61
	beq 0,.L40
	sldi 0,5,1
	sldi 12,5,2
	std 31,-8(1)
	add 0,0,5
	neg 31,5
	sldi 3,5,4
	sldi 0,0,2
	add 7,4,12
	sldi 31,31,2
	sldi 5,5,3
	li 9,0
	li 10,0
	b .L24
	.p2align 4,,15
.L41:
	mr 10,9
.L25:
	fmr 0,12
	add 7,7,3
.L24:
	lfs 12,0(7)
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L26
	fmr 0,12
	addi 10,9,1
.L26:
	add 8,31,7
	lfsx 12,8,5
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L28
	fmr 0,12
	addi 10,9,2
.L28:
	lfsx 12,8,0
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L30
	fmr 0,12
	addi 10,9,3
.L30:
	addi 9,9,4
	cmpd 7,6,9
	ble 7,.L63
	lfsx 12,8,3
	fabs 12,12
	fcmpu 7,12,0
	blt 7,.L41
	fmr 12,0
	b .L25
	.p2align 4,,15
.L36:
	li 3,0
	blr
	.p2align 4,,15
.L63:
	addi 6,6,-1
	ld 31,-8(1)
	srdi 6,6,2
	addi 6,6,1
	sldi 9,6,2
	mulld 6,12,6
	cmpd 7,11,9
	ble 7,.L33
.L23:
	addi 8,9,1
	sldi 6,6,2
	cmpd 7,8,11
	subf 8,9,11
	mtctr 8
	add 4,4,6
	bgt 7,.L52
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L52
	.p2align 4,,15
.L35:
	lfs 12,0(4)
	add 4,4,12
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L34
	fmr 0,12
	mr 10,9
.L34:
	addi 9,9,1
	bdnz .L35
.L33:
	li 0,-48
	addi 3,10,1
	lvx 30,1,0
	li 0,-32
	lvx 31,1,0
	blr
	.p2align 4,,15
.L62:
	rldicr. 8,11,0,57
	li 10,0
	bne 0,.L64
.L4:
	addi 7,8,1
	sldi 9,8,2
	cmpd 7,7,11
	add 4,4,9
	subf 9,8,11
	mtctr 9
	bgt 7,.L51
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L51
	.p2align 4,,15
.L22:
	lfs 12,0(4)
	addi 4,4,4
	fabs 12,12
	fcmpu 7,0,12
	bng 7,.L21
	fmr 0,12
	mr 10,8
.L21:
	addi 8,8,1
	bdnz .L22
	li 0,-48
	addi 3,10,1
	lvx 30,1,0
	li 0,-32
	lvx 31,1,0
	blr
	.p2align 4,,15
.L64:
	lxvd2x 4,0,4
	addis 10,2,.LC2@toc@ha
	addis 5,2,.LC3@toc@ha
	std 31,-8(1)
	vspltisw 2,0
	addi 10,10,.LC2@toc@l
	addis 7,2,.LC4@toc@ha
	addis 9,2,.LC5@toc@ha
	addis 6,2,.LC6@toc@ha
	lxvd2x 51,0,10
	addis 10,2,.LC7@toc@ha
	addi 7,7,.LC4@toc@l
	addi 9,9,.LC5@toc@l
	addi 5,5,.LC3@toc@l
	xvabssp 4,4
	addi 6,6,.LC6@toc@l
	addi 10,10,.LC7@toc@l
	lxvd2x 36,0,7
	vspltisw 18,8
	lxvd2x 37,0,9
	lxvd2x 35,0,5
	mr 9,4
	li 7,0
	lxvd2x 48,0,6
	lxvd2x 49,0,10
	vadduwm 18,18,18
	xxlor 38,51,51
	xxlor 40,4,4
	b .L6
	.p2align 4,,15
.L65:
	lxvd2x 5,0,9
	xvabssp 40,5
.L6:
	addi 5,9,16
	addi 6,9,32
	vadduwm 14,2,16
	addi 10,9,64
	addi 12,9,48
	addi 31,9,80
	addi 3,9,96
	lxvd2x 5,0,5
	lxvd2x 42,0,6
	addi 5,9,112
	addi 6,9,128
	lxvd2x 44,0,10
	lxvd2x 9,0,12
	addi 10,9,160
	addi 12,9,144
	lxvd2x 6,0,31
	lxvd2x 1,0,3
	addi 31,9,176
	addi 3,9,192
	lxvd2x 11,0,5
	lxvd2x 13,0,6
	addi 5,9,208
	addi 6,9,224
	lxvd2x 2,0,10
	lxvd2x 7,0,12
	addi 10,9,240
	lxvd2x 10,0,31
	lxvd2x 3,0,3
	xvabssp 42,42
	xvabssp 5,5
	addi 7,7,64
	lxvd2x 8,0,5
	lxvd2x 0,0,6
	xvabssp 44,44
	xvabssp 9,9
	cmpd 7,8,7
	addi 9,9,256
	lxvd2x 12,0,10
	xvabssp 6,6
	xvabssp 1,1
	xvabssp 11,11
	xvabssp 13,13
	xvabssp 7,7
	xvabssp 2,2
	xvabssp 10,10
	xvabssp 3,3
	xvabssp 8,8
	xvabssp 0,0
	xvabssp 12,12
	xvcmpgtsp 32,40,5
	xvcmpgtsp 62,42,9
	xvcmpgtsp 45,44,6
	xvcmpgtsp 63,1,11
	xvcmpgtsp 39,13,7
	xvcmpgtsp 47,2,10
	xvcmpgtsp 41,3,8
	xvcmpgtsp 33,0,12
	xxsel 5,40,5,32
	xxsel 32,38,35,32
	xxsel 9,42,9,62
	xxsel 6,44,6,45
	xxsel 11,1,11,63
	xxsel 44,38,35,45
	xxsel 7,13,7,39
	xvcmpgtsp 42,5,9
	xxsel 10,2,10,47
	xvcmpgtsp 45,6,11
	xxsel 8,3,8,41
	xxsel 63,36,37,63
	xxsel 0,0,12,33
	xvcmpgtsp 43,7,10
	xxsel 40,36,37,33
	xxsel 62,36,37,62
	xvcmpgtsp 33,8,0
	xxsel 41,38,35,41
	xxsel 39,38,35,39
	xxsel 47,36,37,47
	xxsel 9,5,9,42
	xxsel 42,32,62,42
	xxsel 12,6,11,45
	xxsel 45,44,63,45
	xxsel 11,7,10,43
	xvcmpgtsp 32,9,12
	vadduwm 13,13,18
	xxsel 43,39,47,43
	xxsel 0,8,0,33
	xxsel 33,41,40,33
	xvcmpgtsp 44,11,0
	vadduwm 1,1,18
	xxsel 12,9,12,32
	xxsel 32,42,45,32
	vadduwm 0,2,0
	vadduwm 2,2,17
	xxsel 0,11,0,44
	xxsel 33,43,33,44
	xvcmpgtsp 45,12,0
	vadduwm 1,14,1
	xxsel 0,12,0,45
	xxsel 32,32,33,45
	xvcmpgtsp 33,4,0
	xxsel 51,51,32,33
	xxsel 4,4,0,33
	bgt 7,.L65
	xxsldwi 0,4,4,1
	xscvspdp 10,4
	vspltw 0,19,0
	xxsldwi 12,4,4,3
	xscvspdp 0,0
	mfvsrwz 3,51
	mfvsrwz 6,32
	vspltw 0,19,3
	xscvspdp 12,12
	xxsldwi 4,4,4,2
	mfvsrwz 7,32
	vspltw 0,19,2
	xscvspdp 4,4
	mfvsrwz 9,32
	fcmpu 7,0,10
	rldicl 10,3,0,32
	rldicl 31,6,0,32
	fmr 11,12
	rldicl 5,7,0,32
	rldicl 0,9,0,32
	beq 7,.L66
	bng 7,.L9
	fmr 0,10
	mr 10,31
.L9:
	fcmpu 7,12,4
	bne 7,.L12
	cmplw 7,7,9
	ble 7,.L13
	mr 7,9
.L13:
	rldicl 5,7,0,32
.L14:
	fcmpu 7,0,11
	beq 7,.L67
	bng 7,.L19
	fmr 0,11
	mr 10,5
.L19:
	cmpd 7,11,8
	ld 31,-8(1)
	bgt 7,.L4
	b .L33
	.p2align 4,,15
.L66:
	cmplw 7,3,6
	ble 7,.L8
	mr 3,6
.L8:
	rldicl 10,3,0,32
	b .L9
	.p2align 4,,15
.L40:
	sldi 12,5,2
	li 10,0
	li 9,0
	b .L23
	.p2align 4,,15
.L12:
	bng 7,.L14
	fmr 11,4
	mr 5,0
	b .L14
	.p2align 4,,15
.L67:
	cmpd 7,10,5
	ble 7,.L19
	mr 10,5
	b .L19
.L51:
	li 9,1
	mtctr 9
	b .L22
.L52:
	li 8,1
	mtctr 8
	b .L35
	.long 0
	.byte 0,0,0,0,0,1,0,0
#if _CALL_ELF ==2
	.size	isamin_k,.-isamin_k
#endif
	.section	.rodata.cst16,"aM",@progbits,16
	.align 4
.LC2:
	.long	0
	.long	1
	.long	2
	.long	3
.LC3:
	.long	4
	.long	5
	.long	6
	.long	7
.LC4:
	.long	8
	.long	9
	.long	10
	.long	11
.LC5:
	.long	12
	.long	13
	.long	14
	.long	15
.LC6:
	.long	32
	.long	32
	.long	32
	.long	32
.LC7:
	.long	64
	.long	64
	.long	64
	.long	64
	.ident	"GCC: (SUSE Linux) 7.3.1 20180323 [gcc-7-branch revision 258812]"
	.section	.note.GNU-stack,"",@progbits
