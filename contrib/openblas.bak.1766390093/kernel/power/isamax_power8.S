/*	.file	"isamax.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl isamax_k
	.type	isamax_k, @function
*/

#define ASSEMBLER
#include "common.h"

	PROLOGUE
	
#if _CALL_ELF == 2	
isamax_k:
#endif
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
#if _CALL_ELF ==2
	.localentry	isamax_k,.-isamax_k
#endif
	mr. 11,3
	ble 0,.L36
	cmpdi 7,5,0
	li 3,0
	blelr 7
	cmpdi 7,5,1
	beq 7,.L69
	rldicr. 7,11,0,61
	beq 0,.L40
	sldi 3,5,1
	xxlxor 0,0,0
	sldi 6,5,2
	add 3,3,5
	sldi 0,5,4
	sldi 3,3,2
	sldi 5,5,3
	mr 9,4
	li 8,0
	li 10,0
	.p2align 4,,15
.L31:
	lfs 12,0(9)
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L23
	fmr 0,12
	mr 8,10
.L23:
	lfsx 12,9,6
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L25
	fmr 0,12
	addi 8,10,1
.L25:
	lfsx 12,9,5
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L27
	fmr 0,12
	addi 8,10,2
.L27:
	lfsx 12,9,3
	add 9,9,0
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L29
	fmr 0,12
	addi 8,10,3
.L29:
	addi 10,10,4
	cmpd 7,7,10
	bgt 7,.L31
	addi 7,7,-1
	srdi 7,7,2
	addi 7,7,1
	sldi 9,7,2
	mulld 7,6,7
	cmpd 7,11,9
	ble 7,.L67
.L22:
	addi 10,9,1
	sldi 7,7,2
	cmpd 7,10,11
	subf 10,9,11
	mtctr 10
	add 4,4,7
	bgt 7,.L54
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L54
	.p2align 4,,15
.L35:
	lfs 12,0(4)
	add 4,4,6
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L33
	fmr 0,12
	mr 8,9
.L33:
	addi 9,9,1
	bdnz .L35
.L67:
	addi 3,8,1
	blr
	.p2align 4,,15
.L36:
	li 3,0
	blr
	.p2align 4,,15
.L69:
	rldicr. 10,11,0,57
	bne 0,.L70
	addi 7,10,1
	sldi 9,10,2
	xxlxor 12,12,12
	cmpd 7,7,11
	add 4,4,9
	subf 9,10,11
	li 8,0
	mtctr 9
	bgt 7,.L60
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L60
	.p2align 4,,15
.L61:
	lfs 0,0(4)
	addi 4,4,4
	fabs 0,0
	fcmpu 7,0,12
	bng 7,.L63
	fmr 12,0
	mr 8,10
.L63:
	addi 10,10,1
	bdnz .L61
	b .L67
	.p2align 4,,15
.L70:
	li 0,-64
	std 31,-8(1)
	addis 3,2,.LC2@toc@ha
	vspltisw 18,0
	vspltisw 12,0
	addis 5,2,.LC3@toc@ha
	addis 6,2,.LC6@toc@ha
	stvx 29,1,0
	li 0,-48
	addis 8,2,.LC7@toc@ha
	xxlor 35,50,50
	addi 3,3,.LC2@toc@l
	addi 5,5,.LC3@toc@l
	stvx 30,1,0
	addi 6,6,.LC6@toc@l
	li 0,-32
	addi 8,8,.LC7@toc@l
	lxvd2x 51,0,3
	lxvd2x 34,0,5
	addis 7,2,.LC4@toc@ha
	stvx 31,1,0
	lxvd2x 47,0,6
	addis 9,2,.LC5@toc@ha
	addi 7,7,.LC4@toc@l
	lxvd2x 48,0,8
	addi 9,9,.LC5@toc@l
	vspltisw 17,8
	vadduwm 17,17,17
	lxvd2x 36,0,7
	li 7,0
	lxvd2x 37,0,9
	mr 9,4
	.p2align 4,,15
.L5:
	addi 5,9,16
	addi 6,9,32
	lxvd2x 41,0,9
	vadduwm 31,3,15
	addi 8,9,64
	addi 31,9,48
	addi 12,9,80
	addi 3,9,96
	lxvd2x 5,0,5
	lxvd2x 43,0,6
	addi 5,9,112
	addi 6,9,128
	lxvd2x 1,0,8
	lxvd2x 9,0,31
	addi 8,9,160
	addi 31,9,144
	lxvd2x 6,0,12
	lxvd2x 13,0,3
	addi 12,9,176
	addi 3,9,192
	lxvd2x 11,0,5
	lxvd2x 2,0,6
	xvabssp 41,41
	addi 5,9,208
	addi 6,9,224
	lxvd2x 3,0,8
	lxvd2x 7,0,31
	addi 8,9,240
	lxvd2x 10,0,12
	lxvd2x 4,0,3
	xvabssp 43,43
	xvabssp 5,5
	addi 7,7,64
	lxvd2x 8,0,5
	lxvd2x 0,0,6
	xvabssp 9,9
	xvabssp 1,1
	cmpd 7,10,7
	addi 9,9,256
	lxvd2x 12,0,8
	xvabssp 6,6
	xvabssp 13,13
	xvabssp 11,11
	xvabssp 2,2
	xvabssp 7,7
	xvabssp 3,3
	xvabssp 10,10
	xvabssp 4,4
	xvabssp 8,8
	xvabssp 0,0
	xvabssp 12,12
	xvcmpgtsp 32,5,41
	xvcmpgtsp 61,9,43
	xvcmpgtsp 45,6,1
	xvcmpgtsp 62,11,13
	xvcmpgtsp 38,7,2
	xvcmpgtsp 46,10,3
	xvcmpgtsp 40,8,4
	xvcmpgtsp 39,12,0
	xxsel 5,41,5,32
	xxsel 32,51,34,32
	xxsel 9,43,9,61
	xxsel 6,1,6,45
	xxsel 11,13,11,62
	xxsel 43,51,34,45
	xxsel 7,2,7,38
	xvcmpgtsp 41,9,5
	xxsel 10,3,10,46
	xvcmpgtsp 45,11,6
	xxsel 8,4,8,40
	xxsel 62,36,37,62
	xxsel 0,0,12,39
	xvcmpgtsp 42,10,7
	xxsel 61,36,37,61
	xxsel 40,51,34,40
	xvcmpgtsp 33,0,8
	xxsel 39,36,37,39
	xxsel 38,51,34,38
	xxsel 46,36,37,46
	xxsel 9,5,9,41
	xxsel 41,32,61,41
	xxsel 12,6,11,45
	xxsel 45,43,62,45
	xxsel 11,7,10,42
	xvcmpgtsp 32,12,9
	vadduwm 13,13,17
	xxsel 42,38,46,42
	xxsel 0,8,0,33
	xxsel 33,40,39,33
	xvcmpgtsp 43,0,11
	vadduwm 1,1,17
	xxsel 12,9,12,32
	xxsel 32,41,45,32
	vadduwm 0,3,0
	vadduwm 3,3,16
	xxsel 0,11,0,43
	xxsel 33,42,33,43
	xvcmpgtsp 45,0,12
	vadduwm 1,31,1
	xxsel 0,12,0,45
	xxsel 32,32,33,45
	xvcmpgtsp 33,0,44
	xxsel 50,50,32,33
	xxsel 44,44,0,33
	bgt 7,.L5
	xxsldwi 12,44,44,1
	xscvspdp 10,44
	vspltw 0,18,0
	xxsldwi 0,44,44,3
	xscvspdp 12,12
	mfvsrwz 3,50
	mfvsrwz 6,32
	vspltw 0,18,3
	xscvspdp 0,0
	xxsldwi 44,44,44,2
	mfvsrwz 7,32
	vspltw 0,18,2
	xscvspdp 44,44
	mfvsrwz 9,32
	fcmpu 7,12,10
	rldicl 8,3,0,32
	rldicl 31,6,0,32
	fmr 11,0
	rldicl 0,7,0,32
	rldicl 5,9,0,32
	beq 7,.L71
	bnl 7,.L8
	fmr 12,10
	mr 8,31
.L8:
	xscmpudp 7,0,44
	bne 7,.L11
	cmplw 7,7,9
	ble 7,.L12
	mr 7,9
.L12:
	rldicl 5,7,0,32
.L13:
	fcmpu 7,12,11
	beq 7,.L72
	bnl 7,.L17
	fmr 12,11
	mr 8,5
.L17:
	cmpd 7,11,10
	ble 7,.L16
	addi 7,10,1
	sldi 9,10,2
	cmpd 7,7,11
	add 4,4,9
	subf 9,10,11
	mtctr 9
	bgt 7,.L53
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L53
	.p2align 4,,15
.L21:
	lfs 0,0(4)
	addi 4,4,4
	fabs 0,0
	fcmpu 7,0,12
	bng 7,.L19
	fmr 12,0
	mr 8,10
.L19:
	addi 10,10,1
	bdnz .L21
.L16:
	li 0,-64
	ld 31,-8(1)
	addi 3,8,1
	lvx 29,1,0
	li 0,-48
	lvx 30,1,0
	li 0,-32
	lvx 31,1,0
	blr
	.p2align 4,,15
.L71:
	cmplw 7,3,6
	ble 7,.L7
	mr 3,6
.L7:
	rldicl 8,3,0,32
	b .L8
	.p2align 4,,15
.L40:
	xxlxor 0,0,0
	sldi 6,5,2
	li 8,0
	li 9,0
	b .L22
	.p2align 4,,15
.L11:
	blt 7,.L39
	mr 5,0
	b .L13
	.p2align 4,,15
.L72:
	cmpd 7,8,5
	ble 7,.L17
	mr 8,5
	b .L17
	.p2align 4,,15
.L39:
	xscpsgndp 11,44,44
	b .L13
.L53:
	li 9,1
	mtctr 9
	b .L21
.L54:
	li 10,1
	mtctr 10
	b .L35
.L60:
	li 9,1
	mtctr 9
	b .L61
	.long 0
	.byte 0,0,0,0,0,1,0,0
#if _CALL_ELF ==2
	.size	isamax_k,.-isamax_k
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
