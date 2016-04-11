#include "textflag.h"

TEXT ·IsBlockZero(SB),NOSPLIT,$0-25
	MOVQ	buf_len+8(FP), BX
	MOVQ	buf+0(FP), SI
	CMPQ	BX, $8
	JB	small
	CMPQ	BX, $64
	JB	bigloop
//	CMPB    runtime·support_avx2(SB), $1
//	JE	hugeloop_avx2

	XORPS	X4, X4

hugeloop:
	CMPQ	BX, $64
	JB bigloop
	MOVOU	(SI), X0
	MOVOU	16(SI), X1
	MOVOU	32(SI), X2
	MOVOU	48(SI), X3
	POR	X1, X0
	POR	X2, X0
	POR	X3, X0
	PCMPEQB	X4, X0
	PMOVMSKB	X0, DX
	ADDQ	$64, SI
	SUBQ	$64, BX
	CMPL	DX, $0xffff
	JEQ	hugeloop
	MOVB	$0, ret+24(FP)
	RET

bigloop:
	CMPQ	BX, $8
	JBE leftover
	MOVQ	(SI), CX
	ADDQ	$8, SI
	SUBQ	$8, BX
	CMPQ	CX, $0
	JEQ bigloop
	MOVB	$0, ret+24(FP)
	RET

leftover:
	MOVQ	-8(SI)(BX*1), CX
	CMPQ	CX, $0
	SETEQ	ret+24(FP)
	RET

small:
	CMPQ	BX, $0
	JEQ	equal

	LEAQ	0(BX*8), CX
	NEGQ	CX

	CMPB	SI, $0xf8
	JA	si_high

	MOVQ	(SI), SI
	SHLQ	CX, SI
	JMP	si_finish
si_high:
	MOVQ	-8(SI)(BX*1), SI
	SHRQ	CX, SI
si_finish:
//	CMPQ	SI, $0
equal:
	SETEQ	ret+24(FP)
	RET
