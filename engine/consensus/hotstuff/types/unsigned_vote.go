package types

type UnsignedVote struct {
	View     uint64
	BlockMRH []byte
}

func NewUnsignedVote(view uint64, blockMRH []byte) *Vote {
	return &Vote{
		View:     view,
		BlockMRH: blockMRH,
	}
}

func (uv *UnsignedVote) BytesForSig() []byte {
	return voteBytesForSig(uv.View, uv.BlockMRH)
}
