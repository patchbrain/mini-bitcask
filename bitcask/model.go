package bitcask

type Entry struct {
	KeySz  int32  `json:"keySz"`
	ValSz  int32  `json:"valSz"`
	TStamp int32  `json:"tStamp"`
	Key    string `json:"key"`
	Value  []byte `json:"value"`
}
