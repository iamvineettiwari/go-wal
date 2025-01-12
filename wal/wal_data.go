package wal

import "hash/crc32"

type WalData struct {
	Seq        int32  `json:"seq"`
	Data       []byte `json:"data"`
	CRC        int32  `json:"crc"`
	Checkpoint bool   `json:"check"`
}

func NewWalData(sequenceNo int32, data []byte) *WalData {
	return &WalData{
		Seq:        sequenceNo,
		Data:       data,
		CRC:        int32(crc32.ChecksumIEEE(append(data, byte(sequenceNo)))),
		Checkpoint: false,
	}
}

func (w *WalData) GetLastSequence() int32 {
	return w.Seq
}

func (w *WalData) GetData() []byte {
	return w.Data
}

func (w *WalData) IsCheckpoint() bool {
	return w.Checkpoint
}

func (w *WalData) IsValid() bool {
	return w.CRC == int32(crc32.ChecksumIEEE(append(w.Data, byte(w.Seq))))
}
