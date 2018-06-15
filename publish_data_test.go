package dastard

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

func TestPublishData(t *testing.T) {

	dp := DataPublisher{}
	d := []RawType{10, 10, 10, 10, 15, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10}
	rec := &DataRecord{data: d, presamples: 4}
	records := []*DataRecord{rec, rec, rec}

	if err := dp.PublishData(records); err != nil {
		t.Fail()
	}
	dp.SetLJH22(1, 4, len(d), 1, 1, 8, 1, "TestPublishData.ljh")
	if err := dp.PublishData(records); err != nil {
		t.Fail()
	}
	if dp.LJH22.RecordsWritten != 3 {
		t.Fail()
	}
	if !dp.HasLJH22() {
		t.Error("HasLJH22() false, want true")
	}
	dp.RemoveLJH22()
	if dp.HasLJH22() {
		t.Error("HasLJH22() true, want false")
	}

	if dp.HasPubRecords() {
		t.Error("HasPubRecords() true, want false")
	}
	dp.SetPubRecords()

	if !dp.HasPubRecords() {
		t.Error("HasPubRecords() false, want true")
	}

	dp.PublishData(records)

	dp.RemovePubRecords()
	if dp.HasPubRecords() {
		t.Error("HasPubRecords() true, want false")
	}

	if dp.HasPubSummaries() {
		t.Error("HasPubSummaries() true, want false")
	}
	dp.SetPubSummaries()

	if !dp.HasPubSummaries() {
		t.Error("HasPubSummaries() false, want true")
	}

	dp.PublishData(records)

	dp.RemovePubSummaries()
	if dp.HasPubSummaries() {
		t.Error("HasPubSummaries() true, want false")
	}

	dp.SetLJH3(0, 0, 0, 0, "TestPublishData.ljh3")
	if err := dp.PublishData(records); err != nil {
		t.Error("failed to publish record")
	}
	if dp.LJH3.RecordsWritten != 3 {
		t.Error("wrong number of RecordsWritten, want 1, have", dp.LJH3.RecordsWritten)
	}
	if !dp.HasLJH3() {
		t.Error("HasLJH3() false, want true")
	}
	dp.RemoveLJH3()
	if dp.HasLJH3() {
		t.Error("HasLJH3() true, want false")
	}
}

func TestRawTypeToX(t *testing.T) {
	d := []RawType{0xFFFF, 0x0101, 0xABCD, 0xEF01, 0x2345, 0x6789}
	b := rawTypeToBytes(d)
	encodedStr := hex.EncodeToString(b)
	expectStr := "ffff0101cdab01ef45238967"
	if encodedStr != expectStr {
		t.Errorf("have %v, want %v", encodedStr, expectStr)
	}
	if len(b) != 2*len(d) {
		t.Errorf("wrong length, have %v, want %v", len(b), len(d))
	}
	c := rawTypeToUint16(d)
	expect := []uint16{0xFFFF, 0x0101, 0xABCD, 0xEF01, 0x2345, 0x6789}
	for i, v := range expect {
		if c[i] != v {
			t.Errorf("have %v, want %v", c[i], v)
		}
	}

}

func BenchmarkPublish(b *testing.B) {
	d := make([]RawType, 1000)
	rec := &DataRecord{data: d, presamples: 4}
	records := make([]*DataRecord, 1)
	for i := range records {
		records[i] = rec
	}
	slowPart := func(b *testing.B, dp DataPublisher, records []*DataRecord) {
		for i := 0; i < b.N; i++ {
			dp.PublishData(records)
			b.SetBytes(int64(len(d) * 2 * len(records)))
		}
	}

	b.Run("PubRecords", func(b *testing.B) {
		dp := DataPublisher{}
		dp.SetPubRecords()
		defer dp.RemovePubRecords()
		slowPart(b, dp, records)
	})
	b.Run("PubSummaries", func(b *testing.B) {
		dp := DataPublisher{}
		dp.SetPubSummaries()
		defer dp.RemovePubSummaries()
		slowPart(b, dp, records)
	})
	b.Run("PubLJH22", func(b *testing.B) {
		dp := DataPublisher{}
		dp.SetLJH22(0, 0, len(d), 0, 0, 0, 0, "TestPublishData.ljh")
		defer dp.RemoveLJH22()
		slowPart(b, dp, records)
	})
	b.Run("PubLJH3", func(b *testing.B) {
		dp := DataPublisher{}
		dp.SetLJH3(0, 0, 0, 0, "TestPublishData.ljh3")
		defer dp.RemoveLJH3()
		slowPart(b, dp, records)
	})
	b.Run("PubAll", func(b *testing.B) {
		dp := DataPublisher{}
		dp.SetPubRecords()
		defer dp.RemovePubRecords()
		dp.SetPubSummaries()
		defer dp.RemovePubSummaries()
		dp.SetLJH22(0, 0, len(d), 0, 0, 0, 0, "TestPublishData.ljh")
		defer dp.RemoveLJH22()
		dp.SetLJH3(0, 0, 0, 0, "TestPublishData.ljh3")
		defer dp.RemoveLJH3()
		slowPart(b, dp, records)
	})
	b.Run("PubNone", func(b *testing.B) {
		dp := DataPublisher{}
		slowPart(b, dp, records)
	})
	b.Run("RawTypeToUint16", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data := make([]uint16, len(rec.data))
			for i, v := range rec.data {
				data[i] = uint16(v)
			}
			b.SetBytes(int64(2 * len(rec.data)))
		}
	})
	b.Run("binary.Write", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var buf bytes.Buffer
			binary.Write(&buf, binary.LittleEndian, rec.data)
			b.SetBytes(int64(2 * len(rec.data)))
		}
	})
	b.Run("rawTypeToBytes", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			data := rawTypeToBytes(rec.data)
			b.SetBytes(int64(2 * len(data)))
		}
	})
}
