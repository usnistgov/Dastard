package ringbuffer

import (
	"testing"

	"github.com/fabiokung/shm"
)

func TestBufferOpenClose(t *testing.T) {
	goodname1 := "will_exist_buffer"
	goodname2 := "will_exist_description"
	badname1 := "does_not_exist"
	badname2 := "does_not_exist_either"

	// In case these memory regions exist from earlier tests, remove them.
	names := []string{goodname1, goodname2, badname1, badname2}
	for _, name := range names {
		shm.Unlink(name)
	}

	// Now test the writeable buffer, which we need for testing.
	writebuf, err := NewRingBuffer(goodname1, goodname2)
	if err != nil {
		t.Error("Failed NewRingBuffer")
	}
	defer writebuf.unlink()
	if err = writebuf.create(8192); err != nil {
		t.Error("Failed RingBuffer.create", err)
	}

	// Now 2 buffers that should not be Openable
	r, err := NewRingBuffer(badname1, badname2)
	if err != nil {
		t.Error("Failed NewRingBuffer", err)
	}
	if err = r.Open(); err == nil {
		t.Errorf("NewRingBuffer(%s, %s) succeeds, should fail", badname1, badname2)
	}
	r, err = NewRingBuffer(badname1, goodname2)
	if err != nil {
		t.Error("Failed NewRingBuffer", err)
	}
	if err = r.Open(); err == nil {
		t.Errorf("NewRingBuffer(%s, %s) succeeds, should fail", badname1, goodname2)
	}

	// This buffer should be Openable and Closeable
	r, err = NewRingBuffer(goodname1, goodname2)
	if err != nil {
		t.Error("Failed NewRingBuffer", err)
	}
	if err = r.Open(); err != nil {
		t.Error("Failed RingBuffer.Open", err)
	}
	if err = r.Close(); err != nil {
		t.Error("Failed RingBuffer.Close", err)
	}

	if err = writebuf.Close(); err != nil {
		t.Error("Failed RingBuffer.Close", err)
	}
}

func TestBufferWriteRead(t *testing.T) {
	name1 := "test_ring_buffer"
	name2 := "test_ring_description"
	// In case these memory regions exist from earlier tests, remove them.
	names := []string{name1, name2}
	for _, name := range names {
		shm.Unlink(name)
	}

	writebuf, err := NewRingBuffer(name1, name2)
	if err != nil {
		t.Error("Failed NewRingBuffer", err)
	}
	buffersize := 8192
	if err = writebuf.create(buffersize); err != nil {
		t.Error("Failed RingBuffer.create", err)
	}
	nbeef := 2000
	deadbeef := make([]byte, 0)
	bead5678 := make([]byte, 0)
	for i := 0; i < nbeef; i++ {
		deadbeef = append(deadbeef, []byte{0xde, 0xad, 0xbe, 0xef}...)
		bead5678 = append(bead5678, []byte{0xbe, 0xad, 0x56, 0x78}...)
	}
	writebuf.write(deadbeef)

	b, err := NewRingBuffer(name1, name2)
	if err != nil {
		t.Error("Failed NewRingBuffer", err)
	}
	if err = b.Open(); err != nil {
		t.Error("Failed RingBuffer.Open", err)
	}

	// Test that reading 0 reads 0.
	data, nbytes, err := b.Read(0)
	if err != nil {
		t.Error("Failed to b.Read(0)")
	} else if nbytes != 0 || len(data) > 0 {
		t.Errorf("b.Read(0) returned len(data)=%d, nbytes=%d, want 0 and 0",
			len(data), nbytes)
	}

	// There are <expect> bytes in the buffer. Test that reading them works.
	expect := 4 * nbeef
	if expect*2 <= buffersize {
		t.Error("Test design failure: intend to write enough bytes to exceed half buffer size.")
	}
	readable := b.BytesReadable()
	if readable != expect {
		t.Errorf("b.BytesReadable() returns %d, want %d", readable, expect)
	}
	data, nbytes, err = b.Read(expect)
	if err != nil {
		t.Errorf("Failed to b.Read(%d)", expect)
	} else if nbytes != expect || len(data) != expect {
		t.Errorf("b.Read(%d) returned len(data)=%d, nbytes=%d, want %d and %d",
			expect, len(data), nbytes, expect, expect)
	}
	readable = b.BytesReadable()
	if readable != 0 {
		t.Errorf("b.BytesReadable() returns %d, want 0", readable)
	}

	// There are 0 bytes in the buffer. Verify that.
	data, nbytes, err = b.Read(expect)
	if err != nil {
		t.Errorf("Failed to b.Read(%d) when empty", expect)
	} else if nbytes != 0 || len(data) != 0 {
		t.Errorf("b.Read(%d) returned len(data)=%d, nbytes=%d, want %d and %d",
			expect, len(data), nbytes, 0, 0)
	}

	// Now put bytes in the buffer, clear it, and verify that there are 0.
	writebuf.write(deadbeef)
	b.DiscardAll()
	data, nbytes, err = b.Read(expect)
	if err != nil {
		t.Errorf("Failed to b.Read(%d) when empty", expect)
	} else if nbytes != 0 || len(data) != 0 {
		t.Errorf("b.Read(%d) returned len(data)=%d, nbytes=%d, want %d and %d",
			expect, len(data), nbytes, 0, 0)
	}

	// Now put different bytes in the buffer, verify that they are the right values.
	writebuf.write(bead5678)
	readable = b.BytesReadable()
	if readable != expect {
		t.Errorf("b.BytesReadable() returns %d, want %d", readable, expect)
	}
	data, nbytes, err = b.Read(expect)
	if err != nil {
		t.Errorf("Failed to b.Read(%d)", expect)
	} else if nbytes != expect || len(data) != expect {
		t.Errorf("b.Read(%d) returned len(data)=%d, nbytes=%d, want %d and %d",
			expect, len(data), nbytes, expect, expect)
	}
	for i := 0; i < len(data); i += 4 {
		if data[i] != 0xbe || data[i+1] != 0xad || data[i+2] != 0x56 || data[i+3] != 0x78 {
			t.Errorf("b.Read() returned data[%d:%d] = %v, want 0xbead5678", i, i+4, data[i:i+4])
		}
	}

	// Done with writing and reading. Close buffers.
	if err = b.Close(); err != nil {
		t.Error("Failed RingBuffer.Close", err)
	}
	if err = writebuf.Close(); err != nil {
		t.Error("Failed RingBuffer.Close", err)
	}
	if err = writebuf.unlink(); err != nil {
		t.Error("Failed RingBuffer.unlink", err)
	}
}
