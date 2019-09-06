package dastard

import "testing"

func TestMap(t *testing.T) {
	fname := "maps/ar14_30rows_map.cfg"
	m, err := readMap(fname)
	if err != nil {
		t.Fatalf("Could not read map %q: %v", fname, err)
	}
	if m.Spacing != 520 {
		t.Errorf("map.Spacing=%d, want 520", m.Spacing)
	}
	if len(m.Pixels) != 240 {
		t.Errorf("map has %d pixels, want 240", len(m.Pixels))
	} else {
		x := []int{290, 290, 290, 290}
		y := []int{-3470, -4510, -1910, -870}
		for i, p := range m.Pixels {
			if p.X != x[i] {
				t.Errorf("map.Pixels[%d]=%d, want %d", i, p.X, x[i])
			}
			if p.Y != y[i] {
				t.Errorf("map.Pixels[%d]=%d, want %d", i, p.Y, y[i])
			}
			if i == 3 {
				break
			}
		}
	}
	if _, err1 := readMap("doesnotexist.map.1234"); err1 == nil {
		t.Error("readMap() on nonexistent file should error")
	}
	if _, err1 := readMap("map_test.go"); err1 == nil {
		t.Error("readMap() on non-map file should error")
	}
}
