package dastard

import (
	"fmt"
	"math"
)

// Mix performns the mix for lancero data, handles retarding on datastream
// Retard the raw data stream by 1 sample so it can be mixed with
//
// the appropriate error sample. This corrects for a poor choice in the
// TDM firmware design, but so it goes.
// fb_physical[n] refers to the feedback signal applied during tick [n]
// err_physical[n] refers to the error signal measured during tick [n], eg with fb_physical[n] applied
// fb_data[n]=fb_physical[n+1]
// err_data[n]=err_physical[n]
// in words: at frame [n] we get data for the error measured at frame [n]
// and the feedback that will be applied during frame [n+1]
// we want
// mix[n] = fb_physical[n] + mixFraction * err_physical[n]
// so
// mix[n] = fb_data[n-1]   + mixFraction * err_data[n]
// or
// mix[n+1] = fb_data[n]   + mixFraction * err_data[n+1]
type Mix struct {
	mixFraction float64
	lastFb      RawType
}

// MixRetardFb mixes err into fbs, alters fbs in place to contain the mixed values
// consecutive calls must be on consecutive data.
// The following ASSUMES that error signals are signed. That holds for Lancero
// TDM systems, at least, and that is the only source that uses Mix.
func (m *Mix) MixRetardFb(fbs *[]RawType, errs *[]RawType) {
	lastElement := (*fbs)[len(*fbs)-1]
	const mask = ^RawType(0x03)
	if m.mixFraction == 0.0 {
		for j := 0; j < len(*fbs); j++ {
			fb := m.lastFb
			m.lastFb = (*fbs)[j] & mask
			(*fbs)[j] = fb
		}
		return
	}
	for j := 0; j < len(*fbs); j++ {
		fb := m.lastFb
		mixAmount := float64(int16((*errs)[j])) * m.mixFraction
		// Be careful not to overflow!
		floatMixResult := mixAmount + float64(fb)
		m.lastFb = (*fbs)[j] & mask
		if floatMixResult >= math.MaxUint16 {
			(*fbs)[j] = math.MaxUint16
		} else if floatMixResult < 0 {
			(*fbs)[j] = 0
		} else {
			(*fbs)[j] = RawType(roundint(floatMixResult))
		}
	}
	if m.lastFb == 0 {
		fmt.Println("last element", lastElement)
	}
}
