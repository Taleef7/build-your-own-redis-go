package main

import (
	"fmt"
	"math"
	"testing"
)

// The geoDecodeScore function and its dependencies from main.go will be placed here
// for isolated testing. I will copy the current implementation from app/main.go.

func compactInt64ToInt32_test(v uint64) uint32 {
	v &= 0x5555555555555555
	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF
	return uint32(v)
}

func geoDecodeScore_test(score uint64) (float64, float64) {
	x := score
	y := score >> 1

	gridLatitudeNumber := compactInt64ToInt32_test(x)
	gridLongitudeNumber := compactInt64ToInt32_test(y)

	step := float64(uint64(1) << 26)

	// Direct center calculation
	latitude := minLatitude + (float64(gridLatitudeNumber)+0.5)/step*latitudeRange
	longitude := minLongitude + (float64(gridLongitudeNumber)+0.5)/step*longitudeRange

	return longitude, latitude
}

func TestDecodeMango(t *testing.T) {
	score := uint64(764553105174486)
	expectedLon := -15.721468766145515
	tolerance := 1e-6

	lon, lat := geoDecodeScore_test(score)

	fmt.Printf("Score: %d\n", score)
	fmt.Printf("Decoded Coords: (lon=%.15f, lat=%.15f)\n", lon, lat)
	fmt.Printf("Expected Lon:     %.15f\n", expectedLon)

	diff := math.Abs(lon - expectedLon)
	fmt.Printf("Difference:       %.15f\n", diff)
	fmt.Printf("Tolerance:        %.15f\n", tolerance)

	if diff > tolerance {
		t.Errorf("Longitude is outside the tolerance range. Got %.15f, expected %.15f", lon, expectedLon)
	}
}
