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

func TestDecodeBlueberryFromRemote(t *testing.T) {
	// From remote logs: score used with ZADD for "blueberry"
	score := uint64(3803618426268781)
	// Expected latitude from remote: 51.99678040471944 (± 1e-06)
	expectedLat := 51.99678040471944
	tol := 1e-6

	lon, lat := geoDecodeScore(score)
	diff := math.Abs(lat - expectedLat)
	fmt.Printf("Blueberry (current mapping): score=%d -> lon=%.15f lat=%.15f (diff=%.15f)\n", score, lon, lat, diff)

	// Try swapped mapping: lon from even bits, lat from odd bits
	lonIdx := compactInt64ToInt32_test(score)
	latIdx := compactInt64ToInt32_test(score >> 1)
	step := float64(uint64(1) << 26)
	lon2 := minLongitude + (float64(lonIdx)+0.5)/step*longitudeRange
	lat2 := minLatitude + (float64(latIdx)+0.5)/step*latitudeRange
	diff2 := math.Abs(lat2 - expectedLat)
	fmt.Printf("Blueberry (swapped mapping):  lon=%.15f lat=%.15f (diff=%.15f)\n", lon2, lat2, diff2)

	if diff <= tol || diff2 <= tol {
		return
	}
	t.Fatalf("lat outside tolerance: current diff=%.15f, swapped diff=%.15f", diff, diff2)
}
