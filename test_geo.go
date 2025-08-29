package main

import (
	"fmt"
	"math"
)

const (
	minLatitude   = -85.05112878
	maxLatitude   = 85.05112878
	minLongitude  = -180.0
	maxLongitude  = 180.0
	latitudeRange = maxLatitude - minLatitude
	longitudeRange = maxLongitude - minLongitude
)

func spreadInt32ToInt64(v uint32) uint64 {
	result := uint64(v) & 0xFFFFFFFF
	result = (result | (result << 16)) & 0x0000FFFF0000FFFF
	result = (result | (result << 8)) & 0x00FF00FF00FF00FF
	result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result << 2)) & 0x3333333333333333
	result = (result | (result << 1)) & 0x5555555555555555
	return result
}

func interleave(x, y uint32) uint64 {
	xSpread := spreadInt32ToInt64(x)
	ySpread := spreadInt32ToInt64(y)
	yShifted := ySpread << 1
	return xSpread | yShifted
}

func geoHashScore(lon, lat float64) uint64 {
	// Clamp to supported ranges
	if lon < minLongitude {
		lon = minLongitude
	} else if lon > maxLongitude {
		lon = maxLongitude
	}
	if lat < minLatitude {
		lat = minLatitude
	} else if lat > maxLatitude {
		lat = maxLatitude
	}
	
	// Normalize to the range [0, 2^26)
	scale := float64(uint64(1) << 26) // 2^26 as exact integer
	normalizedLatitude := scale * (lat - minLatitude) / latitudeRange
	normalizedLongitude := scale * (lon - minLongitude) / longitudeRange
	
	// Truncate to integers
	latInt := uint32(normalizedLatitude)
	lonInt := uint32(normalizedLongitude)
	
	return interleave(latInt, lonInt)
}

func compactInt64ToInt32(v uint64) uint32 {
	v = v & 0x5555555555555555
	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF
	return uint32(v)
}

func geoDecodeScore(score uint64) (float64, float64) {
	// Extract longitude bits (they were shifted left by 1 during encoding)
	y := score >> 1
	// Extract latitude bits (they were in the original positions)
	x := score
	
	// Compact both latitude and longitude back to 32-bit integers
	gridLatitudeNumber := compactInt64ToInt32(x)
	gridLongitudeNumber := compactInt64ToInt32(y)
	
	// Calculate the grid boundaries
	scale := float64(uint64(1) << 26) // 2^26 as exact integer
	gridLatitudeMin := minLatitude + latitudeRange*(float64(gridLatitudeNumber)/scale)
	gridLatitudeMax := minLatitude + latitudeRange*(float64(gridLatitudeNumber+1)/scale)
	gridLongitudeMin := minLongitude + longitudeRange*(float64(gridLongitudeNumber)/scale)
	gridLongitudeMax := minLongitude + longitudeRange*(float64(gridLongitudeNumber+1)/scale)
	
	// Calculate the center point of the grid cell
	latitude := (gridLatitudeMin + gridLatitudeMax) / 2
	longitude := (gridLongitudeMin + gridLongitudeMax) / 2
	
	return longitude, latitude
}

type TestCase struct {
	name          string
	latitude      float64
	longitude     float64
	expectedScore uint64
}

func main() {
	// Test cases from the reference
	testCases := []TestCase{
		{"Bangkok", 13.7220, 100.5252, 3962257306574459},
		{"Beijing", 39.9075, 116.3972, 4069885364908765},
		{"Berlin", 52.5244, 13.4105, 3673983964876493},
		{"Paris", 48.8534, 2.3488, 3663832752681684},
		{"Sydney", -33.8688, 151.2093, 3252046221964352},
	}

	fmt.Println("Testing encoding...")
	for _, tc := range testCases {
		actualScore := geoHashScore(tc.longitude, tc.latitude)
		match := actualScore == tc.expectedScore
		status := "❌"
		if match {
			status = "✅"
		}
		fmt.Printf("%s: got %d, expected %d (%s)\n", tc.name, actualScore, tc.expectedScore, status)
	}

	fmt.Println("\nTesting decoding...")
	for _, tc := range testCases {
		decodedLon, decodedLat := geoDecodeScore(tc.expectedScore)
		latDiff := math.Abs(decodedLat - tc.latitude)
		lonDiff := math.Abs(decodedLon - tc.longitude)
		withinTolerance := latDiff < 1e-6 && lonDiff < 1e-6
		status := "❌"
		if withinTolerance {
			status = "✅"
		}
		fmt.Printf("%s: decoded (%.15f, %.15f), expected (%.15f, %.15f) (%s)\n", 
			tc.name, decodedLat, decodedLon, tc.latitude, tc.longitude, status)
		if !withinTolerance {
			fmt.Printf("  Diff: lat=%.9f, lon=%.9f\n", latDiff, lonDiff)
		}
	}
}
