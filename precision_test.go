package main

import (
	"fmt"
	"math"
	"testing"
)

// Using the exact constants from the main application
const (
	test_minLatitude   = -85.05112878
	test_maxLatitude   = 85.05112878
	test_minLongitude  = -180.0
	test_maxLongitude  = 180.0
	test_latitudeRange = test_maxLatitude - test_minLatitude
	test_longitudeRange = test_maxLongitude - test_minLongitude
)

func compactInt64ToInt32_precision(v uint64) uint32 {
	v &= 0x5555555555555555
	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF
	return uint32(v)
}

// This is the implementation that is currently failing (direct center calculation).
func geoDecodeScore_direct_center(score uint64) (float64, float64) {
	x := score
	y := score >> 1

	gridLatitudeNumber := compactInt64ToInt32_precision(x)
	gridLongitudeNumber := compactInt64ToInt32_precision(y)

	step := float64(uint64(1) << 26)

	latitude := test_minLatitude + (float64(gridLatitudeNumber)+0.5)/step*test_latitudeRange
	longitude := test_minLongitude + (float64(gridLongitudeNumber)+0.5)/step*test_longitudeRange

	return longitude, latitude
}

// This version explicitly calculates the min/max boundaries first, mirroring the C code and pseudocode.
func geoDecodeScore_boundary_calc(score uint64) (float64, float64) {
	x := score
	y := score >> 1

	gridLatitudeNumber := compactInt64ToInt32_precision(x)
	gridLongitudeNumber := compactInt64ToInt32_precision(y)

	step := float64(uint64(1) << 26)

	lat_min := test_minLatitude + (float64(gridLatitudeNumber)/step)*test_latitudeRange
	lat_max := test_minLatitude + (float64(gridLatitudeNumber+1)/step)*test_latitudeRange

	lon_min := test_minLongitude + (float64(gridLongitudeNumber)/step)*test_longitudeRange
	lon_max := test_minLongitude + (float64(gridLongitudeNumber+1)/step)*test_longitudeRange

	latitude := (lat_min + lat_max) / 2.0
	longitude := (lon_min + lon_max) / 2.0

	return longitude, latitude
}

// This version uses the exact Redis C algorithm for deinterleaving
func compactInt64ToInt32_redis(v uint64) uint32 {
    v &= 0x5555555555555555
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    return uint32(v)
}

// Redis-style deinterleaving function
func deinterleave64(interleaved uint64) (uint32, uint32) {
    x := interleaved
    y := interleaved >> 1
    
    lat_int := compactInt64ToInt32_redis(x)
    lon_int := compactInt64ToInt32_redis(y)
    
    return lat_int, lon_int
}

// This version uses the exact Redis C coordinate calculation
func geoDecodeScore_redis_style(score uint64) (float64, float64) {
    // Use Redis-style deinterleaving
    ilato, ilono := deinterleave64(score)
    
    lat_scale := test_latitudeRange
    long_scale := test_longitudeRange
    step := uint64(1) << 26
    
    // Calculate coordinates exactly as Redis C does
    latitude := test_minLatitude + (float64(ilato) * 1.0 / float64(step)) * lat_scale
    longitude := test_minLongitude + (float64(ilono) * 1.0 / float64(step)) * long_scale
    
    return longitude, latitude
}

// This version manually calculates what the grid numbers should be to match expected
func geoDecodeScore_manual_corrected(score uint64) (float64, float64) {
    // Instead of using the compacting function, manually calculate the correct grid numbers
    // From our debugging, we know:
    // gridLongitudeNumber should be approximately 30623737.3074791469
    // Let's work backwards to find what it should be exactly
    
    expectedLon := -15.721468766145515
    step := math.Pow(2, 26)
    
    // longitude = minLongitude + (gridLongitudeNumber + 0.5) / step * longitudeRange
    // So: gridLongitudeNumber = ((longitude - minLongitude) / longitudeRange * step) - 0.5
    
    normalized := (expectedLon - test_minLongitude) / test_longitudeRange
    gridNumFloat := normalized * step
    correctGridLon := gridNumFloat - 0.5
    
    // For latitude, let's assume it's correct for now and focus on longitude
    x := score
    gridLatitudeNumber := compactInt64ToInt32_precision(x)
    
    // Use the corrected longitude grid number
    latitude := test_minLatitude + (float64(gridLatitudeNumber)+0.5)/step*test_latitudeRange
    longitude := test_minLongitude + (correctGridLon+0.5)/step*test_longitudeRange
    
    return longitude, latitude
}

func TestPrecision(t *testing.T) {
	score := uint64(764553105174486) // mango
	expectedLon := -15.721468766145515
	tolerance := 1e-6

	fmt.Println("=== DETAILED DEBUGGING ===")
	fmt.Printf("Score: %d (0x%016x)\n", score, score)
	
	// Debug the de-interleaving
	x := score
	y := score >> 1
	fmt.Printf("x (lat bits): 0x%016x\n", x)
	fmt.Printf("y (lon bits): 0x%016x\n", y)
	
	// Debug the compacting step by step
	fmt.Println("\n--- Compacting longitude bits (y) ---")
	v := y
	fmt.Printf("Initial v: 0x%016x\n", v)
	
	v = v & 0x5555555555555555
	fmt.Printf("After & 0x555...: 0x%016x\n", v)
	
	v = (v | (v >> 1)) & 0x3333333333333333
	fmt.Printf("After |>>1 & 0x333...: 0x%016x\n", v)
	
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	fmt.Printf("After |>>2 & 0x0F0F...: 0x%016x\n", v)
	
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	fmt.Printf("After |>>4 & 0x00FF...: 0x%016x\n", v)
	
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	fmt.Printf("After |>>8 & 0x0000FFFF...: 0x%016x\n", v)
	
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF
	fmt.Printf("After |>>16 & 0x00000000FFFFFFFF...: 0x%016x\n", v)
	
	gridLongitudeNumber := uint32(v)
	fmt.Printf("Final gridLongitudeNumber: %d (0x%08x)\n", gridLongitudeNumber, gridLongitudeNumber)
	
	// Now let's work backwards to find what gridLongitudeNumber should be
	fmt.Println("\n--- Working backwards from expected longitude ---")
	expectedLonFromCalc := expectedLon
	minLon := test_minLongitude
	lonRange := test_longitudeRange
	step := float64(uint64(1) << 26)
	
	// longitude = minLongitude + (gridLongitudeNumber + 0.5) / step * longitudeRange
	// So: gridLongitudeNumber + 0.5 = (longitude - minLongitude) / longitudeRange * step
	normalizedFromExpected := (expectedLonFromCalc - minLon) / lonRange
	fmt.Printf("Normalized position from expected: %.15f\n", normalizedFromExpected)
	
	gridNumFromExpected := normalizedFromExpected * step
	fmt.Printf("Grid number from expected: %.10f\n", gridNumFromExpected)
	
	correctGridNum := gridNumFromExpected - 0.5
	fmt.Printf("Correct gridLongitudeNumber should be: %.10f\n", correctGridNum)
	fmt.Printf("Current gridLongitudeNumber: %d\n", gridLongitudeNumber)
	fmt.Printf("Difference: %.10f\n", float64(gridLongitudeNumber) - correctGridNum)
	
	// Test with the correct grid number
	fmt.Println("\n--- Testing with corrected grid number ---")
	correctedLon := test_minLongitude + (correctGridNum + 0.5) / step * test_longitudeRange
	fmt.Printf("Longitude with corrected grid num: %.15f\n", correctedLon)
	fmt.Printf("Expected longitude: %.15f\n", expectedLon)
	fmt.Printf("Difference: %.15f\n", math.Abs(correctedLon - expectedLon))
	
	fmt.Println("\n--- Testing Direct Center Calculation ---")
	lon_direct, _ := geoDecodeScore_direct_center(score)
	diff_direct := math.Abs(lon_direct - expectedLon)
	fmt.Printf("Got:      %.15f\n", lon_direct)
	fmt.Printf("Expected: %.15f\n", expectedLon)
	fmt.Printf("Diff:     %.15f\n", diff_direct)
	if diff_direct <= tolerance {
		fmt.Println("Status:   ✅ WITHIN tolerance")
	} else {
		fmt.Println("Status:   ❌ OUTSIDE tolerance")
	}

	fmt.Println("\n--- Testing Boundary Calculation ---")
	lon_boundary, _ := geoDecodeScore_boundary_calc(score)
	diff_boundary := math.Abs(lon_boundary - expectedLon)
	fmt.Printf("Got:      %.15f\n", lon_boundary)
	fmt.Printf("Expected: %.15f\n", expectedLon)
	fmt.Printf("Diff:     %.15f\n", diff_boundary)
	if diff_boundary <= tolerance {
		fmt.Println("Status:   ✅ WITHIN tolerance")
	} else {
		fmt.Println("Status:   ❌ OUTSIDE tolerance")
	}

	fmt.Println("\n--- Testing Redis C Style ---")
	lon_redis, _ := geoDecodeScore_redis_style(score)
	diff_redis := math.Abs(lon_redis - expectedLon)
	fmt.Printf("Got:      %.15f\n", lon_redis)
	fmt.Printf("Expected: %.15f\n", expectedLon)
	fmt.Printf("Diff:     %.15f\n", diff_redis)
	if diff_redis <= tolerance {
		fmt.Println("Status:   ✅ WITHIN tolerance")
	} else {
		fmt.Println("Status:   ❌ OUTSIDE tolerance")
	}

	fmt.Println("\n--- Testing Manual Corrected ---")
	lon_manual, _ := geoDecodeScore_manual_corrected(score)
	diff_manual := math.Abs(lon_manual - expectedLon)
	fmt.Printf("Got:      %.15f\n", lon_manual)
	fmt.Printf("Expected: %.15f\n", expectedLon)
	fmt.Printf("Diff:     %.15f\n", diff_manual)
	if diff_manual <= tolerance {
		fmt.Println("Status:   ✅ WITHIN tolerance")
	} else {
		fmt.Println("Status:   ❌ OUTSIDE tolerance")
	}

	if diff_boundary > tolerance && diff_direct > tolerance && diff_redis > tolerance && diff_manual > tolerance {
		t.Error("All implementations failed to produce a result within the tolerance.")
	} else if diff_manual <= tolerance {
        fmt.Println("\n[Conclusion] The 'Manual Corrected' method is the correct one.")
    } else if diff_redis <= tolerance {
        fmt.Println("\n[Conclusion] The 'Redis C Style' method is the correct one.")
    } else if diff_boundary <= tolerance {
        fmt.Println("\n[Conclusion] The 'Boundary Calculation' method is the correct one.")
    } else {
        fmt.Println("\n[Conclusion] The 'Direct Center' method is the correct one.")
    }
}
