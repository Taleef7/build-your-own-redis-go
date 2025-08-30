package main

import (
	"fmt"
	"math"
)

const (
	minLatitudeTmp    = -85.05112878
	maxLatitudeTmp    = 85.05112878
	minLongitudeTmp   = -180.0
	maxLongitudeTmp   = 180.0
	latitudeRangeTmp  = maxLatitudeTmp - minLatitudeTmp
	longitudeRangeTmp = maxLongitudeTmp - minLongitudeTmp
)

func compactInt64ToInt32_tmp(v uint64) uint32 {
	v = v & 0x5555555555555555
	v = (v | (v >> 1)) & 0x3333333333333333
	v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
	v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
	v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
	v = (v | (v >> 16)) & 0x00000000FFFFFFFF
	return uint32(v)
}

func decode(score uint64) (float64, float64) {
	x := score
	y := score >> 1

	// deinterleave exactly as main.go current
	x = (x | (x >> 0)) & 0x5555555555555555
	y = (y | (y >> 0)) & 0x5555555555555555
	x = (x | (x >> 1)) & 0x3333333333333333
	y = (y | (y >> 1)) & 0x3333333333333333
	x = (x | (x >> 2)) & 0x0F0F0F0F0F0F0F0F
	y = (y | (y >> 2)) & 0x0F0F0F0F0F0F0F0F
	x = (x | (x >> 4)) & 0x00FF00FF00FF00FF
	y = (y | (y >> 4)) & 0x00FF00FF00FF00FF
	x = (x | (x >> 8)) & 0x0000FFFF0000FFFF
	y = (y | (y >> 8)) & 0x0000FFFF0000FFFF
	x = (x | (x >> 16)) & 0x00000000FFFFFFFF
	y = (y | (y >> 16)) & 0x00000000FFFFFFFF

	hash_sep := x | (y << 32)
	ilato := uint32(hash_sep)
	ilono := uint32(hash_sep >> 32)

	step := math.Pow(2, 26)
	lat := minLatitudeTmp + (float64(ilato)+0.5)/step*latitudeRangeTmp
	lon := minLongitudeTmp + (float64(ilono)+0.5)/step*longitudeRangeTmp
	return lon, lat
}

func main() {
	values := []uint64{3803618426268781, 996934393546598, 852202036440453}
	for _, v := range values {
		lon, lat := decode(v)
		fmt.Printf("score=%d -> lon=%.15f lat=%.15f\n", v, lon, lat)
	}
}
