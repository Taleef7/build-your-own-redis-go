package main

import (
	"fmt"
	"math"
)

const (
	lonMin  = -180.0
	lonMax  = 180.0
	latMin  = -85.05112878
	latMax  = 85.05112878
	geoStep = 26
)

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func geoHashScore(lon, lat float64) uint64 {
	lon = clamp(lon, lonMin, lonMax)
	lat = clamp(lat, latMin, latMax)
	// Normalize to [0,1)
	lonNorm := (lon - lonMin) / (lonMax - lonMin) // (lon+180)/360
	latNorm := (lat - latMin) / (latMax - latMin)
	// Scale to 26-bit integer space
	lonBits := uint64(math.Floor(lonNorm * (1 << geoStep)))
	latBits := uint64(math.Floor(latNorm * (1 << geoStep)))
	if lonBits >= (1 << geoStep) {
		lonBits = (1 << geoStep) - 1
	}
	if latBits >= (1 << geoStep) {
		latBits = (1 << geoStep) - 1
	}
	// Interleave MSB-first: lon bit then lat bit
	var result uint64
	for i := 0; i < geoStep; i++ {
		lonBit := (lonBits >> (geoStep - 1 - i)) & 1
		latBit := (latBits >> (geoStep - 1 - i)) & 1
		result = (result << 1) | lonBit
		result = (result << 1) | latBit
	}
	return result
}

func main() {
	parisLon, parisLat := 2.2944692, 48.8584625
	londonLon, londonLat := -0.127758, 51.507351
	fmt.Println("Paris:", geoHashScore(parisLon, parisLat))
	fmt.Println("London:", geoHashScore(londonLon, londonLat))
}
