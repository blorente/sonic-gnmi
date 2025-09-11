package ec

import (
	"fmt"
)

const (
	ecGetVersionCommand uint16 = 0x0002
)

// Image represents a value of ec_current_image.
type Image uint32

const (
	// Unknown is EC_IMAGE_UNKNOWN.
	Unknown Image = 0
	// RO is EC_IMAGE_RO.
	RO Image = 1
	// RW is EC_IMAGE_RW.
	RW Image = 2
)

func (i Image) String() string {
	switch i {
	case Unknown:
		return "EC_IMAGE_UNKNOWN"
	case RO:
		return "EC_IMAGE_RO"
	case RW:
		return "EC_IMAGE_RW"
	}
	return fmt.Sprintf("unrecognized ec_current_image value (0x%x)", uint32(i))
}

// rawGetVersionResponse represents a raw ec_response_get_version response.
type rawGetVersionResponse struct {
	RO           [32]byte
	RW           [32]byte
	Reserved     [32]byte
	CurrentImage Image
}

// Version represents a processed ec_response_get_version response.
type Version struct {
	RO           string
	RW           string
	CurrentImage Image
}
