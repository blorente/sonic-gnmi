package ec

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// GetVersion issues the EC_GET_VERSION command to the EC.
func GetVersion(ctx context.Context, cd CommandDispatcher) (*Version, error) {
	rsp, err := SendCommand(ctx, cd, Command{
		Code:    ecGetVersionCommand,
		Version: 0,
	})
	if err != nil {
		return nil, err
	}
	rspRdr := bytes.NewReader(rsp)
	var ver rawGetVersionResponse
	if err := binary.Read(rspRdr, binary.LittleEndian, &ver); err != nil {
		return nil, err
	}
	if extraData := rspRdr.Len(); extraData > 0 {
		return nil, fmt.Errorf("%d bytes of extra data at end of response", extraData)
	}
	// Convert the fixed-width ASCII strings returned by EC into Go strings.
	return &Version{
		RO:           string(bytes.Trim(ver.RO[:], "\x00")),
		RW:           string(bytes.Trim(ver.RW[:], "\x00")),
		CurrentImage: ver.CurrentImage,
	}, nil
}
