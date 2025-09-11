package tpm

import (
	"context"

	"github.com/sonic-net/sonic-gnmi/crypta_client/pkg/ec"
)

const (
	ecHavenSendSendTPMCommand uint16 = 0x3e33
)

// SendCommand sends a TPM command to the Haven.
func SendCommand(ctx context.Context, cd ec.CommandDispatcher, cmd []byte) ([]byte, error) {
	rsp, err := ec.SendCommand(ctx, cd, ec.Command{ecHavenSendSendTPMCommand, 0}, cmd)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}
