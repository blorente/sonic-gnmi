package tpm

import (
	"context"

	"github.com/sonic-net/sonic-gnmi/crypta_client/pkg/ec"
)

// TPM implements go-tpm's transport.TPM interface, for transmitting TPM commands to a Haven TPM.
type TPM struct {
	cd ec.CommandDispatcher
}

// New creates a go-tpm transport.TPM based on the given CommandDispatcher.
// Closing the TPM will also close the CommandDispatcher.
func New(cd ec.CommandDispatcher) *TPM {
	return &TPM{
		cd: cd,
	}
}

// Send implements the transport.TPM interface.
func (t *TPM) Send(input []byte) ([]byte, error) {
	ctx := context.Background()
	return SendCommand(ctx, t.cd, input)
}

// Close closes the connection to the TPM.
func (t *TPM) Close() error {
	return t.cd.Close()
}
