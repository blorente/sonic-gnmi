package usb

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"

	"github.com/pkg/errors"
	"github.com/google/gousb"
)

var (
	// ErrRequestTooLarge indicates that the Haven command was too large to send.
	ErrRequestTooLarge = errors.New("Haven command too big")
	// ErrIncompleteWrite indicates that the entire USB packet wasn't written to the device.
	ErrIncompleteWrite = errors.New("incomplete packet write")
	// ErrMalformedResponse indicates that a malformed packet was received from the USB device.
	ErrMalformedResponse = errors.New("malformed USB response")
	// ErrWrongRequestID indicates that a packet for a different request was received from the USB device.
	ErrWrongRequestID = errors.New("USB response was for a different request")
	// ErrBadDevice indicates that there is something wrong about this device for use as a Crypta USB interface.
	ErrBadDevice = errors.New("bad USB device")
	// ErrDeviceNotFound indicates that the requested Crypta USB device was not found.
	ErrDeviceNotFound = errors.New("USB device not found")
)

const (
	// VendorIDGoogle is the USB vendor ID for Google.
	VendorIDGoogle uint16 = 0x18d1
	// ProductIDDauntless is the USB product ID for Dauntless.
	ProductIDDauntless uint16 = 0x022A
	// The maximum size of a Haven EC command or response over USB
	maxHavenRequestSize    = 1024
	maxHavenResponseSize   = 1024
	usbPacketSize          = 64
	maxRetriesWrongRequest = 10
)

// Dispatcher implements the ec.CommandDispatcher interface.
type Dispatcher struct {
	dev         *gousb.Device
	inEndpoint  int
	outEndpoint int
}

// Options contain the configuration options for initializing a USB connection to Haven.
type Options struct {
	VendorID  uint16
	ProductID uint16
}

// New instantiates a new Dispatcher.
func New(opts Options) (*Dispatcher, error) {
	ctx := gousb.NewContext()
	defer ctx.Close()

	vid := gousb.ID(opts.VendorID)
	pid := gousb.ID(opts.ProductID)

	dev, err := ctx.OpenDeviceWithVIDPID(vid, pid)
	if err != nil {
		return nil, err
	}
	if dev == nil {
		return nil, fmt.Errorf("%w: could not find attached USB device for vendor ID %v, product ID %v", ErrDeviceNotFound, vid, pid)
	}

	in, out, err := validateEndpointDescriptor(dev)
	if err != nil {
		return nil, err
	}

	return &Dispatcher{
		dev:         dev,
		inEndpoint:  in,
		outEndpoint: out,
	}, nil
}

// DispatchCommand implements ec.CommandDispatcher.
func (d *Dispatcher) DispatchCommand(ctx context.Context, cmd []byte) ([]byte, error) {
	if len(cmd) > maxHavenRequestSize {
		return nil, fmt.Errorf("%w: Haven command size was %d bytes, but the maximum is %d", ErrRequestTooLarge, len(cmd), maxHavenRequestSize)
	}

	iface, done, err := d.dev.DefaultInterface()
	if err != nil {
		return nil, err
	}
	defer done()

	out, err := iface.OutEndpoint(d.outEndpoint)
	if err != nil {
		return nil, err
	}
	in, err := iface.InEndpoint(d.inEndpoint)
	if err != nil {
		return nil, err
	}

	id, err := generateRequestID()
	if err != nil {
		return nil, err
	}

	formattedRequest := assembleRequestPacket(id, cmd)

	for i := 0; i < maxRetriesWrongRequest; i++ {
		bytesWritten, err := out.WriteContext(ctx, formattedRequest)
		if err != nil {
			return nil, err
		}
		if bytesWritten != len(formattedRequest) {
			return nil, fmt.Errorf("%w: only wrote %v of %v bytes to the device", ErrIncompleteWrite, bytesWritten, len(formattedRequest))
		}
		if len(formattedRequest)%usbPacketSize == 0 {
			// gousb doesn't have native support for libusb's
			// LIBUSB_TRANSFER_ADD_ZERO_PACKET.
			// Dauntless expects a terminating zero-length packet in the case that the
			// formatted request happens to be a multiple of the USB packet size.
			bytesWritten, err := out.WriteContext(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("writing zero-length packet: %w", err)
			}
			if bytesWritten != 0 {
				return nil, fmt.Errorf("%w: wrote %v instead of 0 bytes to the device", ErrIncompleteWrite, bytesWritten)
			}
		}

		rawRsp := make([]byte, maxHavenResponseSize+16)
		bytesRead, err := in.ReadContext(ctx, rawRsp)
		if err != nil {
			return nil, err
		}

		// Check that the response began with the same random request ID we provided above.
		// We do this to avoid cases where another caller crashed and left a response behind.
		rsp, err := parseResponsePacket(id, rawRsp[:bytesRead])
		if errors.Is(err, ErrWrongRequestID) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return rsp, nil
	}
	return nil, fmt.Errorf("%w: giving up after %v attempts to get a response with the expected request ID", ErrWrongRequestID, maxRetriesWrongRequest)
}

// DispatchCommandNoResponse implements ec.CommandDispatcher.
func (d *Dispatcher) DispatchCommandNoResponse(ctx context.Context, cmd []byte) error {
	_, err := d.DispatchCommand(ctx, cmd)
	return err
}

// Close closes the connection to the Haven.
func (d *Dispatcher) Close() error {
	return d.dev.Close()
}

// Consistency check the endpoint descriptor for the device and return the input and output endpoints.
func validateEndpointDescriptor(dev *gousb.Device) (in, out int, err error) {
	iface, done, err := dev.DefaultInterface()
	if err != nil {
		return 0, 0, err
	}
	defer done()

	foundIn, foundOut := false, false
	for addr, endpoint := range iface.Setting.Endpoints {
		if endpoint.MaxPacketSize != usbPacketSize {
			return 0, 0, fmt.Errorf("%w: device reported USB packet size of %v instead of %v", ErrBadDevice, endpoint.MaxPacketSize, usbPacketSize)
		}
		if endpoint.TransferType != gousb.TransferTypeBulk {
			return 0, 0, fmt.Errorf("%w: device reported transfer type of %v instead of %v", ErrBadDevice, endpoint.TransferType, gousb.TransferTypeBulk)
		}

		switch endpoint.Direction {
		case gousb.EndpointDirectionIn:
			// We expect only one IN endpoint.
			if foundIn {
				return 0, 0, fmt.Errorf("%w: device had multiple IN endpoints", ErrBadDevice)
			}
			in = endpoint.Number
			foundIn = true
		case gousb.EndpointDirectionOut:
			if foundOut {
				return 0, 0, fmt.Errorf("%w: device had multiple OUT endpoints", ErrBadDevice)
			}
			out = endpoint.Number
			foundOut = true
		default:
			return 0, 0, fmt.Errorf("%w: unrecognized direction %v for endpoint address %v", ErrBadDevice, endpoint.Direction, addr)
		}
	}

	if !foundIn {
		return 0, 0, fmt.Errorf("%w: no IN endpoint found", ErrBadDevice)
	}
	if !foundOut {
		return 0, 0, fmt.Errorf("%w: no OUT endpoint found", ErrBadDevice)
	}
	return in, out, nil
}

type requestID [16]byte

func generateRequestID() (requestID, error) {
	var result requestID
	if _, err := rand.Read(result[:]); err != nil {
		return result, fmt.Errorf("failed to generate request ID: %w", err)
	}
	return result, nil
}

func assembleRequestPacket(id requestID, cmd []byte) []byte {
	result := make([]byte, 0, 16+len(cmd))
	result = append(result, id[:]...)
	result = append(result, cmd...)
	return result
}

func parseResponsePacket(id requestID, rsp []byte) ([]byte, error) {
	if len(rsp) < 16 {
		return nil, fmt.Errorf("%w: USB packet containing %v bytes was too small (must be at least 16 bytes)", ErrMalformedResponse, len(rsp))
	}
	if !bytes.Equal(rsp[:16], id[:]) {
		return nil, fmt.Errorf("%w: response was for request ID 0x%0x, expected 0x%0x", ErrWrongRequestID, rsp[:16], id[:])
	}
	return rsp[16:], nil
}
