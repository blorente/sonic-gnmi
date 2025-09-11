package common_utils

import (
	"context"
	"testing"
)

func TestBertCapabilityIfNoCapabilityIsSet(t *testing.T) {
	lqHelper, err := NewLinkQualificationHelper()
	if err != nil || lqHelper == nil {
		t.Fatalf("Failed to create LinkQualificationHelper: %v", err)
	}

	defer lqHelper.Close()
	expectEqual(t, lqHelper.SupportsBert(), true)
}

func TestBertCapabilityIfPlatformDoesNotSupportBert(t *testing.T) {
	// Populate switch capability.
	client, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get the redis client: %v", err)
	}
	defer client.Close()

	// Cannot run BERT. Switch doesn't support.
	if err = client.HSet(context.Background(), switchCapabilityTable, bertLqKey, "false").Err(); err != nil {
		t.Fatalf("Failed to set switch capability: %v", err)
	}

	lqHelper, err := NewLinkQualificationHelper()
	if err != nil || lqHelper == nil {
		t.Fatalf("Failed to create LinkQualificationHelper: %v", err)
	}

	defer lqHelper.Close()
	expectEqual(t, lqHelper.SupportsBert(), false)
}

func TestBertCapabilityIfPlatformSupportsBert(t *testing.T) {
	// Populate switch capability.
	client, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get the redis client: %v", err)
	}
	defer client.Close()

	// Switch supports BERT.
	if err = client.HSet(context.Background(), switchCapabilityTable, bertLqKey, "true").Err(); err != nil {
		t.Fatalf("Failed to set switch capability: %v", err)
	}

	lqHelper, err := NewLinkQualificationHelper()
	if err != nil || lqHelper == nil {
		t.Fatalf("Failed to create LinkQualificationHelper: %v", err)
	}

	defer lqHelper.Close()
	expectEqual(t, lqHelper.SupportsBert(), true)
}

func TestPktLqCapabilityIfNoCapabilityIsSet(t *testing.T) {
	lqHelper, err := NewLinkQualificationHelper()
	if err != nil || lqHelper == nil {
		t.Fatalf("Failed to create LinkQualificationHelper: %v", err)
	}

	defer lqHelper.Close()
	expectEqual(t, lqHelper.SupportsPktLq(), true)
}

func TestPktLqCapabilityIfPlatformDoesNotSupportPktLq(t *testing.T) {
	// Populate switch capability.
	client, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get the redis client: %v", err)
	}
	defer client.Close()

	// Cannot run packet linkqual. Switch doesn't support.
	if err = client.HSet(context.Background(), switchCapabilityTable, pktLqKey, "false").Err(); err != nil {
		t.Fatalf("Failed to set switch capability: %v", err)
	}

	lqHelper, err := NewLinkQualificationHelper()
	if err != nil || lqHelper == nil {
		t.Fatalf("Failed to create LinkQualificationHelper: %v", err)
	}

	defer lqHelper.Close()
	expectEqual(t, lqHelper.SupportsPktLq(), false)
}

func TestPktLqCapabilityIfPlatformSupportsPktLq(t *testing.T) {
	// Populate switch capability.
	client, err := getRedisDBClient()
	if err != nil {
		t.Fatalf("Failed to get the redis client: %v", err)
	}
	defer client.Close()

	// Switch supports packet linkqual.
	if err = client.HSet(context.Background(), switchCapabilityTable, pktLqKey, "true").Err(); err != nil {
		t.Fatalf("Failed to set switch capability: %v", err)
	}

	lqHelper, err := NewLinkQualificationHelper()
	if err != nil || lqHelper == nil {
		t.Fatalf("Failed to create LinkQualificationHelper: %v", err)
	}

	defer lqHelper.Close()
	expectEqual(t, lqHelper.SupportsPktLq(), true)
}

func TestLinkQualHelperOnNilInterface(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			// If recover() returns a non-nil value, a panic occurred.
			t.Fatalf("Function under test panicked unexpectedly: %v", r)
		}
	}()

	// Nil pointer to LinkQualificationHelper.
	var h *LinkQualificationHelper

	// None of these functions should panic!
	h.SetBertCapability(true)
	_ = h.SupportsBert()
	h.SetPktLqCapability(false)
	_ = h.SupportsPktLq()
	h.Close()

}
