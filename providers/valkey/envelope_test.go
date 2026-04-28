package valkey

import (
	"encoding/json"
	"testing"
)

func TestEncodePubSubEnvelope(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
		data []byte
	}{
		{name: "standard", key: []byte("my-key"), data: []byte("my-data")},
		{name: "empty key", key: []byte{}, data: []byte("data")},
		{name: "binary data", key: []byte("k"), data: []byte{0, 1, 2, 255}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodePubSubEnvelope(tt.key, tt.data)
			if err != nil {
				t.Fatalf("encodePubSubEnvelope() error = %v", err)
			}

			var env pubsubEnvelope
			if err := json.Unmarshal([]byte(encoded), &env); err != nil {
				t.Fatalf("cannot unmarshal envelope: %v", err)
			}

			if env.Key != string(tt.key) {
				t.Errorf("Key = %q, want %q", env.Key, string(tt.key))
			}
			if string(env.Data) != string(tt.data) {
				t.Errorf("Data = %v, want %v", env.Data, tt.data)
			}
		})
	}
}

func TestIsGroupExistsError(t *testing.T) {
	if isGroupExistsError(nil) {
		t.Error("expected false for nil error")
	}

	var s string
	plainErr := json.Unmarshal([]byte("bad"), &s)
	if isGroupExistsError(plainErr) {
		t.Error("expected false for non-ValkeyError")
	}
}
