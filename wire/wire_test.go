package wire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestSanitizeActionKind(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"compile", "compile"},
		{"gccgo-compile", "gccgo-compile"},
		{"go_test", "go_test"},
		{"build fmt", ""},
		{"compile/link", ""},
		{strings.Repeat("a", 65), ""},
	}
	for _, tt := range tests {
		if got := SanitizeActionKind(tt.in); got != tt.want {
			t.Errorf("SanitizeActionKind(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestRequestActionKindJSON(t *testing.T) {
	var req Request
	if err := json.Unmarshal([]byte(`{"ID":1,"Command":"get","ActionKind":"vet"}`), &req); err != nil {
		t.Fatal(err)
	}
	if req.ActionKind != "vet" {
		t.Fatalf("ActionKind = %q, want vet", req.ActionKind)
	}
	var omitted Request
	if err := json.Unmarshal([]byte(`{"ID":1,"Command":"get"}`), &omitted); err != nil {
		t.Fatal(err)
	}
	if omitted.ActionKind != "" {
		t.Fatalf("omitted ActionKind = %q, want empty", omitted.ActionKind)
	}
}

func TestActionKindContext(t *testing.T) {
	ctx := ContextWithActionKind(context.Background(), "compile")
	if got := ActionKindFromContext(ctx); got != "compile" {
		t.Fatalf("got %q, want compile", got)
	}
	ctx = ContextWithActionKind(context.Background(), "not valid")
	if got := ActionKindFromContext(ctx); got != "" {
		t.Fatalf("invalid kind stored as %q", got)
	}
}
