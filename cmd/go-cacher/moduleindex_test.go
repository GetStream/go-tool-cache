package main

import (
	"context"
	"io"
	"testing"

	"github.com/GetStream/go-tool-cache/wire"
)

// fakeStore records which store served each request.
type fakeStore struct {
	name  string
	calls *[]string
}

func (s fakeStore) get(ctx context.Context, actionID string) (string, string, error) {
	*s.calls = append(*s.calls, s.name+" get "+actionID)
	return "", "", nil
}

func (s fakeStore) put(ctx context.Context, actionID, outputID string, size int64, r io.Reader) (string, error) {
	*s.calls = append(*s.calls, s.name+" put "+actionID)
	return "", nil
}

func TestRouteModuleIndex(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want []string
	}{
		{moduleIndexAuto, []string{"default get idx", "default put idx", "default get obj", "default put obj", "default get none", "default put none"}},
		{moduleIndexRemote, []string{"default get idx", "default put idx", "default get obj", "default put obj", "default get none", "default put none"}},
		{moduleIndexLocal, []string{"local get idx", "local put idx", "default get obj", "default put obj", "default get none", "default put none"}},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			var calls []string
			def := fakeStore{"default", &calls}
			local := fakeStore{"local", &calls}
			get, put := routeModuleIndex(tc.mode, def.get, def.put, local.get, local.put)

			for _, req := range []struct{ kind, actionID string }{
				{moduleIndexActionKind, "idx"},
				{"compile", "obj"},
				{"", "none"}, // cmd/go without the ActionKind patch
			} {
				ctx := wire.ContextWithActionKind(context.Background(), req.kind)
				if _, _, err := get(ctx, req.actionID); err != nil {
					t.Fatal(err)
				}
				if _, err := put(ctx, req.actionID, "out", 0, nil); err != nil {
					t.Fatal(err)
				}
			}
			if len(calls) != len(tc.want) {
				t.Fatalf("calls = %q, want %q", calls, tc.want)
			}
			for i := range calls {
				if calls[i] != tc.want[i] {
					t.Fatalf("calls = %q, want %q", calls, tc.want)
				}
			}
		})
	}
}

func TestValidateModuleIndexMode(t *testing.T) {
	for _, mode := range []string{moduleIndexAuto, moduleIndexRemote, moduleIndexLocal} {
		if err := validateModuleIndexMode(mode); err != nil {
			t.Errorf("validateModuleIndexMode(%q) = %v, want nil", mode, err)
		}
	}
	for _, mode := range []string{"", "Local", "disk"} {
		if err := validateModuleIndexMode(mode); err == nil {
			t.Errorf("validateModuleIndexMode(%q) = nil, want an error", mode)
		}
	}
}
