package main

import (
	"context"
	"fmt"
	"io"

	"github.com/GetStream/go-tool-cache/wire"
)

// moduleIndexActionKind is the ActionKind of the entries cmd/go writes for its
// module index (cache.NewHash("moduleIndex") in cmd/go/internal/modindex). Only a
// cmd/go that sends Request.ActionKind tags them; with any other cmd/go they are
// indistinguishable from other actions and follow the default path.
const moduleIndexActionKind = "moduleIndex"

// Values of the -module-index flag.
const (
	// moduleIndexAuto stores module index entries like any other action. This
	// is the current behavior.
	moduleIndexAuto = "auto"
	// moduleIndexRemote stores module index entries like any other action,
	// including the remote cache when -cache-server is set.
	moduleIndexRemote = "remote"
	// moduleIndexLocal keeps module index entries on the local disk cache
	// only, never contacting the remote cache for them.
	moduleIndexLocal = "local"
)

type getFunc func(ctx context.Context, actionID string) (outputID, diskPath string, _ error)

type putFunc func(ctx context.Context, actionID, outputID string, size int64, r io.Reader) (diskPath string, _ error)

func validateModuleIndexMode(mode string) error {
	switch mode {
	case moduleIndexAuto, moduleIndexRemote, moduleIndexLocal:
		return nil
	}
	return fmt.Errorf("-module-index must be %q, %q or %q, got %q", moduleIndexAuto, moduleIndexRemote, moduleIndexLocal, mode)
}

// routeModuleIndex returns the Get and Put to use for mode. In "local" mode,
// module index entries go to localGet/localPut and everything else to get/put;
// otherwise get and put are returned unchanged.
//
// The module index key includes each source file's mtime, so a fresh checkout
// misses every entry. Over the remote cache each miss and the following upload
// cost a round trip, and `go list ./...` asks one package at a time.
func routeModuleIndex(mode string, get getFunc, put putFunc, localGet getFunc, localPut putFunc) (getFunc, putFunc) {
	if mode != moduleIndexLocal {
		return get, put
	}
	routedGet := func(ctx context.Context, actionID string) (string, string, error) {
		if wire.ActionKindFromContext(ctx) == moduleIndexActionKind {
			return localGet(ctx, actionID)
		}
		return get(ctx, actionID)
	}
	routedPut := func(ctx context.Context, actionID, outputID string, size int64, r io.Reader) (string, error) {
		if wire.ActionKindFromContext(ctx) == moduleIndexActionKind {
			return localPut(ctx, actionID, outputID, size, r)
		}
		return put(ctx, actionID, outputID, size, r)
	}
	return routedGet, routedPut
}
