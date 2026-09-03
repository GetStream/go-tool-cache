// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package wire contains the JSON types that cmd/go uses
// to communicate with child processes implementing
// the cache interface.
package wire

import (
	"context"
	"io"
)

// ActionKindHeader is the HTTP header go-cacher uses to forward Request.ActionKind
// to gocached and gocacheproxy. Unpatched cmd/go omits ActionKind, so the header
// is absent and Prometheus series use action_kind="".
const ActionKindHeader = "Go-Action-Kind"

type actionKindContextKey struct{}

// ContextWithActionKind returns a child context carrying a sanitized ActionKind.
// An empty or invalid kind leaves ctx unchanged.
func ContextWithActionKind(ctx context.Context, kind string) context.Context {
	kind = SanitizeActionKind(kind)
	if kind == "" {
		return ctx
	}
	return context.WithValue(ctx, actionKindContextKey{}, kind)
}

// ActionKindFromContext returns the ActionKind stored by ContextWithActionKind,
// or "" if none.
func ActionKindFromContext(ctx context.Context) string {
	kind, _ := ctx.Value(actionKindContextKey{}).(string)
	return kind
}

// SanitizeActionKind returns kind if it is a coarse cache-action token safe
// for Prometheus labels (compile, link, test, vet, …). Anything else, including
// NewHash names that embed import paths, is treated as unset so cardinality
// cannot explode.
func SanitizeActionKind(kind string) string {
	if kind == "" || len(kind) > 64 {
		return ""
	}
	for i := 0; i < len(kind); i++ {
		c := kind[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return ""
		}
	}
	return kind
}

// Cmd is a command that can be issued to a child process.
//
// If the interface needs to grow, we can add new commands or new versioned
// commands like "get2".
type Cmd string

const (
	CmdGet   = Cmd("get")
	CmdPut   = Cmd("put")
	CmdClose = Cmd("close")
)

// Request is the JSON-encoded message that's sent from cmd/go to
// the GOCACHEPROG child process over stdin. Each JSON object is on its
// own line. A Request of Type "put" with BodySize > 0 will be followed
// by a line containing a base64-encoded JSON string literal of the body.
type Request struct {
	// ID is a unique number per process across all requests.
	// It must be echoed in the Response from the child.
	ID int64

	// Command is the type of request.
	// The cmd/go tool will only send commands that were declared
	// as supported by the child.
	Command Cmd

	// ActionID is non-nil for get and puts.
	ActionID []byte `json:",omitempty"` // or nil if not used

	// ActionKind is an optional coarse kind for the cache action (compile,
	// link, test, vet, …). Stock cmd/go does not send it; a patched toolchain
	// may. Empty means unknown.
	ActionKind string `json:",omitempty"`

	// OutputID is set for Type "put" and "output-file".
	OutputID []byte `json:",omitempty"` // or nil if not used

	// ObjectID is the name of `OutputID` before Go1.24, it will be removed in Go1.25.
	// It's used for backward compatibility.
	ObjectID []byte `json:",omitempty"`

	// Body is the body for "put" requests. It's sent after the JSON object
	// as a base64-encoded JSON string when BodySize is non-zero.
	// It's sent as a separate JSON value instead of being a struct field
	// send in this JSON object so large values can be streamed in both directions.
	// The base64 string body of a Request will always be written
	// immediately after the JSON object and a newline.
	Body io.Reader `json:"-"`

	// BodySize is the number of bytes of Body. If zero, the body isn't written.
	BodySize int64 `json:",omitempty"`
}

// Response is the JSON response from the child process to cmd/go.
//
// With the exception of the first protocol message that the child writes to its
// stdout with ID==0 and KnownCommands populated, these are only sent in
// response to a Request from cmd/go.
//
// Responses can be sent in any order. The ID must match the request they're
// replying to.
type Response struct {
	ID  int64  // that corresponds to Request; they can be answered out of order
	Err string `json:",omitempty"` // if non-empty, the error

	// KnownCommands is included in the first message that cache helper program
	// writes to stdout on startup (with ID==0). It includes the
	// Request.Command types that are supported by the program.
	//
	// This lets us extend the gracefully over time (adding "get2", etc), or
	// fail gracefully when needed. It also lets us verify the program
	// wants to be a cache helper.
	KnownCommands []Cmd `json:",omitempty"`

	// For Get requests.

	Miss      bool   `json:",omitempty"` // cache miss
	OutputID  []byte `json:",omitempty"`
	Size      int64  `json:",omitempty"`
	TimeNanos int64  `json:",omitempty"` // TODO(bradfitz): document

	// DiskPath is the absolute path on disk of the OutputID corresponding
	// a "get" request's ActionID (on cache hit) or a "put" request's
	// provided OutputID.
	DiskPath string `json:",omitempty"`
}
