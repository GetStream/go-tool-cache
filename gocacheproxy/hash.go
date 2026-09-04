package gocacheproxy

import (
	"cmp"
	"errors"
	"slices"

	"github.com/cespare/xxhash/v2"
)

var errBadActionID = errors.New("malformed actionID")

// validActionID reports whether actionID is lowercase hex of even length.
func validActionID(actionID string) bool {
	if len(actionID) < 4 || len(actionID)%2 != 0 {
		return false
	}
	for i := range actionID {
		b := actionID[i]
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return false
	}
	return true
}

func hrwScore(actionID, backendID string) uint64 {
	h := xxhash.New()
	h.WriteString(actionID)
	h.Write([]byte{0})
	h.WriteString(backendID)
	return h.Sum64()
}

type scoredBackend struct {
	b     *Backend
	score uint64
}

// Candidates returns backends in HRW order (highest score first) for actionID.
func (p *Proxy) Candidates(actionID string) ([]*Backend, error) {
	if !validActionID(actionID) {
		return nil, errBadActionID
	}
	items := make([]scoredBackend, len(p.Backends))
	for i, b := range p.Backends {
		items[i] = scoredBackend{b: b, score: hrwScore(actionID, b.URL)}
	}
	slices.SortFunc(items, func(a, b scoredBackend) int {
		return cmp.Compare(b.score, a.score)
	})
	out := make([]*Backend, len(items))
	for i, it := range items {
		out[i] = it.b
	}
	return out, nil
}
