// Package ledge is the Ledge Go SDK (Phase 2b, Tier 4).
//
// A Client wraps net/http to the Rust control plane's binary POST /rpc
// endpoint. Each method encodes a Cap'n Proto Request (generated from the
// shared sdk/schema/ledge.capnp contract into the ledgepb package), POSTs it as
// a standard framed (unpacked) capnp message with Content-Type
// application/x-ledge-capnp, then decodes the framed capnp Response.
//
// Wire-format note: the Rust server uses capnp::serialize (unpacked, framed
// segments). go-capnp Message.Marshal emits exactly that framing and
// capnp.Unmarshal reads it — no packing on either side.
package ledge

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"

	capnp "capnproto.org/go/capnp/v3"

	"github.com/vanshverma/ledge/sdk/go/ledgepb"
)

// contentType is the wire content type for capnp /rpc request and response bodies.
const contentType = "application/x-ledge-capnp"

// RefEntry is a single versioned ref state (decoded from the capnp RefEntry).
type RefEntry struct {
	Target  []byte // 32-byte BLAKE3 content address the ref points at.
	HLC     uint64 // Hybrid logical clock stamp of the write.
	Version uint64 // Monotonic per-ref version for optimistic concurrency.
}

// NamedRef is a named ref carried by a workspace view (e.g. refs/heads/main).
type NamedRef struct {
	Name  string
	Entry RefEntry
}

// WorkspaceInfo is a point-in-time view returned by Fork / GetWorkspace.
type WorkspaceInfo struct {
	ID          string
	ExpiresAtMs uint64
	Refs        []NamedRef
}

// Lease is a workspace lease returned by Renew.
type Lease struct {
	ID          string
	ExpiresAtMs uint64
	CreatedAtMs uint64
	Generation  uint64
}

// GcStats is per-pass garbage-collection accounting returned by RunGc.
type GcStats struct {
	Scanned    uint64
	Reachable  uint64
	Reclaimed  uint64
	BytesFreed uint64
}

// CommitMapping is one (workspace ref -> durable ref) promotion request for Commit.
type CommitMapping struct {
	WorkspaceRef string
	DurableRef   string
}

// CommitOutcome is the result of promoting one workspace ref to a durable ref.
type CommitOutcome struct {
	Target  string
	OK      bool   // true if the promotion landed; false on a concurrent-write conflict.
	Version uint64 // on OK, the promoted version; on conflict, the live version.
}

// RPCError is returned when the server encodes a business error in Response.error.
type RPCError struct{ Message string }

func (e *RPCError) Error() string { return "ledge rpc error: " + e.Message }

// TransportError is returned when the HTTP layer rejects the request (e.g. 400).
type TransportError struct {
	Status int
	Body   string
}

func (e *TransportError) Error() string {
	if e.Body != "" {
		return fmt.Sprintf("ledge transport error: HTTP %d: %s", e.Status, e.Body)
	}
	return fmt.Sprintf("ledge transport error: HTTP %d", e.Status)
}

// Client is a typed client for the Ledge control plane over Cap'n Proto POST /rpc.
//
// Construct with NewClient(baseURL); baseURL has no trailing /rpc.
type Client struct {
	rpcURL string
	http   *http.Client
}

// NewClient returns a Client targeting the server at baseURL (e.g.
// "http://127.0.0.1:8080"). A trailing slash is trimmed so ${base}/rpc is
// well-formed. The default http.Client is used unless WithHTTPClient is set.
func NewClient(baseURL string) *Client {
	return &Client{
		rpcURL: strings.TrimRight(baseURL, "/") + "/rpc",
		http:   http.DefaultClient,
	}
}

// WithHTTPClient overrides the underlying *http.Client (timeouts, transport).
func (c *Client) WithHTTPClient(h *http.Client) *Client {
	c.http = h
	return c
}

// WriteObject stores a git object body. Returns its 32-byte BLAKE3 id.
func (c *Client) WriteObject(gitType uint8, content []byte) ([]byte, error) {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetWriteObject()
		wo := r.WriteObject()
		wo.SetGitType(gitType)
		return wo.SetContent(content)
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_objectId); err != nil {
		return nil, err
	}
	oid, err := resp.ObjectId()
	if err != nil {
		return nil, err
	}
	return cloneBytesOf(oid.Bytes())
}

// ReadObject reads an object's content by its 32-byte id.
func (c *Client) ReadObject(id []byte) ([]byte, error) {
	if len(id) != 32 {
		return nil, fmt.Errorf("object id must be 32 bytes, got %d", len(id))
	}
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetReadObject()
		oid, err := r.ReadObject().NewId()
		if err != nil {
			return err
		}
		return oid.SetBytes(id)
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_objectContent); err != nil {
		return nil, err
	}
	content, err := resp.ObjectContent()
	if err != nil {
		return nil, err
	}
	return clone(content), nil
}

// Fork creates a fresh workspace seeded from sources (durable ref names), leased
// for ttlSeconds (0 = server default). Returns the new workspace view.
func (c *Client) Fork(sources []string, ttlSeconds uint64) (*WorkspaceInfo, error) {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetFork()
		f := r.Fork()
		list, err := f.NewSources(int32(len(sources)))
		if err != nil {
			return err
		}
		for i, s := range sources {
			if err := list.Set(i, s); err != nil {
				return err
			}
		}
		f.SetTtlSeconds(ttlSeconds)
		return nil
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_workspace); err != nil {
		return nil, err
	}
	w, err := resp.Workspace()
	if err != nil {
		return nil, err
	}
	return decodeWorkspace(w)
}

// Commit promotes workspace refs to durable refs. One outcome per mapping, in order.
func (c *Client) Commit(workspaceID string, mappings []CommitMapping) ([]CommitOutcome, error) {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetCommit()
		cm := r.Commit()
		if err := cm.SetWorkspaceId(workspaceID); err != nil {
			return err
		}
		list, err := cm.NewMappings(int32(len(mappings)))
		if err != nil {
			return err
		}
		for i, m := range mappings {
			if err := list.At(i).SetWorkspaceRef(m.WorkspaceRef); err != nil {
				return err
			}
			if err := list.At(i).SetDurableRef(m.DurableRef); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_commitOutcomes); err != nil {
		return nil, err
	}
	list, err := resp.CommitOutcomes()
	if err != nil {
		return nil, err
	}
	out := make([]CommitOutcome, list.Len())
	for i := 0; i < list.Len(); i++ {
		o := list.At(i)
		target, err := o.Target()
		if err != nil {
			return nil, err
		}
		out[i] = CommitOutcome{Target: target, OK: o.Ok(), Version: o.Version()}
	}
	return out, nil
}

// Renew extends a workspace's lease by ttlSeconds (0 = server default).
func (c *Client) Renew(workspaceID string, ttlSeconds uint64) (*Lease, error) {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetRenew()
		rn := r.Renew()
		if err := rn.SetWorkspaceId(workspaceID); err != nil {
			return err
		}
		rn.SetTtlSeconds(ttlSeconds)
		return nil
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_lease); err != nil {
		return nil, err
	}
	l, err := resp.Lease()
	if err != nil {
		return nil, err
	}
	id, err := l.Id()
	if err != nil {
		return nil, err
	}
	return &Lease{
		ID:          id,
		ExpiresAtMs: l.ExpiresAtMs(),
		CreatedAtMs: l.CreatedAtMs(),
		Generation:  l.Generation(),
	}, nil
}

// Release tears down a workspace and its lease.
func (c *Client) Release(workspaceID string) error {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetRelease()
		return r.Release().SetWorkspaceId(workspaceID)
	})
	if err != nil {
		return err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return err
	}
	defer rel()
	return expect(resp, ledgepb.Response_Which_ok)
}

// GetWorkspace fetches a point-in-time view of a workspace by id.
func (c *Client) GetWorkspace(workspaceID string) (*WorkspaceInfo, error) {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetGetWorkspace()
		return r.GetWorkspace().SetWorkspaceId(workspaceID)
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_workspace); err != nil {
		return nil, err
	}
	w, err := resp.Workspace()
	if err != nil {
		return nil, err
	}
	return decodeWorkspace(w)
}

// ListWorkspaces lists every live workspace.
func (c *Client) ListWorkspaces() ([]WorkspaceInfo, error) {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetListWorkspaces()
		return nil
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_workspaceList); err != nil {
		return nil, err
	}
	list, err := resp.WorkspaceList()
	if err != nil {
		return nil, err
	}
	out := make([]WorkspaceInfo, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		w, err := decodeWorkspace(list.At(i))
		if err != nil {
			return nil, err
		}
		out = append(out, *w)
	}
	return out, nil
}

// RunGc runs one mark-and-sweep garbage-collection pass; returns its accounting.
func (c *Client) RunGc() (*GcStats, error) {
	req, err := newRequest(func(r ledgepb.Request) error {
		r.SetRunGc()
		return nil
	})
	if err != nil {
		return nil, err
	}
	resp, rel, err := c.call(req)
	if err != nil {
		return nil, err
	}
	defer rel()
	if err := expect(resp, ledgepb.Response_Which_gcStats); err != nil {
		return nil, err
	}
	g, err := resp.GcStats()
	if err != nil {
		return nil, err
	}
	return &GcStats{
		Scanned:    g.Scanned(),
		Reachable:  g.Reachable(),
		Reclaimed:  g.Reclaimed(),
		BytesFreed: g.BytesFreed(),
	}, nil
}

// transport ------------------------------------------------------------------

// newRequest builds a framed capnp Request via build and returns its bytes.
func newRequest(build func(ledgepb.Request) error) ([]byte, error) {
	msg, seg, err := capnp.NewMessage(capnp.SingleSegment(nil))
	if err != nil {
		return nil, err
	}
	root, err := ledgepb.NewRootRequest(seg)
	if err != nil {
		return nil, err
	}
	if err := build(root); err != nil {
		return nil, err
	}
	return msg.Marshal()
}

// call POSTs the framed request body and returns the decoded Response together
// with a release func the caller MUST defer (it frees the decode message). On a
// non-2xx status it returns a *TransportError; the Response.error variant is
// surfaced by expect.
func (c *Client) call(body []byte) (ledgepb.Response, func(), error) {
	var zero ledgepb.Response
	httpReq, err := http.NewRequest(http.MethodPost, c.rpcURL, bytes.NewReader(body))
	if err != nil {
		return zero, func() {}, err
	}
	httpReq.Header.Set("Content-Type", contentType)

	httpResp, err := c.http.Do(httpReq)
	if err != nil {
		return zero, func() {}, err
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return zero, func() {}, err
	}
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return zero, func() {}, &TransportError{Status: httpResp.StatusCode, Body: string(respBody)}
	}

	msg, err := capnp.Unmarshal(respBody)
	if err != nil {
		return zero, func() {}, fmt.Errorf("decode response: %w", err)
	}
	resp, err := ledgepb.ReadRootResponse(msg)
	if err != nil {
		msg.Release()
		return zero, func() {}, fmt.Errorf("read response root: %w", err)
	}
	return resp, func() { msg.Release() }, nil
}

// expect asserts the response union tag, surfacing a server error if present.
func expect(resp ledgepb.Response, want ledgepb.Response_Which) error {
	got := resp.Which()
	if got == want {
		return nil
	}
	if got == ledgepb.Response_Which_error {
		msg, err := resp.Error()
		if err != nil {
			return err
		}
		return &RPCError{Message: msg}
	}
	return &RPCError{Message: fmt.Sprintf("unexpected response variant %d (wanted %d)", got, want)}
}

// decode helpers (capnp readers -> plain owned values) ------------------------

func decodeWorkspace(w ledgepb.WorkspaceInfo) (*WorkspaceInfo, error) {
	id, err := w.Id()
	if err != nil {
		return nil, err
	}
	refsList, err := w.Refs()
	if err != nil {
		return nil, err
	}
	refs := make([]NamedRef, refsList.Len())
	for i := 0; i < refsList.Len(); i++ {
		nr := refsList.At(i)
		name, err := nr.Name()
		if err != nil {
			return nil, err
		}
		entry, err := nr.Entry()
		if err != nil {
			return nil, err
		}
		target, err := entry.Target()
		if err != nil {
			return nil, err
		}
		tb, err := cloneBytesOf(target.Bytes())
		if err != nil {
			return nil, err
		}
		refs[i] = NamedRef{
			Name: name,
			Entry: RefEntry{
				Target:  tb,
				HLC:     entry.Hlc(),
				Version: entry.Version(),
			},
		}
	}
	return &WorkspaceInfo{ID: id, ExpiresAtMs: w.ExpiresAtMs(), Refs: refs}, nil
}

// clone copies a []byte so the result outlives the capnp message buffer.
func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// cloneBytesOf clones a (b, err) capnp Data accessor result.
func cloneBytesOf(b []byte, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	return clone(b), nil
}
