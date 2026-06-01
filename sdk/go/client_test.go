package ledge

// Smoke test: the real Rust `ledge` server, spawned as a child process, driven
// entirely through the Go Client over capnp POST /rpc. Every assertion crosses
// the HTTP boundary and the capnp encode/decode path.

import (
	"bytes"
	"errors"
	"testing"
)

// withServer builds + spawns the server once for the whole package run.
func newClient(t *testing.T) (*Client, func()) {
	t.Helper()
	bin := buildServer(t)
	srv := startServer(t, bin)
	return NewClient(srv.baseURL), srv.stop
}

func TestSmoke(t *testing.T) {
	client, stop := newClient(t)
	defer stop()

	t.Run("write_read_roundtrip", func(t *testing.T) {
		content := []byte("hello ledge over capnp/http (go)")
		id, err := client.WriteObject(3, content) // git blob type = 3
		if err != nil {
			t.Fatalf("WriteObject: %v", err)
		}
		if len(id) != 32 {
			t.Fatalf("object id must be 32 bytes, got %d", len(id))
		}
		got, err := client.ReadObject(id)
		if err != nil {
			t.Fatalf("ReadObject: %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Fatalf("roundtrip mismatch: got %q want %q", got, content)
		}
	})

	t.Run("content_addressed", func(t *testing.T) {
		content := []byte("deterministic payload")
		a, err := client.WriteObject(3, content)
		if err != nil {
			t.Fatal(err)
		}
		b, err := client.WriteObject(3, content)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("same content must yield same id")
		}
	})

	t.Run("read_missing_raises", func(t *testing.T) {
		missing := bytes.Repeat([]byte{0xAB}, 32)
		_, err := client.ReadObject(missing)
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) {
			t.Fatalf("expected *RPCError, got %v", err)
		}
	})

	t.Run("list_workspaces_empty", func(t *testing.T) {
		list, err := client.ListWorkspaces()
		if err != nil {
			t.Fatal(err)
		}
		if len(list) != 0 {
			t.Fatalf("fresh server must have no workspaces, got %d", len(list))
		}
	})

	t.Run("run_gc_returns_stats", func(t *testing.T) {
		stats, err := client.RunGc()
		if err != nil {
			t.Fatal(err)
		}
		if stats.Reachable > stats.Scanned {
			t.Fatalf("gc invariant: reachable %d > scanned %d", stats.Reachable, stats.Scanned)
		}
	})

	t.Run("fork_get_release_lifecycle", func(t *testing.T) {
		ws, err := client.Fork(nil, 120)
		if err != nil {
			t.Fatalf("Fork: %v", err)
		}
		if len(ws.Refs) != 0 {
			t.Fatalf("fresh workspace must have no refs")
		}
		if ws.ExpiresAtMs == 0 {
			t.Fatalf("expiresAtMs must be set")
		}

		if !containsWorkspace(t, client, ws.ID) {
			t.Fatalf("workspace %s not in list", ws.ID)
		}

		fetched, err := client.GetWorkspace(ws.ID)
		if err != nil {
			t.Fatalf("GetWorkspace: %v", err)
		}
		if fetched.ID != ws.ID {
			t.Fatalf("getWorkspace id mismatch")
		}

		lease, err := client.Renew(ws.ID, 300)
		if err != nil {
			t.Fatalf("Renew: %v", err)
		}
		if lease.ID != ws.ID {
			t.Fatalf("lease id mismatch")
		}
		if lease.Generation < 2 {
			t.Fatalf("renew must bump generation, got %d", lease.Generation)
		}
		if lease.ExpiresAtMs < ws.ExpiresAtMs {
			t.Fatalf("renew must not shorten lease")
		}

		if err := client.Release(ws.ID); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if containsWorkspace(t, client, ws.ID) {
			t.Fatalf("released workspace %s still in list", ws.ID)
		}
	})

	t.Run("commit_empty_mappings", func(t *testing.T) {
		ws, err := client.Fork(nil, 60)
		if err != nil {
			t.Fatal(err)
		}
		outcomes, err := client.Commit(ws.ID, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(outcomes) != 0 {
			t.Fatalf("empty commit must yield no outcomes, got %d", len(outcomes))
		}
		if err := client.Release(ws.ID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("get_unknown_workspace_raises", func(t *testing.T) {
		_, err := client.GetWorkspace("abababababababababababababababab")
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) {
			t.Fatalf("expected *RPCError, got %v", err)
		}
	})
}

func containsWorkspace(t *testing.T, client *Client, id string) bool {
	t.Helper()
	list, err := client.ListWorkspaces()
	if err != nil {
		t.Fatalf("ListWorkspaces: %v", err)
	}
	for _, w := range list {
		if w.ID == id {
			return true
		}
	}
	return false
}
