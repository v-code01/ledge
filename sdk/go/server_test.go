package ledge

// Test harness: build the Rust `ledge` binary once, then spawn it on an
// ephemeral port over a tmp data dir, poll `/healthz` until ready, and hand back
// a base URL + a stop handle. Mirrors sdk/ts/test/server.ts.

import (
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// repoRoot resolves the repository root from this package directory (sdk/go).
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// sdk/go -> repo root is two levels up.
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

// buildServer builds the `ledge` binary once (cargo is incremental).
func buildServer(t *testing.T) string {
	t.Helper()
	root := repoRoot(t)
	cmd := exec.Command("cargo", "build", "--bin", "ledge")
	cmd.Dir = root
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("cargo build --bin ledge failed: %v", err)
	}
	return filepath.Join(root, "target", "debug", "ledge")
}

// runningServer is a spawned server: its base URL and a stop func.
type runningServer struct {
	baseURL string
	stop    func()
}

func waitForHealth(baseURL string, deadline time.Time) bool {
	for time.Now().Before(deadline) {
		resp, err := http.Get(baseURL + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// startServer spawns the prebuilt server on a free port over a fresh tmp dir and
// waits for `/healthz`. Retries a few times on a port/startup race.
func startServer(t *testing.T, binPath string) *runningServer {
	t.Helper()
	for i := 0; i < 8; i++ {
		port := 20000 + rand.Intn(40000)
		dataDir, err := os.MkdirTemp("", "ledge-sdk-go-")
		if err != nil {
			t.Fatalf("mkdtemp: %v", err)
		}
		addr := "127.0.0.1:" + itoa(port)
		baseURL := "http://" + addr

		cmd := exec.Command(binPath, "start", "--addr", addr, "--data-dir", dataDir)
		cmd.Env = append(os.Environ(), "RUST_LOG=warn")
		if err := cmd.Start(); err != nil {
			os.RemoveAll(dataDir)
			continue
		}

		if waitForHealth(baseURL, time.Now().Add(10*time.Second)) {
			stop := func() {
				_ = cmd.Process.Kill()
				_, _ = cmd.Process.Wait()
				os.RemoveAll(dataDir)
			}
			return &runningServer{baseURL: baseURL, stop: stop}
		}

		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		os.RemoveAll(dataDir)
	}
	t.Fatal("server failed to start after retries")
	return nil
}

// itoa is a tiny dependency-free int->string for the port (avoids strconv churn).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
