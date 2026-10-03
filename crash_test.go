package fifoq

// Crash-recovery tests. A helper subprocess (this same test binary)
// performs real persistence operations and is killed with SIGKILL at
// controlled fault injection points along the actual write path:
//   - mid-write of a Publish record (torn batch tail)
//   - mid-write of an Ack record
//   - Compact after snap.tmp is fsynced, before rename
//   - Compact after atomic rename, before log truncation
//   - Compact after log truncation, before the new header
// After each kill the parent reopens the directory, verifies the
// queue invariants, then publishes/acks more and reopens again to
// prove the recovered queue keeps working and torn tails do not
// shadow later commits.

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// TestCrashHelper runs only inside the spawned subprocess.
func TestCrashHelper(t *testing.T) {
	if os.Getenv("FIFOQ_CRASH_HELPER") != "1" {
		return
	}
	dir := os.Getenv("FIFOQ_DIR")
	scenario := os.Getenv("FIFOQ_SCENARIO")

	kill := func(string) {
		syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
	}
	debugCrashHook = kill

	q, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper open:", err)
		os.Exit(2)
	}
	switch scenario {
	case "publish-midwrite":
		debugCrashPoints = map[string]bool{"midwrite": true}
		debugSplitWrite = 20 // tear inside the record
		q.Publish([][]byte{body("c1"), body("c2"), body("c3")})
	case "ack-midwrite":
		debugCrashPoints = map[string]bool{"midwrite": true}
		debugSplitWrite = 15
		q.Ack(0, []uint64{1, 2, 3, 4, 5})
	case "compact-tmp":
		debugCrashPoints = map[string]bool{"compact.tmp": true}
		q.Compact()
	case "compact-renamed":
		debugCrashPoints = map[string]bool{"compact.renamed": true}
		q.Compact()
	case "compact-truncated":
		debugCrashPoints = map[string]bool{"compact.truncated": true}
		q.Compact()
	default:
		fmt.Fprintln(os.Stderr, "unknown scenario", scenario)
		os.Exit(2)
	}
	// If we get here the crash point did not fire.
	os.Exit(0)
}

func runCrashed(t *testing.T, dir, scenario string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCrashHelper", "-test.v")
	cmd.Env = append(os.Environ(),
		"FIFOQ_CRASH_HELPER=1",
		"FIFOQ_DIR="+dir,
		"FIFOQ_SCENARIO="+scenario,
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("scenario %s: helper did not crash:\n%s", scenario, out)
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			if status.Signaled() && status.Signal() == syscall.SIGKILL {
				return
			}
		}
	}
	t.Fatalf("scenario %s: unexpected helper exit: %v\n%s", scenario, err, out)
}

// expectReads verifies exact per-group read results (bodies) and that
// each message "mK" still carries its original ID K.
func expectReads(t *testing.T, q *Queue, want map[int][]string) {
	t.Helper()
	for g := 0; g < NumGroups; g++ {
		msgs, err := q.Read(g, 1<<30)
		if err != nil {
			t.Fatalf("Read(%d): %v", g, err)
		}
		var got []string
		for _, m := range msgs {
			got = append(got, string(m.Body))
			var wantID uint64
			if _, err := fmt.Sscanf(string(m.Body), "m%d", &wantID); err == nil {
				if m.ID != wantID {
					t.Fatalf("group %d: body %q has ID %d, want %d", g, m.Body, m.ID, wantID)
				}
			}
		}
		if !equalStrings(got, want[g]) {
			t.Fatalf("group %d reads = %v, want %v", g, got, want[g])
		}
	}
}

func openDir(t *testing.T, dir string) *Queue {
	t.Helper()
	q, err := Open(dir)
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	return q
}

// setupCompactState publishes m1..m6 and acks so that:
// group0: nothing pending, group1: m4..m6, group2: m1..m6.
func setupCompactState(t *testing.T, dir string) {
	t.Helper()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ids := publishStrings(t, q, "m1", "m2", "m3", "m4", "m5", "m6")
	if err := q.Ack(0, ids); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(1, ids[:3]); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

func wantCompactState() map[int][]string {
	return map[int][]string{
		0: {},
		1: {"m4", "m5", "m6"},
		2: {"m1", "m2", "m3", "m4", "m5", "m6"},
	}
}

// continueAfterRecovery verifies the queue keeps working after
// recovery: publish m7, ack m1 for group 2, reopen, re-verify.
func continueAfterRecovery(t *testing.T, q *Queue, dir string, base map[int][]string) {
	t.Helper()
	publishStrings(t, q, "m7")
	if err := q.Ack(2, []uint64{1}); err != nil {
		t.Fatal(err)
	}
	q = reopen(t, q, dir)
	want := map[int][]string{}
	for g, msgs := range base {
		want[g] = append(append([]string{}, msgs...), "m7")
	}
	// group 2 additionally acknowledged m1.
	filtered := want[2][:0]
	for _, s := range want[2] {
		if s != "m1" {
			filtered = append(filtered, s)
		}
	}
	want[2] = filtered
	expectReads(t, q, want)
	q.Close()
}

func TestCrashPublishMidwrite(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	publishStrings(t, q, "m1", "m2")
	q.Close()

	runCrashed(t, dir, "publish-midwrite")

	q = openDir(t, dir)
	msgs, err := q.Read(0, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	// The crashed batch is all-or-nothing: either 2 or 5 messages.
	var base map[int][]string
	switch len(msgs) {
	case 2:
		base = map[int][]string{0: {"m1", "m2"}, 1: {"m1", "m2"}, 2: {"m1", "m2"}}
	case 5:
		base = map[int][]string{
			0: {"m1", "m2", "c1", "c2", "c3"},
			1: {"m1", "m2", "c1", "c2", "c3"},
			2: {"m1", "m2", "c1", "c2", "c3"},
		}
	default:
		t.Fatalf("partial batch recovered: %d messages", len(msgs))
	}
	expectReads(t, q, base)
	// New publishes after recovery must survive another reopen and not
	// be shadowed by the torn tail.
	publishStrings(t, q, "m3")
	q = reopen(t, q, dir)
	for g := range base {
		base[g] = append(base[g], "m3")
	}
	expectReads(t, q, base)
	q.Close()
}

func TestCrashAckMidwrite(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	publishStrings(t, q, "m1", "m2", "m3", "m4", "m5")
	q.Close()

	runCrashed(t, dir, "ack-midwrite")

	q = openDir(t, dir)
	msgs, _ := q.Read(0, 1<<30)
	// Ack batch all-or-nothing for group 0.
	if len(msgs) != 0 && len(msgs) != 5 {
		t.Fatalf("partial ack batch recovered: %d pending", len(msgs))
	}
	// Other groups never saw the ack.
	for _, g := range []int{1, 2} {
		if got := readStrings(t, q, g, 100); !equalStrings(got, []string{"m1", "m2", "m3", "m4", "m5"}) {
			t.Fatalf("group %d = %v", g, got)
		}
	}
	// Re-ack cleanly, publish more, reopen and verify.
	if err := q.Ack(0, []uint64{1, 2, 3, 4, 5}); err != nil {
		t.Fatal(err)
	}
	publishStrings(t, q, "m6")
	q = reopen(t, q, dir)
	expectReads(t, q, map[int][]string{
		0: {"m6"},
		1: {"m1", "m2", "m3", "m4", "m5", "m6"},
		2: {"m1", "m2", "m3", "m4", "m5", "m6"},
	})
	q.Close()
}

func TestCrashCompact(t *testing.T) {
	for _, scenario := range []string{"compact-tmp", "compact-renamed", "compact-truncated"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			setupCompactState(t, dir)

			runCrashed(t, dir, scenario)

			q := openDir(t, dir)
			expectReads(t, q, wantCompactState())
			continueAfterRecovery(t, q, dir, wantCompactState())
		})
	}
}

// TestCrashCompactThenCleanCompact: after recovering from an
// interrupted compact, a clean Compact must still work and preserve
// the size bound and all data.
func TestCrashCompactThenCleanCompact(t *testing.T) {
	dir := t.TempDir()
	setupCompactState(t, dir)
	runCrashed(t, dir, "compact-renamed")

	q := openDir(t, dir)
	expectReads(t, q, wantCompactState())
	if err := q.Compact(); err != nil {
		t.Fatal(err)
	}
	q = reopen(t, q, dir)
	expectReads(t, q, wantCompactState())
	q.Close()
}
