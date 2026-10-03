package mq

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// 崩溃恢复测试：用子进程在真实持久化路径上的故障点被 SIGKILL 强杀，
// 父进程重新打开目录，核对恢复结果，并验证恢复后可以继续使用。
//
// 覆盖的边界：
//   - Publish 半写记录（半写头部/半写正文）：整批消失，不遮蔽后续提交
//   - Publish 写完未 fsync：整批生效或整体消失，不允许出现半批
//   - Publish fsync 后未返回：必须整体生效
//   - Ack 半写记录 / 写完未 fsync：整批生效或整体消失
//   - Compact 新文件写完未 rename：旧日志完整可用，残留新文件被清理
//   - Compact rename 后未 fsync 目录：新旧视图均为一致状态

// TestHelperProcess 是子进程入口；仅在 MQ_HELPER=1 时执行实际操作。
func TestHelperProcess(t *testing.T) {
	if os.Getenv("MQ_HELPER") != "1" {
		return
	}
	dir := os.Getenv("MQ_DIR")
	q, err := Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	switch os.Getenv("MQ_SCENARIO") {
	case "publish":
		if _, err := q.Publish([][]byte{
			[]byte("crash-B-0"), []byte("crash-B-1"), []byte("crash-B-2"),
		}); err != nil {
			fmt.Fprintln(os.Stderr, "publish:", err)
			os.Exit(1)
		}
	case "ack":
		if err := q.Ack(0, []uint64{2, 4, 6}); err != nil {
			fmt.Fprintln(os.Stderr, "ack:", err)
			os.Exit(1)
		}
	case "compact":
		if err := q.Compact(); err != nil {
			fmt.Fprintln(os.Stderr, "compact:", err)
			os.Exit(1)
		}
	}
	// 未命中故障点时正常退出（对照组）。
	os.Exit(0)
}

// runHelper 启动子进程执行指定场景；faultPoint 为空表示对照组。
func runHelper(t *testing.T, dir, scenario, faultPoint string, faultBytes int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess")
	cmd.Env = append(os.Environ(),
		"MQ_HELPER=1",
		"MQ_DIR="+dir,
		"MQ_SCENARIO="+scenario,
	)
	if faultPoint != "" {
		cmd.Env = append(cmd.Env,
			envFaultPoint+"="+faultPoint,
			envFaultBytes+"="+fmt.Sprint(faultBytes),
		)
	}
	out, err := cmd.CombinedOutput()
	if faultPoint == "" {
		if err != nil {
			t.Fatalf("helper (no fault) failed: %v\n%s", err, out)
		}
		return
	}
	if err == nil {
		t.Fatalf("helper with fault %q exited cleanly, want crash", faultPoint)
	}
	t.Logf("helper crashed as expected at %q: %v", faultPoint, err)
}

// setupBacklog 预置 6 条消息（ID 1..6，正文 "msg-N"）。
func setupBacklog(t *testing.T, dir string) {
	t.Helper()
	q, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	publishN(t, q, 6)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
}

// crashB 是子进程 crash-B 批次的期望消息。
func crashB(start uint64) []Message {
	return []Message{
		{ID: start, Body: []byte("crash-B-0")},
		{ID: start + 1, Body: []byte("crash-B-1")},
		{ID: start + 2, Body: []byte("crash-B-2")},
	}
}

// reopen 重新打开目录。
func reopen(t *testing.T, dir string) *Queue {
	t.Helper()
	q, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return q
}

// 半写 Publish：整批不得出现；后续提交不被残留尾部遮蔽。
func TestCrashPublishPartial(t *testing.T) {
	dir := t.TempDir()
	setupBacklog(t, dir)

	// 记录总长 = 12(头)+12(批次头)+3*(4+9)，写 20 字节必然截断在 payload 中。
	runHelper(t, dir, "publish", "publish-partial", 20)

	q := reopen(t, dir)
	// 半写批次整体消失，旧数据完好。
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q, g, msgs(1, 6))
	}
	// 恢复截断尾部后，后续发布正常追加；ID 从 7 继续。
	ids := publishN(t, q, 2)
	if ids[0] != 7 {
		t.Fatalf("id after recovery = %d, want 7", ids[0])
	}
	q.Close()

	// 再次重启：新提交仍然可读，未被残留尾部遮蔽。
	q2 := reopen(t, dir)
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q2, g, msgs(1, 8))
	}
	q2.Close()
}

// 半写在记录头部（0 字节与 5 字节）：同样整体消失。
func TestCrashPublishPartialHeader(t *testing.T) {
	for _, n := range []int{0, 5} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			dir := t.TempDir()
			setupBacklog(t, dir)
			runHelper(t, dir, "publish", "publish-partial", n)
			q := reopen(t, dir)
			defer q.Close()
			for g := 0; g < NumGroups; g++ {
				wantRead(t, q, g, msgs(1, 6))
			}
		})
	}
}

// Publish 写完未 fsync：整批生效或整体消失，不允许半批。
func TestCrashPublishWritten(t *testing.T) {
	dir := t.TempDir()
	setupBacklog(t, dir)
	runHelper(t, dir, "publish", "publish-written", 0)

	q := reopen(t, dir)
	got := readAll(t, q, 0)
	prefix := msgs(1, 6)
	full := append(append([]Message{}, prefix...), crashB(7)...)
	if len(got) != 6 && len(got) != 9 {
		t.Fatalf("got %d messages, want 6 (lost) or 9 (kept)", len(got))
	}
	want := prefix
	if len(got) == 9 {
		want = full
	}
	for i, m := range want {
		if got[i].ID != m.ID || !bytes.Equal(got[i].Body, m.Body) {
			t.Fatalf("msg %d: got %+v, want %+v", i, got[i], m)
		}
	}
	// 无论哪种结果，继续发布与重开都必须一致。
	n := uint64(len(got))
	ids := publishN(t, q, 1)
	if ids[0] != n+1 {
		t.Fatalf("next id = %d, want %d", ids[0], n+1)
	}
	q.Close()
	q2 := reopen(t, dir)
	defer q2.Close()
	if got2 := readAll(t, q2, 0); uint64(len(got2)) != n+1 {
		t.Fatalf("after reopen got %d, want %d", len(got2), n+1)
	}
}

// Publish fsync 后、返回前强杀：批次必须整体生效。
func TestCrashPublishSynced(t *testing.T) {
	dir := t.TempDir()
	setupBacklog(t, dir)
	runHelper(t, dir, "publish", "publish-synced", 0)

	q := reopen(t, dir)
	defer q.Close()
	want := append(msgs(1, 6), crashB(7)...)
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q, g, want)
	}
}

// 半写 Ack：整批不得生效。
func TestCrashAckPartial(t *testing.T) {
	dir := t.TempDir()
	setupBacklog(t, dir)
	// Ack 记录 = 12(头)+8+3*8，写 10 字节截断在头部。
	runHelper(t, dir, "ack", "ack-partial", 10)

	q := reopen(t, dir)
	// 确认整体未生效：组 0 仍看到全部。
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q, g, msgs(1, 6))
	}
	// 恢复后重新确认，重开核对。
	if err := q.Ack(0, []uint64{2, 4, 6}); err != nil {
		t.Fatal(err)
	}
	q.Close()
	q2 := reopen(t, dir)
	defer q2.Close()
	wantRead(t, q2, 0, append(append(msgs(1, 1), msgs(3, 3)...), msgs(5, 5)...))
	wantRead(t, q2, 1, msgs(1, 6))
}

// Ack 写完未 fsync：整批生效或整体消失。
func TestCrashAckWritten(t *testing.T) {
	dir := t.TempDir()
	setupBacklog(t, dir)
	runHelper(t, dir, "ack", "ack-written", 0)

	q := reopen(t, dir)
	defer q.Close()
	got := readAll(t, q, 0)
	if len(got) != 6 && len(got) != 3 {
		t.Fatalf("group 0 has %d messages, want 6 (ack lost) or 3 (ack kept)", len(got))
	}
	if len(got) == 3 {
		wantRead(t, q, 0, append(append(msgs(1, 1), msgs(3, 3)...), msgs(5, 5)...))
	}
	// 其他组不受影响。
	wantRead(t, q, 1, msgs(1, 6))
}

// Compact 在新文件写完、rename 之前强杀：旧日志完整可用，残留被清理。
func TestCrashCompactNewWritten(t *testing.T) {
	dir := t.TempDir()
	q, _ := Open(dir)
	publishN(t, q, 10)
	// 3..10 全部确认，1、2 部分存活。
	q.Ack(0, []uint64{5, 2, 9, 3, 10, 7, 4, 8, 6})
	q.Ack(1, []uint64{1, 7, 4, 10, 3, 6, 9, 5, 8})
	q.Ack(2, []uint64{8, 3, 6, 10, 4, 9, 5, 7})
	q.Close()

	runHelper(t, dir, "compact", "compact-new-written", 0)

	// 残留的新文件可能仍在磁盘上，Open 必须清理并以旧日志恢复。
	q2 := reopen(t, dir)
	wantRead(t, q2, 0, msgs(1, 1))
	wantRead(t, q2, 1, msgs(2, 2))
	wantRead(t, q2, 2, msgs(1, 2))
	if _, err := os.Stat(filepath.Join(dir, newName)); !os.IsNotExist(err) {
		t.Fatalf("stale %s not cleaned after reopen", newName)
	}
	// 恢复后继续：发布、确认、再压缩、重开。
	ids := publishN(t, q2, 1)
	if ids[0] != 11 {
		t.Fatalf("id = %d, want 11", ids[0])
	}
	q2.Ack(0, []uint64{1, 11})
	q2.Ack(1, []uint64{2, 11})
	q2.Ack(2, []uint64{1, 2, 11})
	if err := q2.Compact(); err != nil {
		t.Fatal(err)
	}
	q2.Close()
	q3 := reopen(t, dir)
	defer q3.Close()
	for g := 0; g < NumGroups; g++ {
		wantRead(t, q3, g, nil)
	}
	// ID 继续递增不复用。
	q4 := q3
	ids2 := publishN(t, q4, 1)
	if ids2[0] != 12 {
		t.Fatalf("id = %d, want 12", ids2[0])
	}
}

// Compact 在 rename 之后、目录 fsync 之前强杀：
// 无论 rename 是否持久化，新旧视图都必须一致且可读。
func TestCrashCompactRenamed(t *testing.T) {
	dir := t.TempDir()
	q, _ := Open(dir)
	publishN(t, q, 10)
	q.Ack(0, []uint64{5, 2, 9, 3, 10, 7, 4, 8, 6})
	q.Ack(1, []uint64{1, 7, 4, 10, 3, 6, 9, 5, 8})
	q.Ack(2, []uint64{8, 3, 6, 10, 4, 9, 5, 7})
	q.Close()

	runHelper(t, dir, "compact", "compact-renamed", 0)

	q2 := reopen(t, dir)
	// 压缩不改变 Read 的逻辑结果。
	wantRead(t, q2, 0, msgs(1, 1))
	wantRead(t, q2, 1, msgs(2, 2))
	wantRead(t, q2, 2, msgs(1, 2))
	// 继续发布、确认，重开后仍正确。
	ids := publishN(t, q2, 2) // 11, 12
	if ids[0] != 11 {
		t.Fatalf("id = %d, want 11", ids[0])
	}
	q2.Ack(2, []uint64{1, 2})
	q2.Close()
	q3 := reopen(t, dir)
	defer q3.Close()
	wantRead(t, q3, 0, append(msgs(1, 1), msgs(11, 12)...))
	wantRead(t, q3, 2, msgs(11, 12))
}

// 对照组：无故障点时子进程正常完成，操作生效。
func TestHelperControl(t *testing.T) {
	dir := t.TempDir()
	setupBacklog(t, dir)
	runHelper(t, dir, "publish", "", 0)
	runHelper(t, dir, "ack", "", 0)
	runHelper(t, dir, "compact", "", 0)

	q := reopen(t, dir)
	defer q.Close()
	want := append(append(append(msgs(1, 1), msgs(3, 3)...), msgs(5, 5)...), crashB(7)...)
	wantRead(t, q, 0, want)
}
