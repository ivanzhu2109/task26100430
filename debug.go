package fifoq

// Test-only crash injection hooks. In production builds these are
// inert (zero values). Tests configure them, including from helper
// subprocesses, to exercise crash recovery along the real write path.

// debugSplitWrite, when > 0, makes the next appendRecord persist only
// that many bytes before invoking debugCrash("midwrite").
var debugSplitWrite int

// debugCrashHook is invoked by debugCrash at named persistence points.
// The crash-test subprocess sets it to kill itself with SIGKILL.
var debugCrashHook func(point string)

// debugCrashPoints, when non-nil, enables crash points by name; each
// enabled point triggers debugCrashHook exactly once when reached.
var debugCrashPoints map[string]bool

func debugCrash(point string) {
	if debugCrashPoints == nil || !debugCrashPoints[point] {
		return
	}
	delete(debugCrashPoints, point)
	if debugCrashHook != nil {
		debugCrashHook(point)
	}
}
