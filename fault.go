package mq

import (
	"os"
	"strconv"
	"syscall"
)

// 测试用故障注入：通过环境变量在真实持久化路径上模拟进程被强杀。
//
//	MQ_FAULT_POINT  故障点名称，见下
//	MQ_FAULT_BYTES  对 "*-partial" 故障点，指定写入多少字节后强杀
//
// 故障点：
//
//	publish-partial / ack-partial  记录写到一半强杀（产生半写尾部）
//	publish-written / ack-written  记录写完但未 fsync 时强杀
//	publish-synced / ack-synced    fsync 完成后、返回前强杀
//	compact-new-written            新快照文件写完并 fsync、rename 之前强杀
//	compact-renamed                rename 完成、目录 fsync 之前强杀
//
// 未设置 MQ_FAULT_POINT 时全部为 no-op，正常路径零开销。
const (
	envFaultPoint = "MQ_FAULT_POINT"
	envFaultBytes = "MQ_FAULT_BYTES"
)

func faultPoint() string { return os.Getenv(envFaultPoint) }

// faultPartialLen 命中 "<point>-partial" 时返回应写入的部分长度，否则返回 -1。
func faultPartialLen(point string, total int) int {
	if faultPoint() != point+"-partial" {
		return -1
	}
	n, _ := strconv.Atoi(os.Getenv(envFaultBytes))
	if n < 0 {
		n = 0
	}
	if n > total {
		n = total
	}
	return n
}

// faultCheck 命中指定故障点时立即强杀当前进程。
func faultCheck(point string) {
	if faultPoint() == point {
		faultKill()
	}
}

// faultKill 以 SIGKILL 终止当前进程：不运行 defer，不执行任何清理，
// 模拟进程被强制终止。
func faultKill() {
	syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
	select {} // 防御：信号未及时送达时挂起
}
