//go:build windows

package integration

import (
	"os/exec"
	"syscall"
)

func hideProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x08000000}
}
