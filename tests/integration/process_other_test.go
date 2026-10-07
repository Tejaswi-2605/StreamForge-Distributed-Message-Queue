//go:build !windows

package integration

import "os/exec"

func hideProcess(cmd *exec.Cmd) {}
