package experiment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// gitEnv makes commits reproducible and independent of the user's git config.
var gitEnv = []string{
	"GIT_AUTHOR_NAME=wetink", "GIT_AUTHOR_EMAIL=wetink@localhost",
	"GIT_COMMITTER_NAME=wetink", "GIT_COMMITTER_EMAIL=wetink@localhost",
	"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0",
}

func git(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), gitEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return strings.TrimSpace(string(out)), nil
}

// command is a process run in its own process group, killed whole on timeout or cancel.
func command(ctx context.Context, dir string, env []string, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

// shell runs a shell command with a timeout; it returns the exit code and the output's tail.
func shell(ctx context.Context, dir string, env []string, timeout time.Duration, script string) (int, string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var out tail
	cmd := command(ctx, dir, env, "sh", "-c", script)
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, out.String(), nil
	case ctx.Err() == context.DeadlineExceeded:
		return -1, out.String() + "\n[timed out after " + timeout.String() + "]", nil
	case errors.As(err, &exit):
		return exit.ExitCode(), out.String(), nil
	}
	return -1, out.String(), err
}

// tail keeps the last tailBytes written to it.
type tail struct{ b []byte }

const tailBytes = 4000

func (t *tail) Write(p []byte) (int, error) {
	t.b = append(t.b, p...)
	if len(t.b) > 2*tailBytes {
		t.b = append([]byte(nil), t.b[len(t.b)-tailBytes:]...)
	}
	return len(p), nil
}

func (t *tail) String() string {
	if len(t.b) > tailBytes {
		return "..." + string(t.b[len(t.b)-tailBytes:])
	}
	return string(t.b)
}

// baseRepo makes (once) a bare repository at path with only the history up to the spec's
// base commit, on branch "base", and returns that commit.
func baseRepo(ctx context.Context, s *Spec, path string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(path); err == nil {
		return git(ctx, path, "rev-parse", "base")
	}
	tmp := path + ".src"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	src := s.Repo
	if local := s.Path(src); isDir(local) {
		src = local
		if !isDir(filepath.Join(local, ".git")) && !isBare(local) {
			// A plain directory: commit it as it is.
			if err := copyDir(local, tmp); err != nil {
				return "", err
			}
			if _, err := git(ctx, tmp, "init", "-q"); err != nil {
				return "", err
			}
			if _, err := git(ctx, tmp, "add", "-A"); err != nil {
				return "", err
			}
			if _, err := git(ctx, tmp, "commit", "-qm", "base"); err != nil {
				return "", err
			}
			src = tmp
		}
	}
	if src != tmp {
		if _, err := git(ctx, "", "clone", "-q", "--bare", src, tmp); err != nil {
			return "", err
		}
	}
	commit, err := git(ctx, tmp, "rev-parse", s.Base+"^{commit}")
	if err != nil {
		return "", err
	}
	if _, err := git(ctx, tmp, "update-ref", "refs/wetink/base", commit); err != nil {
		return "", err
	}
	if _, err := git(ctx, "", "init", "-q", "--bare", path+".tmp"); err != nil {
		return "", err
	}
	if _, err := git(ctx, path+".tmp", "fetch", "-q", tmp, "refs/wetink/base:refs/heads/base"); err != nil {
		os.RemoveAll(path + ".tmp")
		return "", err
	}
	if _, err := git(ctx, path+".tmp", "symbolic-ref", "HEAD", "refs/heads/base"); err != nil {
		return "", err
	}
	return commit, os.Rename(path+".tmp", path)
}

// checkout clones the base repository into dir, on branch "work", with no remote.
func checkout(ctx context.Context, base, dir string) error {
	if _, err := git(ctx, "", "clone", "-q", "--branch", "base", base, dir); err != nil {
		return err
	}
	if _, err := git(ctx, dir, "remote", "remove", "origin"); err != nil {
		return err
	}
	_, err := git(ctx, dir, "checkout", "-q", "-b", "work")
	return err
}

// commitAll commits whatever the agent left in the working tree; false if there was nothing.
func commitAll(ctx context.Context, dir, msg string) (bool, error) {
	if _, err := git(ctx, dir, "add", "-A"); err != nil {
		return false, err
	}
	if out, _ := git(ctx, dir, "status", "--porcelain"); out == "" {
		return false, nil
	}
	_, err := git(ctx, dir, "commit", "-qm", msg)
	return err == nil, err
}

// lay applies src to the checkout at dir: a patch (git apply), or a directory whose files are
// copied over.
func lay(ctx context.Context, src, dir string) error {
	if isDir(src) {
		return copyDir(src, dir)
	}
	_, err := git(ctx, dir, "apply", "--whitespace=nowarn", src)
	return err
}

func isDir(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

func isBare(p string) bool {
	_, err := os.Stat(filepath.Join(p, "HEAD"))
	return err == nil && isDir(filepath.Join(p, "objects"))
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		fi, _ := d.Info()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, fi.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
}
