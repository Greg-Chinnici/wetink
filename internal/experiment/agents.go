package experiment

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Agent launches one coding agent and waits for it.
type Agent interface {
	Name() string
	Run(ctx context.Context, r AgentRun) error
}

// AgentRun is one agent's job in one run.
type AgentRun struct {
	Dir    string // its checkout (the working directory)
	Home   string // its own config directory, outside the checkout
	Cache  string // writable cache directory, outside the checkout
	Prompt string
	Model  string
	// ProxyURL is the agent's model endpoint: http://host:port/s/<session>/<master>.
	ProxyURL string
	// MCPURL is the shared-context server with the agent's identity in the query; "" for none.
	MCPURL string
	Env    []string // the spec's env; added to a scrubbed copy of this process's environment
	Stdout io.Writer
	Stderr io.Writer
}

// Agents are the agents wetink experiment can run.
var Agents = map[string]Agent{"claude": Claude{}, "codex": Codex{}}

// scrubbed is this process's environment without what agents must not see or inherit: wetink's
// and the providers' keys, and anything that would point them elsewhere.
func scrubbed(keep ...string) []string {
	var out []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		upper := strings.ToUpper(name)
		secret := strings.Contains(upper, "KEY") || strings.Contains(upper, "TOKEN") || strings.Contains(upper, "SECRET") ||
			strings.Contains(upper, "PASSWORD") || strings.HasPrefix(upper, "WETINK_") || strings.HasPrefix(upper, "OPENAI_") ||
			strings.HasPrefix(upper, "ANTHROPIC_") || strings.HasPrefix(upper, "CLAUDE") || strings.HasPrefix(upper, "CODEX") ||
			upper == "DATABASE_URL" || upper == "NATS_URL"
		for _, k := range keep {
			if name == k {
				secret = false
			}
		}
		if !secret {
			out = append(out, kv)
		}
	}
	return out
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

// Claude runs Claude Code headless (claude -p), with Bash in Claude Code's sandbox (writes only
// to the checkout and the cache, no network) and no web tools or sub-agents.
//
// With CLAUDE_CODE_OAUTH_TOKEN (from `claude setup-token`) or ANTHROPIC_API_KEY set, each
// agent gets a fresh config directory, so nothing of the user's setup reaches it. Without, it
// uses the logged-in user's config but skips the user's settings and MCP servers.
type Claude struct{}

func (Claude) Name() string { return "claude" }

// ClaudeTools are the tools Claude agents may use (plus the board's, when there is one).
var ClaudeTools = []string{"Bash", "Read", "Edit", "Write", "Glob", "Grep", "TodoWrite"}

func (Claude) Run(ctx context.Context, r AgentRun) error {
	allow := append([]string(nil), ClaudeTools...)
	mcp := map[string]any{"mcpServers": map[string]any{}}
	if r.MCPURL != "" {
		allow = append(allow, "mcp__wetink")
		mcp["mcpServers"] = map[string]any{"wetink": map[string]any{"type": "http", "url": r.MCPURL}}
	}
	settings := map[string]any{
		"sandbox": map[string]any{
			"enabled": true, "failIfUnavailable": true, "autoAllowBashIfSandboxed": true,
			"allowUnsandboxedCommands": false,
			"filesystem":               map[string]any{"allowWrite": []string{r.Cache}},
			"network":                  map[string]any{"allowedDomains": []string{}},
		},
		"permissions": map[string]any{"allow": allow, "deny": []string{"WebFetch", "WebSearch", "Agent", "Task"}},
	}
	mcpPath, settingsPath := filepath.Join(r.Home, "mcp.json"), filepath.Join(r.Home, "settings.json")
	if err := writeJSON(mcpPath, mcp); err != nil {
		return err
	}
	if err := writeJSON(settingsPath, settings); err != nil {
		return err
	}
	// The run keeps the transcript (stream-json); none goes to the user's ~/.claude/projects.
	args := []string{"-p", r.Prompt, "--output-format", "stream-json", "--verbose", "--no-session-persistence",
		"--permission-mode", "dontAsk", "--allowedTools", strings.Join(allow, ","),
		"--mcp-config", mcpPath, "--strict-mcp-config", "--settings", settingsPath, "--add-dir", r.Cache}
	if r.Model != "" {
		args = append(args, "--model", r.Model)
	}
	env := scrubbed("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY")
	if os.Getenv("CLAUDE_CODE_OAUTH_TOKEN") != "" || os.Getenv("ANTHROPIC_API_KEY") != "" {
		env = append(env, "CLAUDE_CONFIG_DIR="+filepath.Join(r.Home, "config"))
	} else {
		args = append(args, "--setting-sources", "project,local")
	}
	env = append(env, r.Env...)
	env = append(env, "ANTHROPIC_BASE_URL="+r.ProxyURL, "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1", "DISABLE_AUTOUPDATER=1")
	cmd := command(ctx, r.Dir, env, "claude", args...)
	cmd.Stdout, cmd.Stderr = r.Stdout, r.Stderr
	return cmd.Run()
}

// Codex runs Codex CLI (codex exec) against the proxy's Responses endpoint, with its
// workspace-write sandbox. The proxy holds the provider key.
type Codex struct{}

func (Codex) Name() string { return "codex" }

func (Codex) Run(ctx context.Context, r AgentRun) error {
	toml := fmt.Sprintf(`model = %q
model_provider = "wetink"
approval_policy = "never"
sandbox_mode = "workspace-write"
web_search = "disabled"

[sandbox_workspace_write]
writable_roots = [%q]
network_access = false

[model_providers.wetink]
name = "wetink proxy"
base_url = %q
wire_api = "responses"
`, r.Model, r.Cache, r.ProxyURL+"/v1")
	if r.MCPURL != "" {
		toml += fmt.Sprintf("\n[mcp_servers.wetink]\nurl = %q\n", r.MCPURL)
	}
	if err := os.WriteFile(filepath.Join(r.Home, "config.toml"), []byte(toml), 0o600); err != nil {
		return err
	}
	env := append(scrubbed(), r.Env...)
	env = append(env, "CODEX_HOME="+r.Home)
	cmd := command(ctx, r.Dir, env, "codex", "exec", "--json", "-C", r.Dir, r.Prompt)
	cmd.Stdout, cmd.Stderr = r.Stdout, r.Stderr
	return cmd.Run()
}

// mcpURL is the shared-context server's URL with an agent's identity.
func mcpURL(base, session, master string) string {
	return base + "?" + url.Values{"session": {session}, "master": {master}}.Encode()
}
