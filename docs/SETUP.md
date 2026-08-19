# Environment Setup — DE C1 Assessment (Path C Hybrid)

Isolated from Intelo. Profile: **`de-assessment-ce`** only. Repo: `~/Projects/databricks-medallion-pipeline/`.

## Phase A — Machine setup

### 1. Superpowers (full plugin — manual in Cursor)

In **Cursor Agent** chat (`Ctrl+L`):

```text
/add-plugin superpowers
```

Install the **whole** plugin (all skills). Verify:

```text
Do you have superpowers? List available skills.
```

**Selective use (Path C):**
- `brainstorming` — before architecture / each medallion layer
- `executing-plans` — multi-session builds from `cursor-workflow/task-breakdown.md`
- `test-driven-development` / `systematic-debugging` — when needed
- **Not** for every small edit — implementation gates use project `.cursor/rules/` + `layer-completion` skill

### 2. Optional: Context7

```text
/add-plugin context7
```

PySpark / Databricks CLI doc accuracy. Cap marketplace plugins at 2 (Superpowers + Context7).

### 3. CLI toolchain

| Tool | Install | Verify |
|------|---------|--------|
| Databricks CLI | `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh \| sh` | `databricks --version` |
| uv | `curl -LsSf https://astral.sh/uv/install.sh \| sh` | `uv --version` |
| JDK 21 | `sudo apt install openjdk-21-jdk` (Linux) | `java -version` |
| ruff | `uv tool install ruff` or `pip install ruff` | `ruff --version` |
| gh | https://cli.github.com | `gh auth status` |

### 4. Databricks CE profile (isolated)

```bash
databricks auth login \
  --host https://dbc-06f970f4-0f19.cloud.databricks.com \
  --profile de-assessment-ce

databricks auth profiles
```

**Never** use Intelo Azure SP / `DEFAULT` profiles for this project.

### 5. AI Dev Kit MCP (isolated clone)

Already at `~/.mcp/de-assessment-ai-dev-kit/`. Project wires it via `.cursor/mcp.json` with `DATABRICKS_CONFIG_PROFILE=de-assessment-ce`.

Rebuild if needed:

```bash
cd ~/.mcp/de-assessment-ai-dev-kit/databricks-mcp-server
uv venv .venv --python 3.11
uv pip install --python .venv/bin/python -e ../databricks-tools-core -e .
```

### 6. Assessment Python venv

```bash
source scripts/env.sh
cd databricks
uv sync --all-packages --all-groups --no-group cluster
```

Requires JDK 21 for local Spark tests.

### 7. Databricks AI Tools (official skills)

```bash
source scripts/env.sh
databricks aitools install
```

Run from repo root after CE auth is configured.

## Isolation checklist

- [ ] Workspace root = this repo (not Intelo)
- [ ] `export DATABRICKS_CONFIG_PROFILE=de-assessment-ce` (or `source scripts/env.sh`)
- [ ] MCP server name: `databricks-de-assessment` (not Intelo `databricks`)
- [ ] No edits to `~/Desktop/Projects/Intelo.ai/**`

## GitHub (Phase D)

```bash
gh auth login   # use ttn email
gh repo create databricks-medallion-pipeline --private --source=. --remote=origin
git push -u origin main
```
