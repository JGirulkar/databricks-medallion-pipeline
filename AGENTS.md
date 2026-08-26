# Databricks Medallion Pipeline — Agent Survival Kit

E-commerce sales medallion pipeline (Bronze → Silver → Gold → Dashboard) for DE C1 assessment.

## Isolation

- **Profile:** `de-assessment-ce` only — `source scripts/env.sh`
- **GitHub:** **`JGirulkar` only** — push, PRs, repo access via `github-assessment` skill or GitHub MCP; never use other accounts or remotes from this workspace
- **Assessment-only** — never use non-assessment profiles or edit other org repos from this workspace

## Layout

```
databricks/jobs/{data_generation,bronze,silver,gold}/  # pipeline code
databricks/bundle/                                    # Asset bundle (CE deploy)
cursor-workflow/                                      # spec, task breakdown
ai-prompts/                                           # prompt history (CRITICAL)
```

## Skills (load on demand)

| Task | Skill |
|------|-------|
| Layer sign-off | `layer-completion` |
| PR body | `pr-description` |
| Local tests | `medallion-pipeline-local-test` |
| CE deploy | `deploy-ce-job` |
| Bronze CE E2E | `bronze-e2e-ce` |
| Docs / prompts | `assessment-artifacts` |
| Medallion patterns | `conventions-medallion` |

## Rules (always apply)

- `explore-before-change.mdc`
- `project-overview.mdc`

## Superpowers (selective)

Full plugin installed — use for planning/TDD/debug only. Implementation gates = project skills above.

## Test tiers

`unit` → `spark` → `cluster` (CE). Run: `./databricks/scripts/run_job_tests.sh`
