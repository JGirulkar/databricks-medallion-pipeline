---
name: pr-description
description: Generate PR with What, Why, Alternatives, Test cases, Acceptance criteria. Use when opening PRs.
---

# PR Description

Adapted from senior 94/100 pr-description skill.

## Required sections

1. **What** — concrete changes
2. **Why** — requirement / layer goal
3. **Alternatives considered** — honest trade-offs
4. **Test cases** — commands + checkboxes
5. **Acceptance criteria** — measurable merge conditions

## Workflow

```bash
git log main...HEAD --oneline
git diff main...HEAD --stat
./scripts/lint.sh
```

Use `gh pr create` with HEREDOC body per `.cursor/rules/pull-request-template.mdc`.

Link to `ai-prompts/` entry for AI usage section.
