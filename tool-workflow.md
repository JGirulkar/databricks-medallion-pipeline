# Tool Workflow — DE C1 Assessment

**Primary AI tool:** agentic coding assistant (terminal-integrated), extended
with project-defined skills, rules, hooks and MCP servers
**Approach:** Path C Hybrid — a general planning/TDD/debugging discipline
(Superpowers plugin) combined with project-specific gates
**Profile:** `de-assessment-ce` (Databricks Community Edition only)

## The harness, not just the model

The assistant is deliberately wrapped in project infrastructure so that
quality does not depend on remembering to ask for it:

| Layer | What I built | Role |
|---|---|---|
| **Skills** (9, project-authored) | `layer-completion`, `deploy-ce-job`, `bronze-e2e-ce`, `conventions-medallion`, `prompt-history-curation`, `assessment-artifacts`, `pr-description`, `github-assessment`, `medallion-pipeline-local-test` | encode my gates and recipes once; the assistant follows them instead of improvising |
| **Rules** (10 `.mdc`) | profile isolation, explore-before-change, medallion boundaries, testing standards, security | always-on constraints |
| **Hooks** (5 lifecycle points) | session start, prompt tracking, file-edit tracking, lint-before-git, session capture | automation that runs whether or not anyone remembers — raw prompt history is captured by the `stop` hook into `ai-prompts/capture/` |
| **MCP servers** (2, assessment-scoped) | Databricks (pinned to `de-assessment-ce`) and GitHub (pinned to the assessment account) | give the assistant hands on the real workspace and repo, with isolation enforced by configuration rather than discipline |
| **Plugins** | Superpowers (brainstorming, TDD, systematic debugging), Databricks plugin (CLI/product skills) | process and platform knowledge |

Two honest findings about the harness itself, discovered mid-project: the
lint hook matched only `git commit` *shell* commands, so commits made through
the GitHub MCP bypassed it entirely; and the one gate marked "optional" in
`layer-completion` (a real CE run) is exactly where a `NameError` reached the
cluster. Both are fixed — the deploy script now runs lint, tests and an import
check itself, and the CE run is mandatory. The lesson: automation you don't
test has holes exactly where you stopped looking.

## The working rhythm

- **Spec before code.** Each layer starts as a design conversation
  (brainstorming skill), becomes a dated spec, then an implementation plan,
  then commits per task. The silver design dialogue — config-driven
  validation, quarantine over delete, one VARIANT config column, rejecting a
  state table because streaming checkpoints already hold that cursor — is in
  [`ai-prompts/05-silver-quality.md`](ai-prompts/05-silver-quality.md).
- **Long gates run in the background.** A cluster E2E takes ~25 minutes; none
  of that time is spent waiting. Runs execute as background tasks while the
  foreground writes docs, prompt history, or the next test — and the gate was
  made safe to background: it emits its JSON report even when a step throws.
- **Division of labour.** I steer method and catch waste; the assistant
  executes, measures, and converts each catch into a test or guard so it holds
  permanently. Concrete catches: switching the verification method from
  25-minute cluster loops to a 1-minute local contract test ("this is too
  prolonged"); stopping an unnecessary quarantine DELETE by pointing at the
  lineage chain the table already carried; removing sequential job triggers
  whose ordering constraint the new design had deleted.
- **Human-in-the-loop on hard calls.** Destructive actions (history rewrite,
  any DELETE), design pivots (orphan-flag RI model), and trade-offs
  (retries off on serverless) are decided by me, with the assistant required
  to present cause, evidence and options first.

## How AI is used across the lifecycle (Part A checklist)

- **Requirement analysis** — the assessment PDF was extracted into
  `docs/ASSESSMENT_FROM_PDF.md` and is re-audited at every phase gate; the
  audit caught a real contradiction (gold: "all 4 aggregations" vs "three
  tables") and a stale reference schema.
- **Pipeline design** — design dialogues with explicit alternatives, each
  accepted/rejected with a reason (see prompt history 03, 05).
- **Code generation** — always against a spec; commits are atomic with
  red→green test pairs so iteration is visible in history.
- **Validating AI-generated code** — nothing is accepted on the assistant's
  word: ruff + 139 tests locally, a contract test whose expectations are
  recomputed independently from the input (so it cannot agree with a wrong
  implementation), and table-level SQL checks after every cluster run. When a
  test fails, the first question is which side is wrong — three contract
  failures were defects in the expectations, not the code.
- **Testing and validation** — test-first for every fix (the red commit
  precedes the green one); scenario coverage is enforced mechanically
  (`test_dq_coverage.py` fails if a declared rule has no violating data).
  Full matrix: [`test-strategy.md`](test-strategy.md).
- **Debugging** — systematic: reproduce cheaply, state a hypothesis, test the
  hypothesis before fixing it (one suspected cause was disproven by a six-line
  diagnostic before the real one was found), then guard the class. Full
  method and findings: [`debugging-notes.md`](debugging-notes.md).
- **Data quality checks** — declared as config (`dq_schema` VARIANT), enforced
  in silver, reported per check per run; the generator carries one violating
  scenario per declared rule. See
  [`data-quality-strategy.md`](data-quality-strategy.md).

## What I avoid sharing with AI tools

- Real customer PII — all data here is synthetically generated (seeded Faker)
- Production credentials of any kind; tokens never appear in prompts (profile
  and keyring auth only)
- Anything from other workspaces: the MCP servers are pinned to the
  assessment profile and account, so the isolation is structural

## Reuse in a real production pipeline

The reusable shape: config-driven validation read at runtime; append-only raw
layer with a rescue column; three-outcome quality handling (reject / flag /
supersede) so temporal failures are not treated as permanent; expectations
recomputed from input as the cheap verification tier; source-level guard tests
for platform restrictions no local run can reproduce; and gates encoded as
skills/hooks rather than habits.

## Lessons learned

- **What worked:** spec-first with visible accept/reject decisions; test-first
  fixes; asserting outcomes against the data rather than run status; the
  contract tier (every cluster-found defect was locally detectable first);
  background-running the long gates.
- **What didn't:** trusting green runs (three times a `success` hid wrong
  data); an "optional" step in a checklist (that word is where a bug reached
  the cluster); hand-maintained lists (a hash column list and a reference
  schema both drifted — both are now derived or guarded); my own first
  hypotheses (wrong more than once — cheap diagnostics before fixes).
- **The one-line summary:** the model writes code quickly; the value is in the
  harness around it — gates it cannot skip, tests it cannot flatter, and a
  human who challenges the expensive and the destructive.
