# Dashboard & Visualization — Prompt History

> **Companions:** [`06-gold-aggregations.md`](06-gold-aggregations.md) (the
> tables every tile reads) · [`DASHBOARD_GUIDE.md`](../databricks/dashboards/DASHBOARD_GUIDE.md)

How the SQL dashboard was directed, drafted, and iterated: the delivery
mechanism, tile and filter decisions, the deploy-time portability trick, and
the aesthetics pass.

---

## P1 — The delivery mechanism: files first, publish like a deploy

**Prompt:**
"Build the dashboard the same way we ship everything else: author it as
files we commit, then publish to the workspace with a script, the same shape
as the job deploys. Use the dashboard tooling for the authoring format.
First a working draft, then we iterate on it — don't try to perfect it in
one shot."

**Context provided:**
- The job deploy script's upsert-in-place pattern (stable ids, re-runnable)
- The dashboard serialization format and its lifecycle commands
  (create / update / publish)

**AI response:**
One committed JSON as the executed source of truth, a deploy script that
upserts by display name (create on first run, update + publish after — the
URL never changes), and the draft-then-iterate cadence: draft one shipped
and published the same session, refinements landed as updates against the
same dashboard id.

**Validation:**
First run created the dashboard and returned its id; the second run took the
update path and republished — both verified against the published-state API.

**Accepted:** Files as source of truth; upsert-by-name; draft-then-iterate.

**Rejected:** Authoring in the workspace UI and exporting afterwards — the
repo would hold a snapshot of the tool's output rather than a source the
deploy provably executes.

**Why:** A dashboard that only exists in a workspace is configuration; a
dashboard that deploys from a committed file is code.

---

## P2 — Tiles: the three required charts, plus context that speeds reading

**Prompt:**
"The three visualizations are fixed: top-10 products by revenue as a bar,
customer revenue distribution as a histogram, segmentation as a pie. Around
them I want the dashboard to answer 'how are we doing' in one glance — put
KPIs on top, and give the date filter a real home with a trend tile."

**Context provided:**
- The four gold tables and their grains
- Measured shapes: max customer revenue ~13K (sets the histogram bin),
  four segments, 502 products

**AI response:**
Three KPI counters (total revenue, qualifying orders, active customers)
above the required charts, a weekly revenue line as the fourth chart, and
the histogram binned at 1,000 (≈13 legible buckets against the ~13K max).
The bar went horizontal — product names read better on the y-axis. The pie
pinned semantic colors per segment (green for High-Value, gray for
Inactive) so the mix reads without consulting the legend.

**Validation:**
Every dataset query executed live before the first deploy; bin width and
KPI values cross-checked against direct SQL (revenue 25.33M, 33,152
qualifying orders, 9,618 active customers).

**Accepted:** The tile set; horizontal bar; pinned segment colors.

**Why:** The required charts answer their specific questions; the KPI row
and trend answer the question every viewer asks first.

---

## P3 — Filters, chosen by what each dataset can actually honor

**Prompt:**
"Filters only work on datasets that carry the field, so pick filters the
gold tables can honor honestly: category for the products chart, a date
range for the trend, declared segment for the customer views. Don't force a
country filter — gold doesn't carry country, and I'm not widening a table's
contract for one dropdown."

**Context provided:**
- The filter-scoping rule (a filter applies only to datasets containing the
  bound field)
- The gold column inventory per table

**AI response:**
A global filters page with exactly those three, each labeled with its scope
so the behaviour is stated rather than discovered: date range (trend +
sparkline KPIs), category (top-10 bar), declared segment (customer KPIs +
histogram). The pie deliberately stays outside the declared-segment filter —
its derived segments are a different taxonomy, and cross-filtering two
vocabularies invites wrong conclusions.

**Validation:**
Each filter's bound field verified present in its dataset's tested query.

**Accepted:** Three filters, scope-labeled titles.

**Rejected:** A country filter (would require widening
`revenue_by_customer` and its contract tests); driving the pie from the
declared-segment filter (taxonomy mismatch).

**Why:** A filter that silently ignores half the tiles is worse than a
filter that states its scope.

---

## P4 — The top-10 filter semantics, decided out loud

**Prompt:**
"If the category filter runs against a dataset that's already limited to
the overall top 10, it narrows those ten — it does not recompute a top 10
for the category. Decide which behaviour we ship and write it down; don't
leave it to be discovered."

**Context provided:**
- The dataset-level `LIMIT 10` and where filters execute relative to it

**AI response:**
Shipped filter-within-the-top-10 for the draft (matches the tile's literal
title), documented the choice and its alternative (moving the limit out of
the dataset to get per-category top-10s) in the guide as a known iteration.

**Accepted:** The simpler semantics, stated in the guide and the filter's
own title.

**Why:** Both behaviours are defensible; only an undocumented one is wrong.

---

## P5 — Portability survives a missing CLI feature

**Prompt:**
"Keep the committed JSON portable — bare table names, environment supplied
at deploy time, same philosophy as the gold SQL placeholders. If the
tooling on this machine can't inject the catalog and schema, the deploy
script does the rendering; the committed file doesn't get hardcoded paths."

**Context provided:**
- The installed CLI rejecting the dataset-catalog/schema flags the newer
  releases provide
- The gold job's render-at-deploy precedent

**AI response:**
The deploy script renders `<catalog>.gold.` onto each `FROM` into a temp
copy at upload time; the committed JSON stays bare. A structural test
enforces the contract from the other side: every `FROM` target must be a
bare, known gold table — so nobody can hardcode an environment into the
source file without a test failing.

**Validation:**
Deploy succeeded with the rendered copy; the guard test fails on any
qualified table name in the committed JSON.

**Accepted:** Render-at-deploy plus the bareness guard.

**Rejected:** Hardcoding `de_assessment.gold.` into the committed queries —
works today, breaks the first time the catalog differs.

**Why:** The environment belongs to the deployment, never to the source.

---

## P6 — The queries file is generated, and the spec is guard-tested

**Prompt:**
"The deliverable layout wants a dashboard_queries.sql — generate it from
the JSON instead of maintaining it by hand; we've already paid once for a
reference file that drifted. And give the dashboard spec the same treatment
as everything else here: tests for the mistakes the format is known to
invite."

**Context provided:**
- The earlier reference-DDL drift incident and its rule (derivation or a
  guard, never diligence)
- The spec format's documented footguns

**AI response:**
`dashboard_queries.sql` is produced by a small generator the deploy script
runs, with a sync test that fails if the export drifts from the JSON. Seven
structural unit tests cover the footguns: grid rows must fill all twelve
columns, every encoding field must exist in its widget's query, dataset
references must resolve, query lines must end with separators (they
concatenate verbatim), table names must stay bare, and every page needs its
grid metadata.

**Validation:**
7/7 green locally; the suite runs inside the existing gold test tier so the
repo-wide gate picks it up.

**Accepted:** Generation over maintenance; the structural tier.

**Why:** Every one of those checks encodes a failure mode the format is
documented to produce — cheaper to guard once than to debug on a published
dashboard.

---

## P7 — Refresh, views and "indexes": analyze before optimizing

**Prompt:**
"Before we call it done: how often should this refresh, should we build
views for the chart queries, and do we need indexes for the filters and
joins? The chart queries are views in themselves — tell me what actually
needs optimizing here and what's already handled by design."

**Context provided:**
- The gold tables' measured sizes and the trigger cadence feeding them
- How published dashboards execute their queries

**AI response:**
No refresh schedule: the published dashboard queries live gold on every
view, so freshness is inherited from the pipeline's trigger (≈2 minutes
behind any silver commit); a schedule would re-read unchanged tables. No
view layer: each dataset already is a named query, and gold itself is the
materialized layer — views over four pre-aggregated tables add indirection,
not speed. No indexes: the platform has none — its equivalents (data
skipping, clustering) pay off by reducing files scanned, and these tables
are effectively one file each, so every tile is a single-file scan. The at-
scale levers (cluster the trends table by date; cluster the silver scan
side) were written into the guide rather than implemented.

**Accepted:** The do-nothing-now conclusion, with the reasoning and the
scale path documented.

**Why:** The dashboard is fast because the gold design made it fast —
recompute-into-small-tables is the performance strategy. Optimizing here
would be decorating the wrong layer.

---

## P8 — Aesthetics in service of analysis speed

**Prompt:**
"Look at the draft again purely for aesthetics — but aesthetics that let
someone get the analysis done quickly and catch things faster, not
decoration. Where a number needs a unit, format it; where a trend is
hiding, surface it; where a chart can carry one more dimension without
clutter, use it."

**Context provided:**
- The published draft
- Measured cardinality of category within the top 10 (five values — safely
  chartable)

**AI response:**
Three changes, all analysis-motivated: the revenue and orders KPIs moved
onto the trends dataset to gain weekly sparklines (a bare number says
nothing about direction) with the revenue value formatted as compact
currency; the top-10 bar gained category coloring, which shows at a glance
which categories dominate the leaderboard; the histogram and pie were
already carrying their weight and stayed. Side effect stated: the sparkline
KPIs now answer to the date filter instead of the segment filter, which
matches what a viewer expects a dated sparkline to do.

**Validation:**
Structural tests re-run green; republished to the same URL and re-verified
via the published-state API.

**Accepted:** Sparklines, currency formatting, category coloring.

**Rejected:** A second y-series (orders) on the revenue trend — the scales
differ by three orders of magnitude and the smaller series would flatline.

**Why:** Every pixel either helps the reader decide something faster or
competes with the pixels that do.

---

## Reusable rules from this activity

| Rule | Origin |
|---|---|
| Dashboards deploy from committed sources, not UI exports | the delivery mechanism |
| A filter's scope is stated, never discovered | scope-labeled filter titles |
| The environment belongs to the deployment, not the source | render-at-deploy |
| Generated exports + guards, never hand-maintained mirrors | dashboard_queries.sql |
| Formatting is an analysis feature, not decoration | the aesthetics pass |
| Optimize the layer that's slow, not the layer you're touching | the refresh/index analysis |
