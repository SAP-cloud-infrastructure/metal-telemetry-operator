# Design

## Context

See proposal.md — Why. The four workflows are modeled on `metaldhcp` with adaptations for mto's chart layout and image name. mto's existing workflows (`lint.yml`, `test.yml`, `test-chart.yml`, `test-e2e.yml`) use pinned-SHA action refs; the new workflows will use tag-style refs instead (a deliberate consistency boundary between auto-scaffolded and hand-authored workflows).

## Goals / Non-Goals

**Goals:**
- Add `chart.yml`, `check-codegen.yml`, `publish-chart.yml`, `publish-docker.yml` as close to the metaldhcp pattern as the mto repo layout allows
- Use consistent, current action versions across all four new files
- Gate publishing on PR labels (`ok-to-charts`, `ok-to-image`) to avoid unintended pushes from forks

**Non-Goals:**
- Updating existing mto workflows (`lint.yml`, `test.yml`, etc.) to tag-style refs
- Adding Dependabot for actions version management
- Introducing a `dev/values.yaml` (mto has no dev overrides)

## Decisions

### Action ref style: tag (@v6) over pinned SHA
mto's scaffolded workflows use pinned SHAs. New hand-authored workflows will use tag-style refs to match metaldhcp and stay readable. The four actions involved (`actions/*`, `docker/*`, `azure/setup-helm`) are all first-party large-org maintainers — tag-drift risk is low. Consistent, current versions across all four files:

| Action | Version |
|---|---|
| `actions/checkout` | `@v6` |
| `actions/setup-go` | `@v6` |
| `azure/setup-helm` | `@v5` |
| `docker/metadata-action` | `@v6` |
| `docker/setup-qemu-action` | `@v4` |
| `docker/setup-buildx-action` | `@v4` |
| `docker/login-action` | `@v4` |
| `docker/build-push-action` | `@v7` |

### Chart path: `dist/chart/` not `chart/<name>/`
mto's chart is generated output from `make helm` and lives at `dist/chart/`. metaldhcp keeps a hand-maintained chart at `chart/metaldhcp/`. The `chart.yml` and `publish-chart.yml` workflows reference `dist/chart/` accordingly.

### No `dev/values.yaml` lint step in `chart.yml`
metaldhcp has a `dev/values.yaml` for local overrides. mto has no equivalent, so the second lint/render step is dropped.

### Codegen check: reuse existing make targets
`check-codegen.yml` runs `make generate manifests fmt` — the same targets mto uses in its other Go workflows. No new Makefile targets needed.

## Risks / Trade-offs

- **`dist/chart/` is generated** — if someone edits it directly without `make helm`, both `chart.yml` (lint) and `check-codegen.yml` (if chart gen were wired in) would disagree. `check-codegen.yml` only runs `generate manifests fmt`, not `helm` regeneration, so chart drift is a gap. Low risk for now; can be addressed if chart generation becomes part of the codegen target.
- **Mixed action ref styles in the repo** — existing scaffolded workflows use SHAs, new ones use tags. This is intentional but a reader may find it surprising. The distinction is documented here.
