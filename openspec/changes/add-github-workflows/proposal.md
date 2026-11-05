# Proposal

## Why

`metal-telemetry-operator` has no CI workflows for chart linting, codegen validation, or publishing artifacts. `metaldhcp` has a proven set of four workflows that cover these gaps and serve as the reference pattern for this project family.

## What Changes

- Add `.github/workflows/chart.yml` — lints and renders the Helm chart on PRs that touch `dist/chart/`
- Add `.github/workflows/check-codegen.yml` — runs `make generate manifests fmt` and fails if uncommitted changes result
- Add `.github/workflows/publish-chart.yml` — packages and pushes the Helm chart to GHCR on push/tag/release
- Add `.github/workflows/publish-docker.yml` — builds and pushes the Docker image to GHCR on push/tag/release

## Capabilities

### New Capabilities

_None — this change adds CI tooling only; no operator behavior changes._

### Modified Capabilities

_None._

## Impact

- New files only: `.github/workflows/chart.yml`, `check-codegen.yml`, `publish-chart.yml`, `publish-docker.yml`
- Requires `GITHUB_TOKEN` (automatically available in Actions) — no additional secrets needed
- Publishing workflows gate on `ok-to-charts` / `ok-to-image` labels for PRs, matching the metaldhcp pattern
