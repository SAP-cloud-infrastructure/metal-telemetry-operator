# Tasks

## 1. Add chart.yml

- [x] 1.1 Create `.github/workflows/chart.yml` that lints and renders `dist/chart/` on PRs touching `dist/chart/**` or `.github/workflows/chart.yml`; verify `helm lint ./dist/chart` and `helm template metal-telemetry-operator ./dist/chart` both pass locally

## 2. Add check-codegen.yml

- [x] 2.1 Create `.github/workflows/check-codegen.yml` that runs `make generate manifests fmt` and fails if the result produces uncommitted changes; verify the job logic by confirming `make generate manifests fmt` runs cleanly on the current repo with no diff

## 3. Add publish-chart.yml

- [x] 3.1 Create `.github/workflows/publish-chart.yml` that packages `dist/chart` as `metal-telemetry-operator-<version>.tgz` and pushes to `oci://ghcr.io/<owner>/charts` on push/tag/release; verify chart version logic produces a valid semver string by tracing the shell script against a sample SHA and tag

## 4. Add publish-docker.yml

- [x] 4.1 Create `.github/workflows/publish-docker.yml` that builds and pushes `ghcr.io/<owner>/metal-telemetry-operator` for `linux/amd64,linux/arm64` on push/tag/release; verify the metadata tags block references `metal-telemetry-operator` (not `metaldhcp`)

## 5. Integration check

- [x] 5.1 Open a draft PR with all four workflows and confirm GitHub Actions picks them up with the expected job names; verify no syntax errors are reported by the Actions UI

## Workflow follow-up

- Archive this change after the PR is merged and the workflows have run at least once successfully.
