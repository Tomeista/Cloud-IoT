# Contributing

Short workflow guide for the SenseIQ codebase. The README documents the
system; this file documents how to work on it.

## Prerequisites

- **Python 3.11+** — the backend and Flink job Dockerfiles pin
  `python:3.11-slim`, so 3.11 is the reference. Local test/lint tooling
  works on newer versions too (verified on 3.14).
- **Node 20** — the frontend Dockerfile pins `node:20-alpine`.
- **git** and **Docker** — for local development and cluster builds.

The full deploy toolchain (helm, kubectl, terraform) is only needed to
push to the cluster; contributions that touch application code do not
require it.

## Getting a local dev environment

Backend / Flink job / sensor simulator — Python:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

Frontend — Node:

```
cd frontend
npm install
```

## Tests and linting

Both are check-only in CI-terms today (no auto-formatting, no rewrites).
They pass on `main`; they should stay passing on every branch.

**Python:**

```
pytest                # unit tests (see tests/README.md)
ruff check .          # lint: pycodestyle errors, pyflakes, import order
```

**Frontend:**

```
cd frontend
npm run lint          # ESLint + eslint-plugin-react-hooks
npm run build         # production build; must succeed
```

A change is ready when all four are green.

## Branch and commit conventions

**Branches** are named `<kind>/<slug>`, matching the categories the
commit-message convention uses — e.g. `feat/lakehouse-delta`,
`fix/pipeline-bugs`, `docs/cluster-screenshots`, `tests/…`, `chore/…`.
Do not push to `main` directly.

**Commits** follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short subject in the imperative>

<body: what changed and, more importantly, why -- linked to the
constraints or bugs that motivated the change>
```

Types used in this repo, with example scopes:

| Type    | When to use                                | Example                                     |
| ------- | ------------------------------------------ | ------------------------------------------- |
| `feat`  | New user-visible or system-visible feature | `feat(frontend): show pipeline data`        |
| `fix`   | Bug fix                                    | `fix(flink): honour configured parallelism` |
| `docs`  | README, CONTRIBUTING, comments             | `docs(readme): add cluster screenshots`     |
| `test`  | Add or change tests                        | `test(archiver): cover _partition_time`     |
| `chore` | Tooling, dependencies, CI                  | `chore(dev): add ruff configuration`        |
| `style` | Formatting or naming only, no behaviour    | `style(flink-job): sort import names`       |

Bodies explain *why*, not *what* (the diff shows *what*). Wrap around
72–80 characters. German or English are both accepted; match the file
you are working on.

## Pair programming

If someone paired on a change, add a `Co-authored-by:` trailer at the
end of the commit message so GitHub attributes both:

```
Co-authored-by: Name <email>
```

Use the `<id>+<username>@users.noreply.github.com` form if the pair
partner would rather not put their personal address in the history.

## Pull requests

- Push the branch and open a PR against `main`.
- PR title = the first line of the commit message (or a superset of it
  if the branch has several commits).
- PR body = a summary of *why*, plus anything a reviewer needs to know
  that is not in the diff (verification steps, migration notes,
  screenshots).
- Wait for review; do not self-merge unless the change is trivial and
  no reviewer is available.
