# Enterprise additions -- what changed and why

These files extend the existing CI/CD and ADF reference material with patterns
you'd expect on a real data platform team. Same rule as the rest of this repo:
label what's actually running vs. what's a reference pattern, and don't claim
more than what's true.

## What's now verified/running (once you copy these in and push)

- **`.github/workflows/ci.yml`** (replaces the existing one) -- splits the
  single job into `test` (now matrix-tested on Python 3.10 and 3.12, with
  coverage), `lint`, and `security` (pip-audit + bandit) running in parallel,
  plus a `ci-gate` job so branch protection only needs one required check.
  This all runs on push/PR with no external dependencies -- it's real the
  moment you push it.
- **`.github/dependabot.yml`** -- weekly automated PRs for pip and Action
  version bumps. Real once enabled (Dependabot is on by default for public
  repos once this file exists).
- **`CODEOWNERS`** -- auto-requests review on matching paths. Real
  immediately, though on a solo repo it just tags yourself; the value is
  showing you know the pattern.

## What's still reference-only (same caveat as the existing adf/README.md)

- **`.github/workflows/cd.yml`** -- introduces GitHub Environments
  (dev/staging/prod) with approval gates for prod. The *mechanism* (environment
  protection rules) is a real GitHub feature you can turn on in Settings, but
  the deploy step itself is unverified against a live workspace, same as
  before.
- **`adf/triggers/tumbling_window_trigger.json`** -- fills the "no trigger
  configured" gap the original `adf/README.md` called out. `runtimeState` is
  set to `"Stopped"` on purpose -- it's a definition, not a live schedule.
- **`adf/triggers/failure_alert_webhook.json`** -- fills the "no failure
  alerting" gap the original README called out. Needs a real webhook URL
  (Teams/Slack incoming webhook or a Logic App) to do anything.

## If asked about this in an interview

"I extended the CI pipeline with parallel lint/test/security jobs and added
the trigger and alerting patterns the original ADF reference was missing.
The CI side is real and running against every push. The CD and ADF pieces
are reference implementations following the standard pattern -- I don't have
a live Azure subscription to deploy them against, so I built them to be
correct and ready to wire in, and documented exactly what's needed to make
that happen." That's a stronger answer than pretending it's all live, and it
shows you understand *why* each piece exists, not just that you copied a
template.
