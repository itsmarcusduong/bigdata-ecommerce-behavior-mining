---
name: github-milestone-workflow
description: Use when planning or doing GitHub project management in this repo - creating milestones, issues (one student, one result), branches, pull requests with review comments, squash-merge, tags and releases.
---

# github-milestone-workflow

Full policy: `docs/GITHUB_WORKFLOW.md`. Milestones M1-M10 map to tags `v0.1.0`...`v1.0.0`.

## Steps
1. **Milestone**: title `M<n> - <name>`, description lists deliverables, due date (editable plan).
2. **Issue** (template *Task*/*Experiment*): one measurable result, one assignee, labels (`task|experiment|bug|docs|kaggle-run` + area label), milestone, Definition of Done checklist. A real-data run is its own `kaggle-run` issue.
3. **Branch** `feat|fix|docs|results/<milestone>-<short>` from `main`.
4. **Commits**: Conventional Commits (`feat(lsh): ...`). Include a body when the reason is not obvious.
5. **PR** from the template: summary, `Closes #n`, verification checklist (`make lint test smoke`), screenshots/metrics if any.
6. **Review**: comment on specific lines (correctness, complexity, leakage, seeds, baselines); author replies/fixes; resolve threads; then **squash-merge** and delete the branch.
7. **README before every merge and release**: a PR that adds/renames a notebook, brings real results or a changed conclusion, a new release/tag, new commands, folders, skills or data/licence facts updates `README.md` in the same PR (status table, notebook table, "Kết quả chính" with numbers from `results/*.json`); otherwise it says "README: không đổi". Before tagging, re-read README against the release notes.
8. **Tag at code-complete**: when the implementation issues of the milestone are merged and CI is green -> `git tag -a v0.N.0 -m "<deliverables>"`, `git push origin v0.N.0`, create a pre-release (notes: what shipped, how verified, known gaps).
9. **Close the milestone** only when its `kaggle-run` issue is closed too (real-data run done, `results/` committed).

## Rules
* `main` stays green (CI = ruff + pytest + notebooks in sync). No direct pushes after M1.
* Do not backdate commits or claim real-data results that were not run; mark such work as `kaggle-run` issues instead.
* Never commit data, tokens, `kaggle.json`. Commits carry no `Co-Authored-By` trailer (owner's decision, 22/11/2026); never rewrite existing history to remove old ones.
* Use `gh api repos/<owner>/<repo>/...` (REST) when GraphQL is unavailable.
