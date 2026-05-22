# Terran-Colony---API-SaaS-for-B2B
Integration Reliability for B2B SaaS - How a Game Designer Would Build It. Integration health is invisible until catastrophic — is fundamentally a game design problem. Each genre offers a different answer to "how do you make an invisible system feel alive and worth maintaining."

## Build direction
See `BUILD_FROM_DARK_PERSONA_THREAT_MODEL.md` for a concrete build plan derived from the dark persona and red-team analysis.

## CloudCommander MVP layout verification
If you expect CloudCommander MVP files (`app/`, `migrations/`, `.github/workflows/ci.yml`, `k8s/staging/`) and they appear missing, verify you are on the commit that contains them.

### Cross-platform checks
```bash
git status
git log --oneline -n 3
git ls-files | wc -l
```

### Windows CMD checks (safe quoting)
```bat
git status
git log --oneline -n 3
git ls-files | find /c /v ""
dir /s /b .github\workflows\ci.yml
dir /s /b app\main.py
dir /s /b migrations\001_initial_schema.sql
dir /s /b k8s\staging\api-deployment.yaml
findstr /s /n /i /c:"/api/v1/commands/resource-allocation" app\api\routers\*.py
findstr /s /n /i /c:"/api/v1/commands/dependency-edge" app\api\routers\*.py
findstr /s /n /i /c:"/api/v1/commands/rollback" app\api\routers\*.py
findstr /s /n /i /c:"/api/v1/telemetry/system/backpressure" app\api\routers\*.py
```

> Note: in CMD, `findstr` treats `/...` tokens as options unless you wrap the search term with `/c:"..."`.

## Git failure quick-fix ("Git failed with 4 errors")
Use this sequence when patch/cherry-pick/apply workflows fail and you need a clean recovery to the latest CloudCommander commit.

### 1) Inspect state
```bash
git status
git branch -vv
git log --oneline -n 10
```

### 2) Abort partial operations (safe no-op if none active)
```bash
git cherry-pick --abort || true
git rebase --abort || true
git merge --abort || true
```

### 3) Remove stale temp patch artifacts
```bash
rm -f cloudcommander.patch
```

### 4) Resync branch to remote/main and verify files
```bash
git fetch origin
git checkout main
git reset --hard origin/main
git ls-files | wc -l
test -f app/main.py && echo "app/main.py present"
test -f .github/workflows/ci.yml && echo "ci workflow present"
```

### Error-to-fix map
- `fatal: bad revision <sha>` -> commit does not exist locally; run `git fetch origin --prune` and re-check `git log --oneline --all`.
- `error: pathspec '<branch>' did not match` -> local branch missing; create it from remote: `git checkout -b <branch> origin/<branch>`.
- `patch does not apply` -> wrong base commit; reset to `origin/main` and re-apply changes manually.
- `working tree has local changes` -> commit or stash before switching: `git add -A && git commit -m "wip"` or `git stash -u`.

## Practical workflow for locked-down laptops (CI-first)
When endpoint security does not allow local dependency installs, use this workflow:

1. Create a feature branch.
2. Make changes locally.
3. Push immediately.
4. Review CI output for pass/fail.
5. Iterate from CI failures.
6. Merge only on green checks.

This repository already supports this mode via GitHub Actions and the reproducible bootstrap in `scripts/ci.sh`.
