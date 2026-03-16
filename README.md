# mcc-live-e2e-template

Minimal Python CLI issue tracker used as the baseline repository for MCC live E2E runs.

## Goals

- no build step
- no third-party dependencies
- deterministic file layout for shuttle playbooks
- fast local verification

## Usage

```bash
python3 -m issue_tracker add "Write docs"
python3 -m issue_tracker list
python3 -m issue_tracker close 1
```

By default the tracker stores data in `issues.json` in the current working directory. Tests should pass with:

```bash
python3 -m unittest
```
