# Patches

Changes to the mock dbt content (`e2e/fixtures/`) that change the generated
manifests. The e2e test applies **all of them at once** to a copy of `e2e/fixtures/` (`git apply`,
in directory order), regenerates the bundle, syncs it to Airflow under load,
and checks every `expect.yaml`.

Each patch is a directory:

```
NNN-name/
  change.patch   unified diff against e2e/fixtures/ (git diff format)
  expect.yaml    what Airflow must show after the sync
```

`expect.yaml`:

```yaml
description: one line
rebuilt_projects: [core]          # projects whose manifest must be re-generated
dags:                             # per DAG; all keys optional
  dbt_core_accounts:
    tasks_added: [accounts_churn]      # a model that must now have a task
    tasks_removed: [billing_summary]   # a model that must no longer have a task
    schedule: "15 8 * * *"             # the DAG's new cron schedule
    upstream:                          # model -> models its task must depend on
      accounts_churn: [dim_accounts_entity]
```

Models are matched against Cosmos task ids (`<model>_run` / `<model>.run`).

To add a patch: edit a scratch copy of `e2e/fixtures/`, `git diff > NNN-name/change.patch`,
and write its `expect.yaml`. Patches must apply cleanly on top of each other.
