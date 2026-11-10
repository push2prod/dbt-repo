# Patches

Changes to the mock dbt projects (`e2e/fixtures/`) that change their structure,
the model graph, and so the manifests and the DAGs Cosmos builds from them. The e2e test applies **all of them at once** to a copy of `e2e/fixtures/` (`git apply`,
in directory order), regenerates the bundle, syncs it to Airflow under load,
and checks every `expect.yaml`.

| patch | project / DAG | structural change |
|---|---|---|
| 010-add-model | core / dbt_core_accounts | new leaf model → new task |
| 020-remove-model | finance / dbt_finance_billing | model deleted → task gone |
| 030-rename-model | marketing / dbt_marketing_content | model renamed → old task gone, new task |
| 040-insert-model | product / dbt_product_search | model inserted mid-chain → new task, edge rewired |
| 050-add-dependency | operations / dbt_operations_warehouse | new upstream model, picked up by the `+` selector |
| 060-remove-dependency | core / dbt_core_orders | ref removed → edge gone, parent drops out of the `+` selector |

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
    upstream_removed:                  # model -> models its task must no longer depend on
      fct_search_daily: [int_search_enriched]
```

Models are matched against Cosmos task ids (`<model>_run` / `<model>.run`).

To add a patch: edit a scratch copy of `e2e/fixtures/`, `git diff > NNN-name/change.patch`,
and write its `expect.yaml`. Patches must apply cleanly on top of each other.
