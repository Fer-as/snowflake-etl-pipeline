# Airflow Connections (Challenge Task 6)

The pipeline needs two Airflow connections:

| Conn ID | Type | Used by | Defined in |
|---|---|---|---|
| `snowflake_default` | Snowflake | Every `SQLExecuteQueryOperator` / `SQLCheckOperator` | Airflow UI, stored encrypted (Fernet) in the PostgreSQL metadata DB |
| `aws_default` | Amazon Web Services | SNS alert callback (`SnsHook`), `connection_healthcheck` DAG (STS, S3, SNS) | `AIRFLOW_CONN_AWS_DEFAULT` in `docker-compose.yaml` |

## Is a dedicated AWS connection required?

The challenge lists an *AWS connection* under Task 6. Before this change, AWS
access worked only through boto3's credential chain, which used the
temporary `aws login` session mounted into the containers. That works, but
Airflow had no AWS connection object, so provider hooks had nothing to
reference, and nothing could test AWS access from inside Airflow.

**Decision:** add `aws_default` **without any keys**.

```yaml
AIRFLOW_CONN_AWS_DEFAULT: '{"conn_type": "aws", "extra": {"region_name": "eu-west-2"}}'
```

When an `aws` connection has no login or password, the Amazon provider's
`AwsBaseHook` falls back to boto3's default credential chain. Here that is
the mounted, auto-refreshing `aws login` session. This approach:

* satisfies Task 6 with a real, testable Airflow AWS connection;
* creates **no** static IAM access keys, so nothing long-lived can leak;
* is version-controlled and reproducible, unlike a connection created by
  clicking in the UI;
* keeps the existing S3 upload scripts unchanged. They run as plain Python
  via `BashOperator` and use the same credential chain, so their behaviour is
  identical.

> Environment-variable connections are **not listed** in the UI's
> *Admin → Connections* page. Verify them with
> `docker compose exec airflow-worker airflow connections get aws_default`.

**Rejected alternatives:**

* An IAM user with access keys in the Airflow connection. Long-lived secrets
  in the metadata DB break the project rule of no static keys.
* An `AssumeRole` connection (`role_arn` in extras). This is the right choice
  for a server deployment, but locally it still needs base credentials to
  assume from. It's listed under future improvements.

## `snowflake_default`

This connection already exists and works. It was created under
*Admin → Connections → +*. To recreate it, for example on a fresh metadata
volume:

| Field | Value |
|---|---|
| Connection Id | `snowflake_default` |
| Connection Type | Snowflake |
| Login | `<snowflake-user>` |
| Password | `<password or programmatic access token>` |
| Schema | `RETAIL` |
| Extra / fields | `account` = `<orgname>-<accountname>`, `warehouse` = `RETAIL_ETL_WH`, `database` = `RETAIL_ETL_DB`, `role` = `<role>` |

CLI equivalent (placeholders only):

```powershell
docker compose exec airflow-api-server airflow connections add snowflake_default `
  --conn-type snowflake --conn-login "<user>" --conn-password "<secret>" --conn-schema RETAIL `
  --conn-extra '{"account": "<orgname>-<accountname>", "warehouse": "RETAIL_ETL_WH", "database": "RETAIL_ETL_DB", "role": "<role>"}'
```

**What goes in "Extra":** provider-specific settings that have no dedicated
field, stored as JSON. For Snowflake that means account, warehouse, database,
role and authenticator. For AWS it means region, role ARN and endpoint URL.
Extras are encrypted with the Fernet key together with the password.

**Account identifier format:** the preferred form is `<orgname>-<accountname>`.
The legacy form is `<account_locator>.<region>`, for example
`xy12345.eu-west-2.aws`. Never include `.snowflakecomputing.com`.

## Testing the connections

```powershell
# Read-only DAG: STS identity, bucket + 3 CSVs, SNS topic, Snowflake tables
docker compose exec airflow-api-server airflow dags unpause connection_healthcheck
docker compose exec airflow-api-server airflow dags trigger connection_healthcheck

# Single connection from the CLI
docker compose exec airflow-worker airflow connections get aws_default -o json
# expected: "conn_type": "aws", "login": null, "password": null,
#           "extra_dejson": {"region_name": "eu-west-2"}
```

Airflow 3 disables connection testing by default (`[core] test_connection =
Disabled`), so both the UI *Test* button and `airflow connections test` refuse
to run. Rather than enabling it, the project uses the health-check DAG. It runs
on the worker with the same credentials and network path as the real tasks.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `The conn_id 'aws_default' isn't defined` | Containers were started before the compose change. Run `docker compose up -d` |
| `ExpiredToken` / session expired | Run `aws login` on the host |
| Snowflake `250001` / `390100` authentication errors | Wrong user, secret or account identifier. Check the format above |
| Snowflake `No active warehouse` | Set `warehouse` in the connection extras |
| `Object does not exist or not authorized` | The role lacks `USAGE`/`SELECT`. Grant it on the database, schema and tables |
