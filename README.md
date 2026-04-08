# Batch Data Pipeline Orders Project

## Airflow environment

This repo already has a local virtualenv at `airflow_env/`.

Use one of these commands from the project root:

```bash
source airflow_env/bin/activate
```

or, if you want a fresh interactive shell with the environment loaded:

```bash
./scripts/airflow_shell.sh
```

Do not run:

```bash
bash airflow_env/bin/activate
```

That starts a subshell, runs the activate script inside it, and exits without changing your current terminal session.
