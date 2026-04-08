from dotenv import load_dotenv
import os
import pytest
from types import SimpleNamespace

# ✅ Absolute path (important fix)
load_dotenv(dotenv_path=os.path.abspath(".env.dev"))

from src.utils.config import SNOWFLAKE_CONFIG
from src.loading.load_to_snowflake import _build_copy_into_sql, _resolve_aws_credentials
import snowflake.connector


def test_snowflake_connection():
    if os.getenv("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("Set RUN_INTEGRATION_TESTS=1 to run Snowflake connectivity tests")

    required_keys = [
        "user",
        "password",
        "account",
        "warehouse",
        "database",
        "schema",
    ]
    missing = [k for k in required_keys if not SNOWFLAKE_CONFIG.get(k)]
    if missing:
        pytest.skip(f"Snowflake env vars missing: {', '.join(missing)}")

    connect_kwargs = {k: SNOWFLAKE_CONFIG[k] for k in required_keys}
    conn = snowflake.connector.connect(**connect_kwargs)
    cur = conn.cursor()

    cur.execute("SELECT CURRENT_DATABASE();")
    result = cur.fetchone()

    assert result is not None


def test_build_copy_into_sql_includes_s3_and_parquet():
    creds = SimpleNamespace(
        access_key="AKIA_TEST",
        secret_key="SECRET_TEST",
        token="TOKEN_TEST",
    )

    sql = _build_copy_into_sql("ORDERS", "s3://bucket/dev/cleaned_data/", creds)

    assert "COPY INTO ORDERS" in sql
    assert "FROM 's3://bucket/dev/cleaned_data/'" in sql
    assert "FILE_FORMAT=(TYPE=PARQUET)" in sql
    assert "PATTERN='.*[.]parquet'" in sql
    assert "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE" in sql
    assert "AWS_KEY_ID='AKIA_TEST'" in sql
    assert "AWS_SECRET_KEY='SECRET_TEST'" in sql
    assert "AWS_TOKEN='TOKEN_TEST'" in sql


def test_resolve_aws_credentials_raises_when_missing(monkeypatch):
    class FakeSession:
        def get_credentials(self):
            return None

    monkeypatch.setattr("src.loading.load_to_snowflake.Session", lambda: FakeSession())

    with pytest.raises(RuntimeError, match="AWS credentials were not found"):
        _resolve_aws_credentials()
