"""Dagster op для очистки временных WebApp-отчетов."""

from dagster import op
from sqlalchemy import text

from config import webapp_report_cleanup_settings


@op(required_resource_keys={"db"})
async def webapp_report_cleanup_op(context) -> None:
    """Удаляет результаты WebApp-отчетов, истекшие больше чем на grace_hours."""
    settings = webapp_report_cleanup_settings()
    grace_hours = max(0, settings.grace_hours)
    sessionmaker = context.resources.db.get_sessionmaker()

    async with sessionmaker() as session:
        await session.execute(text("CREATE SCHEMA IF NOT EXISTS aida"))
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS aida.webapp_report_results (
                    request_id uuid PRIMARY KEY,
                    report_type varchar(50) NOT NULL,
                    status varchar(20) NOT NULL,
                    input_payload jsonb NOT NULL DEFAULT '{}'::jsonb,
                    result_json jsonb,
                    error text,
                    run_id varchar(100),
                    created_at timestamptz NOT NULL DEFAULT now(),
                    finished_at timestamptz,
                    expires_at timestamptz NOT NULL
                )
                """
            )
        )
        await session.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_webapp_report_results_expires_at
                    ON aida.webapp_report_results (expires_at)
                """
            )
        )
        result = await session.execute(
            text(
                """
                DELETE FROM aida.webapp_report_results
                WHERE expires_at < now() - (:grace_hours * interval '1 hour')
                """
            ),
            {"grace_hours": grace_hours},
        )
        await session.commit()

    context.log.info(
        "webapp_report_cleanup: deleted %s rows older than expires_at + %s hours",
        result.rowcount,
        grace_hours,
    )
