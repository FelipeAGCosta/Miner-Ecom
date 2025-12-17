from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from lib.config import make_engine


def _now() -> datetime:
    return datetime.now()


def start_crawler_run(
    marketplace_id: str,
    root_filter: Optional[str],
    max_tasks: Optional[int],
    max_items: Optional[int],
    refresh_days: Optional[int],
    skip_recent_days: Optional[int],
    tasks_total: Optional[int] = None,
    last_task_index_before: Optional[int] = None,
) -> Optional[int]:
    """
    Cria um registro em amazon_crawler_runs e retorna o run_id.
    Se der erro de DB, retorna None (não deve quebrar o crawler).
    """
    sql = """
    INSERT INTO amazon_crawler_runs
    (
      marketplace_id, started_at, status,
      root_filter, max_tasks, max_items, refresh_days, skip_recent_days,
      tasks_total, last_task_index_before
    )
    VALUES
    (
      :marketplace_id, :started_at, 'running',
      :root_filter, :max_tasks, :max_items, :refresh_days, :skip_recent_days,
      :tasks_total, :last_task_index_before
    );
    """
    params = {
        "marketplace_id": marketplace_id,
        "started_at": _now(),
        "root_filter": root_filter,
        "max_tasks": max_tasks,
        "max_items": max_items,
        "refresh_days": refresh_days,
        "skip_recent_days": skip_recent_days,
        "tasks_total": tasks_total,
        "last_task_index_before": last_task_index_before,
    }

    try:
        engine = make_engine()
        with engine.begin() as conn:
            res = conn.execute(text(sql), params)
            run_id = getattr(res, "lastrowid", None)
            if not run_id:
                run_id = conn.execute(text("SELECT LAST_INSERT_ID() AS id")).mappings().first()["id"]
            return int(run_id)
    except Exception:
        return None


def finish_crawler_run(
    run_id: int,
    status: str,
    stop_reason: Optional[str],
    state_saved: bool,
    last_task_index_after: Optional[int],
    tasks_run: Optional[int],
    stats: Dict[str, Any],
    refresh: Dict[str, Any],
) -> None:
    """
    Finaliza um run (success ou failed) atualizando métricas agregadas.
    Se der erro de DB, não levanta exceção.
    """
    sql = """
    UPDATE amazon_crawler_runs
    SET
      ended_at = :ended_at,
      status = :status,
      stop_reason = :stop_reason,
      state_saved = :state_saved,
      last_task_index_after = :last_task_index_after,
      tasks_run = :tasks_run,

      catalog_seen = :catalog_seen,
      with_price = :with_price,
      kept = :kept,
      skipped_recent = :skipped_recent,
      skipped_no_price = :skipped_no_price,
      dup_asins = :dup_asins,
      price_lookups = :price_lookups,
      errors_api = :errors_api,

      refresh_total = :refresh_total,
      refresh_existing = :refresh_existing,
      refresh_recent = :refresh_recent,
      refresh_to_upsert = :refresh_to_upsert,
      refresh_new = :refresh_new,
      refresh_stale = :refresh_stale,

      error_message = :error_message
    WHERE id = :id;
    """

    def _i(x: Any) -> int:
        try:
            return int(x)
        except Exception:
            return 0

    params = {
        "id": run_id,
        "ended_at": _now(),
        "status": status,
        "stop_reason": stop_reason,
        "state_saved": 1 if state_saved else 0,
        "last_task_index_after": last_task_index_after,
        "tasks_run": tasks_run,

        "catalog_seen": _i(stats.get("catalog_seen")),
        "with_price": _i(stats.get("with_price")),
        "kept": _i(stats.get("kept")),
        "skipped_recent": _i(stats.get("skipped_recent")),
        "skipped_no_price": _i(stats.get("skipped_no_price")),
        "dup_asins": _i(stats.get("dup_asins")),
        "price_lookups": _i(stats.get("price_lookups")),
        "errors_api": _i(stats.get("errors_api")),

        "refresh_total": _i(refresh.get("total")),
        "refresh_existing": _i(refresh.get("existing")),
        "refresh_recent": _i(refresh.get("recent")),
        "refresh_to_upsert": _i(refresh.get("to_upsert")),
        "refresh_new": _i(refresh.get("new")),
        "refresh_stale": _i(refresh.get("stale")),

        "error_message": stats.get("error_message"),
    }

    try:
        engine = make_engine()
        with engine.begin() as conn:
            conn.execute(text(sql), params)
    except Exception:
        pass


def fail_crawler_run(run_id: int, error_message: str) -> None:
    """
    Marca failed e salva erro (sem quebrar o processo).
    """
    try:
        engine = make_engine()
        with engine.begin() as conn:
            conn.execute(
                text("""
                UPDATE amazon_crawler_runs
                SET ended_at = :ended_at, status = 'failed', error_message = :err
                WHERE id = :id
                """),
                {"ended_at": _now(), "err": (error_message or "")[:1000], "id": run_id},
            )
    except Exception:
        pass
