from __future__ import annotations

import os
import sys
import time
import subprocess
from datetime import datetime, timezone
from pathlib import Path


STALE_LOCK_HOURS = int(os.getenv("STALE_LOCK_HOURS", "12"))

# Defaults (você pode ajustar depois)
CAND_ARGS = [
    "--max-asins", "1200",
    "--candidate-limit", "50",
    "--top-k", "12",
    "--only-with-image",
    "--only-unmatched",
    "--cooldown-days", "7",
    "--sleep", "0.10",
]

PROM_ARGS = [
    "--max-asins", "2000",
    "--top-per-asin", "12",
    "--max-offers-per-asin", "5",
    "--cooldown-hours", "6",
    "--dist-strict", "8",
    "--dist-relaxed", "10",
    "--gtin-min-title-sim", "55",
    "--gtin-max-dist", "15",
    "--sleep", "0.03",
    "--no-refresh-availability",
]


def project_root() -> Path:
    # scripts/ -> raiz
    return Path(__file__).resolve().parents[1]


def ensure_dirs(root: Path) -> tuple[Path, Path, Path]:
    logs = root / "logs"
    pipeline = logs / "Pipeline"
    booster = logs / "MatchBooster"
    pipeline.mkdir(parents=True, exist_ok=True)
    booster.mkdir(parents=True, exist_ok=True)
    return logs, pipeline, booster


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def lock_age_hours(p: Path) -> int:
    try:
        mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        age = datetime.now(timezone.utc) - mtime
        return int(age.total_seconds() // 3600)
    except Exception:
        return -1


def remove_if_stale(p: Path, logf, stale_hours: int) -> bool:
    if not p.exists():
        return False
    age = lock_age_hours(p)
    logf.write(f"[LOCK] {p} age_hours={age}\n")
    if age == -1:
        return False
    if age >= stale_hours:
        logf.write(f"[LOCK] stale >= {stale_hours}h, removendo: {p}\n")
        try:
            p.unlink(missing_ok=True)  # py3.8+ ok no Windows
        except Exception:
            try:
                os.remove(str(p))
            except Exception:
                pass
        return True
    return False


def acquire_lock(p: Path, logf) -> bool:
    """
    Lock simples: cria arquivo se não existir (O_EXCL).
    """
    try:
        fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()} ts={datetime.now().isoformat()}\n")
        logf.write(f"[LOCK] acquired: {p}\n")
        return True
    except FileExistsError:
        logf.write(f"[LOCK] exists: {p}\n")
        return False
    except Exception as e:
        logf.write(f"[LOCK] acquire error: {p} -> {type(e).__name__}: {e}\n")
        return False


def release_lock(p: Path, logf) -> None:
    try:
        if p.exists():
            p.unlink()
            logf.write(f"[LOCK] released: {p}\n")
    except Exception as e:
        logf.write(f"[LOCK] release error: {p} -> {type(e).__name__}: {e}\n")


def run_step(cmd: list[str], logf) -> int:
    logf.write(f"[CMD] {' '.join(cmd)}\n")
    logf.flush()
    p = subprocess.run(cmd, capture_output=True, text=True, cwd=str(project_root()))
    if p.stdout:
        logf.write(p.stdout)
        if not p.stdout.endswith("\n"):
            logf.write("\n")
    if p.stderr:
        logf.write(p.stderr)
        if not p.stderr.endswith("\n"):
            logf.write("\n")
    logf.write(f"[RC] {p.returncode}\n")
    logf.flush()
    return int(p.returncode)


def main() -> int:
    root = project_root()
    _, pipeline_dir, booster_dir = ensure_dirs(root)

    log_path = booster_dir / f"booster_{now_ts()}.log"
    match_lock = pipeline_dir / "match.lock"
    boost_lock = booster_dir / "match_booster.lock"

    with open(log_path, "w", encoding="utf-8") as logf:
        logf.write("==================================================\n")
        logf.write(f"MATCH BOOSTER RUN - {datetime.now()}\n")
        logf.write("==================================================\n")
        logf.write(f"[INFO] ROOT={root}\n")
        logf.write(f"[INFO] PY={sys.executable}\n")
        logf.write(f"[INFO] STALE_LOCK_HOURS={STALE_LOCK_HOURS}\n")

        # stale cleanup
        remove_if_stale(match_lock, logf, STALE_LOCK_HOURS)
        remove_if_stale(boost_lock, logf, STALE_LOCK_HOURS)

        # se lock existe e não é stale -> sai
        if match_lock.exists():
            logf.write("[INFO] MATCH_LOCK ativo. Saindo.\n")
            return 0
        if boost_lock.exists():
            logf.write("[INFO] BOOST_LOCK ativo. Saindo.\n")
            return 0

        # acquire locks
        if not acquire_lock(boost_lock, logf):
            logf.write("[INFO] Não consegui adquirir BOOST_LOCK. Saindo.\n")
            return 0
        if not acquire_lock(match_lock, logf):
            logf.write("[INFO] Não consegui adquirir MATCH_LOCK. Saindo.\n")
            release_lock(boost_lock, logf)
            return 0

        final_rc = 0
        try:
            logf.write("--------------------------------------------------\n")
            logf.write("[STEP] Candidates\n")
            rc = run_step(
                [sys.executable, "-u", "crawlers_ebay/build_match_candidates_from_amazon.py", *CAND_ARGS],
                logf,
            )
            if rc != 0:
                logf.write("[ERROR] Candidates falhou. Abortando.\n")
                return rc

            logf.write("--------------------------------------------------\n")
            logf.write("[STEP] Promote match_offers\n")
            rc = run_step(
                [sys.executable, "-u", "crawlers_ebay/promote_match_offers.py", *PROM_ARGS],
                logf,
            )
            if rc != 0:
                logf.write("[ERROR] Promote match_offers falhou.\n")
                return rc

            logf.write("[OK] Booster finalizado com sucesso.\n")
            final_rc = 0
            return 0
        finally:
            release_lock(match_lock, logf)
            release_lock(boost_lock, logf)
            logf.write(f"[END] RC={final_rc}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())