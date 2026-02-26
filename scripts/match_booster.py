from __future__ import annotations

import json
import os
import sys
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(str(v).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return float(default)
    try:
        return float(str(v).strip())
    except Exception:
        return float(default)


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name)
    return str(v).strip() if v is not None and str(v).strip() != "" else default


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
            p.unlink(missing_ok=True)
        except Exception:
            try:
                os.remove(str(p))
            except Exception:
                pass
        return True
    return False


def acquire_lock(p: Path, logf) -> bool:
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


def _read_refresh_state(state_path: Path) -> Dict[str, Any]:
    try:
        if not state_path.exists():
            return {}
        return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_refresh_state(state_path: Path, payload: Dict[str, Any]) -> None:
    try:
        state_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _should_run_refresh(state_path: Path, refresh_every_days: int, force: bool) -> bool:
    if force:
        return True
    if refresh_every_days <= 0:
        return False

    st = _read_refresh_state(state_path)
    last = st.get("last_refresh_utc")
    if not last:
        return True

    try:
        last_dt = datetime.fromisoformat(str(last).replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return True

    due = last_dt + timedelta(days=int(refresh_every_days))
    return datetime.now(timezone.utc) >= due


def _build_cand_args(mode: str) -> List[str]:
    # candidates
    candidate_limit = _env_int("BOOST_CAND_LIMIT", 120)
    ebay_pages = _env_int("BOOST_CAND_EBAY_PAGES", 1)
    top_k = _env_int("BOOST_CAND_TOP_K", 20)
    top_k_cheap = _env_int("BOOST_CAND_TOP_K_CHEAP", 20)
    min_title_sim = _env_float("BOOST_CAND_MIN_TITLE_SIM", 35.0)
    sleep_s = _env_float("BOOST_CAND_SLEEP", 0.10)

    conditions = _env_str("BOOST_CAND_CONDITIONS", "NEW,USED")
    sorts = _env_str("BOOST_CAND_SORTS", "price,bestMatch")

    only_with_image = _env_int("BOOST_CAND_ONLY_WITH_IMAGE", 1)

    if mode == "refresh":
        max_asins = _env_int("BOOST_REFRESH_CAND_MAX_ASINS", 80)
        cooldown_days = _env_int("BOOST_REFRESH_CAND_COOLDOWN_DAYS", 0)
        only_unmatched = 0
    else:
        max_asins = _env_int("BOOST_DISCOVERY_CAND_MAX_ASINS", 200)
        cooldown_days = _env_int("BOOST_DISCOVERY_CAND_COOLDOWN_DAYS", 7)
        only_unmatched = _env_int("BOOST_DISCOVERY_ONLY_UNMATCHED", 0)

    args = [
        "--max-asins", str(max_asins),
        "--candidate-limit", str(candidate_limit),
        "--ebay-pages", str(ebay_pages),
        "--top-k", str(top_k),
        "--top-k-cheap", str(top_k_cheap),
        "--min-title-sim", str(min_title_sim),
        "--conditions", conditions,
        "--sorts", sorts,
        "--cooldown-days", str(cooldown_days),
        "--sleep", f"{sleep_s:.2f}",
    ]

    if only_with_image == 1:
        args.insert(0, "--only-with-image")
    if only_unmatched == 1:
        args.insert(0, "--only-unmatched")

    return args


def _build_prom_args(mode: str) -> List[str]:
    # promote (NOVO promote_match_offers.py)
    max_asins = _env_int("BOOST_REFRESH_PROM_MAX_ASINS", 2000) if mode == "refresh" else _env_int("BOOST_DISCOVERY_PROM_MAX_ASINS", 1200)
    top_per_asin = _env_int("BOOST_PROM_TOP_PER_ASIN", 50)

    # você quer 10:
    max_offers_per_asin = _env_int("BOOST_PROM_MAX_OFFERS_PER_ASIN", 10)

    # no promote novo, cooldown pode ser 0 pra testar, e depois você sobe
    cooldown_hours = _env_int("BOOST_REFRESH_PROM_COOLDOWN_HOURS", 0) if mode == "refresh" else _env_int("BOOST_DISCOVERY_PROM_COOLDOWN_HOURS", 12)

    gtin_min_title_sim = _env_float("BOOST_PROM_GTIN_MIN_TITLE_SIM", 40.0)
    title_min_sim = _env_float("BOOST_PROM_TITLE_MIN_SIM", 86.0)
    title_min_sim_no_signals = _env_float("BOOST_PROM_TITLE_MIN_SIM_NO_SIGNALS", 90.0)

    timeout_s = _env_int("BOOST_PROM_TIMEOUT", 25)
    sleep_s = _env_float("BOOST_PROM_SLEEP", 0.02)

    calc_img = _env_int("BOOST_PROM_CALC_IMAGE_DISTANCE", 0)

    args = [
        "--max-asins", str(max_asins),
        "--top-per-asin", str(top_per_asin),
        "--max-offers-per-asin", str(max_offers_per_asin),
        "--cooldown-hours", str(cooldown_hours),

        "--gtin-min-title-sim", f"{gtin_min_title_sim:.2f}",
        "--title-min-sim", f"{title_min_sim:.2f}",
        "--title-min-sim-no-signals", f"{title_min_sim_no_signals:.2f}",

        "--timeout", str(timeout_s),
        "--sleep", f"{sleep_s:.2f}",

        "--no-refresh-availability",
    ]
    if calc_img == 1:
        args.append("--calc-image-distance")

    return args


def main() -> int:
    # carrega .env (Windows não carrega sozinho)
    try:
        from dotenv import load_dotenv  # type: ignore
        env_path = project_root() / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except Exception:
        pass

    stale_lock_hours = _env_int("STALE_LOCK_HOURS", 12)

    root = project_root()
    _, pipeline_dir, booster_dir = ensure_dirs(root)

    log_path = booster_dir / f"booster_{now_ts()}.log"
    match_lock = pipeline_dir / "match.lock"
    boost_lock = booster_dir / "match_booster.lock"
    refresh_state_path = booster_dir / "refresh_state.json"

    boost_mode = _env_str("BOOST_MODE", "auto").lower()
    refresh_every_days = _env_int("BOOST_REFRESH_EVERY_DAYS", 15)
    force_refresh = _env_int("BOOST_REFRESH_FORCE", 0) == 1

    if boost_mode not in ("auto", "refresh", "discovery"):
        boost_mode = "auto"

    if boost_mode == "refresh":
        mode = "refresh"
    elif boost_mode == "discovery":
        mode = "discovery"
    else:
        mode = "refresh" if _should_run_refresh(refresh_state_path, refresh_every_days, force_refresh) else "discovery"

    cand_args = _build_cand_args(mode)
    prom_args = _build_prom_args(mode)

    with open(log_path, "w", encoding="utf-8") as logf:
        logf.write("==================================================\n")
        logf.write(f"MATCH BOOSTER RUN - {datetime.now()}\n")
        logf.write("==================================================\n")
        logf.write(f"[INFO] ROOT={root}\n")
        logf.write(f"[INFO] PY={sys.executable}\n")
        logf.write(f"[INFO] STALE_LOCK_HOURS={stale_lock_hours}\n")
        logf.write(f"[INFO] BOOST_MODE={boost_mode} -> mode={mode}\n")
        logf.write(f"[INFO] REFRESH_EVERY_DAYS={refresh_every_days} FORCE={int(force_refresh)}\n")
        logf.write(f"[INFO] REFRESH_STATE={refresh_state_path}\n")
        logf.write(f"[INFO] CAND_ARGS={' '.join(cand_args)}\n")
        logf.write(f"[INFO] PROM_ARGS={' '.join(prom_args)}\n")

        remove_if_stale(match_lock, logf, stale_lock_hours)
        remove_if_stale(boost_lock, logf, stale_lock_hours)

        if match_lock.exists():
            logf.write("[INFO] MATCH_LOCK ativo. Saindo.\n")
            return 0
        if boost_lock.exists():
            logf.write("[INFO] BOOST_LOCK ativo. Saindo.\n")
            return 0

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
            logf.write(f"[STEP] Candidates ({mode})\n")
            rc = run_step([sys.executable, "-u", "crawlers_ebay/build_match_candidates_from_amazon.py", *cand_args], logf)
            final_rc = rc
            if rc != 0:
                logf.write("[ERROR] Candidates falhou. Abortando.\n")
                return rc

            logf.write("--------------------------------------------------\n")
            logf.write(f"[STEP] Promote match_offers ({mode})\n")
            rc = run_step([sys.executable, "-u", "crawlers_ebay/promote_match_offers.py", *prom_args], logf)
            final_rc = rc
            if rc != 0:
                logf.write("[ERROR] Promote match_offers falhou.\n")
                return rc

            if mode == "refresh":
                _write_refresh_state(refresh_state_path, {
                    "last_refresh_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "refresh_every_days": refresh_every_days,
                })
                logf.write("[INFO] refresh_state atualizado.\n")

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