#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import json
import math
import time
import argparse
import logging
import pathlib
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# ============================================================
# Общие настройки
# ============================================================
VERSION = "-4.1.0-"
BASE_URL = os.environ.get("VK_ADS_BASE_URL", "https://ads.vk.com")

STATS_TIMEOUT = 30
WRITE_TIMEOUT = 30
RETRY_COUNT = 3
RETRY_BACKOFF = 1.8

DEFAULT_MAX_DISABLES_PER_RUN = 15
DEFAULT_USERS_ROOT = os.environ.get("VK_CHECKER_USERS_ROOT", "/opt/vk_checker/v4/users")

# ============================================================
# Логирование
# ============================================================

LOG_DIR = pathlib.Path("/opt/vk_checker/v4/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "vk_checker_v4.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("vk_checker_v4")


# ============================================================
# Утилиты
# ============================================================
def load_global_env() -> None:
    """Глобальный .env (TG_BOT_TOKEN и прочее общее)."""
    env_path = pathlib.Path("/opt/vk_checker/v4/.env")
    try:
        if env_path.exists():
            load_dotenv(dotenv_path=str(env_path), override=False)
        else:
            logger.warning(f"⚠️ Глобальный .env не найден: {env_path}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось загрузить глобальный .env: {e}")


def load_user_env(users_root: str, tg_id: Optional[str]) -> None:
    """Пользовательский .env: /opt/vk_checker/v4/users/<tg_id>/.env"""
    if not tg_id:
        return
    env_path = pathlib.Path(users_root) / str(tg_id) / ".env"
    try:
        if env_path.exists():
            load_dotenv(dotenv_path=str(env_path), override=False)
        else:
            logger.warning(f"⚠️ .env пользователя не найден: {env_path}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось загрузить .env пользователя {tg_id}: {e}")


def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def now_str() -> str:
    return (dt.datetime.now() + dt.timedelta(hours=4)).strftime("%Y-%m-%d %H:%M:%S")


def req_with_retry(
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "3"))
                logger.warning(f"⚠️ VK API rate limit (429). Пауза {retry_after}s перед повтором...")
                time.sleep(retry_after)
                continue

            if resp.status_code >= 400:
                raise requests.HTTPError(f"{resp.status_code} {resp.text}")

            return resp
        except Exception as e:
            last_exc = e
            sleep_for = RETRY_BACKOFF ** (attempt - 1)
            logger.warning(f"{method} {url} попытка {attempt}/{RETRY_COUNT} не удалась: {e}. Повтор через {sleep_for:.1f}s")
            time.sleep(sleep_for)

    assert last_exc is not None
    raise last_exc

# Human reason ================================================

def op_to_human(op: str) -> str:
    op = (op or "").upper()
    return {
        "LT": "<",
        "LTE": "≤",
        "EQ": "=",
        "GTE": "≥",
        "GT": ">",
    }.get(op, op)


def metric_to_human(metric: str) -> str:
    metric = (metric or "").upper()
    return {
        "SPENT": "Расход",
        "CLICKS": "Клики",
        "RESULTS": "Результаты",
        "CLICK_COST": "Цена клика",
        "RESULT_COST": "Цена результата",
        "CPC": "Цена клика",
        "CPA": "Цена результата",
    }.get(metric, metric)

# ============================================================
# Telegram
# ============================================================
def tg_notify(bot_token: str, chat_id: str, text: str, dry_run: bool) -> None:
    if dry_run:
        logger.info("🧪 [DRY RUN] TG уведомление не отправлено (тестовый режим)")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code != 200:
            logger.error(f"TG notify failed: {r.status_code} {r.text}")
    except Exception as e:
        logger.error(f"TG notify exception: {e}")


# ============================================================
# Income loader
# ============================================================
@dataclass
class IncomeStore:
    total: Dict[str, float]
    by_day: Dict[str, Dict[str, float]]  # "dd.mm.YYYY" -> {banner_id: income}

    def income_for_period(self, banner_id: int, period: Dict[str, Any]) -> float:
        bid = str(banner_id)
        ptype = (period or {}).get("type", "ALL_TIME")
        if ptype == "ALL_TIME":
            return safe_float(self.total.get(bid, 0.0))

        today = dt.date.today()
        if ptype == "TODAY":
            key = today.strftime("%d.%m.%Y")
            return safe_float(self.by_day.get(key, {}).get(bid, 0.0))

        if ptype == "YESTERDAY":
            key = (today - dt.timedelta(days=1)).strftime("%d.%m.%Y")
            return safe_float(self.by_day.get(key, {}).get(bid, 0.0))

        if ptype == "LAST_N_DAYS":
            n = int((period or {}).get("n", 1) or 1)
            n = max(1, n)
            s = 0.0
            for i in range(n):
                key = (today - dt.timedelta(days=i)).strftime("%d.%m.%Y")
                s += safe_float(self.by_day.get(key, {}).get(bid, 0.0))
            return s

        return 0.0


def load_income_store(path: str) -> IncomeStore:
    if not path or not os.path.exists(path):
        logger.warning(f"⚠️ Файл доходов {path} не найден — доход будет считаться 0")
        return IncomeStore(total={}, by_day={})

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        total: Dict[str, float] = {}
        by_day: Dict[str, Dict[str, float]] = {}

        if not isinstance(raw, list):
            logger.warning(f"⚠️ Файл доходов {path}: ожидался список, получили {type(raw).__name__}")
            return IncomeStore(total={}, by_day={})

        for entry in raw:
            day_str = entry.get("day")
            data = entry.get("data", {})
            if not day_str or not isinstance(data, dict):
                continue

            day_map: Dict[str, float] = by_day.get(day_str, {})
            for bid, val in data.items():
                try:
                    fval = float(val)
                except Exception:
                    continue
                total[str(bid)] = total.get(str(bid), 0.0) + fval
                day_map[str(bid)] = day_map.get(str(bid), 0.0) + fval

            by_day[day_str] = day_map

        logger.info(f"✅ Загружены доходы: total_banners={len(total)}, days={len(by_day)} из {path}")
        return IncomeStore(total=total, by_day=by_day)

    except Exception as e:
        logger.error(f"Ошибка чтения доходов из {path}: {e}")
        return IncomeStore(total={}, by_day={})


# ============================================================
# VK ADS API
# ============================================================
class VkAdsApi:
    def __init__(self, token: str, base_url: str = BASE_URL, dry_run: bool = False):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.dry_run = dry_run
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        self.banner_info_cache: Dict[int, Dict[str, Any]] = {}
        self.banner_objective_cache: Dict[int, str] = {}  # banner_id -> objective

    def list_banners_by_status(self, status: str, limit: int = 1000) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v2/banners.json"
        offset = 0
        items: List[Dict[str, Any]] = []
        while True:
            params = {"limit": min(limit, 200), "offset": offset, "_status": status}
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = resp.json()
            batch = data.get("items", []) or []
            items.extend(batch)
            logger.info(f"Получено баннеров status={status}: +{len(batch)} (всего {len(items)})")
            if len(batch) < params["limit"]:
                break
            offset += params["limit"]
        return items

    def fetch_banners_info(self, banner_ids: List[int], fields: str = "created,name") -> None:
        if not banner_ids:
            return

        ids_to_fetch = [bid for bid in sorted(set(banner_ids)) if bid not in self.banner_info_cache]
        if not ids_to_fetch:
            return

        url = f"{self.base_url}/api/v2/banners.json"
        chunk_size = 200

        for i in range(0, len(ids_to_fetch), chunk_size):
            chunk = ids_to_fetch[i : i + chunk_size]
            params = {
                "_id__in": ",".join(map(str, chunk)),
                "fields": f"{fields},id",
                "limit": len(chunk),
            }
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = resp.json()
            items = data.get("items", []) or []

            for it in items:
                try:
                    bid = int(it.get("id"))
                except Exception:
                    continue

                info = self.banner_info_cache.get(bid, {})
                for k in ("name", "created", "content", "ad_group_id"):
                    if k in it and it.get(k) is not None:
                        info[k] = it.get(k)

                created_str = info.get("created")
                if created_str and "created_dt" not in info:
                    try:
                        info["created_dt"] = dt.datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pass

                self.banner_info_cache[bid] = info

            logger.info(
                f"Загружено метаданных баннеров: +{len(items)} (chunk {i // chunk_size + 1}/{math.ceil(len(ids_to_fetch) / chunk_size)})"
            )

    def get_banner_name(self, banner_id: int) -> str:
        info = self.banner_info_cache.get(banner_id)
        if info is None:
            self.fetch_banners_info([banner_id], fields="name")
            info = self.banner_info_cache.get(banner_id, {})
        return (info.get("name") or "").strip()

    def get_banner_url(self, banner_id: int) -> str:
        info = self.banner_info_cache.get(banner_id)
        if info is None:
            self.fetch_banners_info([banner_id], fields="content")
            info = self.banner_info_cache.get(banner_id, {})
    
        content = info.get("content")
        if not isinstance(content, dict):
            return ""
    
        # -------------------------
        # 1) Видео: video_portrait_9_16_30s -> high-first_frame -> url
        #    если нет, то любое video_portrait_* -> high-first_frame -> url
        # -------------------------
        def pick_video_key() -> Optional[str]:
            if "video_portrait_9_16_30s" in content:
                return "video_portrait_9_16_30s"
            # любое video_portrait_
            for k in content.keys():
                if isinstance(k, str) and k.startswith("video_portrait_"):
                    return k
            return None
    
        vkey = pick_video_key()
        if vkey:
            vobj = content.get(vkey)
            if isinstance(vobj, dict):
                variants = vobj.get("variants")
                if isinstance(variants, dict):
                    # строго: high-first_frame
                    hf = variants.get("high-first_frame")
                    if isinstance(hf, dict):
                        url = hf.get("url")
                        if isinstance(url, str) and url.strip():
                            return url.strip()
                    # небольшой fallback: любой *first_frame*
                    for kk, vv in variants.items():
                        if isinstance(kk, str) and "first_frame" in kk and isinstance(vv, dict):
                            url = vv.get("url")
                            if isinstance(url, str) and url.strip():
                                return url.strip()
    
        # -------------------------
        # 2) Картинка: image_* -> variants["90x90"] иначе variants["uploaded"] -> url
        #    key может быть image_600x600, image_1080x1080 и т.п.
        # -------------------------
        def pick_image_key() -> Optional[str]:
            for k in content.keys():
                if isinstance(k, str) and k.startswith("image_"):
                    return k
            return None
    
        ikey = pick_image_key()
        if ikey:
            iobj = content.get(ikey)
            if isinstance(iobj, dict):
                variants = iobj.get("variants")
                if isinstance(variants, dict):
                    v90 = variants.get("90x90")
                    if isinstance(v90, dict):
                        url = v90.get("url")
                        if isinstance(url, str) and url.strip():
                            return url.strip()
    
                    up = variants.get("uploaded")
                    if isinstance(up, dict):
                        url = up.get("url")
                        if isinstance(url, str) and url.strip():
                            return url.strip()
    
                    # fallback: если ни 90x90 ни uploaded нет — возьмём любой url из variants
                    for vv in variants.values():
                        if isinstance(vv, dict):
                            url = vv.get("url")
                            if isinstance(url, str) and url.strip():
                                return url.strip()
    
        return ""

    def stats_summary_banners(self, banner_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not banner_ids:
            return {}
        url = f"{self.base_url}/api/v2/statistics/banners/summary.json"
        params = {"id": ",".join(map(str, banner_ids)), "metrics": "base"}
        resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
        data = resp.json()

        result: Dict[int, Dict[str, Any]] = {}
        for it in data.get("items", []) or []:
            try:
                _id = int(it.get("id"))
            except Exception:
                continue

            total = it.get("total", {}) or {}
            base = (total.get("base", {}) or {}).copy()
            vk = base.get("vk", {}) or {}

            spent = safe_float(base.get("spent", 0))
            cpc = safe_float(base.get("cpc", 0))
            clicks = safe_float(base.get("clicks", base.get("clicks_count", 0)))
            goals = safe_float(base.get("goals", base.get("goals_count", base.get("results", 0))))
            vk_cpa = safe_float(vk.get("cpa", 0))

            result[_id] = {
                "spent": spent,
                "cpc": cpc,
                "clicks": clicks,
                "goals": goals,
                "vk.cpa": vk_cpa,
            }
        return result

    def stats_day_banners(self, banner_ids: List[int], date_from: str, date_to: str) -> Dict[int, Dict[str, Any]]:
        if not banner_ids:
            return {}
        url = f"{self.base_url}/api/v2/statistics/banners/day.json"
        params = {"id": ",".join(map(str, banner_ids)), "date_from": date_from, "date_to": date_to, "metrics": "base"}
        resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
        data = resp.json()

        result: Dict[int, Dict[str, Any]] = {}
        for it in data.get("items", []) or []:
            try:
                _id = int(it.get("id"))
            except Exception:
                continue

            total = it.get("total", {}) or {}
            base = (total.get("base", {}) or {}).copy()
            vk = base.get("vk", {}) or {}

            spent = safe_float(base.get("spent", 0))
            cpc = safe_float(base.get("cpc", 0))
            clicks = safe_float(base.get("clicks", base.get("clicks_count", 0)))
            goals = safe_float(base.get("goals", base.get("goals_count", base.get("results", 0))))
            vk_cpa = safe_float(vk.get("cpa", 0))

            result[_id] = {
                "spent": spent,
                "cpc": cpc,
                "clicks": clicks,
                "goals": goals,
                "vk.cpa": vk_cpa,
            }
        return result

    def disable_banner(self, banner_id: int) -> bool:
        if self.dry_run:
            logger.warning(f"🧪 [DRY RUN] Баннер {banner_id} НЕ отключен (тестовый режим)")
            return True
        url = f"{self.base_url}/api/v2/banners/{banner_id}.json"
        try:
            resp = req_with_retry(
                method="POST",
                url=url,
                headers={**self.headers, "Content-Type": "application/json"},
                json_body={"status": "blocked"},
                timeout=WRITE_TIMEOUT,
            )
            if resp.status_code == 204:
                logger.warning("⤷ Баннер успешно отключен (HTTP 204)")
                return True
            logger.warning(f"Не удалось отключить баннер {banner_id}: {resp.status_code} {resp.text}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при отключении баннера {banner_id}: {e}")
            return False

    def enable_banner(self, banner_id: int) -> bool:
        if self.dry_run:
            logger.warning(f"🧪 [DRY RUN] Баннер {banner_id} НЕ включен (тестовый режим)")
            return True
        url = f"{self.base_url}/api/v2/banners/{banner_id}.json"
        try:
            resp = req_with_retry(
                method="POST",
                url=url,
                headers={**self.headers, "Content-Type": "application/json"},
                json_body={"status": "active"},
                timeout=WRITE_TIMEOUT,
            )
            if resp.status_code == 204:
                logger.info("↩ Баннер успешно включён (HTTP 204)")
                return True
            logger.warning(f"Не удалось включить баннер {banner_id}: {resp.status_code} {resp.text}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при включении баннера {banner_id}: {e}")
            return False

    def build_banner_objectives_cache(self) -> Dict[int, str]:
        """
        TARGET_ACTION хранится на уровне групп:
        GET /api/v2/ad_groups.json?_status=active&limit=200&fields=id,banners,objective&offset=...
        Строим mapping banner_id -> objective.
        """
        if self.banner_objective_cache:
            return self.banner_objective_cache

        url = f"{self.base_url}/api/v2/ad_groups.json"
        limit = 200
        offset = 0
        mapping: Dict[int, str] = {}

        while True:
            params = {
                "_status": "active",
                "limit": limit,
                "offset": offset,
                "fields": "id,banners,objective",
            }
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = resp.json()
            items = data.get("items", []) or []

            for g in items:
                objective = (g.get("objective") or "").strip()
                banners = g.get("banners", []) or []
                if not isinstance(banners, list):
                    continue
                for b in banners:
                    try:
                        bid = int(b.get("id"))
                    except Exception:
                        continue
                    mapping[bid] = objective

            if len(items) < limit:
                break
            offset += limit

        self.banner_objective_cache = mapping
        logger.info(f"✅ Загружены objective по группам: banners_with_objective={len(mapping)}")
        return mapping


# ============================================================
# Filters engine
# ============================================================
def op_compare(left: float, op: str, right: float) -> bool:
    op = (op or "").upper()
    if op == "LT":
        return left < right
    if op == "LTE":
        return left <= right
    if op == "EQ":
        return abs(left - right) < 1e-9
    if op == "GTE":
        return left >= right
    if op == "GT":
        return left > right
    return False


def daterange_from_period(period: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    ptype = (period or {}).get("type", "ALL_TIME")
    today = dt.date.today()

    if ptype == "ALL_TIME":
        return None
    if ptype == "TODAY":
        d = today.strftime("%Y-%m-%d")
        return (d, d)
    if ptype == "YESTERDAY":
        d = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        return (d, d)
    if ptype == "LAST_N_DAYS":
        n = int((period or {}).get("n", 1) or 1)
        n = max(1, n)
        date_from = (today - dt.timedelta(days=n - 1)).strftime("%Y-%m-%d")
        date_to = today.strftime("%Y-%m-%d")
        return (date_from, date_to)

    return None


def metric_value_from_stats(stats: Dict[str, Any]) -> Dict[str, float]:
    spent = safe_float(stats.get("spent", 0))
    clicks = safe_float(stats.get("clicks", 0))
    goals = safe_float(stats.get("goals", 0))
    cpc = safe_float(stats.get("cpc", 0))
    vk_cpa = safe_float(stats.get("vk.cpa", 0))

    click_cost = cpc if cpc > 0 else (spent / clicks if clicks > 0 else 0.0)
    result_cost = vk_cpa if vk_cpa > 0 else (spent / goals if goals > 0 else 0.0)

    return {
        "SPENT": spent,
        "CLICKS": clicks,
        "RESULTS": goals,
        "CLICK_COST": click_cost,
        "RESULT_COST": result_cost,
        "CPC": click_cost,
        "CPA": result_cost,
    }

def period_to_label(period: Dict[str, Any]) -> str:
    """Красивое описание периода + датный диапазон."""
    ptype = (period or {}).get("type", "ALL_TIME")
    dr = daterange_from_period(period)
    if dr is None:
        return "ALL_TIME"
    date_from, date_to = dr
    if ptype == "LAST_N_DAYS":
        n = int((period or {}).get("n", 1) or 1)
        return f"LAST_N_DAYS(n={n}) [{date_from}..{date_to}]"
    return f"{ptype} [{date_from}..{date_to}]"


def log_banner_stats(
    banner_id: int,
    periods: List[Dict[str, Any]],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
    target_action: str = "",
) -> None:
    """Печатает статистику баннера по ALL_TIME и всем периодам из filters.json."""
    all_time_key = json.dumps({"type": "ALL_TIME"}, sort_keys=True, ensure_ascii=False)

    # ALL_TIME
    s_all = (stats_by_period.get(all_time_key, {}) or {}).get(banner_id, {}) or {}
    mv_all = metric_value_from_stats(s_all)
    inc_all = income_store.income_for_period(banner_id, {"type": "ALL_TIME"})

    ta_txt = f" target_action={target_action}" if target_action else ""
    logger.info(
        f"[BANNER {banner_id}]{ta_txt} ALL_TIME: "
        f"spent_all_time={mv_all['SPENT']:.2f} "
        f"cpa_all_time={mv_all['RESULT_COST']:.2f} "
        f"cpc_all_time={mv_all['CLICK_COST']:.2f} "
        f"clicks_all_time={mv_all['CLICKS']:.0f} "
        f"results_all_time={mv_all['RESULTS']:.0f} "
        f"income_all_time={inc_all:.2f}"
    )

    # Остальные периоды из filters.json
    for p in periods:
        if not isinstance(p, dict):
            continue
        if (p.get("type") or "ALL_TIME") == "ALL_TIME":
            continue

        key = json.dumps(p, sort_keys=True, ensure_ascii=False)
        s = (stats_by_period.get(key, {}) or {}).get(banner_id, {}) or {}
        mv = metric_value_from_stats(s)
        inc = income_store.income_for_period(banner_id, p)

        logger.info(
            f"[BANNER {banner_id}] PERIOD {period_to_label(p)}: "
            f"spent={mv['SPENT']:.2f} "
            f"cpa={mv['RESULT_COST']:.2f} "
            f"cpc={mv['CLICK_COST']:.2f} "
            f"clicks={mv['CLICKS']:.0f} "
            f"results={mv['RESULTS']:.0f} "
            f"income={inc:.2f}"
        )

def extract_templates(filters_json: Any) -> List[Dict[str, Any]]:
    if isinstance(filters_json, dict):
        tpls = filters_json.get("templates")
        if isinstance(tpls, list):
            return [t for t in tpls if isinstance(t, dict)]
        if "root" in filters_json:
            return [filters_json]
    if isinstance(filters_json, list):
        return [t for t in filters_json if isinstance(t, dict)]
    return []


def accounts_scope_allows(accounts_scope: Dict[str, Any], cabinet_id: str) -> bool:
    if not isinstance(accounts_scope, dict):
        return True
    mode = (accounts_scope.get("mode") or "ALL").upper()
    if mode == "ALL":
        return True
    if mode == "SELECTED":
        selected = accounts_scope.get("selected") or accounts_scope.get("accounts") or accounts_scope.get("ids")
        if isinstance(selected, list):
            return str(cabinet_id) in {str(x) for x in selected}
        return False
    return False


def objective_to_target_action(objective: str) -> str:
    obj = (objective or "").strip()
    if obj == "socialengagement":
        return "BOT_MESSAGE"
    if obj == "site_conversions":
        return "SITE"
    if obj == "leadads":
        return "LEADFORM"
    # всё остальное
    return "APP"


def banner_target_action_from_groups(banner_id: int, banner_objectives: Dict[int, str]) -> str:
    objective = banner_objectives.get(int(banner_id), "")
    return objective_to_target_action(objective)


def eval_conditions(
    conditions: List[Dict[str, Any]],
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
    banner_objectives: Dict[int, str],
) -> bool:
    for cond in conditions or []:
        if not isinstance(cond, dict):
            continue

        ctype = (cond.get("type") or "").upper()

        if ctype == "SPENT":
            period = cond.get("period") or {"type": "ALL_TIME"}
            key = json.dumps(period, sort_keys=True, ensure_ascii=False)
            stats = stats_by_period.get(key, {}).get(banner_id, {}) or {}
            mv = metric_value_from_stats(stats)
            op = cond.get("op", "GTE")
            value = safe_float(cond.get("valueRub", 0))
            if not op_compare(mv["SPENT"], op, value):
                return False

        elif ctype == "INCOME":
            period = cond.get("period") or {"type": "ALL_TIME"}
            income = income_store.income_for_period(banner_id, period)

            mode = (cond.get("mode") or "HAS").upper()
            if mode == "HAS":
                if income <= 0:
                    return False
            elif mode in ("NONE", "NO", "EMPTY"):
                if income > 0:
                    return False
            else:
                op = cond.get("op")
                if op:
                    value = safe_float(cond.get("valueRub", 0))
                    if not op_compare(income, op, value):
                        return False
                else:
                    return False

        elif ctype == "TARGET_ACTION":
            target = (cond.get("target") or "").strip()
            if not target:
                continue
            actual = banner_target_action_from_groups(banner_id, banner_objectives)
            if actual != target:
                return False

        else:
            return False

    return True


def eval_cost_rule(rule: Dict[str, Any], banner_id: int, stats_by_period: Dict[str, Dict[int, Dict[str, Any]]]) -> Tuple[bool, str]:
    if not isinstance(rule, dict):
        return False, ""
    if (rule.get("type") or "").upper() != "COST_RULE":
        return False, ""

    spent_rub = safe_float(rule.get("spentRub", 0))
    metric = (rule.get("metric") or "").upper()
    op = (rule.get("op") or "EQ").upper()
    value = safe_float(rule.get("value", 0))

    period = rule.get("period") or {"type": "ALL_TIME"}
    key = json.dumps(period, sort_keys=True, ensure_ascii=False)
    stats = stats_by_period.get(key, {}).get(banner_id, {}) or {}
    mv = metric_value_from_stats(stats)

    if mv["SPENT"] < spent_rub:
        return False, ""

    if metric not in mv:
        return False, ""

    ok = op_compare(mv[metric], op, value)
    if not ok:
        return False, ""

    # причина
    reason = (
        f"{metric_to_human(metric)} {op_to_human(op)} {value:.2f} "
        f"при расходе ≥ {spent_rub:.2f} (период {period_to_label(period)})"
    )
    return True, reason


def eval_filter_node(
    node: Dict[str, Any],
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
    banner_objectives: Dict[int, str],
) -> Tuple[str, str]:
    if not isinstance(node, dict):
        return "NOOP", ""

    ntype = (node.get("type") or "").upper()
    if ntype != "FILTER":
        child = node.get("child")
        return eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store, banner_objectives)

    mode = (node.get("mode") or "ALL").upper()
    rules = node.get("rules") or []
    if not isinstance(rules, list):
        rules = []

    conditions = node.get("conditions") or []
    if isinstance(conditions, list) and conditions:
        if not eval_conditions(conditions, banner_id, banner_obj, stats_by_period, income_store, banner_objectives):
            child = node.get("child")
            return eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store, banner_objectives)

    rule_hits: List[Tuple[bool, str]] = []
    for r in rules:
        if isinstance(r, dict) and (r.get("type") or "").upper() == "COST_RULE":
            rule_hits.append(eval_cost_rule(r, banner_id, stats_by_period))
        else:
            rule_hits.append((False, ""))

    hit_bools = [x[0] for x in rule_hits]
    if not hit_bools:
        matched = False
    elif mode == "ANY":
        matched = any(hit_bools)
    else:
        matched = all(hit_bools)

    if matched:
        # причина
        reasons = [x[1] for x in rule_hits if x[0] and x[1]]
        if mode == "ANY":
            reason = reasons[0] if reasons else ""
        else:
            reason = "; ".join(reasons) if reasons else ""

        action = node.get("action") or {}
        if isinstance(action, dict) and (action.get("type") or "").upper() == "SET_STATE":
            state = (action.get("state") or "NOOP").upper()
            if state in ("DISABLE", "ENABLE", "NOOP"):
                return state, reason
        return "NOOP", ""

    child = node.get("child")
    return eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store, banner_objectives)


def decide_action_for_banner(
    templates: List[Dict[str, Any]],
    cabinet_id: str,
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
    banner_objectives: Dict[int, str],
) -> Tuple[str, str]:
    ordered = sorted(
        [t for t in templates if isinstance(t, dict)],
        key=lambda x: int(x.get("priority", 9999) or 9999),
    )

    for tpl in ordered:
        root = tpl.get("root") if "root" in tpl else tpl.get("root", tpl.get("ROOT"))
        if not isinstance(root, dict):
            continue

        scope = root.get("accountsScope") or {}
        if not accounts_scope_allows(scope, cabinet_id):
            continue

        conditions = root.get("conditions") or []
        if isinstance(conditions, list) and conditions:
            if not eval_conditions(conditions, banner_id, banner_obj, stats_by_period, income_store, banner_objectives):
                continue

        child = root.get("child") or {}
        state, reason = eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store, banner_objectives)
        if state and state.upper() != "NOOP":
            # добавим в причину название шаблона, чтобы человеку было понятно
            tpl_name = str(tpl.get("name") or tpl.get("id") or "").strip()
            if tpl_name:
                reason = f"[{tpl_name}] {reason}".strip()
            return state.upper(), reason

    return "NOOP", ""


# ============================================================
# disabled_banners.json per cabinet
# ============================================================
def disabled_file_path(users_root: str, tg_id: str, cabinet_id: str) -> pathlib.Path:
    p = pathlib.Path(users_root) / str(tg_id) / str(cabinet_id)
    ensure_dir(p)
    return p / "disabled_banners.json"

def enabled_file_path(users_root: str, tg_id: str, cabinet_id: str) -> pathlib.Path:
    p = pathlib.Path(users_root) / str(tg_id) / str(cabinet_id)
    ensure_dir(p)
    return p / "enabled_banners.json"


def history_file_path(users_root: str, tg_id: str, cabinet_id: str) -> pathlib.Path:
    p = pathlib.Path(users_root) / str(tg_id) / str(cabinet_id)
    ensure_dir(p)
    return p / "history_banners.json"

def append_history(path: pathlib.Path, record: Dict[str, str]) -> None:
    try:
        data: List[Dict[str, str]] = []
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, list):
                data = [x for x in raw if isinstance(x, dict)]
        data.append(record)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка записи history {path}: {e}")

def load_disabled_records(path: pathlib.Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            out: Dict[str, Dict[str, str]] = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    out[str(k)] = {str(kk): "" if vv is None else str(vv) for kk, vv in v.items()}
            return out
        if isinstance(data, list):
            out2: Dict[str, Dict[str, str]] = {}
            for item in data:
                if not isinstance(item, dict):
                    continue
                bid = str(item.get("id_banner", "")).strip()
                if not bid:
                    continue
                out2[bid] = {str(k): "" if v is None else str(v) for k, v in item.items()}
            return out2
    except Exception as e:
        logger.error(f"Ошибка чтения {path}: {e}")
    return {}


def save_disabled_records(path: pathlib.Path, records: Dict[str, Dict[str, str]]) -> None:
    try:
        arr = list(records.values())
        with open(path, "w", encoding="utf-8") as f:
            json.dump(arr, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения {path}: {e}")


def make_banner_record(
    banner_id: int,
    name: str,
    url: str,
    stats_all_time: Dict[str, Any],
    *,
    status: str,
    checker_enabled: str,
    reason: str,
) -> Dict[str, str]:
    mv = metric_value_from_stats(stats_all_time or {})
    return {
        "daytime": now_str(),
        "id_banner": str(banner_id),
        "name_banner": name or "",
        "url": url or "",
        "reason": reason or "",
        "status": status,
        "checker_enabled": checker_enabled,
        "spent_all_time": f"{mv['SPENT']:.2f}",
        "goals_all_time": f"{mv['RESULTS']:.0f}",
        "cpa_all_time": f"{mv['RESULT_COST']:.2f}",
        "clicks_all_time": f"{mv['CLICKS']:.0f}",
        "cpc_all_time": f"{mv['CLICK_COST']:.2f}",
    }

# ============================================================
# User config + settings loader
# ============================================================
def load_user_config(users_root: str, tg_id: str) -> Dict[str, Any]:
    path = pathlib.Path(users_root) / str(tg_id) / f"{tg_id}.json"
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл пользователя: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Некорректный формат {path}: ожидался JSON object")
    return data


def load_user_settings(users_root: str, tg_id: str) -> Dict[str, Any]:
    path = pathlib.Path(users_root) / str(tg_id) / "settings.json"
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception as e:
        logger.error(f"Ошибка чтения settings.json ({path}): {e}")
    return {}


def load_user_filters(users_root: str, tg_id: str) -> List[Dict[str, Any]]:
    path = pathlib.Path(users_root) / str(tg_id) / "filters.json"
    if not path.exists():
        logger.warning(f"⚠️ Не найден filters.json: {path} — действий не будет")
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        tpls = extract_templates(data)
        logger.info(f"✅ Загружены фильтры: templates={len(tpls)} из {path}")
        return tpls
    except Exception as e:
        logger.error(f"Ошибка чтения filters.json ({path}): {e}")
        return []


def discover_users(users_root: str) -> List[str]:
    p = pathlib.Path(users_root)
    if not p.exists():
        return []
    out: List[str] = []
    for child in p.iterdir():
        if child.is_dir():
            out.append(child.name)
    return sorted(out)


# ============================================================
# Сбор периодов из filters.json
# ============================================================
def collect_periods_from_filters(templates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    periods: List[Dict[str, Any]] = [{"type": "ALL_TIME"}]

    def add_period(p: Any) -> None:
        if not isinstance(p, dict):
            return
        if "type" not in p:
            return
        periods.append({"type": p.get("type"), "n": p.get("n")})

    def walk_node(obj: Any) -> None:
        if isinstance(obj, dict):
            if "period" in obj:
                add_period(obj.get("period"))
            for v in obj.values():
                walk_node(v)
        elif isinstance(obj, list):
            for it in obj:
                walk_node(it)

    walk_node(templates)

    uniq: Dict[str, Dict[str, Any]] = {}
    for p in periods:
        key = json.dumps(p, sort_keys=True, ensure_ascii=False)
        uniq[key] = p

    return list(uniq.values())


# ============================================================
# Stats cache
# ============================================================
def build_stats_cache(
    api: VkAdsApi,
    banner_ids: List[int],
    periods: List[Dict[str, Any]],
) -> Dict[str, Dict[int, Dict[str, Any]]]:
    out: Dict[str, Dict[int, Dict[str, Any]]] = {}

    for period in periods:
        key = json.dumps(period, sort_keys=True, ensure_ascii=False)
        dr = daterange_from_period(period)
        if dr is None:
            out[key] = api.stats_summary_banners(banner_ids)
        else:
            date_from, date_to = dr
            out[key] = api.stats_day_banners(banner_ids, date_from, date_to)

    return out


# ============================================================
# Processing one cabinet
# ============================================================
def process_cabinet(
    *,
    users_root: str,
    tg_id: str,
    chat_id: Optional[str],
    tg_bot_token: Optional[str],
    templates: List[Dict[str, Any]],
    income_store: IncomeStore,
    cabinet: Dict[str, Any],
    dry_run: bool,
    max_disables: int,
    ignore_manual_enabled_ads: bool,
) -> None:
    cabinet_id = str(cabinet.get("id") or "").strip()
    cabinet_name = str(cabinet.get("name") or cabinet_id or "CABINET").strip()
    token_ref = str(cabinet.get("token") or "").strip()
    
    token = ""
    if token_ref:
        # 1) если token_ref совпадает с переменной окружения — берём её значение
        env_val = os.environ.get(token_ref)
        if env_val:
            token = str(env_val).strip()
        else:
            # 2) иначе считаем, что в token_ref уже лежит реальный токен
            token = token_ref
    
    # поддержка старого/альтернативного поля token_env (на всякий случай)
    if not token:
        token_env = str(cabinet.get("token_env") or "").strip()
        if token_env:
            token = str(os.environ.get(token_env, "")).strip()

    if not cabinet_id or not token:
        logger.warning(f"[{tg_id}] Пропуск кабинета: не хватает id/token. cabinet={cabinet}")
        return

    logger.info("=" * 80)
    logger.info(f"[USER {tg_id}] CABINET: {cabinet_name} (id={cabinet_id}) | ignore_manual_enabled_ads={ignore_manual_enabled_ads}")

    api = VkAdsApi(token=token, base_url=BASE_URL, dry_run=dry_run)

    active_banners = api.list_banners_by_status("active")
    blocked_banners = api.list_banners_by_status("blocked")

    active_ids = [int(b.get("id")) for b in active_banners if b.get("id") is not None]
    blocked_ids = [int(b.get("id")) for b in blocked_banners if b.get("id") is not None]

    all_ids = sorted(set(active_ids + blocked_ids))
    if not all_ids:
        logger.info("Баннеров не найдено")
        return

    api.fetch_banners_info(all_ids, fields="created,name,content,ad_group_id")

    # Важно: цель (TARGET_ACTION) берём по ad_groups objective
    banner_objectives = api.build_banner_objectives_cache()

    periods = collect_periods_from_filters(templates)
    stats_by_period = build_stats_cache(api, all_ids, periods)

    dis_path = disabled_file_path(users_root, tg_id, cabinet_id)
    disabled_records = load_disabled_records(dis_path)

    en_path = enabled_file_path(users_root, tg_id, cabinet_id)
    enabled_records = load_disabled_records(en_path)

    his_path = history_file_path(users_root, tg_id, cabinet_id)
    
    active_by_id: Dict[int, Dict[str, Any]] = {}
    blocked_by_id: Dict[int, Dict[str, Any]] = {}
    for b in active_banners:
        try:
            active_by_id[int(b.get("id"))] = b
        except Exception:
            pass
    for b in blocked_banners:
        try:
            blocked_by_id[int(b.get("id"))] = b
        except Exception:
            pass

    disabled_count = 0
    notify_disabled: List[str] = []
    notify_enabled: List[str] = []

    # 1) DISABLE для активных
    for bid in active_ids:
        if ignore_manual_enabled_ads and str(bid) in disabled_records:
            logger.info(f"▶ Пропускаем баннер {bid}: already in disabled_banners.json и ignore_manual_enabled_ads=true")
            continue

        ta = banner_target_action_from_groups(bid, banner_objectives)
        log_banner_stats(
            banner_id=bid,
            periods=periods,
            stats_by_period=stats_by_period,
            income_store=income_store,
            target_action=ta,
        )
        
        bobj = active_by_id.get(bid, {})

        state, reason = decide_action_for_banner(
            templates=templates,
            cabinet_id=cabinet_id,
            banner_id=bid,
            banner_obj=bobj,
            stats_by_period=stats_by_period,
            income_store=income_store,
            banner_objectives=banner_objectives,
        )

        if state != "DISABLE":
            continue

        if disabled_count >= max_disables:
            logger.warning("🚨 Достигнут лимит отключений за запуск — дальнейшие баннеры не будут отключаться")
            break

        ok = api.disable_banner(bid)
        if not ok:
            continue

        disabled_count += 1

        name = api.get_banner_name(bid) or "Без названия"
        url = api.get_banner_url(bid)
        stats_all_key = json.dumps({"type": "ALL_TIME"}, sort_keys=True, ensure_ascii=False)
        stats_all = (stats_by_period.get(stats_all_key, {}) or {}).get(bid, {}) or {}
        
        rec = make_banner_record(
            bid, name, url, stats_all,
            status="off",
            checker_enabled="on",
            reason=reason or "Отключено фильтром",
        )
        
        disabled_records[str(bid)] = rec
        enabled_records.pop(str(bid), None)
        append_history(his_path, rec)

        mv = metric_value_from_stats(stats_all)
        income_all = income_store.income_for_period(bid, {"type": "ALL_TIME"})
        notify_disabled.append(
            f"<b>{name}</b> #{bid}\n"
            f"    ⤷ Потрачено(all) = {mv['SPENT']:.2f} ₽ | Доход(all) = {income_all:.2f} ₽\n"
            f"    ⤷ Результаты(all) = {mv['RESULTS']:.0f} | CPA(all) = {mv['RESULT_COST']:.2f} ₽ | CPC(all) = {mv['CLICK_COST']:.2f} ₽"
        )

    # 2) ENABLE для blocked (только те, что мы отключали)
    for bid in blocked_ids:
        if str(bid) not in disabled_records:
            continue

        ta = banner_target_action_from_groups(bid, banner_objectives)
        log_banner_stats(
            banner_id=bid,
            periods=periods,
            stats_by_period=stats_by_period,
            income_store=income_store,
            target_action=ta,
        )
        
        bobj = blocked_by_id.get(bid, {})

        state, reason = decide_action_for_banner(
            templates=templates,
            cabinet_id=cabinet_id,
            banner_id=bid,
            banner_obj=bobj,
            stats_by_period=stats_by_period,
            income_store=income_store,
            banner_objectives=banner_objectives,
        )
        if state != "ENABLE":
            continue
        
        ok = api.enable_banner(bid)
        if not ok:
            continue
        
        name = api.get_banner_name(bid) or "Без названия"
        url = api.get_banner_url(bid)
        stats_all_key = json.dumps({"type": "ALL_TIME"}, sort_keys=True, ensure_ascii=False)
        stats_all = (stats_by_period.get(stats_all_key, {}) or {}).get(bid, {}) or {}
        
        rec = make_banner_record(
            bid, name, url, stats_all,
            status="on",
            checker_enabled="on",   # требование: если включен фильтром -> on
            reason=reason or "Включено фильтром",
        )
        
        # переносим между списками
        disabled_records.pop(str(bid), None)
        enabled_records[str(bid)] = rec
        
        # история: только дописываем
        append_history(his_path, rec)

        notify_enabled.append(
            f"<b>{name}</b> #{bid}\n"
            f"    ⤷ Потрачено(all) = {mv['SPENT']:.2f} ₽ | Доход(all) = {income_all:.2f} ₽\n"
            f"    ⤷ Результаты(all) = {mv['RESULTS']:.0f} | CPA(all) = {mv['RESULT_COST']:.2f} ₽ | CPC(all) = {mv['CLICK_COST']:.2f} ₽"
        )

    save_disabled_records(dis_path, disabled_records)
    save_disabled_records(en_path, enabled_records)
    
    logger.info(f"💾 disabled_banners.json: {dis_path} (records={len(disabled_records)})")
    logger.info(f"💾 enabled_banners.json:  {en_path} (records={len(enabled_records)})")
    logger.info(f"🧾 history_banners.json:  {his_path}")

    if tg_bot_token and chat_id:
        if notify_disabled:
            text = f"<b>[{cabinet_name}]</b>\n<b>Отключены баннеры:</b>\n\n" + "\n\n".join(notify_disabled)
            tg_notify(tg_bot_token, chat_id, text, dry_run=dry_run)

        if notify_enabled:
            text = f"<b>[{cabinet_name}]</b>\n<b>Включены баннеры:</b>\n\n" + "\n\n".join(notify_enabled)
            tg_notify(tg_bot_token, chat_id, text, dry_run=dry_run)


# ============================================================
# main
# ============================================================
def main() -> None:
    load_global_env()

    parser = argparse.ArgumentParser(description="VK checker v4 (dynamic users/filters)")
    parser.add_argument("--user", help="TG user id (folder name) to process only this user", default=None)
    parser.add_argument("--users-root", help="Root dir for users", default=DEFAULT_USERS_ROOT)
    parser.add_argument("--dry-run", action="store_true", help="Do not change VK banners state")
    parser.add_argument("--max-disables", type=int, default=DEFAULT_MAX_DISABLES_PER_RUN, help="Max disables per cabinet per run")
    args = parser.parse_args()

    users_root = args.users_root
    dry_run = bool(args.dry_run)
    max_disables = int(args.max_disables)

    tg_bot_token = os.environ.get("TG_BOT_TOKEN")
    if not tg_bot_token:
        logger.warning("⚠️ TG_BOT_TOKEN не задан — уведомления в Telegram отправляться не будут")

    users: List[str]
    if args.user:
        users = [str(args.user)]
    else:
        users = discover_users(users_root)

    if not users:
        logger.warning(f"Пользователи не найдены в {users_root}")
        return

    logger.info(f"Старт {VERSION} | users={len(users)} | dry_run={dry_run} | users_root={users_root}")

    for tg_id in users:
        load_user_env(users_root, tg_id)
        try:
            cfg = load_user_config(users_root, tg_id)
            settings = load_user_settings(users_root, tg_id)
            ignore_manual_enabled_ads = bool(settings.get("ignore_manual_enabled_ads", False))

            chat_id = cfg.get("chat_id")
            if chat_id is not None:
                chat_id = str(chat_id)

            income_path = str(cfg.get("income_path") or "").strip()
            income_store = load_income_store(income_path) if income_path else IncomeStore(total={}, by_day={})

            templates = load_user_filters(users_root, tg_id)
            if not templates:
                logger.info(f"[USER {tg_id}] Нет активных фильтров — пропуск (ничего делать не будем)")
                continue

            accounts = cfg.get("accounts") or cfg.get("cabinets") or []
            if not isinstance(accounts, list) or not accounts:
                logger.warning(f"[USER {tg_id}] В конфиге нет accounts/cabinets — пропуск")
                continue

            for cab in accounts:
                if not isinstance(cab, dict):
                    continue
                if cab.get("active") is False:
                    continue

                try:
                    process_cabinet(
                        users_root=users_root,
                        tg_id=tg_id,
                        chat_id=chat_id,
                        tg_bot_token=tg_bot_token,
                        templates=templates,
                        income_store=income_store,
                        cabinet=cab,
                        dry_run=dry_run,
                        max_disables=max_disables,
                        ignore_manual_enabled_ads=ignore_manual_enabled_ads,
                    )
                except Exception as e:
                    logger.exception(f"[USER {tg_id}] Ошибка обработки кабинета {cab.get('name') or cab.get('id')}: {e}")

        except Exception as e:
            logger.exception(f"Ошибка обработки пользователя {tg_id}: {e}")

    logger.info("Готово")


if __name__ == "__main__":
    main()
