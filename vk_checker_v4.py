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
VERSION = "-4.1.74-"
BASE_URL = os.environ.get("VK_ADS_BASE_URL", "https://ads.vk.com")

STATS_TIMEOUT = 30
WRITE_TIMEOUT = 30
RETRY_COUNT = 3
RETRY_BACKOFF = 1.8

DEFAULT_MAX_DISABLES_PER_RUN = 20
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
            load_dotenv(dotenv_path=str(env_path), override=True)
        else:
            logger.warning(f"⚠️ .env пользователя не найден: {env_path}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось загрузить .env пользователя {tg_id}: {e}")


def ensure_dir(p: pathlib.Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def chunked(lst: List[int], size: int = 200) -> List[List[int]]:
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default
        
#округление
def fmt_int(x: Any, default: int = 0) -> str:
    try:
        if x is None:
            return str(default)
        return str(int(round(float(x))))
    except Exception:
        return str(default)

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
            goals = safe_float(vk.get("goals", 0))
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
            goals = safe_float(vk.get("goals", 0))
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

    def build_groups_objective_cache(self, group_ids: List[int]) -> Dict[int, str]:
        """
        Получаем objective по ad_group_id:
        GET /api/v2/ad_groups.json?_id__in=...&fields=id,objective
        Возвращаем mapping: group_id -> objective
        """
        url = f"{self.base_url}/api/v2/ad_groups.json"
        mapping: Dict[int, str] = {}
    
        uniq = sorted({int(x) for x in group_ids if int(x) > 0})
        if not uniq:
            return mapping
    
        chunk_size = 200
        for i in range(0, len(uniq), chunk_size):
            chunk = uniq[i:i + chunk_size]
            params = {
                "_id__in": ",".join(map(str, chunk)),
                "limit": len(chunk),
                "fields": "id,objective",
            }
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = resp.json()
            items = data.get("items", []) or []
    
            for g in items:
                try:
                    gid = int(g.get("id"))
                except Exception:
                    continue
                mapping[gid] = (g.get("objective") or "").strip()
    
            logger.info(f"ad_groups _id__in chunk {i // chunk_size + 1}: groups={len(items)}")
    
        logger.info(f"✅ Загружены objective по группам: groups_with_objective={len(mapping)}")
        return mapping

    def fetch_group_ids_from_campaigns(self, campaign_ids: List[int]) -> List[int]:
        """
        /api/v2/ad_plans.json?_status=active&limit=200&_id__in=...&fields=id,name,ad_groups
        Возвращает список ad_group_id.
        """
        uniq = sorted({int(x) for x in campaign_ids if int(x) > 0})
        if not uniq:
            return []

        url = f"{self.base_url}/api/v2/ad_plans.json"
        out: List[int] = []
        chunk_size = 200

        for i in range(0, len(uniq), chunk_size):
            chunk = uniq[i:i + chunk_size]
            params = {
                "_status": "active",
                "limit": 200,
                "_id__in": ",".join(map(str, chunk)),
                "fields": "id,name,ad_groups",
            }
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = resp.json()
            items = data.get("items", []) or []

            for plan in items:
                ad_groups = plan.get("ad_groups", []) or []
                if not isinstance(ad_groups, list):
                    continue
                for g in ad_groups:
                    try:
                        gid = int(g.get("id"))
                    except Exception:
                        continue
                    if gid > 0:
                        out.append(gid)

        return sorted(set(out))

    def fetch_banner_ids_from_groups(self, group_ids: List[int]) -> List[int]:
        """
        /api/v2/ad_groups.json?_status=active&limit=200&_id__in=...&fields=id,name,banners
        Возвращает список banner_id из banners[].
        """
        uniq = sorted({int(x) for x in group_ids if int(x) > 0})
        if not uniq:
            return []

        url = f"{self.base_url}/api/v2/ad_groups.json"
        out: List[int] = []
        chunk_size = 200

        for i in range(0, len(uniq), chunk_size):
            chunk = uniq[i:i + chunk_size]
            params = {
                "_status": "active",
                "limit": 200,
                "_id__in": ",".join(map(str, chunk)),
                "fields": "id,name,banners",
            }
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = resp.json()
            items = data.get("items", []) or []

            for g in items:
                banners = g.get("banners", []) or []
                if not isinstance(banners, list):
                    continue
                for b in banners:
                    try:
                        bid = int(b.get("id"))
                    except Exception:
                        continue
                    if bid > 0:
                        out.append(bid)

        return sorted(set(out))

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
    """Человеческое описание периода + (если нужно) диапазон дат."""
    ptype = (period or {}).get("type", "ALL_TIME")
    dr = daterange_from_period(period)

    if ptype == "ALL_TIME":
        return "Период: за всё время"

    if not dr:
        return f"Период: {ptype}"

    date_from, date_to = dr
    if ptype == "LAST_N_DAYS":
        n = int((period or {}).get("n", 1) or 1)
        return f"Период: последние {n} дн. ({date_from}..{date_to})"

    if ptype == "TODAY":
        return f"Период: сегодня ({date_from})"

    if ptype == "YESTERDAY":
        return f"Период: вчера ({date_from})"

    return f"Период: {ptype} ({date_from}..{date_to})"


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


def banner_target_action_from_groups(ad_group_id: int, group_objectives: Dict[int, str]) -> str:
    objective = group_objectives.get(int(ad_group_id), "")
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
        
            if mode in ("HAS", "EXISTS"):
                if income <= 0:
                    return False
        
            elif mode in ("HAS_NOT", "NOT_HAS", "NOT", "NONE", "NO", "EMPTY", "ZERO"):
                if income > 0:
                    return False
        
            elif mode == "COMPARE":
                op = (cond.get("op") or "").upper().strip()
                if not op:
                    return False
                value = safe_float(cond.get("valueRub", cond.get("value", 0)))
                if not op_compare(income, op, value):
                    return False
        
            elif mode == "COMPARE_SPEND":
                op = (cond.get("op") or "").upper().strip()
                if not op:
                    return False
        
                threshold = safe_float(cond.get("multiplier", 0))
        
                spend_period = cond.get("spendPeriod") or {"type": "ALL_TIME"}
                spend_key = json.dumps(spend_period, sort_keys=True, ensure_ascii=False)
                spend_stats = stats_by_period.get(spend_key, {}).get(banner_id, {}) or {}
                spend = metric_value_from_stats(spend_stats)["SPENT"]
        
                delta = income - spend
                if not op_compare(delta, op, threshold):
                    return False
        
            else:
                return False

        elif ctype == "TARGET_ACTION":
            target = (cond.get("target") or "").strip()
            if not target:
                continue
            gid = int((banner_obj or {}).get("ad_group_id") or (banner_obj.get("ad_group_id") if isinstance(banner_obj, dict) else 0) or 0)
            actual = banner_target_action_from_groups(gid, banner_objectives)
            if actual != target:
                return False

        else:
            return False

    return True

def conditions_to_reason(
    conditions: List[Dict[str, Any]],
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
    banner_objectives: Dict[int, str],
) -> Tuple[str, str]:
    """
    Возвращает (reason, short_reason) по условиям.
    Предполагается, что условия уже ПРОШЛИ (true), но мы всё равно берём фактические значения для текста.
    """
    parts_long: List[str] = []
    parts_short: List[str] = []

    for cond in conditions or []:
        if not isinstance(cond, dict):
            continue

        ctype = (cond.get("type") or "").upper()

        if ctype == "SPENT":
            period = cond.get("period") or {"type": "ALL_TIME"}
            key = json.dumps(period, sort_keys=True, ensure_ascii=False)
            stats = stats_by_period.get(key, {}).get(banner_id, {}) or {}
            mv = metric_value_from_stats(stats)

            op = (cond.get("op") or "GTE").upper()
            value = safe_float(cond.get("valueRub", 0))

            parts_long.append(
                f"Расход {fmt_int(mv['SPENT'])} ₽ {op_to_human(op)} {fmt_int(value)} ₽. {period_to_label(period)}"
            )
            parts_short.append(
                f"Расход {fmt_int(mv['SPENT'])} {op_to_human(op)} {fmt_int(value)}"
            )

        elif ctype == "INCOME":
            period = cond.get("period") or {"type": "ALL_TIME"}
            income = income_store.income_for_period(banner_id, period)
            income_i = fmt_int(income)

            mode = (cond.get("mode") or "HAS").upper()

            if mode in ("HAS", "EXISTS"):
                parts_long.append(f"Доход есть ({income_i} ₽). {period_to_label(period)}")
                parts_short.append(f"Доход {income_i} > 0")

            elif mode in ("HAS_NOT", "NOT_HAS", "NOT", "NONE", "NO", "EMPTY", "ZERO"):
                parts_long.append(f"Доход отсутствует ({income_i} ₽). {period_to_label(period)}")
                parts_short.append(f"Доход {income_i} = 0")

            elif mode == "COMPARE":
                op = (cond.get("op") or "").upper().strip()
                value = safe_float(cond.get("valueRub", cond.get("value", 0)))
                parts_long.append(f"Доход {income_i} ₽ {op_to_human(op)} {fmt_int(value)} ₽. {period_to_label(period)}")
                parts_short.append(f"Доход {income_i} {op_to_human(op)} {fmt_int(value)}")

            elif mode == "COMPARE_SPEND":
                op = (cond.get("op") or "").upper().strip()
                threshold = safe_float(cond.get("multiplier", 0))

                spend_period = cond.get("spendPeriod") or {"type": "ALL_TIME"}
                spend_key = json.dumps(spend_period, sort_keys=True, ensure_ascii=False)
                spend_stats = stats_by_period.get(spend_key, {}).get(banner_id, {}) or {}
                spend = metric_value_from_stats(spend_stats)["SPENT"]

                delta = income - spend

                parts_long.append(
                    f"Δ(доход-расход) {fmt_int(delta)} ₽ {op_to_human(op)} {fmt_int(threshold)} ₽. "
                    f"Доход: {period_to_label(period)}; Расход: {period_to_label(spend_period)}"
                )
                parts_short.append(f"Δ {fmt_int(delta)} {op_to_human(op)} {fmt_int(threshold)}")

            else:
                # неизвестный режим
                parts_long.append(f"Доход {income_i} ₽. {period_to_label(period)}")
                parts_short.append(f"Доход {income_i}")

        elif ctype == "TARGET_ACTION":
            target = (cond.get("target") or "").strip()
            gid = int((banner_obj or {}).get("ad_group_id") or 0)
            actual = banner_target_action_from_groups(gid, banner_objectives)

            parts_long.append(f"Цель {actual} = {target}")
            parts_short.append(f"TA {actual}")

    return "; ".join(parts_long).strip(), "; ".join(parts_short).strip()

def eval_cost_rule(
    rule: Dict[str, Any],
    banner_id: int,
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
) -> Tuple[bool, str, str]:
    if not isinstance(rule, dict):
        return False, "", ""
    if (rule.get("type") or "").upper() != "COST_RULE":
        return False, "", ""

    spent_rub = safe_float(rule.get("spentRub", 0))
    metric = (rule.get("metric") or "").upper()
    op = (rule.get("op") or "EQ").upper()
    value = safe_float(rule.get("value", rule.get("valueRub", 0)))

    period = rule.get("period") or {"type": "ALL_TIME"}
    key = json.dumps(period, sort_keys=True, ensure_ascii=False)
    stats = stats_by_period.get(key, {}).get(banner_id, {}) or {}
    mv = metric_value_from_stats(stats)

    if mv["SPENT"] < spent_rub:
        return False, "", ""

    if metric not in mv:
        return False, "", ""

    actual = float(mv[metric])
    ok = op_compare(actual, op, value)
    if not ok:
        return False, "", ""

    reason = (
        f"{metric_to_human(metric)} {op_to_human(op)} {fmt_int(value)} "
        f"при расходе ≥ {fmt_int(spent_rub)}. {period_to_label(period)}."
    )
    
    short_reason = (
        f"{metric_to_human(metric)} {fmt_int(actual)} {op_to_human(op)} {fmt_int(value)}"
    )

    return True, reason, short_reason


def eval_filter_node(
    node: Dict[str, Any],
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
    banner_objectives: Dict[int, str],
) -> Tuple[str, str, str, bool]:
    """
    Возвращает:
      state: DISABLE / ENABLE / NOOP
      reason: длинная причина (для истории)
      short_reason: короткая причина (для TG)
      matched_action: True если был "осознанно сматчен" action в этом узле (включая NOOP),
                      False если это просто "ничего не нашли" и нужно искать дальше по дереву/шаблонам.
    """
    if not isinstance(node, dict):
        return "NOOP", "", "", False

    ntype = (node.get("type") or "").upper()

    # Если вдруг в дереве встретится не FILTER — спускаемся в child (как и раньше)
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

    # считаем срабатывания COST_RULE
    # ВАЖНО: RESULT_COST имеет приоритет над CLICK_COST
    # Если есть результаты и RESULT_COST проходит по значению — CLICK_COST игнорируется
    rule_hits: List[Tuple[bool, str, str]] = []
    
    # Получаем статистику для проверки наличия результатов
    # Берём период из первого RESULT_COST правила или ALL_TIME
    result_cost_value_ok = False
    for r in rules:
        if isinstance(r, dict) and (r.get("type") or "").upper() == "COST_RULE":
            metric = (r.get("metric") or "").upper()
            if metric in ("RESULT_COST", "CPA"):
                period = r.get("period") or {"type": "ALL_TIME"}
                key = json.dumps(period, sort_keys=True, ensure_ascii=False)
                stats = stats_by_period.get(key, {}).get(banner_id, {}) or {}
                mv = metric_value_from_stats(stats)
                
                # Если есть результаты (goals > 0)
                if mv.get("RESULTS", 0) > 0:
                    op = (r.get("op") or "EQ").upper()
                    value = safe_float(r.get("value", r.get("valueRub", 0)))
                    actual_result_cost = mv.get("RESULT_COST", 0)
                    # Проверяем только значение RESULT_COST, без учёта spentRub
                    if op_compare(actual_result_cost, op, value):
                        result_cost_value_ok = True
                        break
    
    # Теперь оцениваем все правила с учётом приоритета RESULT_COST
    for r in rules:
        if isinstance(r, dict) and (r.get("type") or "").upper() == "COST_RULE":
            metric = (r.get("metric") or "").upper()
            # Если RESULT_COST в норме — пропускаем проверку CLICK_COST (считаем её пройденной)
            if result_cost_value_ok and metric in ("CLICK_COST", "CPC"):
                rule_hits.append((True, "", ""))  # CLICK_COST автоматически проходит
            else:
                rule_hits.append(eval_cost_rule(r, banner_id, stats_by_period))
        else:
            rule_hits.append((False, "", ""))

    hit_bools = [x[0] for x in rule_hits]

    # ВАЖНО:
    # Если rules пустые, matched НЕ должен становиться True "сам по себе",
    # иначе пустой FILTER с action:NOOP станет вечным стоп-краном.
    if not hit_bools:
        matched = False
    elif mode == "ANY":
        matched = any(hit_bools)
    else:
        matched = all(hit_bools)

    if matched:
        reasons = [x[1] for x in rule_hits if x[0] and x[1]]
        short_reasons = [x[2] for x in rule_hits if x[0] and x[2]]

        if mode == "ANY":
            reason = reasons[0] if reasons else ""
            short_reason = short_reasons[0] if short_reasons else ""
        else:
            reason = "; ".join(reasons) if reasons else ""
            short_reason = "; ".join(short_reasons) if short_reasons else ""

        action = node.get("action") or {}
        if isinstance(action, dict) and (action.get("type") or "").upper() == "SET_STATE":
            state = (action.get("state") or "NOOP").upper()
            if state in ("DISABLE", "ENABLE", "NOOP"):
                # matched_action=True => это осознанное решение (в т.ч. NOOP)
                return state, reason, short_reason, True

        # если action невалидный/нет action — считаем, что решения нет
        return "NOOP", "", "", False

    # если не matched — идём в child
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
) -> Tuple[str, str, str]:
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

        # root conditions
        conditions = root.get("conditions") or []
        root_reason = ""
        root_short = ""

        if isinstance(conditions, list) and conditions:
            if not eval_conditions(conditions, banner_id, banner_obj, stats_by_period, income_store, banner_objectives):
                continue

            root_reason, root_short = conditions_to_reason(
                conditions=conditions,
                banner_id=banner_id,
                banner_obj=banner_obj,
                stats_by_period=stats_by_period,
                income_store=income_store,
                banner_objectives=banner_objectives,
            )

        child = root.get("child") or {}
        root_conditions_passed = bool(isinstance(conditions, list) and conditions)
        
        if root_conditions_passed and isinstance(child, dict) and (child.get("type") or "").upper() == "FILTER":
            rules0 = child.get("rules") or []
            if not isinstance(rules0, list):
                rules0 = []
            action0 = child.get("action") or {}
            if (not rules0) and isinstance(action0, dict) and (action0.get("type") or "").upper() == "SET_STATE":
                direct_state = (action0.get("state") or "NOOP").upper()
                if direct_state in ("DISABLE", "ENABLE", "NOOP"):
                    reason = root_reason or "Условия ROOT выполнены"
                    short_reason = root_short
        
                    tpl_name = str(tpl.get("name") or tpl.get("id") or "").strip()
                    if tpl_name and reason:
                        reason = f"[{tpl_name}] {reason}".strip()
        
                    return direct_state, reason, short_reason
        state, reason, short_reason, matched_action = eval_filter_node(
            child, banner_id, banner_obj, stats_by_period, income_store, banner_objectives
        )

        # если фильтр реально принял решение (в т.ч. NOOP) — это терминально
        if matched_action:
            if not reason and root_reason:
                reason = root_reason
            if not short_reason and root_short:
                short_reason = root_short

            tpl_name = str(tpl.get("name") or tpl.get("id") or "").strip()
            if tpl_name and reason:
                reason = f"[{tpl_name}] {reason}".strip()

            # ВАЖНО: state=NOOP => "не трогать", т.е. останавливаемся и не даём нижним приоритетам трогать
            if state.upper() == "NOOP":
                return "NOOP", reason, short_reason

            if state.upper() in ("DISABLE", "ENABLE"):
                return state.upper(), reason, short_reason

            # на всякий случай
            return "NOOP", reason, short_reason

        # matched_action=False => этот шаблон не дал решения, смотрим следующий template (по приоритету)
        continue

    return "NOOP", "", ""

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

def notify_state_path(users_root: str, tg_id: str, cabinet_id: str) -> pathlib.Path:
    p = pathlib.Path(users_root) / str(tg_id) / str(cabinet_id)
    ensure_dir(p)
    return p / "notify_state.json"


def load_last_notify_utc(path: pathlib.Path) -> Optional[dt.datetime]:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        s = str(data.get("last_notify_utc") or "").strip()
        if not s:
            return None
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def save_last_notify_utc(path: pathlib.Path, when_utc: dt.datetime) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"last_notify_utc": when_utc.strftime("%Y-%m-%d %H:%M:%S")},
                      f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка записи notify_state {path}: {e}")


def parse_history_daytime_to_utc(daytime_str: str) -> Optional[dt.datetime]:
    # history хранит daytime в UTC+4
    try:
        local_dt = dt.datetime.strptime(daytime_str, "%Y-%m-%d %H:%M:%S")
        return local_dt - dt.timedelta(hours=4)
    except Exception:
        return None


def read_history_events_since(history_path: pathlib.Path, since_utc: Optional[dt.datetime]) -> List[Dict[str, Any]]:
    if not history_path.exists():
        return []
    try:
        with open(history_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, list):
            return []

        out: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            s = str(item.get("daytime") or "").strip()
            if not s:
                continue
            t_utc = parse_history_daytime_to_utc(s)
            if not t_utc:
                continue
            if since_utc is None or t_utc > since_utc:
                out.append(item)
        return out
    except Exception as e:
        logger.error(f"Ошибка чтения history {history_path}: {e}")
        return []


def is_due_to_send(last_notify_utc: Optional[dt.datetime], every_min: Optional[int]) -> bool:
    if not every_min or int(every_min) <= 0:
        return True
    if last_notify_utc is None:
        return True

    now_utc = dt.datetime.utcnow()
    if last_notify_utc > now_utc + dt.timedelta(minutes=1):
        return True

    return (now_utc - last_notify_utc).total_seconds() >= int(every_min) * 60


def reduce_latest_per_banner(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    last: Dict[str, Dict[str, Any]] = {}
    for e in events:
        bid = str(e.get("id_banner") or "").strip()
        if bid:
            last[bid] = e

    def event_utc(e: Dict[str, Any]) -> float:
        t = parse_history_daytime_to_utc(str(e.get("daytime") or ""))
        return t.timestamp() if t else 0.0

    return sorted(last.values(), key=event_utc)

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
    short_reason: str,
    income: float,
) -> Dict[str, str]:
    mv = metric_value_from_stats(stats_all_time or {})
    return {
        "daytime": now_str(),
        "id_banner": str(banner_id),
        "name_banner": name or "",
        "url": url or "",
        "reason": reason or "",
        "short_reason": short_reason or "",
        "status": status,
        "checker_enabled": checker_enabled,
        "income": fmt_int(income),
        "spent_all_time": fmt_int(mv["SPENT"]),
        "goals_all_time": fmt_int(mv["RESULTS"]),
        "cpa_all_time": fmt_int(mv["RESULT_COST"]),
        "clicks_all_time": fmt_int(mv["CLICKS"]),
        "cpc_all_time": fmt_int(mv["CLICK_COST"]),
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

def load_user_listfile(users_root: str, tg_id: str, filename: str) -> Dict[str, List[str]]:
    """
    white_list.json / black_list.json
    Формат:
    {
      "campaign_ids": ["123", "456"],
      "banner_ids": ["111", "222"]
    }
    Файла может не быть / может быть пустым.
    """
    path = pathlib.Path(users_root) / str(tg_id) / filename
    if not path.exists():
        return {"campaign_ids": [], "banner_ids": []}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"campaign_ids": [], "banner_ids": []}
        cids = data.get("campaign_ids") or []
        bids = data.get("banner_ids") or []
        if not isinstance(cids, list):
            cids = []
        if not isinstance(bids, list):
            bids = []
        return {
            "campaign_ids": [str(x).strip() for x in cids if str(x).strip()],
            "banner_ids": [str(x).strip() for x in bids if str(x).strip()],
        }
    except Exception as e:
        logger.error(f"Ошибка чтения {path}: {e}")
        return {"campaign_ids": [], "banner_ids": []}

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
            # обычные period
            if "period" in obj:
                add_period(obj.get("period"))

            # ВАЖНО: spendPeriod для COMPARE_SPEND
            if "spendPeriod" in obj:
                add_period(obj.get("spendPeriod"))

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
        out[key] = {}

        dr = daterange_from_period(period)

        for chunk in chunked(banner_ids, 200):
            if dr is None:
                part = api.stats_summary_banners(chunk)
            else:
                date_from, date_to = dr
                part = api.stats_day_banners(chunk, date_from, date_to)

            # аккуратно мержим
            out[key].update(part)

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
    tg_notify_enabled: bool,
    tg_notify_every_min: Optional[int],
    limit_disabled_banners_20: bool,
    only_spent_all_time_lte_5000: bool,
    white_list: Dict[str, List[str]],
    black_list: Dict[str, List[str]],
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
    # --- WHITE / BLACK LIST ---
    white_campaign_ids = [int(x) for x in (white_list.get("campaign_ids") or []) if str(x).isdigit()]
    white_banner_ids_direct = [int(x) for x in (white_list.get("banner_ids") or []) if str(x).isdigit()]
    
    black_campaign_ids = [int(x) for x in (black_list.get("campaign_ids") or []) if str(x).isdigit()]
    black_banner_ids_direct = [int(x) for x in (black_list.get("banner_ids") or []) if str(x).isdigit()]
    
    whitelist_set: Optional[set[int]] = None
    blacklist_set: set[int] = set(black_banner_ids_direct)
    
    # black campaigns -> group ids -> banners
    if black_campaign_ids:
        g_ids_black = api.fetch_group_ids_from_campaigns(black_campaign_ids)
        b_ids_black = api.fetch_banner_ids_from_groups(g_ids_black)
        blacklist_set.update(b_ids_black)
    
    # white campaigns -> group ids -> banners
    if white_campaign_ids:
        g_ids_white = api.fetch_group_ids_from_campaigns(white_campaign_ids)
        b_ids_white = api.fetch_banner_ids_from_groups(g_ids_white)
        whitelist_set = set(b_ids_white)
    
    # white direct banners добавляем
    if white_banner_ids_direct:
        if whitelist_set is None:
            whitelist_set = set()
        whitelist_set.update(white_banner_ids_direct)
    
    # Если whitelist существует, но пустой — тогда просто "ничего не трогать"
    if whitelist_set is not None and len(whitelist_set) == 0:
        logger.info("WHITE_LIST задан, но пустой => не трогаем ничего")
        return
    # Важно: цель (TARGET_ACTION) берём по ad_groups objective
    group_ids: List[int] = []
    for bid in all_ids:
        info = api.banner_info_cache.get(bid, {}) or {}
        gid = info.get("ad_group_id")
        try:
            if gid is not None:
                group_ids.append(int(gid))
        except Exception:
            pass
        
    group_objectives = api.build_groups_objective_cache(group_ids)

    periods = collect_periods_from_filters(templates)
    stats_by_period = build_stats_cache(api, all_ids, periods)
    stats_all_key = json.dumps({"type": "ALL_TIME"}, sort_keys=True, ensure_ascii=False)
    stats_all_map = stats_by_period.get(stats_all_key, {}) or {}

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
    effective_max_disables = max_disables
    if limit_disabled_banners_20:
        effective_max_disables = min(int(max_disables), 20)
    else:
        effective_max_disables = 10**9
    notify_disabled: List[str] = []
    notify_enabled: List[str] = []

    # 1) DISABLE для активных
    for bid in active_ids:
        if ignore_manual_enabled_ads and str(bid) in disabled_records:
            logger.info(f"▶ Пропускаем баннер {bid}: already in disabled_banners.json и ignore_manual_enabled_ads=true")
            continue
            
        # --- whitelist/blacklist ---
        if whitelist_set is not None and bid not in whitelist_set:
            logger.info(f"▶ Пропускаем баннер {bid}: не в white_list")
            continue
        if bid in blacklist_set:
            logger.info(f"▶ Пропускаем баннер {bid}: в black_list")
            continue
        
        # --- spent_all_time <= 5000 rule ---
        if only_spent_all_time_lte_5000:
            s_all = stats_all_map.get(bid, {}) or {}
            mv_all = metric_value_from_stats(s_all)
            if mv_all["SPENT"] > 5000.0:
                logger.info(f"▶ Пропускаем баннер {bid}: spent_all_time={mv_all['SPENT']:.2f} > 5000 (only_spent_all_time_lte_5000=true)")
                continue
                
        gid = int((api.banner_info_cache.get(bid, {}) or {}).get("ad_group_id") or 0)
        ta = banner_target_action_from_groups(gid, group_objectives)
        log_banner_stats(
            banner_id=bid,
            periods=periods,
            stats_by_period=stats_by_period,
            income_store=income_store,
            target_action=ta,
        )
        
        gid = int((api.banner_info_cache.get(bid, {}) or {}).get("ad_group_id") or 0)
        bobj = {**(active_by_id.get(bid, {}) or {}), "ad_group_id": gid}

        state, reason, short_reason = decide_action_for_banner(
            templates=templates,
            cabinet_id=cabinet_id,
            banner_id=bid,
            banner_obj=bobj,
            stats_by_period=stats_by_period,
            income_store=income_store,
            banner_objectives=group_objectives,
        )

        if state != "DISABLE":
            continue

        if disabled_count >= effective_max_disables:
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
        income_all = income_store.income_for_period(bid, {"type": "ALL_TIME"})
        
        rec = make_banner_record(
            bid, name, url, stats_all,
            status="off",
            checker_enabled="on",
            reason=reason or "Отключено фильтром",
            short_reason=short_reason or "",
            income=income_all,
        )
        
        disabled_records[str(bid)] = rec
        enabled_records.pop(str(bid), None)
        append_history(his_path, rec)

        mv = metric_value_from_stats(stats_all)
        income_all = income_store.income_for_period(bid, {"type": "ALL_TIME"})

    # 2) ENABLE для blocked (только те, что мы отключали)
    for bid in blocked_ids:
        if str(bid) not in disabled_records:
            continue
            
        # --- whitelist/blacklist ---
        if whitelist_set is not None and bid not in whitelist_set:
            logger.info(f"▶ Пропускаем баннер {bid}: не в white_list")
            continue
        if bid in blacklist_set:
            logger.info(f"▶ Пропускаем баннер {bid}: в black_list")
            continue
        
        # --- spent_all_time <= 5000 rule ---
        if only_spent_all_time_lte_5000:
            s_all = stats_all_map.get(bid, {}) or {}
            mv_all = metric_value_from_stats(s_all)
            if mv_all["SPENT"] > 5000.0:
                logger.info(f"▶ Пропускаем баннер {bid}: spent_all_time={mv_all['SPENT']:.2f} > 5000 (only_spent_all_time_lte_5000=true)")
                continue
                
        gid = int((api.banner_info_cache.get(bid, {}) or {}).get("ad_group_id") or 0)
        ta = banner_target_action_from_groups(gid, group_objectives)
        log_banner_stats(
            banner_id=bid,
            periods=periods,
            stats_by_period=stats_by_period,
            income_store=income_store,
            target_action=ta,
        )
        
        gid = int((api.banner_info_cache.get(bid, {}) or {}).get("ad_group_id") or 0)
        bobj = {**(blocked_by_id.get(bid, {}) or {}), "ad_group_id": gid}

        state, reason, short_reason = decide_action_for_banner(
            templates=templates,
            cabinet_id=cabinet_id,
            banner_id=bid,
            banner_obj=bobj,
            stats_by_period=stats_by_period,
            income_store=income_store,
            banner_objectives=group_objectives,
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
        income_all = income_store.income_for_period(bid, {"type": "ALL_TIME"})
        
        rec = make_banner_record(
            bid, name, url, stats_all,
            status="on",
            checker_enabled="on",
            reason=reason or "Включено фильтром",
            short_reason=short_reason or "",
            income=income_all,
        )
        
        # переносим между списками
        disabled_records.pop(str(bid), None)
        enabled_records[str(bid)] = rec
        
        # история: только дописываем
        append_history(his_path, rec)
        mv = metric_value_from_stats(stats_all)
        

    save_disabled_records(dis_path, disabled_records)
    save_disabled_records(en_path, enabled_records)
    
    logger.info(f"💾 disabled_banners.json: {dis_path} (records={len(disabled_records)})")
    logger.info(f"💾 enabled_banners.json:  {en_path} (records={len(enabled_records)})")
    logger.info(f"🧾 history_banners.json:  {his_path}")

    # --- TG notify (batch by history since last send) ---
    if tg_notify_enabled and tg_bot_token and chat_id:
        state_path = notify_state_path(users_root, tg_id, cabinet_id)
        last_notify_utc = load_last_notify_utc(state_path)

        if is_due_to_send(last_notify_utc, tg_notify_every_min):
            events = read_history_events_since(his_path, last_notify_utc)
            events = reduce_latest_per_banner(events)

            if events:
                off = [e for e in events if str(e.get("status")) == "off"]
                on_ = [e for e in events if str(e.get("status")) == "on"]

                parts: List[str] = [f"<b>[{cabinet_name}]</b>"]

                if off:
                    lines = []
                    for e in off:
                        lines.append(
                            f"<b>{e.get('name_banner','')}</b> #{e.get('id_banner','')}\n"
                            f"    ⤷ Причина: {e.get('short_reason','')}\n"
                            f"    ⤷ Потрачено: {e.get('spent_all_time','')} ₽\n"
                            f"    ⤷ Результат: {e.get('goals_all_time','')} | {e.get('cpa_all_time','')} ₽"
                        )
                    parts.append("<b>Отключены баннеры:</b>\n\n" + "\n\n".join(lines))

                if on_:
                    lines = []
                    for e in on_:
                        lines.append(
                            f"<b>{e.get('name_banner','')}</b> #{e.get('id_banner','')}\n"
                            f"    ⤷ Причина: {e.get('short_reason','')}\n"
                            f"    ⤷ Доход: {e.get('income','')} ₽\n"
                            f"    ⤷ Потрачено: {e.get('spent_all_time','')} ₽\n"
                            f"    ⤷ Результат: {e.get('goals_all_time','')} | {e.get('cpa_all_time','')} ₽"
                        )
                    parts.append("<b>Включены баннеры:</b>\n\n" + "\n\n".join(lines))

                tg_notify(tg_bot_token, chat_id, "\n\n".join(parts), dry_run=dry_run)
                save_last_notify_utc(state_path, dt.datetime.utcnow())
            else:
                logger.info("🔕 TG: нет новых событий с момента последней отправки")
        else:
            logger.info(f"🔕 TG throttle: уведомления не отправляем (tg_notify_every_min={tg_notify_every_min})")
    else:
        if not tg_notify_enabled:
            logger.info("🔕 TG уведомления отключены (tg_notify_enabled=false)")


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

            tg_notify_enabled = bool(settings.get("tg_notify_enabled", True))
            tg_notify_every_min = settings.get("tg_notify_every_min", None)
            try:
                tg_notify_every_min = int(tg_notify_every_min) if tg_notify_every_min is not None else None
            except Exception:
                tg_notify_every_min = None
            
            limit_disabled_banners_20 = bool(settings.get("limit_disabled_banners_20", True))
            only_spent_all_time_lte_5000 = bool(settings.get("only_spent_all_time_lte_5000", False))
            
            white_list = load_user_listfile(users_root, tg_id, "white_list.json")
            black_list = load_user_listfile(users_root, tg_id, "black_list.json")
            
            chat_id = str(tg_id)

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
                        tg_notify_enabled=tg_notify_enabled,
                        tg_notify_every_min=tg_notify_every_min,
                        limit_disabled_banners_20=limit_disabled_banners_20,
                        only_spent_all_time_lte_5000=only_spent_all_time_lte_5000,
                        white_list=white_list,
                        black_list=black_list,
                    )
                except Exception as e:
                    logger.exception(f"[USER {tg_id}] Ошибка обработки кабинета {cab.get('name') or cab.get('id')}: {e}")

        except Exception as e:
            logger.exception(f"Ошибка обработки пользователя {tg_id}: {e}")

    logger.info("Готово")


if __name__ == "__main__":
    main()
