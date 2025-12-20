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
VERSION = "-4.0.0-"
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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("vk_checker_v4")


# ============================================================
# Утилиты
# ============================================================
def load_env() -> None:
    # .env нужен только для TG_BOT_TOKEN и (опционально) VK_ADS_BASE_URL
    try:
        load_dotenv()
    except Exception:
        pass


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
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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

            # VK rate limit
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "3"))
                logger.warning(f"⚠️ VK API rate limit (429). Пауза {retry_after}s перед повтором...")
                time.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                raise requests.HTTPError(f"{resp.status_code} {resp.text}")

            return resp
        except Exception as e:
            last_exc = e
            sleep_for = RETRY_BACKOFF ** (attempt - 1)
            logger.warning(f"{method} {url} попытка {attempt}/{RETRY_COUNT} не удалась: {e}. Повтор через {sleep_for:.1f}s")
            time.sleep(sleep_for)

    assert last_exc is not None
    raise last_exc


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
# Income loader (из пути, который лежит в <tg_id>.json -> income_path)
# Формат ожидается как раньше:
# [
#   {"day": "20.12.2025", "data": {"123": 10.5, "124": 0}},
#   ...
# ]
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

        # неизвестный период — безопасно: как 0
        return 0.0


def load_income_store(path: str) -> IncomeStore:
    if not path or not os.path.exists(path):
        logger.warning(f"⚠️ Файл доходов {path} не найден — фильтр дохода фактически будет нулевой")
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

    def list_banners_by_status(self, status: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        status: "active" | "blocked" | ...
        """
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
                for k in ("name", "created", "url", "link_url", "destination_url"):
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
            self.fetch_banners_info([banner_id], fields="url,link_url,destination_url")
            info = self.banner_info_cache.get(banner_id, {})
        return (info.get("url") or info.get("link_url") or info.get("destination_url") or "").strip()

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

            # В разных кабинетах/версиях могут быть разные поля — достаём максимально мягко
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


# ============================================================
# Filters engine (парсим filters.json из UI)
# Схема (по zip/UI):
# templates: [{id, name, priority, root:{type:"ROOT", accountsScope:{mode:"ALL"|...}, conditions:[...], child:{type:"FILTER", mode:"ALL|ANY", rules:[...], action:{type:"SET_STATE", state:"DISABLE|ENABLE|NOOP"}, child:...}}]
#
# conditions:
#  - {type:"SPENT", period:{type:"ALL_TIME|TODAY|YESTERDAY|LAST_N_DAYS", n?}, op:"LT|LTE|EQ|GTE|GT", valueRub:number}
#  - {type:"INCOME", period:{...}, mode:"HAS"|... }  (поддерживаем HAS/NONE + опционально op/valueRub)
#  - {type:"TARGET_ACTION", target:"BOT_MESSAGE"|...}
#
# rules:
#  - {type:"COST_RULE", spentRub:number, metric:"RESULTS|CLICKS|RESULT_COST|CLICK_COST", op:"LT|LTE|EQ|GTE|GT", value:number}
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
    # неизвестный оператор — безопасно: false
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
    """
    Нормализуем в набор метрик, чтобы COST_RULE мог работать стабильно.
    """
    spent = safe_float(stats.get("spent", 0))
    clicks = safe_float(stats.get("clicks", 0))
    goals = safe_float(stats.get("goals", 0))
    cpc = safe_float(stats.get("cpc", 0))
    vk_cpa = safe_float(stats.get("vk.cpa", 0))

    # Подстраховки: если VK не вернул cost-метрики, можно посчитать
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


def extract_templates(filters_json: Any) -> List[Dict[str, Any]]:
    """
    Возвращает templates[] в максимально мягком режиме, чтобы не упасть на разной структуре.
    """
    if isinstance(filters_json, dict):
        tpls = filters_json.get("templates")
        if isinstance(tpls, list):
            return [t for t in tpls if isinstance(t, dict)]
        # иногда может быть один root
        if "root" in filters_json:
            return [filters_json]  # трактуем как "один шаблон"
    if isinstance(filters_json, list):
        # если уже список шаблонов
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
    # неизвестный mode — безопасно: не применяем
    return False


def banner_target_action(banner_obj: Dict[str, Any]) -> str:
    # Best-effort (VK разные поля может отдавать по-разному)
    for k in ("target", "target_action", "objective", "optimization_goal", "goal_type"):
        v = banner_obj.get(k)
        if isinstance(v, str) and v:
            return v
    return ""


def eval_conditions(
    conditions: List[Dict[str, Any]],
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
) -> bool:
    """
    Все условия интерпретируем как AND.
    """
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
                # если у режима есть op/valueRub — попробуем сравнить
                op = cond.get("op")
                if op:
                    value = safe_float(cond.get("valueRub", 0))
                    if not op_compare(income, op, value):
                        return False
                else:
                    # неизвестный режим без сравнения — безопасно: условие не пройдено
                    return False

        elif ctype == "TARGET_ACTION":
            target = (cond.get("target") or "").strip()
            if not target:
                continue
            actual = banner_target_action(banner_obj)
            if actual != target:
                return False

        else:
            # неизвестное условие — безопасно: не проходим
            return False

    return True


def eval_cost_rule(
    rule: Dict[str, Any],
    banner_id: int,
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
) -> bool:
    """
    COST_RULE: spentRub + (metric op value)
    """
    if not isinstance(rule, dict):
        return False
    if (rule.get("type") or "").upper() != "COST_RULE":
        return False

    spent_rub = safe_float(rule.get("spentRub", 0))
    metric = (rule.get("metric") or "").upper()
    op = (rule.get("op") or "EQ").upper()
    value = safe_float(rule.get("value", 0))

    # В COST_RULE в UI период, судя по коду, может отсутствовать => считаем по ALL_TIME
    period = rule.get("period") or {"type": "ALL_TIME"}
    key = json.dumps(period, sort_keys=True, ensure_ascii=False)
    stats = stats_by_period.get(key, {}).get(banner_id, {}) or {}
    mv = metric_value_from_stats(stats)

    if mv["SPENT"] < spent_rub:
        return False

    # Маппинг метрики
    if metric not in mv:
        # неизвестная метрика — безопасно: не матч
        return False

    return op_compare(mv[metric], op, value)


def eval_filter_node(
    node: Dict[str, Any],
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
) -> str:
    """
    Возвращает state: DISABLE / ENABLE / NOOP
    Логика: если node матчится, возвращаем его action.state, иначе идём в child.
    """
    if not isinstance(node, dict):
        return "NOOP"

    ntype = (node.get("type") or "").upper()
    if ntype != "FILTER":
        # если вдруг это ещё один ROOT/неизвестное — пытаемся провалиться в child
        child = node.get("child")
        return eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store)

    mode = (node.get("mode") or "ALL").upper()  # ALL=AND, ANY=OR
    rules = node.get("rules") or []
    if not isinstance(rules, list):
        rules = []

    # Поддержим также conditions внутри FILTER (на всякий случай)
    conditions = node.get("conditions") or []
    if isinstance(conditions, list) and conditions:
        if not eval_conditions(conditions, banner_id, banner_obj, stats_by_period, income_store):
            # не прошли условия фильтра — значит фильтр не матчится
            child = node.get("child")
            return eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store)

    rule_results: List[bool] = []
    for r in rules:
        if isinstance(r, dict) and (r.get("type") or "").upper() == "COST_RULE":
            rule_results.append(eval_cost_rule(r, banner_id, stats_by_period))
        else:
            # неизвестное правило — считаем false (чтобы случайно не матчить)
            rule_results.append(False)

    matched = False
    if not rule_results:
        matched = False
    elif mode == "ANY":
        matched = any(rule_results)
    else:
        matched = all(rule_results)

    if matched:
        action = node.get("action") or {}
        if isinstance(action, dict) and (action.get("type") or "").upper() == "SET_STATE":
            state = (action.get("state") or "NOOP").upper()
            if state in ("DISABLE", "ENABLE", "NOOP"):
                return state
        # если action неизвестный — NOOP
        return "NOOP"

    child = node.get("child")
    return eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store)


def decide_action_for_banner(
    templates: List[Dict[str, Any]],
    cabinet_id: str,
    banner_id: int,
    banner_obj: Dict[str, Any],
    stats_by_period: Dict[str, Dict[int, Dict[str, Any]]],
    income_store: IncomeStore,
) -> str:
    """
    templates сортируются по priority (меньше — выше).
    Для каждого template:
      - accountsScope должен позволять кабинет
      - root.conditions должны быть true
      - дальше идём в root.child и получаем state
    Берём первый state != NOOP
    """
    # сортировка по priority, fallback=9999
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
            if not eval_conditions(conditions, banner_id, banner_obj, stats_by_period, income_store):
                continue

        child = root.get("child") or {}
        state = eval_filter_node(child, banner_id, banner_obj, stats_by_period, income_store)
        if state and state.upper() != "NOOP":
            return state.upper()

    return "NOOP"


# ============================================================
# disabled_banners.json per cabinet
# Формат хранения: список объектов (каждый объект как вы просили).
# Upsert по id_banner.
# При ENABLE удаляем запись.
# ============================================================
def disabled_file_path(users_root: str, tg_id: str, cabinet_id: str) -> pathlib.Path:
    p = pathlib.Path(users_root) / str(tg_id) / str(cabinet_id)
    ensure_dir(p)
    return p / "disabled_banners.json"


def load_disabled_records(path: pathlib.Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            # если кто-то сохранил как dict — нормализуем
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
        # сохраняем как список объектов (по ТЗ)
        arr = list(records.values())
        with open(path, "w", encoding="utf-8") as f:
            json.dump(arr, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения {path}: {e}")


def make_disabled_record(
    banner_id: int,
    name: str,
    url: str,
    stats_all_time: Dict[str, Any],
) -> Dict[str, str]:
    mv = metric_value_from_stats(stats_all_time or {})
    # goals_all_time = RESULTS, clicks_all_time = CLICKS
    rec = {
        "daytime": now_str(),
        "id_banner": str(banner_id),
        "name_banner": name or "",
        "url": url or "",
        "spent_all_time": f"{mv['SPENT']:.2f}",
        "goals_all_time": f"{mv['RESULTS']:.0f}",
        "cpa_all_time": f"{mv['RESULT_COST']:.2f}",
        "clicks_all_time": f"{mv['CLICKS']:.0f}",
        "cpc_all_time": f"{mv['CLICK_COST']:.2f}",
    }
    return rec


# ============================================================
# User config loader
# /opt/vk_checker/v4/users/<tg_id>/<tg_id>.json
# ожидаем хотя бы:
# {
#   "tg_id": "...",
#   "chat_id": "...",
#   "income_path": "...",
#   "accounts": [{"id":"CAB1","name":"MAIN","token":"..."}]
# }
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
            name = child.name
            # tg id обычно цифры; но не будем жёстко ограничивать
            out.append(name)
    return sorted(out)


# ============================================================
# Сбор нужных периодов из filters.json
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

    # unique by json key
    uniq: Dict[str, Dict[str, Any]] = {}
    for p in periods:
        key = json.dumps(p, sort_keys=True, ensure_ascii=False)
        uniq[key] = p

    return list(uniq.values())


# ============================================================
# Processing one cabinet
# ============================================================
def build_stats_cache(
    api: VkAdsApi,
    banner_ids: List[int],
    periods: List[Dict[str, Any]],
) -> Dict[str, Dict[int, Dict[str, Any]]]:
    """
    Возвращает:
      key(period_json) -> {banner_id -> stats}
    где stats в формате metric_value_from_stats ожидает keys: spent, cpc, clicks, goals, vk.cpa
    """
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
) -> None:
    cabinet_id = str(cabinet.get("id") or "").strip()
    cabinet_name = str(cabinet.get("name") or cabinet_id or "CABINET").strip()
    token = str(cabinet.get("token") or "").strip()

    if not cabinet_id or not token:
        logger.warning(f"[{tg_id}] Пропуск кабинета: не хватает id/token. cabinet={cabinet}")
        return

    logger.info("=" * 80)
    logger.info(f"[USER {tg_id}] CABINET: {cabinet_name} (id={cabinet_id})")

    api = VkAdsApi(token=token, base_url=BASE_URL, dry_run=dry_run)

    # Загружаем список баннеров
    active_banners = api.list_banners_by_status("active")
    blocked_banners = api.list_banners_by_status("blocked")

    active_ids = [int(b.get("id")) for b in active_banners if b.get("id") is not None]
    blocked_ids = [int(b.get("id")) for b in blocked_banners if b.get("id") is not None]

    all_ids = sorted(set(active_ids + blocked_ids))
    if not all_ids:
        logger.info("Баннеров не найдено")
        return

    # Кешируем имя/created/url (по необходимости)
    api.fetch_banners_info(all_ids, fields="created,name,url,link_url,destination_url")

    # Какие периоды вообще нужны по filters.json
    periods = collect_periods_from_filters(templates)
    stats_by_period = build_stats_cache(api, all_ids, periods)

    # disabled_banners.json
    dis_path = disabled_file_path(users_root, tg_id, cabinet_id)
    disabled_records = load_disabled_records(dis_path)

    # Быстрый доступ к объекту баннера по id (active/blocked)
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

    # 1) Отключаем активные, если правило говорит DISABLE
    for bid in active_ids:
        bobj = active_by_id.get(bid, {})

        state = decide_action_for_banner(
            templates=templates,
            cabinet_id=cabinet_id,
            banner_id=bid,
            banner_obj=bobj,
            stats_by_period=stats_by_period,
            income_store=income_store,
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

        # upsert record
        rec = make_disabled_record(bid, name, url, stats_all)
        disabled_records[str(bid)] = rec

        mv = metric_value_from_stats(stats_all)
        income_all = income_store.income_for_period(bid, {"type": "ALL_TIME"})
        notify_disabled.append(
            f"<b>{name}</b> #{bid}\n"
            f"    ⤷ Потрачено(all) = {mv['SPENT']:.2f} ₽ | Доход(all) = {income_all:.2f} ₽\n"
            f"    ⤷ Результаты(all) = {mv['RESULTS']:.0f} | CPA(all) = {mv['RESULT_COST']:.2f} ₽ | CPC(all) = {mv['CLICK_COST']:.2f} ₽"
        )

    # 2) Включаем обратно заблокированные, если правило говорит ENABLE,
    #    но включаем ТОЛЬКО те, что есть в disabled_banners.json (то есть мы отключали их сами)
    for bid in blocked_ids:
        if str(bid) not in disabled_records:
            continue

        bobj = blocked_by_id.get(bid, {})
        state = decide_action_for_banner(
            templates=templates,
            cabinet_id=cabinet_id,
            banner_id=bid,
            banner_obj=bobj,
            stats_by_period=stats_by_period,
            income_store=income_store,
        )

        if state != "ENABLE":
            continue

        ok = api.enable_banner(bid)
        if not ok:
            continue

        name = api.get_banner_name(bid) or "Без названия"
        stats_all_key = json.dumps({"type": "ALL_TIME"}, sort_keys=True, ensure_ascii=False)
        stats_all = (stats_by_period.get(stats_all_key, {}) or {}).get(bid, {}) or {}
        mv = metric_value_from_stats(stats_all)
        income_all = income_store.income_for_period(bid, {"type": "ALL_TIME"})

        # remove record
        disabled_records.pop(str(bid), None)

        notify_enabled.append(
            f"<b>{name}</b> #{bid}\n"
            f"    ⤷ Потрачено(all) = {mv['SPENT']:.2f} ₽ | Доход(all) = {income_all:.2f} ₽\n"
            f"    ⤷ Результаты(all) = {mv['RESULTS']:.0f} | CPA(all) = {mv['RESULT_COST']:.2f} ₽ | CPC(all) = {mv['CLICK_COST']:.2f} ₽"
        )

    # сохраняем disabled_banners.json (в формате списка объектов)
    save_disabled_records(dis_path, disabled_records)
    logger.info(f"💾 disabled_banners.json обновлён: {dis_path} (records={len(disabled_records)})")

    # TG уведомления
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
    load_env()

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
        try:
            # config
            cfg = load_user_config(users_root, tg_id)
            chat_id = cfg.get("chat_id")
            if chat_id is not None:
                chat_id = str(chat_id)

            income_path = str(cfg.get("income_path") or "").strip()
            income_store = load_income_store(income_path) if income_path else IncomeStore(total={}, by_day={})

            # filters
            templates = load_user_filters(users_root, tg_id)
            if not templates:
                logger.info(f"[USER {tg_id}] Нет активных фильтров — пропуск (ничего делать не будем)")
                continue

            # cabinets
            accounts = cfg.get("accounts") or cfg.get("cabinets") or []
            if not isinstance(accounts, list) or not accounts:
                logger.warning(f"[USER {tg_id}] В конфиге нет accounts/cabinets — пропуск")
                continue

            for cab in accounts:
                if not isinstance(cab, dict):
                    continue
                # если есть active=false — пропускаем
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
                    )
                except Exception as e:
                    logger.exception(f"[USER {tg_id}] Ошибка обработки кабинета {cab.get('name') or cab.get('id')}: {e}")

        except Exception as e:
            logger.exception(f"Ошибка обработки пользователя {tg_id}: {e}")

    logger.info("Готово")


if __name__ == "__main__":
    main()
