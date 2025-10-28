from __future__ import annotations

import os
import sys
import time
import json
import math
import logging
import pathlib
import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# ==========================
# Константы и настройки
# ==========================
BASE_URL = os.environ.get("VK_ADS_BASE_URL", "https://ads.vk.com")  # при необходимости переопределить в .env
STATS_TIMEOUT = 30
WRITE_TIMEOUT = 30
RETRY_COUNT = 3
RETRY_BACKOFF = 1.8

# Период для расчёта метрик фильтра (spent, cpc, vk.cpa)
N_DAYS_DEFAULT = 2  # Можно переопределить отдельно для каждого кабинета

# Порог "не трогать, если уже потратили":
SPENT_ALL_TIME_DONT_TOUCH_RUB = 2000

# Базовый фильтр согласно ТЗ
@dataclass
class BaseFilter:
    min_spent_for_cpc: float = 80.0
    cpc_bad_value: float = 80.0  # cpc == 0 или >= 80
    min_spent_for_cpa: float = 300.0
    cpa_bad_value: float = 300.0  # vk.cpa == 0 или >= 300

    def violates(self, spent: float, cpc: float, vk_cpa: float) -> Tuple[bool, str]:
        cond1 = (spent >= self.min_spent_for_cpc) and (cpc == 0 or cpc >= self.cpc_bad_value)
        cond2 = (spent >= self.min_spent_for_cpa) and (vk_cpa == 0 or vk_cpa >= self.cpa_bad_value)
        reason = []
        if cond1:
            reason.append(
                f"spent≥{self.min_spent_for_cpc} & (cpc==0 or cpc≥{self.cpc_bad_value}) => (spent={spent:.2f}, cpc={cpc:.2f})"
            )
        if cond2:
            reason.append(
                f"spent≥{self.min_spent_for_cpa} & (vk.cpa==0 or vk.cpa≥{self.cpa_bad_value}) => (spent={spent:.2f}, vk.cpa={vk_cpa:.2f})"
            )
        return (cond1 or cond2, "; ".join(reason) if reason else "")

# Описание кабинета
@dataclass
class AccountConfig:
    name: str
    token_env: str  # имя переменной окружения с VK токеном
    chat_id_env: str  # имя переменной окружения с TG chat id
    n_days: int = N_DAYS_DEFAULT
    flt: BaseFilter = field(default_factory=BaseFilter)

    @property
    def token(self) -> str:
        t = os.environ.get(self.token_env)
        if not t:
            raise RuntimeError(f"Не найден токен в .env: {self.token_env}")
        return t

    @property
    def chat_id(self) -> str:
        c = os.environ.get(self.chat_id_env)
        if not c:
            raise RuntimeError(f"Не найден chat id в .env: {self.chat_id_env}")
        return c

# Список ваших кабинетов (добавьте/измените по аналогии)
ACCOUNTS: List[AccountConfig] = [
    AccountConfig(
        name="MAIN",
        token_env="VK_TOKEN_MAIN",
        chat_id_env="TG_CHAT_ID_MAIN",
        n_days=2,
        flt=BaseFilter(),  # можно переопределять пороги per-account
    ),
    # AccountConfig(name="CLIENT1", token_env="VK_TOKEN_CLIENT1", chat_id_env="TG_CHAT_ID_CLIENT1", n_days=5,
    #               flt=BaseFilter(min_spent_for_cpc=60, cpc_bad_value=70, min_spent_for_cpa=250, cpa_bad_value=250)),
]

# ==========================
# Логирование
# ==========================
LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / "vk_ads_auto_killer.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("vk_ads_auto")

# ==========================
# Вспомогательные функции
# ==========================

def load_env() -> None:
    if not load_dotenv():
        logger.warning(".env не найден или не загружен — убедитесь, что файл существует")

def short_reason(spent: float, cpc: float, vk_cpa: float, flt: BaseFilter) -> str:
    """Возвращает простую текстовую причину"""
    cond_cpc = (spent >= flt.min_spent_for_cpc) and (cpc == 0 or cpc >= flt.cpc_bad_value)
    cond_cpa = (spent >= flt.min_spent_for_cpa) and (vk_cpa == 0 or vk_cpa >= flt.cpa_bad_value)
    if cond_cpc and cond_cpa:
        return "Дорогая цена клика и результата"
    elif cond_cpc:
        return "Дорогая цена клика"
    elif cond_cpa:
        return "Дорогой результат"
    return "—"
    

def fmt_date(d: str) -> str:
    """Преобразует дату YYYY-MM-DD → DD.MM"""
    try:
        dt_obj = dt.datetime.strptime(d, "%Y-%m-%d")
        return dt_obj.strftime("%d.%m")
    except Exception:
        return d

def req_with_retry(method: str, url: str, headers: Dict[str, str], params: Dict[str, Any] | None = None,
                   json_body: Dict[str, Any] | None = None, timeout: int = 30) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)
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


def tg_notify(bot_token: str, chat_id: str, text: str) -> None:
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


# ==========================
# VK ADS API обёртки (v2)
# ==========================

class VkAdsApi:
    def __init__(self, token: str, base_url: str = BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.headers = {
            # Проверьте схему авторизации в вашей инсталляции (Bearer/Token/кастомный заголовок)
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    # --- Список баннеров (объявлений) ---
    def list_active_banners(self, limit: int = 1000) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/v2/banners.json"
        offset = 0
        items: List[Dict[str, Any]] = []
        while True:
            params = {
                "limit": min(limit, 200),
                "offset": offset,
                "_status": "active",
                # Можно дополнительно ограничить группами: "_ad_group_status": "active",
            }
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = resp.json()
            batch = data.get("items", [])
            items.extend(batch)
            logger.info(f"Получено активных баннеров: +{len(batch)} (всего {len(items)})")
            if len(batch) < params["limit"]:
                break
            offset += params["limit"]
        return items

    # --- Статистика summary (за всё время) ---
    def stats_summary_banners(self, banner_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        if not banner_ids:
            return {}
        url = f"{self.base_url}/api/v2/statistics/banners/summary.json"
        params = {
            "id": ",".join(map(str, banner_ids)),
            "metrics": "base",
        }
        resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
        data = resp.json()
        result: Dict[int, Dict[str, Any]] = {}
        for it in data.get("items", []):
            _id = int(it.get("id"))
            total = it.get("total", {}) or {}
            base = total.get("base", {}) or {}
            vk = base.get("vk", {}) or {}
            result[_id] = {
                "spent_all_time": float(base.get("spent", 0) or 0),
                "cpc_all_time": float(base.get("cpc", 0) or 0),
                "vk.cpa_all_time": float(vk.get("cpa", 0) or 0),
            }
        return result

    # --- Статистика за период (day) с total ---
    def stats_period_banners(self, banner_ids: List[int], date_from: str, date_to: str) -> Dict[int, Dict[str, Any]]:
        if not banner_ids:
            return {}
        url = f"{self.base_url}/api/v2/statistics/banners/day.json"
        params = {
            "id": ",".join(map(str, banner_ids)),
            "date_from": date_from,
            "date_to": date_to,
            "metrics": "base",
        }
        resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
        data = resp.json()
        result: Dict[int, Dict[str, Any]] = {}
        for it in data.get("items", []):
            _id = int(it.get("id"))
            total = it.get("total", {}) or {}
            base = total.get("base", {}) or {}
            vk = base.get("vk", {}) or {}
            rows = it.get("rows", []) or []
            result[_id] = {
                "spent": float(base.get("spent", 0) or 0),
                "cpc": float(base.get("cpc", 0) or 0),
                "vk.cpa": float(vk.get("cpa", 0) or 0),
                "rows": rows,
            }
        return result

    # --- Статистика за ПОСЛЕДНИЕ 2 дня (поко-дневно) ---
    def stats_last_two_days(self, banner_ids: List[int]) -> Dict[int, List[Dict[str, Any]]]:
        today = dt.date.today()
        date_from = (today - dt.timedelta(days=2)).strftime("%Y-%m-%d")
        date_to = (today - dt.timedelta(days=1)).strftime("%Y-%m-%d")
        period = self.stats_period_banners(banner_ids, date_from, date_to)
        # Возвращаем только rows (подневка) в отдельной структуре
        two_days: Dict[int, List[Dict[str, Any]]] = {}
        for _id, d in period.items():
            two_days[_id] = d.get("rows", [])
        return two_days

    # --- Отключение объявления (статус blocked) ---
    def disable_banner(self, banner_id: int) -> bool:
        
        # Отключает объявление (меняет статус на blocked)
        # POST /api/v2/banners/<banner_id>.json
        
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
                logger.info(f"Баннер {banner_id} успешно отключен (HTTP 204)")
                return True
            logger.warning(f"Не удалось отключить баннер {banner_id}: {resp.status_code} {resp.text}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при отключении баннера {banner_id}: {e}")
            return False


# ==========================
# Основная логика
# ==========================

def daterange_for_last_n_days(n_days: int) -> Tuple[str, str]:
    today = dt.date.today()
    since = today - dt.timedelta(days=n_days)
    return since.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def process_account(acc: AccountConfig, tg_token: str) -> None:
    logger.info("=" * 80)
    logger.info(f"КАБИНЕТ: {acc.name} | n_days={acc.n_days}")
    api = VkAdsApi(token=acc.token)

    # 1) Список активных объявлений
    banners = api.list_active_banners()
    if not banners:
        logger.info("Активных объявлений не найдено")
        return
    banner_ids = [int(b["id"]) for b in banners if "id" in b]
    logger.info(f"Всего активных объявлений: {len(banner_ids)}")

    # 2) Стата за всё время
    sum_map = api.stats_summary_banners(banner_ids)

    # 3) Стата за N дней
    date_from, date_to = daterange_for_last_n_days(acc.n_days)
    period_map = api.stats_period_banners(banner_ids, date_from, date_to)

    # 4) Последние 2 дня подневно (в отдельную переменную)
    last2_map = api.stats_last_two_days(banner_ids)

    # 5) Пройтись по объявлениям и применить логику
    for b in banners:
        bid = int(b["id"])
        agid = int(b.get("ad_group_id", 0) or 0)
        spent_all_time = sum_map.get(bid, {}).get("spent_all_time", 0.0)
        period = period_map.get(bid, {})
        spent = float(period.get("spent", 0.0))
        cpc = float(period.get("cpc", 0.0))
        vk_cpa = float(period.get("vk.cpa", 0.0))
        last2_rows = last2_map.get(bid, [])

        logger.info(
            (
                f"[BANNER {bid} | GROUP {agid}]\n"
                f"    spent_all_time={spent_all_time:.2f} RUB\n"
                f"    period[{date_from}..{date_to}]: spent={spent:.2f}, cpc={cpc:.2f}, vk.cpa={vk_cpa:.2f}\n"
                f"    last_2_days_rows={json.dumps(last2_rows, ensure_ascii=False)}"
            )
        )

        # Если объявление уже потратило больше порога — не трогаем
        if spent_all_time > SPENT_ALL_TIME_DONT_TOUCH_RUB:
            logger.info(
                f"    ▶ Пропускаем: spent_all_time>{SPENT_ALL_TIME_DONT_TOUCH_RUB} (не трогаем по правилу)"
            )
            continue

        # Проверка фильтра
        bad, reason = acc.flt.violates(spent=spent, cpc=cpc, vk_cpa=vk_cpa)
        if not bad:
            logger.info("    ✔ Прошёл фильтр — ОК")
            continue

        # Отключаем объяву
        logger.warning(f"    ✖ НЕ ПРОШЁЛ ФИЛЬТР: {reason}")
        disabled = api.disable_banner(bid)
        status_msg = "ОТКЛЮЧЕНО" if disabled else "НЕ УДАЛОСЬ ОТКЛЮЧИТЬ"
        logger.warning(f"    ⇒ {status_msg}")

        # Уведомление в TG
        reason_short = short_reason(spent, cpc, vk_cpa, acc.flt)
        date_from_fmt, date_to_fmt = fmt_date(date_from), fmt_date(date_to)
        banner_name = b.get("name") or "Без названия"
        text = (
            f"<b>[{acc.name}] Баннер \"{banner_name}\" #{bid}</b> — {status_msg}\n"
            f"Причина: {reason_short}\n\n"
            f"<b>Статистика:</b>\n"
            f"Потрачено за всё время = {spent_all_time:.2f} RUB\n"
            f"За период с {date_from_fmt} по {date_to_fmt}:\n"
            f"  Потрачено = {spent:.2f}\n"
            f"  Цена клика = {cpc:.2f}\n"
            f"  Цена результата = {vk_cpa:.2f}"
        )
        tg_notify(bot_token=tg_token, chat_id=acc.chat_id, text=text)


# ==========================
# Точка входа
# ==========================

def main():
    load_env()

    tg_token = os.environ.get("TG_BOT_TOKEN")
    if not tg_token:
        raise RuntimeError("В .env должен быть TG_BOT_TOKEN")

    logger.info("Старт VK ADS авто-проверки/отключалки")

    for acc in ACCOUNTS:
        try:
            process_account(acc, tg_token)
        except Exception as e:
            logger.exception(f"Ошибка обработки кабинета {acc.name}: {e}")

    logger.info("Готово")


if __name__ == "__main__":
    main()
