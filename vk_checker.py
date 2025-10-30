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
MAX_DISABLES_PER_RUN = 10  # максимум баннеров, которые можно отключить за один запуск

DRY_RUN = True  #True для тестов, False для рабочего

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
        return (cond1 and cond2, "; ".join(reason) if reason else "")

# Описание кабинета
@dataclass
class AccountConfig:
    name: str
    token_env: str
    chat_id_env: str
    n_days: int = N_DAYS_DEFAULT
    n_all_time: bool = False
    flt: BaseFilter = field(default_factory=BaseFilter)
    # Разрешенные 
    allowed_campaigns: List[int] = field(default_factory=list)
    allowed_banners: List[int] = field(default_factory=list)
    # Исключения (по умолчанию пустые)
    exceptions_campaigns: List[int] = field(default_factory=list)
    exceptions_banners: List[int] = field(default_factory=list)
    # Дата создания баннера
    banner_date_create: Optional[str] = None

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
    #AccountConfig(
    #    name="MAIN",
    #    token_env="VK_TOKEN_MAIN",
    #    chat_id_env="TG_CHAT_ID_MAIN",
    #    n_days=2,
    #    n_all_time=True,
    #    flt=BaseFilter(),  # можно переопределять пороги per-account
    #    banner_date_create=None,
    #    allowed_campaigns=[],
    #    allowed_banners=[],
    #    exceptions_campaigns=[],
    #    exceptions_banners=[],
    #),
    AccountConfig(
        name="ОСНОВНОЙ",
        token_env="VK_TOKEN_ZEL_1",
        chat_id_env="TG_CHAT_ID_MAIN",
        n_days=2,
        n_all_time=True,
        flt=BaseFilter(min_spent_for_cpc=170, cpc_bad_value=100, min_spent_for_cpa=330, cpa_bad_value=300),
        banner_date_create=None,
        allowed_campaigns=[14739714,14739769,14739806,14740194,14740269,14741230,14741258,14741283,14741312,14741807,14741832,14741866,14741875,14741894,14741928,14741945,14741967,14741995],
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    )
    # AccountConfig(name="CLIENT1", token_env="VK_TOKEN_CLIENT1", chat_id_env="TG_CHAT_ID_CLIENT1", n_days=5,
    #               flt=BaseFilter(min_spent_for_cpc=60, cpc_bad_value=70, min_spent_for_cpa=250, cpa_bad_value=250)),
]

# ==========================
# Логирование
# ==========================
LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / "vk_checker.log"

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
            
            # 💡 Если VK API вернул лимит
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

    def add_banners_from_allowed_campaign(self, campaign_id: int, allowed_banners: List[int]) -> None:
        """
        Добавляет в список разрешённых баннеров все активные баннеры из указанной кампании.
        Делает два запроса:
          1) /api/v2/ad_plans/<id>.json?fields=ad_groups — получает все группы
          2) /api/v2/ad_groups/<group_id>.json?fields=banners — получает баннеры каждой группы
        """
        seen = set(allowed_banners)
        try:
            # 1️⃣ Получаем группы кампании
            time.sleep(0.5)  # ⏳ чтобы не спамить API
            url_plan = f"{self.base_url}/api/v2/ad_plans/{campaign_id}.json"
            resp_plan = req_with_retry(
                "GET",
                url_plan,
                headers=self.headers,
                params={"fields": "ad_groups"},
                timeout=STATS_TIMEOUT,
            )
            data_plan = resp_plan.json()
            ad_groups = data_plan.get("ad_groups", [])
            logger.info(f"Кампания {campaign_id}: получено групп {len(ad_groups)} (для allowed)")
    
            # 2️⃣ Для каждой группы запрашиваем баннеры
            added = 0
            for g in ad_groups:
                gid = g.get("id")
                if not gid:
                    continue
                
                time.sleep(0.4)  # ⏳ пауза между запросами групп (анти-лимит)
                url_group = f"{self.base_url}/api/v2/ad_groups/{gid}.json"
                resp_group = req_with_retry(
                    "GET",
                    url_group,
                    headers=self.headers,
                    params={"fields": "banners"},
                    timeout=STATS_TIMEOUT,
                )
                data_group = resp_group.json()
                banners = data_group.get("banners", [])
                for b in banners:
                    bid = int(b.get("id") or 0)
                    if bid and bid not in seen:
                        allowed_banners.append(bid)
                        seen.add(bid)
                        added += 1
                        # 💡 небольшая микрозадержка при массовом добавлении (если групп много)
                        if added % 10 == 0:
                            time.sleep(0.2)
    
            logger.info(f"Кампания {campaign_id}: добавлено баннеров в allowed {added}")
    
        except Exception as e:
            logger.error(f"Ошибка при добавлении баннеров из кампании {campaign_id} в allowed: {e}")


    def add_banners_from_campaign_to_exceptions(self, campaign_id: int, exceptions_banners: List[int]) -> None:
        """
        Добавляет в список исключений все активные баннеры из указанной кампании.
        Делает два запроса:
          1) /api/v2/ad_plans/<id>.json?fields=ad_groups — получает все группы
          2) /api/v2/ad_groups/<group_id>.json?fields=banners — получает баннеры каждой группы
        """
        seen = set(exceptions_banners)
        try:
            # 1️⃣ Получаем группы кампании
            url_plan = f"{self.base_url}/api/v2/ad_plans/{campaign_id}.json"
            resp_plan = req_with_retry(
                "GET",
                url_plan,
                headers=self.headers,
                params={"fields": "ad_groups"},
                timeout=STATS_TIMEOUT,
            )
            data_plan = resp_plan.json()
            ad_groups = data_plan.get("ad_groups", [])
            logger.info(f"Кампания {campaign_id}: получено групп {len(ad_groups)}")

            # 2️⃣ Для каждой группы запрашиваем баннеры
            added = 0
            for g in ad_groups:
                gid = g.get("id")
                if not gid:
                    continue
                url_group = f"{self.base_url}/api/v2/ad_groups/{gid}.json"
                resp_group = req_with_retry(
                    "GET",
                    url_group,
                    headers=self.headers,
                    params={"fields": "banners"},
                    timeout=STATS_TIMEOUT,
                )
                data_group = resp_group.json()
                banners = data_group.get("banners", [])
                for b in banners:
                    bid = int(b.get("id") or 0)
                    if bid and bid not in seen:
                        exceptions_banners.append(bid)
                        seen.add(bid)
                        added += 1
            logger.info(f"Кампания {campaign_id}: добавлено баннеров в исключения {added}")

        except Exception as e:
            logger.error(f"Ошибка при добавлении баннеров из кампании {campaign_id} в исключения: {e}")

    def get_banner_created(self, banner_id: int) -> Optional[dt.datetime]:
        """
        Получает дату создания баннера.
        GET /api/v2/banners/<id>.json?fields=created
        Добавлен автоповтор и пауза для обхода лимитов API.
        """
        url = f"{self.base_url}/api/v2/banners/{banner_id}.json"
        for attempt in range(1, 4):
            try:
                time.sleep(0.4)  # ⏳ пауза между запросами для снижения нагрузки
                resp = req_with_retry(
                    "GET",
                    url,
                    headers=self.headers,
                    params={"fields": "created"},
                    timeout=STATS_TIMEOUT,
                )
                if resp.status_code == 429:
                    logger.warning(f"⚠️ Rate limit при запросе created баннера {banner_id}, попытка {attempt}")
                    time.sleep(1.5 * attempt)
                    continue
                
                data = resp.json()
                created_str = data.get("created")
                if created_str:
                    # Пример: "2025-10-28 14:39:40"
                    return dt.datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                else:
                    logger.debug(f"Баннер {banner_id}: поле 'created' отсутствует в ответе")
                    return None
    
            except Exception as e:
                logger.warning(f"Не удалось получить дату создания баннера {banner_id}: {e}")
                time.sleep(1.0 * attempt)
        return None


    def get_banner_name(self, banner_id: int) -> str:
        #Получает имя баннера по его ID.
        #GET /api/v2/banners/<id>.json?fields=name
        url = f"{self.base_url}/api/v2/banners/{banner_id}.json"
        for attempt in range(1, 4):
            try:
                time.sleep(0.4)
                resp = req_with_retry(
                    "GET",
                    url,
                    headers=self.headers,
                    params={"fields": "name"},
                    timeout=STATS_TIMEOUT,
                )
                if resp.status_code == 429:
                    logger.warning(f"⚠️ Rate limit при запросе name баннера {banner_id}, попытка {attempt}")
                    time.sleep(1.5 * attempt)
                    continue
                
                data = resp.json()
                name = data.get("name", "")
                return name or ""
            except Exception as e:
                logger.warning(f"Не удалось получить имя баннера {banner_id}: {e}")
                time.sleep(1.0 * attempt)
        return ""

    
    # --- Отключение объявления (статус blocked) ---
    def disable_banner(self, banner_id: int) -> bool:
        if DRY_RUN:
            logger.warning(f"🧪 [DRY RUN] Баннер {banner_id} НЕ отключен (тестовый режим)")
            return True
            
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
    disabled_count = 0
    disabled_ids = []  # список для хранения отключённых баннеров
    notifications = []

    if acc.allowed_campaigns:
        for camp_id in acc.allowed_campaigns:
            api.add_banners_from_allowed_campaign(camp_id, acc.allowed_banners)
        logger.info(f"Итоговый список разрешённых баннеров: {len(acc.allowed_banners)}")
        
    # --- Если есть исключённые кампании, расширяем список исключённых баннеров ---
    if acc.exceptions_campaigns:
        for camp_id in acc.exceptions_campaigns:
            api.add_banners_from_campaign_to_exceptions(camp_id, acc.exceptions_banners)
        logger.info(f"Итоговый список исключённых баннеров: {len(acc.exceptions_banners)}")
        
    # 1) Список активных объявлений
    banners = api.list_active_banners()
    if not banners:
        logger.info("Активных объявлений не найдено")
        return
    banner_ids = [int(b["id"]) for b in banners if "id" in b]
    logger.info(f"Всего активных объявлений: {len(banner_ids)}")

    # 2) Статистика
    sum_map = api.stats_summary_banners(banner_ids)
    
    if acc.n_all_time:
        logger.info(f"Используется режим n_all_time=True — фильтрация по полной статистике")
        # Берем из summary данные
        period_map = {
            bid: {
                "spent": d.get("spent_all_time", 0.0),
                "cpc": d.get("cpc_all_time", 0.0),
                "vk.cpa": d.get("vk.cpa_all_time", 0.0),
            }
            for bid, d in sum_map.items()
        }
        date_from, date_to = None, None
    else:
        date_from, date_to = daterange_for_last_n_days(acc.n_days)
        period_map = api.stats_period_banners(banner_ids, date_from, date_to)


    # 4) Пройтись по объявлениям и применить логику
    for b in banners:
        bid = int(b["id"])
        agid = int(b.get("ad_group_id", 0) or 0)
        # --- Фильтр: разрешённые баннеры ---
        if acc.allowed_banners:
            if bid not in acc.allowed_banners:
                logger.info(f"▶ Пропускаем баннер {bid}: не входит в allowed_banners")
                continue

        # --- Фильтр по дате создания, если указан ---
        if acc.banner_date_create:
            try:
                dt_cutoff = dt.datetime.strptime(acc.banner_date_create, "%d.%m.%Y")
                created_at = api.get_banner_created(bid)
                if not created_at:
                    logger.warning(f"⚠️ Не удалось получить дату создания баннера {bid} — пропускаем на всякий случай")
                    continue
                if created_at.date() < dt_cutoff.date():
                    logger.info(f"▶ Пропускаем баннер {bid}: создан {created_at.date()}, до {dt_cutoff.date()}")
                    continue
            except Exception as e:
                logger.warning(f"Ошибка проверки даты создания баннера {bid}: {e}")
                continue


        spent_all_time = sum_map.get(bid, {}).get("spent_all_time", 0.0)
        period = period_map.get(bid, {})
        spent = float(period.get("spent", 0.0))
        cpc = float(period.get("cpc", 0.0))
        vk_cpa = float(period.get("vk.cpa", 0.0))

        # --- Исключения ---
        if bid in acc.exceptions_banners:
            logger.info(f"▶ Пропускаем баннер {bid}: ИСКЛЮЧЕНИЕ")
            continue
        if agid in acc.exceptions_campaigns:
            logger.info(f"▶ Пропускаем баннер {bid} (Кампания {agid}): ИСКЛЮЧЕНИЕ")
            continue
            
        logger.info(
                f"[BANNER {bid} | GROUP {agid}]:spent = {spent:.2f},cpc = {cpc:.2f},cpa = {vk_cpa:.2f}"
        )

        # Если объявление уже потратило больше порога — не трогаем
        if spent_all_time > SPENT_ALL_TIME_DONT_TOUCH_RUB:
            logger.info(
                f"▶ Пропускаем: spent_all_time>{SPENT_ALL_TIME_DONT_TOUCH_RUB} (не трогаем по правилу)"
            )
            continue

        # Проверка фильтра
        bad, reason = acc.flt.violates(spent=spent, cpc=cpc, vk_cpa=vk_cpa)
        if not bad:
            logger.info("✔ Прошёл фильтр — ОК")
            continue
            
        if disabled_count >= MAX_DISABLES_PER_RUN:
            logger.warning("🚨 Достигнут лимит отключений за запуск — дальнейшие баннеры не будут отключаться")
            break

        # Отключаем объяву
        logger.warning(f"✖ НЕ ПРОШЁЛ ФИЛЬТР: {reason}")
        disabled = api.disable_banner(bid)
        status_msg = "ОТКЛЮЧЕНО" if disabled else "НЕ УДАЛОСЬ ОТКЛЮЧИТЬ"
        logger.warning(f"⇒ {status_msg}")

        if disabled:
            disabled_count += 1
            disabled_ids.append(bid)
          
            # --- Копим уведомление ---
            reason_short = short_reason(spent, cpc, vk_cpa, acc.flt)
            banner_name = api.get_banner_name(bid) or "Без названия"
            notifications.append(
                f"<b>{banner_name}</b> #{bid}\n"
                f"    ⤷ {reason_short}\n"
                f"    ⤷ Потрачено = {spent:.2f} ₽\n"
                f"    ⤷ Цена результата = {vk_cpa:.2f} ₽ | Цена клика = {cpc:.2f} ₽"
            )
        # Уведомление в TG
        #reason_short = short_reason(spent, cpc, vk_cpa, acc.flt)
        #date_from_fmt, date_to_fmt = fmt_date(date_from), fmt_date(date_to)
        #banner_name = api.get_banner_name(bid) or "Без названия"
        #text = (
        #    f"<b>[{acc.name}]</b>\n"
        #    f"<b>Баннер \"{banner_name}\" #{bid}</b> — {status_msg}\n"
        #    f"Причина: {reason_short}\n\n"
        #    f"<b>Статистика:</b>\n"
        #    f"Потрачено за всё время = {spent_all_time:.2f} RUB\n"
        #    f"За период с {date_from_fmt} по {date_to_fmt}:\n"
        #    f"    - Потрачено = {spent:.2f}\n"
        #    f"    - Цена клика = {cpc:.2f}\n"
        #    f"    - Цена результата = {vk_cpa:.2f}"
        #)
        #tg_notify(bot_token=tg_token, chat_id=acc.chat_id, text=text)
  
    # --- Отправка ---
    if notifications:
        combined_text = f"<b>[{acc.name}]</b>\n<b>Отключены баннеры:</b>\n\n" + "\n\n".join(notifications)
        tg_notify(bot_token=tg_token, chat_id=acc.chat_id, text=combined_text)
        logger.info(f"Отправлено итоговое сообщение в TG с {len(notifications)} баннерами")
          
    if disabled_ids:
        # Формируем имя файла, например: logs/disabled_MAIN_2025-10-29.json
        backup_path = LOG_DIR / f"disabled_{acc.name}_{dt.date.today()}.json"
        try:
            # Если файл уже существует, подгружаем старые ID и дописываем
            if backup_path.exists():
                with open(backup_path, "r", encoding="utf-8") as f:
                    old_data = json.load(f)
                    if isinstance(old_data, list):
                        disabled_ids = list(set(old_data + disabled_ids))
            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(disabled_ids, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 Сохранены ID отключённых баннеров: {backup_path} (всего {len(disabled_ids)})")
        except Exception as e:
            logger.error(f"Ошибка сохранения списка отключённых баннеров: {e}")



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
