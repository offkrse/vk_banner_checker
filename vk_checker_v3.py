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
VersionVKChecker = "--3.3--"
BASE_URL = os.environ.get("VK_ADS_BASE_URL", "https://ads.vk.com")  # при необходимости переопределить в .env
STATS_TIMEOUT = 30
WRITE_TIMEOUT = 30
RETRY_COUNT = 3
RETRY_BACKOFF = 1.8
MAX_DISABLES_PER_RUN = 15  # максимум баннеров, которые можно отключить за один запуск

DRY_RUN = False  #True для тестов, False для рабочего

# Период для расчёта метрик фильтра (spent, cpc, vk.cpa)
N_DAYS_DEFAULT = 2  # Можно переопределить отдельно для каждого кабинета

# ==========================
# Логирование
# ==========================
LOG_DIR = pathlib.Path("logs")
LOG_DIR.mkdir(exist_ok=True)
log_file = LOG_DIR / "vk_checker_v3.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger("vk_ads_auto")

# Базовый фильтр согласно ТЗ
@dataclass
class BaseFilter:
    spent_zero_result: float = 100.0     # Потрачено >= N и результатов = 0
    spent_zero_clicks: float = 50.0      # Потрачено >= N и кликов = 0
    min_spent_for_cpc: float = 80.0
    cpc_bad_value: float = 80.0  # cpc == 0 или >= 80
    min_spent_for_cpa: float = 300.0
    cpa_bad_value: float = 300.0  # vk.cpa == 0 или >= 300
    max_loss_rub: float = 2000.0  # потрачено больше дохода на N — отключаем

    def violates(self, spent: float, cpc: float, vk_cpa: float) -> Tuple[bool, str]:
        cond_cpc_bad = (spent >= self.min_spent_for_cpc) and (cpc >= self.cpc_bad_value)
        cond_cpa_bad = (spent >= self.min_spent_for_cpa) and (vk_cpa >= self.cpa_bad_value)
        reason = []
        # Приоритет логики:
        # 1 Если CPA плохой
        if cond_cpa_bad:
            return True, f"CPA плохой ({vk_cpa:.2f} < {self.cpa_bad_value})"
            
        # 2 Если CPC плохой, а CPA ещё не достиг минимального spent — тоже отключаем
        if cond_cpc_bad and spent < self.min_spent_for_cpa and vk_cpa == 0:
            return True, f"CPC плохой ({cpc:.2f} ≥ {self.cpc_bad_value}), а CPA ещё не достиг порога"

        # 3 Потрачено и нет результатов
        if spent >= self.spent_zero_result and vk_cpa == 0:
            return True, f"Нет результатов при потраченных {spent:.2f} ≥ {self.spent_zero_result}"

        # 4 Потрачено и нет кликов
        if spent >= self.spent_zero_clicks and cpc == 0:
            return True, f"Нет кликов при потраченных {spent:.2f} ≥ {self.spent_zero_clicks}"
        
        # Всё остальное — норм
        return False, "Все метрики в норме"

#Загрузка из списка
def load_campaigns(path: str) -> list[int]:
    campaigns = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip().strip(",")  # убираем пробелы и запятые
                if not line or line.startswith("#"):  # игнорируем пустые строки и комментарии
                    continue
                if line.isdigit():
                    campaigns.append(int(line))
                else:
                    logger.warning(f"⚠️ Некорректная строка в {path}: {line}")
    except FileNotFoundError:
        logger.warning(f"⚠️ Файл {path} не найден — список кампаний пуст - [0]")
        return [0]

    if not campaigns:
        logger.warning(f"⚠️ В файле {path} нет кампаний — возвращаем [0]")
        return [0]
    else:
        logger.info(f"✅ Загружено {len(campaigns)} кампаний из {path}")
        return campaigns

    
# Описание кабинета
@dataclass
class AccountConfig:
    # путь до JSON-файла пользователя
    user_json_path: Optional[str] = None

    # поля, которые можно переопределять вручную
    income_json_path: Optional[str] = None
    spent_all_time_dont_touch: float = 2000.0  # Порог "не трогать"
    allowed_banners: List[int] = field(default_factory=list)
    exceptions_campaigns: List[int] = field(default_factory=list)
    exceptions_banners: List[int] = field(default_factory=list)
    check_all_camp: bool = False

    # поля, подтягиваемые из JSON
    name: str = ""
    token_env: Optional[str] = None
    token: Optional[str] = None
    chat_id: Optional[str] = None
    n_days: int = N_DAYS_DEFAULT
    n_all_time: bool = True
    flt: BaseFilter = field(default_factory=BaseFilter)
    allowed_campaigns: List[int] = field(default_factory=list)
    banner_date_create: Optional[str] = None

    def __post_init__(self):
        """Если указан user_json_path — загрузить все данные из него"""
        if not self.user_json_path or not os.path.exists(self.user_json_path):
            logger.warning(f"⚠️ Не найден user_json_path: {self.user_json_path}")
            return

        try:
            with open(self.user_json_path, "r", encoding="utf-8") as f:
                user_data = json.load(f)
        except Exception as e:
            logger.error(f"Ошибка чтения {self.user_json_path}: {e}")
            return

        # общий chat_id
        chat_id = str(user_data.get("chat_id", "")) or None
        self.chat_id = chat_id

        # ищем нужный кабинет
        for cab in user_data.get("cabinets", []):
            if not cab.get("active", False):
                continue
            if cab.get("name") == self.name or cab.get("token_env") == self.token_env:
                self.name = cab.get("name", self.name)
                self.token_env = cab.get("token_env", self.token_env)

                # ✅ Токен из .env
                if self.token_env:
                    self.token = os.environ.get(self.token_env)
                    if not self.token:
                        logger.warning(f"⚠️ Не найден токен в .env: {self.token_env}")

                # кампании
                allowed_file = cab.get("allowed_campaigns_file")
                if allowed_file and os.path.exists(allowed_file):
                    self.allowed_campaigns = load_campaigns(allowed_file)
                else:
                    logger.warning(f"⚠️ Не найден файл кампаний для {self.name}: {allowed_file}")

                # фильтр
                flt_data = cab.get("filter", {})
                if isinstance(flt_data, dict):
                    self.flt = BaseFilter(**flt_data)

                # n_days / n_all_time
                self.n_days = cab.get("n_days", self.n_days)
                self.n_all_time = cab.get("n_all_time", self.n_all_time)
                break

        logger.info(f"✅ Кабинет [{self.name}] загружен из {self.user_json_path}")


# AccountConfig(name="CLIENT1", token_env="VK_TOKEN_CLIENT1", chat_id_env="TG_CHAT_ID_CLIENT1", n_days=5,
#flt=BaseFilter(min_spent_for_cpc=60, cpc_bad_value=70, min_spent_for_cpa=250, cpa_bad_value=250)),
ACCOUNTS: List[AccountConfig] = [
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1342381428.json",
        name="MAIN",
        check_all_camp=True,
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1550242935.json",
        name="Основной",
        income_json_path="/opt/leads_postback/data/krolik.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1550242935.json",
        name="Вадим-Зеленов ТМ1-5919",
        check_all_camp=True,
        income_json_path="/opt/leads_postback/data/krolik.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1550242935.json",
        name="Евгений Зеленов zk 5005 1",
        check_all_camp=True,
        income_json_path="/opt/leads_postback/data/krolik.json",
        spent_all_time_dont_touch=700,
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/6122996123.json",
        name="Роман Зеленов zk 5005 1",
        check_all_camp=True,
        income_json_path="/opt/leads_postback/data/krolik.json",
        spent_all_time_dont_touch=1000,
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
]


# ==========================
# Вспомогательные функции
# ==========================

def load_env() -> None:
    if not load_dotenv():
        logger.warning(".env не найден или не загружен — убедитесь, что файл существует")

#def short_reason(spent: float, cpc: float, vk_cpa: float, flt: BaseFilter) -> str:
#    """Возвращает простую текстовую причину"""
#    cond_cpc = (spent >= flt.min_spent_for_cpc) and (cpc == 0 or cpc >= flt.cpc_bad_value)
#    cond_cpa = (spent >= flt.min_spent_for_cpa) and (vk_cpa == 0 or vk_cpa >= flt.cpa_bad_value)
#    if cond_cpc and cond_cpa:
#        return "Дорогая цена клика и результата"
#    elif cond_cpc:
#        return "Дорогая цена клика"
#    elif cond_cpa:
#        return "Дорогой результат"
#    return "—"
    

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

def load_income_data(path: str) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Загружает JSON с доходами и:
    суммирует их по всем дням (income_total)
    отдельно суммирует доход за сегодня и вчера (income_recent)
    Возвращает (income_total, income_recent)
    """
    if not path or not os.path.exists(path):
        logger.warning(f"⚠️ Файл доходов {path} не найден — фильтр дохода отключён")
        return {}, {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        income_total: Dict[str, float] = {}
        income_recent: Dict[str, float] = {}

        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)
        target_days = {
            today.strftime("%d.%m.%Y"),
            yesterday.strftime("%d.%m.%Y"),
        }

        for entry in raw:
            day_str = entry.get("day")
            data = entry.get("data", {})
            if not isinstance(data, dict):
                continue

            # суммарный доход за всё время
            for bid, val in data.items():
                income_total[bid] = income_total.get(bid, 0.0) + float(val)

            # доход за сегодня/вчера
            if day_str in target_days:
                for bid, val in data.items():
                    income_recent[bid] = income_recent.get(bid, 0.0) + float(val)

        logger.info(
            f"✅ Загружены доходы: всего баннеров={len(income_total)}, "
            f"с доходом за сегодня/вчера={len(income_recent)} из {path}"
        )
        return income_total, income_recent

    except Exception as e:
        logger.error(f"Ошибка чтения доходов из {path}: {e}")
        return {}, {}


def tg_notify(bot_token: str, chat_id: str, text: str) -> None:
    if DRY_RUN:
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


# ==========================
# VK ADS API обёртки (v2)
# ==========================

class VkAdsApi:
    def __init__(self, token: str, base_url: str = BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        # кеш
        self.banner_info_cache: Dict[int, Dict[str, Any]] = {}


    def fetch_banners_info(self, banner_ids: List[int], fields: str = "created,name") -> None:
        """
        Массово подтягивает информацию о баннерах и кладёт в кеш.
        Делает /api/v2/banners.json?_id__in=...&fields=created,name,id пачками.
        """
        if not banner_ids:
            return

        # уникальные id и убираем те, что уже есть в кеше
        ids_to_fetch = [bid for bid in sorted(set(banner_ids)) if bid not in self.banner_info_cache]
        if not ids_to_fetch:
            return

        url = f"{self.base_url}/api/v2/banners.json"
        chunk_size = 200

        for i in range(0, len(ids_to_fetch), chunk_size):
            chunk = ids_to_fetch[i:i + chunk_size]
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
                except (TypeError, ValueError):
                    continue

                info = self.banner_info_cache.get(bid, {})
                name = it.get("name")
                created_str = it.get("created")

                if name is not None:
                    info["name"] = name
                if created_str is not None:
                    info["created"] = created_str
                    try:
                        info["created_dt"] = dt.datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        # если парсинг не удался — оставим как строку
                        pass

                self.banner_info_cache[bid] = info

            logger.info(
                f"Загружено метаданных баннеров: +{len(items)} (chunk {i // chunk_size + 1}/{math.ceil(len(ids_to_fetch)/chunk_size)})"
            )
    
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

    def enable_banner(self, banner_id: int) -> bool:
        """
        Включает баннер обратно (status=active).
        """
        if DRY_RUN:
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
                logger.info(f"↩ Баннер {banner_id} успешно включён (HTTP 204)")
                return True
            logger.warning(f"Не удалось включить баннер {banner_id}: {resp.status_code} {resp.text}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при включении баннера {banner_id}: {e}")
            return False

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

    def add_banners_from_allowed_campaigns_bulk(self, campaign_ids: List[int], allowed_banners: List[int]) -> None:
        """
        Добавляет в список разрешённых баннеров все активные баннеры из списка кампаний.
        Работает пакетно и постранично:
          1️⃣ /api/v2/ad_plans.json?_id__in=...&fields=ad_groups,name
          2️⃣ /api/v2/ad_groups.json?_id__in=...&fields=banners,name
        """
        if not campaign_ids:
            logger.warning("⚠️ Список campaign_ids пуст — ничего не добавлено в allowed_banners")
            return
    
        seen = set(allowed_banners)
        group_ids: list[int] = []
    
        # -------------------------------
        # 1️⃣ Получаем все группы по всем кампаниям (с постраничным выводом)
        # -------------------------------
        try:
            logger.info(f"Запрашиваем группы по {len(campaign_ids)} кампаниям (bulk, с пагинацией)...")
            limit = 200
            offset = 0
    
            while True:
                params = {
                    "_status": "active",
                    "_id__in": ",".join(map(str, campaign_ids)),
                    "fields": "ad_groups,name",
                    "limit": limit,
                    "offset": offset,
                }
                url_plans = f"{self.base_url}/api/v2/ad_plans.json"
                resp = req_with_retry("GET", url_plans, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
                data = resp.json()
                items = data.get("items", [])
                if not items:
                    break
                
                for plan in items:
                    groups = plan.get("ad_groups", [])
                    for g in groups:
                        gid = g.get("id")
                        if gid:
                            group_ids.append(int(gid))
    
                logger.info(f"Получено {len(items)} кампаний (offset={offset}), всего групп {len(group_ids)}")
    
                if len(items) < limit:
                    break
                offset += limit
    
            if not group_ids:
                logger.warning("⚠️ Группы не найдены — нечего добавлять в allowed_banners")
                return
    
        except Exception as e:
            logger.error(f"Ошибка при получении групп из кампаний: {e}")
            return
    
        # -------------------------------
        # 2️⃣ Получаем баннеры по всем группам (с постраничным выводом)
        # -------------------------------
        try:
            logger.info(f"Запрашиваем баннеры по {len(group_ids)} группам (bulk, с пагинацией)...")
            limit = 200
            offset = 0
            added = 0
    
            # делим список group_ids на порции по limit
            for i in range(0, len(group_ids), limit):
                chunk = group_ids[i:i + limit]
                params_groups = {
                    "_status": "active",
                    "_id__in": ",".join(map(str, chunk)),
                    "fields": "banners,name",
                    "limit": limit,
                }
                url_groups = f"{self.base_url}/api/v2/ad_groups.json"
                resp_groups = req_with_retry("GET", url_groups, headers=self.headers, params=params_groups, timeout=STATS_TIMEOUT)
                data_groups = resp_groups.json()
                group_items = data_groups.get("items", [])
    
                for g in group_items:
                    banners = g.get("banners", [])
                    for b in banners:
                        bid = int(b.get("id") or 0)
                        if bid and bid not in seen:
                            allowed_banners.append(bid)
                            seen.add(bid)
                            added += 1
    
                logger.info(f"Получено групп {len(group_items)} (chunk {i // limit + 1}), добавлено баннеров {added}")
    
            logger.info(f"✅ Добавлено {added} баннеров в allowed_banners (всего {len(allowed_banners)})")
    
        except Exception as e:
            logger.error(f"Ошибка при получении баннеров по группам: {e}")


    def get_banner_created(self, banner_id: int) -> Optional[dt.datetime]:
        """
        Возвращает дату создания баннера из кеша.
        Если в кеше нет — дотягивает через fetch_banners_info.
        """
        info = self.banner_info_cache.get(banner_id)
        if info is None:
            # дозагружаем только этот баннер
            self.fetch_banners_info([banner_id], fields="created")
            info = self.banner_info_cache.get(banner_id)
            if info is None:
                logger.debug(f"Баннер {banner_id}: нет данных created даже после fetch_banners_info")
                return None

        created_dt = info.get("created_dt")
        if created_dt is not None:
            return created_dt

        created_str = info.get("created")
        if not created_str:
            return None

        try:
            created_dt = dt.datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
            info["created_dt"] = created_dt
            self.banner_info_cache[banner_id] = info
            return created_dt
        except Exception as e:
            logger.warning(f"Ошибка парсинга даты создания баннера {banner_id}: {e}")
            return None


    def get_banner_name(self, banner_id: int) -> str:
        """
        Возвращает имя баннера из кеша, при необходимости — дотягивает через fetch_banners_info.
        """
        info = self.banner_info_cache.get(banner_id)
        if info is None:
            self.fetch_banners_info([banner_id], fields="name")
            info = self.banner_info_cache.get(banner_id)
            if info is None:
                logger.debug(f"Баннер {banner_id}: нет данных name даже после fetch_banners_info")
                return ""
        return info.get("name", "") or ""

    
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
                logger.warning(f"⤷ Баннер успешно отключен (HTTP 204)")
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

def reenable_profitable_banners(
    acc: AccountConfig,
    api: VkAdsApi,
    tg_token: str,
    income_total: Dict[str, float],
    income_recent: Dict[str, float],
) -> None:
    """
    Включает обратно баннеры, которые мы раньше отключили, если:
      • за сегодня или вчера есть доход (income_recent > 0),
      • и потрачено меньше дохода на max_loss_rub (spent_all_time - income_all <= max_loss_rub).
    Берёт список кандидатов из logs/disabled_<acc.name>.json
    """
    if not income_total or not income_recent:
        logger.info(f"[{acc.name}] Нет данных доходов для перевключения баннеров")
        return

    backup_path = LOG_DIR / f"disabled_{acc.name}.json"
    if not backup_path.exists():
        logger.info(f"[{acc.name}] Файл с отключёнными баннерами {backup_path} не найден — нечего включать")
        return

    try:
        with open(backup_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            logger.warning(f"[{acc.name}] Файл {backup_path} имеет некорректный формат (ожидался список)")
            return
        disabled_ids = [int(x) for x in data]
    except Exception as e:
        logger.error(f"[{acc.name}] Ошибка чтения {backup_path}: {e}")
        return

    # кандидаты: есть доход за сегодня/вчера
    candidate_ids = [bid for bid in disabled_ids if income_recent.get(str(bid), 0.0) > 0]
    if not candidate_ids:
        logger.info(f"[{acc.name}] Среди отключённых баннеров нет тех, у кого есть доход за сегодня/вчера")
        return

    stats_map = api.stats_summary_banners(candidate_ids)
    reenabled_ids: List[int] = []
    notifications: List[str] = []

    for bid in candidate_ids:
        stats = stats_map.get(bid, {}) or {}
        spent_all_time = float(stats.get("spent_all_time", 0.0))
        income_all = float(income_total.get(str(bid), 0.0))
        income_last2 = float(income_recent.get(str(bid), 0.0))

        diff = spent_all_time - income_all

        if income_all <= 0:
            logger.info(
                f"[{acc.name}] ▶ Баннер {bid}: есть свежий доход {income_last2:.2f}, "
                f"но суммарный доход {income_all:.2f} ≤ 0 — не включаем"
            )
            continue

        if diff > acc.flt.max_loss_rub:
            logger.info(
                f"[{acc.name}] ▶ Баннер {bid}: не включаем — потрачено {spent_all_time:.2f}, "
                f"доход {income_all:.2f}, разница {diff:.2f} > {acc.flt.max_loss_rub}"
            )
            continue

        logger.info(
            f"[{acc.name}] ↩ Попытка включить баннер {bid}: "
            f"spent_all_time={spent_all_time:.2f}, income_all={income_all:.2f}, "
            f"доход за 2 дня={income_last2:.2f}, diff={diff:.2f} ≤ {acc.flt.max_loss_rub}"
        )

        if api.enable_banner(bid):
            reenabled_ids.append(bid)
            banner_name = api.get_banner_name(bid) or "Без названия"
            notifications.append(
                f"<b>{banner_name}</b> #{bid}\n"
                f"    ⤷ Потрачено = {spent_all_time:.2f} ₽ | Доход = {income_all:.2f} ₽\n"
                f"    ⤷ Доход за сегодня/вчера = {income_last2:.2f} ₽"
            )

    if not reenabled_ids:
        logger.info(f"[{acc.name}] Подходящих баннеров для включения не найдено")
        return

    # Обновляем список отключённых баннеров (удаляем те, что включили)
    remaining_ids = [bid for bid in disabled_ids if bid not in reenabled_ids]
    try:
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(remaining_ids, f, ensure_ascii=False, indent=2)
        logger.info(
            f"[{acc.name}] Обновлён файл отключённых баннеров {backup_path}: "
            f"было={len(disabled_ids)}, осталось={len(remaining_ids)}, включено={len(reenabled_ids)}"
        )
    except Exception as e:
        logger.error(f"[{acc.name}] Ошибка сохранения {backup_path} после включения баннеров: {e}")

    if notifications and acc.chat_id:
        text = f"<b>[{acc.name}]</b>\n<b>Включены баннеры:</b>\n\n" + "\n\n".join(notifications)
        tg_notify(bot_token=tg_token, chat_id=acc.chat_id, text=text)
        

def process_account(acc: AccountConfig, tg_token: str) -> None:
    logger.info("=" * 80)
    logger.info(f"КАБИНЕТ: {acc.name} | n_days={acc.n_days}")
    
    # --- Загружаем данные о доходах (если указаны)
    income_total: Dict[str, float] = {}
    income_recent: Dict[str, float] = {}
    if acc.income_json_path:
        income_total, income_recent = load_income_data(acc.income_json_path)

    # 💡 Проверка списка кампаний
    if acc.allowed_campaigns == [0] or not acc.allowed_campaigns:
        if acc.check_all_camp:
            logger.info(f"{acc.name}: allowed_campaigns пуст, но check_all_camp=True → обрабатываем ВСЕ кампании")
            acc.allowed_campaigns = []  # пустой список => разрешаем всё
        else:
            logger.info(f"Пропуск кабинета {acc.name}: файл кампаний пуст или не найден (check_all_camp=False)")
            return
        
    api = VkAdsApi(token=acc.token)
    disabled_count = 0
    disabled_ids = []  # список для хранения отключённых баннеров
    notifications = []
    
    try:
        reenable_profitable_banners(acc, api, tg_token, income_total, income_recent)
    except Exception as e:
        logger.error(f"[{acc.name}] Ошибка при попытке перевключения баннеров: {e}")
    
    if acc.allowed_campaigns:
        api.add_banners_from_allowed_campaigns_bulk(acc.allowed_campaigns, acc.allowed_banners)
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

    #Подтягиваем created + name по всем активным баннерам одним bulk-запросом
    api.fetch_banners_info(banner_ids, fields="created,name")

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

        # --- Исключения ---
        if bid in acc.exceptions_banners:
            logger.info(f"▶ Пропускаем баннер {bid}: ИСКЛЮЧЕНИЕ")
            continue
        if agid in acc.exceptions_campaigns:
            logger.info(f"▶ Пропускаем баннер {bid} (Группа {agid}): ИСКЛЮЧЕНИЕ")
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

        # доходы
        income_all = float(income_total.get(str(bid), 0.0)) if income_total else 0.0

        # --- Если есть доход и баннер в пределах max_loss_rub — считаем его ОК и не трогаем ---
        if income_total and income_all > 0:
            diff = spent_all_time - income_all
            if diff <= acc.flt.max_loss_rub:
                logger.info(
                    f"▶ Пропускаем баннер {bid}: доход {income_all:.2f} ₽, "
                    f"потрачено {spent_all_time:.2f} ₽, разница {diff:.2f} ≤ {acc.flt.max_loss_rub} (прибыльный)"
                )
                continue

        period = period_map.get(bid, {})
        spent = float(period.get("spent", 0.0))
        cpc = float(period.get("cpc", 0.0))
        vk_cpa = float(period.get("vk.cpa", 0.0))

        logger.info(
            f"[BANNER {bid} | GROUP {agid}]: "
            f"spent = {spent:.2f}, cpc = {cpc:.2f}, cpa = {vk_cpa:.2f}, "
            f"spent_all_time = {spent_all_time:.2f}, income_all = {income_all:.2f}"
        )

        # --- Решаем, отключать ли баннер ---
        disable_this = False
        reason = ""

        # 🔹 НОВЫЙ ФИЛЬТР: потрачено ≥ 4000 и доход = 0 → отключаем (жёсткое правило)
        if income_total and income_all == 0.0 and spent_all_time >= 4000.0 and spent_all_time <= 6000.0:
            disable_this = True
            reason = (
                f"Потрачено за всё время {spent_all_time:.2f} ₽ и доход = 0 ₽ "
                f"(порог 4000 ₽)"
            )
        else:
            # 🔹 Старое правило: если баннер уже много потратил — не трогаем,
            # НО только если не сработало жёсткое правило выше
            if spent_all_time > acc.spent_all_time_dont_touch:
                logger.info(
                    f"▶ Пропускаем баннер {bid}: spent_all_time={spent_all_time:.2f} "
                    f"> {acc.spent_all_time_dont_touch:.2f} (не трогаем по правилу all_time_dont_touch)"
                )
                continue

            # Проверка фильтра CPC/CPA
            bad, reason_filter = acc.flt.violates(spent=spent, cpc=cpc, vk_cpa=vk_cpa)
            if not bad:
                logger.info("✔ Прошёл фильтр — ОК")
                continue

            disable_this = True
            reason = reason_filter

        if not disable_this:
            continue

        if disabled_count >= MAX_DISABLES_PER_RUN:
            logger.warning("🚨 Достигнут лимит отключений за запуск — дальнейшие баннеры не будут отключаться")
            break

        logger.warning(f"✖ НЕ ПРОШЁЛ ФИЛЬТР: {reason}")
        disabled = api.disable_banner(bid)
        status_msg = "ОТКЛЮЧЕНО" if disabled else "НЕ УДАЛОСЬ ОТКЛЮЧИТЬ"

        if disabled:
            disabled_count += 1
            disabled_ids.append(bid)

            banner_name = api.get_banner_name(bid) or "Без названия"
            notifications.append(
                f"<b>{banner_name}</b> #{bid}\n"
                f"    ⤷ Потрачено = {spent_all_time:.2f} ₽ | Доход = {income_all:.2f} ₽\n"
                f"    ⤷ Результат = {vk_cpa:.2f} ₽ | Цена клика = {cpc:.2f} ₽"
            )
  
    # --- Отправка ---
    if notifications:
        combined_text = f"<b>[{acc.name}]</b>\n<b>Отключены баннеры:</b>\n\n" + "\n\n".join(notifications)
        tg_notify(bot_token=tg_token, chat_id=acc.chat_id, text=combined_text)
        logger.info(f"Отправлено итоговое сообщение в TG с {len(notifications)} баннерами")
          
    if disabled_ids:
        # Формируем имя файла, например: logs/disabled_MAIN.json
        backup_path = LOG_DIR / f"disabled_{acc.name}.json"
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
