
from __future__ import annotations
import json as std_json
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
VersionVKChecker = "3.1.95 DEBUG"
BASE_URL = os.environ.get("VK_ADS_BASE_URL", "https://ads.vk.com")  # при необходимости переопределить в .env
STATS_TIMEOUT = 30
WRITE_TIMEOUT = 30
RETRY_COUNT = 3
RETRY_BACKOFF = 1.8
MAX_DISABLES_PER_RUN = 15  # максимум баннеров, которые можно отключить за один запуск

DRY_RUN = True  #True для тестов, False для рабочего

# Период для расчёта метрик фильтра (spent, cpc, vk.cpa)
N_DAYS_DEFAULT = 2  # Можно переопределить отдельно для каждого кабинета

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
        user_json_path="/opt/vk_checker/data/users/388320243.json",
        name="Роман Каракозик Вадим-5919",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/karakoz_karas.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/493796058.json",
        name="Алена Кукаркина 19 декабря-5919",
        check_all_camp=True,
        spent_all_time_dont_touch=700,
        income_json_path="/opt/leads_postback/data/nalickinrf.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/493796058.json",
        name="Алена Кукаркина zk 5005 1",
        check_all_camp=True,
        spent_all_time_dont_touch=700,
        income_json_path="/opt/leads_postback/data/nalickinrf.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/493796058.json",
        name="Алена Кукаркина zk 5005 2",
        check_all_camp=True,
        spent_all_time_dont_touch=700,
        income_json_path="/opt/leads_postback/data/nalickinrf.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1170798256.json",
        name="Вадим ZK2",
        check_all_camp=True,
        spent_all_time_dont_touch=2000,
        income_json_path="/opt/leads_postback/data/insta.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1170798256.json",
        name="Гузель март 1 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=2000,
        income_json_path="/opt/leads_postback/data/insta.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1170798256.json",
        name="Гузель март 2 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=2000,
        income_json_path="/opt/leads_postback/data/insta.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1170798256.json",
        name="Вадим 5919",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/insta.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1170798256.json",
        name="Рустам Гим ZK 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=2000,
        income_json_path="/opt/leads_postback/data/insta.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/466605189.json",
        name="Гузель июль 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=1200,
        income_json_path="/opt/leads_postback/data/insta.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/826253263.json",
        name="Myagkov_уткавалютка.рф-7587",
        check_all_camp=True,
        spent_all_time_dont_touch=2000,
        income_json_path="/opt/leads_postback/data/utkavalutkarf.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/826253263.json",
        name="Кирилл zk 5005 1",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/utkavalutkarf.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1520701648.json",
        name="Оля Пунтус 31 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/monzi.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/1520701648.json",
        name="Ольга Пунтус-5919 монзи",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/monzi.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/653111182.json",
        name="Osetrov_капикэш.рф-7587",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/lisicka.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/653111182.json",
        name="Максим Осетров zk 5005",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/lisicka.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/374364626.json",
        name="Кирилл/Николай zk 5005 1",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/ptichka.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/374364626.json",
        name="Николай Орехов ZK 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/ptichka.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/674545170.json",
        name="Николай ВК ZK 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/kupr.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/674545170.json",
        name="Иван Куприянов zk 1 5005",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/kupr.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/577772146.json",
        name="Николай Орехов zk 5005 1",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/zaymdozp.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/285360489.json",
        name="Пчелка новый 2025 zk 5005 1",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/pchelkazaim.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/285360489.json",
        name="Никита Мишустин 28 августа 7007",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/pchelkazaim.json",
        allowed_banners=[],
        exceptions_campaigns=[],
        exceptions_banners=[],
    ),
    AccountConfig(
        user_json_path="/opt/vk_checker/data/users/285360489.json",
        name="Мишустин-5959-5919",
        check_all_camp=True,
        spent_all_time_dont_touch=1000,
        income_json_path="/opt/leads_postback/data/pchelkazaim.json",
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
    
def parse_json_safely(resp: requests.Response, url: str, *, allow_empty: bool = False) -> dict:
    """
    Безопасно разбирает JSON:
      - 204 / пустой body -> {} (если allow_empty=True), иначе бросаем исключение
      - Content-Type не JSON -> логируем фрагмент и бросаем исключение
      - Невалидный JSON -> логируем и бросаем исключение
    """
    ct = (resp.headers.get("Content-Type") or "").lower()
    text = resp.text or ""

    # 204 / пустое тело
    if resp.status_code == 204 or (not text.strip()):
        if allow_empty:
            logging.warning(f"⚠️ {url} вернул пустой ответ (HTTP {resp.status_code})")
            return {}
        raise ValueError(f"Empty body from {url}, HTTP {resp.status_code}")

    # Контент не JSON
    if "application/json" not in ct and "json" not in ct:
        snippet = text[:400].replace("\n", " ")
        logging.error(
            f"❌ Ожидали JSON от {url}, но Content-Type={ct} (HTTP {resp.status_code}). "
            f"Фрагмент тела: {snippet}"
        )
        raise ValueError(f"Non-JSON response from {url}, HTTP {resp.status_code}")

    try:
        return resp.json()
    except std_json.JSONDecodeError as e:
        snippet = text[:400].replace("\n", " ")
        logging.error(
            f"❌ Не удалось разобрать JSON от {url} (HTTP {resp.status_code}): {e}. "
            f"Фрагмент тела: {snippet}"
        )
        raise
    except Exception as e:
        logging.error(f"❌ Ошибка при разборе JSON от {url}: {e}")
        raise

def fmt_date(d: str) -> str:
    """Преобразует дату YYYY-MM-DD → DD.MM"""
    try:
        dt_obj = dt.datetime.strptime(d, "%Y-%m-%d")
        return dt_obj.strftime("%d.%m")
    except Exception:
        return d

def req_with_retry(method: str, url: str, headers: Dict[str, str],
                   params: Dict[str, Any] | None = None,
                   json_body: Dict[str, Any] | None = None,
                   timeout: int = 30) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            logger.debug(f"HTTP {method} {url} params={params} json={json_body}")
            resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)
            
            # 💡 Лимиты VK API
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

def load_income_data(path: str) -> Dict[str, float]:
    """
    Загружает JSON с доходами и суммирует их по всем дням.
    Возвращает словарь {banner_id -> total_income_float}
    """
    if not path or not os.path.exists(path):
        logger.warning(f"⚠️ Файл доходов {path} не найден — фильтр дохода отключён")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)

        income_total: Dict[str, float] = {}
        for entry in raw:
            data = entry.get("data", {})
            if not isinstance(data, dict):
                continue
            for bid, val in data.items():
                income_total[bid] = income_total.get(bid, 0.0) + float(val)

        logger.info(f"✅ Загружены доходы по {len(income_total)} баннерам из {path}")
        return income_total
    except Exception as e:
        logger.error(f"Ошибка чтения доходов из {path}: {e}")
        return {}


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
            # Проверьте схему авторизации в вашей инсталляции (Bearer/Token/кастомный заголовок)
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }

    def add_banners_from_campaign_to_exceptions(self, campaign_id: int, exceptions_banners: List[int]) -> None:
        """
        Расширяет exceptions_banners всеми активными баннерами кампании.
        """
        try:
            logger.info(f"Исключения: собираем баннеры кампании {campaign_id}")
            # 1) получаем группы кампании
            params_plans = {
                "_status": "active",
                "_id__in": str(campaign_id),
                "fields": "ad_groups,name",
                "limit": 200,
                "offset": 0,
            }
            url_plans = f"{self.base_url}/api/v2/ad_plans.json"
            resp_plans = req_with_retry("GET", url_plans, headers=self.headers, params=params_plans, timeout=STATS_TIMEOUT)
            data_plans = parse_json_safely(resp_plans, url_plans)
            items = data_plans.get("items", []) or []
            group_ids: List[int] = []
            for plan in items:
                for g in (plan.get("ad_groups", []) or []):
                    gid = g.get("id")
                    if gid:
                        group_ids.append(int(gid))
            if not group_ids:
                logger.info(f"Исключения: у кампании {campaign_id} нет групп (или неактивны)")
                return
            # 2) собираем баннеры групп
            seen = set(exceptions_banners)
            for i in range(0, len(group_ids), 200):
                chunk = group_ids[i:i+200]
                params_groups = {
                    "_status": "active",
                    "_id__in": ",".join(map(str, chunk)),
                    "fields": "banners,name",
                    "limit": 200,
                }
                url_groups = f"{self.base_url}/api/v2/ad_groups.json"
                resp_groups = req_with_retry("GET", url_groups, headers=self.headers, params=params_groups, timeout=STATS_TIMEOUT)
                data_groups = parse_json_safely(resp_groups, url_groups)
                for g in (data_groups.get("items", []) or []):
                    for b in (g.get("banners", []) or []):
                        bid = int(b.get("id") or 0)
                        if bid and bid not in seen:
                            exceptions_banners.append(bid)
                            seen.add(bid)
            logger.info(f"Исключения: добавлено баннеров из кампании {campaign_id}: {len(seen)} всего в списке")
        except Exception as e:
            logger.error(f"Ошибка при расширении исключений для кампании {campaign_id}: {e}")
    
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
            }
            resp = req_with_retry("GET", url, headers=self.headers, params=params, timeout=STATS_TIMEOUT)
            data = parse_json_safely(resp, url)
            batch = data.get("items", []) or []
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
        data = parse_json_safely(resp, url)
        result: Dict[int, Dict[str, Any]] = {}
        for it in data.get("items", []) or []:
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
        data = parse_json_safely(resp, url)
        result: Dict[int, Dict[str, Any]] = {}
        for it in data.get("items", []) or []:
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
                data = parse_json_safely(resp, url_plans)
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
                data_groups = parse_json_safely(resp_groups, url_groups)
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
        url = f"{self.base_url}/api/v2/banners/{banner_id}.json"
        for attempt in range(1, 4):
            try:
                time.sleep(0.4)
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
                data = parse_json_safely(resp, url)
                created_str = data.get("created")
                if created_str:
                    return dt.datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                else:
                    logger.debug(f"Баннер {banner_id}: поле 'created' отсутствует в ответе")
                    return None
            except Exception as e:
                logger.warning(f"Не удалось получить дату создания баннера {banner_id}: {e}")
                time.sleep(1.0 * attempt)
        return None


    def get_banner_name(self, banner_id: int) -> str:
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
                data = parse_json_safely(resp, url)
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


def process_account(acc: AccountConfig, tg_token: str) -> None:
    logger.info("=" * 80)
    logger.info(f"КАБИНЕТ: {acc.name} | n_days={acc.n_days}")

    # --- Загружаем данные о доходах (если указаны)
    income_total = {}
    if acc.income_json_path:
        income_total = load_income_data(acc.income_json_path)

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
        # --- Проверка дохода: если баннер не убыточен, пропускаем остальные фильтры
        if income_total:
            income_all = float(income_total.get(str(bid), 0.0))

            # Если доход = 0 — считаем, что данных нет, пропускаем проверку дохода
            if income_all > 0:
                diff = spent_all_time - income_all

                # если потрачено <= доход + max_loss_rub — баннер прибыльный, не трогаем
                if diff <= acc.flt.max_loss_rub:
                    logger.info(
                        f"▶ Пропускаем баннер {bid}: доход {income_all:.2f}, потрачено {spent_all_time:.2f}, "
                        f"разница {diff:.2f} ≤ {acc.flt.max_loss_rub} (прибыльный)"
                    )
                    continue


        period = period_map.get(bid, {})
        spent = float(period.get("spent", 0.0))
        cpc = float(period.get("cpc", 0.0))
        vk_cpa = float(period.get("vk.cpa", 0.0))
        income_all = float(income_total.get(str(bid), 0.0)) if income_total else 0.0

        # --- Исключения ---
        if bid in acc.exceptions_banners:
            logger.info(f"▶ Пропускаем баннер {bid}: ИСКЛЮЧЕНИЕ")
            continue
        if agid in acc.exceptions_campaigns:
            logger.info(f"▶ Пропускаем баннер {bid} (Кампания {agid}): ИСКЛЮЧЕНИЕ")
            continue
            
        logger.info(
                f"[BANNER {bid} | GROUP {agid}]:spent = {spent:.2f},cpc = {cpc:.2f},cpa = {vk_cpa:.2f}, income = {income_all:.2f}"
        )

        # Если объявление уже потратило больше порога — не трогаем
        if spent_all_time > acc.spent_all_time_dont_touch:
            logger.info(
                f"▶ Пропускаем: spent_all_time>{acc.spent_all_time_dont_touch} (не трогаем по правилу)"
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

        if disabled:
            disabled_count += 1
            disabled_ids.append(bid)
          
            # --- Копим уведомление ---
            #reason_short = short_reason(spent, cpc, vk_cpa, acc.flt)
            banner_name = api.get_banner_name(bid) or "Без названия"
            notifications.append(
                f"<b>{banner_name}</b> #{bid}\n"
                f"    ⤷ Потрачено = {spent_all_time:.2f} ₽ | Доход = {income_all:.2f} ₽\n "
                f"    ⤷ Результат = {vk_cpa:.2f} ₽ | Цена клика = {cpc:.2f} ₽"
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
