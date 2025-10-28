import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import time

# ======== НАСТРОЙКИ ========
load_dotenv()

SPENT_LIMIT = 300       # лимит по потраченным средствам
CPA_LIMIT = 200         # лимит по стоимости за результат
VK_HOST = "https://ads.vk.com"
LOG_FILE = "vk_ads.log"
BATCH_SIZE = 50

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

ACCOUNTS = [
    {
        "name": "User1",
        "token": os.getenv("VK_TOKEN_USER1"),
        "telegram_chat": os.getenv("TELEGRAM_CHAT_USER1"),
    }
]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


# ======== УТИЛИТЫ ========

def send_telegram_message(chat_id: str, text: str):
    """Отправить сообщение в Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": text})
    except Exception as e:
        logging.error(f"Ошибка при отправке в Telegram: {e}")


def get_vk(url, token, params=None, retries=5):
    """GET-запрос к VK Ads API"""
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(retries):
        r = requests.get(f"{VK_HOST}{url}", headers=headers, params=params)
        if r.status_code == 429:
            logging.warning("⏳ Лимит запросов VK Ads достигнут. Ждём 2 секунды...")
            time.sleep(2)
            continue

        if r.status_code != 200:
            raise Exception(f"Ошибка VK GET {url}: {r.status_code} {r.text}")

        time.sleep(0.3)
        return r.json()

    raise Exception(f"Ошибка VK GET {url}: слишком много попыток")


def post_vk(url, token, data=None):
    """POST-запрос (например, блокировка баннера)"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(f"{VK_HOST}{url}", headers=headers, json=data)
    if r.status_code not in (200, 204):
        raise Exception(f"Ошибка VK POST {url}: {r.status_code} {r.text}")
    time.sleep(0.2)
    return r.json() if r.text else {}


# ======== СТАТИСТИКА ========

def fetch_banner_stats(token, banner_ids, date_from, date_to):
    """Запрашивает статистику по баннерам за указанный диапазон дат"""
    if not banner_ids:
        return {}

    ids_str = ",".join(map(str, banner_ids))
    url = "/api/v2/statistics/banners/day.json"
    params = {
        "ids": ids_str,
        "metrics": "base",
        "attribution": "conversion",
        "date_from": date_from,
        "date_to": date_to,
    }

    response = get_vk(url, token, params=params)
    items = response.get("items", [])
    stats = {}

    for item in items:
        banner_id = item.get("id")
        total_spent = 0.0
        total_goals = 0.0

        for row in item.get("rows", []):
            base = row.get("base", {})
            spent = float(base.get("spent", 0) or 0)
            goals = float(base.get("vk", {}).get("goals", 0) or 0)
            total_spent += spent
            total_goals += goals

        total_cpa = round(total_spent / total_goals, 2) if total_goals > 0 else 0.0
        stats[banner_id] = {"spent": total_spent, "cpa": total_cpa}

    return stats


# ======== ОСНОВНАЯ ЛОГИКА ========

def check_ads(account):
    token = account["token"]
    chat_id = account["telegram_chat"]
    user_name = account["name"]

    logging.info(f"===== Проверка аккаунта: {user_name} =====")

    try:
        # Получаем активные кампании
        ad_plans = get_vk("/api/v2/ad_plans.json", token, params={"_status": "active"}).get("items", [])
        logging.info(f"Найдено активных кампаний: {len(ad_plans)}")

        if not ad_plans:
            logging.warning("Нет активных кампаний.")
            return

        for plan in ad_plans:
            plan_id = plan["id"]
            plan_name = plan["name"]

            logging.info(f"▶ Кампания: {plan_name} (ID {plan_id})")

            # Получаем активные баннеры для этой кампании
            banners = get_vk("/api/v2/banners.json", token, params={
                "_status": "active",
                "_ad_group_status": "active",
                "_ad_plan_id": plan_id,   # фильтр по кампании
                "limit": 200,
            }).get("items", [])

            if not banners:
                logging.info(f"  └─ Нет активных баннеров в кампании {plan_name}")
                continue

            logging.info(f"  └─ Активных баннеров: {len(banners)}")

            # Считаем статистику за вчера и сегодня
            date_from = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
            date_to = datetime.today().strftime("%Y-%m-%d")

            banner_ids = [b["id"] for b in banners]
            stats = fetch_banner_stats(token, banner_ids, date_from, date_to)

            for banner in banners:
                banner_id = banner["id"]
                banner_name = banner.get("name", f"Banner {banner_id}")
                stat = stats.get(banner_id, {"spent": 0, "cpa": 0})
                spent = stat["spent"]
                cpa = stat["cpa"]

                logging.info(f"     • {banner_name}: spent={spent}, cpa={cpa}")

                # Проверка лимитов
                if spent >= SPENT_LIMIT and cpa >= CPA_LIMIT:
                    try:
                        post_vk(f"/api/v2/banners/{banner_id}.json", token, data={"status": "blocked"})
                        msg = f"🚫 Кампания [{plan_name}] → {banner_name} отключен (spent={spent}, cpa={cpa})"
                        send_telegram_message(chat_id, msg)
                        logging.warning(msg)
                    except Exception as e:
                        logging.error(f"Ошибка при блокировке баннера {banner_id}: {e}")

    except Exception as e:
        logging.error(f"Ошибка при обработке аккаунта {user_name}: {e}")


# ======== ТОЧКА ВХОДА ========
if __name__ == "__main__":
    logging.info(f"\n\n===== Запуск проверки {datetime.now()} =====")
    for acc in ACCOUNTS:
        check_ads(acc)
    logging.info("===== Проверка завершена =====\n")
