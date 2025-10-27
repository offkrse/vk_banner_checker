import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json

# ======== ЗАГРУЗКА ОКРУЖЕНИЯ ========
load_dotenv()

# ======== НАСТРОЙКИ ========
SPENT_LIMIT = 300       # лимит по потраченным средствам
CPA_LIMIT = 200         # лимит по стоимости за результат
VK_HOST = "https://ads.vk.com"
LOG_FILE = "vk_ads.log"
# ============================

# Настройка логгера
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# Аккаунты VK ADS
ACCOUNTS = [
    {
        "name": "User1",
        "token": os.getenv("VK_TOKEN_USER1"),
        "telegram_chat": os.getenv("TELEGRAM_CHAT_USER1"),
    },
    {
        "name": "User2",
        "token": os.getenv("VK_TOKEN_USER2"),
        "telegram_chat": os.getenv("TELEGRAM_CHAT_USER2"),
    },
]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


# ======== ФУНКЦИИ ========

def send_telegram_message(chat_id: str, text: str):
    """Отправить сообщение в Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": text})
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения в Telegram: {e}")


def get_vk(url, token, params=None):
    """GET-запрос к VK ADS API"""
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(f"{VK_HOST}{url}", headers=headers, params=params)
    if r.status_code != 200:
        raise Exception(f"Ошибка VK GET {url}: {r.status_code} {r.text}")
    return r.json()


def post_vk(url, token, data=None):
    """POST-запрос к VK ADS API"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(f"{VK_HOST}{url}", headers=headers, json=data)
    if r.status_code not in (200, 204):
        raise Exception(f"Ошибка VK POST {url}: {r.status_code} {r.text}")
    return r


def check_ads(account):
    token = account["token"]
    chat_id = account["telegram_chat"]
    user_name = account["name"]

    logging.info(f"===== Проверка аккаунта: {user_name} =====")

    try:
        # Получаем рекламные кампании
        ad_plans = get_vk("/api/v2/ad_plans.json", token).get("items", [])
        logging.info(f"Найдено {len(ad_plans)} кампаний")

        for plan in ad_plans:
            plan_id = plan["id"]
            plan_name = plan["name"]

            logging.info(f"▶ Кампания: {plan_name} (ID {plan_id})")

            # Получаем группы кампании
            ad_groups = get_vk("/api/v2/ad_groups.json", token, params={"ad_plan_id": plan_id}).get("items", [])
            logging.info(f"  ├─ Найдено групп: {len(ad_groups)}")

            for group in ad_groups:
                group_id = group["id"]
                group_name = group.get("name", "Без названия")

                logging.info(f"  │  ├─ Группа: {group_name} (ID {group_id})")

                # Получаем баннеры
                banners = get_vk("/api/v2/banners.json", token, params={"ad_group_id": group_id}).get("items", [])
                logging.info(f"  │  │  ├─ Найдено баннеров: {len(banners)}")

                for banner in banners:
                    banner_id = banner["id"]
                    banner_name = banner.get("name", f"Banner {banner_id}")

                    # Получаем статистику за последние сутки
                    date_to = datetime.today().strftime("%Y-%m-%d")
                    date_from = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

                    stats_url = "/api/v2/statistics/banners/summary.json"
                    params = {
                        "id": banner_id,
                        "metrics": "base",
                        "attribution": "conversion",
                        "date_from": date_from,
                        "date_to": date_to,
                    }

                    stat_data = get_vk(stats_url, token, params=params)
                    items = stat_data.get("items", [])

                    if not items:
                        logging.info(f"  │  │  └─ {banner_name}: нет данных статистики")
                        continue

                    metrics = items[0]["total"]["base"]
                    spent = float(metrics.get("spent", 0))
                    cpa = float(metrics.get("vk", {}).get("cpa", 0))

                    # Записываем данные по баннеру
                    logging.info(
                        f"  │  │  └─ {banner_name} (ID {banner_id}): spent={spent}, cpa={cpa}"
                    )

                    # Проверяем условия отключения
                    if spent >= SPENT_LIMIT and cpa >= CPA_LIMIT:
                        try:
                            post_vk(f"/api/v2/banners/{banner_id}.json", token, data={"status": "blocked"})
                            msg = f"[{plan_name}] [{group_name}] [{banner_name}] — отключен (spent={spent}, cpa={cpa})"
                            send_telegram_message(chat_id, msg)
                            logging.warning(f"  │  │     🚫 Отключен баннер: {msg}")
                        except Exception as e:
                            logging.error(f"  │  │     ❌ Ошибка при блокировке баннера {banner_id}: {e}")

    except Exception as e:
        logging.error(f"Ошибка при обработке аккаунта {user_name}: {e}")


# ======== ТОЧКА ВХОДА ========
if __name__ == "__main__":
    logging.info(f"\n\n===== Запуск проверки {datetime.now()} =====")
    for acc in ACCOUNTS:
        check_ads(acc)
    logging.info("===== Проверка завершена =====\n")
