import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import time

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

# ======== АККАУНТЫ ========
ACCOUNTS = [
    {
        "name": "User1",
        "token": os.getenv("VK_TOKEN_USER1"),
        "telegram_chat": os.getenv("TELEGRAM_CHAT_USER1"),
    }
]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


# ======== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========

def send_telegram_message(chat_id: str, text: str):
    """Отправить сообщение в Telegram"""
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": chat_id, "text": text})
    except Exception as e:
        logging.error(f"Ошибка при отправке сообщения в Telegram: {e}")


def get_vk(url, token, params=None, retries=5):
    """GET-запрос к VK ADS API с ограничением скорости и повторными попытками"""
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(retries):
        r = requests.get(f"{VK_HOST}{url}", headers=headers, params=params)

        # Если слишком много запросов — ждём
        if r.status_code == 429:
            logging.warning("⏳ Лимит запросов VK Ads достигнут. Ожидание 2 секунды...")
            time.sleep(2)
            continue

        if r.status_code != 200:
            raise Exception(f"Ошибка VK GET {url}: {r.status_code} {r.text}")

        # добавляем задержку, чтобы не долбить API
        time.sleep(0.3)
        return r.json()

    raise Exception(f"Ошибка VK GET {url}: слишком много попыток после 429")


def post_vk(url, token, data=None):
    """POST-запрос к VK ADS API"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(f"{VK_HOST}{url}", headers=headers, json=data)

    if r.status_code == 429:
        logging.warning("⏳ Лимит запросов VK Ads (POST). Ждём 2 секунды...")
        time.sleep(2)
        return post_vk(url, token, data)

    if r.status_code not in (200, 204):
        raise Exception(f"Ошибка VK POST {url}: {r.status_code} {r.text}")

    time.sleep(0.3)
    return r


# ======== ОСНОВНАЯ ПРОВЕРКА ========

def check_ads(account):
    token = account["token"]
    chat_id = account["telegram_chat"]
    user_name = account["name"]

    logging.info(f"===== Проверка аккаунта: {user_name} =====")

    try:
        # 1️⃣ Кампании
        ad_plans = get_vk("/api/v2/ad_plans.json", token).get("items", [])
        logging.info(f"Найдено {len(ad_plans)} кампаний")

        for plan in ad_plans:
            plan_id = plan["id"]
            plan_name = plan.get("name", "Без названия")
            plan_status = plan.get("status", "unknown")

            logging.info(f"▶ Кампания: {plan_name} (ID {plan_id}) [Статус: {plan_status}]")

            # 2️⃣ Группы
            ad_groups = get_vk("/api/v2/ad_groups.json", token, params={"ad_plan_id": plan_id}).get("items", [])
            logging.info(f"  ├─ Найдено групп: {len(ad_groups)}")

            for group in ad_groups:
                group_id = group["id"]
                group_name = group.get("name", "Без названия")
                group_status = group.get("status", "unknown")

                logging.info(f"  │  ├─ Группа: {group_name} (ID {group_id}) [Статус: {group_status}]")

                # 3️⃣ Баннеры
                banners = get_vk("/api/v2/banners.json", token, params={"ad_group_id": group_id}).get("items", [])
                logging.info(f"  │  │  ├─ Найдено баннеров: {len(banners)}")

                for banner in banners:
                    banner_id = banner["id"]
                    banner_name = banner.get("name", f"Banner {banner_id}")
                    banner_status = banner.get("status", "unknown")

                    # 4️⃣ Получаем статистику
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

                    stat_data = {}
                    spent = 0
                    cpa = 0

                    try:
                        stat_data = get_vk(stats_url, token, params=params)
                        items = stat_data.get("items", [])
                        if items:
                            metrics = items[0]["total"]["base"]
                            spent = float(metrics.get("spent", 0))
                            cpa = float(metrics.get("vk", {}).get("cpa", 0))
                    except Exception as e:
                        logging.warning(f"  │  │  └─ Ошибка получения статистики для {banner_name}: {e}")

                    # Лог всех баннеров, независимо от статуса
                    logging.info(
                        f"  │  │  └─ Баннер: {banner_name} (ID {banner_id}) "
                        f"[Статус: {banner_status}] | spent={spent}, cpa={cpa}"
                    )

                    # Проверка условий отключения
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
