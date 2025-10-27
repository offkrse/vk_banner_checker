import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import time

# ======== НАСТРОЙКИ ========
load_dotenv()

VK_HOST = "https://ads.vk.com"
LOG_FILE = "vk_ads.log"
SPENT_METRIC = "spent"
CPA_METRIC = "cpa"

# ======== НАСТРОЙКА ЛОГГЕРА ========
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ======== АККАУНТЫ ========
ACCOUNTS = [
    {
        "name": "MainAccount",
        "token": os.getenv("VK_TOKEN_USER1"),
    }
]

# ======== VK API ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ========
def get_vk(url, token, params=None, retries=5):
    """GET-запрос к VK ADS API с контролем лимитов"""
    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(retries):
        r = requests.get(f"{VK_HOST}{url}", headers=headers, params=params)

        if r.status_code == 429:
            logging.warning("⏳ Лимит запросов VK Ads достигнут. Ожидание 2 секунды...")
            time.sleep(2)
            continue

        if r.status_code != 200:
            raise Exception(f"Ошибка VK GET {url}: {r.status_code} {r.text}")

        time.sleep(0.3)  # задержка между запросами
        return r.json()

    raise Exception(f"Ошибка VK GET {url}: слишком много попыток после 429")


# ======== ОСНОВНАЯ ЛОГИКА ========
def check_campaigns(account):
    token = account["token"]
    user_name = account["name"]

    logging.info(f"===== Проверка аккаунта: {user_name} =====")

    try:
        # 1️⃣ Получаем список кампаний
        ad_plans = get_vk("/api/v2/ad_plans.json", token).get("items", [])
        logging.info(f"Найдено {len(ad_plans)} кампаний")

        # 2️⃣ Для каждой кампании — получаем статистику
        for plan in ad_plans:
            plan_id = plan["id"]
            plan_name = plan.get("name", f"Campaign {plan_id}")
            plan_status = plan.get("status", "unknown")

            # Период статистики — последние сутки
            date_to = datetime.today().strftime("%Y-%m-%d")
            date_from = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

            stats_url = "/api/v2/statistics/ad_plans/summary.json"
            params = {
                "id": plan_id,
                "metrics": "base",
                "attribution": "conversion",
                "date_from": date_from,
                "date_to": date_to,
            }

            try:
                stat_data = get_vk(stats_url, token, params=params)
                items = stat_data.get("items", [])

                if not items:
                    logging.info(
                        f"▶ Кампания: {plan_name} (ID {plan_id}) | Статус: {plan_status} | Нет данных статистики"
                    )
                    continue

                metrics = items[0]["total"]["base"]
                spent = float(metrics.get("spent", 0))
                cpa = float(metrics.get("vk", {}).get("cpa", 0))

                logging.info(
                    f"▶ Кампания: {plan_name} (ID {plan_id}) | Статус: {plan_status} | spent={spent}, cpa={cpa}"
                )

            except Exception as e:
                logging.error(
                    f"Ошибка получения статистики кампании {plan_name} (ID {plan_id}): {e}"
                )

    except Exception as e:
        logging.error(f"Ошибка при обработке аккаунта {user_name}: {e}")


# ======== ТОЧКА ВХОДА ========
if __name__ == "__main__":
    logging.info(f"\n\n===== Запуск проверки {datetime.now()} =====")
    for acc in ACCOUNTS:
        check_campaigns(acc)
    logging.info("===== Проверка завершена =====\n")
