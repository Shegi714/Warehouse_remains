import os
import time
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# 🧪 Глобальные переменные окружения (используются GitHub Secrets)
SOURCE_SHEET_ID = os.environ.get("SOURCE_SHEET_ID")
TARGET_SHEET_ID = os.environ.get("TARGET_SHEET_ID")
GOOGLE_CREDS_PATH = os.environ.get("GOOGLE_CREDS_PATH")

# ⚙️ Настройки подключения к Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
client = gspread.authorize(creds)

# 📑 Чтение данных токенов и кабинетов
source_sheet = client.open_by_key(SOURCE_SHEET_ID).sheet1
rows = source_sheet.get_all_values()[1:]
data = [{"token": row[0], "cabinet": row[1]} for row in rows if len(row) >= 2 and row[0] and row[1]]

# 📡 Параметры отчета
params = {
    "locale": "ru",
    "groupByBrand": "false",
    "groupBySubject": "false",
    "groupBySa": "true",
    "groupByNm": "true",
    "groupByBarcode": "true",
    "groupBySize": "true",
    "filterPics": "0",
    "filterVolume": "0"
}

# 🔁 Ретрий создания отчета
def create_report(token, retries=3, delay=5):
    url = "https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0"
    }

    for attempt in range(1, retries + 1):
        try:
            print(f"📡 [{attempt}/{retries}] Создание отчета...")
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            json_data = response.json()
            task_id = json_data.get("data", {}).get("taskId") or json_data.get("taskId")
            if task_id:
                print(f"✅ Получен taskId: {task_id}")
                return task_id
            else:
                print(f"❗ Нет taskId в ответе: {json_data}")
        except Exception as e:
            print(f"⚠️ Ошибка запроса: {e}")
        time.sleep(delay)
    return None

# ⏳ Проверка готовности отчета (до 5 попыток)
def wait_for_report(token, task_id, retries=5, delay=10):
    url = f"https://seller-analytics-api.wildberries.ru/api/v1/warehouse_remains/tasks/{task_id}/download"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0"
    }

    for attempt in range(1, retries + 1):
        try:
            print(f"⏳ [{attempt}/{retries}] Проверка готовности отчета...")
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                print("📥 Отчет готов, данные загружены.")
                return response.json()
            else:
                print(f"🔁 Ответ {response.status_code}, ждем...")
        except Exception as e:
            print(f"⚠️ Ошибка при получении отчета: {e}")
        time.sleep(delay)
    print("❌ Отчет не был готов вовремя.")
    return None

# 📊 Запись отчета в Google Sheets
def write_report_to_sheet(sheet_obj, cabinet_name, report_data):
    try:
        try:
            worksheet = sheet_obj.worksheet(cabinet_name)
            worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet_obj.add_worksheet(title=cabinet_name, rows="1000", cols="20")

        headers = ["brand", "subjectName", "vendorCode", "nmId", "barcode", "techSize", "volume"]
        warehouse_names = set()
        for item in report_data:
            for w in item.get("warehouses", []):
                warehouse_names.add(w["warehouseName"])
        headers += sorted(warehouse_names)

        rows = [headers]
        for item in report_data:
            base = [
                item.get("brand", ""),
                item.get("subjectName", ""),
                item.get("vendorCode", ""),
                item.get("nmId", ""),
                item.get("barcode", ""),
                item.get("techSize", ""),
                item.get("volume", "")
            ]
            quantities = {w["warehouseName"]: w["quantity"] for w in item.get("warehouses", [])}
            qty_row = [quantities.get(name, 0) for name in sorted(warehouse_names)]
            rows.append(base + qty_row)

        worksheet.update(rows)
        print(f"✅ Отчет записан в лист: {cabinet_name}")
    except Exception as e:
        print(f"🛑 Ошибка записи в Google Sheet '{cabinet_name}': {e}")

# 🚀 Главная функция
def main():
    target_sheet = client.open_by_key(TARGET_SHEET_ID)

    for entry in data:
        cabinet = entry["cabinet"]
        token = entry["token"]
        print(f"\n🔄 Работаем с кабинетом: {cabinet}")

        task_id = create_report(token)
        if not task_id:
            print(f"❌ Пропуск {cabinet}, не удалось создать отчет.")
            continue

        report = wait_for_report(token, task_id)
        if not report:
            print(f"❌ Пропуск {cabinet}, отчет не готов.")
            continue

        write_report_to_sheet(target_sheet, cabinet, report)

if __name__ == "__main__":
    main()
