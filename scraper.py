import time
import pandas as pd
import os
import re
import gspread
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup

# -------------------------
# CONFIG
# -------------------------

LOGIN_URL = "https://shopini.com/admin/staff-activity-logs"
BASE_URL = "https://shopini.com/admin/staff-activity-logs?page="
NUM_PAGES = 300

EMAIL = os.getenv("SHOPINI_EMAIL")
PASSWORD = os.getenv("SHOPINI_PASSWORD")

SHEET_NAME = "Shopini Staff Logs"

# -------------------------
# GOOGLE SHEETS AUTH
# -------------------------

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(
    "credentials.json",
    scopes=scope
)

client = gspread.authorize(creds)
sheet = client.open(SHEET_NAME).worksheet("Sheet1")

# -------------------------
# START SELENIUM (HEADLESS)
# -------------------------

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 15)

# -------------------------
# LOGIN
# -------------------------

driver.get(LOGIN_URL)

wait.until(EC.presence_of_element_located((By.NAME, "email"))).send_keys(EMAIL)
wait.until(EC.presence_of_element_located((By.NAME, "password"))).send_keys(PASSWORD)
wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))).click()

time.sleep(3)

# -------------------------
# HELPERS
# -------------------------

def clean_number(text):
    return re.sub(r"[^\d]", "", text)

# -------------------------
# SCRAPE TABLE
# -------------------------

all_rows = []

for page in range(1, NUM_PAGES + 1):

    driver.get(BASE_URL + str(page))
    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    rows = soup.select("table tbody tr")

    for row in rows:

        cells = row.find_all("td")
        if not cells:
            continue

        record_id = cells[0].get_text(strip=True)

        staff_block = row.select_one("td:nth-of-type(2) div div")

        if staff_block:
            divs = staff_block.find_all("div")
            staff_name = divs[0].get_text(strip=True) if len(divs) > 0 else None
            staff_email = divs[1].get_text(strip=True) if len(divs) > 1 else None
        else:
            staff_name = None
            staff_email = None

        operation_span = row.select_one("td:nth-of-type(3) span")
        operation = operation_span.get_text(strip=True) if operation_span else None
        operation_type = operation.lower().replace(" ", "_") if operation else None

        description_div = row.select_one("td:nth-of-type(4) div")

        if description_div:
            description = description_div.get("title") or description_div.get_text(strip=True)
        else:
            description = None

        ip_code = row.select_one("td:nth-of-type(5) code")
        ip_address = ip_code.get_text(strip=True) if ip_code else None

        time_cells = row.select("td:nth-of-type(6) div")

        if len(time_cells) >= 2:
            performed_at = f"{time_cells[0].get_text(strip=True)} {time_cells[1].get_text(strip=True)}"
        else:
            performed_at = None

        all_rows.append({
            "ID": record_id,
            "Staff Name": staff_name,
            "Staff Email": staff_email,
            "Operation": operation,
            "Operation Type": operation_type,
            "Description": description,
            "IP Address": ip_address,
            "Performed At": performed_at
        })

driver.quit()

table_df = pd.DataFrame(all_rows)

table_df["ID"] = table_df["ID"].astype(str)

# -------------------------
# LOAD EXISTING SHEET
# -------------------------

existing_data = sheet.get_all_records()

if existing_data:
    existing_df = pd.DataFrame(existing_data)
    existing_df["ID"] = existing_df["ID"].astype(str)
else:
    existing_df = pd.DataFrame(columns=table_df.columns)

# -------------------------
# APPEND ONLY NEW IDs
# -------------------------

new_rows = table_df[~table_df["ID"].isin(existing_df["ID"])]

final_df = pd.concat([existing_df, new_rows], ignore_index=True)

sheet.clear()

sheet.update(
    [final_df.columns.values.tolist()] +
    final_df.values.tolist()
)

print(f"Added {len(new_rows)} new rows")
