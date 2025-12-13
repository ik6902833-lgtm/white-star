import os
import sqlite3
import datetime
from typing import Optional
import json
import urllib.request
import urllib.error

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

# Базовая папка этого модуля
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Путь к БД с результатами проверки
# По умолчанию кладём БД рядом с main.py, чтобы не было проблем с правами на /data
DB_PATH = os.getenv("CIS_DB_PATH", os.path.join(BASE_DIR, "cis_checks.db"))

# Список стран СНГ / ближнего зарубежья по ISO-кодам
CIS_COUNTRIES = {
    "RU",  # Россия
    "UA",  # Украина
    "BY",  # Беларусь
    "KZ",  # Казахстан
    "KG",  # Кыргызстан
    "UZ",  # Узбекистан
    "TJ",  # Таджикистан
    "AZ",  # Азербайджан
    "AM",  # Армения
    "MD",  # Молдова
    "GE",  # Грузия
    "TM",  # Туркменистан
}

INDEX_PATH = os.path.join(BASE_DIR, "index.html")

app = FastAPI(title="CIS WebApp Checker")

# Разрешаем CORS на всякий случай (если будешь вызывать из фронта)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- ИНИЦИАЛИЗАЦИЯ БД ----------

def init_db() -> None:
    """
    Создаём SQLite-базу в DB_PATH (папка уже существует, т.к. это BASE_DIR).
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cis_checks (
            user_id    INTEGER PRIMARY KEY,
            ip         TEXT,
            country    TEXT,
            is_cis     INTEGER,      -- 1 = да, 0 = нет, -1 = неизвестно
            checked_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()


@app.on_event("startup")
def on_startup() -> None:
    init_db()

# ---------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ----------

def get_client_ip(request: Request) -> Optional[str]:
    """
    Вытаскиваем IP пользователя с учётом прокси Render (X-Forwarded-For).
    НИЧЕГО не режем — всегда возвращаем то, что есть.
    """
    xff = (
        request.headers.get("x-forwarded-for")
        or request.headers.get("X-Forwarded-For")
    )
    if xff:
        # может быть "ip1, ip2, ip3"
        return xff.split(",")[0].strip()

    if request.client:
        return request.client.host

    return None


def get_country_by_ip(ip: Optional[str]) -> Optional[str]:
    """
    Определяем страну по IP через внешний сервис.
    Используем https://ipapi.co/{ip}/json/ и НИЧЕГО не режем,
    чтобы по-любому попытаться получить страну.
    """
    if not ip:
        return None

    url = f"https://ipapi.co/{ip}/json/"

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "cis-checker/1.0"},
        )
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            if resp.status != 200:
                return None
            data = json.loads(
                resp.read().decode("utf-8", errors="ignore")
            )
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError):
        return None

    code = data.get("country_code")
    if isinstance(code, str) and len(code) == 2:
        return code.upper()
    return None


def save_result(
    user_id: int,
    ip: Optional[str],
    country: Optional[str],
    is_cis: Optional[bool],
) -> None:
    """
    Сохраняем результат проверки в БД.
    is_cis:
      True  -> 1
      False -> 0
      None  -> -1 (не удалось определить)
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    if is_cis is True:
        is_cis_val = 1
    elif is_cis is False:
        is_cis_val = 0
    else:
        is_cis_val = -1

    cur.execute(
        """
        INSERT INTO cis_checks(user_id, ip, country, is_cis, checked_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            ip         = excluded.ip,
            country    = excluded.country,
            is_cis     = excluded.is_cis,
            checked_at = excluded.checked_at
        """,
        (
            user_id,
            ip or "",
            country or "",
            is_cis_val,
            datetime.datetime.utcnow().isoformat(),
        ),
    )
    conn.commit()
    conn.close()


def load_status(user_id: int):
    """
    Возвращает кортеж (ip, country, is_cis, checked_at) или None, если записи нет.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT ip, country, is_cis, checked_at FROM cis_checks WHERE user_id=?",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row

# ---------- РОУТЫ ----------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, uid: Optional[int] = Query(None)):
    """
    Главная страница проверки.
    * uid — ID пользователя Телеграма, который передаёт бот (персональная ссылка).
    * По IP определяем страну, считаем is_cis и сохраняем в БД.
    * Возвращаем твой статический index.html (чёрный фон, надпись и т.д.).
    """

    ip = get_client_ip(request)
    country = get_country_by_ip(ip)
    is_cis = None

    if country is not None:
        is_cis = country in CIS_COUNTRIES

    if uid is not None:
        # ВСЕГДА сохраняем IP и то, что удалось определить по стране
        save_result(uid, ip, country, is_cis)

    # Отдаём твой index.html, если он есть
    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)

    # Запасной вариант, если файла нет
    fallback_html = """
    <html>
      <head>
        <meta charset="utf-8" />
        <title>Проверено</title>
        <style>
          body {
            margin: 0;
            padding: 0;
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100vh;
            background-color: #000000;
            color: #ffffff;
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
          }
        </style>
      </head>
      <body>
        <h1>Проверено, перейдите в бота для дальнейших действий</h1>
      </body>
    </html>
    """
    return HTMLResponse(content=fallback_html)


@app.get("/api/status/{user_id}", response_class=JSONResponse)
async def api_status(user_id: int):
    """
    Эндпоинт для бота.

    Возвращает, что знаем о пользователе:
    {
      "user_id": 123,
      "checked": true/false,
      "is_cis": true/false/null,
      "country": "UA" / "RU" / ... / null,
      "ip": "1.2.3.4" / null,
      "checked_at": "2025-12-12T..."
    }
    """

    row = load_status(user_id)
    if not row:
        return JSONResponse(
            {
                "user_id": user_id,
                "checked": False,
                "is_cis": None,
                "country": None,
                "ip": None,
                "checked_at": None,
            }
        )

    ip, country, is_cis_val, checked_at = row

    if is_cis_val == 1:
        is_cis = True
    elif is_cis_val == 0:
        is_cis = False
    else:
        is_cis = None

    return JSONResponse(
        {
            "user_id": user_id,
            "checked": True,
            "is_cis": is_cis,
            "country": country or None,
            "ip": ip or None,
            "checked_at": checked_at,
        }
    )
