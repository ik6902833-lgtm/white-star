# cis_webapp/main.py
import os
import sqlite3
import datetime
from typing import Optional, Dict, Any
import json
import urllib.request
import urllib.error

from fastapi import FastAPI, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

# ================== НАСТРОЙКИ ==================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# БД кладём рядом с main.py по умолчанию
DB_PATH = os.getenv("CIS_DB_PATH", os.path.join(BASE_DIR, "cis_checks.db"))

INDEX_PATH = os.path.join(BASE_DIR, "index.html")

# СНГ / ближнее зарубежье (включая Украину)
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

# ISO numeric (на случай если боту удобнее проверять цифрой)
ISO_NUMERIC = {
    "UA": "804",
    "RU": "643",
    "BY": "112",
    "KZ": "398",
    "KG": "417",
    "MD": "498",
    "AM": "051",
    "AZ": "031",
    "TJ": "762",
    "TM": "795",
    "UZ": "860",
    "GE": "268",
}

app = FastAPI(title="CIS WebApp Checker")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ================== БАЗА ДАННЫХ ==================

def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cis_checks (
            user_id              INTEGER PRIMARY KEY,
            ip                   TEXT,
            country_code         TEXT,
            country_name         TEXT,
            country_code_numeric TEXT,
            is_cis               INTEGER,      -- 1 = да, 0 = нет, -1 = неизвестно
            checked_at           TEXT,
            raw_json             TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def ensure_column(table: str, column: str, ddl: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if column not in cols:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
            conn.commit()
        except Exception:
            pass
    conn.close()


@app.on_event("startup")
def on_startup() -> None:
    init_db()
    # миграции на случай старой базы
    ensure_column("cis_checks", "country_code", "country_code TEXT")
    ensure_column("cis_checks", "country_name", "country_name TEXT")
    ensure_column("cis_checks", "country_code_numeric", "country_code_numeric TEXT")
    ensure_column("cis_checks", "raw_json", "raw_json TEXT")


# ================== ВСПОМОГАТЕЛЬНОЕ ==================

def get_client_ip(request: Request) -> Optional[str]:
    """
    Берём IP с учётом прокси (Render/CF и т.п.).
    Ничего не “обрезаем” кроме списка XFF — берём первый.
    """
    headers = request.headers

    for key in (
        "cf-connecting-ip",
        "true-client-ip",
        "x-real-ip",
        "x-forwarded-for",
        "X-Forwarded-For",
    ):
        v = headers.get(key)
        if v:
            # может быть "ip1, ip2, ip3"
            return v.split(",")[0].strip()

    if request.client:
        return request.client.host
    return None


def fetch_geo_by_ip(ip: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Определяем страну через ipapi.co
    - если ip есть: https://ipapi.co/{ip}/json/
    - если ip нет: https://ipapi.co/json/
    """
    url = f"https://ipapi.co/{ip}/json/" if ip else "https://ipapi.co/json/"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "cis-checker/2.0"})
        with urllib.request.urlopen(req, timeout=6.0) as resp:
            if getattr(resp, "status", 200) != 200:
                return None
            raw = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError):
        return None
    except Exception:
        return None

    return None


def normalize_country_code(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    s = str(code).strip().upper()
    if len(s) == 2:
        return s
    # иногда сервисы отдают "UKR" — приведём к UA
    if s == "UKR":
        return "UA"
    return None


def compute_country_numeric(code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    code = code.upper()
    return ISO_NUMERIC.get(code)


def save_result(
    user_id: int,
    ip: Optional[str],
    country_code: Optional[str],
    country_name: Optional[str],
    country_numeric: Optional[str],
    is_cis: Optional[bool],
    raw_json: Optional[dict],
) -> None:
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
        INSERT INTO cis_checks(
            user_id, ip, country_code, country_name, country_code_numeric,
            is_cis, checked_at, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            ip                   = excluded.ip,
            country_code          = excluded.country_code,
            country_name          = excluded.country_name,
            country_code_numeric  = excluded.country_code_numeric,
            is_cis                = excluded.is_cis,
            checked_at            = excluded.checked_at,
            raw_json              = excluded.raw_json
        """,
        (
            int(user_id),
            ip or "",
            country_code or "",
            country_name or "",
            country_numeric or "",
            is_cis_val,
            datetime.datetime.utcnow().isoformat(),
            json.dumps(raw_json, ensure_ascii=False) if raw_json else "",
        ),
    )
    conn.commit()
    conn.close()


def load_status(user_id: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            ip, country_code, country_name, country_code_numeric,
            is_cis, checked_at, raw_json
        FROM cis_checks
        WHERE user_id=?
        """,
        (int(user_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row


# ================== РОУТЫ ==================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request, uid: Optional[int] = Query(None)):
    """
    Страница проверки:
    - берём IP
    - запрашиваем geo
    - вычисляем is_cis
    - сохраняем в БД по uid (если uid передан)
    - возвращаем index.html
    """
    ip = get_client_ip(request)
    geo = fetch_geo_by_ip(ip)

    country_code = None
    country_name = None
    country_numeric = None
    is_cis: Optional[bool] = None

    if geo:
        country_code = normalize_country_code(geo.get("country_code") or geo.get("country"))
        country_name = (geo.get("country_name") or geo.get("country") or "").strip() or None
        country_numeric = compute_country_numeric(country_code) or None

        if country_code is not None:
            is_cis = country_code in CIS_COUNTRIES
        else:
            is_cis = None

    if uid is not None:
        save_result(
            user_id=int(uid),
            ip=ip,
            country_code=country_code,
            country_name=country_name,
            country_numeric=country_numeric,
            is_cis=is_cis,
            raw_json=geo,
        )

    if os.path.exists(INDEX_PATH):
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())

    return HTMLResponse(
        content="""
        <html>
          <head>
            <meta charset="utf-8" />
            <title>Проверено</title>
            <style>
              body {
                margin: 0; padding: 0;
                display: flex; align-items: center; justify-content: center;
                height: 100vh; background: #000; color: #fff;
                font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
                text-align: center;
              }
            </style>
          </head>
          <body>
            <h1>Проверено, перейдите в бота для дальнейших действий</h1>
          </body>
        </html>
        """.strip()
    )


@app.get("/api/status/{user_id}", response_class=JSONResponse)
async def api_status(user_id: int):
    """
    Эндпоинт для бота.

    Чтобы твой бот 100% не путался, отдаём сразу несколько “синонимов” полей:
      - checked (bool)
      - is_cis (bool|null) + cis (bool|null)
      - country_code + cc + iso
      - country_code_numeric (например 804)
      - country_name + country
    """
    row = load_status(user_id)
    if not row:
        return JSONResponse(
            {
                "user_id": user_id,
                "checked": False,

                "is_cis": None,
                "cis": None,

                "country_code": None,
                "cc": None,
                "iso": None,

                "country_code_numeric": None,
                "numeric": None,

                "country_name": None,
                "country": None,

                "ip": None,
                "checked_at": None,
            }
        )

    ip, country_code, country_name, country_numeric, is_cis_val, checked_at, raw_json = row

    if is_cis_val == 1:
        is_cis = True
    elif is_cis_val == 0:
        is_cis = False
    else:
        is_cis = None

    cc = country_code or None
    cn = country_name or None
    num = country_numeric or None

    # raw_json можно не отдавать (чтобы не раздувать ответ), но можно включить при отладке
    # raw = json.loads(raw_json) if raw_json else None

    return JSONResponse(
        {
            "user_id": user_id,
            "checked": True,

            "is_cis": is_cis,
            "cis": is_cis,

            "country_code": cc,
            "cc": cc,
            "iso": cc,

            "country_code_numeric": num,
            "numeric": num,

            "country_name": cn,
            "country": cn if cn else cc,  # иногда боту удобнее получить хоть что-то

            "ip": ip or None,
            "checked_at": checked_at,
        }
    )
