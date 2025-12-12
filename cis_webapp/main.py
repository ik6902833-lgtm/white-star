from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
import httpx

app = FastAPI()

# Cтраны СНГ по ISO-кодам
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


async def detect_country(ip: str) -> str | None:
    """
    По IP определяем страну через бесплатный API.
    Здесь пример с https://freeipapi.com/ (без ключа, до 60 запросов/минуту).
    """
    try:
        url = f"https://freeipapi.com/api/json/{ip}"
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        # freeipapi возвращает ISO-код страны в поле countryCode
        return data.get("countryCode")
    except Exception:
        return None


@app.post("/api/check", response_class=JSONResponse)
async def api_check(request: Request):
    """
    Эндпоинт, который читает IP пользователя и возвращает:
    { ok: true, ip: "...", country: "UA", is_cis: true/false/null }
    """
    # На Render/прокси реальный IP часто в X-Forwarded-For
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        ip = xff.split(",")[0].strip()
    else:
        ip = request.client.host

    country_code = await detect_country(ip)
    if country_code:
        country_code = country_code.upper()

    is_cis = None
    if country_code:
        is_cis = country_code in CIS_COUNTRIES

    return JSONResponse(
        {
            "ok": True,
            "ip": ip,
            "country": country_code,
            "is_cis": is_cis,
        }
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """
    Простая страница для Telegram WebApp:
    - чёрный фон
    - надпись "Проверено,перейдите в бота для дальнейших действий"
    - внутри тихонько делает POST /api/check и отправляет результат в бота
      через Telegram.WebApp.sendData(...)
    """
    html = """
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <title>Проверка страны</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      padding: 0;
      height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background: #000000;
      color: #ffffff;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      text-align: center;
    }
    .wrapper {
      max-width: 480px;
      padding: 16px;
    }
    .title {
      font-size: 20px;
      line-height: 1.5;
    }
    .status {
      margin-top: 16px;
      font-size: 14px;
      opacity: 0.7;
    }
  </style>
</head>
<body>
  <div class="wrapper">
    <div class="title">
      Проверено,перейдите в бота для дальнейших действий
    </div>
    <div class="status" id="status">
      Идёт проверка страны по IP...
    </div>
  </div>

  <script>
    (function () {
      if (!window.Telegram || !window.Telegram.WebApp) {
        document.getElementById('status').textContent =
          'Откройте эту страницу через кнопку в Telegram-боте.';
        return;
      }

      const tg = window.Telegram.WebApp;
      tg.ready();
      tg.expand();

      async function runCheck() {
        try {
          const res = await fetch('/api/check', { method: 'POST' });
          const data = await res.json();
          document.getElementById('status').textContent = 'Проверка завершена.';

          tg.sendData(JSON.stringify({
            event: 'cis_check',
            is_cis: data.is_cis,
            country: data.country || null
          }));
        } catch (e) {
          document.getElementById('status').textContent =
            'Не удалось выполнить проверку, попробуйте позже.';
          tg.sendData(JSON.stringify({
            event: 'cis_check',
            is_cis: null,
            error: String(e)
          }));
        }
      }

      runCheck();
    })();
  </script>
</body>
</html>
"""
    return HTMLResponse(html)
