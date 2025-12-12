import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()

HTML_PAGE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <title>Проверка СНГ</title>
    <style>
        body {
            margin: 0;
            padding: 0;
            background: #000000;
            color: #ffffff;
            font-family: system-ui, -apple-system, BlinkMacSystemFont,
                         "Segoe UI", Roboto, sans-serif;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            text-align: center;
        }
        h1 {
            font-size: 32px;
            margin-bottom: 16px;
        }
        p {
            font-size: 20px;
            margin: 4px 0;
        }
    </style>
</head>
<body>
    <h1>Проверка СНГ</h1>
    <p>Проверено ✅</p>
    <p>Перейдите в бота для дальнейших действий</p>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def index():
    # Просто отдаём HTML-страницу
    return HTML_PAGE


if __name__ == "__main__":
    # Локальный запуск (на Render это обычно не используется,
    # там запускается startCommand из render.yaml)
    port = int(os.environ.get("PORT", "8000"))
    import uvicorn

    uvicorn.run("cis_webapp.main:app", host="0.0.0.0", port=port)
