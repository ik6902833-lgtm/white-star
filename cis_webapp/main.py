import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def index():
    # Страница с чёрным фоном и текстом
    return """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8" />
    <title>Проверка СНГ</title>
    <style>
        body {
            margin: 0;
            padding: 0;
            background-color: #000000;
            color: #ffffff;
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            text-align: center;
        }
        .container {
            max-width: 480px;
            padding: 24px;
        }
        h1 {
            font-size: 28px;
            margin-bottom: 16px;
        }
        p {
            font-size: 18px;
            line-height: 1.5;
            margin: 8px 0;
        }
        .ok {
            font-size: 22px;
            margin-top: 12px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Проверка СНГ</h1>
        <p class="ok">Проверено ✅</p>
        <p>Перейдите в бота для дальнейших действий.</p>
    </div>
</body>
</html>
    """


if __name__ == "__main__":
    # Локальный запуск (на своём ПК)
    port = int(os.environ.get("PORT", 8000))
    import uvicorn
    uvicorn.run("cis_webapp.main:app", host="0.0.0.0", port=port)
