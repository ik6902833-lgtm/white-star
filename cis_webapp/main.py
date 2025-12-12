import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()


@app.get("/", response_class=HTMLResponse)
async def index():
    # Простая страница с чёрным фоном и текстом
    return """
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Проверка СНГ</title>
        <style>
            html, body {
                margin: 0;
                padding: 0;
                width: 100%;
                height: 100%;
                background-color: #000000;
                color: #ffffff;
                font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            }
            .wrapper {
                width: 100%;
                height: 100%;
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                text-align: center;
                padding: 16px;
                box-sizing: border-box;
            }
            .title {
                font-size: 24px;
                font-weight: 600;
                margin-bottom: 12px;
            }
            .subtitle {
                font-size: 16px;
                opacity: 0.8;
            }
        </style>
    </head>
    <body>
        <div class="wrapper">
            <div class="title">Проверено ✅</div>
            <div class="subtitle">
                Перейдите в бота для дальнейших действий
            </div>
        </div>
    </body>
    </html>
    """


if __name__ == "__main__":
    # Важно: Render подставляет PORT в переменную окружения
    port = int(os.environ.get("PORT", 8000))
    # Запускаем uvicorn так, как хочет Render
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=port)
