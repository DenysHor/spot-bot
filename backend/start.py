import os

import uvicorn

from app.core.config import settings


if __name__ == "__main__":
    port = int(os.getenv("PORT", str(settings.app_port)))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, workers=1)
