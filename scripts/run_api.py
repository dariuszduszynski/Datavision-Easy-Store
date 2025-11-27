import os

import uvicorn
from des.api.server import app


def main() -> None:
    host = os.getenv("DES_API_HOST", "127.0.0.1")
    port = int(os.getenv("DES_API_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
