#!/usr/bin/env python3
import os

import uvicorn

from des.router.service import app

if __name__ == "__main__":
    host = os.getenv("DES_ROUTER_HOST", "0.0.0.0")
    port = int(os.getenv("DES_ROUTER_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
