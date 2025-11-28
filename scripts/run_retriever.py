#!/usr/bin/env python3
import os

import uvicorn

from des.retriever.service import app

if __name__ == "__main__":
    host = os.getenv("DES_RETRIEVER_HOST", "0.0.0.0")
    port = int(os.getenv("DES_RETRIEVER_PORT", "8001"))
    uvicorn.run(app, host=host, port=port)
