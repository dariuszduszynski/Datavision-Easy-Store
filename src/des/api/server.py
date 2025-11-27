from fastapi import FastAPI, HTTPException

app = FastAPI(title="Datavision Easy Store API", version="1.0.0")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/files/{file_id}")
async def get_file(file_id: str):
    # Na razie prosty stub:
    if file_id == "demo":
        return {"file_id": file_id, "status": "ok", "content": "placeholder"}
    raise HTTPException(status_code=404, detail="file not found")
