#!/usr/bin/env python3
"""Stock Analyzer — screening & backtesting web service."""

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from src.analyzer.api.routes import create_router
from src.analyzer.engine import AnalysisEngine

app = FastAPI(title="Stock Analyzer")
app.mount("/static", StaticFiles(directory="static"), name="static")

engine = AnalysisEngine.from_config()
app.include_router(create_router(engine))

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
