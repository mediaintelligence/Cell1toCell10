import asyncio
import logging
import os
from typing import Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google.cloud import pubsub_v1
from prometheus_client import Counter, Histogram, make_asgi_app
import uvicorn
import httpx

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter('cell_requests_total', 'Total requests to cells', ['cell_id'])
REQUEST_LATENCY = Histogram('cell_request_latency_seconds', 'Request latency', ['cell_id'])

# FastAPI app
app = FastAPI(title="MIZ OKI 3.0 Cell Launcher")

# Pydantic model for cell registration
class CellRegistration(BaseModel):
    cell_id: str
    endpoint: str
    status: str = "active"

# In-memory cell registry (replace with Firestore in production, e.g., using google.cloud.firestore)
CELL_REGISTRY: Dict[str, CellRegistration] = {}

# GCP Pub/Sub setup (optional)
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
USE_PUBSUB = bool(PROJECT_ID and PROJECT_ID != "your-project-id")
if USE_PUBSUB:
    TOPIC_NAME = "cell-communication"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
else:
    logger.warning("GCP_PROJECT_ID not set properly; skipping Pub/Sub integration.")

# HTTP client for A2A communication
async_client = httpx.AsyncClient()

@app.post("/api/v1/register")
async def register_cell(cell: CellRegistration):
    """Register a cell with the launcher."""
    with REQUEST_LATENCY.labels(cell_id=cell.cell_id).time():
        CELL_REGISTRY[cell.cell_id] = cell
        REQUEST_COUNT.labels(cell_id=cell.cell_id).inc()
        logger.info(f"Registered cell: {cell.cell_id} at {cell.endpoint}")
        
        # Publish registration event to Pub/Sub if enabled
        if USE_PUBSUB:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: publisher.publish(topic_path, f"Cell {cell.cell_id} registered".encode())
                )
            except Exception as e:
                logger.error(f"Failed to publish to Pub/Sub: {e}")
        
        return {"status": "success", "cell_id": cell.cell_id}

@app.get("/api/v1/health")
async def health_check():
    """Check health of all registered cells in parallel."""
    async def check_single(cell_id: str, endpoint: str):
        try:
            response = await async_client.get(f"{endpoint}/health")
            return cell_id, response.json()
        except Exception as e:
            return cell_id, {"status": "error", "message": str(e)}

    tasks = [check_single(cell_id, cell.endpoint) for cell_id, cell in CELL_REGISTRY.items()]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    
    results = {}
    for result in results_list:
        if isinstance(result, Exception):
            logger.error(f"Health check error: {result}")
            continue  # Skip or handle broadly
        cell_id, health = result
        results[cell_id] = health
    
    return results

@app.get("/api/v1/cells")
async def list_cells():
    """List all registered cells."""
    return {"cells": [cell.dict() for cell in CELL_REGISTRY.values()]}

# Prometheus metrics endpoint
prometheus_app = make_asgi_app()
app.mount("/metrics", prometheus_app)

@app.on_event("startup")
async def startup():
    logger.info("Starting MIZ OKI 3.0 Cell Launcher... (cells will register dynamically)")

@app.on_event("shutdown")
async def shutdown():
    """Cleanup on shutdown."""
    await async_client.aclose()
    logger.info("Shutting down MIZ OKI 3.0 Cell Launcher...")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
