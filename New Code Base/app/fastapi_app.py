from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Any, Optional
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="MIZ OKI 3.0",
    description="Multi-Agent Intelligence System API",
    version="3.0.0"
)

# Request/Response models
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    service: str
    version: str
    environment: str

class TaskRequest(BaseModel):
    task_type: str
    data: Dict[str, Any]
    options: Optional[Dict[str, Any]] = {}

class TaskResponse(BaseModel):
    task_id: str
    status: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: str

# Health check endpoint
@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint for Cloud Run"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat(),
        service="miz-oki-app",
        version="3.0.0",
        environment=os.getenv("ENVIRONMENT", "development")
    )

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "MIZ OKI 3.0 - Multi-Agent Intelligence System",
        "version": "3.0.0",
        "endpoints": {
            "health": "/health",
            "docs": "/docs",
            "openapi": "/openapi.json"
        }
    }

# Process task endpoint
@app.post("/process", response_model=TaskResponse)
async def process_task(request: TaskRequest):
    """Process a task using the multi-agent system"""
    try:
        logger.info(f"Processing task: {request.task_type}")
        
        # TODO: Integrate with the actual multi-agent system
        # For now, return a mock response
        result = {
            "task_type": request.task_type,
            "data_received": request.data,
            "options": request.options,
            "processing_time": "0.5s",
            "agents_used": ["coordinator", "analyzer", "executor"]
        }
        
        return TaskResponse(
            task_id=f"task_{datetime.utcnow().timestamp()}",
            status="completed",
            result=result,
            timestamp=datetime.utcnow().isoformat()
        )
        
    except Exception as e:
        logger.error(f"Error processing task: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# System status endpoint
@app.get("/status")
async def system_status():
    """Get system status"""
    return {
        "status": "operational",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "api": "healthy",
            "agents": "ready",
            "orchestration": "active"
        },
        "environment": {
            "project_id": os.getenv("GCP_PROJECT_ID", "not-set"),
            "environment": os.getenv("ENVIRONMENT", "development"),
            "log_level": os.getenv("LOG_LEVEL", "INFO")
        }
    }

# Error handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "timestamp": datetime.utcnow().isoformat()
        }
    )

# Startup event
@app.on_event("startup")
async def startup_event():
    logger.info("MIZ OKI 3.0 starting up...")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    logger.info(f"Project ID: {os.getenv('GCP_PROJECT_ID', 'not-set')}")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("MIZ OKI 3.0 shutting down...")

# For running with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))