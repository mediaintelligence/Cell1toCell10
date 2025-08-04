# MIZ OKI 3.0 - Multi-Agent Intelligence System

## Overview
MIZ OKI 3.0 is a multi-agent intelligence system designed for deployment on Google Cloud Run.

## Deployment

### Prerequisites
- Google Cloud Project with billing enabled
- gcloud CLI installed and configured
- Docker installed (for local testing)

### Quick Deploy

1. Set your project ID:
```bash
export PROJECT_ID=your-gcp-project-id
```

2. Build and deploy using Cloud Build:
```bash
gcloud builds submit --config cloudbuild.yaml --substitutions=_PROJECT_ID=$PROJECT_ID
```

### Manual Deployment

1. Build the Docker image:
```bash
docker build -t gcr.io/$PROJECT_ID/miz-oki-app:latest .
```

2. Push to Container Registry:
```bash
docker push gcr.io/$PROJECT_ID/miz-oki-app:latest
```

3. Deploy to Cloud Run:
```bash
gcloud run deploy miz-oki-app \
  --image gcr.io/$PROJECT_ID/miz-oki-app:latest \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

## Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
uvicorn app.fastapi_app:app --reload --port 8080
```

3. Access the API:
- API: http://localhost:8080
- Docs: http://localhost:8080/docs
- Health: http://localhost:8080/health

## API Endpoints

- `GET /` - Root endpoint
- `GET /health` - Health check
- `GET /status` - System status
- `POST /process` - Process a task

## Environment Variables

- `GCP_PROJECT_ID` - Google Cloud Project ID
- `ENVIRONMENT` - Environment (development/production)
- `LOG_LEVEL` - Logging level (INFO/DEBUG)
- `PORT` - Port to run the service (default: 8080)