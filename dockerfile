# docker-compose.yml for MoA System

version: ‘3.8’

services:
moa-orchestrator:
build:
context: .
dockerfile: Dockerfile
environment:
- ENVIRONMENT=production
- LOG_LEVEL=INFO
- PROJECT_ID=${GCP_PROJECT_ID}
- ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
- OPENAI_API_KEY=${OPENAI_API_KEY}
ports:
- “8000:8000”
volumes:
- ./logs:/app/logs
- ./config:/app/config
depends_on:
- redis
- postgres
- neo4j
restart: unless-stopped
deploy:
resources:
limits:
memory: 4G
cpus: ‘2’
reservations:
memory: 2G
cpus: ‘1’

redis:
image: redis:7-alpine
ports:
- “6379:6379”
volumes:
- redis_data:/data
command: redis-server –appendonly yes
restart: unless-stopped

postgres:
image: postgres:15
environment:
- POSTGRES_DB=moa_system
- POSTGRES_USER=moa_user
- POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
ports:
- “5432:5432”
volumes:
- postgres_data:/var/lib/postgresql/data
- ./init.sql:/docker-entrypoint-initdb.d/init.sql
restart: unless-stopped

neo4j:
image: neo4j:5
environment:
- NEO4J_AUTH=neo4j/${NEO4J_PASSWORD}
- NEO4J_PLUGINS=[“graph-data-science”]
ports:
- “7474:7474”
- “7687:7687”
volumes:
- neo4j_data:/data
- neo4j_logs:/logs
restart: unless-stopped

prometheus:
image: prom/prometheus
ports:
- “9090:9090”
volumes:
- ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
- prometheus_data:/prometheus
restart: unless-stopped

grafana:
image: grafana/grafana
ports:
- “3000:3000”
environment:
- GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD}
volumes:
- grafana_data:/var/lib/grafana
- ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards
restart: unless-stopped

volumes:
redis_data:
postgres_data:
neo4j_data:
neo4j_logs:
prometheus_data:
grafana_data:

-----

# Kubernetes deployment.yaml

apiVersion: apps/v1
kind: Deployment
metadata:
name: moa-orchestrator
namespace: moa-system
spec:
replicas: 3
selector:
matchLabels:
app: moa-orchestrator
template:
metadata:
labels:
app: moa-orchestrator
spec:
containers:
- name: moa-orchestrator
image: moa-system:latest
ports:
- containerPort: 8000
env:
- name: ENVIRONMENT
value: “production”
- name: PROJECT_ID
valueFrom:
secretKeyRef:
name: moa-secrets
key: project-id
- name: ANTHROPIC_API_KEY
valueFrom:
secretKeyRef:
name: moa-secrets
key: anthropic-key
resources:
requests:
memory: “2Gi”
cpu: “1000m”
limits:
memory: “4Gi”
cpu: “2000m”
livenessProbe:
httpGet:
path: /health
port: 8000
initialDelaySeconds: 30
periodSeconds: 10
readinessProbe:
httpGet:
path: /ready
port: 8000
initialDelaySeconds: 5
periodSeconds: 5

-----

# Service configuration

apiVersion: v1
kind: Service
metadata:
name: moa-orchestrator-service
namespace: moa-system
spec:
selector:
app: moa-orchestrator
ports:
- protocol: TCP
port: 80
targetPort: 8000
type: LoadBalancer

-----

# Horizontal Pod Autoscaler

apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
name: moa-orchestrator-hpa
namespace: moa-system
spec:
scaleTargetRef:
apiVersion: apps/v1
kind: Deployment
name: moa-orchestrator
minReplicas: 2
maxReplicas: 10
metrics:

- type: Resource
  resource:
  name: cpu
  target:
  type: Utilization
  averageUtilization: 70
- type: Resource
  resource:
  name: memory
  target:
  type: Utilization
  averageUtilization: 80