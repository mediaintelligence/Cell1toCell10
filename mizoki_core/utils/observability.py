from prometheus_client import Counter, Histogram, Gauge

REQUESTS = Counter('mizoki_requests_total', 'Total requests', ['svc','endpoint','method','status'])
DURATION = Histogram('mizoki_request_duration_seconds', 'Request duration', ['svc','endpoint'])
ACTIVE = Gauge('mizoki_active', 'Active in-flight requests', ['svc'])

def count(svc: str, endpoint: str, method: str, status: str):
    REQUESTS.labels(svc, endpoint, method, status).inc()

def observe(svc: str, endpoint: str, seconds: float):
    DURATION.labels(svc, endpoint).observe(seconds)

def inflight_inc(svc: str): ACTIVE.labels(svc).inc()

def inflight_dec(svc: str): ACTIVE.labels(svc).dec()