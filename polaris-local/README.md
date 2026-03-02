# Polaris local (no Auth0)

Deploy **only** Apache Polaris for local testing. No oauth2-proxy, no secrets.

## Deploy (from this repo root)

```bash
kubectl create namespace polaris-local --dry-run=client -o yaml | kubectl apply -f -
kustomize build --enable-helm polaris-local | kubectl apply -f -
```

Wait for the pod: `kubectl get pods -n polaris-local -w` (Ctrl+C when `polaris-*` is Running).

## Test

**1. Port-forward** (run in one terminal; leave it running):

```bash
# Catalog API (Iceberg REST)
kubectl port-forward -n polaris-local svc/polaris 8181:8181
```

In a second terminal, optionally also forward the **management** port (health/readiness):

```bash
kubectl port-forward -n polaris-local svc/polaris-mgmt 8182:8182
```

**2. Verify the server is up**

```bash
# Health/readiness (Quarkus) – should return JSON
curl -s http://localhost:8182/q/health
# or
curl -s http://localhost:8182/q/health/ready
```

**3. Catalog API (`/v1/config` may 404 until catalog is bootstrapped)**

With in-memory storage, `GET /v1/config` often returns **404** until the catalog is bootstrapped. A JSON 404 from the server still means Polaris is running.

```bash
curl -s http://localhost:8181/v1/config
# If 404: bootstrap once (see below), then retry
```

**4. Optional – bootstrap catalog so `/v1/config` works**

Run the Polaris admin bootstrap once (creates default catalog in the in-memory store):

```bash
kubectl run polaris-bootstrap \
  -n polaris-local \
  --image=apache/polaris-admin-tool:latest \
  --restart=Never \
  --rm -it \
  -- env QUARKUS_HTTP_HOST=0.0.0.0 \
  bootstrap -r POLARIS -c POLARIS,default,default -p
```

(You may need to point it at the Polaris service, e.g. `--server=http://polaris.polaris-local.svc.cluster.local:8181` if the admin tool supports it; otherwise run with port-forward and use `http://host.docker.internal:8181` or similar from the job.)

After bootstrap, retry: `curl -s http://localhost:8181/v1/config`

**5. Use with Spark/Trino**  
Catalog type `rest`, URI `http://localhost:8181` (with port-forward running).

## Cleanup

```bash
kubectl delete namespace polaris-local
```

# Apache Polaris (Iceberg REST catalog) – local setup for dev team

This repo includes a **polaris-local** overlay to run [Apache Polaris](https://polaris.apache.org/) (Iceberg REST catalog) on your local Kubernetes (e.g. Docker Desktop, minikube) **without Auth0** – for development and testing.

## What’s included

- **polaris-local/** – Kustomize overlay that deploys Polaris 1.3.0-incubating with:
  - In-memory storage (no DB required)
  - No ingress, no oauth2-proxy, no secrets
  - Single replica, minimal resources

## Prerequisites

- `kubectl` configured for your cluster
- `kustomize` with Helm support (e.g. `kustomize build --enable-helm`), or use `kubectl kustomize` if your version supports `--enable-helm`

## Deploy (from this repo root)

```bash
# From: /Users/hongong/Working/ongxuanhong/de07-iceberg-vs-delta (or your clone root)

kubectl create namespace polaris-local --dry-run=client -o yaml | kubectl apply -f -
kustomize build --enable-helm polaris-local | kubectl apply -f -
```

Wait for the pod:

```bash
kubectl get pods -n polaris-local -w
```

Press Ctrl+C when the `polaris-*` pod is **Running**.

## Test the catalog

### 1. Port-forward (leave running in a terminal)

```bash
# Iceberg REST catalog API
kubectl port-forward -n polaris-local svc/polaris 8181:8181
```

Optional – management/health port:

```bash
kubectl port-forward -n polaris-local svc/polaris-mgmt 8182:8182
```

### 2. Health check

```bash
curl -s http://localhost:8182/q/health
# or
curl -s http://localhost:8182/q/health/ready
```

### 3. Catalog API

With in-memory storage, `GET /v1/config` may return **404** until the catalog is bootstrapped. That still means Polaris is up.

```bash
curl -s http://localhost:8181/v1/config
```

### 4. Bootstrap catalog (optional)

Run once to create the default catalog so `/v1/config` returns config:

```bash
kubectl run polaris-bootstrap \
  -n polaris-local \
  --image=apache/polaris-admin-tool:latest \
  --restart=Never \
  --rm -it \
  -- env QUARKUS_HTTP_HOST=0.0.0.0 \
  bootstrap -r POLARIS -c POLARIS,default,default -p
```

If the admin tool needs the server URL, use the service DNS (e.g. `--server=http://polaris.polaris-local.svc.cluster.local:8181`) or, with port-forward, `http://host.docker.internal:8181`.

Then retry: `curl -s http://localhost:8181/v1/config`

### 5. Use with Spark / Trino

- **Catalog type:** `rest`
- **URI:** `http://localhost:8181` (with port-forward running)

## Cleanup

```bash
kubectl delete namespace polaris-local
```

## Reference

- **Polaris docs:** https://polaris.apache.org/
- **Helm chart:** the overlay uses the official chart from `https://downloads.apache.org/incubator/polaris/helm-chart` (version 1.3.0-incubating).
