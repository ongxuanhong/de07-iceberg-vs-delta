# USER GUIDE: Polaris Catalog cho 2 Team (DE/DS)

Tài liệu này tổng hợp cách setup và kiểm thử mô hình phân quyền dữ liệu theo team:

- **DE team**: quản lý `bronze_catalog`, `silver_catalog`
- **DS team**: đọc `silver_catalog`, ghi `gold_catalog`

Phân quyền chính nằm ở **Polaris** (không phụ thuộc rule thủ công trong Trino).

---

## 1) Kiến trúc đang dùng

- Object storage: **MinIO** (`minio`, bucket `bronze/silver/gold`)
- Catalog service: **Polaris** (`http://localhost:8181`)
- SQL engine test: **Trino**
  - `bronze_de` -> Polaris `bronze_catalog`
  - `silver_de` -> Polaris `silver_catalog`
  - `silver_ds` -> Polaris `silver_catalog` + `READ_ONLY`
  - `gold_ds` -> Polaris `gold_catalog`

---

## 2) Prerequisites

Chạy trong thư mục:

```bash
cd polaris-local
```

Yêu cầu local:

- Docker / Docker Compose
- `curl`
- `jq`

Biến quan trọng:

- Root Polaris credential (mặc định trong script):
  - `ROOT_CLIENT_ID=root`
  - `ROOT_CLIENT_SECRET=s3cr3t`
- Realm:
  - `POLARIS_REALM=POLARIS`

---

## 3) Khởi động stack

```bash
docker compose -f docker-compose.yaml up -d
docker compose -f docker-compose.yaml ps
```

Kiểm tra nhanh:

- MinIO UI: `http://localhost:9001`
- Polaris health: `http://localhost:8182/q/health`

---

## 4) Provision Polaris cho 2 team

Script chính: `polaris_management.sh`

Nó sẽ:

1. Tạo catalog (nếu chưa có): `bronze_catalog`, `silver_catalog`, `gold_catalog`
2. Tạo principal: `team_de`, `team_ds`
3. Tạo principal roles:
   - `team_role_de`
   - `team_role_ds`
4. Tạo catalog roles:
   - `bronze_role_de`, `silver_role_de`
   - `silver_read_role_ds`, `gold_write_role_ds`
5. Gán role mapping + grant privilege

Chạy:

```bash
./polaris_management.sh
```

Nếu cần rotate credential cho `team_de` / `team_ds`:

```bash
RECREATE_TEAM_PRINCIPALS=true ./polaris_management.sh
```

Sau khi rotate, script sẽ in ra:

- `team_de CLIENT_ID / CLIENT_SECRET`
- `team_ds CLIENT_ID / CLIENT_SECRET`

---

## 5) Cập nhật credential vào catalog connectors

Sau khi rotate, cập nhật vào các file:

- `trino/catalog/bronze_de.properties`
- `trino/catalog/silver_de.properties`
  - dùng credential của `team_de`
- `trino/catalog/silver_ds.properties`
- `trino/catalog/gold_ds.properties`
  - dùng credential của `team_ds`

Reload Trino:

```bash
docker compose -f docker-compose.yaml restart trino
```

---

## 6) Test compliance 2 team (qua Trino)

Vào Trino CLI:

```bash
docker exec -it trino trino
```

### 6.1 DE team expectations

`bronze_de`: đọc/ghi được

```sql
CREATE SCHEMA IF NOT EXISTS bronze_de.raw;
CREATE TABLE IF NOT EXISTS bronze_de.raw.events (id BIGINT, amount DOUBLE);
INSERT INTO bronze_de.raw.events VALUES (1, 10.0), (2, 20.0);
SELECT * FROM bronze_de.raw.events;
```

`silver_de`: đọc/ghi được

```sql
CREATE SCHEMA IF NOT EXISTS silver_de.curated;
CREATE TABLE IF NOT EXISTS silver_de.curated.daily_totals AS
SELECT id, amount FROM bronze_de.raw.events;
SELECT * FROM silver_de.curated.daily_totals;
```

### 6.2 DS team expectations

`silver_ds`: chỉ đọc

```sql
SELECT * FROM silver_ds.curated.daily_totals;
```

Thử ghi vào `silver_ds` phải fail:

```sql
INSERT INTO silver_ds.curated.daily_totals VALUES (3, 30.0);
```

`gold_ds`: đọc/ghi được

```sql
CREATE SCHEMA IF NOT EXISTS gold_ds.analytics;
CREATE TABLE IF NOT EXISTS gold_ds.analytics.model_output AS
SELECT * FROM silver_ds.curated.daily_totals;
SELECT * FROM gold_ds.analytics.model_output;
```

---

## 7) API verify trực tiếp trên Polaris (không qua Trino)

Lấy root token:

```bash
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL" \
  | jq -r '.access_token')
```

List catalogs:

```bash
curl -s http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Polaris-Realm: POLARIS" | jq
```

---

## 8) Troubleshooting nhanh

### A. `trino` restart liên tục / container dead

- Kiểm tra log:

```bash
docker compose -f docker-compose.yaml logs --tail=200 trino
```

- Lỗi thường gặp khi bật auth:
  - thiếu `internal-communication.shared-secret` trong `trino/config.properties`

### B. `404` khi script gọi API catalog-role

- Do catalog chưa tồn tại.
- Đảm bảo script đã chạy phần `Ensuring catalogs exist...`

### C. DS không đọc được `silver`

- Kiểm tra grant của `silver_read_role_ds` trong Polaris
- Kiểm tra file `trino/catalog/silver_ds.properties` có `iceberg.security=READ_ONLY`

---

## 9) Khuyến nghị vận hành

- Dùng `polaris_management.sh` làm **single source of truth** cho phân quyền team.
- Tránh hardcode credentials lâu dài trong file `*.properties`; nên chuyển sang env/secret manager.
- Khi tích hợp Auth0 API:
  - đồng bộ role membership (`team_de`, `team_ds`) -> Polaris principal/principal-role mappings
  - giữ logic authorization ở Polaris.

# Testing access
```bash
# Team DE
--Check catalogs exist
SHOW CATALOGS;

-- Allowed
SHOW TABLES FROM bronze_de.inventory;


-- Allowed
select * 
from silver_de.event_streaming.polaris_smoke_test;

-- Allowed 
INSERT INTO silver_de.event_streaming.polaris_smoke_test
VALUES (
  BIGINT '1004',
  'Test first name',
  'Test last name',
  'Test email',
  current_timestamp,
  current_date
);

# Team DS
--Check catalogs exist
SHOW CATALOGS;

-- Denied
SHOW TABLES FROM bronze_ds.inventory;

-- Allowed
select * 
from silver_ds.event_streaming.polaris_smoke_test;

-- Denied 
INSERT INTO silver_ds.event_streaming.polaris_smoke_test
VALUES (1004, 'Test first name', 'Test last name', 'Test email');

-- Allowed
SELECT * FROM silver_ds.event_streaming.polaris_smoke_test;

-- Denied 
INSERT INTO silver_ds.event_streaming.polaris_smoke_test
VALUES (1004, 'Test first name', 'Test last name', 'Test email');

-- Denied 
SELECT * FROM bronze_ds.inventory.customers;

```