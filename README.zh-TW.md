[English](README.md) | 繁體中文

# ntu-tentacle

一個為 Nautrouds 生態系設計的輕量 relay，橋接 Unix Domain Socket（UDS）與 TCP（可選 TLS）target，讓後端服務（包括 gRPC）可以透過 UDS 曝露，並提供 connection pooling、健康探測、hot reload 與 metrics 上報。

## Features

- **UDS ↔ TCP/TLS relay** — 透明的 byte-level 轉發（`copy_bidirectional`），不會解析 HTTP/2 或 gRPC frame，因此任何跑在連線上的協定都會原封不動地被轉發。
- **Connection pooling** — 每個 target 都有獨立的 semaphore-based pool 限制同時連線數，且 pool 滿時不會卡住新的 accept。
- **健康探測** — 每 2 秒探測一次 target 的 TCP 是否存活；只有在 target 可連線時才會啟動對應的 UDS listener，離線時自動收掉。
- **TLS client 支援** — 支援自訂 CA、mutual TLS（client cert/key），ALPN 固定為 `h2`/`http1.1`，確保會依賴 ALPN 判斷協定的上游能正確協商出 gRPC（HTTP/2）。
- **Hot reload** — 送出 `SIGHUP` 會重新讀取設定並套用新的 target 清單，不會中斷現有連線。
- **Graceful shutdown** — `SIGINT`/`SIGTERM` 會先等待 in-flight 連線結束（drain）才讓行程退出。
- **內建 metrics** — 連線數、成功/失敗次數、傳輸位元組數、延遲直方圖會被編碼為 protobuf，並定期透過 UDS 推送出去。

## Architecture

啟動時，`ntu-tentacle` 會讀取設定並為每個 target 解析對應資訊（包含 TLS 憑證）；針對每個 target 都會啟動一個 relay，先探測該 target 的 TCP 是否存活，再綁定對應的 UDS——當有 client 連進來時，relay 會撥號到該 TCP（或升級為 TLS）的 target，並雙向轉發資料，同時也會定期把 metrics 透過 UDS 推送出去。送出 `SIGHUP` 可以在不中斷現有連線的情況下套用新的設定。

## Installation

### Prerequisites

- Rust 1.85+（edition 2024）
- `protoc`（Protocol Buffers compiler，建置 `tentacle-metrics` 依賴時需要）
- 僅支援 Unix

### Building from source

```bash
cargo build --release
```

### Docker

```bash
docker build -f docker/Dockerfile -t ntu-tentacle .
```

## Configuration

設定透過環境變數讀取。

| 變數 | 說明 | 預設值 |
|---|---|---|
| `NAUTROUDS_SERVICE_NAME` | 服務名稱，用於建立 socket 目錄 | **必填** |
| `NAUTROUDS_TARGET_ADDR` | 逗號分隔的 target TCP 位址清單（例如 `localhost:8080`） | **必填**，除非有提供 targets 檔 |
| `NAUTROUDS_TARGETS_FILE` | YAML targets 檔路徑（見下方說明） | 無 |
| `NAUTROUDS_SERVICES_DIR` | 建立服務 socket 目錄的根目錄 | `/var/run/nautrouds/services` |
| `NAUTROUDS_MAX_CONNS` | 每個 target 的最大同時連線數 | `1024` |
| `NAUTROUDS_METRICS_INTERVAL_SECS` | 兩次 metrics 推送之間的間隔秒數 | `15` |

### Targets YAML 檔（選填，用於 per-target TLS 設定）

設定 targets 檔會**完全取代**由環境變數解析出的 target 清單。內容是一個 YAML mapping，key 為 target 位址，value 為選填的 TLS 設定；`cert` 與 `key` 必須同時設定或同時省略，若只設定其中一個，該 target 會被跳過並記錄 warning，不會導致啟動失敗。

```yaml
localhost:8080: {}

api.internal:9090:
  ca: /etc/ntu-tentacle/certs/ca.pem

secure-backend:9443:
  ca: /etc/ntu-tentacle/certs/ca.pem
  cert: /etc/ntu-tentacle/certs/client.pem
  key: /etc/ntu-tentacle/certs/client.key
```

## Running

```bash
export NAUTROUDS_SERVICE_NAME=myapp
export NAUTROUDS_TARGET_ADDR=localhost:8080
./target/release/ntu-tentacle
```
