# Chiến lược CI cho toàn bộ dự án

Tài liệu mô tả cách tổ chức Continuous Integration (CI) cho toàn dự án
**microservices-demo-complete-data-platform** (12 microservices + data platform),
được chia rõ làm 2 giai đoạn: **Trước PR** và **Sau PR**.

---

## 1. Nguyên tắc chung

| Giai đoạn | Sự kiện | Mục tiêu | Cần secret | Deploy |
|---|---|---|---|---|
| **Trước PR (pre-merge)** | `pull_request` | Phản hồi nhanh, kiểm tra theo phạm vi thay đổi | Không | Không |
| **Sau PR (post-merge)** | `push` vào `main`/`release/*` | Kiểm tra đầy đủ, build image, deploy & E2E | Có (chỉ nếu đẩy registry) | **Local (kind/minikube)** |

- **Trước PR**: chỉ lint + unit test + validate manifest, chạy song song theo
  ngôn ngữ, dùng path-filter để chỉ chạy phần bị thay đổi. Không tiếp cận cluster
  thật, không cần credential → **an toàn và nhanh** (< 5–10 phút).
- **Sau PR**: chạy lại toàn bộ test, build + push image, rồi **deploy cục bộ
  (local cluster)** bằng kind/minikube kèm smoke test và E2E cho luồng Kafka.
- **Legacy deploy GKE** (các job `deployment-tests` trong `ci-pr.yaml`,
  `ci-main.yaml`, `cleanup.yaml`, `cloudbuild.yaml`) được **giữ nguyên mã** nhưng
  **không sử dụng** cho luồng CI chính. Lý do: không phụ thuộc GCP/GKE, chi phí thấp,
  lặp lại được trên bất kỳ runner nào.

---

## 2. Trước PR (Pre-merge)

Sự kiện: `pull_request` → branch `main`.

Các job chạy song song, mỗi job có `paths` riêng để chỉ kích hoạt khi có thay đổi
trong phạm vi tương ứng.

| Job | Phạm vi | Nội dung kiểm tra |
|---|---|---|
| `go-services` | `src/shippingservice`, `src/productcatalogservice`, `src/frontend`, `src/checkoutservice` | `gofmt`, `go vet`, `golangci-lint`, `go test` |
| `dotnet-cartservice` | `src/cartservice` | `dotnet format --verify-no-changes`, `dotnet test` |
| `node-services` | `src/currencyservice`, `src/paymentservice` | `npm ci`, `node --check` (chưa có test script; bổ sung dần) |
| `python-services` | `src/emailservice`, `src/recommendationservice`, `src/loadgenerator`, `src/data_processing` | `ruff check`, `python -m py_compile` (chưa có suite pytest; bổ sung dần) |
| `java-adservice` | `src/adservice` | `./gradlew test` (dự án dùng Gradle) |
| `manifests` | `kubernetes-manifests/`, `kustomize/`, `helm-chart/` | `kubectl kustomize build`, `kubeconform -strict`, `helm lint`, `helm template` |
| `proto` (tùy chọn) | `protos/` | `buf lint` |

### Lưu ý cho `go-services`
Bổ sung `checkoutservice` vào danh sách `GO_PACKAGE` (hiện chỉ có
`shippingservice`, `productcatalogservice`, `frontend/validator`). Viết unit test
cho `publishOrderEvent` (mock `kafka.Writer`) để kiểm tra nhánh success / retry
kiệt / payload malformed.

### Lưu ý cho `manifests`
Thêm `kafka.yaml`, `minio.yaml`, `postgresql.yaml`, `spark-iceberg-config.yaml`
vào danh sách file được `kubeconform` kiểm tra (hiện chúng nằm ngoài path
`kustomize/**` chính).

---

## 3. Sau PR (Post-merge)

Sự kiện: `push` vào `main` (và `release/*`).

```
push main
   │
   ▼
[1] tests            — chạy lại toàn bộ các job pre-PR trên code đã merge
   │
   ▼
[2] build-images     — skaffold build --default-repo=<registry> --tag=$GITHUB_SHA
   │                   (cache Buildx, quét trivy nếu muốn)
   ▼
[3] deploy-local     — khởi tạo local cluster (kind) trong runner
   │                   skaffold run → kubectl wait pods → smoke test
   ▼
[4] e2e-data-platform— Xác minh luồng dữ liệu:
       • deploy kafka/minio/postgresql
       • kafka-topics --list → xác nhận topic "orders"
       • loadgenerator chạy → PlaceOrder → consume 1 message topic "orders"
         → assert JSON schema hợp lệ (order_id, event_id, items, occurred_at)
       • seed_database.py chạy → verify dữ liệu trong PostgreSQL/MinIO
```

### Deploy cục bộ (local)
- Dùng **kind** (hoặc minikube) ngay trên GitHub-hosted runner: không cần GKE,
  không cần GCP credentials, tái lập được ở mọi nơi.
- Khi đã có container image (bước 2), kéo vào kind địa phương:
  `kind create cluster` → `skaffold run --kube-context kind-kind`.
- E2E Kafka là phần **bắt buộc** của dự án data platform, không phải tùy chọn.

### Release (`push release/*` hoặc tag)
- Kế thừa `make-release.yaml`: promote image từ staging → registry production,
  regenerate `release/kubernetes-manifests.yaml`.

---

## 4. Flow tổng thể

```text
Push feature branch
   │
   ▼
----- PRE-PR (pull_request) -----
   lint + unit test + validate manifest  (theo path, song song, không secret)
   │   ◆ bắt buộc pass trước khi merge ◆
   ▼
Merge PR → main
   │
   ▼
----- POST-PR (push main) -----
   1. tests đầy đủ
   2. build + push image ($GITHUB_SHA)
   3. deploy local (kind) + smoke test
   4. E2E data platform (Kafka → ingestion → storage)
   │
   ▼
   (tag/release/*) → promote production

PR đóng → (tùy chọn) dọn dẹp local cluster
```

---

## 5. Thay đổi so với CI hiện tại

1. **Tách bạch**: phần fast-check chuyển sang sự kiện `pull_request` (chạy code
   của PR, không secret) — an toàn hơn `pull_request_target`.
2. **Bỏ deploy GKE khỏi luồng chính**: `deployment-tests` trong `ci-pr.yaml` /
   `ci-main.yaml`, `cloudbuild.yaml` **giữ nguyên file** nhưng không được dùng;
   thay thế bằng `deploy-local`.
3. **Đầy đủ ngôn ngữ**: bổ sung test cho `checkoutservice`, Node, Python,
   `data_processing`, Java vào ma trận test.
4. **Validate data platform manifests** (`kafka`, `minio`, `postgresql`,
   `spark-iceberg`).
5. **E2E bắt buộc** cho luồng `order event → topic "orders"`.

---

## 6. Checklist triển khai

- [x] Tạo workflow `pre-pr.yaml` (các job ở mục 2, path-filtered, sự kiện `pull_request`).
- [ ] Bổ sung `checkoutservice` vào Go test matrix + viết unit test `publishOrderEvent`.
- [x] Tạo workflow `post-main.yaml`: tests → build image → deploy local (kind) → smoke → E2E Kafka.
- [x] Cập nhật `manifests` job: validate `kubernetes-manifests/`, `kustomize/`, `helm-chart/`.
- [ ] Thêm job lint/test cho `src/data_processing` (ruff, pytest) — hiện mới ở mức `py_compile`.
- [x] **Không xóa** các file legacy GKE (`ci-pr.yaml`, `ci-main.yaml`,
      `cloudbuild.yaml`, `cleanup.yaml`) — job `deployment-tests` đã bị chặn `if: false`.
- [x] Thêm env `KAFKA_BROKER=kafka:9092` cho `checkoutservice`.

---

## 7. Ghi chú vận hành (rút ra khi kiểm tra thực tế)

- **GitHub chỉ đăng ký workflow file có trên default branch (`main`)** (hoặc đã
  từng chạy ít nhất 1 lần). Vì vậy một workflow mới như `post-main.yaml`:
  - `workflow_dispatch` trả `404` cho tới khi file tồn tại trên `main`.
  - Push lên nhánh `release/*` cũng **không trigger** workflow nếu file chưa
    được đăng ký (GitHub không scan workflow file ở branch không phải default).
  - Cách duy nhất để test luồng `post-main` trước khi merge vào `main` là đưa
    file lên `main` (qua PR thật) hoặc chấp nhận test sau khi merge.
- Push **tạo nhánh mới** là `CreateEvent`, không phải `push` → không trigger
  `on: push`. Cần push commit thay đổi thực lên nhánh đã tồn tại.
- Empty commit (không thay đổi file) với `paths-ignore: gitops/**` sẽ bị skip.
- `pre-pr.yaml` được đăng ký ngay vì event `pull_request` đã kích hoạt nó chạy;
  đó là lý do các job pre-pr hiển thị đúng trên PR.