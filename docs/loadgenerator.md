# LoadGenerator — bật / tắt / verify

Tài liệu vận hành cho **loadgenerator** trên cluster `kind-ci-local` (ad-hoc, không
nằm trong GitOps).

## LoadGenerator là gì?

LoadGenerator **không phải** một loại traffic. Nó là **công cụ sinh traffic**
(Python/Locust, `src/loadgenerator`) mô phỏng người dùng thật:

- Tự phát các chuỗi thao tác: browse sản phẩm → thêm vào giỏ → checkout.
- Traffic tạo ra đi tới service nội bộ `frontend:80` (ClusterIP) — **không** dùng
  `frontend-external` (LoadBalancer/MetalLB).
- Vì mô phỏng checkout, nó kích hoạt luồng: `checkoutservice.PlaceOrder` →
  `publishOrderEvent` → Kafka topic `orders`.

Luồng này giúp kiểm tra end-to-end data platform (order event → Kafka) mà không
cần người dùng thật.

## Khi nào dùng

- Verify luồng Kafka `orders` trên cluster local (smoke test ad-hoc).
- Load test / xem hệ thống chịu traffic như thế nào.

Lưu ý: đây là deploy **ad-hoc** — ArgoCD app `staging` / `loadgenerator` không
quản lý resource này, nên nó không bị tự sửa/xóa. Khi nhìn trong ArgoCD UI có
thể thấy nó như "unknown resource" trong namespace.

## Bật (enable)

```bash
NS=onlineboutique-staging

# 1. Apply manifest (Deployment + ServiceAccount) vào đúng namespace
kubectl apply -n $NS -f kubernetes-manifests/loadgenerator.yaml

# 2. Gắn image local đã có sẵn trong kind node
#    (manifest dùng tên trần "loadgenerator", phải thay bằng image thật)
kubectl -n $NS set image deploy/loadgenerator main=docker.io/library/loadgenerator:c0f58f19

# 3. Chặn kubelet đi pull — dùng ảnh local đã load vào node
kubectl -n $NS patch deploy/loadgenerator -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"main","imagePullPolicy":"IfNotPresent"}]}}}}'

# 4. Đợi pod Running
#    (init container frontend-check ping frontend:80; nếu không tới được sẽ CrashLoop)
kubectl -n $NS rollout status deploy/loadgenerator
```

## Bắt buộc khi muốn verify luồng Kafka

Topic `orders` **không tự tạo** — `checkoutservice` dùng `segmentio/kafka-go`
writer mặc định `AllowAutoTopicCreation=false` (`src/checkoutservice/main.go`),
nên nếu topic chưa tồn tại sẽ thấy lỗi:

```
Failed to publish order event: [3] Unknown Topic Or Partition
```

Tạo topic thủ công một lần:

```bash
kubectl -n $NS exec deploy/kafka -- /usr/bin/kafka-topics \
  --bootstrap-server localhost:9092 --create \
  --topic orders --partitions 1 --replication-factor 1 --if-not-exists
```

## Tắt

Tạm dừng (giữ nguyên config, có thể bật lại):

```bash
kubectl -n onlineboutique-staging scale deploy/loadgenerator --replicas=0
# bật lại:
kubectl -n onlineboutique-staging scale deploy/loadgenerator --replicas=1
```

Gỡ hẳn:

```bash
kubectl -n onlineboutique-staging delete deploy/loadgenerator
kubectl -n onlineboutique-staging delete sa loadgenerator
```

## Verify

```bash
# 1. LoadGenerator tạo traffic? (Aggregated: # reqs / # fails)
kubectl -n onlineboutique-staging logs -l app=loadgenerator --tail=20

# 2. Topic "orders" tồn tại?
kubectl -n onlineboutique-staging exec deploy/kafka -- \
  /usr/bin/kafka-topics --bootstrap-server localhost:9092 --list | grep orders

# 3. Checkoutservice đã publish event thành công?
kubectl -n onlineboutique-staging logs -l app=checkoutservice --tail=100 | grep -i "successfully published"

# 4. (tùy chọn) Consume 1 message để kiểm tra schema
kubectl -n onlineboutique-staging exec deploy/kafka -- \
  /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders --max-messages 1 --timeout-ms 30000
```

## Mẹo nhỏ

- Trong **zsh**, `#` ở cuối dòng lệnh bị coi là tham số (không phải comment) nếu
  chưa bật `setopt interactive_comments`. Khi copy lệnh có comment kèm `# ...` sẽ
  báo lỗi `tail: #: No such file or directory` — bỏ phần comment hoặc dùng
  double-quote, hoặc chạy trong bash.
