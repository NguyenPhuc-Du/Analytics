# Analytics — Olist E-commerce

Repo **độc lập** cho giai đoạn Analytics (Big Data). Clone folder này là đủ: gold đã nằm trong `data/gold/`, không cần `DataSet` hay repo Processing.

Nhóm Analytics đọc tầng gold, trả lời 6 nhóm câu hỏi nghiệp vụ trên cụm Spark (1 master + 2 worker), ghi bảng Parquet và báo cáo có đối chiếu số liệu.

Không làm sạch CSV, không join lại payment/review, không chạy lại job gold.

## Cấu trúc

```
Analytics/
├── docker-compose.yml      # cụm Spark 1 master + 2 worker
├── Dockerfile              # apache/spark:3.5.3 + numpy (PySpark ML)
├── jobs/
│   ├── 01_analytics.py     # spark-submit
│   ├── common.py
│   ├── contract.py         # đường dẫn + số handover 13.494.400,74
│   └── sql/                # 14 câu SQL (Q1–Q6)
├── data/
│   ├── gold/               # INPUT — 7 bảng Parquet
│   ├── HANDOVER.md         # Processing → Analytics
│   ├── ANALYTICS_HANDOVER.md
│   └── analytics/          # OUTPUT — job ghi ra
├── reports/                # log + bảng xem trước (lần chạy 2026-10-29)
├── docs/                   # diễn giải
└── evidence/               # ảnh Spark UI
```

## Chạy

Cần Docker Desktop (engine đang chạy). Trong thư mục gốc repo:

```powershell
docker compose up -d --build
# http://localhost:8080 → 2 worker, 4 core

docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/jobs/01_analytics.py
```

Git Bash: thêm `MSYS_NO_PATHCONV=1` trước `docker compose exec` nếu Windows biến `/opt/...` thành đường dẫn ổ đĩa.

Nếu PowerShell báo không nhận `docker`:

```powershell
$env:Path += ";$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin"
```

Trong lúc job chạy: http://localhost:4040. Tắt cụm: `docker compose down`.

Đối chiếu bắt buộc: `SUM(revenue) = 13.494.400,74` BRL (handover mục 7).

## 6 chủ đề

| # | Chủ đề | Bảng output |
|---|---|---|
| 1 | Tăng trưởng & vận hành | `kpi_monthly` |
| 2 | Danh mục & mùa vụ | `category_pareto`, `category_seasonality` |
| 3 | Địa lý & logistics | `state_logistics`, `insight_distance` |
| 4 | RFM & giữ chân | `rfm_scores`, `repeat_*`, `customer_segments` |
| 5 | Seller | `seller_pareto`, `seller_watchlist` |
| 6 | Trễ / review / thanh toán | `insight_late_vs_review`, `delay_vs_review`, `payment_mix` |

Quy tắc: `is_valid_sale`, đếm `customer_unique_id`, chuỗi thời gian 2017-01→2018-08, review trên `fact_orders`, lọc `dq_*` khi phân tích giao hàng.

## Đọc kết quả

- Số liệu lần chạy: `reports/analytics_report.md`
- Diễn giải: `docs/03-cau-hoi-va-kpi.md`, `docs/04-phan-cum-rfm.md`
- Presentation: `data/ANALYTICS_HANDOVER.md` + Parquet trong `data/analytics/`
