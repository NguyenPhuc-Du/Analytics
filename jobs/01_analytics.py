# jobs/01_analytics.py
import io
import json
import os
from datetime import datetime

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession, functions as F

from common import Timer, dir_size_mb, fmt, setup_logger, wait_for_executors, write_parquet
from contract import (
    EXPECTED_KPI_MONTHS,
    EXPECTED_REVENUE,
    EXPECTED_RFM_CUSTOMERS,
    GOLD_DIR,
    GOLD_TABLES,
    OUT_DIR,
    REPORT_DIR,
    SQL_DIR,
)

log = setup_logger("analytics", os.path.join(REPORT_DIR, "analytics_run.log"))

SQL_STEPS = [
    ("q1_kpi_monthly", "kpi_monthly", "1 tháng"),
    ("q2_category_pareto", "category_pareto", "1 danh mục"),
    ("q2_category_seasonality", "category_seasonality", "1 danh mục × 1 tháng (2017)"),
    ("q3_state_logistics", "state_logistics", "1 bang"),
    ("q3_distance", "insight_distance", "1 band km"),
    ("q4_rfm_score", "rfm_scores", "1 khách"),
    ("q4_repeat_profile", "repeat_profile", "one_shot vs repeat"),
    ("q4_repeat_categories", "repeat_categories", "1 danh mục của khách lặp"),
    ("q4_repeat_states", "repeat_states", "1 bang của khách lặp"),
    ("q5_seller_pareto", "seller_pareto", "1 seller"),
    ("q5_seller_watchlist", "seller_watchlist", "seller doanh thu cao / ops kém"),
    ("q6_late_vs_review", "insight_late_vs_review", "late vs on_time"),
    ("q6_delay_vs_review", "delay_vs_review", "1 band ngày trễ"),
    ("q6_payment", "payment_mix", "PTTT × kỳ trả góp"),
]


def read_sql(name):
    path = os.path.join(SQL_DIR, name + ".sql")
    with io.open(path, encoding="utf-8") as f:
        return f.read().strip().rstrip(";")


def scalar(spark, sql):
    return spark.sql(sql).collect()[0][0]


def load_gold(spark):
    for name in GOLD_TABLES:
        path = os.path.join(GOLD_DIR, name)
        if not os.path.isdir(path):
            raise SystemExit("Không thấy {}.".format(path))
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(name)
        log.info("Đọc %-24s %s dòng", name, fmt(df.count()))


def check_handover(spark):
    rev = float(scalar(spark, "SELECT SUM(revenue) FROM agg_sales_monthly"))
    if abs(rev - EXPECTED_REVENUE) >= 0.01:
        raise SystemExit("SUM(revenue)={} khác handover {}".format(rev, EXPECTED_REVENUE))
    log.info("Handover OK: SUM(revenue)=%s", rev)
    return rev


def run_sql_steps(spark):
    tables = {}
    for sql_name, out_name, grain in SQL_STEPS:
        with Timer() as t:
            df = spark.sql(read_sql(sql_name))
            path = os.path.join(OUT_DIR, out_name)
            write_parquet(df, path, single_file=True)
            written = spark.read.parquet(path)
            written.createOrReplaceTempView(out_name)
            rows = written.count()
        tables[out_name] = {
            "sql": sql_name,
            "grain": grain,
            "rows": rows,
            "seconds": t.seconds,
            "size_mb": dir_size_mb(path),
        }
        log.info("%-24s %s dòng | %.2fs", out_name, fmt(rows), t.seconds)
    return tables


def distance_corr(spark):
    d = (
        spark.table("fact_order_items").alias("i")
        .join(spark.table("fact_orders").alias("o"), F.col("i.order_id") == F.col("o.order_id"))
        .where(
            "i.is_valid_sale AND i.distance_km IS NOT NULL "
            "AND o.order_status = 'delivered' "
            "AND NOT o.dq_carrier_before_purchase "
            "AND NOT o.dq_delivered_without_date"
        )
        .select(
            F.col("i.distance_km").alias("distance_km"),
            F.col("i.freight_value").cast("double").alias("freight_value"),
            F.col("i.delivery_days").cast("double").alias("delivery_days"),
            F.col("i.delivery_delay_days").cast("double").alias("delivery_delay_days"),
        )
    )
    out = {
        "corr_distance_freight": d.stat.corr("distance_km", "freight_value"),
        "corr_distance_delivery_days": d.stat.corr("distance_km", "delivery_days"),
        "corr_distance_delay_days": d.stat.corr("distance_km", "delivery_delay_days"),
    }
    log.info("Tương quan khoảng cách: %s", out)
    return out


def run_kmeans(spark):
    with Timer() as t:
        feat = spark.table("agg_customer_rfm").select(
            "customer_unique_id",
            F.col("recency_days").cast("double").alias("recency_days"),
            F.col("frequency").cast("double").alias("frequency"),
            F.log1p(F.col("monetary").cast("double")).alias("log_monetary"),
            "monetary",
        )
        assembled = VectorAssembler(
            inputCols=["recency_days", "frequency", "log_monetary"],
            outputCol="raw_features",
        ).transform(feat)
        scaled = (
            StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)
            .fit(assembled)
            .transform(assembled)
        )
        model = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster").fit(scaled)
        for i, c in enumerate(model.clusterCenters()):
            log.info("Centroid %s = %s", i, [round(float(x), 4) for x in c])

        seg = model.transform(scaled).select("customer_unique_id", "cluster")
        customers = spark.table("rfm_scores").join(seg, "customer_unique_id")
        write_parquet(customers, os.path.join(OUT_DIR, "customer_segments"))

        profile = (
            customers.groupBy("cluster")
            .agg(
                F.count("*").alias("customers"),
                F.round(F.avg("recency_days"), 1).alias("avg_recency"),
                F.round(F.avg("frequency"), 3).alias("avg_frequency"),
                F.round(F.avg("monetary"), 2).alias("avg_monetary"),
                F.sum(F.when(F.col("rfm_segment") == "Champions", 1).otherwise(0)).alias("champions"),
                F.sum(F.when(F.col("rfm_segment") == "Loyal", 1).otherwise(0)).alias("loyal"),
                F.sum(F.when(F.col("rfm_segment") == "At Risk", 1).otherwise(0)).alias("at_risk"),
            )
            .orderBy("cluster")
        )
        write_parquet(profile, os.path.join(OUT_DIR, "cluster_profile"))
        rows = [r.asDict() for r in profile.collect()]
    log.info("K-Means k=4 xong sau %.2fs", t.seconds)
    return t.seconds, rows


def preview(spark, sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


PREVIEW_NOTES = {
    "KPI tháng (đỉnh + Black Friday)": (
        "**Bảng `kpi_monthly`:** 1 dòng = 1 tháng trong 2017-01→2018-08. "
        "`revenue` = giá hàng (chưa ship); `revenue_mom_pct` = % so với tháng trước; "
        "`late_delivery_pct` = % đơn đã giao bị trễ; `avg_review_score` = sao cấp đơn.\n\n"
        "**Đọc số:** 11/2017 đỉnh doanh thu (rank 1), MoM +52%, trễ 14,3%, review 3,95. "
        "03/2018 trễ 21,4% / review 3,76 — tệ nhất trong các tháng doanh thu lớn.\n\n"
        "**Ý nghĩa:** Sàn **bán được nhiều hơn không đồng nghĩa vận hành ổn**. Black Friday và 02–03/2018 cho thấy "
        "tăng đơn kéo quá tải giao hàng rồi kéo sao xuống. Nếu chỉ tối ưu ads tháng 11 mà không tăng năng lực "
        "seller/kho/đơn vị vận chuyển, doanh thu đỉnh sẽ đổi bằng uy tín. 04–05/2018 gần bằng doanh thu T11 "
        "nhưng trễ thấp hơn và review ~4,2: đó mới là tháng 'khỏe' để nhân rộng, không phải T11."
    ),
    "Top 10 danh mục": (
        "**Bảng `category_pareto`:** 1 dòng = 1 ngành. `freight_to_revenue_pct` = ship/doanh thu; "
        "`cumulative_share_pct` = cộng dồn Pareto.\n\n"
        "**Đọc số:** `health_beauty` 9,3% doanh thu. Đồng hồ giá TB 200 vs chăn ga 93. "
        "Nội thất/gia dụng ship ~23%. Top 10 mới ~62% — chưa tới 80%.\n\n"
        "**Ý nghĩa:** Sàn **không phụ thuộc một ngành** (không ai >10%), nên campaign nên theo cụm "
        "(làm đẹp + đồng hồ + nhà cửa) chứ không 'all in' một SKU. "
        "`watches_gifts` là hàng giá trị: ship rẻ tương đối (8%) — đáng đẩy margin. "
        "`bed_bath_table` là hàng volume: đừng so AOV với đồng hồ. "
        "Nội thất/gia dụng: mỗi 100 BRL hàng mất ~23 BRL ship — niêm yết giá 'free ship' sẽ lỗ; "
        "nên hiện ship sớm hoặc gom đơn. Top 10 chưa 80% nghĩa là đuôi 64 ngành vẫn góp ~38%: "
        "cắt ngành nhỏ ồ ạt có thể mất doanh thu, hãy cắt theo review/trễ chứ không chỉ theo rank."
    ),
    "Bang (top 8)": (
        "**Bảng `state_logistics`:** 1 dòng = 1 bang khách. `gmv` = hàng + ship.\n\n"
        "**Đọc số:** SP ~38% doanh thu, 8,7 ngày, ship 13,8%, review 4,21. "
        "BA 19,3 ngày / trễ 14% / review 3,88; RJ trễ 13,5% / review 3,91.\n\n"
        "**Ý nghĩa:** SP vừa là thị trường lớn vừa là thị trường **dễ làm hài lòng** vì gần cụm seller. "
        "Mở rộng BA/RJ/ES không chỉ là 'thêm khách' — khách trả AOV cao hơn (BA 181 vs SP 143) "
        "nhưng trải nghiệm tệ hơn, nên review thấp sẽ đè tăng trưởng dài hạn. "
        "Hành động: SLA và seller local cho Đông Bắc/RJ, hoặc báo ngày giao thật trước khi khách checkout "
        "để giảm sốc, chứ không copy playbook SP sang toàn quốc."
    ),
    "Khoảng cách": (
        "**Bảng `insight_distance`:** band km seller→khách. `avg_delay_days` âm = giao sớm hơn hẹn.\n\n"
        "**Đọc số:** 0–200 km: ship 12,7 / 7 ngày / trễ 6%. 1000km+: 31,7 / 19 ngày / trễ 12%. "
        "Pearson km–ship và km–ngày giao ~0,39; km–số ngày trễ so với hẹn ~0.\n\n"
        "**Ý nghĩa:** Logistics Olist **có cấu trúc địa lý**, không phải nhiễu. "
        "Khoảng cách tăng gấp ~5 thì ship gần gấp 2,5 và ngày giao gần gấp 3 — "
        "định giá ship theo vùng là hợp lý, không phải 'bắt khách xa'. "
        "Trung bình vẫn giao **trước hạn** (delay âm) nên deadline trên giấy không phải gốc rễ; "
        "gốc rễ là **ngày chờ thực tế 19 ngày + tỷ lệ trễ 12%** ở band xa, đủ để khách 1 sao "
        "(xem mục giao trễ). Ưu tiên: gần hóa hàng (seller/kho vùng) hơn là siết hạn hẹn trên hệ thống."
    ),
    "Repeat vs one-shot": (
        "**Bảng `repeat_profile`:** one_shot vs `frequency>=2`.\n\n"
        "**Đọc số:** 96,96% mua đúng 1 lần. 3,04% lặp chi lifetime 308 vs 161, nhưng AOV 146 vs 161.\n\n"
        "**Ý nghĩa:** Olist là **marketplace one-shot**, không phải mô hình subscription. "
        "KPI 'tỷ lệ khách quay lại' nếu tính trên 95k người sẽ gần 0 và vô dụng — "
        "phải tách 2.887 người đã lặp. Họ không mua đơn đắt hơn; họ **mua thêm lần**. "
        "Giữ chân nhóm này (voucher lần 2, ngành nhà cửa) mang lifetime gấp đôi. "
        "Đốt budget win-back 92k one-shot sẽ đắt và loãng. "
        "AOV one-shot cao hơn một chút có thể vì đơn 'thử' đắt / quà, không phải họ trung thành hơn."
    ),
    "RFM segment": (
        "**Bảng `rfm_scores`:** Champion/Loyal/At Risk chỉ khi đã mua ≥2 lần.\n\n"
        "**Đọc số:** 251 Champions, 675 Loyal, 1.961 At Risk; còn lại ~91k one-shot, phần lớn Lost.\n\n"
        "**Ý nghĩa:** Sách CRM 'Champions 20%' **không áp** được: nhóm giá trị thật chỉ ~3% "
        "và Champions vỏn vẹn 251 người — đủ để chăm tay, không đủ để gánh doanh thu sàn. "
        "At Risk (1.961) là hàng đợi gọi lại có xác suất cao hơn One-shot Lost (65k). "
        "One-shot Recent (9k) mới mua: nurture lần 2 trong 90 ngày (đúng ngành `bed_bath_table`) "
        "còn cửa; One-shot Lost gần như chi phí chìm. "
        "Không lấy NTILE-F làm persona vì 97% hòa frequency=1."
    ),
    "Giao trễ vs review": (
        "**Bảng `insight_late_vs_review`:** late vs on_time, đơn delivered có review.\n\n"
        "**Đọc số:** Trễ 2,56 sao vs đúng hạn 4,29 (kém 1,73). Sao 1–2: 54% vs 9%. Có comment: 56% vs 39%.\n\n"
        "**Ý nghĩa:** Giao trễ **không phải chi tiết vận hành nhỏ** — nó đổi 4,3 sao (rất tốt) thành 2,6 "
        "(gần thất bại trên storefront). Hơn nửa đơn trễ bị 1–2 sao và hay viết lời: "
        "đây là review **công khai kéo listing xuống**, không chỉ 'khách hơi bực'. "
        "Cải thiện on-time 1 điểm % trên ~7.600 đơn trễ có đòn bẩy sao rõ hơn tăng ads. "
        "Đây là tương quan mạnh có điều kiện lọc dq_*; không chứng minh mọi sao thấp đều do trễ, "
        "nhưng đủ để ưu tiên SLA seller trong watchlist."
    ),
    "Ngưỡng ngày trễ": (
        "**Bảng `delay_vs_review`:** band ngày trễ so với hạn.\n\n"
        "**Đọc số:** Đúng hạn 6,6% một sao. Trễ 1–3 ngày: 25%. 4–7 ngày: 59% / review 2,1. "
        "8–14 ngày: ~71% / 1,67 sao. ≥15 ngày không tệ hơn đáng kể.\n\n"
        "**Ý nghĩa:** Khách **chưa 'chắc chắn 100% 1 sao'** nhưng sau **4 ngày trễ** đa số đã phạt nặng; "
        "sau **1 tuần** điểm kịch sàn — thêm ngày nữa gần như không đổi. "
        "Ops nên can thiệp (báo khách, voucher, đổi carrier) ở mốc 1–3 ngày, "
        "khi review còn 3,3 chứ đừng đợi 15 ngày. "
        "Cam kết giao 'trong 2 tuần' rồi trễ 4 ngày vẫn chết sao: hãy hứa sát thực tế hơn "
        "(band xa 19 ngày) để vào nhóm on_time_or_early."
    ),
}

GLOSSARY = """
## 7. Các bảng không có trong mục xem trước — ý nghĩa kết quả

| Bảng | Kết quả | Ý nghĩa với sàn |
|---|---|---|
| `category_seasonality` | Mọi ngành lớn **đỉnh tháng 11** (~15–20% doanh thu năm). | Nhập hàng / ads / CSKH dồn trước Black Friday; rải đều 12 tháng sẽ thiếu hàng đúng lúc cầu cao và thừa lúc cầu thấp. |
| `repeat_categories` | Khách lặp mua `bed_bath_table`, nội thất, thể thao — không phải đồng hồ. | Lần 2 là **bổ sung nhà cửa**, không phải upsell đồng hồ. Email/voucher lần 2 nên theo ngành này. |
| `repeat_states` | Lặp nhiều ở SP rồi RJ, MG — giống bản đồ doanh thu. | Không phải 'chỉ dân SP mới trung thành'. Repeat đi theo mật độ đơn, không phải văn hóa vùng. |
| `seller_pareto` | ~18% seller nắm ~80% doanh thu. | Tập trung quản trị chất lượng vào vài trăm shop lớn; rải QC 3.000 shop sẽ loãng. |
| `seller_watchlist` | 153 shop vừa to vừa (sao thấp / giao chậm / trễ nhiều). | Đây là rủi ro **uy tín × doanh thu**: ẩn/cảnh cáo sớm hơn shop nhỏ. Doanh thu cao không được miễn SLA. |
| `payment_mix` | Thẻ ~75% đơn; trả góp ≥7 kỳ AOV ~335 vs 1 kỳ ~101. | Trả góp là **kênh đơn đắt**, không phải nút thần 'bật góp là tăng AOV'. Giữ góp cho đơn lớn; boleto (~20%) vẫn cần cho khách không thẻ. |
| `customer_segments` / `cluster_profile` | K-Means tách đúng 2.887 người lặp thành 1 cụm. | Model xác nhận frequency là trục thật. Dùng luật CRM cho Champion/At Risk; dùng cụm 0 vs 2 để tách one-shot rẻ / đắt. |

## 8. Từ điển chỉ số (dùng chung)

| Chỉ số | Nghĩa |
|---|---|
| `revenue` | Tổng **giá sản phẩm**, chưa gồm vận chuyển (BRL). |
| `freight` / `freight_value` | Phí vận chuyển. |
| `gmv` / `order_value` | `revenue` + ship = số khách trả cho hàng. |
| `avg_order_value` (AOV) | GMV trung bình mỗi đơn. |
| `orders` / `customers` / `items` | Số đơn / khách thật (`customer_unique_id`) / món. |
| `late_delivery_pct` / `late_pct` | % đơn (hoặc món) **đã giao** nhận sau ngày hẹn. |
| `avg_delivery_days` | Ngày từ mua đến khách nhận. NULL nếu chưa giao. |
| `delivery_delay_days` / `avg_delay_days` | Ngày nhận − ngày hẹn. **Âm = sớm**, dương = trễ. |
| `avg_review_score` | Sao 1–5 **cấp đơn**, không nhân theo số món. |
| `freight_to_revenue_pct` | Ship / revenue × 100. Cao = hàng cồng kềnh ăn biên. |
| `revenue_share_pct` | Tỷ trọng trên tổng đang xét. |
| `cumulative_share_pct` | Cộng dồn từ lớn đến nhỏ (đường Pareto). |
| `revenue_mom_pct` | (tháng này − tháng trước) / tháng trước × 100. |
| `revenue_yoy_pct` | So với cùng tháng năm trước (`LAG` 12). Tháng 2017 = NULL. |
| `recency_days` | Ngày từ lần mua cuối đến 2018-10-17 (neo gold). |
| `frequency` | Số đơn hợp lệ của khách. |
| `monetary` | Tổng GMV lifetime của khách. |
| `r_score` / `f_score` / `m_score` | NTILE 5. F gần như nhiễu vì 97% frequency = 1. |
| Pearson | −1…1. 0,39 = tăng cùng chiều vừa; gần 0 = gần như không tuyến tính. |

## 9. Kết luận ngắn (kết quả này nói gì về sàn)

1. **Doanh thu đỉnh ≠ sức khỏe:** Black Friday và 03/2018 bán mạnh nhưng trễ và sao rớt — phải tăng năng lực giao trước khi tăng ads.
2. **Giao trễ là đòn bẩy sao:** trễ mất ~1,7 sao; sau 4–7 ngày đa số đã 1 sao. Can thiệp sớm, hứa ngày giao sát thực tế.
3. **Địa lý quyết định trải nghiệm:** SP dễ tính vì gần seller; BA/RJ trả AOV cao hơn nhưng review thấp — mở vùng phải kèm SLA.
4. **Khách không quay lại là mặc định:** 97% one-shot. CRM chỉ đáng tiền trên ~3% đã lặp + one-shot recent; đừng làm loyalty cho cả 95k người.
5. **Seller lệch Pareto:** ~18% shop = 80% doanh thu. Watchlist 153 shop to mà ops kém là rủi ro lớn nhất phía cung.

Diễn giải dài: `docs/03-cau-hoi-va-kpi.md`.
"""


def write_report(run, tables, checks, previews, profile_rows, corr):
    lines = [
        "# Báo cáo Analytics\n",
        "> Sinh bởi `jobs/01_analytics.py`. Dưới mỗi bảng: **cột nghĩa gì** → **số nói gì** → **ý nghĩa với sàn**.\n",
        "## 1. Lần chạy\n",
        "| Thuộc tính | Giá trị |\n|---|---|",
        "| Thời điểm | {} |".format(run["started_at"]),
        "| Master | {} ({} executor, {} core) |".format(
            run["master"], run["executors"], run["default_parallelism"]
        ),
        "| Gold SUM(revenue) | {} |".format(run["gold_revenue"]),
        "| SQL | {} s |".format(run["sql_seconds"]),
        "| K-Means | {} s |".format(run["kmeans_seconds"]),
        "| Tổng | {} s |".format(run["total_seconds"]),
        "\n## 2. Tương quan khoảng cách\n",
        "| Cặp | Pearson |\n|---|---:|",
        "| distance vs freight | {} |".format(corr["corr_distance_freight"]),
        "| distance vs delivery_days | {} |".format(corr["corr_distance_delivery_days"]),
        "| distance vs delay_days | {} |".format(corr["corr_distance_delay_days"]),
        "\nPearson −1…1 đo mức tăng/giảm **cùng chiều tuyến tính**. "
        "~0,39 với freight và ngày giao: xa hơn thì cước và thời gian giao tăng vừa. "
        "Gần 0 với `delivery_delay_days`: số ngày **trễ so với hạn hẹn** gần như không tăng theo km "
        "(SLA đã chèn buffer đường dài).\n\n"
        "**Ý nghĩa:** Ship và ngày giao **có cấu trúc theo km**, nên bảng giá theo vùng là có căn cứ. "
        "Nhưng 'trễ hạn' không tăng theo km — vấn đề band xa là **chờ lâu thật** (19 ngày) "
        "và **tỷ lệ ca trễ** cao hơn, không phải deadline trên giấy quá chặt.\n",
        "\n## 3. Bảng kết quả\n",
        "| Bảng | 1 dòng = | Số dòng | Thời gian (s) |\n|---|---|---:|---:|",
    ]
    for name, t in tables.items():
        lines.append("| `{}` | {} | {} | {} |".format(name, t["grain"], fmt(t["rows"]), t["seconds"]))

    lines += ["\n## 4. Đối chiếu\n", "| Kiểm tra | Được | Kỳ vọng | Kết quả |\n|---|---:|---:|---|"]
    for c in checks:
        lines.append(
            "| {} | {} | {} | {} |".format(
                c["name"], fmt(c["got"]), fmt(c["expected"]), "ĐẠT" if c["ok"] else "**SAI**"
            )
        )

    lines.append("\n## 5. Xem trước\n")
    for title, rows in previews:
        lines.append("### {}\n".format(title))
        if not rows:
            lines.append("*(trống)*\n")
            continue
        cols = list(rows[0].keys())
        lines.append("| " + " | ".join(str(c) for c in cols) + " |")
        lines.append("|" + "---|" * len(cols))
        for r in rows:
            lines.append("| " + " | ".join("" if r[c] is None else str(r[c]) for c in cols) + " |")
        lines.append("")
        note = PREVIEW_NOTES.get(title)
        if note:
            lines.append(note)
            lines.append("")

    lines.append("## 6. K-Means profile (đặt tên cụm sau khi đọc bảng này)\n")
    if profile_rows:
        cols = list(profile_rows[0].keys())
        lines.append("| " + " | ".join(str(c) for c in cols) + " |")
        lines.append("|" + "---|" * len(cols))
        for r in profile_rows:
            lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    lines.append(
        "\n**Cách đọc:** `cluster` là nhãn K-Means (k=4, seed=42) trên "
        "(recency, frequency, log1p(monetary)) đã scale — **không** phải tên CRM. "
        "Frequency TB ≈ 1 ở cụm 0/2/3 vì ~97% khách one-shot.\n\n"
        "| Cụm | Đặt tên sau khi xem số | Vì sao |\n"
        "|---|---|---|\n"
        "| 0 | One-shot giá trị thấp | F=1, monetary ~70, recency ~196 (còn tương đối mới) |\n"
        "| 1 | Khách mua lại | F=2,11; đúng 2.887 người; chứa hết Champions/Loyal/At Risk |\n"
        "| 2 | One-shot chi cao | F=1, monetary ~320 |\n"
        "| 3 | One-shot lost | F=1, recency ~475 ngày (lâu không mua) |\n\n"
        "**Ý nghĩa:** K-Means **tự tìm ra** đúng 2.887 người mua lại (cụm 1) mà không cần gán nhãn trước — "
        "frequency là trục thật, model không hỏng. Nó **không** tách nổi Champion vs At Risk trong 3% đó "
        "(N quá nhỏ). Dùng cụm 0 (one-shot rẻ) vs 2 (one-shot chi ~320) cho campaign khác nhau: "
        "0 = đừng tốn voucher lớn; 2 = có thể mời lần 2 bằng hàng nhà cửa. "
        "Cụm 3 (lost, ~475 ngày) gần như chi phí chìm.\n"
    )
    lines.append(GLOSSARY)

    path = os.path.join(REPORT_DIR, "analytics_report.md")
    with io.open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    return path


def main():
    job = Timer().__enter__()
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(REPORT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder.appName("olist-analytics")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext
    executors = wait_for_executors(sc)
    log.info(
        "Spark %s | master=%s | executors=%d | parallelism=%d",
        spark.version, sc.master, executors, sc.defaultParallelism,
    )

    load_gold(spark)
    gold_rev = check_handover(spark)

    with Timer() as sql_timer:
        tables = run_sql_steps(spark)
    corr = distance_corr(spark)
    kmeans_seconds, profile_rows = run_kmeans(spark)

    n_months = int(scalar(spark, "SELECT COUNT(*) FROM kpi_monthly"))
    n_rfm = int(scalar(spark, "SELECT COUNT(*) FROM rfm_scores"))
    n_late = int(scalar(spark, "SELECT COUNT(*) FROM insight_late_vs_review"))
    checks = [
        {"name": "Gold SUM(revenue)", "got": gold_rev, "expected": EXPECTED_REVENUE,
         "ok": abs(gold_rev - EXPECTED_REVENUE) < 0.01},
        {"name": "kpi_monthly số tháng", "got": n_months, "expected": EXPECTED_KPI_MONTHS,
         "ok": n_months == EXPECTED_KPI_MONTHS},
        {"name": "rfm_scores số khách", "got": n_rfm, "expected": EXPECTED_RFM_CUSTOMERS,
         "ok": n_rfm == EXPECTED_RFM_CUSTOMERS},
        {"name": "late vs review có 2 nhóm", "got": n_late, "expected": 2, "ok": n_late == 2},
    ]
    for c in checks:
        log.info("Đối chiếu %-4s %s", "OK" if c["ok"] else "SAI", c["name"])

    previews = [
        ("KPI tháng (đỉnh + Black Friday)", preview(
            spark,
            "SELECT purchase_year, purchase_month, orders, revenue, revenue_mom_pct, "
            "late_delivery_pct, avg_review_score, is_black_friday_month, revenue_rank "
            "FROM kpi_monthly ORDER BY revenue_rank LIMIT 5",
        )),
        ("Top 10 danh mục", preview(spark, "SELECT * FROM category_pareto LIMIT 10")),
        ("Bang (top 8)", preview(spark, "SELECT * FROM state_logistics LIMIT 8")),
        ("Khoảng cách", preview(spark, "SELECT * FROM insight_distance ORDER BY band_order")),
        ("Repeat vs one-shot", preview(spark, "SELECT * FROM repeat_profile")),
        ("RFM segment", preview(
            spark,
            "SELECT rfm_segment, COUNT(*) AS customers FROM rfm_scores "
            "GROUP BY rfm_segment ORDER BY customers DESC",
        )),
        ("Giao trễ vs review", preview(spark, "SELECT * FROM insight_late_vs_review")),
        ("Ngưỡng ngày trễ", preview(spark, "SELECT * FROM delay_vs_review ORDER BY band_order")),
    ]

    job.__exit__()
    run = {
        "started_at": started_at,
        "master": sc.master,
        "executors": executors,
        "default_parallelism": sc.defaultParallelism,
        "gold_revenue": gold_rev,
        "sql_seconds": sql_timer.seconds,
        "kmeans_seconds": kmeans_seconds,
        "total_seconds": job.seconds,
    }
    with io.open(os.path.join(REPORT_DIR, "analytics_summary.json"), "w", encoding="utf-8") as f:
        json.dump(
            {"run": run, "tables": tables, "checks": checks, "corr": corr, "cluster_profile": profile_rows},
            f, ensure_ascii=False, indent=2, default=str,
        )

    report = write_report(run, tables, checks, previews, profile_rows, corr)
    failed = [c["name"] for c in checks if not c["ok"]]
    log.info("Xong %.2fs. Báo cáo: %s", job.seconds, report)
    spark.stop()
    if failed:
        raise SystemExit("Đối chiếu SAI: " + "; ".join(failed))


if __name__ == "__main__":
    main()