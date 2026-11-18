# Kết quả

> Sinh tự động bằng `python tools/make_results_md.py` từ `results/*.json` (lần chạy Kaggle thật).
> Không sửa tay. Cột *Nguồn* là khóa JSON để trích dẫn trong báo cáo. `FRACTION` = tỷ lệ người dùng.

## Dữ liệu (REES46) - `results/01_ingest_eda.json`

FRACTION = **1**, chạy lúc `2026-11-17T22:04:23.868790+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Sự kiện | 109,950,743 | `n_events` |
| Người dùng (chính xác) | 5,316,649 | `overview.n_users` |
| Sản phẩm | 206,876 | `overview.n_products` |
| Phiên | 23,016,650 | `overview.n_sessions` |
| view / cart / purchase | 104,335,509 / 3,955,446 / 1,659,788 | `overview.event_types` |
| Thiếu category_code | 0.3221 | `overview.null_category_code` |
| Thiếu brand | 0.1394 | `overview.null_brand` |
| CSV -> Parquet (s) | 454.5 | `timings_s.csv_to_parquet` |
| Tỷ lệ phiên có mua | 0.06095 | `session_stats.session_conversion` |

## RQ1 - Sketch luồng - `results/02_streaming_sketches.json`

FRACTION = **1**, chạy lúc `2026-11-17T22:53:07.547005+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| CMS top-k overlap | 1 | `cms.top_k_overlap` |
| CMS trong biên eps*N | 1 | `cms.within_bound_fraction` |
| CMS bộ nhớ (MB) | 5.437 | `cms.memory_mb` |
| CMS phân tán = tuần tự | có | `cms.distributed_equals_sequential` |
| Bloom FPR thực nghiệm (mục tiêu 0.01) | 0.0007587 | `bloom.empirical_fpr` |
| Bloom bộ nhớ (MB) | 7.101 | `bloom.memory_mb` |
| DGIM sai số tối đa (lý thuyết <= 0.5) | 0.2924 | `dgim.max_rel_err` |
| DGIM sai số TB | 0.116 | `dgim.mean_rel_err` |
| HLL rsd=0.01: sai số | 0.01479 | `hll.2.rel_err` |
| Distinct users chính xác | 5,316,649 | `exact_distinct_users` |

## RQ2 - Tập phổ biến (Instacart) - `results/03_frequent_itemsets.json`

FRACTION = **1**, chạy lúc `2026-11-18T01:24:09.239914+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Số giỏ | 3,055,045 | `instacart.n_baskets` |
| Support 0.001: cặp phổ biến | 2,025 | `instacart.benchmark.-1.frequent_pairs` |
| Support 0.001: ứng viên A-Priori | 1,517,074 | `instacart.benchmark.-1.apriori_candidates` |
| Support 0.001: ứng viên PCY | 6,161 | `instacart.benchmark.-1.pcy_candidates` |
| Support 0.001: PCY giảm ứng viên | 0.9959 | `instacart.benchmark.-1.pcy_candidate_reduction` |
| A-Priori = PCY | có | `instacart.benchmark.-1.same_result` |
| Thời gian A-Priori / PCY / FP-Growth (s) | 266.3 / 379.1 / 1,534 | `instacart.benchmark[-1].t_*` |
| FP-Growth: số itemset | 4,118 | `instacart.n_itemsets` |
| Luật lift > 2 | 524 | `instacart.n_rules_lift_gt_2` |
| REES46 giỏ phiên / luật | 325,169 / 128 | `rees46_sessions` |

## RQ3 - LSH - `results/04_lsh_similarity.json`

FRACTION = **1**, chạy lúc `2026-11-18T02:57:07.107317+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Số tập sản phẩm | 168,626 | `n_product_sets` |
| Tập đánh giá | 10,000 | `eval_subset` |
| Cặp đúng (Jaccard >= ngưỡng) | 3 | `n_true_pairs_eval` |
| Ngưỡng | 0.3 | `threshold` |
| LSH 128 hash: recall / precision | 1 / 1 | `lsh_vs_naive[1]` |
| Phân tầng theo danh mục: số tập / cặp đúng | 10,410 / 35 | `eval_category_stratified` |
| Phân tầng: LSH 128 hash recall [CI95] | 1 [0.9011, 1] | `eval_category_stratified.lsh_vs_naive[1]` |
| Phân tầng: MLlib recall [CI95] | 0.9143 [0.7762, 0.9704] | `eval_category_stratified.lsh_vs_naive[-1]` |
| MLlib MinHashLSH: recall | 0.6667 | `lsh_vs_naive.-1.recall` |
| Mũ thực nghiệm naive | 2.114 | `naive_exponent` |
| LSH toàn bộ: số cặp | 1,578 | `lsh_all.pairs` |
| LSH toàn bộ: thời gian (s) | 244 | `lsh_all.seconds` |
| Naive ngoại suy (giờ) | 89.93 | `lsh_all.naive_extrapolated_hours` |

## RQ4 - PageRank / HITS - `results/05_link_analysis.json`

FRACTION = **1**, chạy lúc `2026-11-17T23:20:17.965464+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Nút / cạnh | 152,189 / 4,445,337 | `graph` |
| Spearman(PR, views) | 0.7948 | `pagerank.spearman.rank.views` |
| Spearman(PR, purchases) | 0.5957 | `pagerank.spearman.rank.purchases` |
| Top-1000 trùng views | 0.569 | `pagerank.top1000_overlap_with_views` |
| Topic mass: toàn cục -> cá nhân hóa | 0.08299 -> 0.631 (apparel) | `topic_sensitive` |
| PageRank thời gian (s) | 271.9 | `pagerank.seconds` |
| HITS thời gian (s) | 1,243 | `hits_seconds` |

## RQ5 - Hệ gợi ý - `results/06_recommender.json`

FRACTION = **0.05**, chạy lúc `2026-11-17T21:27:53.201417+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Cutoff / validation cutoff | 2019-11-15 / 2019-11-08 | `cutoff, val_cutoff` |
| User đánh giá / user chung | 10,298 / 4,713 | `n_eval_users, n_common_users` |
| NDCG@10 popularity | 0.06521 | `models.popularity.ndcg@10` |
| NDCG@10 item-kNN | 0.05788 | `models.item_knn.ndcg@10` |
| NDCG@10 ALS | 0.09218 | `models.als_best.ndcg@10` |
| Recall@10 ALS | 0.1405 | `models.als_best.recall@10` |
| ALS chọn (rank, alpha) | (64, 10) | `models.als_best.rank/alpha` |
| ALS - popularity: diff [CI95] | +0.0270 [+0.0205, +0.0334], p=0 | `significance_ndcg@10_paired_bootstrap` |
| ALS - item-kNN: diff [CI95] | +0.0343 [+0.0284, +0.0405], p=0 | `significance_ndcg@10_paired_bootstrap` |
| item-kNN - popularity: diff [CI95] | -0.0073 [-0.0131, -0.0015], p=0.997 | `significance_ndcg@10_paired_bootstrap` |

## Phụ - Phân khúc và phễu - `results/07_segmentation_funnel.json`

FRACTION = **1**, chạy lúc `2026-11-18T00:45:56.281740+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Người mua | 697,470 | `rfm.n_buyers` |
| k tốt nhất | 4 | `rfm.best_k` |
| Tỷ lệ mua lại Instacart | 0.5897 | `instacart.reorder_rate` |

## RQ6 - Khả năng mở rộng - `results/08_scalability_benchmark.json`

> **Cảnh báo:** file này từ lần chạy trước bản sửa PR #48: phần theo kích thước dữ liệu (mũ) **không hợp lệ**; chỉ phần speed-up theo lõi dùng được.

FRACTION = **chưa chạy**, chạy lúc `2026-11-17T21:24:09.604523+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Base fraction | chưa chạy | `base_fraction` |
| Mũ: aggregate | -0.2738 | `exponents.t_aggregate` |
| Mũ: FP-Growth | -0.2499 | `exponents.t_fpgrowth` |
| Mũ: PageRank 5 vòng | -0.1043 | `exponents.t_pagerank5` |
| Mũ: ALS 3 vòng | 0.3322 | `exponents.t_als3` |
| Speed-up 4 lõi: PageRank | 2.19 | `speedup.-1.speedup_pagerank5` |
| Speed-up 4 lõi: ALS | 1.76 | `speedup.-1.speedup_als3` |
| Speed-up 4 lõi: aggregate | 1.31 | `speedup.-1.speedup_aggregate` |

## Hadoop 3.3.6 thật - `results/hadoop/hadoop_vs_pipe.json`

Dữ liệu: **synthetic** (3000 user) - kiểm chứng cách chạy, không phải số liệu dữ liệu thật.

| Job | Khóa Hadoop / pipe | Tổng Hadoop / pipe | Trùng khớp |
|---|---|---|---|
| event_counts | 2,381 / 2,381 | 82,744 / 82,744 | có |
| pair_counts | 8,548 / 8,548 | 16,399 / 16,399 | có |
