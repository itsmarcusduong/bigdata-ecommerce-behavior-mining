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

FRACTION = **1**, chạy lúc `2026-11-21T08:51:51.874193+00:00`

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

**Count-Min theo seed** (`cms_seeds`)

| seed | never_under | within_bound | mean_abs_err |
|---|---|---|---|
| 42 | có | 1 | 10.6 |
| 7 | có | 1 | 8.565 |
| 2024 | có | 1 | 10.02 |

**HyperLogLog: phân bố sai số qua các salt** (`hll_salted`)

| rsd | mean_rel_err | sd_rel_err | mean_abs_rel_err | within_2rsd |
|---|---|---|---|---|
| 0.01 | -0.004797 | 0.01048 | 0.007289 | 0.9 |
| 0.05 | 0.01357 | 0.03299 | 0.02832 | 1 |
| 0.2 | 0.05792 | 0.1622 | 0.1422 | 1 |

## RQ2 - Tập phổ biến (Instacart) - `results/03_frequent_itemsets.json`

FRACTION = **1**, chạy lúc `2026-11-18T01:24:09.239914+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Số giỏ | 3,055,045 | `instacart.n_baskets` |
| Support 0.001: cặp phổ biến | 2025 | `instacart.benchmark.-1.frequent_pairs` |
| Support 0.001: ứng viên A-Priori | 1,517,074 | `instacart.benchmark.-1.apriori_candidates` |
| Support 0.001: ứng viên PCY | 6161 | `instacart.benchmark.-1.pcy_candidates` |
| Support 0.001: PCY giảm ứng viên | 0.9959 | `instacart.benchmark.-1.pcy_candidate_reduction` |
| A-Priori = PCY | có | `instacart.benchmark.-1.same_result` |
| Thời gian A-Priori / PCY / FP-Growth (s) | 266.3 / 379.1 / 1,534 | `instacart.benchmark[-1].t_*` |
| FP-Growth: số itemset | 4118 | `instacart.n_itemsets` |
| Luật lift > 2 | 524 | `instacart.n_rules_lift_gt_2` |
| REES46 giỏ phiên / luật | 325,169 / 128 | `rees46_sessions` |

## RQ3 - LSH - `results/04_lsh_similarity.json`

FRACTION = **1**, chạy lúc `2026-11-21T09:31:32.015306+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Số tập sản phẩm | 168,626 | `n_product_sets` |
| Tập đánh giá | 10,000 | `eval_subset` |
| Cặp đúng (Jaccard >= ngưỡng) | 3 | `n_true_pairs_eval` |
| Ngưỡng | 0.3 | `threshold` |
| LSH 128 hash: recall / precision | 1 / 1 | `lsh_vs_naive[1]` |
| MLlib MinHashLSH: recall | 0.6667 | `lsh_vs_naive.-1.recall` |
| Mũ thực nghiệm naive | 2.114 | `naive_exponent` |
| LSH toàn bộ: số cặp | 1578 | `lsh_all.pairs` |
| LSH toàn bộ: thời gian (s) | 202.3 | `lsh_all.seconds` |
| Naive ngoại suy (giờ) | 80.56 | `lsh_all.naive_extrapolated_hours` |

**LSH: recall theo tập đánh giá, ngưỡng, phương pháp (qua các seed)** (`lsh_eval_summary`)

| subset | threshold | method | n_true | recall_mean | recall_min | precision_min |
|---|---|---|---|---|---|---|
| multi-category | 0.2 | 128h, P>=0.9 | 386 | 0.975 | 0.9715 | 1 |
| multi-category | 0.2 | 128h, P>=0.99 | 386 | 1 | 1 | 1 |
| multi-category | 0.2 | 256h, P>=0.99 | 386 | 0.9983 | 0.9948 | 1 |
| multi-category | 0.2 | 64h, P>=0.8 | 386 | 1 | 1 | 1 |
| multi-category | 0.2 | MLlib 10 tables | 386 | 0.8696 | 0.7979 | 1 |
| multi-category | 0.3 | 128h, P>=0.9 | 86 | 1 | 1 | 1 |
| multi-category | 0.3 | 128h, P>=0.99 | 86 | 1 | 1 | 1 |
| multi-category | 0.3 | 256h, P>=0.99 | 86 | 1 | 1 | 1 |
| multi-category | 0.3 | 64h, P>=0.8 | 86 | 0.9922 | 0.9767 | 1 |
| multi-category | 0.3 | MLlib 10 tables | 86 | 0.9767 | 0.9302 | 1 |
| random | 0.2 | 128h, P>=0.9 | 64 | 0.974 | 0.9688 | 1 |
| random | 0.2 | 128h, P>=0.99 | 64 | 1 | 1 | 1 |
| random | 0.2 | 256h, P>=0.99 | 64 | 0.9948 | 0.9844 | 1 |
| random | 0.2 | 64h, P>=0.8 | 64 | 1 | 1 | 1 |
| random | 0.2 | MLlib 10 tables | 64 | 0.6198 | 0.6094 | 1 |
| random | 0.3 | 128h, P>=0.9 | 3 | 1 | 1 | 1 |
| random | 0.3 | 128h, P>=0.99 | 3 | 1 | 1 | 1 |
| random | 0.3 | 256h, P>=0.99 | 3 | 1 | 1 | 1 |
| random | 0.3 | 64h, P>=0.8 | 3 | 1 | 1 | 1 |
| random | 0.3 | MLlib 10 tables | 3 | 0.6667 | 0.6667 | 1 |

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

FRACTION = **1**, chạy lúc `2026-11-21T17:45:30.012000+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Cutoff / validation cutoff | 2019-11-15 / 2019-11-08 | `cutoff, val_cutoff` |
| User đánh giá / user chung | 206,186 / 172,676 | `n_eval_users, n_common_users` |
| Mô hình được chọn (luật đặt trước, theo validation) | history_als | `selected` |
| Seed | [42, 7, 2024] | `seeds` |

**Validation: mọi cấu hình (NDCG@10, popularity bù user thiếu)** (`validation_grid`)

| family | params | val_ndcg@10 | val_recall@10 | fit_s |
|---|---|---|---|---|
| popularity |  | 0.06933 | 0.1187 | 0 |
| recent_popularity | days=3 | 0.06915 | 0.1191 | 1.2 |
| recent_popularity | days=7 | 0.06955 | 0.1192 | 1.5 |
| recent_popularity | days=14 | 0.06955 | 0.1192 | 2 |
| history | half_life_days=1 | 0.1207 | 0.1606 | 0.5 |
| history | half_life_days=3 | 0.1227 | 0.1624 | 0.2 |
| history | half_life_days=7 | 0.1237 | 0.1639 | 0.2 |
| item_knn | history=cart+purchase | 0.07636 | 0.1274 | 1,002 |
| item_knn | history=view+cart+purchase | 0.08128 | 0.1432 | 1,020 |
| als | rank=16, alpha=10 | 0.09112 | 0.1469 | 364.6 |
| als | rank=16, alpha=40 | 0.08629 | 0.1432 | 354.2 |
| als | rank=32, alpha=10 | 0.09548 | 0.1545 | 691.6 |
| als | rank=32, alpha=40 | 0.09091 | 0.1507 | 694.6 |
| als | rank=64, alpha=10 | 0.09902 | 0.1603 | 1,564 |
| als | rank=64, alpha=40 | 0.09442 | 0.1572 | 1,574 |
| history_als | half_life_days=7, rank=64, alpha=10 | 0.1336 | 0.1891 | 0.4 |

**Test (một lần) trên user chung, theo mô hình và seed** (`test_by_model_seed`)

| model | seed | ndcg@10 | recall@10 | precision@10 | map@10 | n_users |
|---|---|---|---|---|---|---|
| popularity | nan | 0.05913 | 0.1016 | 0.01594 | 0.03958 | 50,000 |
| recent_popularity | nan | 0.05997 | 0.1042 | 0.01635 | 0.03986 | 50,000 |
| history | nan | 0.1038 | 0.1452 | 0.0203 | 0.08272 | 50,000 |
| item_knn | nan | 0.06817 | 0.1179 | 0.01743 | 0.04658 | 50,000 |
| als | 42 | 0.08791 | 0.1378 | 0.02016 | 0.06489 | 50,000 |
| history_als | 42 | 0.1122 | 0.1665 | 0.02357 | 0.08661 | 50,000 |
| als | 7 | 0.08864 | 0.1387 | 0.02029 | 0.06557 | 50,000 |
| history_als | 7 | 0.1122 | 0.1665 | 0.02357 | 0.08664 | 50,000 |
| als | 2,024 | 0.0882 | 0.1389 | 0.02032 | 0.06486 | 50,000 |
| history_als | 2,024 | 0.1122 | 0.1667 | 0.0236 | 0.08661 | 50,000 |

**Mô hình được chọn so với từng mô hình khác (bootstrap ghép cặp)** (`significance_selected_vs_others`)

| selected | other | seed | diff | ci95 | verdict |
|---|---|---|---|---|---|
| history_als | popularity | 42 | 0.05302 | [0.05058, 0.0556] | better |
| history_als | popularity | 7 | 0.05304 | [0.05048, 0.05546] | better |
| history_als | popularity | 2024 | 0.05307 | [0.05053, 0.05559] | better |
| history_als | recent_popularity | 42 | 0.05218 | [0.04972, 0.05473] | better |
| history_als | recent_popularity | 7 | 0.0522 | [0.04964, 0.05459] | better |
| history_als | recent_popularity | 2024 | 0.05223 | [0.04964, 0.05476] | better |
| history_als | history | 42 | 0.008399 | [0.00794, 0.008847] | better |
| history_als | history | 7 | 0.008418 | [0.007976, 0.008869] | better |
| history_als | history | 2024 | 0.008451 | [0.007992, 0.008877] | better |
| history_als | item_knn | 42 | 0.04398 | [0.04172, 0.04632] | better |
| history_als | item_knn | 7 | 0.044 | [0.04171, 0.04613] | better |
| history_als | item_knn | 2024 | 0.04403 | [0.04195, 0.04632] | better |
| history_als | als | 42 | 0.02424 | [0.02224, 0.0262] | better |
| history_als | als | 7 | 0.02353 | [0.02157, 0.02536] | better |
| history_als | als | 2024 | 0.024 | [0.02206, 0.02596] | better |

## Phụ - Phân khúc và phễu - `results/07_segmentation_funnel.json`

FRACTION = **1**, chạy lúc `2026-11-21T11:05:25.198983+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Người mua | 697,470 | `rfm.n_buyers` |
| Thuật toán được chọn | kmeans | `rfm.selected_algo` |
| k tốt nhất | 4 | `rfm.best_k` |
| Tỷ lệ mua lại Instacart | 0.5897 | `instacart.reorder_rate` |

**Phân cụm: thuật toán x k (qua các seed)** (`rfm.comparison`)

| algo | k | sil_mean | sil_std | degenerate_fits | ari_between_seeds |
|---|---|---|---|---|---|
| bisecting | 3 | 0.3655 | 0.00955 | 0 | 0.6727 |
| bisecting | 4 | 0.3838 | 0.01504 | 0 | 0.6206 |
| bisecting | 5 | 0.2717 | 0.04137 | 0 | 0.6005 |
| bisecting | 6 | 0.3194 | 0.01405 | 0 | 0.5827 |
| gmm | 3 | 0.2348 | 0 | 0 | 1 |
| gmm | 4 | 0.1994 | 0.03223 | 0 | 0.9758 |
| gmm | 5 | 0.2119 | 0.004494 | 0 | 0.996 |
| gmm | 6 | 0.1785 | 0.03288 | 0 | 0.9739 |
| kmeans | 3 | 0.4548 | 0.08693 | 0 | 0.3628 |
| kmeans | 4 | 0.4572 | 0.005554 | 0 | 0.8875 |
| kmeans | 5 | 0.4252 | 0.0009653 | 0 | 0.5221 |
| kmeans | 6 | 0.4198 | 0.01957 | 0 | 0.4 |

## RQ6 - Khả năng mở rộng - `results/08_scalability_benchmark.json`

FRACTION = **1**, chạy lúc `2026-11-18T02:53:21.444756+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Base fraction | 1 | `base_fraction` |
| Mũ: aggregate | 0.4164 | `exponents.t_aggregate` |
| Mũ: FP-Growth | 0.3899 | `exponents.t_fpgrowth` |
| Mũ: PageRank 5 vòng | 0.2571 | `exponents.t_pagerank5` |
| Mũ: ALS 3 vòng | 0.7186 | `exponents.t_als3` |
| Speed-up 4 lõi: PageRank | 2.12 | `speedup.-1.speedup_pagerank5` |
| Speed-up 4 lõi: ALS | 1.97 | `speedup.-1.speedup_als3` |
| Speed-up 4 lõi: aggregate | 1.79 | `speedup.-1.speedup_aggregate` |

## Hadoop MapReduce trên dữ liệu thật - `results/09_hadoop_real.json`

FRACTION = **1**, chạy lúc `2026-11-21T10:35:19.376490+00:00`

| Chỉ số | Giá trị | Nguồn |
|---|---|---|
| Engine | hadoop | `engine` |
| Dữ liệu vào | [2019-Oct.csv, 2019-Nov.csv] | `inputs` |
| Kết quả Hadoop = Spark | có | `identical` |
| Đếm sự kiện: số khóa / khóa lệch | 356,254 / 0 | `compare_event_counts` |
| Đếm cặp: số khóa / khóa lệch | 355,861 / 0 | `compare_pair_counts` |
| Thời gian Hadoop: đếm / giỏ+cặp (s) | 327.1 / 102.3 | `jobs.*.seconds` |
| Thời gian Spark: đếm / cặp (s) | 100.7 / 168.6 | `spark` |

## Gợi ý: mở rộng lưới history và ALS (ADR-0006) - `results/10_recsys_grid_extension.json`

_chưa chạy_

## Gợi ý: test của cấu hình mở rộng, 3 seed (ADR-0007) - `results/11_recsys_test.json`

_chưa chạy_

## Hadoop 3.3.6 thật trên dữ liệu synthetic - `results/hadoop/hadoop_vs_pipe.json`

Dữ liệu: **synthetic** (3000 user) - kiểm chứng cách chạy, không phải số liệu dữ liệu thật.

| Job | Khóa Hadoop / pipe | Tổng Hadoop / pipe | Trùng khớp |
|---|---|---|---|
| event_counts | 2381 / 2381 | 82,744 / 82,744 | có |
| pair_counts | 8548 / 8548 | 16,399 / 16,399 | có |
