# Kết quả (điền từ `results/*.json` sau khi chạy Kaggle)

> **Chưa có số liệu thật trong repo.** Các con số sinh ra khi chạy `make smoke` trên dữ liệu synthetic chỉ để kiểm thử code, không được đưa vào báo cáo.
> Khi chạy Kaggle xong: copy JSON vào `results/`, điền bảng dưới (kèm `FRACTION`, ngày chạy, link phiên bản notebook).

| RQ | File nguồn | Chỉ số chính | Giá trị | FRACTION | Link Kaggle |
|---|---|---|---|---|---|
| Dataset | `01_ingest_eda.json` | n_events, n_users, n_products | ⬜ | | |
| RQ1 | `02_streaming_sketches.json` | CMS top-20 overlap, rel. err, MB; Bloom FPR; DGIM max err | ⬜ | | |
| RQ2 | `03_frequent_itemsets.json` | PCY candidate reduction; runtime theo support; #rules lift>2 | ⬜ | | |
| RQ3 | `04_lsh_similarity.json` | recall/precision LSH; speed-up vs naive; mũ phức tạp | ⬜ | | |
| RQ4 | `05_link_analysis.json` | Spearman(PR, views/purchases); overlap top-1000; topic mass | ⬜ | | |
| RQ5 | `06_recommender.json` | NDCG@10, Recall@10: popularity / item-kNN / ALS; ablation | ⬜ | | |
| (phụ) | `07_segmentation_funnel.json` | k tốt nhất, silhouette, hồ sơ phân khúc, funnel | ⬜ | | |
| RQ6 | `08_scalability_benchmark.json` | mũ thực nghiệm, speed-up theo lõi | ⬜ | | |
| Hadoop | `results/hadoop/*` | log job, thời gian, so khớp với Spark | ⬜ | | |
