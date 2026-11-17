# Đối chiếu rubric (100 điểm) với bằng chứng trong repo

| Tiêu chí | Trọng số | Bằng chứng trong repo | Trạng thái |
|---|---|---|---|
| **Technical Implementation** | 40% | | |
| Áp dụng đúng & hiệu quả kỹ thuật CS246 | | streaming (`sketches.py`), PCY/A-Priori (`fpm.py`), MinHash/LSH (`lsh.py`), PageRank/HITS (`graph.py`), ALS/kNN (`recsys.py`), K-Means (`segmentation.py`) | code + test xong |
| Code quality, documentation | | ruff trong CI, docstring, type hints, `docs/`, `CLAUDE.md` | xong |
| Scalability | | notebook 08, Parquet + partition, user-level sampling, mergeable sketches, Hadoop Streaming | chạy Kaggle: **chờ** |
| Performance benchmarking | | `results/08_scalability_benchmark.json`, hình `08_*` | chờ chạy Kaggle |
| **Dataset & Experimentation** | 20% | | |
| Dataset quality & size | | REES46 ~110M sự kiện + Instacart 32M dòng (`docs/DATASETS.md`) | xong |
| Data preprocessing | | `io.py`, `sessions.py`, notebook 01 (schema tường minh, lọc dòng hỏng, parse thời gian) | xong |
| Experimental design | | RQ1-RQ6 trong `PROJECT.md`, tách thời gian, seed cố định | xong |
| Baseline comparisons | | naive all-pairs, A-Priori vs PCY vs FP-Growth, popularity, item-kNN, exact counts | code xong; số liệu chờ |
| **Results & Analysis** | 20% | | |
| Meaningful metrics | | P/R/HR/MAP/NDCG@K, recall/precision LSH, FPR, sai số tương đối, speed-up | code xong |
| Statistical significance | | lặp nhiều seed/`FRACTION` và báo mean±std (issue #riêng); bootstrap CI cho metric gợi ý | **cần làm** (xem issue) |
| Error analysis | | notebook 06 §4 (theo độ dài lịch sử), 04 (cặp bị bỏ sót), 02 (đuôi dài của CMS) | code xong |
| Ablation studies | | ALS rank/alpha, tín hiệu hành vi, (b, r) LSH, số bucket PCY, damping PR | code xong |
| **Innovation & Impact** | 10% | | |
| Novelty | | đồ thị điều hướng (session transitions) + topic-sensitive PR; so sánh PR vs popularity; PCY vs FP-Growth trên 2 loại giỏ | |
| Practical applicability | | gợi ý cross-sell, phân khúc RFM, phát hiện sản phẩm "trọng yếu" | |
| **Presentation & Report** | 10% | | |
| Documentation / Visualization | | `README.md`, `docs/`, hình trong `results/figures` | |
| Reproducibility | | `requirements.txt`, `make test/smoke`, notebook sinh tự động, CI | xong |
| Professional presentation | | slide + demo (M10) | chờ |

"chờ" = cần chạy trên Kaggle với dữ liệu thật; xem milestone M9 và các issue `kaggle-run`.
