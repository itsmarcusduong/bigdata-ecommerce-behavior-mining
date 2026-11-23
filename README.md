# Mining Massive E-commerce Behaviour Data with Spark & Hadoop

Bài tập lớn môn **Khai phá dữ liệu lớn** (CS246 / Mining Massive Datasets): khai phá **109.950.743 sự kiện** hành vi mua sắm
(REES46, Kaggle) và **3,06 triệu giỏ hàng** (Instacart) bằng các kỹ thuật cốt lõi của môn học, chạy trên **Kaggle Notebooks** với **PySpark**,
đối chiếu với **Hadoop 3.3.6 MapReduce** trên cùng dữ liệu thật.

> *English summary:* an end-to-end big-data-mining pipeline over 110M e-commerce events - streaming sketches (Count-Min, Bloom, DGIM,
> HyperLogLog), frequent itemsets (A-Priori / PCY / FP-Growth), MinHash-LSH similar-item search, PageRank/HITS link analysis,
> six recommender families compared under a selection rule fixed before the run, RFM segmentation with three clustering
> algorithms, scalability benchmarks, and Hadoop Streaming jobs that agree with Spark on every key. Every algorithm is unit-tested
> against a reference implementation; every number in the report comes from `results/*.json` of a real Kaggle run.
> **No raw data is stored in this repo.**

## Trạng thái

| Hạng mục | Trạng thái |
|---|---|
| Thư viện `src/bdm` + test pytest (đối chiếu `networkx`, brute force, MapReduce vs Spark, khóa JSON của kết quả thật) | ✅ CI xanh |
| Notebook 01–09 trên **toàn bộ dữ liệu** (FRACTION = 1,0), kết quả trong `results/`, bảng tự sinh [docs/RESULTS.md](docs/RESULTS.md) | ✅ |
| Hadoop 3.3.6 trên CSV thật (notebook 09), trùng Spark từng khóa | ✅ |
| Báo cáo, slide, release [v1.0.0](https://github.com/itsmarcusduong/bigdata-ecommerce-behavior-mining/releases/tag/v1.0.0) | ✅ |
| Notebook 10: mở rộng lưới gợi ý để kiểm kết luận của 06 ([ADR-0006](docs/decisions/ADR-0006-recsys-grid-extension.md)) | ⏳ đang chạy trên Kaggle |

## Ý tưởng một dòng cho mỗi kỹ thuật

| Notebook | Câu hỏi | Kỹ thuật |
|---|---|---|
| [01](notebooks/01_ingest_eda.ipynb) | Dữ liệu 14,7 GB trông như thế nào? | Spark, Parquet, EDA quy mô lớn |
| [02](notebooks/02_streaming_sketches.ipynb) | Đếm sản phẩm hot / người dùng phân biệt / cửa sổ trượt khi không giữ được mọi thứ trong RAM? | Count-Min (3 seed), Bloom, DGIM, HLL (10 salt) |
| [03](notebooks/03_frequent_itemsets.ipynb) | Sản phẩm nào hay được mua cùng nhau? | A-Priori, **PCY**, FP-Growth, luật kết hợp |
| [04](notebooks/04_lsh_similarity.ipynb) | Tìm sản phẩm tương tự không cần so sánh mọi cặp | MinHash, **LSH** vs MLlib, recall có khoảng Wilson, 3 seed |
| [05](notebooks/05_link_analysis.ipynb) | Sản phẩm nào "quan trọng" trong đồ thị duyệt web? | **PageRank**, topic-sensitive PR, HITS |
| [06](notebooks/06_recommender.ipynb) | Mô hình gợi ý nào tốt nhất? | 6 họ (popularity, recent popularity, history, item-kNN, ALS, history + ALS), luật chọn đặt trước, 3 seed, bootstrap ghép cặp |
| [07](notebooks/07_segmentation_funnel.ipynb) | Có những nhóm khách hàng nào? | RFM; K-Means, Bisecting K-Means, GMM × k × 3 seed; funnel |
| [08](notebooks/08_scalability_benchmark.ipynb) | Hệ thống mở rộng thế nào? | benchmark theo dữ liệu và số lõi |
| [09](notebooks/09_hadoop_real.ipynb) | MapReduce có cho cùng kết quả với Spark trên dữ liệu thật? | Hadoop 3.3.6 Streaming, so từng khóa |
| [10](notebooks/10_recsys_grid_extension.ipynb) | Kết luận của 06 có còn đúng khi ALS và history được chỉnh rộng hơn? | cổng tái tạo + lưới mở rộng (ADR-0006) |

## Kết quả chính (v1.0.0, toàn bộ dữ liệu)

Số lấy từ `results/*.json`; bảng đầy đủ kèm khóa nguồn ở [docs/RESULTS.md](docs/RESULTS.md).

- **Sketch:** Count-Min 5,4 MB không bao giờ underestimate ở 3 seed, top-k overlap 1,0; Bloom FPR 0,08% (mục tiêu 1%).
- **Tập phổ biến:** PCY giảm 99,6% cặp ứng viên so với A-Priori, cùng kết quả, nhưng chậm hơn trong bản RDD Python.
- **LSH:** 1.578 cặp tương tự trong 202 s (so mọi cặp ~81 giờ); ở Jaccard 0,2 recall 0,975 so với 0,870 của MinHashLSH mặc định.
- **PageRank:** Spearman 0,79 với lượt xem, 0,60 với lượt mua.
- **Gợi ý:** history + ALS được chọn trên validation, tốt hơn mọi họ ở cả 3 seed (NDCG@10 test 0,1122; history 0,1038; ALS 0,0883;
  popularity 0,0591). Cấu hình thắng nằm ở biên lưới, nên notebook 10 đang kiểm lại.
- **Phân khúc:** K-Means k = 4, hòa với k = 3; nhóm trung thành 10,8% người mua chiếm 50,8% chi tiêu.
- **Hadoop:** 356.254 khóa đếm và 355.861 cặp sản phẩm trùng Spark, 0 khóa lệch.
- **Mở rộng:** ~6,9 triệu sự kiện/s; 4 lõi nhanh 1,8–2,1× so với 1 lõi.

## Kiến trúc

```mermaid
flowchart LR
  A[Kaggle input<br/>REES46 CSV 14,7 GB<br/>Instacart] --> B[Spark local[*]<br/>schema tường minh]
  B --> C[(Parquet<br/>partition theo ngày)]
  C --> D1[Sketches<br/>CMS · Bloom · DGIM · HLL]
  C --> D2[Itemsets<br/>PCY · FP-Growth]
  C --> D3[LSH<br/>MinHash]
  C --> D4[Graph<br/>PageRank · HITS]
  C --> D5[Recsys<br/>6 họ · luật đặt trước]
  C --> D6[RFM · 3 thuật toán phân cụm · funnel]
  A --> H[Hadoop 3.3.6 Streaming<br/>trên CSV thật]
  D1 & D2 & D3 & D4 & D5 & D6 --> R[results/*.json + figures]
  H --> R
  R --> P[PR có review → main → release]
```

GitHub Actions điều khiển Kaggle qua API (workflow `kaggle-run`: run / output / status / meta); mỗi lần chạy ghi vào một nhánh
`results/...` và vào `main` qua pull request có comment review.

## Chạy nhanh

```bash
pip install -r requirements.txt          # cần Java 11/17/21
make test                                # pytest, Spark local
make smoke                               # chạy các notebook trên dữ liệu synthetic
```
Chạy dữ liệu thật trên Kaggle: [docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md) (API / Actions) hoặc [docs/KAGGLE_RUNBOOK.md](docs/KAGGLE_RUNBOOK.md) (làm tay).

## Cấu trúc repo

```
PROJECT.md              đặc tả đề tài, RQ, dữ liệu, rubric, rủi ro
PLAN.md                 mốc M1-M11 và tiêu chí xong (đặt trước khi xem kết quả)
CLAUDE.md               hướng dẫn cho trợ lý AI/cộng tác viên (lệnh, quy ước, bất biến)
docs/                   INSTRUCTIONS, DATASETS, GITHUB_WORKFLOW, HADOOP, RUBRIC_MAPPING, REPORT_OUTLINE, RESULTS
docs/decisions/         ADR: dữ liệu và split, luật chọn mô hình, Hadoop, Kaggle qua Actions, phương án dự phòng, mở rộng lưới
src/bdm/                thư viện: io, sessions, sketches, lsh, fpm, graph, recsys, segmentation, eda, results
notebooks/src/*.py      nguồn notebook (jupytext "# %%"); notebooks/*.ipynb sinh bằng tools/build_notebooks.py
hadoop/                 mapper/reducer Hadoop Streaming + script chạy
tests/                  unit/integration test (pytest, Spark local)
tools/                  kaggle_run.py, make_results_md.py, build_manifest.py, build_notebooks.py, make_synthetic.py, compare_mapreduce.py, create_tags.sh
results/                JSON + hình + log Hadoop + metadata license từ các lần chạy Kaggle; MANIFEST.json (SHA-256)
.claude/skills/         kaggle-spark-job, bdm-experiment-protocol, cs246-algorithms, github-milestone-workflow, report-writing, critical-review
.github/                CI, kaggle-run, hadoop-streaming, release-tags; issue/PR template
```

## Dữ liệu, giấy phép và ghi nguồn
- **REES46:** dữ liệu do [REES46 Marketing Platform](https://rees46.com) thu thập, công bố trên Kaggle tại
  [mkechinov/ecommerce-behavior-data-from-multi-category-store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
  License `copyright-authors`; được dùng miễn phí khi ghi nguồn hai link trên.
- **Instacart:** [psparks/instacart-market-basket-analysis](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis)
  (The Instacart Online Grocery Shopping Dataset 2017); chỉ dùng cho học tập, phi thương mại.
- Bằng chứng license (metadata API Kaggle kèm ngày truy cập): `results/licenses/`. Repo không chứa dữ liệu thô. Chi tiết: [docs/DATASETS.md](docs/DATASETS.md).

## Quy trình làm việc
Milestone, issue một kết quả, PR có review đối lập, luật chọn mô hình ghi ADR trước khi chạy, tag và release:
[docs/GITHUB_WORKFLOW.md](docs/GITHUB_WORKFLOW.md), [CLAUDE.md](CLAUDE.md).
