# Mining Massive E-commerce Behaviour Data with Spark & Hadoop

Bài tập lớn môn **Khai phá dữ liệu lớn** (CS246 / Mining Massive Datasets): khai phá **~110 triệu sự kiện** hành vi mua sắm
(REES46, Kaggle) và **32 triệu dòng giỏ hàng** (Instacart) bằng các kỹ thuật cốt lõi của môn học, chạy trên **Kaggle Notebooks** với **PySpark**.

> *English summary:* an end-to-end big-data-mining pipeline over 110M+ e-commerce events - streaming sketches (Count-Min, Bloom, DGIM, HLL),
> frequent itemsets (A-Priori / PCY / FP-Growth), MinHash-LSH similar-item search, PageRank/HITS link analysis, implicit-feedback ALS
> recommendation, K-Means customer segmentation, plus scalability benchmarks and Hadoop Streaming MapReduce jobs.
> Every algorithm is unit-tested against a reference implementation. **No raw data is stored in this repo.**

## Trạng thái

| Hạng mục | Trạng thái |
|---|---|
| Thư viện `src/bdm` + 36 unit test (đối chiếu `networkx`, brute-force, MapReduce vs Spark) | ✅ |
| 8 notebook Kaggle chạy end-to-end trên dữ liệu synthetic cùng schema | ✅ |
| Chạy trên dữ liệu thật (Kaggle) + số liệu báo cáo | ⬜ cần thực hiện - xem [docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md) và các issue `kaggle-run` |
| Tag `v0.1.0`…`v0.9.0` + GitHub pre-release | ⬜ chạy `tools/create_tags.sh` một lần trên máy bạn (phiên tự động không được phép tạo tag/release) |
| Hadoop thật (standalone) | ⬜ script có sẵn, chưa chạy - xem [docs/HADOOP.md](docs/HADOOP.md) |

## Ý tưởng một dòng cho mỗi kỹ thuật

| Notebook | Câu hỏi | Kỹ thuật |
|---|---|---|
| [01](notebooks/01_ingest_eda.ipynb) | Dữ liệu 14 GB trông như thế nào? | Spark, Parquet, EDA quy mô lớn |
| [02](notebooks/02_streaming_sketches.ipynb) | Đếm sản phẩm hot / người dùng quay lại / cửa sổ trượt khi không giữ được mọi thứ trong RAM? | Count-Min, Bloom, DGIM, HLL |
| [03](notebooks/03_frequent_itemsets.ipynb) | Sản phẩm nào hay được mua cùng nhau? | A-Priori, **PCY**, FP-Growth, luật kết hợp |
| [04](notebooks/04_lsh_similarity.ipynb) | Tìm sản phẩm tương tự không cần so sánh mọi cặp | MinHash, **LSH**, S-curve |
| [05](notebooks/05_link_analysis.ipynb) | Sản phẩm nào "quan trọng" trong đồ thị duyệt web? | **PageRank**, topic-sensitive PR, HITS |
| [06](notebooks/06_recommender.ipynb) | Gợi ý gì cho từng người? | ALS implicit, item-kNN, đánh giá theo thời gian |
| [07](notebooks/07_segmentation_funnel.ipynb) | Có những nhóm khách hàng nào? | RFM, K-Means, funnel |
| [08](notebooks/08_scalability_benchmark.ipynb) | Hệ thống mở rộng thế nào? | benchmark theo dữ liệu và số lõi |

## Kiến trúc

```mermaid
flowchart LR
  A[Kaggle input<br/>REES46 CSV 14 GB<br/>Instacart 32M lines] --> B[Spark local[*]<br/>schema tường minh]
  B --> C[(Parquet<br/>partition theo ngày)]
  C --> D1[Sketches<br/>CMS · Bloom · DGIM · HLL]
  C --> D2[Itemsets<br/>PCY · FP-Growth]
  C --> D3[LSH<br/>MinHash]
  C --> D4[Graph<br/>PageRank · HITS]
  C --> D5[Recsys<br/>ALS · kNN]
  C --> D6[K-Means · funnel]
  A --> H[Hadoop Streaming<br/>MapReduce đối chiếu]
  D1 & D2 & D3 & D4 & D5 & D6 --> R[results/*.json + figures]
  H --> R
```

## Chạy nhanh

```bash
pip install -r requirements.txt          # cần Java 11/17/21
make test                                # 36 test, ~3-4 phút
make smoke                               # chạy cả 8 notebook trên dữ liệu synthetic
```
Chạy dữ liệu thật trên Kaggle: [docs/INSTRUCTIONS.md](docs/INSTRUCTIONS.md).

## Cấu trúc repo

```
PROJECT.md              đặc tả đề tài, RQ, dữ liệu, rubric, rủi ro
CLAUDE.md               hướng dẫn cho trợ lý AI/cộng tác viên (lệnh, quy ước, bất biến)
docs/                   INSTRUCTIONS, DATASETS, GITHUB_WORKFLOW, HADOOP, RUBRIC_MAPPING, REPORT_OUTLINE, RESULTS
src/bdm/                thư viện: io, sessions, sketches, lsh, fpm, graph, recsys, segmentation, eda
notebooks/src/*.py      nguồn notebook (jupytext "# %%"); notebooks/*.ipynb sinh bằng tools/build_notebooks.py
hadoop/                 mapper/reducer Hadoop Streaming + script chạy
tests/                  unit/integration test (pytest, Spark local)
tools/                  make_synthetic.py (dữ liệu giả cùng schema), build_notebooks.py
results/                JSON + hình từ các lần chạy Kaggle (commit qua PR)
.claude/skills/         skill: kaggle-spark-job, bdm-experiment-protocol, cs246-algorithms, github-milestone-workflow, report-writing
.github/                CI, issue/PR template
```

## Dữ liệu và đạo đức
REES46 (Michael Kechinov, Kaggle) và Instacart 2017 là dữ liệu công khai ẩn danh, dùng cho mục đích học tập; repo không chứa dữ liệu thô. Xem [docs/DATASETS.md](docs/DATASETS.md).

## Quy trình làm việc
Milestone M1-M10, issue một-người-một-kết-quả, PR có review, tag `v0.N.0`: [docs/GITHUB_WORKFLOW.md](docs/GITHUB_WORKFLOW.md).
