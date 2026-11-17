# Hướng dẫn chạy từng notebook trên Kaggle (làm tay, không cần API)

Làm lần lượt **01 → 08**. Mỗi notebook là một Kaggle Notebook riêng. Thời gian ghi dưới đây chỉ là ước lượng thô,
hãy ghi lại thời gian thật (cell cuối của mỗi notebook in ra).

## 0. Cách tự động: GitHub Actions chạy Kaggle giúp bạn (khuyên dùng)

Runner của GitHub có Internet tới kaggle.com, nên chỉ cần thêm token một lần:
1. Kaggle -> Settings -> API -> **Create New Token** (thu hồi các token cũ đã lộ).
2. GitHub repo -> **Settings -> Secrets and variables -> Actions -> New repository secret**:
   tên `KAGGLE_API_TOKEN`, giá trị là token (chuỗi `KGAT_...`).
3. **Actions -> kaggle-run -> Run workflow**: nhập `notebooks` (ví dụ `01`), `fraction` (`0.05` để thử).
   Workflow tải notebook lên Kaggle, chờ chạy xong, tải `results/*.json` + hình về và đẩy vào nhánh
   `results/kaggle-...`; từ đó mở PR để review. Nếu Kaggle chạy quá ~5.5 giờ, chạy lại với `mode = output`.
4. Thứ tự: `01` trước (tạo Parquet), sau đó `02 03`, `04 05`, `06 07`, cuối cùng `08`
   (tài khoản Kaggle thường chỉ chạy song song vài phiên CPU).

Phần dưới đây là cách làm tay nếu không dùng Actions.

## A. Thiết lập chung (một lần)

1. Kaggle → Settings: **xác minh số điện thoại** (bắt buộc để bật Internet trong notebook).
2. Mở hai dataset, bấm *New Notebook* một lần để chắc tài khoản truy cập được:
   * REES46: `mkechinov/ecommerce-behavior-data-from-multi-category-store` (~14 GB, 2 file CSV)
   * Instacart: `psparks/instacart-market-basket-analysis` (CSV thường)
3. Lấy file notebook: tải từ GitHub (`notebooks/0N_*.ipynb` → nút *Download raw file*) về máy.

## B. Quy trình chung cho mỗi notebook

1. Kaggle → **Create → New Notebook** → **File → Import Notebook** → chọn file `.ipynb` vừa tải.
2. Panel phải **Session options**: *Accelerator = None* (CPU), **Internet = On** (cell đầu `git clone` repo), *Persistence = Files only*.
3. **Add Input** theo bảng ở mục C (dataset, hoặc *Notebook Output* của notebook 01).
4. Sửa tham số ở **cell đầu tiên** (cell có `BDM_FRACTION`): đổi số trong
   `os.environ.get("BDM_FRACTION", "0.05")` thành `"1.0"` cho bản cuối (0.05 = thử nhanh trên 5% người dùng).
5. **Save Version → Save & Run All (Commit)**. Chờ trạng thái *Complete*; xem log nếu *Failed*.
6. Mở tab **Output** của version vừa chạy, tải `results/*.json` và `results/figures/*.png` về, đặt vào cùng
   thư mục trong repo. **Đọc từng file JSON** (không có 0 dòng, giá trị không bất thường) rồi mới commit.
7. Đặt tên notebook theo bảng (để đồng nhất và dễ thêm làm input).

Mẹo: chạy thử `0.05` trước cho cả 8 notebook (nhanh, bắt lỗi sớm), rồi mới chạy `1.0`.

## C. Chi tiết từng notebook

| # | Tên đặt trên Kaggle | Add Input | Bản thử → bản cuối | Kết quả cần tải về | Kiểm tra sau khi chạy |
|---|---|---|---|---|---|
| 01 | `bdm-01-ingest-eda` | dataset REES46 | `0.05` → `1.0` | `results/01_ingest_eda.json`, `figures/01_*.png`; **thư mục `events_parquet` nằm trong Output** | `n_events` ≈ 110M khi `1.0`; có thời gian `csv_to_parquet` |
| 02 | `bdm-02-streaming-sketches` | Notebook Output của 01 | `0.05` → `1.0` | `02_streaming_sketches.json`, `02_cms_accuracy.png` | CMS không bao giờ underestimate; Bloom FPR gần lý thuyết; HLL sai số vài % |
| 03 | `bdm-03-frequent-itemsets` | dataset Instacart + Output của 01 | `0.05` → `1.0` | `03_frequent_itemsets.json`, `03_apriori_pcy_fpgrowth.png` | A-Priori, PCY, FP-Growth ra cùng tập cặp; PCY ít ứng viên hơn |
| 04 | `bdm-04-lsh-similarity` | Output của 01 | `0.05` → `1.0` | `04_lsh_similarity.json`, `04_s_curve.png` | recall LSH ≥ ~0.9 (tham số `min_prob`); LSH nhanh hơn naive khi tăng kích thước |
| 05 | `bdm-05-link-analysis` | Output của 01 | `0.05` → `1.0` | `05_link_analysis.json`, `05_pr_*.png` | PageRank hội tụ, tổng ≈ 1; có kết quả topic-sensitive và HITS |
| 06 | `bdm-06-recommender` | Output của 01 | `0.05` → `1.0` | `06_recommender.json`, `06_model_comparison.png` | ALS so với popularity/item-kNN trên tập test theo ngày; có P/R/NDCG@K |
| 07 | `bdm-07-segmentation-funnel` | dataset Instacart + Output của 01 | `0.05` → `1.0` | `07_segmentation_funnel.json`, `07_*.png` | funnel view→cart→purchase giảm dần; RFM có k cụm rõ ràng |
| 08 | `bdm-08-scalability-benchmark` | Output của 01 | giữ mặc định | `08_scalability_benchmark.json`, `08_*_scaling.png` | thời gian tăng theo kích thước; speedup theo số core |

Ghi chú:
* Notebook 02–08 đọc `events_parquet` từ Output của 01. Nếu không thêm, chúng quay về đọc CSV thô (chạy được nhưng
  chậm hơn nhiều): hãy chạy 01 xong trước.
* Thêm *Notebook Output* ở bước *Add Input → Your Work / Notebook Output Files*, chọn `bdm-01-ingest-eda`.
* Nếu hết bộ nhớ (`OutOfMemoryError`, kernel chết): giảm `BDM_FRACTION` (0.5, 0.25), hoặc giảm `BDM_SPARK_MEM`
  trong cell đầu (ví dụ `"16g"`), và xem `docs/INSTRUCTIONS.md` mục E.
* Notebook 08 chạy lâu nhất (nhiều lần chạy lặp theo kích thước/số core); biến `BDM_BENCH_FRACTIONS`, `BDM_BENCH_CORES`
  có thể thu nhỏ trong cell tham số nếu quá lâu.

## D. Đưa kết quả vào repo

```bash
git checkout -b feat/kaggle-results
git add results/*.json results/figures
git commit -m "feat: real Kaggle results for notebooks 01-08"
git push -u origin feat/kaggle-results
```
Mở PR, ghi vào PR link các notebook Kaggle và `FRACTION` đã dùng. Chỉ số liệu từ các lần chạy thật này mới được
đưa vào báo cáo (xem `CLAUDE.md`, bất biến số 1).
