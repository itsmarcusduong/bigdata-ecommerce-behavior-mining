# Hướng dẫn thực hiện dự án (Project Instructions)

> Mục tiêu: chạy toàn bộ pipeline trên **Kaggle Notebooks (CPU)** với dữ liệu thật, thu `results/*.json` và hình để làm báo cáo.

## A. Chuẩn bị (một lần)

1. Tài khoản Kaggle, xác minh số điện thoại (cần để bật **Internet** trong notebook - cần để `git clone` repo).
2. Clone repo về máy: `git clone https://github.com/itsmarcusduong/bigdata-ecommerce-behavior-mining` và `pip install -r requirements.txt`.
3. Chạy kiểm thử cục bộ (cần Java 11/17/21): `make test` và `make smoke` (chạy cả 8 notebook trên dữ liệu synthetic ~ vài phút).
4. Nếu repo **private**: tạo GitHub fine-grained token (quyền *Contents: read* cho repo này) và lưu vào Kaggle → *Add-ons → Secrets* với tên
   `GITHUB_TOKEN`. Notebook tự dùng secret này khi `git clone`.

## B. Chạy trên Kaggle - theo thứ tự

Với **mỗi** notebook 01..08 (file `notebooks/0X_*.ipynb`):

1. Kaggle → *New Notebook* → *File → Import Notebook* → tải file `.ipynb` từ repo.
2. *Settings*: Accelerator **None** (không cần GPU), **Internet On**, Persistence tùy chọn.
3. *Add Input*: REES46 (mọi notebook), Instacart (03 và 07), Output của notebook 01 (02-08).
4. Ô tham số nằm trong cell bootstrap: `FRACTION` (tỷ lệ **user** giữ lại), `SPARK_MEM`.
   Hoặc đặt `os.environ["BDM_FRACTION"] = "0.1"` ở cell đầu.
5. **Chạy lũy tiến**: `FRACTION=0.01` (kiểm tra lỗi, ~vài phút) → `0.1` → `1.0` (kết quả báo cáo). Đừng chạy 1.0 trước khi 0.01 sạch lỗi.
6. *Save Version → Save & Run All (Commit)*. Tải `results/` (JSON + `figures/`) trong Output về.
7. Đưa kết quả vào repo qua **Pull Request** (`results/<tên>.json`, `results/figures/*.png`) và đóng issue "Run on Kaggle" tương ứng.

| Notebook | Input cần | Gợi ý cấu hình | Lưu ý |
|---|---|---|---|
| 01 ingest & EDA | REES46 | `FRACTION=1.0` | tạo `events_parquet/` (làm input cho 02-08) |
| 02 streaming | 01 output | 1.0 hoặc 0.5 | phần Bloom/CMS đưa dữ liệu từng ngày về driver: cần RAM driver ≥ 16 GB |
| 03 itemsets | Instacart + 01 output | support 0.1%-1% | PCY/A-Priori dùng RDD; FP-Growth dùng `numPartitions` nếu OOM |
| 04 LSH | 01 output | 0.1-1.0 | `N_EVAL` (3000) quyết định chi phí baseline naive |
| 05 link analysis | 01 output | 0.1-1.0 | đồ thị điều hướng, 40 vòng PageRank |
| 06 recommender | 01 output | 0.1-0.5 trước | đặt `BDM_CUTOFF` nếu đổi khoảng thời gian; grid ALS có 6 mô hình |
| 07 segmentation | Instacart + 01 output | 1.0 | |
| 08 benchmark | 01 output | chạy **cuối** | khởi động lại Spark với 1/2/4 lõi |

## C. Hadoop (đáp ứng yêu cầu "Hadoop/Spark")

* Spark là engine chính (chạy `local[*]` trên Kaggle vì chỉ có 1 máy). Dữ liệu trung gian là **Parquet** (định dạng chuẩn của hệ Hadoop).
* Thêm **Hadoop MapReduce (Streaming)** để đối chiếu: `make mapreduce` (Unix pipe) hoặc `hadoop/run_hadoop_streaming.sh` (Hadoop 3.3.6 thật).
  Chi tiết: [HADOOP.md](HADOOP.md). Chụp log job Hadoop và đưa vào `results/hadoop/` làm bằng chứng.

## D. Quy trình làm việc trên GitHub

Đọc [GITHUB_WORKFLOW.md](GITHUB_WORKFLOW.md): mỗi kết quả = một issue (một người), một nhánh `feat/...`, một PR có review, squash-merge,
tag `v0.N.0` khi đóng milestone.

## D2. Chạy bằng Kaggle API (không cần bấm trên giao diện)

Cần một lần trên máy của bạn (không chạy được từ phiên Claude vì mạng bị chặn tới kaggle.com):
```bash
pip install kaggle
# Kaggle -> Settings -> API -> tạo token; KHÔNG dán token vào chat/commit
mkdir -p ~/.kaggle && echo <TOKEN> > ~/.kaggle/access_token && chmod 600 ~/.kaggle/access_token   # Windows: dùng biến môi trường KAGGLE_API_TOKEN
kaggle datasets list -s ecommerce-behavior        # kiểm tra kết nối
```
Chạy từng notebook (tài khoản `minhquangdng`; sửa `USER` trong `tools/kaggle_run.py` nếu đổi):
```bash
python tools/kaggle_run.py push 01 --fraction 0.05    # chạy thử trên 5% người dùng
python tools/kaggle_run.py status 01                   # chờ COMPLETE
python tools/kaggle_run.py output 01                   # tải results/*.json + hình về results/
python tools/kaggle_run.py push 02 03 04 05 06 07 08 --fraction 1.0   # sau khi 01 đã COMPLETE
```
Notebook 02-08 tự gắn output của 01 làm input (`kernel_sources`). Kiểm tra từng file JSON trước khi commit; chỉ số liệu từ lần chạy Kaggle thật mới được đưa vào báo cáo.

## E. Xử lý sự cố

| Triệu chứng | Cách xử lý |
|---|---|
| `java: command not found` (local) | cài JDK 17; Kaggle đã có Java |
| `OutOfMemoryError` / kernel chết | giảm `FRACTION`; giảm `BDM_SPARK_MEM` nếu vượt RAM; tăng `BDM_SHUFFLE_PARTITIONS` (400); bỏ `cache()`; không `collect()` bảng lớn |
| Disk full `/kaggle/working` | xóa `spark-tmp/`; ghi Parquet nén; chỉ giữ output cần thiết |
| `FileNotFoundError: 2019-Oct.csv` | chưa Add Input đúng dataset; hoặc dữ liệu nằm trong thư mục con (resolver tìm đệ quy) |
| `git clone` lỗi trên Kaggle | bật Internet; repo private → thêm secret `GITHUB_TOKEN` |
| Task Python worker `No module named bdm` | dùng `bdm.config.get_spark` (đã set `PYTHONPATH` cho worker); đừng tạo SparkSession tay |
| Spark quá chậm | kiểm tra đã đọc Parquet (không phải CSV); giảm `max_iter`; dùng `FRACTION` |

## F. Checklist trước khi nộp

- [ ] 8 notebook chạy `Save & Run All` thành công với số liệu `FRACTION=1.0` (hoặc nêu rõ lý do dùng ít hơn)
- [ ] `results/*.json` và `results/figures/*.png` đã commit; `docs/RESULTS.md` điền từ JSON
- [ ] Báo cáo theo [REPORT_OUTLINE.md](REPORT_OUTLINE.md); mọi con số truy được tới một file JSON
- [ ] Tất cả milestone đóng, tag `v1.0.0`, Release có link Kaggle notebooks
