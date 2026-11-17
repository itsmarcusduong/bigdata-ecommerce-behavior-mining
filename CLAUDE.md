# CLAUDE.md - hướng dẫn làm việc trong repo này

Dự án: khai phá dữ liệu hành vi thương mại điện tử quy mô lớn (REES46 ~110M sự kiện + Instacart 32M dòng) bằng PySpark trên Kaggle.
Đặc tả đầy đủ: `PROJECT.md`. Hướng dẫn chạy: `docs/INSTRUCTIONS.md`. Ngôn ngữ tài liệu: tiếng Việt; code/identifier/commit: tiếng Anh.

## Lệnh thường dùng
```bash
pip install -r requirements.txt     # cần Java 11/17/21
make lint                           # ruff check src tests tools
make test                           # pytest (khởi tạo Spark local, ~2-3 phút)
python tools/build_notebooks.py     # notebooks/src/*.py -> notebooks/*.ipynb  (phải commit cả hai)
make smoke                          # chạy cả 8 notebook trên dữ liệu synthetic
make mapreduce                      # Hadoop Streaming qua Unix pipe
```

## Bất biến (không được vi phạm)
1. **Không bịa số liệu.** Số trong báo cáo/README chỉ lấy từ `results/*.json` sinh bởi lần chạy Kaggle thật. Kết quả của `make smoke`/test (synthetic) chỉ để kiểm thử.
2. **Không rò rỉ thời gian**: gợi ý tách train/test theo ngày (`sessions.time_split`); ma trận, đồ thị hàng xóm, danh sách phổ biến chỉ xây từ train.
3. **Mỗi thuật toán có tham chiếu trong test** (networkx, brute force, đáp án chính xác). Sửa thuật toán → chạy lại test tương ứng.
4. **Không `collect()`/`toPandas()` bảng lớn**; chỉ kết quả đã tổng hợp hoặc mẫu giới hạn. Sample theo **user** (`io.sample_by_user`) để giữ nguyên phiên.
5. **Dùng `bdm.config.get_spark`** (cấu hình bộ nhớ + `PYTHONPATH` cho worker). Không băm bằng `hash()` của Python trong job phân tán (bị salt) - dùng `zlib.crc32` hoặc hàm Spark.
6. **Không commit dữ liệu, token, `kaggle.json`.** `data/sample/*.csv` và Parquet nằm trong `.gitignore`.
7. Notebook `.ipynb` chỉ được sinh từ `notebooks/src/*.py`; CI kiểm tra đồng bộ. Không sửa tay `.ipynb`.

## Quy ước code
* Python ≥ 3.10, ruff (line 120), docstring ngắn nêu *ý nghĩa thuật toán và đảm bảo* (ví dụ "không bao giờ underestimate").
* Hàm thuần NumPy/Python cho sketch (mergeable); logic Spark tách thành DataFrame/RDD ops; tham số hóa qua đối số, không hằng số ẩn.
* Mỗi thí nghiệm kết thúc bằng `save_result("<nn>_<tên>", {...})` và `save_fig`.

## Quy trình GitHub (xem `docs/GITHUB_WORKFLOW.md`)
Milestone M1-M10 → issue (template Task) → nhánh `feat/...` → PR (`Closes #n`) → review comment → squash-merge → tag `v0.N.0` khi đóng milestone.
Commit theo Conventional Commits. Không backdate lịch sử. Khi tạo commit/PR bằng Claude, giữ các dòng attribution mà môi trường yêu cầu.

## Skills trong `.claude/skills/`
| Skill | Dùng khi |
|---|---|
| `kaggle-spark-job` | viết/sửa notebook hoặc job Spark chạy trên Kaggle (đường dẫn, RAM, Parquet, lũy tiến FRACTION) |
| `bdm-experiment-protocol` | thiết kế/chạy/báo cáo một thí nghiệm (baseline, metric, ablation, benchmark, error analysis) |
| `cs246-algorithms` | cài đặt/kiểm tra LSH, PCY, PageRank, sketch, ALS - công thức, bẫy thường gặp, cách test |
| `github-milestone-workflow` | mở/đóng milestone, issue, PR, review, tag, release |
| `report-writing` | viết báo cáo/slide từ `results/*.json` theo rubric |

## Cấu trúc nhanh
`src/bdm/{config,io,sessions,eda,sketches,lsh,fpm,graph,recsys,segmentation,results}.py` · `notebooks/src/01..08` · `hadoop/streaming/*` · `tests/*`.
