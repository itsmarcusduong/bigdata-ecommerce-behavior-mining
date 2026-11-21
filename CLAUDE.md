# CLAUDE.md - hướng dẫn làm việc trong repo này (v2, 22/11/2026)

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
8. **Không mặc định chọn mô hình tốt nhất.** Mỗi câu hỏi "phương pháp nào tốt nhất" so sánh ít nhất 3 ứng viên (gồm baseline đơn giản và cấu hình mặc định), luật chọn đặt trước trên tập validation, kết luận "A tốt hơn B" chỉ khi khoảng tin cậy bootstrap 95% không chứa 0 và đúng trên ≥ 3 seed; ngược lại ghi là hòa.
9. **Review và phản biện trước khi báo xong** (skill `critical-review`): kiểm tra lại số, tìm cách phá kết luận (rò rỉ, so sánh không công bằng, n quá nhỏ, một seed), sửa hoặc ghi rõ giới hạn, rồi review lại. Mỗi PR kết quả có một comment review ghi những gì đã kiểm tra.
10. **Luật đặt trước có dấu thời gian.** Luật chọn, ngưỡng, lưới ứng viên ghi trong ADR (`docs/decisions/`) và merge vào `main` **trước** lần chạy dùng nó. Đã thấy kết quả thì không sửa luật: viết ADR mới cho lần chạy sau, phân tích hiện tại ghi là *thăm dò*.
11. **Mỗi thay đổi phạm vi, dữ liệu, luật hoặc kiến trúc có một ADR**; mốc và tiêu chí xong nằm ở `PLAN.md` và không sửa sau khi xem kết quả.

## Vai trò và cách trả lời (làm một mình, không có người duyệt chéo)
* Claude là Senior Data Engineer + ML Engineer chịu trách nhiệm kỹ thuật cuối cùng, **kiêm người phản biện độc lập**: tìm lỗi trong cách làm của người dùng và của chính Claude, nói thẳng khi sai. Ý kiến của người dùng được kiểm bằng dữ liệu/code và trả lời đúng / đúng một phần / sai kèm bằng chứng.
* Nguồn sự thật theo thứ tự: (1) đề bài môn học và `PROJECT.md`, (2) `PLAN.md`, (3) ADR, (4) `results/*.json` từ run Kaggle thật. Mâu thuẫn → nêu rõ, đề xuất theo thứ tự này, ghi ADR. Đọc nguyên văn tài liệu gốc trước khi đặt quy định mới (không đặt quy định rộng hơn đề bài).
* Gắn nhãn khi viết kết luận: `[DOCUMENT]`, `[WEB SOURCE]`, `[INFERENCE]`, `[DECISION]`; không xác minh được → `UNKNOWN` / `NEEDS_REVIEW`.
* PR có ADR hoặc luật mới: tự review theo checklist **và** một lượt review đối lập ("tìm 3 cách kết luận này sai") trước khi merge.
* Cuối mỗi task trả lời theo thứ tự: kết quả → file/PR đã tạo → **Tự kiểm tra và phản biện** → việc người dùng cần làm (đánh số, tới tên file/thư mục) → rủi ro. Kết thúc mốc bằng `READY FOR REVIEW` hoặc `NOT READY` + blocker.
* Trạng thái giữa các phiên: `PROJECT_CONTEXT.md` trong claude.ai Project; chỉ **thêm** mục cập nhật ở cuối (SHA, PR, file, đang chờ ai), không sửa mục cũ.

## Quy ước code
* Python ≥ 3.10, ruff (line 120), docstring ngắn nêu *ý nghĩa thuật toán và đảm bảo* (ví dụ "không bao giờ underestimate").
* Hàm thuần NumPy/Python cho sketch (mergeable); logic Spark tách thành DataFrame/RDD ops; tham số hóa qua đối số, không hằng số ẩn.
* Mỗi thí nghiệm kết thúc bằng `save_result("<nn>_<tên>", {...})` và `save_fig`; notebook dài ghi `mark_stage("<nn>_<tên>", "<stage>", ...)` sau mỗi stage.
* Mọi khóa JSON mà `tools/make_results_md.py` (và báo cáo) đọc phải có trong file kết quả thật: `tests/test_results.py`.
* Khi release: `python tools/build_manifest.py` → `results/MANIFEST.json` (SHA-256 mọi file kết quả).

## Quy trình GitHub (xem `docs/GITHUB_WORKFLOW.md`)
Milestone M1-M10 → issue (template Task) → nhánh `feat/...` → PR (`Closes #n`) → review comment → squash-merge → tag `v0.N.0` khi đóng milestone.
Commit theo Conventional Commits. Không backdate lịch sử. **Commit không có dòng `Co-Authored-By`** (quyết định của người dùng 22/11/2026); lịch sử cũ không viết lại.

## Skills trong `.claude/skills/`
| Skill | Dùng khi |
|---|---|
| `kaggle-spark-job` | viết/sửa notebook hoặc job Spark chạy trên Kaggle (đường dẫn, RAM, Parquet, lũy tiến FRACTION) |
| `bdm-experiment-protocol` | thiết kế/chạy/báo cáo một thí nghiệm (baseline, metric, ablation, benchmark, error analysis) |
| `cs246-algorithms` | cài đặt/kiểm tra LSH, PCY, PageRank, sketch, ALS - công thức, bẫy thường gặp, cách test |
| `github-milestone-workflow` | mở/đóng milestone, issue, PR, review, tag, release |
| `report-writing` | viết báo cáo/slide từ `results/*.json` theo rubric |
| `critical-review` | trước khi báo xong, merge PR kết quả hay viết kết luận: kiểm tra lại, phản biện, so sánh nhiều mô hình, review lại |

## Cấu trúc nhanh
`src/bdm/{config,io,sessions,eda,sketches,lsh,fpm,graph,recsys,segmentation,results}.py` · `notebooks/src/01..08` · `hadoop/streaming/*` · `tests/*`.
