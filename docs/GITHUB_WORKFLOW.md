# GitHub workflow

## Nguyên tắc
* `main` luôn xanh (CI: ruff + pytest + notebook đồng bộ). Không push trực tiếp vào `main` sau milestone M1.
* **Một issue = một kết quả có thể kiểm chứng** (một người chịu trách nhiệm). Issue nằm trong đúng **milestone**.
* **Một PR = một issue** (`Closes #n`), nhánh ngắn, được **người khác review** (hoặc tự review bằng checklist nếu làm một mình), squash-merge.
* Mỗi milestone kết thúc bằng **tag** `v0.N.0` + GitHub Release (ghi deliverable, link notebook Kaggle, số liệu chính).

## Nhánh và commit
| Loại | Mẫu | Ví dụ |
|---|---|---|
| Tính năng | `feat/<milestone>-<ngắn>` | `feat/m3-streaming-sketches` |
| Sửa lỗi | `fix/<ngắn>` | `fix/pcy-support-rounding` |
| Tài liệu | `docs/<ngắn>` | `docs/kaggle-instructions` |
| Kết quả thực nghiệm | `results/<nb>-<fraction>` | `results/02-full` |

Commit theo [Conventional Commits](https://www.conventionalcommits.org): `feat(sketches): add DGIM`, `test(graph): compare PageRank with networkx`,
`docs: ...`, `fix: ...`, `chore: ...`, `results: ...`. Thân commit giải thích *vì sao*.

## Vòng đời một công việc
1. Tạo issue từ template **Task** (mục tiêu đo được + Definition of Done), gắn milestone + label (`task`, `m3`, `streaming`, ...).
2. `git switch -c feat/m3-streaming-sketches` → code + test + notebook → `make test`, `make lint`, `make smoke`.
3. Mở PR từ template (liên kết `Closes #n`, ghi cách kiểm chứng). Dán kết quả/ảnh khi có.
4. Review: comment theo dòng (đúng/sai, độ phức tạp, rò rỉ dữ liệu, seed, baseline). Người viết trả lời và sửa; **không merge khi còn thread mở**.
5. Squash-merge, xóa nhánh, issue tự đóng.
6. Khi mọi issue *triển khai* của milestone đã merge (code-complete, CI xanh): `git tag -a v0.N.0 -m "..."`, `git push origin v0.N.0`, tạo Release (pre-release).
7. Milestone chỉ được **đóng** khi cả issue `kaggle-run` của nó đã đóng (đã chạy dữ liệu thật và commit `results/`).

## Milestones, issue và tag

| Milestone | Tuần | Nội dung | Tag |
|---|---|---|---|
| M1 Setup & proposal | 1-2 | repo, đặc tả, tài liệu, CI, skill | v0.1.0 |
| M2 Ingestion & EDA | 3-4 | CSV→Parquet, EDA (notebook 01) | v0.2.0 |
| M3 Streaming sketches | 5 | CMS, Bloom, DGIM, HLL (02) | v0.3.0 |
| M4 Frequent itemsets | 6 | A-Priori, PCY, FP-Growth (03) | v0.4.0 |
| M5 LSH similar items | 7 | MinHash/LSH vs naive (04) | v0.5.0 |
| M6 Link analysis | 8 | PageRank, topic PR, HITS (05) | v0.6.0 |
| M7 Recommender | 9-10 | ALS, kNN, metrics (06) | v0.7.0 |
| M8 Segmentation + Hadoop | 11 | RFM/K-Means, funnel, Hadoop Streaming (07) | v0.8.0 |
| M9 Experiments & benchmark | 12-13 | chạy đầy đủ trên Kaggle, ablation, benchmark (08) | v0.9.0 |
| M10 Report & demo | 14-15 | báo cáo, slide, demo | v1.0.0 |

> Quy ước tag: `v0.N.0` đánh dấu *code-complete + kiểm thử* của milestone; kết quả Kaggle thật được ghi nhận qua các issue
> "Run on Kaggle" và PR `results/...`. `v1.0.0` chỉ gắn sau khi M9 hoàn tất và báo cáo xong.

## Label
`task`, `bug`, `experiment`, `docs`, `kaggle-run`, `blocked`, và nhãn kỹ thuật: `streaming`, `itemsets`, `lsh`, `graph`, `recsys`, `clustering`, `hadoop`, `infra`.

## Cam kết lịch sử trung thực
Lịch sử git phản ánh đúng thời điểm làm việc thật. Không backdate commit; ngày đến hạn của milestone là kế hoạch, có thể chỉnh trong
*Issues → Milestones*.
