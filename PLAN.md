# PLAN - mốc và tiêu chí xong

Tiêu chí xong là điều kiện kiểm được (file, khóa JSON, test, PR). **Không sửa tiêu chí sau khi đã thấy kết quả**;
muốn đổi thì thêm dòng "Đổi tiêu chí" có ngày và lý do ở cuối file.

Ghi chú trung thực: file này lập ngày 21/11/2026. Tiêu chí của M1-M8 được chép lại từ `docs/GITHUB_WORKFLOW.md`,
`docs/INSTRUCTIONS.md` (mục F) và các issue đã có từ trước; tiêu chí của M9-M10 đặt mới ở đây, **trước** khi làm báo
cáo, slide và release.

| Mốc | Nội dung | Tiêu chí xong | Trạng thái |
|---|---|---|---|
| M1 | Repo, đặc tả, CI, skill | `PROJECT.md`, CLAUDE.md, CI (ruff + pytest + notebook đồng bộ) xanh; tag v0.1.0 | xong |
| M2 | CSV→Parquet, EDA (01) | `results/01_ingest_eda.json` ở FRACTION 1.0, `n_events` = số dòng CSV − header | xong (109.950.743) |
| M3 | Sketch (02) | CMS không bao giờ underestimate ở ≥ 3 seed; HLL nhiều salt, sai số so với rsd | xong (#69) |
| M4 | Itemsets (03) | A-Priori = PCY = FP-Growth cùng tập cặp; số ứng viên PCY < A-Priori | xong |
| M5 | LSH (04) | Recall có khoảng Wilson trên tập có ≥ 50 cặp thật, 2 ngưỡng, 3 seed; so MLlib cùng tập | xong (#71) |
| M6 | Link analysis (05) | PageRank tổng ≈ 1, khớp networkx trong test; topic PR và HITS | xong |
| M7 | Recommender (06) | Giao thức ADR-0002 trên dữ liệu thật: lưới validation, ≥ 3 seed, bootstrap ghép cặp, verdict | xong (#76; không cần ADR-0005) |
| M8 | Phân khúc + Hadoop (07, 09) | 07: 3 thuật toán × k × 3 seed, 0 fit suy biến; 09: Hadoop thật = Spark, 0 khóa lệch | xong (#72, #73) |
| M9 | Thí nghiệm + benchmark (08) | Benchmark theo kích thước và số lõi; issue #22 đóng khi 02/04/06/07 có kết quả nhiều seed | xong (#22 đóng ở #76) |
| M10 | Báo cáo, slide, release | (1) báo cáo có tóm tắt tiếng Anh, tên phân khúc, mục 6.1-6.6, 7, 8, 9 cập nhật; (2) **mọi số** trong báo cáo và slide khớp một khóa JSON (kiểm bằng review độc lập); (3) slide; (4) `results/MANIFEST.json` (SHA-256 mọi JSON) ở commit release; (5) tag v1.0.0 + Release; đóng #22-#25 | xong (v1.0.0 tại 3b75ab3) |
| M11 (sau v1.0.0) | Mở rộng lưới gợi ý (10, ADR-0006) | cổng tái tạo qua; lưới history {7,14,30} × ALS {64,128}×{3,10} trên validation; test 3 seed; verdict cho history_als vs ALS, history_als vs history, history vs ALS; báo cáo/slide/release notes sửa nếu kết luận đổi (v1.1.0) | xong: validation (#82), test 3 seed qua notebook 11 (ADR-0007, PR kết quả 11); kết luận 06 giữ nguyên |
