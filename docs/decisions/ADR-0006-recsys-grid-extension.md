# ADR-0006: Mở rộng lưới cho history và ALS (notebook 10)

* Trạng thái: Accepted
* Ngày: 24/11/2026, **trước** lần chạy notebook 10. Người dùng đồng ý chạy ngày 24/11/2026.

## Bối cảnh
Ở run 06 theo ADR-0002 (#76), cấu hình thắng của hai họ nằm ở **biên lưới**: history half-life 7 ngày (lớn nhất đã
thử) và ALS rank 64 (lớn nhất), α = 10 (nhỏ nhất). Kết luận chính "history thắng ALS, history_als thắng mọi họ" có thể
sai nếu ALS chưa được chỉnh đủ: đó là so sánh không công bằng (skill `critical-review`, mục 2). Chạy lại toàn bộ 06
với lưới rộng hơn mất khoảng 16 giờ, vượt giới hạn 12 giờ của Kaggle.

## Quyết định
Một notebook riêng, `10_recsys_grid_extension`, cùng dữ liệu, cùng mốc (fit < 2019-11-08 ≤ validation < 2019-11-15 ≤
test), cùng metric và cùng luật chọn như ADR-0002. Chỉ đổi **một yếu tố**: lưới của history và ALS.

1. **Cổng tái tạo** (chạy trước, trên validation): popularity, history half-life 7 và ALS rank 64 / α 10 / seed 42
   phải cho NDCG@10 validation khớp `results/06_recommender.json` (popularity và history: sai khác ≤ 0,0001; ALS:
   ≤ 0,002). Nếu không khớp: dừng, ghi `gate.passed = false`, **không kết luận gì** về cấu hình mới.
2. **Lưới**: history half-life ∈ {7, 14, 30} ngày; ALS rank ∈ {64, 128} × α ∈ {3, 10} (seed 42 trên validation).
   history_als = history tốt nhất + ALS tốt nhất.
3. **Chọn**: họ có NDCG@10 validation cao nhất trong popularity, history, ALS, history_als của notebook 10, cùng
   item-kNN (0,08128) và recent popularity (0,06955) lấy nguyên từ run 06 (cùng tập validation, không ở biên lưới).
4. **Test, một lần**: popularity, history, ALS × 3 seed {42, 7, 2024}, history_als × 3 seed, trên user mà history và
   ALS đều phục vụ được. Item-kNN và recent popularity **không** chạy lại: không ở biên lưới, và history_als đã hơn
   chúng ≥ 0,044 NDCG@10 ở mọi seed (#76). "A tốt hơn B" chỉ khi CI 95% bootstrap ghép cặp (2.000 lần) của A − B
   không chứa 0 ở **mọi** seed; ngược lại là hòa.
5. Chỉ mở rộng **một lần**. Nếu cấu hình thắng mới vẫn ở biên (rank 128, α 3 hoặc half-life 30), ghi là giới hạn,
   không mở rộng tiếp trong đồ án này.

## Hệ quả
* Kết luận của #76 được giữ nếu họ được chọn và các verdict không đổi; nếu đổi, báo cáo, slide và release notes được
  sửa và phát hành v1.1.0. Kết quả 06 cũ vẫn giữ trong repo (lịch sử), báo cáo ghi rõ bản nào là kết luận cuối.
* So sánh với item-kNN và recent popularity ở notebook 10 dựa trên validation và kết quả test của #76, không phải
  bootstrap mới.
