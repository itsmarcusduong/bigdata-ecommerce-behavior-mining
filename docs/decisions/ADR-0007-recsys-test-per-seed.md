# ADR-0007: Test của notebook 10 chạy song song, một kernel mỗi seed (notebook 11)

* Trạng thái: Accepted
* Ngày: 28/11/2026, **trước** lần chạy notebook 11. Người dùng chọn phương án này (24/11/2026, sau khi kernel 10 bị
  dừng ở mốc 12 giờ).

## Bối cảnh
Notebook 10 (ADR-0006) hoàn tất cổng tái tạo và validation (#82): họ được chọn vẫn là history_als, cấu hình tốt nhất là
history half-life 7 ngày và ALS rank 128, α 10. Kernel bị Kaggle hủy ở mốc 12 giờ sau khi refit seed 42 (một lần refit
ALS rank 128 trên toàn train mất 14.532 giây), nên **chưa có kết quả test**.

## Quyết định
1. Không chọn lại gì: cấu hình test cố định theo validation của notebook 10 (`results/10_recsys_grid_extension_stages.json`,
   stage `validation`): history half-life 7 ngày; ALS rank 128, α 10, reg 0,05, 8 vòng; history_als = history trước,
   ALS lấp phần còn lại. Mốc ngày, ma trận, metric và fallback như notebook 06/10.
2. Notebook `11_recsys_test_seed` chạy **một seed mỗi kernel**, ba kernel song song (seed 42, 7, 2024). Mỗi kernel đo
   popularity, history, ALS và history_als **một lần** trên test, trên user mà history và ALS đều phục vụ được, rồi
   bootstrap ghép cặp (2.000 lần, seed = seed của kernel) cho các cặp: history_als − history, history_als − ALS,
   history_als − popularity, history − ALS.
3. `tools/combine_seed_runs.py` gộp ba file thành `results/11_recsys_test.json`: một cặp là **tốt hơn** chỉ khi CI 95%
   không chứa 0 và cùng dấu dương ở **cả 3 seed**, **kém hơn** nếu âm ở cả 3, ngược lại **hòa** (luật ADR-0002).
   Thiếu một seed → không ra verdict cho cặp đó.
4. Tập user đánh giá phải giống nhau giữa ba kernel (popularity và history là tất định): tool kiểm `n_common_users`
   và NDCG@10 của popularity/history trùng nhau giữa các seed; lệch → không gộp, báo lỗi.

## Hệ quả
* Kết luận "history_als tốt hơn ALS và history" với cấu hình mở rộng chỉ được nêu khi đủ 3 seed và verdict "better".
* Item-kNN và recent popularity vẫn không chạy lại (ADR-0006).
