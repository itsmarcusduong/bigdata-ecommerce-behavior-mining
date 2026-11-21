# ADR-0002: Luật chọn mô hình đặt trước

* Trạng thái: Accepted
* Có hiệu lực: #64 (3dbb807, 21/11/2026 07:23 UTC, CLAUDE.md bất biến 8 + skill `critical-review`) và
  #67 (814744a, 08:12 UTC, cài vào notebook 04/06/07). Mọi run dùng luật này được dispatch từ 08:13 UTC trở đi.
  Viết lại thành ADR ngày 21/11/2026.

## Bối cảnh
Kết quả đầu tiên của 06 (ALS chỉnh trên test) và 07 (chọn K-Means k=4 mặc định) đã chọn mô hình mà không có
luật đặt trước. Làm một mình, không có người duyệt chéo, nên luật phải cố định trước khi thấy kết quả.

## Quyết định
* Chung: ≥ 3 ứng viên, gồm baseline đơn giản và cấu hình mặc định; mỗi họ có lưới riêng; báo mọi ứng viên.
* **06 gợi ý**: họ popularity, recent_popularity {3,7,14 ngày}, history {half-life 1,3,7}, item_knn {cart+purchase, all},
  ALS rank {16,32,64} × alpha {10,40}, history_als. Chọn họ có NDCG@10 validation cao nhất (mọi user validation,
  bù bằng popularity). Trên test, "A tốt hơn B" chỉ khi khoảng tin cậy 95% của bootstrap ghép cặp (2000 lần) của A−B
  không chứa 0 ở **mọi** seed {42, 7, 2024}; ngược lại là hòa.
* **07 phân cụm**: K-Means, Bisecting K-Means, GMM × k ∈ {3,4,5,6} × 3 seed. Chọn silhouette trung bình cao nhất
  trong các cấu hình không có fit suy biến (`clusters_used < k`); "tốt hơn" chỉ khi cao hơn ở mọi seed.
* **04 LSH**: báo recall với khoảng Wilson trên tập đánh giá nhiều danh mục, 2 ngưỡng (0,3; 0,2), 3 seed; LSH tự cài
  và MLlib được so ở cùng tập.

## Hệ quả
* Mọi phân tích dựa trên tiêu chí khác (ví dụ ARI ổn định giữa seed ở 07) là *thăm dò*, không thay luật chọn.
  Nếu muốn dùng, ghi vào ADR mới cho lần chạy sau.
