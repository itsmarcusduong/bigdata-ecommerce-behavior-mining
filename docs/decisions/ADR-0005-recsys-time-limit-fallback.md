# ADR-0005: Phương án khi notebook 06 vượt giới hạn thời gian Kaggle

* Trạng thái: Accepted
* Ngày: 22/11/2026 (giờ VN), **trước** khi có kết quả của lần chạy 06 theo giao thức ADR-0002
  (kernel `bdm-06-recommender` version 5, bắt đầu 21/11 08:13 UTC, vẫn RUNNING lúc 13:34 UTC).

## Bối cảnh
Giao thức 06 có 15 cấu hình validation + 3 seed refit ALS + bootstrap. Kaggle dừng phiên sau 12 h
(khoảng 20:13 UTC 21/11). Nếu bị dừng, không có `06_recommender.json` mới.

## Quyết định
1. Nếu version 5 hoàn tất (`06_recommender.json` có `created_utc` ngày 21/11 và khóa `validation_grid`): dùng nó.
2. Nếu không: chạy lại **cùng code, cùng lưới, cùng seed, cùng mốc ngày**, chỉ đổi **một yếu tố**:
   `FRACTION = 0.5` (lấy mẫu theo user). Nếu vẫn vượt: `0.25`. Không bỏ ứng viên, không đổi metric hay luật.
3. Báo cáo ghi rõ FRACTION của 06 khác các notebook khác và vì sao; so sánh giữa các họ vẫn trên cùng tập user.

## Hệ quả
Kết luận về "mô hình nào tốt nhất" giữ nguyên tính hợp lệ (luật không đổi); số tuyệt đối của 06 không so trực tiếp
với bản FRACTION = 1.0 cũ (#63).
