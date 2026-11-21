# ADR-0001: Dữ liệu và cách chia train/validation/test

* Trạng thái: Accepted (phần license: NEEDS_REVIEW đến khi `results/licenses/` có metadata từ API Kaggle)
* Có hiệu lực: #32 (notebook 06), #47 (thêm tuần validation); viết lại thành ADR ngày 21/11/2026

## Bối cảnh
Đề bài yêu cầu dữ liệu đủ lớn để cần Spark/Hadoop, lấy từ Kaggle, không crawl. Hành vi người dùng có tương quan
theo thời gian và theo người, nên chia ngẫu nhiên theo sự kiện sẽ rò rỉ tương lai vào huấn luyện.

## Quyết định
* Dữ liệu chính: Kaggle `mkechinov/ecommerce-behavior-data-from-multi-category-store` (REES46, file 2019-Oct và
  2019-Nov, 109.950.743 sự kiện theo `results/01_ingest_eda.json`). Phụ: `psparks/instacart-market-basket-analysis`.
* Lấy mẫu khi cần chỉ theo **người dùng** (`io.sample_by_user`, crc32 của `user_id`), giữ nguyên phiên.
* Gợi ý (06): tách theo **ngày**. Train < 2019-11-15 ≤ test. Validation = tuần 2019-11-08..14 lấy từ train;
  chọn cấu hình trên validation, rồi fit lại trên toàn bộ train và đo test **một lần** cho mỗi lần chạy.
  Ma trận, đồ thị hàng xóm, danh sách phổ biến chỉ xây từ phần trước mốc tương ứng.
* License: chưa kiểm từ nguồn gốc. `docs/DATASETS.md` hiện chỉ trỏ tới trang Kaggle. Cần chạy
  `kaggle-run` với `mode=meta` cho notebook 03 (đọc cả hai dataset) để lưu metadata + ngày truy cập vào
  `results/licenses/`, rồi cập nhật DATASETS.md theo đúng điều khoản ghi trong metadata.

## Hệ quả
* Không có tập test bị "nhìn" khi chọn mô hình; mọi so sánh trên test là trên cùng tập user.
* Kết quả ở FRACTION < 1 là trên tập user con, không phải tập sự kiện con, và phải ghi rõ FRACTION.
