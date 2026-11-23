# Quyết định kiến trúc (ADR)

Mỗi thay đổi về phạm vi, dữ liệu, luật chọn mô hình hoặc kiến trúc có một ADR: `ADR-NNNN-<ten>.md` với các mục
Trạng thái · Bối cảnh · Quyết định · Hệ quả. Luật chọn phải ở trạng thái **Accepted** và đã merge vào `main`
**trước** lần chạy dùng nó (thời điểm commit là bằng chứng). Muốn đổi luật sau khi đã thấy kết quả: không sửa ADR cũ,
viết ADR mới cho lần chạy sau và ghi phân tích hiện tại là *thăm dò*.

ADR-0001 đến ADR-0004 được **viết lại sau** (21/11/2026) để ghi các quyết định đã áp dụng từ trước; mỗi ADR trỏ tới
commit/PR nơi quyết định thực sự có hiệu lực, để thời điểm vẫn kiểm chứng được qua git.

| ADR | Quyết định | Trạng thái | Có hiệu lực từ |
|---|---|---|---|
| [0001](ADR-0001-data-and-split.md) | Dữ liệu REES46 + Instacart, tách train/val/test theo ngày | Accepted (license REES46 đã kiểm; Instacart gốc NEEDS_REVIEW) | #32, #47 |
| [0002](ADR-0002-model-selection-rules.md) | Luật chọn mô hình đặt trước cho 04, 06, 07 | Accepted | #64, #67 (trước run 21/11 08:13 UTC) |
| [0003](ADR-0003-hadoop-local-mode.md) | Hadoop 3.3.6 local mode để đối chiếu độ đúng với Spark | Accepted | #67, #68 |
| [0004](ADR-0004-kaggle-via-actions.md) | Chạy Kaggle qua GitHub Actions, mỗi run một nhánh results/ + PR | Accepted | #39, #48 |
| [0005](ADR-0005-recsys-time-limit-fallback.md) | Phương án nếu notebook 06 vượt giới hạn 12 h của Kaggle | Accepted | PR này, trước khi có kết quả 06 mới |
| [0006](ADR-0006-recsys-grid-extension.md) | Mở rộng lưới history và ALS (notebook 10), cổng tái tạo trước | Accepted | PR này, trước run 10 |
