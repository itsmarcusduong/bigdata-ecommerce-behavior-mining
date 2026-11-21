# ADR-0004: Chạy Kaggle qua GitHub Actions

* Trạng thái: Accepted
* Có hiệu lực: workflow `kaggle-run` (#39), sửa lấy output (#48), thử lại khi bị giới hạn (#44). Viết lại thành ADR ngày 21/11/2026.

## Bối cảnh
Phiên Claude không ra được kaggle.com. Các notebook cần chạy lặp lại (nhiều seed, sửa lỗi, chạy lại sau fix), nên
tự động hóa đáng chi phí.

## Quyết định
* `kaggle-run` (dispatch): `run` = push + chờ + tải; `output` = chỉ tải; `status` = xem trạng thái; `meta` = lưu
  metadata/license dataset. Secret `KAGGLE_API_TOKEN` chỉ nằm trong GitHub Secrets.
* Mỗi run ghi vào nhánh `results/kaggle-<nb>-<fraction>-<run id>`; kết quả vào `main` qua PR có comment review.
* Chỉ lấy file trong `/kaggle/working/results` (không lấy bản sao cũ trong thư mục repo clone).
* Notebook ghi `results/<nb>_stages.json` sau mỗi stage (`bdm.results.mark_stage`) để run bị dừng giữa chừng vẫn
  cho biết đã tới đâu.

## Hệ quả
Mỗi số trong báo cáo truy được tới JSON → nhánh results → run id Actions → phiên bản kernel Kaggle.
Actions giới hạn 6 h mỗi job, Kaggle 12 h mỗi phiên: notebook dài phải lấy kết quả bằng `mode=output` sau đó.
