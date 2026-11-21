## Summary
<!-- What does this PR do and why? Link the milestone deliverable. -->

Closes #<issue>

## Changes
-

## How it was verified
- [ ] `ruff check src tests tools` passes
- [ ] `pytest` passes (includes reference checks: networkx / brute force)
- [ ] Notebook(s) re-built with `python tools/build_notebooks.py` and run on the synthetic sample (`make smoke`)
- [ ] Real-data run on Kaggle (attach `results/*.json` / link to the Kaggle notebook version) - if applicable

## Checklist
- [ ] Baseline / reference implementation compared (see `bdm-experiment-protocol`)
- [ ] Docs updated (`docs/`, README tables)
- [ ] No data files, tokens or Kaggle credentials committed

## Critical review (bắt buộc với PR kết quả hoặc thuật toán)
- [ ] JSON đúng lần chạy (`created_utc`, `fraction`), số liệu hợp lý, mọi số trong mô tả có khóa nguồn
- [ ] Không rò rỉ / không tune trên test; các mô hình được chấm trên cùng tập đánh giá
- [ ] ≥ 3 ứng viên, luật chọn đặt trước; kết luận "tốt hơn" có CI 95% và ≥ 3 seed (hoặc ghi là hòa)
- [ ] Đã thử phá kết luận (n nhỏ, seed may mắn, mặc định thư viện); điểm còn mở ghi ở đây
