# Khung báo cáo (LaTeX/Word) - 12-18 trang

1. **Giới thiệu** - bài toán, động cơ, đóng góp (3-4 gạch), câu hỏi nghiên cứu RQ1-RQ6.
2. **Dữ liệu** - REES46 + Instacart: kích thước thật (từ `01_ingest_eda.json`), schema, chất lượng, đạo đức/giấy phép. Bảng "vì sao là dữ liệu lớn".
3. **Kiến trúc hệ thống** - sơ đồ pipeline, Spark/Parquet/Hadoop Streaming, cấu hình Kaggle (CPU, RAM), tối ưu (partition, sampling theo user, không collect).
4. **Phương pháp** (mỗi mục: ý tưởng → thuật toán → độ phức tạp → tham số)
   4.1 Thuật toán luồng (CMS, Bloom, DGIM, HLL) 4.2 Tập phổ biến (A-Priori, PCY, FP-Growth, luật kết hợp)
   4.3 Shingling/MinHash/LSH và S-curve 4.4 PageRank/topic PR/HITS 4.5 Gợi ý (ALS implicit, item-kNN, popularity) 4.6 RFM + K-Means.
5. **Thiết lập thực nghiệm** - baseline, tách thời gian, độ đo, ablation, seed, phần cứng.
6. **Kết quả và phân tích** - mỗi RQ một tiểu mục: bảng + hình (lấy từ `results/`), phân tích lỗi, thảo luận.
7. **Khả năng mở rộng** - 1%→100% dữ liệu, 1/2/4 lõi, MapReduce vs Spark.
8. **Giới hạn và hướng phát triển** - dữ liệu 2019/một cửa hàng, cold-start, không chạy cụm Hadoop thật (nếu vậy), online evaluation.
9. **Kết luận.**
10. **Tài liệu tham khảo, phụ lục** (đường dẫn repo, tag, notebook Kaggle, bảng tham số đầy đủ).

Quy tắc: mỗi con số trong báo cáo ghi nguồn `results/<file>.json` (+ khóa); hình lấy từ `results/figures`.
