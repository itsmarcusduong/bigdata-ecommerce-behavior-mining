# PROJECT.md - Đặc tả đề tài

## 1. Tên đề tài

**Khai phá dữ liệu hành vi thương mại điện tử quy mô lớn (110 triệu sự kiện) với Spark/Hadoop:
thuật toán luồng, tập phổ biến, tìm kiếm tương tự (LSH), phân tích liên kết (PageRank/HITS) và hệ gợi ý.**

*English:* Mining 110M+ e-commerce behaviour events with Spark/Hadoop: streaming sketches, frequent itemsets,
LSH, link analysis and scalable recommendation.

Môn học: **Khai phá dữ liệu lớn (Mining Massive Datasets, theo CS246)** - Bài tập lớn.
Nhóm đề tài: *Nhóm 4 - Business & Market Analysis* (Project 4.2 "Customer behaviour") kết hợp *Project 1.2
(E-commerce recommendation)* trong danh sách gợi ý của giảng viên. Đề tài **không trùng** với các ý tưởng
nhóm khác đã đăng ký (Amazon recsys, H&M, stock, taxi, chess, GitHub analyzer, ...).

## 2. Bối cảnh và vấn đề

Một sàn thương mại điện tử ghi lại mỗi lượt xem / thêm giỏ / mua dưới dạng log sự kiện. Dữ liệu này
(i) quá lớn để xử lý trên một máy bằng pandas, (ii) đến dưới dạng **luồng**, (iii) chứa cấu trúc **tập mua cùng nhau**,
**sản phẩm tương tự**, **đồ thị điều hướng** và **sở thích ngầm** của người dùng. Đề tài xây một pipeline khai phá
trọn vẹn trên dữ liệu thật, đánh giá từng kỹ thuật bằng baseline, độ đo và benchmark khả năng mở rộng.

## 3. Câu hỏi nghiên cứu (RQ) và thí nghiệm

| RQ | Câu hỏi | Kỹ thuật CS246 | Notebook | Kết quả cần báo cáo |
|---|---|---|---|---|
| RQ1 | Sketch (Count-Min, Bloom, DGIM, HLL) đánh đổi bộ nhớ-độ chính xác thế nào so với đáp án chính xác? | Streaming algorithms | 02 | overlap top-k, sai số tương đối, FPR thực nghiệm vs lý thuyết, MB bộ nhớ |
| RQ2 | PCY giảm bao nhiêu cặp ứng viên so với A-Priori; FP-Growth nhanh hơn ở mức support nào; luật nào có lift cao? | Frequent itemsets, association rules | 03 | bảng runtime/candidates theo support, top rules |
| RQ3 | LSH đạt recall bao nhiêu ở threshold Jaccard 0.3 và nhanh hơn so sánh toàn cặp bao nhiêu lần? | Shingling, MinHash, LSH | 04 | recall/precision/candidates, đường S-curve, mũ phức tạp thực nghiệm |
| RQ4 | PageRank trên đồ thị điều hướng có cho thông tin ngoài độ phổ biến? Topic-sensitive PR thay đổi gì? | PageRank, HITS | 05 | Spearman với views/purchases, overlap top-1000, mass theo chủ đề |
| RQ5 | ALS thắng popularity/item-kNN bao nhiêu trên tách theo thời gian? Tín hiệu hành vi nào quan trọng? | Recommendation, matrix factorisation | 06 | P/R/HR/MAP/NDCG@10,20; ablation; lỗi theo độ dài lịch sử |
| RQ6 | Pipeline mở rộng ra sao theo kích thước dữ liệu và số lõi? | Scalability | 08 (+ Hadoop streaming) | mũ thực nghiệm, speed-up vs số lõi |
| (phụ) | Phân khúc khách hàng và phễu chuyển đổi | Clustering (K-Means), RFM | 07 | k theo silhouette, hồ sơ phân khúc, phễu theo ngành hàng |

## 4. Dữ liệu (không cần crawl)

| Dữ liệu | Nguồn | Quy mô | Vai trò |
|---|---|---|---|
| **E-commerce behavior data from multi category store (REES46)** | Kaggle `mkechinov/ecommerce-behavior-data-from-multi-category-store` | 2019-Oct ~42.4M dòng (5.3 GB) + 2019-Nov ~67.5M dòng (8.4 GB) = **~110M sự kiện, ~14 GB CSV** | dataset chính: luồng, đồ thị, LSH, gợi ý, phân khúc |
| **Instacart Market Basket** | Kaggle `psparks/instacart-market-basket-analysis` hoặc cuộc thi `instacart-market-basket-analysis` | 3.42M orders, **32.4M order lines**, 49.7K sản phẩm | giỏ hàng thật để khai phá tập phổ biến / luật kết hợp, hành vi mua lại |

Chi tiết schema, kích thước, lưu ý chất lượng: [docs/DATASETS.md](docs/DATASETS.md).
Vì hai bộ dữ liệu public đã đủ lớn và đa dạng nên **không cần crawl**; nếu cần mở rộng, xem mục "Mở rộng" trong DATASETS.md.

## 5. Kiến trúc và công cụ

```
Kaggle input (CSV, 14 GB) ──► Spark (local[*], 4 lõi, ~29 GB RAM) ──► Parquet (partition theo ngày)
                                   │
        ┌──────────────┬───────────┼─────────────┬──────────────┬────────────────┐
     sketches        itemsets      LSH       PageRank/HITS     ALS / kNN       K-Means / funnel
   (CMS,Bloom,DGIM)  (PCY,FP-G)  (MinHash)   (DataFrame joins)  (MLlib)         (MLlib)
        └──────────────┴───────────┴─────────────┴──────────────┴────────────────┘
                              results/*.json + figures  ──►  report / slides
 Hadoop Streaming (mapper/reducer Python): đếm sự kiện + đếm cặp sản phẩm - đối chứng với Spark
```

* **Spark 3.5 (PySpark)** là engine chính; Spark dùng thư viện client Hadoop và đọc/ghi Parquet (cột, nén, partition) - vai trò của
  HDFS/Parquet trong hệ Hadoop. Kaggle chỉ có **một máy**, nên cụm Hadoop/YARN thật không khả thi trên Kaggle; Spark chạy `local[*]`.
* **Hadoop MapReduce (Streaming)**: cùng mapper/reducer chạy bằng Unix pipe (đã kiểm thử đối chiếu với Spark) hoặc bằng Hadoop 3.3.6
  thật ở chế độ standalone/pseudo-distributed - xem [docs/HADOOP.md](docs/HADOOP.md).
* Thư viện dùng chung `src/bdm/`, notebook Kaggle sinh từ `notebooks/src/*.py` bằng `tools/build_notebooks.py`.

## 6. Quy trình đánh giá (nguyên tắc khoa học)

1. **Luôn có baseline** (naive all-pairs, A-Priori, popularity, item-kNN, đáp án chính xác bằng Spark).
2. **Tách theo thời gian** cho gợi ý (train trước `2019-11-15`, test sau) - không rò rỉ.
3. **Đối chiếu tham chiếu** trong unit test: PageRank/HITS vs `networkx`, PCY vs brute force, MapReduce vs Spark, sketch vs đáp án đúng.
4. **Ablation**: rank/alpha của ALS, loại tín hiệu hành vi, (b, r) của LSH, số bucket PCY, damping của PageRank.
5. **Benchmark**: 1%→100% dữ liệu, 1/2/4 lõi.
6. **Mọi số liệu trong báo cáo lấy từ `results/*.json` sinh ra khi chạy Kaggle**; số trong CI/synthetic chỉ để kiểm thử code.

## 7. Sản phẩm bàn giao (deliverables)

* Repo GitHub: mã nguồn, test, notebook Kaggle, `results/` (JSON + hình), tài liệu.
* 8 notebook Kaggle chạy được từ đầu đến cuối (01-08) + benchmark.
* Báo cáo (outline ở [docs/REPORT_OUTLINE.md](docs/REPORT_OUTLINE.md)) và slide demo.
* Lịch sử GitHub chuẩn: milestone, issue, PR, review comment, tag/release.

## 8. Đối chiếu rubric của giảng viên

Xem [docs/RUBRIC_MAPPING.md](docs/RUBRIC_MAPPING.md) (Technical 40% / Dataset 20% / Results 20% / Innovation 10% / Presentation 10%).

## 9. Rủi ro và giới hạn đã biết

| Rủi ro | Giảm thiểu |
|---|---|
| OOM trên Kaggle (29 GB RAM) | đọc Parquet, `FRACTION` theo user, chỉnh `BDM_SPARK_MEM`/shuffle partitions, không `collect()` dữ liệu lớn |
| Phiên Kaggle tối đa vài giờ, `/kaggle/working` giới hạn dung lượng | chạy theo notebook độc lập, lưu Parquet 1 lần rồi dùng làm input |
| `purchase` rất thưa | dùng ground truth là purchase + báo cáo n_users đánh giá; ablation tín hiệu |
| Dữ liệu 2019, một cửa hàng | nêu rõ trong phần giới hạn; không khái quát hóa |
| Hadoop cụm thật không có trên Kaggle | Spark local + Hadoop Streaming standalone, nói rõ trong báo cáo |
| Giấy phép dữ liệu | chỉ dùng cho học tập/phi thương mại, trích dẫn nguồn (xem DATASETS.md) |

## 10. Tài liệu tham khảo

* Leskovec, Rajaraman, Ullman. *Mining of Massive Datasets* (MMDS), Stanford CS246.
* Cormode & Muthukrishnan (2005), *An improved data stream summary: the Count-Min Sketch*.
* Datar, Gionis, Indyk, Motwani (2002), *Maintaining stream statistics over sliding windows* (DGIM).
* Park, Chen, Yu (1995), *An effective hash-based algorithm for mining association rules* (PCY).
* Han, Pei, Yin (2000), *Mining frequent patterns without candidate generation* (FP-Growth).
* Broder (1997), *On the resemblance and containment of documents* (MinHash); Indyk & Motwani (1998), LSH.
* Brin & Page (1998), PageRank; Haveliwala (2002), topic-sensitive PageRank; Kleinberg (1999), HITS.
* Hu, Koren, Volinsky (2008), *Collaborative filtering for implicit feedback datasets* (ALS-implicit).
* Dataset: REES46 / Michael Kechinov (Kaggle); The Instacart Online Grocery Shopping Dataset 2017.
