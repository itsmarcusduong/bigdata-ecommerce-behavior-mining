# Hadoop trong dự án

Yêu cầu môn học nhắc tới Hadoop/Spark. Cách dự án đáp ứng, trung thực về giới hạn của Kaggle (một máy, không có cụm YARN/HDFS):

| Thành phần | Cách dùng | Trạng thái kiểm chứng |
|---|---|---|
| **Spark 3.5** (PySpark) | engine chính cho mọi notebook (`local[*]`) | chạy end-to-end trên dữ liệu synthetic (CI/`make smoke`) |
| **Parquet** | định dạng lưu trữ cột của hệ Hadoop, partition theo `event_date` | kiểm thử roundtrip trong `tests/test_pipeline.py` |
| **MapReduce / Hadoop Streaming** | mapper/reducer Python: đếm (sản phẩm, loại sự kiện) và đếm cặp sản phẩm theo phiên (2 job nối tiếp) | `tests/test_hadoop_streaming.py` so kết quả với Spark qua Unix pipe |
| **Hadoop 3.3.6 thật** | `hadoop/run_hadoop_streaming.sh` (tải Hadoop, chạy job đếm + hai job cặp với `hadoop-streaming.jar`); workflow `hadoop-streaming` chạy trên runner GitHub với dữ liệu synthetic và so với Unix pipe (`tools/compare_mapreduce.py`) | **chưa chạy trong môi trường phát triển** (không tải được Hadoop ở đó) - cần chạy trên Kaggle (Internet On) hoặc máy bạn và lưu log vào `results/hadoop/` |

## Chạy
```bash
make mapreduce                                   # Unix pipe, dữ liệu synthetic
hadoop/run_local_mapreduce.sh /kaggle/input/.../2019-Oct.csv /kaggle/working/mr
hadoop/run_hadoop_streaming.sh /kaggle/input/.../2019-Oct.csv /kaggle/working/hadoop-out   # Hadoop thật, standalone
```
Hadoop standalone dùng hệ thống file cục bộ (`file://`) nên `-input`/`-output` là đường dẫn thường. Trên máy riêng bạn có thể dựng pseudo-distributed
(HDFS 1 node) rồi `hdfs dfs -put` dữ liệu và đổi đường dẫn - mapper/reducer không đổi.

## Mapping MapReduce ↔ Spark trong báo cáo
* Job "đếm" ≙ `groupBy().count()`; combiner = reducer cộng (hợp lệ vì phép cộng kết hợp, giao hoán).
* Job "cặp sản phẩm": mapper phát `(session, product)`, reducer dựng giỏ rồi phát mọi cặp; job 2 cộng đếm. So với PCY của Spark: Spark giữ giỏ trong RAM/cache
  và có bộ lọc bucket; MapReduce ghi trung gian ra đĩa → dùng làm điểm so sánh chi phí I/O.
* Sketch (CMS) là *mergeable*: xây theo từng split bằng `mapPartitions`, gộp bằng `reduce` (notebook 02 §B).
