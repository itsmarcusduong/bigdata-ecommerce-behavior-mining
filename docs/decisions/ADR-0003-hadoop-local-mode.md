# ADR-0003: Hadoop MapReduce chạy local mode trên Kaggle

* Trạng thái: Accepted
* Có hiệu lực: #67 (notebook 09), #68 (sửa đọc counter). Viết lại thành ADR ngày 21/11/2026.

## Bối cảnh
Đề bài nêu Hadoop/Spark. Kaggle là một máy, không có HDFS/YARN. Spark `local[*]` là engine chính.

## Quyết định
Chạy Hadoop 3.3.6 Streaming ở local mode (4 slot) trên đúng hai file CSV thật, ba job (đếm sự kiện; giỏ → cặp; cộng
cặp), và so **từng khóa** với một bản Spark đọc cùng CSV dạng text. Tiêu chí: 0 khóa lệch và `Map input records` bằng
số dòng CSV. Thời gian chỉ là chỉ báo (một lần chạy).

## Hệ quả
Kết luận được phép: độ đúng của cài đặt MapReduce trên dữ liệu thật. Không được kết luận về hiệu năng cụm Hadoop.
