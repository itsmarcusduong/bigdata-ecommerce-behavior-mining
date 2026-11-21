# Datasets

## 1. REES46 - eCommerce behavior data from multi category store (dataset chính)

* Kaggle: `https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store`
* Files dùng: `2019-Oct.csv`, `2019-Nov.csv` (các tháng sau có trong dataset nhưng ta chỉ dùng 2 tháng để vừa tài nguyên Kaggle).
* Quy mô (theo các báo cáo bên thứ ba đã đọc): Oct ≈ **42.4M** dòng / 5.3 GB; Nov ≈ **67.5M** dòng / 8.4 GB → **~110M sự kiện, ~14 GB**.
  *Hãy xác nhận lại số dòng bằng notebook 01 (`results/01_ingest_eda.json`) - đó mới là số được báo cáo.*

| Cột | Ý nghĩa |
|---|---|
| `event_time` | thời điểm sự kiện, dạng `2019-10-01 00:00:00 UTC` |
| `event_type` | `view`, `cart`, `remove_from_cart`, `purchase` |
| `product_id`, `category_id` | id sản phẩm, id danh mục |
| `category_code` | danh mục dạng `electronics.smartphone` (**có nhiều null**) |
| `brand` | thương hiệu (có null) |
| `price` | giá (float) |
| `user_id` | id người dùng |
| `user_session` | id phiên (null ở một số dòng) → dùng làm *giỏ* và *đường duyệt* |

Lưu ý chất lượng: `category_code`/`brand` thiếu nhiều; `purchase` chiếm tỷ lệ nhỏ; sự kiện trùng lặp có thể xuất hiện; một
tập `price` bằng 0/âm có thể có → notebook 01 đo các tỷ lệ này. Giấy phép: **NEEDS_REVIEW** - chưa đọc từ nguồn gốc. Bằng chứng sẽ là metadata API Kaggle
lưu trong `results/licenses/` (workflow `kaggle-run`, `mode=meta`, xem ADR-0001). Trong lúc chờ: chỉ dùng cho học tập,
trích dẫn nguồn, không phân phối lại dữ liệu trong repo - repo **không chứa dữ liệu**.

## 2. Instacart Market Basket Analysis (giỏ hàng thật)

* Kaggle: cuộc thi `instacart-market-basket-analysis` hoặc dataset cộng đồng `psparks/instacart-market-basket-analysis` (CSV thường).
  `bdm.config.find_file` tìm file theo tên trong `/kaggle/input/**` và tự giải nén `*.csv.zip` nếu cần, nên dùng nguồn nào cũng được
  (với cuộc thi: bạn phải chấp nhận luật của cuộc thi trên Kaggle trước).
* Bảng: `orders.csv` (3,421,083 dòng), `order_products__prior.csv` (**32,434,489** dòng), `order_products__train.csv` (1,384,617),
  `products.csv` (49,688), `aisles.csv` (134), `departments.csv` (21).
* Vai trò: khai phá **tập phổ biến / luật kết hợp** trên giỏ hàng thật (A-Priori, PCY, FP-Growth) và phân tích mua lại.
* Giấy phép: **NEEDS_REVIEW** (cùng cách kiểm như REES46, ADR-0001). Ghi chú hiện có, chưa kiểm từ nguồn gốc: dữ liệu do
  Instacart công bố (2017) cho mục đích nghiên cứu/phi thương mại; trích dẫn nguồn.

## 3. Vì sao đủ "dữ liệu lớn"?

| Tiêu chí | Thực tế |
|---|---|
| Volume | ~110M dòng + 32M dòng (≈ 14-15 GB CSV), vượt khả năng pandas trên 1 máy 29 GB RAM |
| Velocity | log sự kiện theo thời gian, được phát lại theo ngày (micro-batch) cho thuật toán luồng |
| Variety | sự kiện hành vi, giỏ hàng quan hệ, đồ thị điều hướng, metadata danh mục/thương hiệu/giá |
| Cần kỹ thuật lớn | O(n²) so sánh cặp là bất khả thi → LSH; bộ đếm đầy đủ tốn RAM → sketch; ghép cặp bùng nổ → PCY/FP-Growth |

## 4. Mở rộng (tùy chọn, nếu giảng viên yêu cầu thêm nguồn hoặc muốn kiểm tra khái quát hóa)

* Các tháng 2019-Dec / 2020-Jan..Apr của cùng dataset (≈ 233M sự kiện nếu dùng cả 4 tháng đầu) để benchmark mở rộng lớn hơn.
* Retailrocket recommender dataset (Kaggle, ~2.7M sự kiện) và OTTO session dataset để kiểm tra khái quát hóa của hệ gợi ý.
* Crawl: **không cần**. Nếu cần chứng minh năng lực thu thập dữ liệu, thêm một crawler nhỏ cho metadata sản phẩm công khai và ghi vào
  issue riêng (đừng để nó chặn tiến độ chính).

## 5. Cách gắn dữ liệu trên Kaggle

1. Notebook → *Add Input* → tìm `ecommerce-behavior-data-from-multi-category-store` và dataset Instacart.
2. Chạy notebook 01 → *Save Version (Save & Run All)* → output chứa `events_parquet/`.
3. Với notebook 02-08: *Add Input* → *Notebook Output Files* → chọn notebook 01. `bdm.io.load_events` tự tìm thư mục `events_parquet`,
   nếu không có sẽ đọc lại CSV.
