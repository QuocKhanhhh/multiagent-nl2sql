# Data Dictionary for Analytics Database

Đây là mô tả chi tiết về schema cơ sở dữ liệu. Hãy tuân thủ nghiêm ngặt các định nghĩa, kiểu dữ liệu và mối quan hệ dưới đây khi sinh ra SQL.

---

### Bảng: `dw.dim_articles`
- **Mô tả:** Bảng chứa thông tin chi tiết về từng bài viết.
- **Khóa chính:** `article_id`
- **Các cột:**
  - `article_id` (INTEGER): ID duy nhất của bài viết.
  - `title` (TEXT): Tiêu đề bài viết. Dùng `ILIKE '%...%'` để tìm kiếm không phân biệt hoa thường.
  - `source_name` (TEXT): **Tên nguồn/nhà xuất bản** (ví dụ: 'Vietcetera', 'CafeF'). **Đây là cột dùng để trả lời câu hỏi về "Nguồn".**
  - `source_url` (TEXT): Đường dẫn gốc của bài viết.
  - `content` (TEXT): Nội dung đầy đủ.
  - `embedding` (VECTOR): Vector embedding của nội dung.

---

### Bảng: `dw.dim_authors`
- **Mô tả:** Bảng chứa thông tin về tác giả.
- **Khóa chính:** `author_id`
- **Các cột:**
  - `author_id` (INTEGER): ID duy nhất của tác giả.
  - `author_name` (TEXT): Tên tác giả. **Đây là cột dùng để trả lời câu hỏi về "Tác giả".**

---

### Bảng: `dw.dim_topics`
- **Mô tả:** Bảng chứa thông tin về chủ đề.
- **Khóa chính:** `topic_id`
- **Các cột:**
  - `topic_id` (INTEGER): ID duy nhất của chủ đề.
  - `topic_name` (TEXT): Tên chủ đề đã được chuẩn hóa.

---

### Bảng: `dw.dim_date`
- **Mô tả:** Bảng chiều thời gian, phân rã ngày tháng.
- **Khóa chính:** `date_id`
- **Các cột:**
  - `date_id` (INTEGER): ID duy nhất của ngày.
  - `full_date` (DATE): Ngày đầy đủ (YYYY-MM-DD).
  - `year` (INTEGER): Năm.
  - `month` (INTEGER): Tháng (1-12).
  - `day` (INTEGER): Ngày trong tháng (1-31).

---

### Bảng: `dw.fact_articles`
- **Mô tả:** Bảng fact trung tâm, kết nối các chiều và chứa các metric.
- **Khóa chính:** `fact_id`
- **Các cột (Metrics):**
  - `word_count` (INTEGER): **Số từ** trong bài viết. Dùng để trả lời câu hỏi về "số từ", "bài viết dài/ngắn theo số từ".
  - `read_time` (INTEGER): **Thời gian đọc** ước tính (đơn vị: phút). Dùng để trả lời câu hỏi về "thời gian đọc". **Luôn so sánh cột này như một con số (ví dụ: `read_time < 5`), không dùng hàm timestamp.**
  - `sentiment` (TEXT): Cảm xúc (`pos`=tích cực, `neg`=tiêu cực, `neu`=trung lập).
  - `keywords` (TEXT): Danh sách từ khóa, cách nhau bởi dấu phẩy. Dùng `ILIKE '%...%'` để tìm kiếm.
- **Các cột (Khóa ngoại):**
  - `article_id` (INTEGER): Join với `dw.dim_articles` qua `dw.dim_articles.article_id`.
  - `author_id` (INTEGER): Join với `dw.dim_authors` qua `dw.dim_authors.author_id`.
  - `topic_id` (INTEGER): Join với `dw.dim_topics` qua `dw.dim_topics.topic_id`.
  - `date_id` (INTEGER): Join với `dw.dim_date` qua `dw.dim_date.date_id`.

---