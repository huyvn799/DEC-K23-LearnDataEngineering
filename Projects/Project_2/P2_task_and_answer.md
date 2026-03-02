# Yêu cầu
DoD: Sử dụng code Python, tải về thông tin của 200k sản phẩm (list product id bên dưới) của Tiki và lưu thành các file .json. Mỗi file có thông tin của khoảng 1000 sản phẩm. Các thông in cần lấy bao gồm: id, name, url_key, price, description, images url. Yêu cầu chuẩn hoá nội dung trong "description" và tìm phương án rút ngắn thời gian lấy dữ liệu.
- List product_id: https://1drv.ms/u/s!AukvlU4z92FZgp4xIlzQ4giHVa5Lpw?e=qDXctn
- API get product detail: https://api.tiki.vn/product-detail/api/v1/products/138083218

# Hướng dẫn cài đặt và chạy chương trình
1. Cài đặt venv
pip install venv

2. Cài đặt các thư viện cần thiết
cd Projects/Project_2
python3 -m venv venv
source venv/bin/activate
pip install pandas requests beautifulsoup4 aiohttp aiofiles lxml uvloop fake-useragent

3. Chạy chương trình
nohup python3 -u main.py >> crawl_tiki.log 2>&1 &

# Nội dung chương trình:
- Semaphore(20) để tránh lỗi many requests và làm tràn bộ nhớ
- Cứ 1000 sản phẩm thành công -> 1 file output
- Có duy nhất 1 file ghi các id sản phẩm lỗi, phục vụ việc theo dõi và chạy lại chương trình cho những sản phẩm lỗi
- Có hàm xử lý cho trường thông tin dài như description, images
- Sau khi thực hiện lần đầu, chương trình sẽ tiếp tục cho đến khi không tìm thêm được sản phẩm

# Kết quả
- ROUND chạy: 15 
- Tổng thời gian: 3.63 giờ
- Sản phẩm lỗi 404: 8212/200000