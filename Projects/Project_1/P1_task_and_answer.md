# Linux CMD Project

Data: https://raw.githubusercontent.com/yinghaoz1/tmdb-movie-dataset-analysis/master/tmdb-movies.csv

Dữ liệu trên đang được đặt trên Linux server, cần team Data Engineer sử dụng command line Linux hỗ trợ các tác vụ sau để có các thông tin cơ bản về dữ liệu

cd Project_1
1. Sắp xếp các bộ phim theo ngày phát hành giảm dần rồi lưu ra một file mới
1.1. Dùng bash shell
- lấy header -> header.txt
$ head -n 1 tmdb-movies.csv | tee input/header.txt
- lấy data trong file csv không có header
$ tail -n +2 tmdb-movies.csv > input/movies_no_header.csv
- tìm vị trí của header cần sorted 
$ cat < input/header.txt | tr ',' '\n' | cat -n | grep "release"
Output: release_date: 16
        release_year: 19
- thực hiện sort giảm dần -> lưu vào file movies_by_release_date_desc.csv
Syntax: sort -t',' -k<column_number> <filename.csv>
$ sort -t',' -k19,19nr

1.2. Dùng thư viện csvkit của python
Tải thư viện csvsort:
sudo apt install python3
sudo apt install python3-pip
sudo apt install csvkit

csvsort -c release_date -r tmdb-movies.csv > output/1_movies_sorted.csv

Pros: Tự động sort theo date và convert sang format yyyy-mm-dd

2. Lọc ra các bộ phim có đánh giá trung bình trên 7.5 rồi lưu ra một file mới
2.1. Dùng bash shell
awk -F',' '$18 > 7.5' input/movies_no_header.csv | cat < header.txt >> 2_test.csv 

2.2. Dùng csvkit:
csvsql --query "SELECT * FROM 'tmdb-movies' WHERE vote_average > 7.5" tmdb-movies.csv > output/2_movies_above_average.csv


3. Tìm ra phim nào có doanh thu cao nhất và doanh thu thấp nhất
csvsql
3.1. Doanh thu cao nhất
csvsort -c 'revenue' -r tmdb-movies.csv | head -n 2 > 3.highest_revenue.csv
3.2. Doanh thu thấp nhất
csvsql --query "SELECT original_title,revenue FROM 'tmdb-movies' ORDER BY revenue ASC LIMIT 1" tmdb-movies.csv > output/3.lowest_revenue.csv


4. Tính tổng doanh thu tất cả các bộ phim
csvsql --query "SELECT SUM(revenue) AS sum_revenue FROM 'tmdb-movies'" tmdb-movies.csv > output/4.sum_revenue.csv

5. Top 10 bộ phim đem về lợi nhuận cao nhất
csvsql --query "SELECT original_title, (revenue - budget) AS profit FROM 'tmdb-movies' ORDER BY profit DESC LIMIT 10" tmdb-movies.csv > output/5.top10_profit.csv

6. Đạo diễn nào có nhiều bộ phim nhất và diễn viên nào đóng nhiều phim nhất

7. Thống kê số lượng phim theo các thể loại. Ví dụ có bao nhiêu phim thuộc thể loại Action, bao nhiêu thuộc thể loại Family, ….

8. Idea của bạn để có thêm những phân tích cho dữ liệu?