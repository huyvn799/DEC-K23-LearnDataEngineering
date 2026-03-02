import uvloop
import asyncio
import aiohttp
from fake_useragent import UserAgent
import pandas as pd
import time
import os
import re
from bs4 import BeautifulSoup
import lxml
import random

# --- CẤU HÌNH ---
INPUT_FILE = 'products-0-200000.csv'
DATA_DIR = 'data'
ERROR_FILE = 'errors/all_errors.csv'
CHUNK_SIZE = 1000
CONCURRENCY = 15


os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(ERROR_FILE), exist_ok=True)

ua = UserAgent() # tạo user-agent
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy()) # uvloop để tăng cường xử lý cho asyncio
sem = asyncio.Semaphore(CONCURRENCY)

# --- HÀM HỖ TRỢ ---
def get_random_headers():
    """
    Lấy ngẫu nhiên 1 Header khi random USER-AGENT
    """
    return {
        "User-Agent": ua.random,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://tiki.vn/",
        "Origin": "https://tiki.vn",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    }

def clean_description(html_content):
    """
    xử lý nội dung dài của description
    """
    if not html_content: return ""
    soup = BeautifulSoup(html_content, "lxml")
    text = soup.get_text()
    text = re.sub(r'[\r\n]+', '|', text).strip()
    text = re.sub(r'[\s]+', ' ', text).strip()
    text = re.sub(r'( \|)+', '', text).strip()
    text = re.sub(r'(\.)+$', '.', text).strip()
    return text

def extract_all_urls(images_data):
    """
    thu thập tất cả các image_urls của 1 sản phẩm
    """
    all_urls = []
    if not images_data: return ""
    for img in images_data:
        urls = [val for key, val in img.items() if "url" in key.lower() and val]
        all_urls.extend(urls)
    return "|".join(list(set(all_urls)))

async def fetch_product(session, p_id):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{p_id}"
    current_headers = get_random_headers() # Tạo header mới cho mỗi request
    async with sem:
        try:
            # Thêm độ trễ ngẫu nhiên từ 0.5 đến 1.5 giây cho mỗi request
            await asyncio.sleep(random.uniform(0.3, 1.0))
            
            async with session.get(url, headers=current_headers, timeout=20) as response:
                status = response.status
                if status == 200:
                    data = await response.json()
                    return {
                        "id": data.get("id"),
                        "name": data.get("name"),
                        "url_key": data.get("url_key"),
                        "price": data.get("price"),
                        "description": clean_description(data.get("description")),
                        "images_url": extract_all_urls(data.get("images"))
                    }, None
                else:
                    return None, {"id": p_id, "error_code": str(status)}
        except Exception as e:
            return None, {"id": p_id, "error_code": str(type(e).__name__)}

def update_error_file(new_errors, successful_ids):
    """
    Cập nhật file lỗi: 
    1. Đọc file lỗi cũ (nếu có).
    2. Loại bỏ những ID đã thành công trong lượt vừa rồi.
    3. Thêm những lỗi mới phát sinh (hoặc cập nhật mã lỗi mới).
    """
    existing_errors = []
    if os.path.exists(ERROR_FILE):
        existing_errors = pd.read_csv(ERROR_FILE).to_dict('records')

    # Loại bỏ ID đã thành công khỏi danh sách lỗi cũ
    updated_errors = [e for e in existing_errors if e['id'] not in successful_ids]
    
    # Thêm/Cập nhật các lỗi mới từ lượt chạy vừa rồi
    # Dùng dictionary để tránh trùng ID trong danh sách lỗi
    error_dict = {e['id']: e['error_code'] for e in updated_errors}
    for ne in new_errors:
        error_dict[ne['id']] = ne['error_code']
    
    # Chuyển lại thành list để lưu
    final_errors = [{"id": k, "error_code": v} for k, v in error_dict.items()]
    
    if final_errors:
        pd.DataFrame(final_errors).to_csv(ERROR_FILE, index=False, encoding="utf-8-sig")
    else:
        if os.path.exists(ERROR_FILE): os.remove(ERROR_FILE)

# --- LUỒNG XỬ LÝ CHÍNH ---
async def run_crawler(product_ids, start_file_index):
    success_buffer = []
    all_current_errors = []
    all_success_ids = []
    file_counter = start_file_index
    
    total_success = 0
    step = 100

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(product_ids), step):
            batch_start = time.time()
            batch_ids = product_ids[i : i + step]
            tasks = [fetch_product(session, pid) for pid in batch_ids] # 1 list các tuple từ fetch_product
            results = await asyncio.gather(*tasks) # tất cả tuple của 1 batch
            
            # 1 tuple có 2 phần tử -> index 0: success, index 1: error
            batch_success = [r[0] for r in results if r[0]] # lấy index 0
            batch_error = [r[1] for r in results if r[1]] # lấy index 1
            
            # batch_error_404 = [b_err for b_err in batch_error if b_err.get("error_code") == "404"]

            success_buffer.extend(batch_success) # đếm cho tới 1000 sản phẩm -> lưu file csv
            all_current_errors.extend(batch_error)
            all_success_ids.extend([s['id'] for s in batch_success])
            total_success += len(batch_success)

            # Thống kê nhanh lỗi trong Batch
            err_counts = pd.Series([e['error_code'] for e in batch_error]).value_counts().to_dict()
            batch_time = time.time() - batch_start
            
            print(f"📦 Batch {i//100 + 1}: OK {len(batch_success)} | Total Errors {len(batch_error)} | Errors: {err_counts} | Time: {batch_time:.2f}s", flush=True)

            while len(success_buffer) >= CHUNK_SIZE:
                to_save = success_buffer[:CHUNK_SIZE]
                success_buffer = success_buffer[CHUNK_SIZE:]
                pd.DataFrame(to_save).to_csv(f"{DATA_DIR}/products_batch_{file_counter}.csv", index=False, encoding="utf-8-sig")
                file_counter += 1
            
            print(f"-> Batch progress: {i+len(batch_ids)}/{len(product_ids)} | Success: {total_success}")

        if success_buffer:
            pd.DataFrame(success_buffer).to_csv(f"{DATA_DIR}/products_batch_{file_counter}.csv", index=False, encoding="utf-8-sig")
            file_counter += 1

    return all_current_errors, all_success_ids, file_counter

async def main():
    df_input = pd.read_csv(INPUT_FILE)
    ids_to_crawl = df_input['id'].tolist()
    
    start_all = time.time()
    retry_round = 1
    total_final_success = 0
    next_file_index = 1

    wait_time = 90

    while ids_to_crawl:
        round_start = time.time()
        print(f"\n{'='*25} ROUND {retry_round} {'='*25}")
        print(f"Target: {len(ids_to_crawl)} IDs")
        
        # 1. Chạy Crawler
        current_errors, success_ids, next_file_index = await run_crawler(ids_to_crawl, next_file_index)
        total_final_success += len(success_ids)
        
        # 2. Cập nhật file lỗi (Xóa ID thành công, thêm ID lỗi mới)
        update_error_file(current_errors, success_ids)
        
        round_time = time.time() - round_start
        err_counts = pd.Series([e['error_code'] for e in current_errors]).value_counts().to_dict()
        print(f"\n✨ ROUND {retry_round} FINISHED in {round_time/60:.2f} minutes", flush=True)
        print(f"📊 Round Stats: Success {len(success_ids)} | Errors {err_counts}", flush=True)

        # 3. Xác định ID cho vòng tiếp theo (chỉ lấy 429 hoặc Timeout)
        if os.path.exists(ERROR_FILE):
            df_err = pd.read_csv(ERROR_FILE)
            
            # Các lỗi có thể chạy lại
            retry_codes = ['429', 'TimeoutError', 'ClientConnectorError', 'ServerDisconnectedError']
            if retry_round < 6: retry_codes.append('404')
            
            ids_to_crawl = df_err[df_err['error_code'].isin(retry_codes)]['id'].tolist()

            if ids_to_crawl:
                # Trong hàm main(), đoạn tính wait_time:
                # Logic tính toán wait_time giữa các Round
                if retry_round == 1:
                    wait_time = 30   # Sau Round 1, nếu có lỗi thì nghỉ 30s
                elif retry_round == 2:
                    wait_time = 120  # Nếu vẫn còn lỗi, nghỉ hẳn 2 phút
                else:
                    wait_time = 300  # Từ Round 3 trở đi, nghỉ 5 phút để "tẩy trắng" IP trong mắt Tiki

                print(f"⚠️ Cần retry {len(ids_to_crawl)} lỗi")
                print(f"⚠️ Nghỉ {wait_time}s... Chờ ROUND tiếp theo")
                await asyncio.sleep(wait_time)
                retry_round += 1
            else:
                print("✅ Không còn lỗi có thể retry (chỉ còn 404 hoặc lỗi cứng).")
                break
        else:
            print("🎉 Không còn ID lỗi nào. Hoàn tất!")
            break

    # --- TỔNG KẾT ---
    print("\n" + "📊 FINAL STATISTICS")
    print(f"⏱️ Total Time: {(time.time() - start_all)/3600:.2f} hours")
    print(f"✅ Total Success: {total_final_success}")
    if os.path.exists(ERROR_FILE):
        df_err_final = pd.read_csv(ERROR_FILE)
        print(f"❌ Remaining Errors: {len(df_err_final)}")
        print(df_err_final['error_code'].value_counts())

if __name__ == "__main__":
    asyncio.run(main())