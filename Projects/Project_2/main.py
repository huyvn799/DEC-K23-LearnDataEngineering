import asyncio
import aiohttp
import pandas as pd
import time
import os
import re
from bs4 import BeautifulSoup

# --- CẤU HÌNH ---
INPUT_FILE = 'products-0-200000.csv'
DATA_DIR = 'data'
ERROR_FILE = 'errors/all_errors.csv'
CHUNK_SIZE = 1000
CONCURRENCY = 20
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.dirname(ERROR_FILE), exist_ok=True)

sem = asyncio.Semaphore(CONCURRENCY)

# --- HÀM HỖ TRỢ ---
def clean_description(html_content):
    if not html_content: return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text()
    text = re.sub(r'[\r\n]+', '|', text).strip()
    text = re.sub(r'[\s]+', ' ', text).strip()
    text = re.sub(r'( \|)+', '', text).strip()
    text = re.sub(r'(\.)+$', '.', text).strip()
    return text

def extract_all_urls(images_data):
    all_urls = []
    if not images_data: return ""
    for img in images_data:
        urls = [val for key, val in img.items() if "url" in key.lower() and val]
        all_urls.extend(urls)
    return "|".join(list(set(all_urls)))

async def fetch_product(session, p_id):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{p_id}"
    async with sem:
        try:
            async with session.get(url, headers=HEADERS, timeout=20) as response:
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
            batch_ids = product_ids[i : i + step]
            tasks = [fetch_product(session, pid) for pid in batch_ids]
            results = await asyncio.gather(*tasks)
            
            batch_success = [r[0] for r in results if r[0]]
            batch_error = [r[1] for r in results if r[1]]
            
            success_buffer.extend(batch_success)
            all_current_errors.extend(batch_error)
            all_success_ids.extend([s['id'] for s in batch_success])
            total_success += len(batch_success)

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

    while ids_to_crawl:
        print(f"\n{'='*25} ROUND {retry_round} {'='*25}")
        print(f"Target: {len(ids_to_crawl)} IDs")
        
        # 1. Chạy Crawler
        current_errors, success_ids, next_file_index = await run_crawler(ids_to_crawl, next_file_index)
        total_final_success += len(success_ids)
        
        # 2. Cập nhật file lỗi (Xóa ID thành công, thêm ID lỗi mới)
        update_error_file(current_errors, success_ids)
        
        # 3. Xác định ID cho vòng tiếp theo (chỉ lấy 429 hoặc Timeout)
        if os.path.exists(ERROR_FILE):
            df_err = pd.read_csv(ERROR_FILE)
            # Lọc các lỗi có thể cứu vãn được
            retry_condition = df_err['error_code'].isin(['429', 'TimeoutError', 'ClientConnectorError', 'ServerDisconnectedError'])
            ids_to_crawl = df_err[retry_condition]['id'].tolist()
            
            if ids_to_crawl:
                print(f"⚠️ Cần retry {len(ids_to_crawl)} lỗi mạng/rate-limit. Nghỉ 5s...")
                await asyncio.sleep(5)
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