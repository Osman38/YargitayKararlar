import aiohttp
import asyncio
import random
import logging
from bs4 import BeautifulSoup
from tqdm import tqdm
from connection import fetch_karar_id_by_year, karar_detay_batch_insert

# Log dosyası ayarları
logging.basicConfig(filename='errors.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

base_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

def get_random_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

async def fetch_karar_detay(session, karar_id):
    base_url = f"https://karararama.yargitay.gov.tr/getDokuman?id={karar_id}"
    headers = base_headers.copy()
    headers["X-Forwarded-For"] = get_random_ip()
    try:
        async with session.get(base_url, headers=headers) as response:
            if response.status == 200:
                json_data = await response.json()
                if 'data' in json_data:
                    soup = BeautifulSoup(json_data['data'], 'lxml')
                    body = soup.find('body')
                    return karar_id, body.prettify() if body else "Detay yok"
            return karar_id, "Detay yok"
    except Exception as e:
        logging.error(f"Error fetching karar detail for ID {karar_id}: {e}")
        return karar_id, "FETCH_FAILED"

async def process_year_group(years, session):
    total_karar = sum(len(fetch_karar_id_by_year(year)) for year in years)
    with tqdm(total=total_karar, desc=f"Yıllar: {years[0]}-{years[-1]}", unit="karar", leave=True) as pbar:
        tasks = []
        for year in years:
            task = asyncio.create_task(insert_karar_detay_for_year(year, session, pbar))
            tasks.append(task)
        await asyncio.gather(*tasks)

async def insert_karar_detay_for_year(year, session, pbar):
    karar_ids = fetch_karar_id_by_year(year)
    total_karar = len(karar_ids)
    batch_size = 100
    batch = []
    for karar_id in karar_ids:
        batch.append(fetch_karar_detay(session, karar_id))
        if len(batch) == batch_size:
            results = await asyncio.gather(*batch)
            karar_detay_batch_insert([{"karar_id": kid, "karar_detay": detay} for kid, detay in results])
            pbar.update(len(batch))
            batch = []
    if batch:
        results = await asyncio.gather(*batch)
        karar_detay_batch_insert([{"karar_id": kid, "karar_detay": detay} for kid, detay in results])
        pbar.update(len(batch))

async def main():
    years = list(range(1997, 2025))
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(years), 3):
            await process_year_group(years[i:i+3], session)

if __name__ == '__main__':
    asyncio.run(main())
