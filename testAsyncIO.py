import aiohttp
import asyncio
import random
import logging
from bs4 import BeautifulSoup
from tqdm import tqdm
from sqlalchemy import text
from connection import fetch_karar_id_by_year, karar_detay_batch_insert, get_connection

# test
logging.basicConfig(filename='errors.log', level=logging.ERROR, format='%(asctime)s:%(levelname)s:%(message)s')

async def fetch_karar_detay(session, karar_id, semaphore, pbar):
    base_url = f"https://karararama.yargitay.gov.tr/getDokuman?id={karar_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "X-Forwarded-For": f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    }
    async with semaphore:
        try:
            async with session.get(base_url, headers=headers) as response:
                if response.status == 200:
                    json_data = await response.json()
                    if 'data' in json_data:
                        soup = BeautifulSoup(json_data['data'], 'lxml')
                        body = soup.find('body')
                        pbar.update(1)
                        return {"karar_id": karar_id, "karar_detay": body.prettify()}
                return {"karar_id": karar_id, "karar_detay": "Detay yok"}
        except Exception as e:
            logging.error(f"Error fetching karar detail for ID {karar_id}: {e}")
            return {"karar_id": karar_id, "karar_detay": "FETCH_FAILED"}

async def manage_requests(karar_ids, session, pbar):
    semaphore = asyncio.Semaphore(100)  # Asenkron istek limitini 100 olarak ayarlayın
    results = await asyncio.gather(*(fetch_karar_detay(session, karar_id, semaphore, pbar) for karar_id in karar_ids))
    return results

async def retry_failed_requests(year, session):
    failed_query = """
        SELECT karar_id FROM kararlar 
        WHERE EXTRACT(YEAR FROM karar_tarihi) = :year AND karar_detay = 'FETCH_FAILED'
    """
    connection = get_connection()
    failed_ids = [row[0] for row in connection.execute(text(failed_query), {'year': year}).fetchall()]
    connection.close()

    if failed_ids:
        pbar = tqdm(total=len(failed_ids), desc=f"Retry Year: {year}", unit="karar")
        results = await manage_requests(failed_ids, session, pbar)
        karar_detay_batch_insert(results)
        pbar.close()

async def main():
    years = list(range(2006, 2025))
    async with aiohttp.ClientSession() as session:
        for year in years:
            await retry_failed_requests(year, session)  # Sadece başarısız olanları tekrar dene

if __name__ == '__main__':
    asyncio.run(main())
