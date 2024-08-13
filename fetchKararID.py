import requests
from datetime import datetime, timedelta
import time
import sys
import random
from tqdm import tqdm
from connection import insert_kararlar

def convert_date_format(date_str):
    return datetime.strptime(date_str, '%d.%m.%Y').date()

def countdown(seconds, message):
    for i in range(seconds, 0, -1):
        sys.stdout.write(f"\r{message} -- {i} saniye bekleniyor...")
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * len(message + " -- 00 saniye bekleniyor...") + "\r")
    sys.stdout.flush()

url = "https://karararama.yargitay.gov.tr/aramadetaylist"
base_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

def get_random_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

def payload_data(page_size=1, page_num=1, baslangicTarihi="01.01.1997", bitisTarihi="31.12.1997"):
    data = {"data": dict(
        pageSize=page_size,
        pageNumber=page_num,
        arananKelime="",
        yargitayMah="Büyük Genel Kurulu",
        hukuk="23. Hukuk Dairesi",
        ceza="23. Ceza Dairesi",
        esasYil="",
        esasIlkSiraNo="",
        esasSonSiraNo="",
        kararYil="",
        kararIlkSiraNo="",
        kararSonSiraNo="",
        baslangicTarihi=baslangicTarihi,
        bitisTarihi=bitisTarihi,
        siralama="1",
        siralamaDirection="desc",
        birimYrgKurulDaire="Hukuk Genel Kurulu+Ceza Genel Kurulu+Ceza Daireleri Başkanlar Kurulu+Hukuk Daireleri Başkanlar Kurulu+Büyük Genel Kurulu",
        birimYrgHukukDaire="1. Hukuk Dairesi+2. Hukuk Dairesi+3. Hukuk Dairesi+4. Hukuk Dairesi+5. Hukuk Dairesi+6. Hukuk Dairesi+7. Hukuk Dairesi+8. Hukuk Dairesi+9. Hukuk Dairesi+10. Hukuk Dairesi+11. Hukuk Dairesi+12. Hukuk Dairesi+13. Hukuk Dairesi+14. Hukuk Dairesi+15. Hukuk Dairesi+16. Hukuk Dairesi+17. Hukuk Dairesi+18. Hukuk Dairesi+19. Hukuk Dairesi+20. Hukuk Dairesi+21. Hukuk Dairesi+22. Hukuk Dairesi+23. Hukuk Dairesi",
        birimYrgCezaDaire="1. Ceza Dairesi+2. Ceza Dairesi+3. Ceza Dairesi+4. Ceza Dairesi+5. Ceza Dairesi+6. Ceza Dairesi+7. Ceza Dairesi+8. Ceza Dairesi+9. Ceza Dairesi+10. Ceza Dairesi+11. Ceza Dairesi+12. Ceza Dairesi+13. Ceza Dairesi+14. Ceza Dairesi+15. Ceza Dairesi+16. Ceza Dairesi+17. Ceza Dairesi+18. Ceza Dairesi+19. Ceza Dairesi+20. Ceza Dairesi+21. Ceza Dairesi+22. Ceza Dairesi+23. Ceza Dairesi",
    )}
    return data


def fetch_data():
    start_date = datetime(1997, 1, 1)
    end_date = datetime.now()

    while start_date < end_date:
        baslangicTarihi = start_date.strftime("%d.%m.%Y")
        next_date = start_date + timedelta(days=90)
        bitisTarihi = next_date.strftime("%d.%m.%Y")

        initial_payload = payload_data(page_size=1, page_num=1, baslangicTarihi=baslangicTarihi,
                                       bitisTarihi=bitisTarihi)
        try:
            initial_response = requests.post(url, headers=base_headers, json=initial_payload, timeout=10)
        except requests.exceptions.RequestException as e:
            countdown(60, f"Bağlantı hatası, 60 saniye sonra yeniden denenecek: {e}")
            continue

        total_records = initial_response.json()['data']['recordsTotal']
        total_pages = (total_records // 100) + (1 if total_records % 100 != 0 else 0)

        progress_bar = tqdm(total=total_records, desc=f"Fetching data from {baslangicTarihi} to {bitisTarihi}",
                            unit=" records", ncols=100, colour="green")

        for page_num in range(1, total_pages + 1):
            headers = base_headers.copy()
            headers["User-Agent"] = base_headers["User-Agent"]
            headers["X-Forwarded-For"] = get_random_ip()

            payload = payload_data(page_size=100, page_num=page_num, baslangicTarihi=baslangicTarihi,
                                   bitisTarihi=bitisTarihi)
            try:
                response = requests.post(url, headers=headers, json=payload, timeout=10)
                response.raise_for_status()  # Raises stored HTTPError, if one occurred.
            except requests.exceptions.RequestException as e:
                countdown(60, f"Bağlantı hatası, 60 saniye sonra yeniden denenecek: {e}")
                continue

            response_data = response.json()
            karar_data = response_data['data']['data']
            karar_bilgileri = []
            for data in karar_data:
                karar_dict = {
                    "karar_id": data['id'],
                    "daire": data['daire'],
                    "esas_no": data['esasNo'],
                    "karar_no": data['kararNo'],
                    "karar_tarihi": data['kararTarihi']
                }
                karar_bilgileri.append(karar_dict)
            insert_kararlar(karar_bilgileri)
            progress_bar.update(len(karar_bilgileri))

        progress_bar.close()
        start_date += timedelta(days=90)  # Başlangıç tarihini son çeyreğe taşı.


if __name__ == "__main__":
    fetch_data()