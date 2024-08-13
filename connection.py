from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError,DBAPIError
from dotenv import load_dotenv
import os

#  .env dosyasını yükle
load_dotenv()
# Veritabanı bağlantı dizesi
DATABASE_URL = os.getenv('DATABASE_URL')

# Veritabanı bağlantısı oluştur
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


def get_connection():
    # Veritabanı bağlantısı sağlayan fonksiyon
    try:
        connection = engine.connect()
        return connection
    except SQLAlchemyError as e:
        print(f"Veritabanı bağlantı hatası: {e}")
        return None


def execute_query(query, parameters=None):
    # Verilen SQL sorgusunu yürüten ve sonuçları döndüren fonksiyon
    try:
        with get_connection() as conn:
            if parameters:
                result = conn.execute(text(query), **parameters)
            else:
                result = conn.execute(text(query))
            return result.fetchall()
    except SQLAlchemyError as e:
        print(f"Sorgu yürütme hatası: {e}")
        return None


def insert_kararlar(karar_bilgileri):
    query = """
    INSERT INTO kararlar(daire, esas_no, karar_no, karar_tarihi, karar_id)
    VALUES (:daire, :esas_no, :karar_no, :karar_tarihi, :karar_id)
    """

    try:
        with get_connection() as conn:
            for karar_info in karar_bilgileri:
                conn.execute(text(query), karar_info)
            conn.connection.commit()  # Commit işlemi burada yapılıyor.
            # print(f"{len(karar_bilgileri)} karar başarıyla eklendi.")
    except SQLAlchemyError as e:
        print(f"Kararlar eklenirken bir hata oluştu: {e}")
        if conn:
            conn.connection.rollback()  # Hata durumunda rollback yapılıyor.

def fetch_karar_id_by_year(year):
    connection = None
    try:
        connection = get_connection()
        if connection is not None:
            query = """
               SELECT karar_id 
               FROM kararlar 
               WHERE EXTRACT(YEAR FROM karar_tarihi) = :year
               """
            results = connection.execute(text(query), {'year': year})
            return [karar_id[0] for karar_id in results.fetchall()]  # Tüm karar_id'leri bir liste olarak döndür
    except SQLAlchemyError as e:
        print(f"Sorgu çalıştırılırken hata oluştu: {e}")
    finally:
        if connection:
            connection.close()

def karar_detay_batch_insert(karar_details, batch_size=1000):
    try:
        connection = get_connection()
        if connection:
            for i in range(0, len(karar_details), batch_size):
                batch = karar_details[i:i+batch_size]
                karar_ids = [detail['karar_id'] for detail in batch]
                karar_detays = [detail['karar_detay'] for detail in batch]
                query = """
                    UPDATE kararlar SET karar_detay = data.karar_detay
                    FROM (SELECT unnest(array[:karar_id]) AS karar_id, unnest(array[:karar_detay]) AS karar_detay) AS data
                    WHERE kararlar.karar_id = data.karar_id
                    """
                connection.execute(text(query), {'karar_id': karar_ids, 'karar_detay': karar_detays})
                connection.commit()
    except DBAPIError as e:
        if connection:
            connection.rollback()
        print(f"Database operation failed: {e}")
    finally:
        if connection:
            connection.close()



# Fonksiyonu kullanırken bir liste sözlük geçirmelisiniz
karar_details = [
    {'karar_id': 123, 'karar_detay': 'Detay 1'},
    {'karar_id': 456, 'karar_detay': 'Detay 2'},
    # daha fazla karar_id ve karar_detay...
]
karar_detay_batch_insert(karar_details)

