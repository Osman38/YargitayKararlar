-- karar_id değerleri eşit olanları listeler ve sayısını verir.
SELECT karar_id, COUNT(*) AS count
FROM kararlar
GROUP BY karar_id
HAVING COUNT(*) > 1
ORDER BY count DESC;

-- karar_id değerleri benzeyenleri silmek için
WITH ranked_kararlar AS (SELECT id,
                                karar_id,
                                ROW_NUMBER() OVER (PARTITION BY karar_id ORDER BY id) AS rn
                         FROM kararlar)
DELETE
FROM kararlar
WHERE id IN (SELECT id
             FROM ranked_kararlar
             WHERE rn > 1);



-- Yıllara göre karar_id değerlerini listeleme
SELECT karar_id
FROM kararlar
WHERE EXTRACT(YEAR FROM karar_tarihi) = 2023


