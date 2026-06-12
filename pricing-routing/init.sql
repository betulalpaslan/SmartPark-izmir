CREATE TABLE IF NOT EXISTS lot_prices (
    lot_id           TEXT PRIMARY KEY,
    lot_name         TEXT,
    price_per_hour   NUMERIC(6, 2) NOT NULL DEFAULT 10.00,
    rules            JSONB,
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);

-- İZELMAN tarifesine yakın örnek fiyatlar (TRY/saat).
-- Gerçek lot_id'ler API'den akışa geçince UPDATE ile güncellenebilir.
INSERT INTO lot_prices (lot_id, lot_name, price_per_hour) VALUES
    ('1',  'Plevne Bulvarı',      12.00),
    ('2',  'Konak Meydanı',       15.00),
    ('3',  'Alsancak Garı',       12.00),
    ('4',  'Kıbrıs Şehitleri Cd', 10.00),
    ('5',  'Basmane',              8.00),
    ('6',  'Kemeraltı',           10.00),
    ('7',  'Bornova Merkez',       8.00),
    ('8',  'Karşıyaka Çarşı',      8.00),
    ('9',  'Mavişehir',           10.00),
    ('10', 'Çiğli Merkez',         7.00)
ON CONFLICT (lot_id) DO NOTHING;
