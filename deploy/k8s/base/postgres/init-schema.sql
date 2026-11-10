-- Schema matching design doc §2.3
CREATE TABLE IF NOT EXISTS items (
    key     VARCHAR(64) PRIMARY KEY,
    value   TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1
);

-- High-performance set-based seeding of 1,000,000 rows
-- Each key is 64 bytes ("key-" + 60 digits), each value is exactly 512 bytes.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM items LIMIT 1) THEN
        RAISE NOTICE 'Seeding 1,000,000 rows into items table...';
        INSERT INTO items (key, value, version)
        SELECT 
            'key-' || lpad(i::text, 60, '0'),
            'val-v1-' || repeat('x', 505),
            1
        FROM generate_series(0, 999999) AS s(i);
        RAISE NOTICE 'Seeding complete.';
    END IF;
END $$;
