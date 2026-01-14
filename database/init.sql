CREATE TABLE IF NOT EXISTS price_aggregates (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_price DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_symbol_window ON price_aggregates (symbol, window_start);
