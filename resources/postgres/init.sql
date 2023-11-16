CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE card_events (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    cc_num BIGINT NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    event_datetime TIMESTAMP NOT NULL,
    event_unix_time BIGINT NOT NULL,
    category VARCHAR(255) NOT NULL,
    merchant VARCHAR(255) NOT NULL,
    value DECIMAL(10, 2) NOT NULL,
    location GEOMETRY(Point, 4326),  -- Armazena lat/lon como um ponto em um sistema de coordenadas geográficas (SRID 4326)
    lon VARCHAR(255) NOT NULL,
    lat VARCHAR(255) NOT NULL
);

CREATE INDEX idx_credit_events_trans_date ON card_events (event_date);