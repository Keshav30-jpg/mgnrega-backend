-- schema.sql
CREATE TABLE IF NOT EXISTS districts (
  id SERIAL PRIMARY KEY,
  state_name TEXT NOT NULL,
  district_name TEXT NOT NULL,
  district_code TEXT,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mgnrega_monthly (
  id SERIAL PRIMARY KEY,
  district_id INT REFERENCES districts(id),
  year INT NOT NULL,
  month INT NOT NULL,
  persons_benefitted BIGINT DEFAULT 0,
  person_days BIGINT DEFAULT 0,
  wages_paid BIGINT DEFAULT 0,
  households_worked BIGINT DEFAULT 0,
  source_date TIMESTAMPTZ DEFAULT now(),
  raw_json JSONB,
  UNIQUE(district_id, year, month)
);

CREATE INDEX IF NOT EXISTS idx_mgnrega_district_month ON mgnrega_monthly(district_id, year, month);
