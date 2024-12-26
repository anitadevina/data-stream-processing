CREATE TABLE IF NOT EXISTS dim_times (
    id SERIAL PRIMARY KEY,
    quarter VARCHAR(5) NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    date DATE NOT NULL UNIQUE
);

WITH date_series AS (
    SELECT generate_series(
        '2023-01-01'::DATE,
        '2024-12-31'::DATE,
        '1 day'::INTERVAL
    ) AS date
)
INSERT INTO dim_times (quarter, day, month, year, date)
SELECT
    CASE
        WHEN EXTRACT(MONTH FROM date) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 7 AND 9 THEN 'Q3'
        WHEN EXTRACT(MONTH FROM date) BETWEEN 10 AND 12 THEN 'Q4'
    END AS quarter,
    EXTRACT(DAY FROM date)::INT AS day,
    EXTRACT(MONTH FROM date)::INT AS month,
    EXTRACT(YEAR FROM date)::INT AS year,
    date::DATE AS date
FROM date_series
ON CONFLICT (date) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_quarter_periods(
    id SERIAL PRIMARY KEY,
    quarter_period VARCHAR(10) NOT NULL UNIQUE
);

WITH quarters AS (
    SELECT DISTINCT 
        CONCAT(dt.quarter, ' ', dt."year") AS quarter_data,
        dt."year",
        dt.quarter
    FROM dim_times dt
    ORDER BY dt."year", dt.quarter
)
INSERT INTO dim_quarter_periods (quarter_period)
SELECT
    quarter_data AS quarter_period
FROM quarters
ON CONFLICT (quarter_period) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_training_programs(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_employees (
    employee_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    age INT NOT NULL,
    department VARCHAR(255),
    position VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_candidates (
    candidate_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    age INT NOT NULL,
    position VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fact_employee_payrolls(
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGSERIAL NOT NULL,
    salary INT,
    overtime_pay INT,
    payment_date_id INT,
    FOREIGN KEY (employee_id) REFERENCES dim_employees(employee_id) ON DELETE CASCADE,
    FOREIGN KEY (payment_date_id) REFERENCES dim_times(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS fact_employee_performances(
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGSERIAL NOT NULL,
    review_period_id INT,
    rating DECIMAL(5, 2),
    comments TEXT,
    FOREIGN KEY (employee_id) REFERENCES dim_employees(employee_id) ON DELETE CASCADE,
    FOREIGN KEY (review_period_id) REFERENCES dim_quarter_periods(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS fact_employee_trainings(
    id BIGSERIAL PRIMARY KEY,
    employee_id BIGSERIAL NOT NULL,
    training_program_id INT,
    start_date_id INT,
    end_date_id INT,
    status VARCHAR(50),
    FOREIGN KEY (employee_id) REFERENCES dim_employees(employee_id) ON DELETE CASCADE,
    FOREIGN KEY (training_program_id) REFERENCES dim_training_programs(id) ON DELETE CASCADE,
    FOREIGN KEY (start_date_id) REFERENCES dim_times(id) ON DELETE CASCADE,
    FOREIGN KEY (end_date_id) REFERENCES dim_times(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS fact_candidate_recruitments(
    id BIGSERIAL PRIMARY KEY,
    candidate_id BIGSERIAL NOT NULL,
    application_date_id INT,
    status VARCHAR(50),
    interview_date_id INT,
    offer_status VARCHAR(50),
    FOREIGN KEY (candidate_id) REFERENCES dim_candidates(candidate_id) ON DELETE CASCADE,
    FOREIGN KEY (application_date_id) REFERENCES dim_times(id) ON DELETE CASCADE,
    FOREIGN KEY (interview_date_id) REFERENCES dim_times(id) ON DELETE CASCADE
);