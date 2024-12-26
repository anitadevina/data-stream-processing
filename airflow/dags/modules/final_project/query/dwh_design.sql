CREATE TABLE IF NOT EXISTS fact_employees (
    employee_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255),
    gender VARCHAR(10),
    age INT,
    department VARCHAR(255),
    position VARCHAR(255),
    salary INT,
    overtime_pay INT,
    payment_date DATE
);

CREATE TABLE IF NOT EXISTS dim_trainings (
    employee_id BIGSERIAL,
    training_program VARCHAR(255),
    start_date DATE,
    end_date DATE,
    status VARCHAR(50),
    CONSTRAINT dim_trainings_employee_id_fkey FOREIGN KEY (employee_id) REFERENCES fact_employees(employee_id) ON DELETE CASCADE,
    CONSTRAINT pk_dim_trainings PRIMARY KEY (employee_id, training_program)
);

CREATE TABLE IF NOT EXISTS dim_performances (
    employee_id BIGSERIAL,
    review_period VARCHAR(50),
    rating DECIMAL(5, 2),
    comments TEXT,
    CONSTRAINT dim_performances_employee_id_fkey FOREIGN KEY (employee_id) REFERENCES fact_employees(employee_id) ON DELETE CASCADE,
    CONSTRAINT pk_dim_performances PRIMARY KEY (employee_id, review_period)
);

CREATE TABLE IF NOT EXISTS dim_recruitments (
    candidate_id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255),
    gender VARCHAR(10),
    age INT,
    position VARCHAR(255),
    application_date DATE,
    status VARCHAR(50),
    interview_date DATE,
    offer_status VARCHAR(50),
    predict VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS employee_candidate_maps (
    employee_id BIGSERIAL,
    candidate_id BIGSERIAL,
    PRIMARY KEY (employee_id, candidate_id),
    FOREIGN KEY (employee_id) REFERENCES fact_employees(employee_id),
    FOREIGN KEY (candidate_id) REFERENCES dim_recruitments(candidate_id)
);