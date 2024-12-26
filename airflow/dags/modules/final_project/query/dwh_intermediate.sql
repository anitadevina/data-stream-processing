CREATE TABLE IF NOT EXISTS int_employee_managements (
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

CREATE TABLE IF NOT EXISTS int_training_developments (
    employee_id BIGSERIAL,
    training_program VARCHAR(255),
    start_date DATE,
    end_date DATE,
    status VARCHAR(50),
    FOREIGN KEY (employee_id) REFERENCES int_employee_managements(employee_id) ON DELETE CASCADE,
    CONSTRAINT pk_training_developments PRIMARY KEY (employee_id, training_program)
);

CREATE TABLE IF NOT EXISTS int_performance_managements (
    employee_id BIGSERIAL,
    review_period VARCHAR(50),
    rating DECIMAL(5, 2),
    comments TEXT,
    FOREIGN KEY (employee_id) REFERENCES int_employee_managements(employee_id) ON DELETE CASCADE,
    CONSTRAINT pk_performance_managements PRIMARY KEY (employee_id, review_period)
);

CREATE TABLE IF NOT EXISTS int_recruitment_selections (
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

CREATE TABLE IF NOT EXISTS int_employee_candidate_maps (
    employee_id BIGSERIAL,
    candidate_id BIGSERIAL,
    PRIMARY KEY (employee_id, candidate_id),
    FOREIGN KEY (employee_id) REFERENCES int_employee_managements(employee_id),
    FOREIGN KEY (candidate_id) REFERENCES int_recruitment_selections(candidate_id)
);
