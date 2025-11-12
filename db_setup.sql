CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.policy (
    policy_no           VARCHAR(50)    NOT NULL PRIMARY KEY,
    cust_id             VARCHAR(50)    NOT NULL,        
    policytype          VARCHAR(50),
    pol_issue_date      DATE,
    pol_eff_date        DATE,
    pol_expiry_date     DATE,
    make                VARCHAR(50), 
    model               VARCHAR(50), 
    model_year          INT, 
    chassis_no          VARCHAR(50), 
    use_of_vehicle      VARCHAR(100),
    product             VARCHAR(100), 
    sum_insured         NUMERIC(15, 2), -- Use NUMERIC for currency
    premium             NUMERIC(15, 2), -- Use NUMERIC for currency
    deductable          INT 
);

CREATE TABLE IF NOT EXISTS demo.claim (
    claim_no                        VARCHAR(50)    NOT NULL PRIMARY KEY,
    policy_no                       VARCHAR(50)    NOT NULL,   
    claim_date                      DATE,           -- Use DATE type
    months_as_customer              INT,
    injury                          BIGINT,
    property                        BIGINT,
    vehicle                         BIGINT,
    total                           BIGINT,
    collision_type                  VARCHAR(50),
    number_of_vehicles_involved     INT, 
    driver_age                      INT,            -- Age is typically an integer
    insured_relationship            VARCHAR(50),
    license_issue_date              DATE,           -- Use DATE type
    incident_date                   DATE,           -- Use DATE type
    incident_hour                   INT,
    incident_type                   VARCHAR(50),
    incident_severity               VARCHAR(50),
    number_of_witnesses             INT, 
    suspicious_activity             BOOLEAN         -- Use BOOLEAN for true/false
);

CREATE TABLE IF NOT EXISTS demo.customer (
    customer_id INT NOT NULL PRIMARY KEY,
    date_of_birth DATE NULL,                        -- Use DATE type
    borough VARCHAR(100) NULL,
    neighborhood VARCHAR(150) NULL,
    zip_code VARCHAR(10) NULL,
    name VARCHAR(255) NULL
);

CREATE PUBLICATION dbz_publication FOR TABLE demo.policy, demo.claim, demo.customer;

SELECT * FROM demo.policy LIMIT 10;
SELECT * FROM demo.claim LIMIT 10;
SELECT * FROM demo.customer LIMIT 10;

SELECT * FROM demo.customer WHERE customer_id = 14583;
SELECT * FROM demo.policy WHERE policy_no='102117868';

-- Query to find columns with at least one NULL value in demo.policy
SELECT 'demo.policy' AS table_name, 'policytype' AS column_name FROM demo.policy WHERE policytype IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'pol_issue_date' AS column_name FROM demo.policy WHERE pol_issue_date IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'pol_eff_date' AS column_name FROM demo.policy WHERE pol_eff_date IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'pol_expiry_date' AS column_name FROM demo.policy WHERE pol_expiry_date IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'make' AS column_name FROM demo.policy WHERE make IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'model' AS column_name FROM demo.policy WHERE model IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'model_year' AS column_name FROM demo.policy WHERE model_year IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'chassis_no' AS column_name FROM demo.policy WHERE chassis_no IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'use_of_vehicle' AS column_name FROM demo.policy WHERE use_of_vehicle IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'product' AS column_name FROM demo.policy WHERE product IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'sum_insured' AS column_name FROM demo.policy WHERE sum_insured IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'premium' AS column_name FROM demo.policy WHERE premium IS NULL LIMIT 1
UNION ALL
SELECT 'demo.policy' AS table_name, 'deductable' AS column_name FROM demo.policy WHERE deductable IS NULL LIMIT 1

UNION ALL

-- Query to find columns with at least one NULL value in demo.claim
SELECT 'demo.claim' AS table_name, 'claim_date' AS column_name FROM demo.claim WHERE claim_date IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'months_as_customer' AS column_name FROM demo.claim WHERE months_as_customer IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'injury' AS column_name FROM demo.claim WHERE injury IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'property' AS column_name FROM demo.claim WHERE property IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'vehicle' AS column_name FROM demo.claim WHERE vehicle IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'total' AS column_name FROM demo.claim WHERE total IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'collision_type' AS column_name FROM demo.claim WHERE collision_type IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'number_of_vehicles_involved' AS column_name FROM demo.claim WHERE number_of_vehicles_involved IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'driver_age' AS column_name FROM demo.claim WHERE driver_age IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'insured_relationship' AS column_name FROM demo.claim WHERE insured_relationship IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'license_issue_date' AS column_name FROM demo.claim WHERE license_issue_date IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'incident_date' AS column_name FROM demo.claim WHERE incident_date IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'incident_hour' AS column_name FROM demo.claim WHERE incident_hour IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'incident_type' AS column_name FROM demo.claim WHERE incident_type IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'incident_severity' AS column_name FROM demo.claim WHERE incident_severity IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'number_of_witnesses' AS column_name FROM demo.claim WHERE number_of_witnesses IS NULL LIMIT 1
UNION ALL
SELECT 'demo.claim' AS table_name, 'suspicious_activity' AS column_name FROM demo.claim WHERE suspicious_activity IS NULL LIMIT 1

UNION ALL

-- Query to find columns with at least one NULL value in demo.customer
SELECT 'demo.customer' AS table_name, 'date_of_birth' AS column_name FROM demo.customer WHERE date_of_birth IS NULL LIMIT 1
UNION ALL
SELECT 'demo.customer' AS table_name, 'borough' AS column_name FROM demo.customer WHERE borough IS NULL LIMIT 1
UNION ALL
SELECT 'demo.customer' AS table_name, 'neighborhood' AS column_name FROM demo.customer WHERE neighborhood IS NULL LIMIT 1
UNION ALL
SELECT 'demo.customer' AS table_name, 'zip_code' AS column_name FROM demo.customer WHERE zip_code IS NULL LIMIT 1
UNION ALL
SELECT 'demo.customer' AS table_name, 'name' AS column_name FROM demo.customer WHERE name IS NULL LIMIT 1;
