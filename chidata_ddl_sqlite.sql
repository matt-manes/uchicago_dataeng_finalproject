-- Normalized tables for business_licenses and food_inspections
-- business_licenses.csv tables
------------------------------------------------|
DROP TABLE IF EXISTS business_addresses;

CREATE TABLE
    business_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        street TEXT,
        zip INTEGER,
        ward INTEGER,
        latitude REAL,
        longitude REAL
    );

------------------------------------------------|
DROP TABLE IF EXISTS businesses;

CREATE TABLE
    businesses (
        account_number INTEGER PRIMARY KEY,
        legal_name TEXT,
        dba TEXT,
        address_id INTEGER
    );

------------------------------------------------|
DROP TABLE IF EXISTS license_codes;

CREATE TABLE
    license_codes (code INTEGER PRIMARY KEY, description TEXT);

------------------------------------------------|
DROP TABLE IF EXISTS application_types;

CREATE TABLE
    application_types (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT);

------------------------------------------------|
DROP TABLE IF EXISTS application_payments;

CREATE TABLE
    application_payments (id INTEGER PRIMARY KEY AUTOINCREMENT, date DATE);

------------------------------------------------|
DROP TABLE IF EXISTS license_statuses;

CREATE TABLE
    license_statuses (
        id INTEGER PRIMARY KEY,
        status TEXT,
        description TEXT
    );

------------------------------------------------|
DROP TABLE IF EXISTS licenses;

CREATE TABLE
    licenses (
        license_number INTEGER PRIMARY KEY,
        account_number INTEGER,
        start_date DATE,
        expiration_date DATE,
        issue_date DATE,
        status_id INTEGER,
        status_change_date DATE
    );

------------------------------------------------|
DROP TABLE IF EXISTS license_applications;

CREATE TABLE
    license_applications (
        id INTEGER PRIMARY KEY,
        license_number INTEGER,
        license_code INTEGER,
        account_number INTEGER,
        application_type_id INTEGER,
        payment_id INTEGER,
        created_date DATE,
        completed_date DATE,
        approval_date DATE,
        conditional_approval TEXT,
        site_number INTEGER
    );

------------------------------------------------|
-- food_inspections.csv
------------------------------------------------|
DROP TABLE IF EXISTS facility_types;

CREATE TABLE
    facility_types (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);

------------------------------------------------|
DROP TABLE IF EXISTS risk_levels;

CREATE TABLE
    risk_levels (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);

------------------------------------------------|
DROP TABLE IF EXISTS facility_addresses;

CREATE TABLE
    facility_addresses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        street TEXT,
        zip INTEGER,
        latitude REAL,
        longitude REAL,
        facility_type_id INTEGER,
        risk_id INTEGER
    );

------------------------------------------------|
DROP TABLE IF EXISTS inspected_businesses;

CREATE TABLE
    inspected_businesses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        license_number INTEGER,
        dba TEXT,
        aka TEXT
    );

------------------------------------------------|
DROP TABLE IF EXISTS inspection_types;

CREATE TABLE
    inspection_types (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);

------------------------------------------------|
DROP TABLE IF EXISTS result_types;

CREATE TABLE
    result_types (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT
    );

------------------------------------------------|
DROP TABLE IF EXISTS inspections;

CREATE TABLE
    inspections (
        id INTEGER PRIMARY KEY,
        license_number INTEGER,
        facility_address_id INTEGER,
        inspection_type_id INTEGER,
        result_type_id INTEGER,
        date DATE
    );

------------------------------------------------|
DROP TABLE IF EXISTS violation_types;

CREATE TABLE
    violation_types (id INTEGER PRIMARY KEY, name TEXT);

------------------------------------------------|
DROP TABLE IF EXISTS violations;

CREATE TABLE
    violations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        inspection_id INTEGER,
        violation_type_id INTEGER,
        comment TEXT
    );