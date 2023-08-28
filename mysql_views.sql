USE chidata;

-- Number of inspected businesses by ward -----------------------------------------------------------------------
-- Most other views have a property to num businesses ratio by ward, this should help simplify
CREATE OR REPLACE VIEW
    num_inspected_businesses_by_ward AS
SELECT
    COUNT(*) AS num_businesses,
    business_addresses.ward
FROM
    inspected_businesses
    INNER JOIN licenses ON inspected_businesses.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
GROUP BY
    business_addresses.ward;

-- Inspections by ward -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    inspections_by_ward AS
SELECT
    COUNT(*) AS inspection_count,
    num_inspected_businesses_by_ward.num_businesses,
    COUNT(*) / num_inspected_businesses_by_ward.num_businesses AS inspections_to_business_ratio,
    business_addresses.ward
FROM
    inspections
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN num_inspected_businesses_by_ward ON business_addresses.ward = num_inspected_businesses_by_ward.ward
GROUP BY
    business_addresses.ward
ORDER BY
    inspections_to_business_ratio DESC;

-- Inspection results by ward -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    inspection_results_by_ward AS
SELECT
    result_types.id,
    result_types.description AS result,
    COUNT(*) AS num_results,
    num_inspected_businesses_by_ward.num_businesses,
    COUNT(*) / num_inspected_businesses_by_ward.num_businesses AS results_to_business_ratio,
    business_addresses.ward
FROM
    inspections
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN result_types ON inspections.result_type_id = result_types.id
    INNER JOIN num_inspected_businesses_by_ward ON business_addresses.ward = num_inspected_businesses_by_ward.ward
GROUP BY
    business_addresses.ward,
    result_types.id
ORDER BY
    business_addresses.ward,
    results_to_business_ratio DESC;

-- Failed inspections by ward -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    failed_inspections_by_ward AS
SELECT
    *
FROM
    inspection_results_by_ward
WHERE
    result = 'Fail'
ORDER BY
    results_to_business_ratio DESC;

-- Passed inspections by ward -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    passed_inspections_by_ward AS
SELECT
    'Pass' AS result,
    SUM(num_results) AS total_results,
    num_businesses,
    SUM(num_results) / num_businesses AS results_to_business_ratio,
    ward
FROM
    inspection_results_by_ward
WHERE
    id in (6, 7)
GROUP BY
    ward
ORDER BY
    results_to_business_ratio DESC;

-- Violation type occurrences -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    violation_type_occurrences AS
SELECT
    violation_types.id,
    violation_types.name AS violation,
    COUNT(*) AS occurrences
FROM
    violations
    INNER JOIN violation_types ON violations.violation_type_id = violation_types.id
GROUP BY
    violations.violation_type_id
ORDER BY
    occurrences DESC;

-- Violation type occurrences by ward -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    violation_type_occurrence_by_ward AS
SELECT
    violation_types.id,
    violation_types.name AS violation,
    COUNT(*) AS occurrences,
    COUNT(*) / num_inspected_businesses_by_ward.num_businesses AS violations_to_business_ratio,
    business_addresses.ward
FROM
    violations
    INNER JOIN violation_types ON violations.violation_type_id = violation_types.id
    INNER JOIN inspections ON violations.inspection_id = inspections.id
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN num_inspected_businesses_by_ward ON business_addresses.ward = num_inspected_businesses_by_ward.ward
GROUP BY
    business_addresses.ward,
    violations.violation_type_id
ORDER BY
    business_addresses.ward,
    occurrences DESC;

-- Total violations by ward ---------------------------------------------------------------
CREATE OR REPLACE VIEW
    total_violations_by_ward AS
SELECT
    COUNT(*) as num_violations,
    business_addresses.ward,
    COUNT(*) / num_inspected_businesses_by_ward.num_businesses AS total_violations_to_business_ratio
FROM
    violations
    INNER JOIN inspections ON violations.inspection_id = inspections.id
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN num_inspected_businesses_by_ward ON business_addresses.ward = num_inspected_businesses_by_ward.ward
GROUP BY
    business_addresses.ward
ORDER BY
    total_violations_to_business_ratio DESC;

-- Inspection types by ward -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    inspection_type_occurrence_by_ward AS
SELECT
    inspection_types.id AS inspection_type_id,
    COUNT(*) AS num_inspections,
    inspection_types.name AS inspection_type,
    num_inspected_businesses_by_ward.num_businesses,
    COUNT(*) / num_inspected_businesses_by_ward.num_businesses AS inspection_type_to_business_ratio,
    business_addresses.ward
FROM
    inspections
    INNER JOIN inspection_types ON inspections.inspection_type_id = inspection_types.id
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN num_inspected_businesses_by_ward ON business_addresses.ward = num_inspected_businesses_by_ward.ward
GROUP BY
    business_addresses.ward,
    inspection_types.id
ORDER BY
    business_addresses.ward,
    num_inspections DESC;

-- Number of canvass inspections by ward -----------------------------------------------------------------------
CREATE OR REPLACE VIEW
    canvass_inspections_by_ward AS
SELECT
    *
FROM
    inspection_type_occurrence_by_ward
WHERE
    inspection_type_id = 1
ORDER BY
    inspection_type_to_business_ratio DESC;

-- Number of inspections due to complaints by ward ----------------------------------------------------
-- Complaint, Short Form Complaint, or Suspected Food Poisoning
CREATE OR REPLACE VIEW
    complaint_inspections_by_ward AS
SELECT
    'Complaint|Suspected Food Poisoning' AS reason,
    SUM(num_inspections) AS total_inspections,
    num_businesses,
    Sum(num_inspections) / num_businesses AS inspections_to_business_ratio,
    ward
FROM
    inspection_type_occurrence_by_ward
WHERE
    inspection_type_id in (3, 13, 15)
GROUP BY
    ward
ORDER BY
    inspection_type_to_business_ratio DESC;