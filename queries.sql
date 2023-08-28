-- For the sqlite database
-- Number of food inspections to number of businesses by ward
SELECT
    COUNT(inspections.id) AS inspection_count,
    COUNT(DISTINCT inspected_businesses.license_number) as num_businesses,
    COUNT(inspections.id) / COUNT(DISTINCT inspected_businesses.license_number) as inspections_to_business_ratio,
    business_addresses.ward
FROM
    inspections
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN inspected_businesses ON licenses.license_number = inspected_businesses.license_number
GROUP BY
    business_addresses.ward
ORDER BY
    inspections_to_business_ratio DESC;

-- Wards ranked by failed inspections to number of businesses ratio
SELECT
    COUNT(inspections.id) AS failed_inspections,
    COUNT(DISTINCT inspected_businesses.license_number) as num_businesses,
    COUNT(inspections.id) / COUNT(DISTINCT inspected_businesses.license_number) as failed_inspections_to_business_ratio,
    business_addresses.ward
FROM
    inspections
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN inspected_businesses ON licenses.license_number = inspected_businesses.license_number
WHERE
    inspections.result_type_id = 2
GROUP BY
    business_addresses.ward
ORDER BY
    failed_inspections_to_business_ratio DESC;

-- Wards ranked by passed/passed w/ conditions inspections to number of businesses ratio
SELECT
    COUNT(inspections.id) AS passed_inspections,
    COUNT(DISTINCT inspected_businesses.license_number) as num_businesses,
    COUNT(inspections.id) / COUNT(DISTINCT inspected_businesses.license_number) as passed_inspections_to_business_ratio,
    business_addresses.ward
FROM
    inspections
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
    INNER JOIN inspected_businesses ON licenses.license_number = inspected_businesses.license_number
WHERE
    inspections.result_type_id in (6, 7)
GROUP BY
    business_addresses.ward
ORDER BY
    passed_inspections_to_business_ratio DESC;

-- Violation type occurrences
SELECT
    violation_types.id,
    violation_types.name as violation,
    COUNT(*) as occurrences
FROM
    violations
    INNER JOIN violation_types ON violations.violation_type_id = violation_types.id
GROUP BY
    violations.violation_type_id
ORDER BY
    occurrences DESC;

-- Violation type occurrences by ward
SELECT
    COUNT(violations.violation_type_id) as violation_count,
    violation_types.name as violation,
    business_addresses.ward
FROM
    violations
    INNER JOIN violation_types ON violations.violation_type_id = violation_types.id
    INNER JOIN inspections ON violations.inspection_id = inspections.id
    INNER JOIN licenses ON inspections.license_number = licenses.license_number
    INNER JOIN businesses ON licenses.account_number = businesses.account_number
    INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
GROUP BY
    business_addresses.ward,
    violations.violation_type_id
ORDER BY
    business_addresses.ward,
    violation_count DESC;

-- 2nd Most frequent violation by ward / all wards have violation type id 55 as top violation
SELECT
    *
FROM
    (
        SELECT
            COUNT(violations.violation_type_id) as occurences,
            violation_types.name as violation,
            business_addresses.ward
        FROM
            violations
            INNER JOIN violation_types ON violations.violation_type_id = violation_types.id
            INNER JOIN inspections ON violations.inspection_id = inspections.id
            INNER JOIN licenses ON inspections.license_number = licenses.license_number
            INNER JOIN businesses ON licenses.account_number = businesses.account_number
            INNER JOIN business_addresses ON businesses.address_id = business_addresses.id
        WHERE
            violations.violation_type_id <> 55
        GROUP BY
            business_addresses.ward,
            violations.violation_type_id
        ORDER BY
            business_addresses.ward,
            violation_count DESC
    ) as violation_frequency
GROUP BY
    violation_frequency.ward;