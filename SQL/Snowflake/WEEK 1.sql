// EXERCISE 1

-- 1. Select all columns from the CUSTOMER table.
SELECT *
FROM customer
LIMIT 3;

-- 2. Select only the C_NAME and C_NATIONKEY columns.
SELECT C_NAME, 
       C_NATIONKEY
FROM customer
LIMIT 3;

-- 3. Select only the distinct values of C_NATIONKEY.
SELECT DISTINCT C_NATIONKEY
FROM customer
LIMIT 25;

-- 4. Retrieve all customers who are from nation 15 or nation 23
SELECT *
FROM customer
WHERE C_NATIONKEY = 15 OR C_NATIONKEY = 23
LIMIT 10;

-- 5. Retrieve all customers with an account balance greater than 5000.
SELECT C_NAME,
       C_ACCTBAL
FROM customer   
WHERE C_ACCTBAL > 5000
LIMIT 10;

-- 6. Retrieve all customers with an account balance between 2000 and 7000.
SELECT C_NAME, 
       C_ACCTBAL
FROM customer
WHERE C_ACCTBAL BETWEEN 2000 AND 7000
LIMIT 10;

-- 7. Retrieve all customers not from nation 3 or 5
SELECT C_NAME,
       C_NATIONKEY
FROM customer
WHERE C_NATIONKEY NOT IN (3,5)
LIMIT 10;

// EXERCISE 2

-- 1.List the first 10 customers by account balance, highest to lowest.
SELECT C_NAME,
       C_ACCTBAL
FROM customer
ORDER BY C_ACCTBAL DESC
LIMIT 10;

-- 2. Sort customers alphabetically by name, showing only C_NAME, C_ACCTBAL, and C_NATIONKEY.
SELECT C_NAME,
       C_ACCTBAL,
       C_NATIONKEY
FROM customer
ORDER BY C_NAME
LIMIT 10;

// EXCERCISE 3

-- 1. List customers where: Nation key is 1 and account balance is greater than 1000.
SELECT C_NAME, 
       C_NATIONKEY,
       C_ACCTBAL
FROM customer
WHERE C_NATIONKEY = 1 AND C_ACCTBAL > 1000
ORDER BY C_ACCTBAL DESC
LIMIT 10;

-- 2. List customers where: Nation key is 2 or account balance is less than 500.
SELECT C_NAME, 
       C_NATIONKEY,
       C_ACCTBAL
FROM customer
WHERE C_NATIONKEY = 2 AND C_ACCTBAL < 500
ORDER BY C_ACCTBAL DESC
LIMIT 10;

-- 3. List customers where: Account balance is greater than 7000 and not from nation 8.
SELECT C_NAME, 
       C_NATIONKEY,
       C_ACCTBAL
FROM customer
WHERE C_NATIONKEY != 8 AND C_ACCTBAL > 7000
ORDER BY C_ACCTBAL DESC
LIMIT 10;