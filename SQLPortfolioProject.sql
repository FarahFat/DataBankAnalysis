SELECT * FROM customer_nodes;
SELECT * FROM customer_transactions;
SELECT * FROM regions;

#A. Customer Nodes Exploration
#1. How many unique nodes are there on the Data Bank system?
SELECT COUNT(DISTINCT(node_id)) AS Uniqueodes FROM customer_nodes;

#2. What is the number of nodes per region?
SELECT cn.region_id, r.region_name, COUNT(DISTINCT(cn.node_id)) AS Unique_nodes 
FROM customer_nodes cn
INNER JOIN regions r ON cn.region_id = r.region_id
GROUP BY r.region_id, r.region_name
ORDER BY r.region_id;

#No of nodes present by region
SELECT cn.region_id, r.region_name, COUNT(cn.node_id) AS Unique_nodes 
FROM customer_nodes cn
INNER JOIN regions r ON cn.region_id = r.region_id
GROUP BY r.region_id, r.region_name
ORDER BY r.region_id;

#3. How many customers are allocated to each region?
SELECT cn.region_id, r.region_name, COUNT(DISTINCT(cn.customer_id)) AS UniqueCustomers
FROM customer_nodes cn
INNER JOIN regions r ON cn.region_id = r.region_id
GROUP BY r.region_id, r.region_name
ORDER BY r.region_id;

#4. How many days on average are customers reallocated to a different node?
#this I believe captures avg of the 1 first allocation & all reallocations.
SELECT AVG(DATEDIFF(end_date,start_date)) AS AvgAllocationDays
FROM customer_nodes
WHERE end_date != '9999-12-31';

#Given the question where we have to check "Re"allocations as in how many times the customer was reassigned then we can skip the 1st time & just check for reassignments 
WITH RankedAllocations AS (
    SELECT
        customer_id,
        start_date,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date) as RowNumber
    FROM
        data_bank.customer_nodes
        WHERE end_date != '9999-12-31'
)
SELECT AVG(DATEDIFF(end_date, start_date)) AS AverageReallocationDays
FROM
    RankedAllocations
    WHERE RowNumber >1;
    
#5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
#Solution-1
#couldn't complete



    
#B. Customer Transactions
#1. What is the unique count and total amount for each transaction type?
SELECT
    txn_type,
    COUNT(txn_date) AS UniqueTransactionCount, -- Assuming txn_date is a unique identifier
    SUM(txn_amount) AS TotalAmount
FROM
    customer_transactions
GROUP BY
    txn_type;

#2. What is the average total historical deposit counts and amounts for all customers?
SELECT ROUND(AVG(deposit_count),0) AS average_deposit_count, ROUND(AVG(total_amount),0) AS average_deposit_amount
FROM (
    SELECT customer_id, COUNT(*) AS deposit_count, SUM(txn_amount) AS total_amount
    FROM customer_transactions
    WHERE txn_type = 'deposit'
    GROUP BY customer_id
) AS customer_deposits;

#3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
WITH updated_transactions AS (
    SELECT 
        customer_id,
        COUNT(CASE WHEN txn_type = 'deposit' THEN 1 END) AS deposit_count,
        COUNT(CASE WHEN txn_type = 'purchase' THEN 1 END) AS purchase_count,
        COUNT(CASE WHEN txn_type = 'withdrawal' THEN 1 END) AS withdrawal_count,
        MONTH(txn_date) AS month_number, 
        MONTHNAME(txn_date) AS month_name 
    FROM 
        customer_transactions
    GROUP BY 
        customer_id, MONTH(txn_date), MONTHNAME(txn_date) 
)
SELECT month_name AS month, COUNT(customer_id) AS customers_count
FROM updated_transactions
WHERE deposit_count > 1 AND (purchase_count > 0 OR withdrawal_count > 0)
GROUP BY month_number, month_name
ORDER BY month_number; 

#4. What is the closing balance for each customer at the end of the month?
WITH impact AS (
    SELECT 
        customer_id,
        DATE_FORMAT(txn_date, '%Y-%m') AS txn_month,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount
            END) AS impact_of_transactions
    FROM customer_transactions
    GROUP BY customer_id, DATE_FORMAT(txn_date, '%Y-%m')
),
MonthlyTotal AS (
    SELECT 
        customer_id,
        txn_month,
        SUM(impact_of_transactions) AS total_balance
    FROM impact
    GROUP BY customer_id, txn_month
)
SELECT 
    customer_id,
    txn_month,
    SUM(total_balance) OVER (PARTITION BY customer_id ORDER BY txn_month ASC) AS closing_balance
FROM MonthlyTotal;


#5. What is the percentage of customers who increase their closing balance by more than 5%?
WITH monthlybalance AS (
    SELECT
        customer_id,
        DATE_FORMAT(txn_date, '%Y-%m') AS month,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE -txn_amount
        END) AS balance
    FROM customer_transactions
    GROUP BY customer_id, month
),
closing_balance AS (
    SELECT
        customer_id,
        month,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY month) AS closing_balance
    FROM monthlybalance
),
percentgrowth AS (
    SELECT
        customer_id,
        month,
        closing_balance,
        LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY month) AS previous_balance,
        (closing_balance - COALESCE(LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY month), closing_balance)) / 
        COALESCE(LAG(closing_balance) OVER (PARTITION BY customer_id ORDER BY month), 1) * 100 AS percent_growth
    FROM closing_balance
)
SELECT
    ROUND((COUNT(DISTINCT customer_id) / (SELECT COUNT(DISTINCT customer_id) FROM monthlybalance)) * 100, 1) AS percentage_customers_more5pct
FROM percentgrowth
WHERE percent_growth > 5;



C. Data Allocation Challenge
1. running a customer balance column that includes the impact of each transaction
SELECT customer_id,
       txn_date,
       txn_type,
       txn_amount,
       SUM(CASE WHEN txn_type = 'Deposit' THEN txn_amount
		WHEN txn_type = 'Dithdrawal' THEN -txn_amount
		WHEN txn_type = 'Purchase' THEN -txn_amount
		ELSE 0
	   END) OVER(PARTITION BY customer_id ORDER BY txn_date) AS RunningBalance
FROM customer_transactions;

2. Customer balance at the end of each month
SELECT customer_id,
       MONTH(txn_date) AS month,
       FORMAT(txn_date, 'MMMM') AS month_name,
       SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
                WHEN txn_type = 'withdrawal' THEN -txn_amount
                WHEN txn_type = 'purchase' THEN -txn_amount
                ELSE 0 END) AS closing_balance
FROM customer_transactions
GROUP BY customer_id, 
         MONTH(txn_date), 
         FORMAT(txn_date, 'MMMM');


3. minimum, average, and maximum values of the running balance for each customer    
WITH running_balance AS
(
	SELECT customer_id,
	       txn_date,
	       txn_type,
	       txn_amount,
	       SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
			WHEN txn_type = 'withdrawal' THEN -txn_amount
			WHEN txn_type = 'purchase' THEN -txn_amount
			ELSE 0
		   END) OVER(PARTITION BY customer_id ORDER BY txn_date) AS running_balance
	FROM customer_transactions
)
SELECT customer_id,
       AVG(running_balance) AS AVGRunningBalance,
       MIN(running_balance) AS MINRunningalance,
       MAX(running_balance) AS max_running_balance
FROM running_balance
GROUP BY customer_id;