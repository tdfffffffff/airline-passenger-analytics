-- 1. How many categories are in [customer_suppport]? 
SELECT COUNT(DISTINCT category) AS unique_category
FROM customer_support; -- 36 rows returned
-- It seems like only the capitalised entries in the `category` column clearly represent defined categories of customer support queries, while the remaining 28 rows contain random texts and results that do not fit as customer support queries. 
-- Hence, since a substantial amount of irrelevant data (28 rows) can undermine the integrity of our dataset, we will be cleaning the data by removing these rows for a more accurate analysis.
SET SQL_SAFE_UPDATES = 0;
DELETE FROM customer_support
WHERE category NOT IN ('SHIPPING', 'REFUND', 'PAYMENT', 'ORDER', 'INVOICE', 'FEEDBACK', 'CONTACT', 'CANCEL');
SELECT category AS Category, COUNT(*) AS NumberOfEntries
FROM customer_support
GROUP BY category
ORDER BY category DESC;

SET SQL_SAFE_UPDATES = 1; -- Therefore, after cleaning the data, there are 8 categories in customer_support, namely 'SHIPPING', 'REFUND', 'PAYMENT', 'ORDER', 'INVOICE', 'FEEDBACK', 'CONTACT', 'CANCEL'.

-- 2. [customer_suppport] For each category, display the number of records that contained colloquial variation and offensive language.
SELECT 
category,
COUNT(CASE 
WHEN flags LIKE '%Q%' AND flags LIKE '%W%' THEN 1 
END) AS count
FROM customer_support
GROUP BY category
ORDER BY category;

-- 3. [flight_delay] For each airline, display the instances of cancellations and delays. 
SELECT airline, 
COUNT(*) AS numOfFlights, 'Cancelled' AS Status
FROM flight_delay
WHERE Cancelled = 1
GROUP BY airline

UNION

SELECT airline, 
COUNT(*) AS numOfFlights, 'Delayed' AS Status
FROM flight_delay
WHERE ArrDelay > 0 
GROUP BY airline
ORDER BY airline, Status; -- Notably, there are no flight cancellations

-- 4. [flight_delay] For each month, which route has the most instances of delays?
SELECT MonthName, Route, DelayInstances
FROM (
    SELECT 
        MONTHNAME(STR_TO_DATE(Date, '%d-%m-%Y')) AS MonthName,
        CONCAT(Origin, ' - ', Dest) AS Route,
        COUNT(*) AS DelayInstances,
        RANK() OVER(PARTITION BY MONTH(STR_TO_DATE(Date, '%d-%m-%Y')) ORDER BY COUNT(*) DESC) AS RouteRank
    FROM flight_delay
    WHERE ArrDelay > 0
    AND STR_TO_DATE(Date, '%d-%m-%Y') BETWEEN '2019-01-01' AND '2019-12-06'
    GROUP BY MONTH(STR_TO_DATE(Date, '%d-%m-%Y')), MonthName, Route
) AS MonthlyRoutes
WHERE RouteRank = 1
ORDER BY FIELD(MonthName, 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December');

-- 5. [sia_stock] For the year 2023, display the quarter-on-quarter changes in high and low prices and the quarterly average price.
SELECT 
    MIN(YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y'))) AS start_year,
    MAX(YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y'))) AS end_year,
    MIN(MONTH(STR_TO_DATE(StockDate, '%m/%d/%Y'))) AS start_month,
    MAX(MONTH(STR_TO_DATE(StockDate, '%m/%d/%Y'))) AS end_month
FROM sia_stock;

SELECT 
    MIN(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS earliest_date,
    MAX(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS latest_date
FROM sia_stock;

SELECT *
FROM sia_stock
WHERE YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) = 2023;

SELECT 
    YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS year,
    MONTH(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS month,
    ROUND(AVG(High), 2) AS avg_high_price,
    ROUND(AVG(Low), 2) AS avg_low_price,
    ROUND(AVG((High + Low) / 2), 2) AS avg_price
FROM sia_stock
WHERE YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) = 2023
GROUP BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), MONTH(STR_TO_DATE(StockDate, '%m/%d/%Y'))
ORDER BY month;

SELECT 
    YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS year,
    QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS quarter,
    ROUND(AVG(High), 2) AS avg_high_price,
    ROUND(AVG(Low), 2) AS avg_low_price,
    ROUND(AVG((High + Low) / 2), 2) AS avg_price
FROM sia_stock
WHERE YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) = 2023
GROUP BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y'))
ORDER BY quarter;

SELECT 
    YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS year,
    QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS quarter,
    ROUND(AVG(High), 2) AS avg_high_price,
    ROUND(AVG(Low), 2) AS avg_low_price,
    ROUND(AVG((High + Low) / 2), 2) AS avg_price
FROM sia_stock
WHERE YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) = 2022
GROUP BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y'))
ORDER BY quarter;

WITH QuarterlyData AS (
    SELECT 
        YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS year,
        QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS quarter,
        ROUND(AVG(High), 2) AS avg_high_price,
        ROUND(AVG(Low), 2) AS avg_low_price,
        ROUND(AVG((High + Low) / 2), 2) AS avg_price
    FROM sia_stock
    WHERE YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) IN (2022, 2023)
    GROUP BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y'))
),
QoQComparison AS (
    SELECT
		a.year AS year,
        a.quarter AS quarter,
        a.avg_high_price AS high_2023,
        b.avg_high_price AS high_2022,
        ROUND((a.avg_high_price - b.avg_high_price) / b.avg_high_price * 100, 2) AS qoq_high_change_percentage,
        a.avg_low_price AS low_2023,
        b.avg_low_price AS low_2022,
        ROUND((a.avg_low_price - b.avg_low_price) / b.avg_low_price * 100, 2) AS qoq_low_change_percentage,
        a.avg_price AS avg_2023,
        b.avg_price AS avg_2022,
        ROUND((a.avg_price - b.avg_price) / b.avg_price * 100, 2) AS qoq_avg_change_percentage
        
    FROM QuarterlyData a
    JOIN QuarterlyData b
    ON a.quarter = b.quarter AND a.year = 2023 AND b.year = 2022
)
SELECT 
	year,
    quarter,
    qoq_high_change_percentage,
    qoq_low_change_percentage,
    qoq_avg_change_percentage
FROM QoQComparison
ORDER BY quarter;

-- 6.[customer_booking] For each sales_channel and each route, display the following ratios
-- average length_of_stay / average flight_hour 
-- average wants_extra_baggage / average flight_hour
-- average wants_preferred_seat / average flight_hour
-- average wants_in_flight_meals / average flight_hour
-- Our underlying objective: Are there any correlations between flight hours, length of stay, and various preferences (ie. extra baggage, preferred seats, in-flight meals)?

SELECT 
    sales_channel,
    route,
    flight_duration,
    COUNT(*) AS sum_flights,
    ROUND(AVG(length_of_stay) / AVG(flight_duration), 2) AS AverageLengthOfStayPerFlightDuration,
    ROUND(AVG(wants_extra_baggage) / AVG(flight_duration), 2) AS AverageBaggageRequestPerFlightDuration,
    ROUND(AVG(wants_preferred_seat) / AVG(flight_duration), 2) AS AveragePreferredSeatRequestPerFlightDuration,
    ROUND(AVG(wants_in_flight_meals) / AVG(flight_duration), 2) AS AverageInFlightMealRequestPerFlightDuration
FROM customer_booking
GROUP BY route, sales_channel, flight_duration
ORDER BY sum_flights DESC, route;

-- 7. [airlines_reviews] Airline seasonality. For each Airline and Class, display the averages of SeatComfort, FoodnBeverages, InflightEntertainment, ValueForMoney, and OverallRating for the seasonal and non-seasonal periods, respectively.
-- Since seasonality refers to fluctuations in flight demand due to external factors, we will assume that the seasonality of flights is determined by the variable `MonthFlown`.
SELECT 
    Airline,
    Class,
    CASE 
        WHEN SUBSTRING(MonthFlown, 1, 3) IN ('Jun', 'Jul', 'Aug', 'Sep') THEN 'Seasonal'
        ELSE 'Non-Seasonal'
    END AS Period,
    ROUND(AVG(SeatComfort), 2) AS AvgSeatComfort,
    ROUND(AVG(FoodnBeverages), 2) AS AvgFoodnBeverages,
    ROUND(AVG(InflightEntertainment), 2) AS AvgInflightEntertainment,
    ROUND(AVG(ValueForMoney), 2) AS AvgValueForMoney,
    ROUND(AVG(OverallRating), 2) AS AvgOverallRating
FROM airlines_reviews
GROUP BY 
    Airline, 
    Class, 
    Period
ORDER BY 
    Airline, 
    FIELD(Class, 'First Class', 'Business Class', 'Premium Economy', 'Economy Class'),
    Period;

-- 8. [airlines_reviews] What are the common complaints? For each Airline and TypeofTraveller, list the top 5 common issues.
-- (a) Count the frequency of words to shortlist categories of complaints
WITH Words AS (
  SELECT LOWER(word) AS word
  FROM (
    SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(Reviews, ' ', numbers.n), ' ', -1) AS word
    FROM airlines_reviews
    JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) numbers
	ON CHAR_LENGTH(Reviews) - CHAR_LENGTH(REPLACE(Reviews, ' ', '')) >= numbers.n - 1
    WHERE Recommended = 'no'
    UNION ALL
    SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(Reviews, ' ', numbers.n), ' ', -1) AS word
    FROM airlines_reviews
    JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) numbers
    ON CHAR_LENGTH(Reviews) - CHAR_LENGTH(REPLACE(Reviews, ' ', '')) >= numbers.n - 1
    WHERE Recommended = 'no'
  ) AS all_words
),
WordCounts AS (
  SELECT word, COUNT(*) AS count
  FROM Words
  GROUP BY word
)
SELECT word, count
FROM WordCounts
ORDER BY count DESC;

-- (b) Next, we identified some words indicative of complaint categories, such as "service", "experience", "food", "delayed", "seat(s)" etc. Hence, we grouped them into different complaint categories.
SELECT 
    c.Airline,
    c.TypeofTraveller,
    c.Complaint,
    COUNT(*) AS Frequency,  -- Counting the number of occurrences of each complaint category
    
    -- Calculate avgComplaintRating for negative reviews (Recommended = 'no')
    AVG(CASE WHEN c.Recommended = 'no' THEN c.OverallRating END) AS avgComplaintRating,
    
    -- Calculate avgOverallRating for all reviews (Recommended = 'yes' or 'no')
    a.avgOverallRating

FROM (
    -- Seat Issues (linked to SeatComfort)
    SELECT 
        Airline,
        TypeofTraveller,
        'Seat Issues' AS Complaint,
        OverallRating,
        Recommended
    FROM airlines_reviews
    WHERE (Reviews LIKE '%seat%' OR Reviews LIKE '%leg%' OR Reviews LIKE '%uncomfortable%' OR Reviews LIKE '%space%')
    
    UNION ALL

    -- Delays (linked to ValueforMoney)
    SELECT 
        Airline,
        TypeofTraveller,
        'Delays' AS Complaint,
        OverallRating,
        Recommended
    FROM airlines_reviews
    WHERE (Reviews LIKE '%delayed%' OR Reviews LIKE '%cancelled%' OR Reviews LIKE '%on time%')
    
    UNION ALL

    -- Service Issues (linked to StaffService)
    SELECT 
        Airline,
        TypeofTraveller,
        'Service Issues' AS Complaint,
        OverallRating,
        Recommended
    FROM airlines_reviews
    WHERE (Reviews LIKE '%service%' OR Reviews LIKE '%experience%' OR Reviews LIKE '%staff%' OR Reviews LIKE '%communication%' OR Reviews LIKE '%rude%' OR Reviews LIKE '%airline%')
    
    UNION ALL

    -- Food Issues (linked to FoodnBeverages)
    SELECT 
        Airline,
        TypeofTraveller,
        'Food Issues' AS Complaint,
        OverallRating,
        Recommended
    FROM airlines_reviews
    WHERE (Reviews LIKE '%food%' OR Reviews LIKE '%meal%' OR Reviews LIKE '%catering%')
    
    UNION ALL

    -- Inflight Entertainment Issues (linked to InflightEntertainment)
    SELECT 
        Airline,
        TypeofTraveller,
        'Inflight Entertainment' AS Complaint,
        OverallRating,
        Recommended
    FROM airlines_reviews
    WHERE (Reviews LIKE '%entertainment%' OR Reviews LIKE '%movie%' OR Reviews LIKE '%screen%' OR Reviews LIKE '%wifi%')
) AS c

-- Subquery to calculate avgOverallRating for each Airline and TypeofTraveller combination
JOIN (
    SELECT 
        Airline,
        TypeofTraveller,
        AVG(OverallRating) AS avgOverallRating
    FROM airlines_reviews
    GROUP BY Airline, TypeofTraveller
) AS a
ON c.Airline = a.Airline AND c.TypeofTraveller = a.TypeofTraveller

GROUP BY c.Airline, c.TypeofTraveller, c.Complaint, a.avgOverallRating
ORDER BY c.Airline, c.TypeofTraveller, c.Complaint;

-- 9.[airlines_reviews] and additional data*. Are there any systematic differences in customer preferences/complaints pre- and post- COVID specific to Singapore Airlines? 
-- In addition to customer satisfaction, what do you think contributed to the strong performance of Singapore Airlines in recent periods?

-- Pre-COVID: Period before the outbreak of COVID-19 (before 23 Jan 2020)
-- Post-COVID: Period after the initial impact of the pandemic began to subside (after 13 Feb 2023 where border restrictions are lifted and normal travel resumes).

-- Pre-COVID vs Post-COVID ratings
WITH PrePostCovid AS (
    SELECT 
        CASE
            WHEN STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') < STR_TO_DATE('2020-01-23', '%Y-%m-%d') THEN 'Pre-COVID'
            WHEN STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') > STR_TO_DATE('2023-02-13', '%Y-%m-%d') THEN 'Post-COVID'
            ELSE 'During-COVID'
        END AS COVID_Period,
        AVG(SeatComfort) AS AvgSeatComfort,
        AVG(FoodnBeverages) AS AvgFoodnBeverages,
        AVG(InflightEntertainment) AS AvgInflightEntertainment,
        AVG(ValueForMoney) AS AvgValueForMoney,
        AVG(OverallRating) AS AvgOverallRating
    FROM airlines_reviews
    WHERE 
        (STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') < STR_TO_DATE('2020-01-23', '%Y-%m-%d')
        OR STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') > STR_TO_DATE('2023-02-13', '%Y-%m-%d'))
        AND Airline = 'Singapore Airlines'
    GROUP BY COVID_Period
)
SELECT 
    COVID_Period,
    ROUND(AvgSeatComfort, 2) AS AvgSeatComfort,
    ROUND(AvgFoodnBeverages, 2) AS AvgFoodnBeverages,
    ROUND(AvgInflightEntertainment, 2) AS AvgInflightEntertainment,
    ROUND(AvgValueForMoney, 2) AS AvgValueForMoney,
    ROUND(AvgOverallRating, 2) AS AvgOverallRating
FROM PrePostCovid
ORDER BY COVID_Period;

-- Pre-COVID vs Post-COVID ranking of complaints based on frequency 
WITH ComplaintCounts AS (
    SELECT 
        CASE 
            WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-01-23' THEN 'Pre-COVID'
            WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') >= '2023-02-13' THEN 'Post-COVID'
        END AS COVID_Period,
        
        COUNT(CASE WHEN Reviews LIKE '%service%' OR Reviews LIKE '%experience%' OR Reviews LIKE '%staff%' OR Reviews LIKE '%communication%' OR Reviews LIKE '%rude%' OR Reviews LIKE '%airline%' THEN 1 END) AS ServiceQualityIssues,
        COUNT(CASE WHEN Reviews LIKE '%delayed%' OR Reviews LIKE '%cancelled%' OR Reviews LIKE '%ticket%' OR Reviews LIKE '%check-in%' OR Reviews LIKE '%boarding%' THEN 1 END) AS TimelinessAndOperationalIssues,
        COUNT(CASE WHEN Reviews LIKE '%seat%' OR Reviews LIKE '%uncomfortable%' OR Reviews LIKE '%space%' THEN 1 END) AS SeatingAndComfortIssues,
        COUNT(CASE WHEN Reviews LIKE '%suitcase%' OR Reviews LIKE '%baggage%' OR Reviews LIKE '%lost%' THEN 1 END) AS BaggageHandlingIssues,
        COUNT(CASE WHEN Reviews LIKE '%food%' OR Reviews LIKE '%meal%' THEN 1 END) AS FoodAndInflightAmenitiesIssues,
        COUNT(CASE WHEN Reviews LIKE '%refund%' OR Reviews LIKE '%compensation%' THEN 1 END) AS RefundsAndCompensationIssues,
        COUNT(CASE WHEN Reviews LIKE '%ticket%' OR Reviews LIKE '%booking%' OR Reviews LIKE '%check%' OR Reviews LIKE '%check-in%' THEN 1 END) AS BookingAndTicketingIssues,
        COUNT(CASE WHEN Reviews LIKE '%airways%' OR Reviews LIKE '%flight%' OR Reviews LIKE '%boarding%' OR Reviews LIKE '%communication%' THEN 1 END) AS FlightExperienceIssues
    FROM airlines_reviews
    WHERE STR_TO_DATE(ReviewDate, '%d/%m/%Y') IS NOT NULL
    AND (
        STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-01-23'  -- Pre-COVID condition
        OR STR_TO_DATE(ReviewDate, '%d/%m/%Y') >= '2023-02-13'  -- Post-COVID condition
    )
    GROUP BY COVID_Period
),
RankedComplaints AS (
    SELECT
        COVID_Period,
        'ServiceQualityIssues' AS ComplaintCategory,
        ServiceQualityIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'TimelinessAndOperationalIssues' AS ComplaintCategory,
        TimelinessAndOperationalIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'SeatingAndComfortIssues' AS ComplaintCategory,
        SeatingAndComfortIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'BaggageHandlingIssues' AS ComplaintCategory,
        BaggageHandlingIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'FoodAndInflightAmenitiesIssues' AS ComplaintCategory,
        FoodAndInflightAmenitiesIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'RefundsAndCompensationIssues' AS ComplaintCategory,
        RefundsAndCompensationIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'BookingAndTicketingIssues' AS ComplaintCategory,
        BookingAndTicketingIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'FlightExperienceIssues' AS ComplaintCategory,
        FlightExperienceIssues AS ComplaintCount
    FROM ComplaintCounts
),
TopComplaints AS (
    SELECT 
        COVID_Period, 
        ComplaintCategory, 
        ComplaintCount,
        ROW_NUMBER() OVER (PARTITION BY COVID_Period ORDER BY ComplaintCount DESC) AS ComplaintRank
    FROM RankedComplaints
)
SELECT 
    COVID_Period,
    ComplaintCategory,
    ComplaintCount,
    ComplaintRank
FROM TopComplaints
WHERE ComplaintRank <= 5  -- Top 5 complaints per COVID period
ORDER BY COVID_Period, ComplaintRank;

-- Pre-COVID vs Post-COVID ranking of complaints based on type of traveller and frequency 
WITH ComplaintCategories AS (
    SELECT
        CASE
            WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-01-23' THEN 'Pre-COVID'
            WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') >= '2023-02-13' THEN 'Post-COVID'
        END AS COVID_Period,
        Airline,
        TypeofTraveller,
        CASE
            WHEN Reviews LIKE '%food%' THEN 'Food Issues'
            WHEN Reviews LIKE '%seat%' OR Reviews LIKE '%uncomfortable%' THEN 'Seat Issues'
            WHEN Reviews LIKE '%service%' OR Reviews LIKE '%experience%' OR Reviews LIKE '%staff%' THEN 'Service Issues'
            WHEN Reviews LIKE '%delayed%' OR Reviews LIKE '%cancelled%' THEN 'Delays'
            WHEN Reviews LIKE '%entertainment%' THEN 'Entertainment Issues'
            ELSE 'Other Issues'
        END AS ComplaintCategory,
        OverallRating,
        StaffService AS ComplaintRating
    FROM airlines_reviews
    WHERE Airline = 'Singapore Airlines'
      AND (
        STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-01-23'
        OR STR_TO_DATE(ReviewDate, '%d/%m/%Y') >= '2023-02-13'
      )
),
ComplaintAggregates AS (
    SELECT
        COVID_Period,
        Airline,
        TypeofTraveller,
        ComplaintCategory,
        COUNT(*) AS Frequency,
        AVG(ComplaintRating) AS avgComplaintRating,
        AVG(OverallRating) AS avgOverallRating
    FROM ComplaintCategories
    WHERE ComplaintCategory != 'Other Issues' -- Exclude "Other Issues"
    GROUP BY COVID_Period, Airline, TypeofTraveller, ComplaintCategory
)
SELECT
    COVID_Period,
    Airline,
    TypeofTraveller,
    ComplaintCategory,
    Frequency,
    ROUND(avgComplaintRating, 4) AS avgComplaintRating,
    ROUND(avgOverallRating, 4) AS avgOverallRating
FROM ComplaintAggregates
ORDER BY COVID_Period, Airline, TypeofTraveller, ComplaintCategory;