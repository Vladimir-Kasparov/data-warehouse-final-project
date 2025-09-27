--1 Create Dim_playlist with all fields from playlisttrack and playlist name
CREATE TABLE dwh.Dim_playlist AS
SELECT
    pt.playlistid,
    pt.trackid,
    p.name as playlist_name,
    pt.last_update
FROM stg.playlisttrack pt
LEFT JOIN stg.playlist p ON pt.playlistid = p.playlistid



--2 Create Dim_customer with cleaned name fields and email domain
CREATE TABLE dwh.Dim_customer AS
SELECT customerid,
       INITCAP(firstname) as firstname,           -- Capitalize first letter
       INITCAP(lastname) as lastname,             -- Capitalize first letter
       company,
       address,
       city,
       state,
       country,
       postalcode,
       phone,
       fax,
       email,
       supportrepid,
       SPLIT_PART(email, '@', 2) AS email_domain,         -- Extract domain from email
       last_update
FROM stg.customer



--3 Create Dim_employee table with department info, email domain, years of employment, and manager flag
CREATE TABLE dwh.Dim_employee AS
SELECT
    e.*,  -- All original fields from employee
    b.department_name,  
    b.budget,        
    EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM e.hiredate) AS years_employed,  -- Calculate years employed
    SPLIT_PART(e.email, '@', 2) AS email_domain,  -- Extract email domain (after '@')
    
    -- Determine if employee is a manager by checking if their ID appears in 'reportsto' of other employees
    CASE 
        WHEN e.employeeid IN (
            SELECT DISTINCT reportsto
            FROM stg.employee
            WHERE reportsto IS NOT NULL
        ) THEN 1
        ELSE 0
    END AS is_manager
FROM stg.employee as e
LEFT JOIN stg.department_budget b ON e.departmentid = b.department_id



--4 Create Dim_track with details from album, artist, genre, and mediatype
CREATE TABLE dwh.Dim_track AS
SELECT
    -- Bring all track columns explicitly
    t.trackid,
    t.name AS track_name,
    t.albumid,
    t.mediatypeid,
    t.genreid,
    t.composer,
    ROUND(t.milliseconds / 1000.0, 2) AS seconds,  --  convert to seconds
    t.bytes,
    t.unitprice,

    -- Extra fields from joined tables
    a.title AS album_title,
    ar.name AS artist_name,
    g.name AS genre_name,
    m.name AS mediatype_name,

    -- Duration formatted as MM:SS
    TO_CHAR(
        (t.milliseconds / 1000)::int * interval '1 second',
        'MI:SS'
    ) AS duration_formatted

FROM stg.track t   -- Join related tables to track data
LEFT JOIN stg.album a ON t.albumid = a.albumid
LEFT JOIN stg.artist ar ON a.artistid = ar.artistid
LEFT JOIN stg.genre g ON t.genreid = g.genreid
LEFT JOIN stg.mediatype m ON t.mediatypeid = m.mediatypeid

--5 Create Fact_invoice
CREATE TABLE dwh.Fact_invoice AS
SELECT
    invoiceid,
    customerid,
    invoicedate,
    total
FROM stg.invoice


--6 Create Fact_invoiceline
CREATE TABLE dwh.Fact_invoiceline AS
SELECT *,
       quantity * unitprice AS total_line
FROM stg.invoiceline


--7 Create Dim_curency
CREATE TABLE dwh.Dim_currency AS
SELECT *
FROM stg.usd_ils_rates


--SQL Analytics – Question 1
-- Pre-aggregate: track count per playlist
WITH playlist_counts AS (
  SELECT
    dp.playlist_name,
    COUNT(DISTINCT dp.trackid) AS track_count   -- count distinct tracks per playlist (protects from duplicates)
  FROM dwh.dim_playlist AS dp
  GROUP BY dp.playlist_name
),
-- Pick the playlist with the most tracks (tie-break by name for determinism)
max_playlist AS (
  SELECT playlist_name, track_count
  FROM playlist_counts
  ORDER BY track_count DESC, playlist_name ASC
  LIMIT 1
),
-- Pick the playlist with the fewest tracks (tie-break by name)
min_playlist AS (
  SELECT playlist_name, track_count
  FROM playlist_counts
  ORDER BY track_count ASC, playlist_name ASC
  LIMIT 1
),
-- Average number of tracks per playlist (based on the per-playlist counts above)
avg_tracks AS (
  SELECT ROUND(AVG(track_count)::numeric, 2) AS avg_tracks_per_playlist
  FROM playlist_counts
)
-- Single-row output combining all three results
SELECT
  mp.playlist_name AS max_playlist_name,
  mp.track_count   AS max_track_count,
  mn.playlist_name AS min_playlist_name,
  mn.track_count   AS min_track_count,
  a.avg_tracks_per_playlist
FROM max_playlist mp
CROSS JOIN min_playlist mn
CROSS JOIN avg_tracks a;


--SQL Analytics – Question 2

-- 1) Compute sales count per track (include tracks with zero sales via LEFT JOIN)
WITH TrackSales AS (
    SELECT 
        t.trackid, 
        COUNT(il.invoicelineid) AS sales_count  -- number of invoice lines per track
    FROM dwh.dim_track t
    LEFT JOIN dwh.fact_invoiceline il ON t.trackid = il.trackid
    GROUP BY t.trackid
),
-- 2) Bucket tracks into sales ranges
TrackSalesGroups AS (
    SELECT
        trackid,
        sales_count,
        CASE
            WHEN sales_count = 0 THEN '0'
            WHEN sales_count BETWEEN 1 AND 5 THEN '1-5'
            WHEN sales_count BETWEEN 6 AND 10 THEN '6-10'
            ELSE '10<'
        END AS sales_group
    FROM TrackSales
)
-- 3) Count how many tracks fall into each bucket (ordered logically)
SELECT
    sales_group,
    COUNT(*) AS num_tracks
FROM TrackSalesGroups
GROUP BY sales_group
ORDER BY 
    CASE sales_group
        WHEN '0' THEN 1
        WHEN '1-5' THEN 2
        WHEN '6-10' THEN 3
        ELSE 4
    END;


--SQL Analytics – Question 3a (Top 5 + Bottom 5 countries by total sales)

-- 1) Total sales per country
WITH country_sales AS (
    SELECT
        dc.country,
        SUM(fi.total) AS total_sales
    FROM dwh.fact_invoice fi
    JOIN dwh.dim_customer dc ON fi.customerid = dc.customerid
    GROUP BY dc.country
),
-- 2) Top 5 by sales
top5 AS (
    SELECT country, total_sales
    FROM country_sales
    ORDER BY total_sales DESC
    LIMIT 5
),
-- 3) Bottom 5 by sales
bottom5 AS (
    SELECT country, total_sales
    FROM country_sales
    ORDER BY total_sales ASC
    LIMIT 5
),
-- 4) Union the two sets
top_bottom_countries AS (
    SELECT * FROM top5
    UNION ALL
    SELECT * FROM bottom5
)
-- Final output: only 10 rows (top 5 + bottom 5)
SELECT country, total_sales
FROM top_bottom_countries
ORDER BY total_sales DESC;


--SQL Analytics – Question 3b (Genre share and rank within the selected countries)

-- Reuse the same top/bottom countries from (3a) and compute genre shares
WITH country_sales AS (
    SELECT
        dc.country,
        SUM(fi.total) AS total_sales
    FROM dwh.fact_invoice fi
    JOIN dwh.dim_customer dc ON fi.customerid = dc.customerid
    GROUP BY dc.country
),
top5 AS (
    SELECT country, total_sales
    FROM country_sales
    ORDER BY total_sales DESC
    LIMIT 5
),
bottom5 AS (
    SELECT country, total_sales
    FROM country_sales
    ORDER BY total_sales ASC
    LIMIT 5
),
top_bottom_countries AS (
    SELECT * FROM top5
    UNION ALL
    SELECT * FROM bottom5
),
-- Sales by genre within those countries
genre_sales AS (
    SELECT
        c.country,
        t.genre_name,
        SUM(il.unitprice * il.quantity) AS genre_sales   -- use line total; replace with il.total_line if exists
    FROM dwh.fact_invoiceline il
    JOIN dwh.fact_invoice f ON il.invoiceid = f.invoiceid
    JOIN dwh.dim_customer c ON f.customerid = c.customerid
    JOIN dwh.dim_track t ON il.trackid = t.trackid
    WHERE c.country IN (SELECT country FROM top_bottom_countries)
    GROUP BY c.country, t.genre_name
),
-- Add per-country total to compute percentages
genre_share AS (
    SELECT
        gs.country,
        gs.genre_name,
        gs.genre_sales,
        SUM(gs.genre_sales) OVER (PARTITION BY gs.country) AS country_total
    FROM genre_sales gs
)
-- Final: percentage and rank per country
SELECT
    country,
    genre_name,
    ROUND((genre_sales / NULLIF(country_total, 0)) * 100, 2) || '%' AS genre_percentage,
    RANK() OVER (PARTITION BY country ORDER BY genre_sales DESC) AS genre_rank
FROM genre_share
ORDER BY country, genre_rank;


--SQL Analytics – Question 4

-- Goal: For each country (with "singleton" countries grouped under 'Other'),
-- return: number of customers, avg orders per customer, avg revenue per customer.
WITH customers AS (
  SELECT
    c.customerid,
    c.country
  FROM dwh.dim_customer c
),
-- Per-customer stats (include customers with 0 orders via LEFT JOIN)
customer_stats AS (
  SELECT
    cu.customerid,
    cu.country,
    COALESCE(COUNT(fi.invoiceid), 0) AS orders_per_customer,   -- number of invoices
    COALESCE(SUM(fi.total), 0)       AS revenue_per_customer   -- sum of invoice totals
  FROM customers cu
  LEFT JOIN dwh.fact_invoice fi
         ON fi.customerid = cu.customerid
  GROUP BY cu.customerid, cu.country
),
-- Count how many customers each country has (to identify singletons)
country_counts AS (
  SELECT country, COUNT(*) AS customers_in_country
  FROM customer_stats
  GROUP BY country
),
-- Relabel countries having exactly one customer as 'Other'
labeled_customers AS (
  SELECT
    cs.customerid,
    CASE
      WHEN cc.customers_in_country = 1 THEN 'Other'
      ELSE cs.country
    END AS country_group,
    cs.orders_per_customer,
    cs.revenue_per_customer
  FROM customer_stats cs
  JOIN country_counts cc USING (country)
)
-- Aggregate the final KPIs by (possibly relabeled) country
SELECT
  country_group                               AS country,
  COUNT(*)                                    AS customers_count,
  ROUND(AVG(orders_per_customer)::numeric, 2)  AS avg_orders_per_customer,
  ROUND(AVG(revenue_per_customer)::numeric, 2) AS avg_revenue_per_customer
FROM labeled_customers
GROUP BY country_group
ORDER BY customers_count DESC, country;


--SQL Analytics – Question 5

WITH yearly_sales AS (
    -- Calculates, for each employee and each year:
    SELECT
        e.employeeid,  -- Employee identifier
        EXTRACT(YEAR FROM i.invoicedate) AS year,  -- Year of the invoice
        COUNT(DISTINCT c.customerid) AS num_customers,  -- Number of unique customers handled by the employee in that year
        SUM(i.total) AS total_sales  -- Total sales amount generated by the employee in that year
    FROM dwh.Dim_employee e
    JOIN dwh.Dim_customer c ON e.employeeid = c.supportrepid  -- Link employees to customers they support
    JOIN dwh.Fact_invoice i ON c.customerid = i.customerid  -- Link customers to their invoices
    GROUP BY e.employeeid, EXTRACT(YEAR FROM i.invoicedate)  -- Group by employee and year
),

growth_calc AS (
    -- Calculate the previous year's sales for each employee to compare and measure growth
    SELECT 
        *,
        LAG(total_sales) OVER (PARTITION BY employeeid ORDER BY year) AS prev_year_sales  -- Sales amount from the previous year
    FROM yearly_sales
),

final AS (
    -- Join with employee details and calculate tenure (in years) and year-over-year sales growth percentage
    SELECT 
        CONCAT(e.firstname, ' ', e.lastname) AS employeename,  -- Full employee name
        EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM e.hiredate) AS yearsemployed,  -- Number of years employed in the company
        g.year,  -- Current sales year
        g.num_customers,  -- Number of customers handled in that year
        g.total_sales,  -- Total sales amount in that year
        g.prev_year_sales,  -- Total sales from the previous year (for reference)
        ROUND(100.0 * (g.total_sales - g.prev_year_sales) / NULLIF(g.prev_year_sales, 0), 2)||'%' AS growth_percent  -- Year-over-year sales growth percentage (rounded to 2 decimals). Returns NULL if prev_year_sales = 0
    FROM growth_calc g
    JOIN dwh.Dim_employee e ON g.employeeid = e.employeeid  -- Join with employee table to get full details
)

-- Display all results, sorted by employee name and year
SELECT * 
FROM final
ORDER BY employeename, year;




-- Bonus question: Top Revenue-Generating Track per Artist
WITH track_revenue AS (
    -- Calculate total revenue per track along with artist and track details
    SELECT
        t.trackid,
        t.track_name,
        t.artist_name,
        SUM(il.unitprice * il.quantity) AS total_revenue
    FROM dwh.dim_track t
    JOIN dwh.fact_invoiceline il ON t.trackid = il.trackid
    GROUP BY t.trackid, t.track_name, t.artist_name
),
ranked_tracks AS (
    -- Rank tracks per artist by their total revenue, highest first
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY artist_name ORDER BY total_revenue DESC) AS rn
    FROM track_revenue
)
SELECT
    artist_name,
    track_name,
    total_revenue
FROM ranked_tracks
WHERE rn = 1
ORDER BY artist_name;











