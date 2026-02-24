-- Average selling price per car maker
-- Identificar los fabricantes de carros con los precios de venta promedio más altos, para análisis de mercado
WITH MakerAveragePrice AS (
    SELECT 
        maker,
        AVG(sellingprice) AS avg_price
    FROM cars
    GROUP BY maker
)
SELECT maker, avg_price
FROM MakerAveragePrice
WHERE avg_price > 20000
ORDER BY avg_price DESC;



-- Rank by selling price within each state
-- Identificar los carros más costosos en cada estado
SELECT 
    state,
    maker,
    model,
    sellingprice,
    RANK() OVER (PARTITION BY state ORDER BY sellingprice DESC) AS rank_in_state
FROM cars
ORDER BY state, rank_in_state;


-- Most recent sale date per maker
-- Ventas más recientes para cada fabricante de carros
WITH LatestSales AS (
    SELECT 
        maker,
        model,
        saledate,
        ROW_NUMBER() OVER (PARTITION BY maker ORDER BY saledate DESC) AS row_num
    FROM cars
)
SELECT maker, model, saledate
FROM LatestSales
WHERE row_num = 1;


-- Total and average mileage (odometer) per year 
-- Analizar uso promedio y total de los carros por año
WITH MileageStats AS (
    SELECT 
        year,
        SUM(odometer) AS total_mileage,
        AVG(odometer) AS avg_mileage
    FROM cars
    WHERE odometer IS NOT NULL
    GROUP BY year
)
SELECT year, total_mileage, avg_mileage
FROM MileageStats
ORDER BY year DESC;


-- Missing data / outlier data
SELECT 
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE vin IS NULL OR vin = '') AS missing_vin,
    COUNT(*) FILTER (WHERE odometer IS NULL OR odometer < 0) AS invalid_odometer,
    COUNT(*) FILTER (WHERE sellingprice IS NULL OR sellingprice <= 0) AS invalid_price
FROM cars;


--Top 5 most sold
SELECT 
    maker,
    COUNT(*) AS total_cars
FROM cars
GROUP BY maker
ORDER BY total_cars DESC
LIMIT 5;


-- Average selling price by maker
SELECT 
    maker,
    ROUND(AVG(sellingprice), 2) AS average_price
FROM cars
GROUP BY maker
ORDER BY average_price DESC;


-- Comparison between MMR (estimated value) and actual price
SELECT 
    maker,
    ROUND(AVG(sellingprice), 2) AS average_selling_price,
    ROUND(AVG(mmr), 2) AS average_mmr,
    ROUND(AVG(sellingprice - mmr), 2) AS average_difference
FROM cars
GROUP BY maker
ORDER BY average_difference DESC;


-- Monthly sales trend
SELECT 
    DATE_TRUNC('month', saledate) AS month,
    COUNT(*) AS cars_sold,
    ROUND(AVG(sellingprice), 2) AS average_price
FROM cars
GROUP BY month
ORDER BY month;


-- Top 5 best-selling vendors
SELECT 
    seller,
    COUNT(*) AS total_sales,
    ROUND(AVG(sellingprice), 2) AS average_price
FROM cars
GROUP BY seller
ORDER BY total_sales DESC
LIMIT 5;


-- % of cars sold below MMR
SELECT 
    ROUND(100.0 * COUNT(*) FILTER (WHERE sellingprice < mmr) / COUNT(*), 2) AS percent_below_mmr
FROM cars;