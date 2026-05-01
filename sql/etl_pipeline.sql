-- ========================================
-- SOURCE TABLE SETUP
-- ========================================
CREATE TABLE source_cars_data (
    branch_id VARCHAR(200),
    dealer_id VARCHAR(200),
    model_id VARCHAR(200),
    revenue BIGINT,
    units_sold BIGINT,
    date_id VARCHAR(200),
    day INT,
    month INT,
    year INT,
    branch_name VARCHAR(2000),
    dealer_name VARCHAR(2000)
)

-- ========================================
-- WATERMARK TABLE (INCREMENTAL LOADING)
-- ========================================
CREATE TABLE watermark_table (
    last_load_date VARCHAR(200)
)

-- test and simulate the value updation in watermark_table
INSERT INTO watermark_table (last_load_date)
SELECT min(date_id)
FROM source_cars_data;

-- ========================================
-- STORED PROCEDURE FOR WATERMARK UPDATE
-- ========================================
CREATE PROCEDURE UpdateWatermarkTable 
    @lastLoadDate VARCHAR(200)
AS
BEGIN
    -- transaction start
    BEGIN TRANSACTION;

    -- update the last_load_date column in watermark table
    UPDATE watermark_table
    SET last_load_date = @lastLoadDate
    COMMIT TRANSACTION;
END


