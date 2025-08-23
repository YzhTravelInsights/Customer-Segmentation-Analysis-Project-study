
USE cohort_rem;
-- SELECT * FROM onlineretail; # 测试是否链接成功
# WITH user_first_purchase AS (
#     SELECT CustomerID,
#            DATE_FORMAT(MIN(InvoiceDate),'%Y-%m')AS cohort_month FROM onlineretail GROUP BY CustomerID
# ),user_orders_cohort AS(
#     SELECT user_first_purchase.CustomerID,
#     user_first_purchase.cohort_month,
#     DATE_FORMAT(onlineretail.InvoiceDate,'%Y-%m') AS order_month,
#     PERIOD_DIFF(DATE_FORMAT(onlineretail.InvoiceDate,'%Y-%m'),
#     DATE_FORMAT(user_first_purchase.cohort_month,'%Y-%m'))AS month_offset FROM onlineretail
#     LEFT JOIN user_first_purchase ON  onlineretail.CustomerID = user_first_purchase.CustomerID
#     )

-- 采用存储过程自动生成生成 cohort 矩阵
DELIMITER //

-- 存储过程名称与功能一致：生成带百分比的Cohort矩阵
CREATE PROCEDURE GenerateCohortMatrixWithPercentages_1()
BEGIN
    DECLARE max_offset INT;
    DECLARE sql_columns_count TEXT DEFAULT '';  -- 留存用户数量列
    DECLARE sql_columns_percent TEXT DEFAULT ''; -- 留存率百分比列
    DECLARE i INT DEFAULT 0;

    -- 1. 计算最大月份偏移（修正PERIOD_DIFF参数格式为YYYYMM）
    SELECT MAX(month_offset) INTO max_offset
    FROM (
        SELECT
            PERIOD_DIFF(
                DATE_FORMAT(InvoiceDate, '%Y%m'),  -- 修正：YYYYMM格式
                DATE_FORMAT(MIN(InvoiceDate) OVER (PARTITION BY CustomerID), '%Y%m')  -- 修正：YYYYMM格式
            ) AS month_offset
        FROM onlineretail
    ) t;

    -- 2. 动态生成“数量列”和“百分比列”
    WHILE i <= max_offset DO
        -- 生成用户数量列（m0_count, m1_count...）
        SET sql_columns_count = CONCAT(sql_columns_count,
            'COUNT(DISTINCT CASE WHEN month_offset = ', i, ' THEN CustomerID END) AS m', i, '_count,');

        -- 生成留存率百分比列（m1_pct = m1_count/m0_count * 100...）
        IF i > 0 THEN  -- 首月（i=0）无需计算百分比
            SET sql_columns_percent = CONCAT(sql_columns_percent,
                'ROUND(
                    (COUNT(DISTINCT CASE WHEN month_offset = ', i, ' THEN CustomerID END)
                    / NULLIF(COUNT(DISTINCT CASE WHEN month_offset = 0 THEN CustomerID END), 0)) * 100, 2
                ) AS m', i, '_pct,');
        END IF;

        SET i = i + 1;
    END WHILE;

    -- 3. 拼接完整SQL（移除末尾多余的逗号）
    SET @sql = CONCAT('
        WITH user_first_purchase AS (
            SELECT
                CustomerID,
                DATE_FORMAT(MIN(InvoiceDate), ''%Y-%m'') AS cohort_month  -- 同期群标签（YYYY-MM格式，方便阅读）
            FROM onlineretail
            GROUP BY CustomerID
        ),
        user_orders_cohort AS (
            SELECT
                ufp.CustomerID,
                ufp.cohort_month,
                -- 修正：计算月份偏移（订单月份 - 首次消费月份）
                PERIOD_DIFF(
                    DATE_FORMAT(o.InvoiceDate, ''%Y-%m''),  -- 订单月份（YYYY-MM）
                    DATE_FORMAT(ufp.cohort_month, ''%Y-%m'')  -- 首次消费月份（YYYY-MM）
                ) AS month_offset
            FROM onlineretail o
            JOIN user_first_purchase ufp
                ON o.CustomerID = ufp.CustomerID
        )
        SELECT
            cohort_month,  -- 同期群月份
            ', TRIM(TRAILING ',' FROM sql_columns_count), ',  -- 留存数量列
            ', TRIM(TRAILING ',' FROM sql_columns_percent), '  -- 留存率百分比列
        FROM user_orders_cohort
        GROUP BY cohort_month
        ORDER BY cohort_month;'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DELIMITER ;

-- 调用存储过程（名称与定义一致）
CALL GenerateCohortMatrixWithPercentages_1();


