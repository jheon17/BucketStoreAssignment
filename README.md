# 1. 월별 주문 회원 수, 주문 수, 주문 금액, 할인 금액, 배송비
  1)mysql
     SELECT 
    DATE_FORMAT(o.order_date, '%Y-%m') AS month,
    COUNT(DISTINCT o.customer_id) AS num_customers,
    COUNT(o.order_id) AS num_orders,
    SUM(o.total_amount) AS total_order_amount,
    SUM(o.discount_amount) AS total_discount_amount,
    SUM(o.shipping_fee) AS total_shipping_fee
FROM orders o
GROUP BY DATE_FORMAT(o.order_date, '%Y-%m');

  2)oracle
    SELECT
    TO_CHAR(order_date, 'YYYY-MM') AS month,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_order_amount,
    SUM(discount_amount) AS total_discount_amount,
    SUM(shipping_fee) AS total_shipping_fee
FROM
    orders
GROUP BY
    TO_CHAR(order_date, 'YYYY-MM')
ORDER BY
    month;
    
3. 매월 1회이상 주문한 회원 수 (이 중 월별 주문금액의 최근 3개월간 평균이 100만원이 넘는 회원 수)
  1)mysql
    WITH monthly_orders AS (
    SELECT 
        o.customer_id,
        DATE_FORMAT(o.order_date, '%Y-%m') AS month,
        COUNT(o.order_id) AS num_orders,
        SUM(o.total_amount) AS total_order_amount
    FROM orders o
    GROUP BY o.customer_id, DATE_FORMAT(o.order_date, '%Y-%m')
),
recent_avg_order AS (
    SELECT 
        customer_id,
        AVG(total_order_amount) AS avg_order_amount_last_3_months
    FROM monthly_orders
    WHERE month >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 3 MONTH), '%Y-%m')
    GROUP BY customer_id
)
SELECT 
    COUNT(customer_id) AS high_spending_customers_count
FROM recent_avg_order
WHERE avg_order_amount_last_3_months > 1000000;

  2)oracle
    WITH monthly_totals AS (
    SELECT
        customer_id,
        TO_CHAR(order_date, 'YYYY-MM') AS month,
        SUM(total_amount) AS monthly_total
    FROM
        orders
    GROUP BY
        customer_id,
        TO_CHAR(order_date, 'YYYY-MM')
),
customer_averages AS (
    SELECT
        customer_id,
        AVG(monthly_total) OVER (PARTITION BY customer_id ORDER BY month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS avg_last_3_months_amount
    FROM
        monthly_totals
)
SELECT
    COUNT(DISTINCT customer_id) AS active_customers,
    COUNT(DISTINCT CASE WHEN avg_last_3_months_amount > 1000000 THEN customer_id END) AS high_value_customers
FROM
    customer_averages;

3. 3월에 A 브랜드를 처음으로 구매한 회원들의 주문 건
  1)mysql
    WITH first_purchase_in_march AS (
    SELECT
        o.customer_id,
        MIN(o.order_date) AS first_purchase_date
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN brands b ON p.brand_id = b.brand_id
    WHERE b.brand_name = 'A' AND MONTH(o.order_date) = 3
    GROUP BY o.customer_id
)
SELECT 
    COUNT(DISTINCT o.customer_id) AS num_customers,
    COUNT(o.order_id) AS num_orders,
    SUM(o.total_amount) AS total_order_amount,
    SUM(o.discount_amount) AS total_discount_amount,
    SUM(o.shipping_fee) AS total_shipping_fee
FROM orders o
WHERE o.customer_id IN (SELECT customer_id FROM first_purchase_in_march);

  2)oracle
    WITH march_first_time_a_brand_customers AS (
    SELECT
        o.customer_id
    FROM
        orders o
    JOIN products p ON o.product_id = p.product_id
    JOIN brands b ON p.brand_id = b.brand_id
    WHERE
        TO_CHAR(o.order_date, 'YYYY-MM') = '2024-03'
        AND b.brand_name = 'A'
        AND NOT EXISTS (
            SELECT 1
            FROM orders prev_o
            JOIN products prev_p ON prev_o.product_id = prev_p.product_id
            WHERE
                prev_o.customer_id = o.customer_id
                AND prev_p.brand_id = b.brand_id
                AND prev_o.order_date < TO_DATE('2024-03-01', 'YYYY-MM-DD')
        )
)
SELECT
    COUNT(DISTINCT o.customer_id) AS march_first_time_customers,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS total_order_amount,
    SUM(o.discount_amount) AS total_discount_amount,
    SUM(o.shipping_fee) AS total_shipping_fee
FROM
    orders o
WHERE
    o.customer_id IN (SELECT customer_id FROM march_first_time_a_brand_customers);


4. 회원 페이지 뷰 로그를 access time 기준으로 30분 단위 유저 세션 그룹핑(일별 세션 수, 월별 평균 세션 유지 시간, 세션별 평균 총 페이지(+유니크 페이지) 수 (전 기간))
  1)mysql
    WITH sessions AS (
    SELECT 
        pv.user_id,
        pv.access_timestamp,
        IF(
            TIMESTAMPDIFF(MINUTE, LAG(pv.access_timestamp) OVER (PARTITION BY pv.user_id ORDER BY pv.access_timestamp), pv.access_timestamp) > 30 
            OR LAG(pv.access_timestamp) OVER (PARTITION BY pv.user_id ORDER BY pv.access_timestamp) IS NULL,
            1,
            0
        ) AS new_session
    FROM page_views pv
),
numbered_sessions AS (
    SELECT 
        user_id,
        access_timestamp,
        page,
        SUM(new_session) OVER (PARTITION BY user_id ORDER BY access_timestamp) AS session_id
    FROM sessions
),
session_durations AS (
    SELECT 
        session_id,
        TIMESTAMPDIFF(MINUTE, MIN(access_timestamp), MAX(access_timestamp)) AS session_duration
    FROM numbered_sessions
    GROUP BY session_id
    HAVING session_duration > 0
),
session_page_counts AS (
    SELECT 
        session_id,
        COUNT(page) AS total_pages,
        COUNT(DISTINCT page) AS unique_pages
    FROM numbered_sessions
    GROUP BY session_id
)

-- 일별 세션 수
SELECT 
    DATE(access_timestamp) AS day,
    COUNT(DISTINCT session_id) AS daily_sessions_count
FROM numbered_sessions
GROUP BY DATE(access_timestamp);

-- 월별 평균 세션 유지 시간
SELECT 
    DATE_FORMAT(MIN(access_timestamp), '%Y-%m') AS month,
    ROUND(AVG(session_duration), 2) AS avg_session_duration
FROM session_durations sd
JOIN numbered_sessions ns ON sd.session_id = ns.session_id
GROUP BY DATE_FORMAT(MIN(access_timestamp), '%Y-%m');

-- 세션별 평균 총 페이지 수 및 유니크 페이지 수 (전 기간)
SELECT 
    ROUND(AVG(total_pages), 2) AS avg_total_pages,
    ROUND(AVG(unique_pages), 2) AS avg_unique_pages
FROM session_page_counts;
);

  2)oracle
    WITH user_sessions AS (
    SELECT
        user_id,
        TRUNC(access_timestamp, 'MI') AS session_start_time,
        LEAST(TRUNC(access_timestamp + INTERVAL '30' MINUTE, 'MI'), TRUNC(SYSDATE)) AS session_end_time,
        COUNT(DISTINCT page) AS unique_page_count,
        COUNT(page) AS total_page_count
    FROM
        page_views
    GROUP BY
        user_id,
        TRUNC(access_timestamp, 'MI')
),
daily_session_stats AS (
    SELECT
        TRUNC(session_start_time) AS session_day,
        COUNT(*) AS daily_sessions
    FROM
        user_sessions
    GROUP BY
        TRUNC(session_start_time)
),
monthly_session_duration AS (
    SELECT
        TO_CHAR(session_start_time, 'YYYY-MM') AS month,
        AVG(EXTRACT(MINUTE FROM (session_end_time - session_start_time))) AS avg_session_duration
    FROM
        user_sessions
    WHERE
        session_end_time > session_start_time
    GROUP BY
        TO_CHAR(session_start_time, 'YYYY-MM')
),
overall_session_page_stats AS (
    SELECT
        ROUND(AVG(total_page_count), 2) AS avg_total_page_count,
        ROUND(AVG(unique_page_count), 2) AS avg_unique_page_count
    FROM
        user_sessions
)
-- Aggregating results from daily, monthly, and overall statistics
SELECT
    (SELECT SUM(daily_sessions) FROM daily_session_stats) AS 일별_세션_수,
    (SELECT AVG(avg_session_duration) FROM monthly_session_duration) AS 월별_평균_세션_유지_시간,
    (SELECT avg_total_page_count FROM overall_session_page_stats) AS 세션별_평균_총_페이지_수,
    (SELECT avg_unique_page_count FROM overall_session_page_stats) AS 세션별_평균_유니크_페이지_수
FROM dual;




