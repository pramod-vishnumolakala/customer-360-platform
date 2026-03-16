-- ============================================================
-- Customer 360 Platform — Azure Synapse DDL & Analytics
-- Author: Pramod Vishnumolakala
-- ============================================================

-- ── Customer 360 unified profile ────────────────────────────────────
CREATE TABLE IF NOT EXISTS gold.customer_360_profile (
    customer_id              VARCHAR(20)     NOT NULL,
    -- Policy dimensions
    total_policies           INT,
    active_policies          INT,
    total_annual_premium     DECIMAL(18,2),
    avg_annual_premium       DECIMAL(18,2),
    latest_policy_date       DATE,
    first_policy_date        DATE,
    renewal_count            INT,
    product_lines_count      INT,
    highest_tier             VARCHAR(20),
    customer_tenure_years    DECIMAL(5,1),
    renewal_rate             DECIMAL(5,4),
    customer_value_tier      VARCHAR(30),
    -- Claims dimensions
    total_claims             INT,
    open_claims              INT,
    total_incurred_loss      DECIMAL(18,2),
    avg_claim_amount         DECIMAL(18,2),
    avg_claim_cycle_days     DECIMAL(7,1),
    catastrophic_claims      INT,
    latest_claim_date        DATE,
    claim_frequency          DECIMAL(7,4),
    loss_ratio               DECIMAL(7,4),
    -- Telematics
    avg_telematics_risk      DECIMAL(5,2),
    avg_miles_monthly        DECIMAL(9,2),
    avg_harsh_braking        DECIMAL(7,2),
    avg_speeding_pct         DECIMAL(5,2),
    last_trip_date           DATE,
    -- Churn
    churn_risk_score         INT,
    is_churn_risk            BIT,
    -- Metadata
    gold_loaded_at           DATETIME2 DEFAULT GETDATE()
)
WITH (
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX
);


-- ── Policy silver table ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.policies (
    policy_id            VARCHAR(36)    NOT NULL,
    customer_id          VARCHAR(20)    NOT NULL,
    policy_number        VARCHAR(30),
    policy_type          VARCHAR(30),
    policy_status        VARCHAR(20),
    product_line         VARCHAR(30),
    state_code           CHAR(2),
    effective_date       DATE,
    expiration_date      DATE,
    inception_date       DATE,
    annual_premium       DECIMAL(18,2),
    premium_monthly      DECIMAL(18,2),
    premium_tier         VARCHAR(20),
    coverage_days        INT,
    days_since_inception INT,
    is_active            BIT,
    is_renewal           BIT,
    silver_loaded_at     DATETIME2,
    source_system        VARCHAR(30)
)
WITH (
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX
);


-- ── Claims silver table ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.claims (
    claim_id             VARCHAR(36)    NOT NULL,
    policy_id            VARCHAR(36)    NOT NULL,
    loss_date            DATE,
    report_date          DATE,
    close_date           DATE,
    claim_status         VARCHAR(20),
    peril_code           VARCHAR(20),
    incurred_loss        DECIMAL(18,2),
    paid_loss            DECIMAL(18,2),
    reserved_amount      DECIMAL(18,2),
    severity             VARCHAR(20),
    days_to_report       INT,
    days_to_close        INT,
    is_open              BIT,
    siu_referral_flag    BIT,
    silver_loaded_at     DATETIME2
)
WITH (
    DISTRIBUTION = HASH(policy_id),
    CLUSTERED COLUMNSTORE INDEX
);


-- ============================================================
-- ANALYTICAL QUERIES
-- ============================================================

-- 1. Customer lifetime value by segment (Power BI source)
SELECT
    customer_value_tier,
    COUNT(*)                             AS customer_count,
    ROUND(AVG(total_annual_premium), 2)  AS avg_premium,
    ROUND(AVG(customer_tenure_years), 1) AS avg_tenure_years,
    ROUND(AVG(renewal_rate) * 100, 1)    AS avg_renewal_rate_pct,
    ROUND(AVG(loss_ratio), 3)            AS avg_loss_ratio,
    SUM(CASE WHEN is_churn_risk = 1 THEN 1 ELSE 0 END) AS churn_risk_count,
    ROUND(SUM(CASE WHEN is_churn_risk = 1 THEN 1.0 ELSE 0 END) / COUNT(*) * 100, 1) AS churn_risk_pct
FROM gold.customer_360_profile
GROUP BY customer_value_tier
ORDER BY avg_premium DESC;


-- 2. Churn risk cohort — top 500 at-risk high-value customers
SELECT TOP 500
    customer_id,
    customer_value_tier,
    total_annual_premium,
    churn_risk_score,
    renewal_rate,
    open_claims,
    loss_ratio,
    avg_telematics_risk,
    customer_tenure_years
FROM gold.customer_360_profile
WHERE is_churn_risk = 1
  AND customer_value_tier IN ('TIER_1_PLATINUM', 'TIER_2_GOLD')
ORDER BY total_annual_premium DESC, churn_risk_score DESC;


-- 3. Cross-sell opportunity matrix
SELECT
    product_lines_count,
    highest_tier,
    COUNT(*)                           AS customer_count,
    ROUND(AVG(total_annual_premium),2) AS avg_premium,
    ROUND(AVG(renewal_rate)*100, 1)    AS renewal_rate_pct,
    SUM(CASE WHEN product_lines_count = 1 THEN 1 ELSE 0 END) AS mono_line,
    SUM(CASE WHEN product_lines_count >= 2 THEN 1 ELSE 0 END) AS multi_line
FROM gold.customer_360_profile
GROUP BY product_lines_count, highest_tier
ORDER BY product_lines_count, avg_premium DESC;


-- 4. Claims cycle time performance by product line
SELECT
    p.product_line,
    COUNT(c.claim_id)                    AS total_claims,
    ROUND(AVG(c.days_to_report), 1)      AS avg_days_to_report,
    ROUND(AVG(c.days_to_close), 1)       AS avg_days_to_close,
    ROUND(AVG(c.incurred_loss), 2)       AS avg_incurred_loss,
    SUM(c.incurred_loss)                 AS total_incurred_loss,
    SUM(CASE WHEN c.severity = 'CATASTROPHIC' THEN 1 ELSE 0 END) AS catastrophic_count,
    SUM(CASE WHEN c.siu_referral_flag = 1 THEN 1 ELSE 0 END)     AS siu_referrals
FROM silver.claims c
JOIN silver.policies p ON c.policy_id = p.policy_id
WHERE c.report_date >= DATEADD(MONTH, -12, GETDATE())
GROUP BY p.product_line
ORDER BY total_incurred_loss DESC;


-- 5. Pipeline SLA report — ADF pipeline latency (last 7 days)
SELECT
    CAST(gold_loaded_at AS DATE)  AS load_date,
    COUNT(*)                       AS profiles_loaded,
    MIN(gold_loaded_at)            AS first_load,
    MAX(gold_loaded_at)            AS last_load
FROM gold.customer_360_profile
WHERE gold_loaded_at >= DATEADD(DAY, -7, GETDATE())
GROUP BY CAST(gold_loaded_at AS DATE)
ORDER BY load_date DESC;
