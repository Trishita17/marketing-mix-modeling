
-- =====================================================
-- Unity Catalog DDL Statements for MMM Sample Data
-- =====================================================

-- Table 1: Campaign Results (Test vs Control)
CREATE OR REPLACE TABLE tadityadb.default.campaign_results (
  campaign_id STRING NOT NULL COMMENT 'Unique campaign identifier',
  campaign_week STRING COMMENT 'Campaign week in YYYY-WW format',
  campaign_start_date DATE COMMENT 'Campaign start date',
  campaign_type STRING COMMENT 'Brand, Performance, or Hybrid campaign',
  region STRING COMMENT 'Geographic region for campaign',
  total_spend DECIMAL(12,2) COMMENT 'Total campaign spend in USD',
  test_sales DECIMAL(12,2) COMMENT 'Sales from treatment group (with marketing)',
  control_sales DECIMAL(12,2) COMMENT 'Sales from control group (without marketing)',
  incremental_sales DECIMAL(12,2) COMMENT 'Test sales - Control sales', 
  lift_percent DECIMAL(5,2) COMMENT 'Percentage lift vs control group',
  roas DECIMAL(8,3) COMMENT 'Return on Ad Spend (test_sales / total_spend)',
  iroas DECIMAL(8,3) COMMENT 'Incremental ROAS (incremental_sales / total_spend)',
  created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
  data_source STRING COMMENT 'Source system for this data'
) 
USING DELTA 
PARTITIONED BY (campaign_week)
COMMENT 'Campaign performance data with test/control results for MMM analysis';

-- Table 2: Campaign Tactics (Spend Breakdown)  
CREATE OR REPLACE TABLE tadityadb.default.campaign_tactics (
  campaign_id STRING NOT NULL COMMENT 'Foreign key to campaign_results table',
  tactic STRING NOT NULL COMMENT 'Marketing tactic (video, audio, display, etc.)',
  spend_amount DECIMAL(12,2) COMMENT 'Amount spent on this tactic in USD',
  spend_percentage DECIMAL(5,1) COMMENT 'Percentage of total campaign spend',
  tactic_category STRING COMMENT 'High-level category (Video, Audio, Display, etc.)',
  channel_type STRING COMMENT 'Digital or Traditional channel type',
  created_timestamp TIMESTAMP COMMENT 'Record creation timestamp',
  data_source STRING COMMENT 'Source system for this data'
)
USING DELTA
COMMENT 'Detailed spend breakdown by marketing tactic for each campaign';

-- Create indexes for better query performance
--CREATE INDEX idx_campaign_results_date ON your_catalog.mmm_schema.--campaign_results (campaign_start_date);
--CREATE INDEX idx_campaign_tactics_id ON your_catalog.mmm_schema.--campaign_tactics (campaign_id);

-- =====================================================
-- Sample Queries for MMM Analysis
-- =====================================================

-- Query 1: Basic campaign performance summary
SELECT 
  campaign_week,
  COUNT(*) as num_campaigns,
  SUM(total_spend) as total_spend,
  SUM(incremental_sales) as total_incremental_sales,
  AVG(iroas) as avg_iroas,
  AVG(lift_percent) as avg_lift_percent
FROM tadityadb.default.campaign_results 
GROUP BY campaign_week
ORDER BY campaign_week;

-- Query 2: Tactic performance analysis
SELECT 
  t.tactic,
  COUNT(DISTINCT t.campaign_id) as num_campaigns,
  SUM(t.spend_amount) as total_spend,
  SUM(c.incremental_sales) as total_incremental_sales,
  SUM(c.incremental_sales) / SUM(t.spend_amount) as avg_iroas
FROM tadityadb.default.campaign_tactics t
JOIN tadityadb.default.campaign_results c ON t.campaign_id = c.campaign_id
GROUP BY t.tactic
ORDER BY avg_iroas DESC;

-- Query 3: Weekly data for MMM model (pivot tactics to columns)
WITH weekly_tactics AS (
  SELECT 
    c.campaign_week,
    c.campaign_start_date,
    c.incremental_sales,
    c.total_spend,
    t.tactic,
    t.spend_amount
  FROM tadityadb.default.campaign_results c
  JOIN tadityadb.default.campaign_tactics t ON c.campaign_id = t.campaign_id
)
SELECT 
  campaign_week,
  campaign_start_date,
  SUM(incremental_sales) as response_variable,
  SUM(CASE WHEN tactic = 'video' THEN spend_amount ELSE 0 END) as video_spend,
  SUM(CASE WHEN tactic = 'audio' THEN spend_amount ELSE 0 END) as audio_spend,
  SUM(CASE WHEN tactic = 'display' THEN spend_amount ELSE 0 END) as display_spend,
  SUM(CASE WHEN tactic = 'search' THEN spend_amount ELSE 0 END) as search_spend,
  SUM(CASE WHEN tactic = 'social' THEN spend_amount ELSE 0 END) as social_spend,
  SUM(total_spend) as total_spend
FROM weekly_tactics
GROUP BY campaign_week, campaign_start_date
ORDER BY campaign_start_date;
