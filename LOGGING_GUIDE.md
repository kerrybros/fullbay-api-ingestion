# 📋 Fullbay API Ingestion Logging Guide

This guide explains where to check logs and how to monitor the ingestion process to ensure no data loss and maintain visibility.

## 🔍 **Where to Check Logs**

### 1. **Database Logs (Primary Source)**
The most comprehensive logging happens in the database itself. You can check:

#### **Quick Log Check Script**
```bash
python check_logs.py
```

This script provides:
- ✅ Recent ingestion activity (last 24 hours)
- ✅ Line items activity and totals
- ✅ Processing errors and data quality issues
- ✅ Comprehensive data summary
- ✅ Daily activity breakdown

#### **Manual Database Queries**
You can also run these queries directly:

```sql
-- Check recent raw data ingestion
SELECT 
    DATE(ingestion_timestamp) as date,
    COUNT(*) as raw_records,
    COUNT(DISTINCT fullbay_invoice_id) as unique_invoices
FROM fullbay_raw_data 
WHERE ingestion_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY DATE(ingestion_timestamp)
ORDER BY date DESC;

-- Check recent line items
SELECT 
    DATE(ingestion_timestamp) as date,
    COUNT(*) as line_items,
    COUNT(DISTINCT fullbay_invoice_id) as unique_invoices,
    SUM(line_total_price) as total_value
FROM fullbay_line_items 
WHERE ingestion_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY DATE(ingestion_timestamp)
ORDER BY date DESC;

-- Check for processing errors
SELECT 
    fullbay_invoice_id,
    processing_errors,
    ingestion_timestamp
FROM fullbay_raw_data 
WHERE processing_errors IS NOT NULL
AND ingestion_timestamp >= NOW() - INTERVAL '24 hours'
ORDER BY ingestion_timestamp DESC;
```

### 2. **Console Output (Real-time)**
When running tests or the Lambda function, you'll see real-time logs in the console:

- 🔍 **API Request Details**: URLs, parameters, response status
- 📊 **Data Processing**: Records processed, line items created
- ❌ **Errors**: Any failures during processing
- ✅ **Success Metrics**: Final counts and summaries

### 3. **File Logs (Future Enhancement)**
The system is set up to create log files in a `logs/` directory. Currently, this directory doesn't exist, but the infrastructure is ready.

## 📊 **What the Logs Show You**

### **Data Quality Monitoring**
The logs automatically track:
- ✅ **Missing Data**: Invoice numbers, customer info, unit info
- ⚠️ **Data Issues**: Zero/negative prices, missing labor hours
- 📈 **Success Rates**: Percentage of records processed successfully

### **Performance Metrics**
- ⏱️ **Processing Time**: How long each ingestion takes
- 📊 **Volume**: Number of records and line items processed
- 💰 **Value**: Total dollar amounts processed

### **Error Tracking**
- 🚨 **Processing Errors**: Failed invoice processing
- ❌ **API Errors**: Connection or authentication issues
- ⚠️ **Data Validation**: Invalid or missing required fields

## 🔧 **How to Monitor for Data Loss**

### **Daily Monitoring Checklist**

1. **Run the Log Checker**
   ```bash
   python check_logs.py
   ```

2. **Verify These Metrics**:
   - ✅ Raw records count matches expected invoices
   - ✅ Line items count is reasonable (usually 10-20 per invoice)
   - ✅ No processing errors in the last 24 hours
   - ✅ Data quality percentages are acceptable (< 10% issues)

3. **Check for Anomalies**:
   - 📉 **Sudden drops** in daily volume
   - 📈 **Unusual spikes** in error rates
   - ⚠️ **Missing data** patterns

### **Weekly Monitoring**

1. **Review Data Quality Trends**
   ```sql
   SELECT 
       DATE(ingestion_timestamp) as date,
       COUNT(*) as total_line_items,
       COUNT(CASE WHEN line_total_price <= 0 THEN 1 END) as zero_prices,
       COUNT(CASE WHEN unit_vin IS NULL THEN 1 END) as missing_vin
   FROM fullbay_line_items
   WHERE ingestion_timestamp >= NOW() - INTERVAL '7 days'
   GROUP BY DATE(ingestion_timestamp)
   ORDER BY date DESC;
   ```

2. **Check Processing Success Rates**
   ```sql
   SELECT 
       DATE(ingestion_timestamp) as date,
       COUNT(*) as total_invoices,
       COUNT(CASE WHEN processing_errors IS NOT NULL THEN 1 END) as failed_invoices
   FROM fullbay_raw_data
   WHERE ingestion_timestamp >= NOW() - INTERVAL '7 days'
   GROUP BY DATE(ingestion_timestamp)
   ORDER BY date DESC;
   ```

## 🚨 **Alert Conditions**

### **Immediate Attention Required**
- ❌ **Zero records** processed in 24 hours
- 🚨 **High error rate** (> 20% processing failures)
- ⚠️ **Data quality issues** > 15% missing critical fields

### **Investigation Needed**
- 📉 **Volume drops** > 50% from normal
- 📈 **Unusual error patterns** (new error types)
- ⏱️ **Performance degradation** (processing time > 2x normal)

## 📈 **Current System Status**

Based on the latest log check:

### **✅ System Health**
- **Raw Data**: 30 records, 30 unique invoices
- **Line Items**: 537 items, $73,500.36 total value
- **Processing**: 1 minor error (Invoice 17556190)
- **Data Quality**: 94% good quality (6% missing unit info)

### **📊 Data Breakdown**
- **PART**: 246 items ($35,777.01)
- **LABOR**: 220 items ($34,632.02)
- **SUPPLY**: 69 items ($2,966.33)
- **FREIGHT**: 2 items ($125.00)

## 🔧 **Enhancing Logging (Optional)**

### **Enable File Logging**
To enable file logging, modify your test scripts to use:

```python
from utils import setup_logging

# Set up logging to both console and file
logger = setup_logging(
    log_level="INFO",
    log_file="logs/ingestion.log"
)
```

### **Add Custom Monitoring**
You can add custom logging to track specific metrics:

```python
from utils import log_ingestion_summary, log_data_quality_report

# Log comprehensive summary
log_ingestion_summary(
    logger=logger,
    start_time=start_time,
    end_time=end_time,
    records_processed=len(api_data),
    records_inserted=successful_inserts,
    line_items_created=line_items_created,
    errors_count=error_count,
    target_date=target_date,
    execution_id=execution_id
)
```

## 📞 **Getting Help**

If you notice issues:

1. **Run the log checker**: `python check_logs.py`
2. **Check recent errors**: Look for processing errors in the output
3. **Verify data quality**: Review the quality metrics
4. **Compare with expectations**: Check if volume matches your business expectations

The logging system is designed to give you complete visibility into the ingestion process and help you quickly identify and resolve any issues.
