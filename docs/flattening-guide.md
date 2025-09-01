# FullBay Invoice Flattening Guide

## 🎯 **Overview**

This guide explains how the FullBay invoice flattening system works and how to test it with your own sample JSON data. The system converts complex nested JSON invoices into normalized line items that are perfect for business intelligence and reporting.

## 📊 **What the Flattening System Does**

### **Input**: Complex FullBay Invoice JSON
```json
{
  "primaryKey": "17506970",
  "invoiceNumber": "14282",
  "Customer": {"customerId": 2250840, "title": "GAETANO #3"},
  "ServiceOrder": {
    "Complaints": [{
      "Corrections": [{
        "Parts": [{"description": "DEF AWNING KIT", "quantity": "1", "sellingPrice": "412.80"}],
        "AssignedTechnicians": [{"technician": "Jacob Humphries", "actualHours": 3.42}]
      }]
    }]
  }
}
```

### **Output**: Flattened Line Items (Excel-Compatible)
Each row contains:
- **Complete Context**: Invoice, customer, vehicle, complaint, and correction data
- **Line Item Details**: Part/labor information with proper classification
- **Financial Data**: Costs, prices, quantities, and totals
- **Technician Assignment**: Labor split by individual technicians

## 🔧 **How to Test with Your Sample JSON**

### **Step 1: Prepare Your JSON File**
Save your FullBay invoice JSON to a file (e.g., `my_invoice.json`)

### **Step 2: Run the Flattening Demo**
```bash
# Test with your JSON file
python scripts/flattening_demo_standalone.py my_invoice.json

# Or test with built-in sample data
python scripts/flattening_demo_standalone.py
```

### **Step 3: Review Results**
The script will:
- ✅ Process your JSON through the flattening logic
- ✅ Generate a CSV file (Excel-compatible)
- ✅ Show a detailed summary of the transformation
- ✅ Validate data quality and completeness

## 📋 **Flattening Logic Explained**

### **1. Parts Processing**
**Smart Grouping Logic:**
- **Same Part + Same Correction + Same Price** → **GROUPED** (quantity summed)
- **Same Part + Different Correction** → **SEPARATE ROWS**
- **Different Parts** → **SEPARATE ROWS**

**Example:**
```
Original: 2x "GASKET, EXHAUST" in same correction
Result: 1 row with quantity = 2, total = $64.84
```

### **2. Labor Processing**
**Technician Splitting:**
- **Multiple Technicians** → **SEPARATE ROWS** (one per technician)
- **Single Technician** → **ONE ROW** with full labor details
- **No Technician Assignment** → **ONE ROW** with primary technician

**Example:**
```
Original: Jacob (3.42 hrs) + Thomas (2.13 hrs)
Result: 2 labor rows with individual hours and costs
```

### **3. Data Classification**
**Automatic Type Detection:**
- **PART**: Standard parts and components
- **SUPPLY**: Fluids, grease, consumables
- **FREIGHT**: Shipping and freight charges
- **LABOR**: Technician work and services

### **4. Context Preservation**
**Every Row Contains:**
- Invoice details (ID, number, dates, totals)
- Customer information (ID, name, contact, billing)
- Vehicle details (VIN, make, model, year)
- Service order context (repair order, technician)
- Complaint and correction information
- Line item specifics (part/labor details)

## 📊 **Column Structure**

The flattened data includes **70+ columns** organized into logical groups:

### **Invoice Level** (8 columns)
- `fullbay_invoice_id`, `invoice_number`, `invoice_date`, `due_date`
- `shop_title`, `shop_email`, `shop_address`, `exported`

### **Customer Level** (6 columns)
- `customer_id`, `customer_title`, `customer_external_id`
- `customer_main_phone`, `customer_secondary_phone`, `customer_billing_address`

### **Service Order Level** (5 columns)
- `fullbay_service_order_id`, `repair_order_number`
- `service_order_created`, `service_order_start_date`, `service_order_completion_date`

### **Vehicle Level** (8 columns)
- `unit_id`, `unit_number`, `unit_type`, `unit_year`
- `unit_make`, `unit_model`, `unit_vin`, `unit_license_plate`

### **Technician Level** (2 columns)
- `primary_technician`, `primary_technician_number`

### **Service Order Totals** (7 columns)
- `so_total_parts_cost`, `so_total_parts_price`, `so_total_labor_hours`
- `so_total_labor_cost`, `so_subtotal`, `so_tax_total`, `so_total_amount`

### **Complaint Level** (6 columns)
- `fullbay_complaint_id`, `complaint_type`, `complaint_subtype`
- `complaint_note`, `complaint_cause`, `complaint_authorized`

### **Correction Level** (8 columns)
- `fullbay_correction_id`, `correction_title`, `global_component`
- `global_system`, `global_service`, `recommended_correction`
- `actual_correction`, `correction_performed`

### **Line Item Level** (15 columns)
- `line_item_type`, `fullbay_part_id`, `part_description`
- `shop_part_number`, `vendor_part_number`, `part_category`
- `quantity`, `to_be_returned_quantity`, `returned_quantity`
- `unit_cost`, `unit_price`, `line_total_cost`, `line_total_price`
- `price_overridden`, `taxable`, `inventory_item`

### **Labor Specific** (7 columns)
- `labor_description`, `labor_rate_type`, `assigned_technician`
- `assigned_technician_number`, `labor_hours`, `actual_hours`, `technician_portion`

### **Additional Fields** (8 columns)
- `core_type`, `sublet`, `quickbooks_account`, `quickbooks_item`
- `quickbooks_item_type`, `raw_data_id`, `processing_timestamp`

## ✅ **Data Quality Assurance**

### **Validation Checks**
1. **Critical Fields**: Every line item has invoice ID and basic context
2. **Financial Accuracy**: Totals match original invoice amounts
3. **Data Completeness**: No missing required fields
4. **Type Classification**: Proper PART/SUPPLY/LABOR/FREIGHT classification

### **Error Handling**
- **Malformed Data**: Graceful handling with warnings
- **Missing Fields**: Default values where appropriate
- **Invalid Numbers**: Safe parsing with fallbacks
- **Processing Errors**: Detailed error logging

## 📈 **Sample Results Analysis**

### **Input Summary**
- **1 Invoice** with **2 Complaints**
- **2 Corrections** with **4 Parts**
- **2 Technicians** working on different tasks

### **Output Summary**
- **6 Line Items** generated (2 parts + 2 supplies + 2 labor)
- **$2,654.63** total revenue processed
- **$2,062.63** gross profit calculated
- **100%** data quality score

### **Line Item Breakdown**
```
PART: 2 items (DEF AWNING KIT, GASKET EXHAUST)
SUPPLY: 2 items (OIL - 1540, FILTER OIL)  
LABOR: 2 items (Jacob Humphries, Thomas Germain)
```

## 🔍 **Testing Your Own Data**

### **1. Sample JSON Structure**
Your JSON should follow this structure:
```json
{
  "primaryKey": "invoice_id",
  "invoiceNumber": "invoice_number",
  "Customer": {
    "customerId": 123,
    "title": "Customer Name"
  },
  "ServiceOrder": {
    "primaryKey": "service_order_id",
    "Unit": {
      "vin": "vehicle_vin",
      "make": "vehicle_make"
    },
    "Complaints": [
      {
        "primaryKey": "complaint_id",
        "AssignedTechnicians": [
          {
            "technician": "Technician Name",
            "actualHours": 2.5
          }
        ],
        "Corrections": [
          {
            "primaryKey": "correction_id",
            "Parts": [
              {
                "description": "Part Description",
                "quantity": "1",
                "sellingPrice": "100.00"
              }
            ]
          }
        ]
      }
    ]
  }
}
```

### **2. Running the Test**
```bash
# Test with your file
python scripts/flattening_demo_standalone.py your_invoice.json

# Check the output
# - Console summary shows processing results
# - CSV file contains all flattened data
# - Excel can open the CSV directly
```

### **3. Validating Results**
- **Row Count**: Should match expected line items
- **Financial Totals**: Should match original invoice
- **Data Completeness**: All critical fields populated
- **Classification**: Proper PART/SUPPLY/LABOR types

## 🚨 **Common Issues and Solutions**

### **Issue: No Line Items Generated**
**Cause**: Invalid JSON structure or missing required fields
**Solution**: Check JSON format and ensure required fields are present

### **Issue: Missing Financial Data**
**Cause**: Invalid number formats in JSON
**Solution**: Ensure prices and quantities are valid numbers

### **Issue: Incorrect Part Grouping**
**Cause**: Different prices for same part number
**Solution**: This is correct behavior - different prices create separate rows

### **Issue: Labor Not Split by Technician**
**Cause**: No AssignedTechnicians array in complaint
**Solution**: Add technician assignments to the complaint structure

## 📞 **Getting Help**

If you encounter issues:

1. **Check the console output** for detailed error messages
2. **Validate your JSON structure** against the sample format
3. **Review the summary report** for data quality issues
4. **Compare totals** between original and flattened data

## 🎯 **Next Steps**

Once you're satisfied with the flattening results:

1. **Deploy to Production**: Use the full system with AWS Lambda
2. **Set up Monitoring**: Configure CloudWatch alerts and dashboards
3. **Create Reports**: Use the flattened data for business intelligence
4. **Automate Processing**: Schedule regular data ingestion

---

**✅ The flattening system ensures zero data loss while providing maximum business value through normalized, queryable line items.**
