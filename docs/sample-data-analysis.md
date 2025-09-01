# Sample Data Analysis: JSON Flattening

This document shows how your sample Fullbay Invoice JSON would be flattened into the new database structure.

## 📊 **Your Sample JSON Summary**
- **Invoice #14282** for **GAETANO #3**
- **Repair Order #15448** for **2019 Freightliner Cascadia**
- **11 Complaints** with various corrections
- **35+ individual parts** across all corrections
- **Multiple technicians** working on different tasks

## 🗃️ **Flattened Structure Preview**

### **Table 1: `fullbay_raw_data`** (1 record)
```sql
INSERT INTO fullbay_raw_data (fullbay_invoice_id, raw_json_data, processed)
VALUES ('17506970', '{ ... complete JSON ... }', false);
```

### **Table 2: `fullbay_line_items`** (~40+ records)

Here are key examples showing the flattening logic:

## 📋 **Parts Examples (Grouped Correctly)**

### **Example 1: Same Part, Same Correction → GROUP**
From Correction #42557049 (Computer Check):
```
Part: "GASKET, EXHAUST" - TCX/AMS013
- Found 2 instances at $11.79 each in SAME correction
- Result: 1 row with quantity = 2
```

**Flattened Row:**
| Field | Value |
|-------|-------|
| line_item_type | 'PART' |
| shop_part_number | 'TCX/AMS013' |
| part_description | 'GASKET, EXHAUST' |
| quantity | 2 |
| unit_cost | 11.79 |
| unit_price | 32.42 |
| line_total_cost | 23.58 |
| line_total_price | 64.84 |
| fullbay_correction_id | 42557049 |

### **Example 2: Same Part, Different Correction → SEPARATE**
```
"GASKET, EXHAUST" - TCX/AMS013 appears in:
- Correction #42557049 (qty: 2, price: $32.42)
- Correction #42500198 (qty: 4, price: $32.42) [if exists]
Result: 2 separate rows
```

### **Example 3: Different Parts → SEPARATE**
```
All these are separate rows:
- DEF AWNING KIT (4921911) - qty: 1, $412.80
- CLAMP (TCX/T130158342AC2) - qty: 6, $35.75  
- OIL - 1540 (1540) - qty: 40, $6.48
- FILTER, OIL (P7505) - qty: 1, $57.79
```

## 👨‍🔧 **Labor Examples (By Technician)**

### **Multiple Techs on Different Jobs → SEPARATE ROWS**

**Labor Row 1:**
| Field | Value |
|-------|-------|
| line_item_type | 'LABOR' |
| assigned_technician | 'Jacob Humphries' |
| labor_description | 'COMPUTER CHECK' |
| actual_hours | 3.42 |
| technician_portion | 100 |
| line_total_price | 1650.00 |
| fullbay_correction_id | 42557049 |

**Labor Row 2:**
| Field | Value |
|-------|-------|
| line_item_type | 'LABOR' |
| assigned_technician | 'Thomas Germain' |
| labor_description | 'LOF-PM SERVICE-REGULAR' |
| actual_hours | 2.13 |
| technician_portion | 100 |
| line_total_price | 210.00 |
| fullbay_correction_id | 42940071 |

## 📊 **Complete Row Example**

Here's what ONE complete flattened row looks like (using the DEF AWNING KIT):

```sql
INSERT INTO fullbay_line_items (
    -- Invoice Info (repeated on every row)
    fullbay_invoice_id, invoice_number, invoice_date, 
    -- Customer Info (repeated on every row)
    customer_id, customer_title, customer_billing_address,
    -- Service Order Info (repeated on every row)
    fullbay_service_order_id, repair_order_number,
    -- Unit Info (repeated on every row)  
    unit_id, unit_number, unit_make, unit_model, unit_vin,
    -- Complaint Info (repeated on every row)
    fullbay_complaint_id, complaint_note,
    -- Correction Info (repeated on every row)
    fullbay_correction_id, actual_correction,
    -- LINE ITEM SPECIFIC DATA (unique per row)
    line_item_type, fullbay_part_id, part_description, 
    shop_part_number, quantity, unit_cost, unit_price, 
    line_total_cost, line_total_price,
    -- Service Order Totals (repeated on every row)
    so_total_amount, so_tax_total
) VALUES (
    -- Invoice Info
    '17506970', '14282', '2025-05-30',
    -- Customer Info  
    2250840, 'GAETANO #3', '5001 BELLEVUE, Detroit, MI 48202',
    -- Service Order Info
    '18959643', '15448', 
    -- Unit Info
    '6271523', '734', 'Freightliner', 'Cascadia', '3AKBHKDV0KSKC8657',
    -- Complaint Info
    47868748, 'CHECK ENGINE LIGHT',
    -- Correction Info  
    42557049, 'COMPUTER CHECK. SCR CONVERSION CODES PRESENT...',
    -- LINE ITEM SPECIFIC (this is the unique part)
    'PART', 70166538, 'DEF AWNING KIT', 
    '4921911', 1, 206.40, 412.80, 
    206.40, 412.80,
    -- Service Order Totals
    4769.33, 127.83
);
```

## 📈 **Expected Row Count Breakdown**

From your sample JSON, we'd create approximately:

| Category | Count | Details |
|----------|-------|---------|
| **Parts** | ~32-35 rows | Each unique part+correction combination |
| **Labor** | ~11 rows | One per technician assignment |
| **Supplies** | ~3-5 rows | Grease, fluids, misc supplies |
| **Freight** | ~1 row | Shipping charges |
| **TOTAL** | **~47-54 rows** | |

## ✅ **Data Integrity Checks**

**No Data Loss - Everything Preserved:**
- ✅ Complete raw JSON stored in `fullbay_raw_data`  
- ✅ All invoice totals repeated on each row
- ✅ All customer/unit context on each row
- ✅ All parts tracked individually with proper grouping
- ✅ All technician hours tracked separately
- ✅ All financial details preserved

**Grouping Logic Applied Correctly:**
- ✅ Parts grouped ONLY when: same part# + same price + same correction
- ✅ Labor separated by technician 
- ✅ Context repeated consistently

## 🤔 **Questions for Confirmation**

1. **Does the parts grouping logic look correct?**
   - Same part in same correction → GROUP
   - Same part in different correction → SEPARATE

2. **Does the labor breakdown make sense?**
   - One row per technician per correction?

3. **Are there any fields missing** that you need from the JSON?

4. **Should I proceed to implement the data processing logic** to actually flatten your JSON this way?

Once confirmed, I'll create the flattening logic that transforms the JSON into these exact rows!