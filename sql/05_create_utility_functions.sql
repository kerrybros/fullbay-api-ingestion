-- =====================================================
-- Utility Functions and Triggers
-- =====================================================
-- Functions for maintaining summary tables and other database operations

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Function to refresh daily summary
CREATE OR REPLACE FUNCTION refresh_daily_summary(target_date DATE DEFAULT CURRENT_DATE)
RETURNS VOID AS $$
BEGIN
    -- Delete existing summary for the target date
    DELETE FROM daily_summary WHERE summary_date = target_date;
    
    -- Insert new summary
    INSERT INTO daily_summary (
        summary_date,
        total_invoices,
        exported_invoices,
        unexported_invoices,
        total_revenue,
        total_parts_revenue,
        total_labor_revenue,
        total_supplies_revenue,
        total_misc_revenue,
        total_sublet_revenue,
        total_labor_hours,
        total_actual_hours,
        total_parts_quantity,
        unique_parts_count,
        unique_customers,
        unique_vehicles,
        unique_technicians
    )
    SELECT 
        target_date,
        COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
        COUNT(DISTINCT CASE WHEN exported THEN fullbay_invoice_id END) as exported_invoices,
        COUNT(DISTINCT CASE WHEN NOT exported THEN fullbay_invoice_id END) as unexported_invoices,
        SUM(invoice_total) as total_revenue,
        SUM(CASE WHEN line_item_type = 'PART' THEN line_total_price ELSE 0 END) as total_parts_revenue,
        SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_revenue,
        SUM(CASE WHEN line_item_type = 'SUPPLY' THEN line_total_price ELSE 0 END) as total_supplies_revenue,
        SUM(CASE WHEN line_item_type = 'MISC' THEN line_total_price ELSE 0 END) as total_misc_revenue,
        SUM(CASE WHEN line_item_type = 'SUBLET' THEN line_total_price ELSE 0 END) as total_sublet_revenue,
        SUM(labor_hours) as total_labor_hours,
        SUM(actual_hours) as total_actual_hours,
        SUM(CASE WHEN line_item_type = 'PART' THEN quantity ELSE 0 END) as total_parts_quantity,
        COUNT(DISTINCT CASE WHEN line_item_type = 'PART' THEN shop_part_number END) as unique_parts_count,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT unit_vin) as unique_vehicles,
        COUNT(DISTINCT assigned_technician) as unique_technicians
    FROM fullbay_line_items 
    WHERE DATE(invoice_date) = target_date;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh monthly summary
CREATE OR REPLACE FUNCTION refresh_monthly_summary(target_year INTEGER, target_month INTEGER)
RETURNS VOID AS $$
BEGIN
    -- Delete existing summary for the target month
    DELETE FROM monthly_summary WHERE summary_year = target_year AND summary_month = target_month;
    
    -- Insert new summary
    INSERT INTO monthly_summary (
        summary_year,
        summary_month,
        total_invoices,
        exported_invoices,
        unexported_invoices,
        total_revenue,
        total_parts_revenue,
        total_labor_revenue,
        total_supplies_revenue,
        total_misc_revenue,
        total_sublet_revenue,
        total_labor_hours,
        total_actual_hours,
        total_parts_quantity,
        unique_parts_count,
        unique_customers,
        unique_vehicles,
        unique_technicians
    )
    SELECT 
        target_year,
        target_month,
        COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
        COUNT(DISTINCT CASE WHEN exported THEN fullbay_invoice_id END) as exported_invoices,
        COUNT(DISTINCT CASE WHEN NOT exported THEN fullbay_invoice_id END) as unexported_invoices,
        SUM(invoice_total) as total_revenue,
        SUM(CASE WHEN line_item_type = 'PART' THEN line_total_price ELSE 0 END) as total_parts_revenue,
        SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_revenue,
        SUM(CASE WHEN line_item_type = 'SUPPLY' THEN line_total_price ELSE 0 END) as total_supplies_revenue,
        SUM(CASE WHEN line_item_type = 'MISC' THEN line_total_price ELSE 0 END) as total_misc_revenue,
        SUM(CASE WHEN line_item_type = 'SUBLET' THEN line_total_price ELSE 0 END) as total_sublet_revenue,
        SUM(labor_hours) as total_labor_hours,
        SUM(actual_hours) as total_actual_hours,
        SUM(CASE WHEN line_item_type = 'PART' THEN quantity ELSE 0 END) as total_parts_quantity,
        COUNT(DISTINCT CASE WHEN line_item_type = 'PART' THEN shop_part_number END) as unique_parts_count,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT unit_vin) as unique_vehicles,
        COUNT(DISTINCT assigned_technician) as unique_technicians
    FROM fullbay_line_items 
    WHERE EXTRACT(YEAR FROM invoice_date) = target_year 
    AND EXTRACT(MONTH FROM invoice_date) = target_month;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh customer summary
CREATE OR REPLACE FUNCTION refresh_customer_summary(target_customer_id INTEGER DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
    -- If specific customer provided, refresh only that customer
    IF target_customer_id IS NOT NULL THEN
        DELETE FROM customer_summary WHERE customer_id = target_customer_id;
        
        INSERT INTO customer_summary (
            customer_id,
            customer_title,
            total_invoices,
            first_invoice_date,
            last_invoice_date,
            total_revenue,
            total_parts_revenue,
            total_labor_revenue,
            total_supplies_revenue,
            total_misc_revenue,
            total_sublet_revenue,
            unique_vehicles,
            vehicle_list,
            total_labor_hours,
            total_actual_hours,
            total_parts_quantity,
            unique_parts_count
        )
        SELECT 
            customer_id,
            MAX(customer_title) as customer_title,
            COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
            MIN(invoice_date) as first_invoice_date,
            MAX(invoice_date) as last_invoice_date,
            SUM(invoice_total) as total_revenue,
            SUM(CASE WHEN line_item_type = 'PART' THEN line_total_price ELSE 0 END) as total_parts_revenue,
            SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_revenue,
            SUM(CASE WHEN line_item_type = 'SUPPLY' THEN line_total_price ELSE 0 END) as total_supplies_revenue,
            SUM(CASE WHEN line_item_type = 'MISC' THEN line_total_price ELSE 0 END) as total_misc_revenue,
            SUM(CASE WHEN line_item_type = 'SUBLET' THEN line_total_price ELSE 0 END) as total_sublet_revenue,
            COUNT(DISTINCT unit_vin) as unique_vehicles,
            json_agg(DISTINCT unit_vin)::text as vehicle_list,
            SUM(labor_hours) as total_labor_hours,
            SUM(actual_hours) as total_actual_hours,
            SUM(CASE WHEN line_item_type = 'PART' THEN quantity ELSE 0 END) as total_parts_quantity,
            COUNT(DISTINCT CASE WHEN line_item_type = 'PART' THEN shop_part_number END) as unique_parts_count
        FROM fullbay_line_items 
        WHERE customer_id = target_customer_id
        GROUP BY customer_id;
    ELSE
        -- Refresh all customers
        DELETE FROM customer_summary;
        
        INSERT INTO customer_summary (
            customer_id,
            customer_title,
            total_invoices,
            first_invoice_date,
            last_invoice_date,
            total_revenue,
            total_parts_revenue,
            total_labor_revenue,
            total_supplies_revenue,
            total_misc_revenue,
            total_sublet_revenue,
            unique_vehicles,
            vehicle_list,
            total_labor_hours,
            total_actual_hours,
            total_parts_quantity,
            unique_parts_count
        )
        SELECT 
            customer_id,
            MAX(customer_title) as customer_title,
            COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
            MIN(invoice_date) as first_invoice_date,
            MAX(invoice_date) as last_invoice_date,
            SUM(invoice_total) as total_revenue,
            SUM(CASE WHEN line_item_type = 'PART' THEN line_total_price ELSE 0 END) as total_parts_revenue,
            SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_revenue,
            SUM(CASE WHEN line_item_type = 'SUPPLY' THEN line_total_price ELSE 0 END) as total_supplies_revenue,
            SUM(CASE WHEN line_item_type = 'MISC' THEN line_total_price ELSE 0 END) as total_misc_revenue,
            SUM(CASE WHEN line_item_type = 'SUBLET' THEN line_total_price ELSE 0 END) as total_sublet_revenue,
            COUNT(DISTINCT unit_vin) as unique_vehicles,
            json_agg(DISTINCT unit_vin)::text as vehicle_list,
            SUM(labor_hours) as total_labor_hours,
            SUM(actual_hours) as total_actual_hours,
            SUM(CASE WHEN line_item_type = 'PART' THEN quantity ELSE 0 END) as total_parts_quantity,
            COUNT(DISTINCT CASE WHEN line_item_type = 'PART' THEN shop_part_number END) as unique_parts_count
        FROM fullbay_line_items 
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh vehicle summary
CREATE OR REPLACE FUNCTION refresh_vehicle_summary(target_vin VARCHAR(50) DEFAULT NULL)
RETURNS VOID AS $$
BEGIN
    -- If specific VIN provided, refresh only that vehicle
    IF target_vin IS NOT NULL THEN
        DELETE FROM vehicle_summary WHERE unit_vin = target_vin;
        
        INSERT INTO vehicle_summary (
            unit_vin,
            unit_number,
            unit_year,
            unit_make,
            unit_model,
            unit_type,
            total_invoices,
            first_invoice_date,
            last_invoice_date,
            total_revenue,
            total_parts_revenue,
            total_labor_revenue,
            total_supplies_revenue,
            total_misc_revenue,
            total_sublet_revenue,
            total_labor_hours,
            total_actual_hours,
            total_parts_quantity,
            unique_parts_count,
            primary_customer_id,
            primary_customer_title
        )
        SELECT 
            unit_vin,
            MAX(unit_number) as unit_number,
            MAX(unit_year) as unit_year,
            MAX(unit_make) as unit_make,
            MAX(unit_model) as unit_model,
            MAX(unit_type) as unit_type,
            COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
            MIN(invoice_date) as first_invoice_date,
            MAX(invoice_date) as last_invoice_date,
            SUM(invoice_total) as total_revenue,
            SUM(CASE WHEN line_item_type = 'PART' THEN line_total_price ELSE 0 END) as total_parts_revenue,
            SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_revenue,
            SUM(CASE WHEN line_item_type = 'SUPPLY' THEN line_total_price ELSE 0 END) as total_supplies_revenue,
            SUM(CASE WHEN line_item_type = 'MISC' THEN line_total_price ELSE 0 END) as total_misc_revenue,
            SUM(CASE WHEN line_item_type = 'SUBLET' THEN line_total_price ELSE 0 END) as total_sublet_revenue,
            SUM(labor_hours) as total_labor_hours,
            SUM(actual_hours) as total_actual_hours,
            SUM(CASE WHEN line_item_type = 'PART' THEN quantity ELSE 0 END) as total_parts_quantity,
            COUNT(DISTINCT CASE WHEN line_item_type = 'PART' THEN shop_part_number END) as unique_parts_count,
            MAX(customer_id) as primary_customer_id,
            MAX(customer_title) as primary_customer_title
        FROM fullbay_line_items 
        WHERE unit_vin = target_vin
        GROUP BY unit_vin;
    ELSE
        -- Refresh all vehicles
        DELETE FROM vehicle_summary;
        
        INSERT INTO vehicle_summary (
            unit_vin,
            unit_number,
            unit_year,
            unit_make,
            unit_model,
            unit_type,
            total_invoices,
            first_invoice_date,
            last_invoice_date,
            total_revenue,
            total_parts_revenue,
            total_labor_revenue,
            total_supplies_revenue,
            total_misc_revenue,
            total_sublet_revenue,
            total_labor_hours,
            total_actual_hours,
            total_parts_quantity,
            unique_parts_count,
            primary_customer_id,
            primary_customer_title
        )
        SELECT 
            unit_vin,
            MAX(unit_number) as unit_number,
            MAX(unit_year) as unit_year,
            MAX(unit_make) as unit_make,
            MAX(unit_model) as unit_model,
            MAX(unit_type) as unit_type,
            COUNT(DISTINCT fullbay_invoice_id) as total_invoices,
            MIN(invoice_date) as first_invoice_date,
            MAX(invoice_date) as last_invoice_date,
            SUM(invoice_total) as total_revenue,
            SUM(CASE WHEN line_item_type = 'PART' THEN line_total_price ELSE 0 END) as total_parts_revenue,
            SUM(CASE WHEN line_item_type = 'LABOR' THEN line_total_price ELSE 0 END) as total_labor_revenue,
            SUM(CASE WHEN line_item_type = 'SUPPLY' THEN line_total_price ELSE 0 END) as total_supplies_revenue,
            SUM(CASE WHEN line_item_type = 'MISC' THEN line_total_price ELSE 0 END) as total_misc_revenue,
            SUM(CASE WHEN line_item_type = 'SUBLET' THEN line_total_price ELSE 0 END) as total_sublet_revenue,
            SUM(labor_hours) as total_labor_hours,
            SUM(actual_hours) as total_actual_hours,
            SUM(CASE WHEN line_item_type = 'PART' THEN quantity ELSE 0 END) as total_parts_quantity,
            COUNT(DISTINCT CASE WHEN line_item_type = 'PART' THEN shop_part_number END) as unique_parts_count,
            MAX(customer_id) as primary_customer_id,
            MAX(customer_title) as primary_customer_title
        FROM fullbay_line_items 
        WHERE unit_vin IS NOT NULL
        GROUP BY unit_vin;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create triggers for updated_at columns
CREATE TRIGGER update_fullbay_raw_data_updated_at 
    BEFORE UPDATE ON fullbay_raw_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fullbay_line_items_updated_at 
    BEFORE UPDATE ON fullbay_line_items 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_ingestion_metadata_updated_at 
    BEFORE UPDATE ON ingestion_metadata 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_daily_summary_updated_at 
    BEFORE UPDATE ON daily_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_monthly_summary_updated_at 
    BEFORE UPDATE ON monthly_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_customer_summary_updated_at 
    BEFORE UPDATE ON customer_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_vehicle_summary_updated_at 
    BEFORE UPDATE ON vehicle_summary 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
