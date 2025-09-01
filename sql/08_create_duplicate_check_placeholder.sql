-- =====================================================
-- Duplicate Invoice Check - PLACEHOLDER
-- =====================================================
-- TODO: Implement duplicate invoice detection logic
-- This will be used to check if an invoice has already been processed
-- before attempting to insert it into the database

-- Placeholder table for tracking processed invoices
CREATE TABLE IF NOT EXISTS processed_invoices_tracker (
    id SERIAL PRIMARY KEY,
    fullbay_invoice_id VARCHAR(50) UNIQUE NOT NULL,
    first_processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    processing_count INTEGER DEFAULT 1,
    status VARCHAR(50) DEFAULT 'PROCESSED', -- 'PROCESSED', 'SKIPPED', 'ERROR'
    skip_reason TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for quick lookup
CREATE INDEX IF NOT EXISTS idx_processed_invoices_tracker_invoice_id ON processed_invoices_tracker(fullbay_invoice_id);
CREATE INDEX IF NOT EXISTS idx_processed_invoices_tracker_status ON processed_invoices_tracker(status);
CREATE INDEX IF NOT EXISTS idx_processed_invoices_tracker_last_processed ON processed_invoices_tracker(last_processed_at);

-- Placeholder function for duplicate check
CREATE OR REPLACE FUNCTION check_invoice_already_processed(p_invoice_id VARCHAR(50))
RETURNS TABLE (
    is_duplicate BOOLEAN,
    last_processed_at TIMESTAMP WITH TIME ZONE,
    processing_count INTEGER,
    skip_reason TEXT
) AS $$
BEGIN
    -- TODO: Implement actual duplicate detection logic
    -- For now, just check if invoice exists in tracker
    RETURN QUERY
    SELECT 
        EXISTS(SELECT 1 FROM processed_invoices_tracker WHERE fullbay_invoice_id = p_invoice_id) as is_duplicate,
        pit.last_processed_at,
        pit.processing_count,
        pit.skip_reason
    FROM processed_invoices_tracker pit
    WHERE pit.fullbay_invoice_id = p_invoice_id;
    
    -- If no record found, return false for duplicate
    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, NULL::TIMESTAMP WITH TIME ZONE, 0, NULL::TEXT;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Placeholder function to mark invoice as processed
CREATE OR REPLACE FUNCTION mark_invoice_processed(
    p_invoice_id VARCHAR(50),
    p_status VARCHAR(50) DEFAULT 'PROCESSED',
    p_skip_reason TEXT DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    tracker_id INTEGER;
BEGIN
    -- TODO: Implement actual processing logic
    -- For now, just insert or update the tracker
    INSERT INTO processed_invoices_tracker (
        fullbay_invoice_id,
        last_processed_at,
        processing_count,
        status,
        skip_reason
    ) VALUES (
        p_invoice_id,
        CURRENT_TIMESTAMP,
        1,
        p_status,
        p_skip_reason
    )
    ON CONFLICT (fullbay_invoice_id) 
    DO UPDATE SET
        last_processed_at = CURRENT_TIMESTAMP,
        processing_count = processed_invoices_tracker.processing_count + 1,
        status = EXCLUDED.status,
        skip_reason = EXCLUDED.skip_reason,
        updated_at = CURRENT_TIMESTAMP
    RETURNING id INTO tracker_id;
    
    RETURN tracker_id;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE processed_invoices_tracker IS 'Tracks processed invoices to prevent duplicates - PLACEHOLDER FOR FUTURE IMPLEMENTATION';
COMMENT ON FUNCTION check_invoice_already_processed IS 'Checks if invoice has already been processed - PLACEHOLDER FOR FUTURE IMPLEMENTATION';
COMMENT ON FUNCTION mark_invoice_processed IS 'Marks invoice as processed - PLACEHOLDER FOR FUTURE IMPLEMENTATION';

-- TODO: Future implementation considerations:
-- 1. Check invoice date vs last processed date
-- 2. Check for data changes in existing records
-- 3. Implement incremental update logic
-- 4. Add business rules for when to reprocess
-- 5. Add audit trail for reprocessing decisions
