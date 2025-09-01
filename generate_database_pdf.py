#!/usr/bin/env python3
"""
Generate a comprehensive PDF that matches exactly to the database schema and data
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
import datetime
import os

def get_database_connection():
    """Get database connection."""
    db_params = {
        'host': 'fullbay-postgres-db.cc74yq2i4j8n.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'fullbay_data',
        'user': 'postgres',
        'password': '5255Tillman'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def get_database_schema():
    """Get the exact database schema."""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'fullbay_line_items' 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        cursor.close()
        return columns
    except Exception as e:
        print(f"❌ Error getting schema: {e}")
        return None
    finally:
        conn.close()

def get_database_statistics():
    """Get database statistics."""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Total rows
        cursor.execute("SELECT COUNT(*) as total_rows FROM fullbay_line_items")
        total_rows = cursor.fetchone()['total_rows']
        
        # April 2025 data
        cursor.execute("""
            SELECT COUNT(*) as april_rows 
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        """)
        april_rows = cursor.fetchone()['april_rows']
        
        # Line item type breakdown
        cursor.execute("""
            SELECT line_item_type, COUNT(*) as count
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
            GROUP BY line_item_type
            ORDER BY count DESC
        """)
        line_item_breakdown = cursor.fetchall()
        
        # Invoice count
        cursor.execute("""
            SELECT COUNT(DISTINCT fullbay_invoice_id) as invoice_count
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        """)
        invoice_count = cursor.fetchone()['invoice_count']
        
        # Customer count
        cursor.execute("""
            SELECT COUNT(DISTINCT customer_id) as customer_count
            FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
        """)
        customer_count = cursor.fetchone()['customer_count']
        
        cursor.close()
        
        return {
            'total_rows': total_rows,
            'april_rows': april_rows,
            'line_item_breakdown': line_item_breakdown,
            'invoice_count': invoice_count,
            'customer_count': customer_count
        }
    except Exception as e:
        print(f"❌ Error getting statistics: {e}")
        return None
    finally:
        conn.close()

def get_sample_data():
    """Get sample data from the database."""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get sample data with all columns
        cursor.execute("""
            SELECT * FROM fullbay_line_items 
            WHERE invoice_date >= '2025-04-01' AND invoice_date <= '2025-04-30'
            ORDER BY invoice_date, fullbay_invoice_id, id
            LIMIT 10
        """)
        
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        cursor.close()
        
        return {
            'rows': rows,
            'columns': columns
        }
    except Exception as e:
        print(f"❌ Error getting sample data: {e}")
        return None
    finally:
        conn.close()

def create_pdf_report():
    """Create the comprehensive PDF report."""
    print("📊 GENERATING COMPREHENSIVE DATABASE PDF REPORT")
    print("=" * 60)
    
    # Get data
    schema = get_database_schema()
    stats = get_database_statistics()
    sample_data = get_sample_data()
    
    if not schema or not stats or not sample_data:
        print("❌ Failed to get required data")
        return
    
    # Create PDF filename
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    pdf_filename = f"fullbay_database_report_{timestamp}.pdf"
    
    print(f"📄 Creating PDF: {pdf_filename}")
    
    # Create PDF document
    doc = SimpleDocTemplate(pdf_filename, pagesize=A4)
    story = []
    
    # Get styles
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=18,
        spaceAfter=30,
        alignment=TA_CENTER,
        textColor=colors.darkblue
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=14,
        spaceAfter=12,
        textColor=colors.darkblue
    )
    
    # Title page
    story.append(Paragraph("FULLBAY API DATABASE REPORT", title_style))
    story.append(Spacer(1, 20))
    
    # Report info
    report_info = f"""
    <b>Report Generated:</b> {datetime.datetime.now().strftime('%B %d, %Y at %I:%M %p')}<br/>
    <b>Database Schema:</b> fullbay_line_items<br/>
    <b>Total Columns:</b> {len(schema)}<br/>
    <b>Total Records:</b> {stats['total_rows']:,}<br/>
    <b>April 2025 Records:</b> {stats['april_rows']:,}
    """
    
    story.append(Paragraph(report_info, styles['Normal']))
    story.append(PageBreak())
    
    # Database Schema Section
    story.append(Paragraph("DATABASE SCHEMA", heading_style))
    story.append(Spacer(1, 12))
    
    # Schema table
    schema_data = [['#', 'Column Name', 'Data Type', 'Nullable', 'Default']]
    
    for col in schema:
        default = col['column_default'] or 'NULL'
        if default.startswith('nextval'):
            default = 'AUTO'
        
        schema_data.append([
            str(col['ordinal_position']),
            col['column_name'],
            col['data_type'],
            'YES' if col['is_nullable'] == 'YES' else 'NO',
            default
        ])
    
    schema_table = Table(schema_data, colWidths=[0.4*inch, 2.2*inch, 1.2*inch, 0.8*inch, 1.2*inch])
    schema_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ALIGN', (0, 0), (0, -1), 'CENTER'),  # Position column
        ('ALIGN', (1, 1), (1, -1), 'LEFT'),    # Column name left-aligned
        ('ALIGN', (2, 1), (2, -1), 'LEFT'),    # Data type left-aligned
        ('ALIGN', (3, 1), (3, -1), 'CENTER'),  # Nullable center-aligned
        ('ALIGN', (4, 1), (4, -1), 'LEFT'),    # Default left-aligned
    ]))
    
    story.append(schema_table)
    story.append(PageBreak())
    
    # Database Statistics Section
    story.append(Paragraph("DATABASE STATISTICS", heading_style))
    story.append(Spacer(1, 12))
    
    # Summary stats
    summary_data = [
        ['Metric', 'Value'],
        ['Total Records in Database', f"{stats['total_rows']:,}"],
        ['April 2025 Records', f"{stats['april_rows']:,}"],
        ['Unique Invoices (April 2025)', f"{stats['invoice_count']:,}"],
        ['Unique Customers (April 2025)', f"{stats['customer_count']:,}"],
        ['Total Columns', f"{len(schema)}"]
    ]
    
    summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
    summary_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
    ]))
    
    story.append(summary_table)
    story.append(Spacer(1, 20))
    
    # Line item breakdown
    story.append(Paragraph("Line Item Type Breakdown (April 2025)", styles['Heading3']))
    story.append(Spacer(1, 12))
    
    breakdown_data = [['Line Item Type', 'Count']]
    for item in stats['line_item_breakdown']:
        breakdown_data.append([item['line_item_type'], str(item['count'])])
    
    breakdown_table = Table(breakdown_data, colWidths=[3*inch, 2*inch])
    breakdown_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkgreen),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightgreen),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
    ]))
    
    story.append(breakdown_table)
    story.append(PageBreak())
    
    # Sample Data Section
    story.append(Paragraph("SAMPLE DATA (First 10 Records - April 2025)", heading_style))
    story.append(Spacer(1, 12))
    
    # Note about sample data
    story.append(Paragraph(
        f"<i>Showing first 10 records from {stats['april_rows']:,} total April 2025 records. "
        f"All {len(schema)} columns are included.</i>", 
        styles['Normal']
    ))
    story.append(Spacer(1, 12))
    
    # Sample data table (first 10 columns for readability)
    sample_columns = sample_data['columns'][:10]  # First 10 columns
    sample_data_table = [sample_columns]  # Header
    
    for row in sample_data['rows']:
        row_data = []
        for col in sample_columns:
            value = row[col]
            if value is None:
                row_data.append('NULL')
            elif isinstance(value, (int, float)):
                row_data.append(str(value))
            else:
                # Truncate long text
                text = str(value)
                if len(text) > 30:
                    text = text[:27] + '...'
                row_data.append(text)
        sample_data_table.append(row_data)
    
    # Create table with appropriate column widths
    col_widths = [0.8*inch] * len(sample_columns)  # Equal widths for sample data
    
    sample_table = Table(sample_data_table, colWidths=col_widths)
    sample_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.darkred),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.lightcoral),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('FONTSIZE', (0, 1), (-1, -1), 6),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.lightcoral, colors.lightpink]),
    ]))
    
    story.append(sample_table)
    story.append(Spacer(1, 20))
    
    # Column information
    story.append(Paragraph("Column Information", styles['Heading3']))
    story.append(Spacer(1, 12))
    
    col_info = f"""
    <b>Total Columns Shown:</b> {len(sample_columns)} (of {len(schema)})<br/>
    <b>Columns Displayed:</b> {', '.join(sample_columns)}<br/>
    <b>Note:</b> Long text values are truncated for display. Full data is available in the database.
    """
    
    story.append(Paragraph(col_info, styles['Normal']))
    
    # Build PDF
    try:
        doc.build(story)
        print(f"✅ PDF generated successfully: {pdf_filename}")
        print(f"📁 File location: {os.path.abspath(pdf_filename)}")
        print(f"📊 PDF contains:")
        print(f"   - Complete database schema ({len(schema)} columns)")
        print(f"   - Database statistics")
        print(f"   - Line item breakdown")
        print(f"   - Sample data preview")
        return pdf_filename
    except Exception as e:
        print(f"❌ Error generating PDF: {e}")
        return None

if __name__ == "__main__":
    pdf_file = create_pdf_report()
    if pdf_file:
        print(f"\n🎉 PDF report generated: {pdf_file}")
    else:
        print("\n❌ Failed to generate PDF report")
