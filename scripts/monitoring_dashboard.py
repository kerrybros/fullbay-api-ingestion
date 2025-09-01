#!/usr/bin/env python3
"""
Fullbay Ingestion Monitoring Dashboard

This script provides real-time monitoring and reporting capabilities for the
Fullbay API data ingestion system. It queries the database views and generates
comprehensive status reports.
"""

import sys
import os
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import argparse

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from config import Config
    from database import DatabaseManager
except ImportError:
    print("⚠️  Warning: Cannot import config and database modules. Running in display-only mode.")
    Config = None
    DatabaseManager = None


class IngestionMonitor:
    """
    Monitor for Fullbay data ingestion system.
    """
    
    def __init__(self, config=None):
        """Initialize the monitor."""
        self.config = config
        self.db_manager = DatabaseManager(config) if config and DatabaseManager else None
        
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status."""
        if not self.db_manager:
            return {"error": "Database manager not available"}
            
        try:
            self.db_manager.connect()
            
            status = {}
            
            # Get ingestion statistics
            stats = self.db_manager.get_ingestion_statistics()
            status['ingestion_stats'] = stats
            
            # Get data quality metrics
            quality = self.db_manager.calculate_data_quality_metrics()
            status['data_quality'] = quality
            
            # Calculate health score
            health_score = self._calculate_health_score(stats, quality)
            status['health_score'] = health_score
            
            # Get recent activity
            recent_activity = self._get_recent_activity()
            status['recent_activity'] = recent_activity
            
            self.db_manager.close()
            return status
            
        except Exception as e:
            return {"error": str(e)}
    
    def _get_recent_activity(self) -> Dict[str, Any]:
        """Get recent activity summary."""
        try:
            with self.db_manager._get_connection() as conn:
                with conn.cursor() as cursor:
                    # Get activity for last 7 days
                    cursor.execute("""
                        SELECT * FROM v_recent_activity 
                        ORDER BY activity_date DESC 
                        LIMIT 7
                    """)
                    activity_data = cursor.fetchall()
                    
                    # Get processing performance
                    cursor.execute("""
                        SELECT * FROM v_ingestion_performance 
                        ORDER BY ingestion_date DESC 
                        LIMIT 7
                    """)
                    performance_data = cursor.fetchall()
                    
                    return {
                        'activity': [dict(row) for row in activity_data],
                        'performance': [dict(row) for row in performance_data]
                    }
        except Exception as e:
            return {"error": str(e)}
    
    def _calculate_health_score(self, stats: Dict[str, Any], quality: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate overall system health score."""
        try:
            health_factors = []
            
            # Processing success rate (40% weight)
            if stats.get('total_raw_records', 0) > 0:
                success_rate = (stats.get('processed_records', 0) / stats['total_raw_records']) * 100
                health_factors.append(('processing_success', success_rate, 0.4))
            else:
                health_factors.append(('processing_success', 100, 0.4))
            
            # Data quality score (30% weight)
            quality_score = quality.get('overall_quality_score', 0)
            health_factors.append(('data_quality', quality_score, 0.3))
            
            # Recent activity score (20% weight) - based on data freshness
            activity_score = 100
            if stats.get('last_ingestion'):
                last_ingestion = stats['last_ingestion']
                if isinstance(last_ingestion, str):
                    last_ingestion = datetime.fromisoformat(last_ingestion.replace('Z', '+00:00'))
                hours_since = (datetime.now(timezone.utc) - last_ingestion).total_seconds() / 3600
                if hours_since > 48:  # No activity for 2+ days
                    activity_score = 0
                elif hours_since > 24:  # No activity for 1+ day
                    activity_score = 50
                elif hours_since > 12:  # No activity for 12+ hours
                    activity_score = 80
            health_factors.append(('recent_activity', activity_score, 0.2))
            
            # Error rate score (10% weight)
            error_score = 100
            if stats.get('error_records', 0) > 0 and stats.get('total_raw_records', 0) > 0:
                error_rate = (stats['error_records'] / stats['total_raw_records']) * 100
                error_score = max(0, 100 - (error_rate * 2))  # Penalize errors heavily
            health_factors.append(('error_rate', error_score, 0.1))
            
            # Calculate weighted average
            total_score = sum(score * weight for _, score, weight in health_factors)
            
            # Determine health status
            if total_score >= 90:
                status = "EXCELLENT"
                color = "🟢"
            elif total_score >= 80:
                status = "GOOD"
                color = "🟡"
            elif total_score >= 60:
                status = "WARNING"
                color = "🟠"
            else:
                status = "CRITICAL"
                color = "🔴"
            
            return {
                'overall_score': round(total_score, 1),
                'status': status,
                'color': color,
                'factors': {name: round(score, 1) for name, score, _ in health_factors}
            }
            
        except Exception as e:
            return {
                'overall_score': 0,
                'status': 'ERROR',
                'color': '❌',
                'error': str(e)
            }
    
    def display_dashboard(self):
        """Display the monitoring dashboard."""
        print("🚀 FULLBAY INGESTION MONITORING DASHBOARD")
        print("=" * 60)
        print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Get system status
        status = self.get_system_status()
        
        if 'error' in status:
            print(f"❌ Error: {status['error']}")
            return
        
        # Display health score
        health = status.get('health_score', {})
        print(f"🏥 SYSTEM HEALTH: {health.get('color', '❓')} {health.get('status', 'UNKNOWN')} "
              f"({health.get('overall_score', 0)}%)")
        print()
        
        # Display ingestion statistics
        stats = status.get('ingestion_stats', {})
        print("📊 INGESTION STATISTICS:")
        print("-" * 30)
        print(f"📥 Total Raw Records: {stats.get('total_raw_records', 0):,}")
        print(f"✅ Processed Records: {stats.get('processed_records', 0):,}")
        print(f"❌ Error Records: {stats.get('error_records', 0):,}")
        print(f"📋 Total Line Items: {stats.get('total_line_items', 0):,}")
        print(f"🏢 Unique Customers: {stats.get('unique_customers', 0):,}")
        print(f"💰 Total Financial Value: ${stats.get('total_financial_value', 0):,.2f}")
        
        # Line item breakdown
        breakdown = stats.get('line_item_breakdown', {})
        if breakdown:
            print(f"\n📋 LINE ITEM BREAKDOWN:")
            for item_type, data in breakdown.items():
                print(f"   {item_type}: {data.get('count', 0):,} items (${data.get('total_value', 0):,.2f})")
        
        print()
        
        # Display data quality
        quality = status.get('data_quality', {})
        print(f"🎯 DATA QUALITY: {quality.get('overall_quality_score', 0):.1f}%")
        print("-" * 30)
        
        quality_issues = [
            ('missing_invoice_numbers', 'Missing Invoice Numbers'),
            ('missing_customer_info', 'Missing Customer Info'),
            ('missing_unit_info', 'Missing Unit Info'),
            ('zero_negative_prices', 'Zero/Negative Prices'),
            ('missing_labor_hours', 'Missing Labor Hours'),
            ('missing_part_numbers', 'Missing Part Numbers')
        ]
        
        for key, label in quality_issues:
            count = quality.get(key, 0)
            if count > 0:
                print(f"⚠️  {label}: {count:,}")
        
        if not any(quality.get(key, 0) > 0 for key, _ in quality_issues):
            print("✅ No data quality issues detected")
        
        print()
        
        # Display recent activity
        activity = status.get('recent_activity', {})
        recent_data = activity.get('activity', [])
        
        if recent_data:
            print("📈 RECENT ACTIVITY (Last 7 Days):")
            print("-" * 40)
            print("Date       | Invoices | Line Items | Total Value")
            print("-" * 40)
            
            for day in recent_data[:7]:
                date_str = str(day.get('activity_date', 'N/A'))[:10]
                invoices = day.get('invoices_processed', 0)
                line_items = day.get('line_items_created', 0)
                total_value = day.get('total_value_processed', 0) or 0
                
                print(f"{date_str} | {invoices:8,} | {line_items:10,} | ${total_value:10,.0f}")
        
        print()
        
        # Display alerts/warnings
        self._display_alerts(status)
    
    def _display_alerts(self, status: Dict[str, Any]):
        """Display system alerts and warnings."""
        alerts = []
        
        # Check for processing errors
        stats = status.get('ingestion_stats', {})
        if stats.get('error_records', 0) > 0:
            error_rate = (stats['error_records'] / max(stats.get('total_raw_records', 1), 1)) * 100
            if error_rate > 5:
                alerts.append(f"🚨 High error rate: {error_rate:.1f}% ({stats['error_records']} errors)")
        
        # Check data quality
        quality = status.get('data_quality', {})
        if quality.get('overall_quality_score', 100) < 90:
            alerts.append(f"⚠️  Data quality below 90%: {quality.get('overall_quality_score', 0):.1f}%")
        
        # Check for stale data
        if stats.get('last_ingestion'):
            try:
                last_ingestion = stats['last_ingestion']
                if isinstance(last_ingestion, str):
                    last_ingestion = datetime.fromisoformat(last_ingestion.replace('Z', '+00:00'))
                hours_since = (datetime.now(timezone.utc) - last_ingestion).total_seconds() / 3600
                
                if hours_since > 48:
                    alerts.append(f"🕒 No data ingestion for {hours_since:.1f} hours")
                elif hours_since > 24:
                    alerts.append(f"⏰ Data ingestion delayed: {hours_since:.1f} hours since last run")
            except:
                pass
        
        # Display alerts
        if alerts:
            print("🚨 ALERTS & WARNINGS:")
            print("-" * 25)
            for alert in alerts:
                print(f"   {alert}")
        else:
            print("✅ NO ALERTS - System operating normally")
        
        print()


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Fullbay Ingestion Monitoring Dashboard')
    parser.add_argument('--json', action='store_true', help='Output in JSON format')
    parser.add_argument('--health-only', action='store_true', help='Show only health status')
    
    args = parser.parse_args()
    
    # Try to load config
    config = None
    if Config and DatabaseManager:
        try:
            config = Config()
        except Exception as e:
            print(f"⚠️  Warning: Could not load config: {e}")
    
    # Create monitor
    monitor = IngestionMonitor(config)
    
    if args.json:
        # JSON output for programmatic use
        status = monitor.get_system_status()
        print(json.dumps(status, indent=2, default=str))
    elif args.health_only:
        # Health status only
        status = monitor.get_system_status()
        health = status.get('health_score', {})
        print(f"{health.get('color', '❓')} {health.get('status', 'UNKNOWN')} ({health.get('overall_score', 0)}%)")
    else:
        # Full dashboard
        monitor.display_dashboard()


if __name__ == "__main__":
    main()