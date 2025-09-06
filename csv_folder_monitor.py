#!/usr/bin/env python3
"""
CSV Folder Monitor
Monitors the 'Fullbay Employee Statistics' folder for new CSV files and processes them automatically.
"""

import os
import sys
import time
from pathlib import Path
from csv_schema_detector import CSVSchemaDetector

def monitor_folder(folder_path: str):
    """Monitor a folder for new CSV files."""
    print(f"👀 Monitoring folder: {folder_path}")
    print("📁 Drop CSV files into this folder to process them automatically")
    print("⏹️  Press Ctrl+C to stop monitoring")
    print("=" * 60)
    
    # Get initial list of files
    initial_files = set()
    if os.path.exists(folder_path):
        initial_files = set(os.listdir(folder_path))
    
    try:
        while True:
            # Check for new files
            if os.path.exists(folder_path):
                current_files = set(os.listdir(folder_path))
                new_files = current_files - initial_files
                
                # Process new CSV files
                for file_name in new_files:
                    if file_name.lower().endswith('.csv'):
                        file_path = os.path.join(folder_path, file_name)
                        print(f"\n🆕 New CSV file detected: {file_name}")
                        
                        # Process the file
                        detector = CSVSchemaDetector()
                        try:
                            success = detector.process_csv_file(file_path)
                            if success:
                                print(f"✅ Successfully processed: {file_name}")
                            else:
                                print(f"❌ Failed to process: {file_name}")
                        except Exception as e:
                            print(f"❌ Error processing {file_name}: {e}")
                        finally:
                            detector.close()
                
                initial_files = current_files
            
            # Wait before checking again
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\n⏹️  Monitoring stopped by user")
    except Exception as e:
        print(f"❌ Error in monitoring: {e}")

def main():
    """Main function."""
    folder_path = "Fullbay Employee Statistics"
    
    # Create folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"📁 Created folder: {folder_path}")
    
    # Start monitoring
    monitor_folder(folder_path)

if __name__ == "__main__":
    main()
