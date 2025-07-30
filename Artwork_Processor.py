#!/usr/bin/env python3
"""
🎨 ARTWORK RELEASE DATA PROCESSOR - ENHANCED CLEAN BLUE UI
- Beautiful light blue UI design with Kivy for Mac
- Enhanced data processing with aggressive cleaning
- No duplicate removal (preserves all matching records)
- Dual output: Combined SharePoint data + Final formatted data
- Advanced error handling and flexible date parsing
- Mac-optimized interface with excellent performance
"""

import pandas as pd
import numpy as np
import os
import platform
import threading
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import pickle
import re
import warnings
warnings.filterwarnings('ignore')

# Kivy imports
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.gridlayout import GridLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.progressbar import ProgressBar
from kivy.uix.scrollview import ScrollView
from kivy.uix.textinput import TextInput
from kivy.uix.filechooser import FileChooserIconView
from kivy.uix.popup import Popup
from kivy.clock import Clock
from kivy.core.window import Window
from kivy.logger import Logger
import subprocess

# Clean Blue UI Configuration
Window.size = (1100, 800)
Window.clearcolor = (0.96, 0.98, 1, 1)  # Very light blue background

class EnhancedArtworkDataProcessor:
    def __init__(self):
        # Core data
        self.production_files = []
        self.consolidated_data = pd.DataFrame()
        self.project_tracker_data = pd.DataFrame()
        self.combined_data = pd.DataFrame()
        self.final_output_data = pd.DataFrame()
        
        # Performance settings
        self.max_workers = min(os.cpu_count() * 2, 16)
        
        # Cache system
        self.cache_file = Path.home() / "Desktop" / "processor_cache.json"
        self.data_cache_file = Path.home() / "Desktop" / "data_cache.pkl"
        self.output_folder = Path.home() / "Desktop" / "Automated_Data_Processing_Output"
        self.output_folder.mkdir(exist_ok=True)
        self.file_cache = self.load_cache()
        
        # Configuration
        self.project_tracker_file = ""
        self.start_date_var = ""
        self.end_date_var = ""
        self.processing_logs = []
        
        # Setup paths
        self.setup_paths()
        
        # Enhanced target columns matching the current version
        self.target_columns = ['Item Number', 'Product Vendor Company Name', 'Brand', 'Product Name', 'SKU New/Existing']
        
        # Final output column order with renamed headers (from current version)
        self.final_columns = [
            'HUGO ID', 'Product Vendor Company Name', 'Item Number', 'Product Name', 'Brand', 'SKU', 
            'Artwork Release Date', '5 Weeks After Artwork Release', 'Entered into HUGO Date', 
            'Entered in HUGO?', 'Store Date', 'Re-Release Status', 'Packaging Format 1', 
            'Printer Company Name 1', 'Vendor e-mail 1', 'Printer e-mail 1', 
            'Printer Code 1 (LW Code)', 'File Name'
        ]
        
        Logger.info("🎨 Enhanced Artwork Release Data Processor initialized")
    
    def setup_paths(self):
        """Setup Mac-optimized paths with SharePoint detection"""
        is_mac = platform.system() == 'Darwin'
        
        if is_mac:
            base_path = Path.home() / "Lowe's Companies Inc"
        else:
            base_path = Path("C:/Users") / os.getenv('USERNAME', 'mjayash') / "Lowe's Companies Inc"
        
        # SharePoint paths (matching current version)
        self.sharepoint_paths = [
            base_path / "Private Brands - Packaging Operations - Building Products",
            base_path / "Private Brands - Packaging Operations - Hardlines & Seasonal", 
            base_path / "Private Brands - Packaging Operations - Home Décor"
        ]
        
        # Default project tracker path
        self.default_project_tracker_path = base_path / "Private Brands Packaging File Transfer - PQM Compliance reporting" / "Project tracker.xlsx"
    
    def check_sharepoint_access(self):
        """Check if user has access to SharePoint directories"""
        try:
            for path in self.sharepoint_paths:
                if path.exists():
                    return True
            return False
        except Exception:
            return False
    
    def load_cache(self):
        """Load processing cache"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            return {}
        except:
            return {}
    
    def save_cache(self):
        """Save processing cache"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.file_cache, f, indent=2)
        except Exception as e:
            Logger.error(f"Cache save error: {e}")
    
    def get_file_hash(self, file_path):
        """Fast file change detection"""
        try:
            stat = os.stat(file_path)
            return f"{stat.st_size}_{int(stat.st_mtime)}"
        except:
            return None
    
    def log_message(self, message):
        """Store log messages"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.processing_logs.append(formatted_message)
    
    def load_date_ranges(self, callback=None):
        """Enhanced date range loading with flexible parsing"""
        if not self.project_tracker_file:
            return
        
        try:
            if callback:
                Clock.schedule_once(lambda dt: callback("📅 Loading date ranges..."), 0)
            
            df = pd.read_excel(self.project_tracker_file, nrows=1000)
            
            # Enhanced date column search
            release_col = None
            possible_date_columns = [
                'artwork release date', 'release date', 'releasedate', 
                'date', 'artwork date', 'artworkreleasedate'
            ]
            
            for col in df.columns:
                col_lower = str(col).lower().replace(' ', '').replace('_', '')
                for possible_name in possible_date_columns:
                    if possible_name.replace(' ', '') in col_lower:
                        release_col = col
                        break
                if release_col:
                    break
            
            if not release_col:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ Date column not found"), 0)
                return
            
            # Enhanced date parsing
            dates = pd.to_datetime(df[release_col], errors='coerce', dayfirst=True).dropna()
            if len(dates) == 0:
                return
            
            min_date = dates.min().date().strftime("%Y-%m-%d")
            max_date = dates.max().date().strftime("%Y-%m-%d")
            
            self.start_date_var = min_date
            self.end_date_var = max_date
            
            if callback:
                Clock.schedule_once(lambda dt: callback(f"📅 Range: {min_date} to {max_date}"), 0)
                
        except Exception as e:
            if callback:
                Clock.schedule_once(lambda dt: callback(f"❌ Date error: {e}"), 0)
    
    def scan_production_folders(self, callback=None):
        """Enhanced production folder scanning"""
        start_time = time.time()
        
        if callback:
            Clock.schedule_once(lambda dt: callback("🔍 Scanning production folders..."), 0)
        
        all_files = []
        
        def scan_path(sp_path):
            """Optimized path scanning"""
            files = []
            if not sp_path.exists():
                return []
            
            try:
                for root, dirs, filenames in os.walk(sp_path):
                    if root.endswith("_Production Item List"):
                        excel_files = [
                            os.path.join(root, f) for f in filenames
                            if f.lower().endswith(('.xlsx', '.xls', '.xlsm'))
                            and not f.startswith(('~', '.', '$'))
                        ]
                        files.extend(excel_files)
            except Exception as e:
                self.log_message(f"Error scanning {sp_path}: {str(e)}")
            
            return files
        
        # Parallel scanning
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(scan_path, path) for path in self.sharepoint_paths]
            
            for future in as_completed(futures):
                path_files = future.result()
                all_files.extend(path_files)
        
        self.production_files = all_files
        scan_time = time.time() - start_time
        
        if callback:
            Clock.schedule_once(
                lambda dt: callback(f"✅ Found {len(all_files)} production files in {scan_time:.2f}s"), 0
            )
        
        self.log_message(f"Found {len(all_files)} production files")
        return len(all_files) > 0
    
    def intelligent_data_extraction(self, callback=None):
        """Enhanced data extraction with aggressive cleaning"""
        if callback:
            Clock.schedule_once(lambda dt: callback("⚡ Extracting production data..."), 0)
        
        self.log_message("Extracting production data...")
        
        # Enhanced column patterns (from current version)
        column_patterns = {
            'Item Number': ['item #', 'item#', 'itemnumber', 'item number', 'item no', 'itemno'],
            'Product Vendor Company Name': ['vendor name', 'vendorname', 'vendor', 'supplier'],
            'Brand': ['brand', 'brandname', 'brand name'],
            'Product Name': ['item description', 'itemdescription', 'description', 'product description', 'desc', 'product name'],
            'SKU New/Existing': ['SKU', 'SKU new/existing', 'SKU new existing', 'SKU new/carry forward', 'SKU new carry forward', 'SKU new']
        }
        
        def extract_from_file(file_path):
            """Enhanced file extraction with aggressive item number cleaning"""
            try:
                df = pd.read_excel(file_path, header=None, nrows=1000)
                if df.empty:
                    return pd.DataFrame()
                
                best_extraction = pd.DataFrame()
                best_score = 0
                
                for potential_header_row in range(min(50, len(df))):
                    try:
                        potential_headers = df.iloc[potential_header_row].astype(str).str.lower().str.strip()
                        
                        # Handle multi-line headers
                        combined_headers = potential_headers.copy()
                        if potential_header_row + 1 < len(df):
                            next_row_headers = df.iloc[potential_header_row + 1].astype(str).str.lower().str.strip()
                            combined_headers = potential_headers + " " + next_row_headers
                            combined_headers = combined_headers.str.replace(r'\s+', ' ', regex=True).str.strip()
                        
                        column_mapping = {}
                        score = 0
                        
                        for target_col, search_patterns in column_patterns.items():
                            for col_idx, header in enumerate(combined_headers):
                                if pd.isna(header) or header == '' or header == 'nan' or 'nan nan' in header:
                                    continue
                                
                                clean_header = re.sub(r'[^a-z0-9]', '', header.strip().lower())
                                
                                for pattern in search_patterns:
                                    clean_pattern = re.sub(r'[^a-z0-9]', '', pattern.lower())
                                    if clean_pattern in clean_header:
                                        column_mapping[target_col] = col_idx
                                        score += 1
                                        break
                                
                                if target_col in column_mapping:
                                    break
                        
                        if score >= 2:
                            try:
                                full_df = pd.read_excel(file_path, header=potential_header_row, nrows=10000)
                                
                                if not full_df.empty and len(full_df.columns) > max(column_mapping.values()):
                                    extracted_data = pd.DataFrame()
                                    
                                    for target_col in self.target_columns:
                                        if target_col in column_mapping:
                                            col_idx = column_mapping[target_col]
                                            if col_idx < len(full_df.columns):
                                                source_col_name = full_df.columns[col_idx]
                                                extracted_data[target_col] = full_df[source_col_name].astype(str).str.strip()
                                        else:
                                            extracted_data[target_col] = ''
                                    
                                    # ENHANCED Item Number cleaning (from current version)
                                    if 'Item Number' in extracted_data.columns:
                                        def clean_item_number_aggressive(value):
                                            try:
                                                if pd.isna(value):
                                                    return ''
                                                
                                                # Convert to string and remove ALL whitespace (including internal)
                                                clean_val = str(value).replace(' ', '').replace('\t', '').replace('\n', '').replace('\r', '').strip()
                                                
                                                # Handle common non-values
                                                if clean_val.lower() in ['nan', 'none', 'null', '']:
                                                    return ''
                                                
                                                # Handle Excel scientific notation
                                                if 'e+' in clean_val.lower() or 'e-' in clean_val.lower():
                                                    try:
                                                        float_val = float(clean_val)
                                                        clean_val = f"{float_val:.0f}"
                                                    except:
                                                        pass
                                                
                                                # Remove decimal points and everything after
                                                if '.' in clean_val:
                                                    clean_val = clean_val.split('.')[0]
                                                
                                                # Extract only digits
                                                numbers_only = re.sub(r'[^\d]', '', clean_val)
                                                
                                                # Convert to integer and back to ensure clean format
                                                if numbers_only and numbers_only.isdigit() and len(numbers_only) > 0:
                                                    return str(int(numbers_only))
                                                
                                                return ''
                                            except Exception as e:
                                                return ''
                                        
                                        # Apply aggressive cleaning to Item Number
                                        extracted_data['Item Number'] = extracted_data['Item Number'].apply(clean_item_number_aggressive)
                                        
                                        # CRITICAL: Only keep rows with valid Item Number (never empty)
                                        extracted_data = extracted_data[
                                            (extracted_data['Item Number'] != '') & 
                                            (extracted_data['Item Number'] != 0) &
                                            (extracted_data['Item Number'].notna())
                                        ]
                                        
                                        # Ensure string format for consistency
                                        extracted_data['Item Number'] = extracted_data['Item Number'].astype(str)
                                    
                                    # Only keep rows with valid Item Number
                                    if 'Item Number' in extracted_data.columns:
                                        valid_items = extracted_data['Item Number'] != ''
                                        extracted_data = extracted_data[valid_items]
                                    
                                    if len(extracted_data) > 0:
                                        file_name = os.path.basename(file_path)
                                        extracted_data['Source_File'] = file_name
                                        extracted_data['Source_Folder'] = os.path.basename(os.path.dirname(file_path))
                                        
                                        if score > best_score or len(extracted_data) > len(best_extraction):
                                            best_extraction = extracted_data.copy()
                                            best_score = score
                            
                            except Exception:
                                continue
                    
                    except Exception:
                        continue
                
                return best_extraction
                
            except Exception:
                return pd.DataFrame()
        
        # Process files in parallel
        all_extracted_data = []
        successful = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(extract_from_file, file_path) for file_path in self.production_files]
            
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                if not result.empty:
                    all_extracted_data.append(result)
                    successful += 1
                
                if (i + 1) % 20 == 0 and callback:
                    progress = (i + 1) / len(self.production_files) * 100
                    Clock.schedule_once(
                        lambda dt: callback(f"⚡ Processing: {i + 1}/{len(self.production_files)} ({progress:.0f}%)"), 0
                    )
        
        # Consolidate data with COMPREHENSIVE CLEANING (from current version)
        if all_extracted_data:
            self.consolidated_data = pd.concat(all_extracted_data, ignore_index=True)
            self.consolidated_data = self.consolidated_data.drop_duplicates(subset=['Item Number', 'Source_File'], keep='first')
            
            # COMPREHENSIVE DATA CLEANING FOR COMBINED OUTPUT
            self.log_message("Cleaning and trimming consolidated data...")
            
            # Clean and trim all text columns
            text_columns = ['Product Vendor Company Name', 'Brand', 'Product Name', 'SKU New/Existing', 'Source_File', 'Source_Folder']
            for col in text_columns:
                if col in self.consolidated_data.columns:
                    self.consolidated_data[col] = self.consolidated_data[col].astype(str).str.strip()
                    # Remove extra internal spaces
                    self.consolidated_data[col] = self.consolidated_data[col].str.replace(r'\s+', ' ', regex=True)
                    # Replace 'nan' strings with empty strings
                    self.consolidated_data[col] = self.consolidated_data[col].replace(['nan', 'None', 'NaN'], '')
            
            # Special cleaning for Item Number with comprehensive space removal
            if 'Item Number' in self.consolidated_data.columns:
                def clean_item_number_comprehensive(value):
                    try:
                        if pd.isna(value):
                            return ''
                        
                        # Convert to string and strip all whitespace
                        clean_val = str(value).strip()
                        
                        # Handle common non-values
                        if clean_val.lower() in ['nan', 'none', 'null', '']:
                            return ''
                        
                        # Remove ALL spaces (including internal ones) and non-digit characters
                        numbers_only = re.sub(r'[^\d]', '', clean_val)
                        
                        # Convert to integer and back to string to ensure clean format
                        if numbers_only and numbers_only.isdigit() and len(numbers_only) > 0:
                            return str(int(numbers_only))
                        
                        return ''
                    except:
                        return ''
                
                # Apply comprehensive cleaning to Item Number
                self.consolidated_data['Item Number'] = self.consolidated_data['Item Number'].apply(clean_item_number_comprehensive)
                
                # Log before/after counts for debugging
                before_count = len(self.consolidated_data)
                
                # Remove rows with empty Item Numbers
                self.consolidated_data = self.consolidated_data[
                    (self.consolidated_data['Item Number'] != '') & 
                    (self.consolidated_data['Item Number'].notna())
                ]
                
                after_count = len(self.consolidated_data)
                self.log_message(f"Item Number cleaning: {before_count} -> {after_count} records (removed {before_count - after_count} empty/invalid)")
            
            # Final data quality check
            self.consolidated_data = self.consolidated_data.fillna('')
            
            self.log_message(f"Extracted and cleaned {len(self.consolidated_data)} records with valid Item Numbers")
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"✅ Extracted from {successful}/{len(self.production_files)} files"), 0
                )
            
            return True
        else:
            self.log_message("No data extracted")
            return False
    
    def process_project_tracker(self, callback=None):
        """Enhanced project tracker processing with flexible column detection"""
        try:
            if not self.project_tracker_file or not os.path.exists(self.project_tracker_file):
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ Project tracker file not found"), 0)
                return False
            
            if callback:
                Clock.schedule_once(lambda dt: callback("📋 Processing project tracker..."), 0)
            
            self.log_message("Processing project tracker...")
            
            df = pd.read_excel(self.project_tracker_file)
            
            def find_column(df, possible_names):
                df_cols_lower = [col.lower() for col in df.columns]
                for name in possible_names:
                    name_lower = name.lower()
                    for i, col in enumerate(df_cols_lower):
                        if name_lower in col or col in name_lower:
                            return df.columns[i]
                return None
            
            # Enhanced column mappings (from current version)
            column_mappings = {
                'HUGO ID': ['PKG3'],
                'File Name': ['File Name', 'FileName', 'Name'],
                'Rounds': ['Rounds', 'Round'],
                'Printer Company Name 1': ['PAComments', 'PA Comments', 'Comments'],
                'Vendor e-mail 1': ['VendorEmail', 'Vendor Email', 'VendorE-mail'],
                'Printer e-mail 1': ['PrinterEmail', 'Printer Email', 'PrinterE-mail'],
                'PKG1': ['PKG1'],
                'Artwork Release Date': ['ReleaseDate', 'Release Date'],
                '5 Weeks After Artwork Release': ['5 Weeks After Artwork Release', '5 weeks after artwork release'],
                'Entered into HUGO Date': ['entered into HUGO Date', 'Entered into HUGO Date'],
                'Entered in HUGO?': ['Entered in HUGO?', 'entered in HUGO?'],
                'Store Date': ['Store Date', 'store date'],
                'Packaging Format 1': ['Packaging Format 1', 'packaging format 1'],
                'Printer Code 1 (LW Code)': ['Printer Code 1 (LW Code)', 'printer code 1 (LW Code)']
            }
            
            # Find columns
            found_columns = {}
            for target_name, possible_names in column_mappings.items():
                found_col = find_column(df, possible_names)
                if found_col:
                    found_columns[target_name] = found_col
            
            if 'Rounds' not in found_columns:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ Rounds column not found"), 0)
                return False
            
            # Filter data
            rounds_col = found_columns['Rounds']
            filter_values = ["File Release", "File Re-Release R2", "File Re-Release R3"]
            mask = df[rounds_col].isin(filter_values)
            filtered_df = df[mask].copy()
            
            if len(filtered_df) == 0:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ No matching records found"), 0)
                return False
            
            # Create result dataframe
            result = pd.DataFrame(index=filtered_df.index)
            
            # Map all columns
            for target_name, source_col in found_columns.items():
                if target_name == 'Artwork Release Date':
                    # Special date formatting
                    release_dates = filtered_df[source_col]
                    date_mask = pd.notna(release_dates) & (release_dates != "")
                    result[target_name] = ""
                    if date_mask.any():
                        valid_dates = pd.to_datetime(release_dates[date_mask], errors='coerce')
                        formatted_dates = valid_dates.dt.strftime("%d/%m/%y")
                        result.loc[date_mask, target_name] = formatted_dates
                else:
                    result[target_name] = filtered_df[source_col].fillna("")
            
            # Calculate Re-Release Status with empty cells for "No" (from current version)
            rounds_upper = filtered_df[found_columns['Rounds']].astype(str).str.upper()
            re_release_status = np.where(
                rounds_upper.str.contains('R2|R3', na=False, regex=True), 
                'Yes', 
                ''  # EMPTY instead of "No" as requested
            )
            result['Re-Release Status'] = re_release_status
            
            self.project_tracker_data = result
            self.log_message(f"Processed {len(result)} project tracker records")
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"✅ Project tracker: {len(result)} records processed"), 0
                )
            
            return True
            
        except Exception as e:
            self.log_message(f"Project tracker error: {str(e)}")
            if callback:
                Clock.schedule_once(lambda dt: callback(f"❌ Project tracker error: {e}"), 0)
            return False
    
    def combine_datasets(self, callback=None):
        """Enhanced combination with NO DUPLICATE REMOVAL (from current version)"""
        try:
            if callback:
                Clock.schedule_once(lambda dt: callback("🔗 Combining datasets..."), 0)
            
            self.log_message("Combining datasets...")
            
            if self.consolidated_data.empty or self.project_tracker_data.empty:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ Missing data for combination"), 0)
                return False
            
            step1_data = self.consolidated_data.copy()
            step2_data = self.project_tracker_data.copy()
            
            # Enhanced number cleaning (from current version)
            def clean_to_number(value):
                try:
                    if pd.isna(value) or str(value).strip() == '' or str(value).lower() in ['nan', 'none', 'null']:
                        return ''
                    
                    clean_val = str(value).strip()
                    
                    # Handle Excel scientific notation
                    if 'e+' in clean_val.lower() or 'e-' in clean_val.lower():
                        try:
                            float_val = float(clean_val)
                            clean_val = f"{float_val:.0f}"
                        except:
                            pass
                    
                    # Remove decimal points
                    if '.' in clean_val:
                        clean_val = clean_val.split('.')[0]
                    
                    # Remove non-digits
                    numbers_only = re.sub(r'[^\d]', '', clean_val)
                    
                    if numbers_only and numbers_only.isdigit():
                        return str(int(numbers_only))
                    
                    return ''
                except:
                    return ''
            
            # Clean merge keys
            step1_data['Merge_Key'] = step1_data['Item Number'].apply(clean_to_number)
            step2_data['Merge_Key'] = step2_data['PKG1'].apply(clean_to_number)
            
            # Remove empty keys - BUT NO DUPLICATE REMOVAL (from current version)
            step1_valid = step1_data[step1_data['Merge_Key'] != ''].copy()
            step2_valid = step2_data[step2_data['Merge_Key'] != ''].copy()
            
            # REMOVED: Duplicate removal lines that were here previously
            # step1_valid = step1_valid.drop_duplicates(subset=['Merge_Key'], keep='first')
            # step2_valid = step2_valid.drop_duplicates(subset=['Merge_Key'], keep='first')
            
            # Merge datasets
            combined = pd.merge(step1_valid, step2_valid, on='Merge_Key', how='outer', indicator=True)
            
            # Add data source indicators
            combined['Data_Source'] = combined['_merge'].map({
                'both': 'Step1 + Step2',
                'left_only': 'Step1 Only',
                'right_only': 'Step2 Only'
            })
            
            if '_merge' in combined.columns:
                combined = combined.drop(columns=['_merge'])
            
            self.combined_data = combined
            
            matched_count = len(combined[combined['Data_Source'] == 'Step1 + Step2'])
            self.log_message(f"Combined datasets: {len(combined)} total, {matched_count} matched")
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"✅ Combined data ready: {len(combined)} records, {matched_count} matched"), 0
                )
            
            return True
            
        except Exception as e:
            self.log_message(f"Combination error: {str(e)}")
            if callback:
                Clock.schedule_once(lambda dt: callback(f"❌ Combination error: {e}"), 0)
            return False
    
    def filter_by_date_range(self, start_date, end_date, callback=None):
        """Enhanced date filtering with flexible parsing (from current version)"""
        try:
            if callback:
                Clock.schedule_once(lambda dt: callback("📅 Filtering by date range..."), 0)
            
            self.log_message(f"Filtering by date range: {start_date} to {end_date}")
            
            if self.combined_data.empty:
                self.log_message("Combined data is empty - cannot filter by date")
                return False
            
            self.log_message(f"Combined data has {len(self.combined_data)} records before date filtering")
            
            # Enhanced date column search with multiple possible names (from current version)
            date_column = None
            possible_date_columns = [
                'artwork release date', 'release date', 'releasedate', 
                'date', 'artwork date', 'artworkreleasedate'
            ]
            
            # First try exact match
            for col in self.combined_data.columns:
                for possible_name in possible_date_columns:
                    if possible_name.lower() in col.lower().replace(' ', '').replace('_', ''):
                        date_column = col
                        break
                if date_column:
                    break
            
            if not date_column:
                self.log_message(f"No date column found. Available columns: {list(self.combined_data.columns)}")
                # Try to find any column with 'date' in the name
                for col in self.combined_data.columns:
                    if 'date' in col.lower():
                        date_column = col
                        self.log_message(f"Using fallback date column: {date_column}")
                        break
            
            if not date_column:
                self.log_message("ERROR: No date column found at all - skipping date filter")
                # Don't fail completely - just return the data without date filtering
                self.log_message("Proceeding without date filtering...")
                return True
            
            self.log_message(f"Using date column: '{date_column}'")
            filtered_df = self.combined_data.copy()
            
            # Enhanced date parsing function (from current version)
            def parse_date_enhanced(date_val):
                try:
                    if pd.isna(date_val) or str(date_val).strip() == '' or str(date_val).lower() in ['nan', 'none', 'nat', 'null']:
                        return None
                    
                    # Handle string dates
                    date_str = str(date_val).strip()
                    
                    # Try multiple date formats
                    date_formats = [
                        '%d/%m/%y', '%d/%m/%Y',  # DD/MM/YY, DD/MM/YYYY
                        '%m/%d/%y', '%m/%d/%Y',  # MM/DD/YY, MM/DD/YYYY  
                        '%Y-%m-%d', '%Y/%m/%d',  # YYYY-MM-DD, YYYY/MM/DD
                        '%d-%m-%Y', '%d-%m-%y',  # DD-MM-YYYY, DD-MM-YY
                        '%Y%m%d'                 # YYYYMMDD
                    ]
                    
                    # Try each format
                    for fmt in date_formats:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt).date()
                            return parsed_date
                        except ValueError:
                            continue
                    
                    # Try pandas to_datetime as fallback
                    try:
                        parsed = pd.to_datetime(date_val, errors='coerce', dayfirst=True)
                        return parsed.date() if pd.notna(parsed) else None
                    except:
                        pass
                    
                    return None
                    
                except Exception as e:
                    return None
            
            # Apply enhanced date parsing
            filtered_df['Parsed_Date'] = filtered_df[date_column].apply(parse_date_enhanced)
            
            # Log parsing results
            total_records = len(filtered_df)
            valid_dates = filtered_df['Parsed_Date'].notna().sum()
            self.log_message(f"Date parsing results: {valid_dates}/{total_records} valid dates found")
            
            if valid_dates == 0:
                self.log_message("WARNING: No valid dates found after parsing - proceeding without date filter")
                if 'Parsed_Date' in filtered_df.columns:
                    filtered_df = filtered_df.drop(columns=['Parsed_Date'])
                self.combined_data = filtered_df
                return True
            
            # Sample some parsed dates for debugging
            sample_dates = filtered_df[filtered_df['Parsed_Date'].notna()]['Parsed_Date'].head(5).tolist()
            self.log_message(f"Sample parsed dates: {sample_dates}")
            
            # Convert string dates to date objects
            try:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            except:
                self.log_message("Error parsing input dates - proceeding without filter")
                return True
            
            # Apply date filter
            mask = (
                filtered_df['Parsed_Date'].notna() & 
                (filtered_df['Parsed_Date'] >= start_date_obj) & 
                (filtered_df['Parsed_Date'] <= end_date_obj)
            )
            
            filtered_df = filtered_df[mask].copy()
            
            # Remove temporary column
            if 'Parsed_Date' in filtered_df.columns:
                filtered_df = filtered_df.drop(columns=['Parsed_Date'])
            
            self.combined_data = filtered_df
            
            self.log_message(f"Date filtering complete: {len(filtered_df)} records remain")
            
            # If no records after filtering, it's still a success but warn the user
            if len(filtered_df) == 0:
                self.log_message(f"WARNING: No records found in date range {start_date} to {end_date}")
                self.log_message("This may be normal if no data exists for this date range")
                return True  # Don't fail, just return empty result
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"✅ Date filtering complete: {len(filtered_df)} records"), 0
                )
            
            return True
            
        except Exception as e:
            self.log_message(f"Date filtering error: {str(e)}")
            # Don't fail completely - just proceed without date filtering
            self.log_message("Proceeding without date filtering due to error...")
            return True
    
    def format_final_output(self, callback=None):
        """Enhanced final output formatting (from current version)"""
        try:
            if callback:
                Clock.schedule_once(lambda dt: callback("📋 Formatting final output..."), 0)
            
            self.log_message("Formatting final output...")
            
            if self.combined_data.empty:
                self.log_message("Combined data is empty - creating empty final output")
                # Create empty final output with correct structure
                self.final_output_data = pd.DataFrame(columns=self.final_columns)
                return True
            
            # Create final output dataframe
            final_df = pd.DataFrame()
            
            # Column mapping from combined data to final output (with renamed columns from current version)
            column_mapping = {
                'HUGO ID': 'HUGO ID',
                'Product Vendor Company Name': 'Product Vendor Company Name',  # Renamed from Vendor Name
                'Item Number': 'Item Number',  # Renamed from Item #
                'Product Name': 'Product Name',  # Renamed from Item Description
                'Brand': 'Brand',
                'SKU': 'SKU New/Existing',  # Renamed
                'Artwork Release Date': 'Artwork Release Date',
                '5 Weeks After Artwork Release': '5 Weeks After Artwork Release',
                'Entered into HUGO Date': 'Entered into HUGO Date',
                'Entered in HUGO?': 'Entered in HUGO?',
                'Store Date': 'Store Date',
                'Re-Release Status': 'Re-Release Status',
                'Packaging Format 1': 'Packaging Format 1',
                'Printer Company Name 1': 'Printer Company Name 1',
                'Vendor e-mail 1': 'Vendor e-mail 1',
                'Printer e-mail 1': 'Printer e-mail 1',
                'Printer Code 1 (LW Code)': 'Printer Code 1 (LW Code)',
                'File Name': 'File Name'
            }
            
            # Extract columns in exact order
            for final_col in self.final_columns:
                if final_col in column_mapping:
                    source_col = column_mapping[final_col]
                    if source_col in self.combined_data.columns:
                        final_df[final_col] = self.combined_data[source_col]
                    else:
                        final_df[final_col] = ''
                else:
                    final_df[final_col] = ''
            
            # Clean up the data
            final_df = final_df.fillna('')
            
            # CRITICAL: Only keep records with valid Item Number (never empty) if we have data
            if len(final_df) > 0:
                valid_mask = (final_df['Item Number'].astype(str).str.strip() != '') & (final_df['Item Number'].astype(str).str.strip() != 'nan')
                final_df = final_df[valid_mask]
            
            self.final_output_data = final_df
            
            self.log_message(f"Final formatting complete: {len(final_df)} records")
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"✅ Final formatting complete: {len(final_df)} records"), 0
                )
            
            return True
            
        except Exception as e:
            self.log_message(f"Formatting error: {str(e)}")
            # Create empty final output as fallback
            self.final_output_data = pd.DataFrame(columns=self.final_columns)
            return True  # Don't fail completely
    
    def save_dual_files(self, callback=None):
        """Enhanced dual file saving (from current version)"""
        try:
            if callback:
                Clock.schedule_once(lambda dt: callback("💾 Saving output files..."), 0)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            date_range_str = f"{self.start_date_var.replace('-', '')}_to_{self.end_date_var.replace('-', '')}"
            
            output_files = []
            
            # Save Combined Data from SharePoint (consolidated production files)
            if not self.consolidated_data.empty:
                combined_file = self.output_folder / f"Combined_Data_{date_range_str}_{timestamp}.xlsx"
                
                with pd.ExcelWriter(combined_file, engine='xlsxwriter') as writer:
                    # Combined data sheet from SharePoint scanning
                    self.consolidated_data.to_excel(writer, sheet_name='Combined Data', index=False)
                    
                    # Summary sheet for combined data
                    combined_summary_data = {
                        'Metric': [
                            'Total Combined Records',
                            'Date Range Start',
                            'Date Range End',
                            'Processing Date',
                            'Total Production Files Scanned',
                            'Records with Item Number',
                            'Unique Source Folders',
                            'Status'
                        ],
                        'Value': [
                            len(self.consolidated_data),
                            self.start_date_var,
                            self.end_date_var,
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            len(self.production_files),
                            len(self.consolidated_data[self.consolidated_data['Item Number'].astype(str).str.strip() != '']),
                            len(self.consolidated_data['Source_Folder'].unique()) if 'Source_Folder' in self.consolidated_data.columns else 0,
                            'SUCCESS - Data extracted from SharePoint'
                        ]
                    }
                    
                    combined_summary_df = pd.DataFrame(combined_summary_data)
                    combined_summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Source Files sheet
                    if 'Source_Folder' in self.consolidated_data.columns and len(self.consolidated_data) > 0:
                        source_summary = self.consolidated_data.groupby(['Source_Folder', 'Source_File']).size().reset_index(name='Record_Count')
                        source_summary.to_excel(writer, sheet_name='Source Files', index=False)
                    
                    # Format sheets
                    workbook = writer.book
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#E0E0E0',
                        'font_color': '#000000',
                        'align': 'center'
                    })
                    
                    # Format combined data sheet
                    worksheet = writer.sheets['Combined Data']
                    for col_num, col_name in enumerate(self.consolidated_data.columns):
                        worksheet.write(0, col_num, col_name, header_format)
                        if 'name' in col_name.lower() or 'description' in col_name.lower():
                            worksheet.set_column(col_num, col_num, 25)
                        elif 'date' in col_name.lower():
                            worksheet.set_column(col_num, col_num, 15)
                        else:
                            worksheet.set_column(col_num, col_num, 12)
                
                output_files.append(str(combined_file))
                self.log_message(f"SharePoint combined data saved: {combined_file.name}")
            else:
                self.log_message("No SharePoint combined data to save (empty dataset)")

            # Save final formatted output (main file) - even if empty
            final_file = self.output_folder / f"Final_Output_{date_range_str}_{timestamp}.xlsx"
            
            with pd.ExcelWriter(final_file, engine='xlsxwriter') as writer:
                # Main data sheet
                self.final_output_data.to_excel(writer, sheet_name='Final Data', index=False)
                
                # Summary sheet
                summary_data = {
                    'Metric': [
                        'Total Final Records',
                        'Date Range Start',
                        'Date Range End',
                        'Total Columns',
                        'Processing Date',
                        'Project Tracker File',
                        'Records with Item Number',
                        'Records with HUGO ID',
                        'Status'
                    ],
                    'Value': [
                        len(self.final_output_data),
                        self.start_date_var,
                        self.end_date_var,
                        len(self.final_columns),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        Path(self.project_tracker_file).name if self.project_tracker_file else '',
                        len(self.final_output_data[self.final_output_data['Item Number'].astype(str).str.strip() != '']) if len(self.final_output_data) > 0 else 0,
                        len(self.final_output_data[self.final_output_data['HUGO ID'].astype(str).str.strip() != '']) if len(self.final_output_data) > 0 else 0,
                        'SUCCESS - Final data processed' if len(self.final_output_data) > 0 else 'SUCCESS - No records in date range'
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Format sheets
                workbook = writer.book
                header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#E0E0E0',
                    'font_color': '#000000',
                    'align': 'center'
                })
                
                # Format main sheet
                worksheet = writer.sheets['Final Data']
                for col_num, value in enumerate(self.final_columns):
                    worksheet.write(0, col_num, value, header_format)
                    if 'name' in value.lower() or 'description' in value.lower():
                        worksheet.set_column(col_num, col_num, 25)
                    elif 'date' in value.lower():
                        worksheet.set_column(col_num, col_num, 15)
                    else:
                        worksheet.set_column(col_num, col_num, 12)
            
            output_files.append(str(final_file))
            self.log_message(f"Final output saved: {final_file.name}")
            
            self.log_message(f"Total files saved: {len(output_files)}")
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"💾 Saved 2 files: {Path(output_files[0]).name} & {Path(output_files[1]).name}"), 0
                )
            
            return output_files
            
        except Exception as e:
            self.log_message(f"Save error: {str(e)}")
            if callback:
                Clock.schedule_once(lambda dt: callback(f"❌ Save error: {e}"), 0)
            return []

class EnhancedArtworkReleaseApp(App):
    def build(self):
        self.title = "Artwork Release Data"
        
        # Initialize enhanced processor
        self.processor = EnhancedArtworkDataProcessor()
        
        # Check SharePoint access
        if not self.processor.check_sharepoint_access():
            Clock.schedule_once(self.show_sharepoint_warning, 0.5)
        
        # Main layout with clean blue theme
        main_layout = BoxLayout(orientation='vertical', spacing=8, padding=15)
        
        # Title with clean blue styling
        title = Label(
            text='Artwork Release Data',
            font_size='22sp',
            size_hint_y=None,
            height=50,
            color=(0.09, 0.4, 0.8, 1),  # Nice blue color
            bold=True
        )
        main_layout.add_widget(title)
        
        # Subtitle
        subtitle = Label(
            text='RunTime depends on the sharepoint speed',
            font_size='14sp',
            size_hint_y=None,
            height=30,
            color=(0.2, 0.5, 0.9, 1)  # Lighter blue
        )
        main_layout.add_widget(subtitle)
        
        # Configuration section
        config_layout = BoxLayout(
            orientation='vertical', 
            size_hint_y=None, 
            height=140, 
            spacing=8,
            padding=10
        )
        
        # File selection row
        file_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=45, spacing=10)
        
        select_btn = Button(
            text='📁 Select Project Tracker File',
            size_hint_x=None,
            width=220,
            background_color=(0.13, 0.59, 0.95, 1),  # Clean blue
            color=(1, 1, 1, 1),  # White text
            font_size='13sp'
        )
        select_btn.bind(on_press=self.select_file)
        file_layout.add_widget(select_btn)
        
        self.file_label = Label(
            text='No file selected',
            font_size='13sp',
            color=(0.3, 0.3, 0.3, 1),  # Dark gray but readable
            text_size=(None, None),
            halign='left',
            valign='middle'
        )
        file_layout.add_widget(self.file_label)
        
        config_layout.add_widget(file_layout)
        
        # Date range row
        date_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=45, spacing=10)
        
        date_label = Label(
            text='Date Range:',
            size_hint_x=None,
            width=100,
            color=(0.2, 0.2, 0.2, 1),  # Clean dark text
            font_size='13sp'
        )
        date_layout.add_widget(date_label)
        
        self.start_date = TextInput(
            text='',
            hint_text='YYYY-MM-DD',
            size_hint_x=None,
            width=130,
            multiline=False,
            background_color=(1, 1, 1, 1),  # White background
            foreground_color=(0.2, 0.2, 0.2, 1),  # Dark text
            font_size='12sp'
        )
        date_layout.add_widget(self.start_date)
        
        to_label = Label(
            text='to',
            size_hint_x=None,
            width=30,
            color=(0.4, 0.4, 0.4, 1),
            font_size='13sp'
        )
        date_layout.add_widget(to_label)
        
        self.end_date = TextInput(
            text='',
            hint_text='YYYY-MM-DD',
            size_hint_x=None,
            width=130,
            multiline=False,
            background_color=(1, 1, 1, 1),  # White background
            foreground_color=(0.2, 0.2, 0.2, 1),  # Dark text
            font_size='12sp'
        )
        date_layout.add_widget(self.end_date)
        
        load_dates_btn = Button(
            text='📅 Auto-Load Dates',
            size_hint_x=None,
            width=140,
            background_color=(0.38, 0.69, 0.98, 1),  # Light blue
            color=(1, 1, 1, 1),
            font_size='12sp'
        )
        load_dates_btn.bind(on_press=self.load_dates)
        date_layout.add_widget(load_dates_btn)
        
        config_layout.add_widget(date_layout)
        
        main_layout.add_widget(config_layout)
        
        # Processing section
        process_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=65, spacing=15)
        
        self.process_btn = Button(
            text='🚀 START ENHANCED PROCESSING',
            size_hint_x=None,
            width=250,
            background_color=(0.11, 0.73, 0.31, 1),  # Clean green
            color=(1, 1, 1, 1),
            font_size='15sp',
            bold=True
        )
        self.process_btn.bind(on_press=self.start_processing)
        process_layout.add_widget(self.process_btn)
        
        clear_btn = Button(
            text='🗑️ Clear Cache',
            size_hint_x=None,
            width=130,
            background_color=(0.96, 0.42, 0.42, 1),  # Clean red
            color=(1, 1, 1, 1),
            font_size='12sp'
        )
        clear_btn.bind(on_press=self.clear_cache)
        process_layout.add_widget(clear_btn)
        
        open_btn = Button(
            text='📁 Open Output Folder',
            size_hint_x=None,
            width=150,
            background_color=(0.61, 0.35, 0.95, 1),  # Clean purple
            color=(1, 1, 1, 1),
            font_size='12sp'
        )
        open_btn.bind(on_press=self.open_output)
        process_layout.add_widget(open_btn)
        
        main_layout.add_widget(process_layout)
        
        # Progress bar
        self.progress = ProgressBar(
            size_hint_y=None, 
            height=25,
            value=0
        )
        main_layout.add_widget(self.progress)
        
        # Status label
        self.status = Label(
            text='Ready • Select project tracker file to begin enhanced processing',
            font_size='13sp',
            size_hint_y=None,
            height=35,
            color=(0.15, 0.45, 0.8, 1)  # Clean blue text
        )
        main_layout.add_widget(self.status)
        
        # Log section
        log_scroll = ScrollView()
        self.log_text = TextInput(
            text='[INFO] Enhanced Artwork Release Data Processor Ready\n[INFO] Features: Advanced data cleaning • No duplicate removal • Dual output files\n[INFO] Enhanced error handling • Flexible date parsing • Aggressive item number cleaning\n[INFO] Mac-optimized Kivy UI with excellent performance\n',
            multiline=True,
            readonly=True,
            font_size='11sp',
            background_color=(0.98, 0.99, 1, 1),  # Very light blue background
            foreground_color=(0.1, 0.1, 0.1, 1),  # Dark text for readability
            cursor_color=(0.2, 0.5, 0.9, 1)  # Blue cursor
        )
        log_scroll.add_widget(self.log_text)
        main_layout.add_widget(log_scroll)
        
        # Set default dates to last 90 days
        from datetime import timedelta
        current_date = datetime.now().date()
        start_date = current_date - timedelta(days=90)
        self.start_date.text = start_date.strftime('%Y-%m-%d')
        self.end_date.text = current_date.strftime('%Y-%m-%d')
        
        return main_layout
    
    def show_sharepoint_warning(self, dt):
        """Show SharePoint access warning"""
        content = Label(
            text='⚠️ SharePoint Access Required\n\nThis application requires access to Lowe\'s SharePoint directories.\nPlease ensure you have proper network access and try again.\n\nContact IT support if you need SharePoint access.',
            font_size='14sp',
            halign='center',
            color=(0.8, 0.4, 0.1, 1)  # Orange warning color
        )
        
        popup = Popup(
            title='SharePoint Access Warning',
            content=content,
            size_hint=(0.7, 0.5),
            background_color=(1, 0.95, 0.9, 1)  # Light orange background
        )
        popup.open()
        
        # Auto-close after 5 seconds
        Clock.schedule_once(lambda dt: popup.dismiss(), 5)
    
    def select_file(self, instance):
        """Enhanced file selection with better initial path"""
        content = BoxLayout(orientation='vertical', spacing=10, padding=10)
        
        # Try to start from the default project tracker location
        initial_path = str(Path.home())
        if self.processor.default_project_tracker_path.parent.exists():
            initial_path = str(self.processor.default_project_tracker_path.parent)
        
        filechooser = FileChooserIconView(
            filters=['*.xlsx', '*.xls'],
            path=initial_path
        )
        content.add_widget(filechooser)
        
        button_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=50, spacing=10)
        
        select_btn = Button(
            text='Select File',
            background_color=(0.13, 0.59, 0.95, 1),
            color=(1, 1, 1, 1)
        )
        cancel_btn = Button(
            text='Cancel',
            background_color=(0.6, 0.6, 0.6, 1),
            color=(1, 1, 1, 1)
        )
        
        button_layout.add_widget(select_btn)
        button_layout.add_widget(cancel_btn)
        content.add_widget(button_layout)
        
        popup = Popup(
            title='Select Project Tracker File',
            content=content,
            size_hint=(0.9, 0.8),
            background_color=(0.96, 0.98, 1, 1)
        )
        
        def select_file_action(btn):
            if filechooser.selection:
                file_path = filechooser.selection[0]
                self.processor.project_tracker_file = file_path
                self.file_label.text = f"📄 {Path(file_path).name}"
                self.file_label.color = (0.11, 0.73, 0.31, 1)  # Green when selected
                self.log(f"✅ Selected: {Path(file_path).name}")
            popup.dismiss()
        
        def cancel_action(btn):
            popup.dismiss()
        
        select_btn.bind(on_press=select_file_action)
        cancel_btn.bind(on_press=cancel_action)
        
        popup.open()
    
    def load_dates(self, instance):
        """Load date ranges with enhanced parsing"""
        if not self.processor.project_tracker_file:
            self.log("❌ Please select project tracker file first")
            return
        
        def date_callback(message):
            self.log(message)
            if "Range:" in message:
                # Extract dates from message
                parts = message.split("Range: ")[1].split(" to ")
                if len(parts) == 2:
                    self.start_date.text = parts[0]
                    self.end_date.text = parts[1]
                    self.processor.start_date_var = parts[0]
                    self.processor.end_date_var = parts[1]
                    # Update colors to show success
                    self.start_date.background_color = (0.9, 1, 0.9, 1)  # Light green
                    self.end_date.background_color = (0.9, 1, 0.9, 1)  # Light green
        
        threading.Thread(
            target=self.processor.load_date_ranges,
            args=(date_callback,),
            daemon=True
        ).start()
    
    def start_processing(self, instance):
        """Start enhanced processing workflow"""
        if not self.processor.project_tracker_file:
            self.log("❌ Please select project tracker file first")
            return
        
        # Update date variables
        self.processor.start_date_var = self.start_date.text
        self.processor.end_date_var = self.end_date.text
        
        if not self.processor.start_date_var or not self.processor.end_date_var:
            self.log("❌ Please enter both start and end dates")
            return
        
        self.process_btn.text = '🔄 ENHANCED PROCESSING...'
        self.process_btn.disabled = True
        self.process_btn.background_color = (0.7, 0.7, 0.7, 1)  # Gray while processing
        self.progress.value = 0
        
        def process_thread():
            try:
                start_time = time.time()
                
                # Step 1: Scan production folders
                self.update_status("🔍 Scanning production folders...")
                self.update_progress(10)
                if not self.processor.scan_production_folders(self.log):
                    raise Exception("No production files found")
                
                # Step 2: Extract production data with enhanced cleaning
                self.update_status("⚡ Extracting production data...")
                self.update_progress(25)
                if not self.processor.intelligent_data_extraction(self.log):
                    raise Exception("Production data extraction failed")
                
                # Step 3: Process project tracker
                self.update_status("📋 Processing project tracker...")
                self.update_progress(45)
                if not self.processor.process_project_tracker(self.log):
                    raise Exception("Project tracker processing failed")
                
                # Step 4: Combine datasets (NO DUPLICATE REMOVAL)
                self.update_status("🔗 Combining datasets...")
                self.update_progress(60)
                if not self.processor.combine_datasets(self.log):
                    raise Exception("Data combination failed")
                
                # Step 5: Filter by date range
                self.update_status("📅 Filtering by date range...")
                self.update_progress(75)
                if not self.processor.filter_by_date_range(self.processor.start_date_var, self.processor.end_date_var, self.log):
                    raise Exception("Date filtering failed")
                
                # Step 6: Format final output
                self.update_status("📋 Formatting final output...")
                self.update_progress(85)
                if not self.processor.format_final_output(self.log):
                    raise Exception("Final output formatting failed")
                
                # Step 7: Save dual files
                self.update_status("💾 Saving dual output files...")
                self.update_progress(95)
                output_files = self.processor.save_dual_files(self.log)
                
                total_time = time.time() - start_time
                self.update_progress(100)
                
                self.log(f"🎉 ENHANCED PROCESSING COMPLETE! Total time: {total_time:.2f} seconds")
                self.log(f"📊 SharePoint combined records: {len(self.processor.consolidated_data)}")
                self.log(f"📋 Final output records: {len(self.processor.final_output_data)}")
                
                Clock.schedule_once(
                    lambda dt: self.show_success(
                        len(self.processor.final_output_data), 
                        len(self.processor.consolidated_data), 
                        total_time
                    ), 0
                )
                
            except Exception as e:
                self.log(f"❌ Error: {e}")
                Clock.schedule_once(lambda dt: self.show_error(str(e)), 0)
            finally:
                Clock.schedule_once(self.reset_ui, 0)
        
        threading.Thread(target=process_thread, daemon=True).start()
    
    def update_status(self, message):
        """Update status label thread-safely"""
        Clock.schedule_once(lambda dt: setattr(self.status, 'text', message), 0)
    
    def update_progress(self, value):
        """Update progress bar thread-safely"""
        Clock.schedule_once(lambda dt: setattr(self.progress, 'value', value), 0)
    
    def show_success(self, artwork_count, combined_count, time_taken):
        """Show enhanced success popup"""
        content = Label(
            text=f'🎉 Enhanced Processing Complete!\n\n📋 Final Artwork Records: {artwork_count:,}\n📁 Combined SharePoint Data: {combined_count:,}\n⏱️ Processing Time: {time_taken:.2f} seconds\n\n💾 Two files saved to Desktop/Automated_Data_Processing_Output:\n• Combined_Data_[date].xlsx (all SharePoint production files)\n• Final_Output_[date].xlsx (formatted final data)\n\n✨ Enhanced features applied:\n• Aggressive item number cleaning\n• No duplicate removal (preserves all records)\n• Flexible date parsing with multiple formats\n• Advanced error handling throughout',
            font_size='14sp',
            halign='center',
            color=(0.15, 0.45, 0.8, 1)
        )
        
        popup = Popup(
            title='Success - Enhanced Processing Complete',
            content=content,
            size_hint=(0.8, 0.7),
            background_color=(0.96, 0.98, 1, 1)
        )
        popup.open()
    
    def show_error(self, error_msg):
        """Show error popup with enhanced feedback"""
        content = Label(
            text=f'❌ Processing Error\n\n{error_msg}\n\nCheck the log for more details.\nTry adjusting date range or check file access.',
            font_size='14sp',
            halign='center',
            color=(0.8, 0.2, 0.2, 1)
        )
        
        popup = Popup(
            title='Processing Error',
            content=content,
            size_hint=(0.7, 0.5),
            background_color=(1, 0.95, 0.95, 1)
        )
        popup.open()
    
    def reset_ui(self, dt=None):
        """Reset UI after processing"""
        self.process_btn.text = '🚀 START ENHANCED PROCESSING'
        self.process_btn.disabled = False
        self.process_btn.background_color = (0.11, 0.73, 0.31, 1)  # Back to green
        self.progress.value = 0
        self.status.text = 'Ready • Enhanced processing complete'
        self.status.color = (0.11, 0.73, 0.31, 1)  # Green success color
    
    def clear_cache(self, instance):
        """Clear cache with enhanced feedback"""
        try:
            if self.processor.cache_file.exists():
                self.processor.cache_file.unlink()
            if self.processor.data_cache_file.exists():
                self.processor.data_cache_file.unlink()
            self.processor.file_cache = {}
            self.log("🗑️ Cache cleared successfully - will re-scan all files")
        except Exception as e:
            self.log(f"❌ Cache clear error: {e}")
    
    def open_output(self, instance):
        """Open output folder with cross-platform support"""
        try:
            if platform.system() == 'Darwin':  # Mac
                subprocess.run(['open', str(self.processor.output_folder)])
            elif platform.system() == 'Windows':  # Windows
                subprocess.run(['explorer', str(self.processor.output_folder)])
            else:  # Linux
                subprocess.run(['xdg-open', str(self.processor.output_folder)])
                
            self.log(f"📁 Opened output folder: {self.processor.output_folder}")
        except Exception as e:
            self.log(f"❌ Error opening folder: {e}")
    
    def log(self, message):
        """Add message to log with clean formatting"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"
        
        def update_log(dt):
            self.log_text.text += formatted_message
            # Auto-scroll to bottom
            self.log_text.cursor = (len(self.log_text.text), 0)
        
        Clock.schedule_once(update_log, 0)

if __name__ == '__main__':
    try:
        print("🎨 Starting Enhanced Artwork Release Data Processor - Mac Optimized")
        print("📊 Features: Enhanced data processing • Dual output files • Advanced cleaning")
        print("🔧 No duplicate removal • Flexible date parsing • Aggressive item number cleaning")
        
        EnhancedArtworkReleaseApp().run()
        
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        print("Install required packages:")
        print("pip install pandas openpyxl xlsxwriter kivy numpy")
    except Exception as e:
        print(f"❌ Error: {e}")
        input("Press Enter to exit...")
