#!/usr/bin/env python3
"""
🎨 ARTWORK RELEASE DATA PROCESSOR - CLEAN BLUE UI
- Beautiful light blue UI design
- Clear text visibility and clean appearance
- Dual output: Combined file + Final file
- Mac-optimized Kivy interface
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

class ArtworkDataProcessor:
    def __init__(self):
        # Core data
        self.production_files = []
        self.consolidated_data = pd.DataFrame()
        self.project_tracker_data = pd.DataFrame()
        self.combined_data = pd.DataFrame()
        
        # Performance settings
        self.max_workers = min(os.cpu_count() * 2, 16)
        
        # Cache system
        self.cache_file = Path.home() / "Desktop" / "processor_cache.json"
        self.data_cache_file = Path.home() / "Desktop" / "data_cache.pkl"
        self.output_folder = Path.home() / "Desktop" / "Data_Processing_Output"
        self.output_folder.mkdir(exist_ok=True)
        self.file_cache = self.load_cache()
        
        # Configuration
        self.project_tracker_file = ""
        self.start_date_var = ""
        self.end_date_var = ""
        
        # Setup paths
        self.setup_paths()
        
        # Target columns for Step 1
        self.target_columns = ['Item #', 'Vendor Name', 'Brand', 'Item Description', 'SKU New/Existing']
        
        Logger.info("🎨 Artwork Release Data Processor initialized")
    
    def setup_paths(self):
        """Setup Mac-optimized paths"""
        if platform.system() == 'Darwin':  # Mac
            base = Path.home() / "Lowe's Companies Inc"
        else:  # Windows/Linux
            base = Path("C:/Users") / os.getenv('USERNAME', 'mjayash') / "Lowe's Companies Inc"
        
        self.sharepoint_paths = [
            base / "Private Brands - Packaging Operations - Building Products",
            base / "Private Brands - Packaging Operations - Hardlines & Seasonal",
            base / "Private Brands - Packaging Operations - Home Décor"
        ]
        
        self.project_tracker_path = base / "Private Brands Packaging File Transfer - PQM Compliance reporting" / "Project tracker.xlsx"
    
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
    
    def load_date_ranges(self, callback=None):
        """Load date ranges from project tracker"""
        if not self.project_tracker_file:
            return
        
        try:
            if callback:
                Clock.schedule_once(lambda dt: callback("📅 Loading date ranges..."), 0)
            
            df = pd.read_excel(self.project_tracker_file, nrows=1000)
            
            # Find ReleaseDate column
            release_col = None
            for col in df.columns:
                if 'release' in str(col).lower() and 'date' in str(col).lower():
                    release_col = col
                    break
            
            if not release_col:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ ReleaseDate column not found"), 0)
                return
            
            dates = pd.to_datetime(df[release_col], errors='coerce').dropna()
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
    
    def aggressive_file_scan(self, callback=None):
        """Ultra-fast file scanning with caching"""
        start_time = time.time()
        
        if callback:
            Clock.schedule_once(lambda dt: callback("🔍 Scanning shared drive files..."), 0)
        
        all_files = []
        new_files = []
        cached_files = []
        
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
            except:
                pass
            
            return files
        
        # Parallel scanning
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(scan_path, path) for path in self.sharepoint_paths]
            
            for future in as_completed(futures):
                path_files = future.result()
                all_files.extend(path_files)
        
        # Smart caching
        for file_path in all_files:
            current_hash = self.get_file_hash(file_path)
            
            if current_hash and file_path in self.file_cache:
                if self.file_cache[file_path]['hash'] == current_hash:
                    cached_files.append(file_path)
                    continue
            
            new_files.append(file_path)
            if current_hash:
                self.file_cache[file_path] = {
                    'hash': current_hash,
                    'processed_date': datetime.now().isoformat()
                }
        
        self.production_files = all_files
        scan_time = time.time() - start_time
        
        if callback:
            Clock.schedule_once(
                lambda dt: callback(f"✅ Found {len(all_files)} files ({len(new_files)} new) in {scan_time:.2f}s"), 0
            )
        
        return len(all_files) > 0, new_files, cached_files
    
    def turbo_extraction(self, files_to_process, callback=None):
        """Turbo-charged data extraction"""
        start_time = time.time()
        
        column_patterns = {
            'Item #': ['item #', 'item#', 'itemnumber', 'item number', 'item no', 'itemno'],
            'Vendor Name': ['vendor name', 'vendorname', 'vendor', 'supplier'],
            'Brand': ['brand', 'brandname', 'brand name'],
            'Item Description': ['item description', 'itemdescription', 'description', 'product description', 'desc'],
            'SKU New/Existing': ['SKU', 'SKU new/existing', 'SKU new existing', 'SKU new/carry forward', 'SKU new']
        }
        
        def extract_file(file_path):
            """Extract single file with aggressive optimization"""
            try:
                df = pd.read_excel(file_path, header=None, nrows=1000)
                if df.empty:
                    return pd.DataFrame()
                
                best_extraction = pd.DataFrame()
                best_score = 0
                
                # Try each row as header (up to 50)
                for row_idx in range(min(50, len(df))):
                    headers = df.iloc[row_idx].astype(str).str.lower().str.strip()
                    
                    # Multi-line header handling
                    if row_idx + 1 < len(df):
                        next_headers = df.iloc[row_idx + 1].astype(str).str.lower().str.strip()
                        headers = headers + " " + next_headers
                        headers = headers.str.replace(r'\s+', ' ', regex=True).str.strip()
                    
                    # Find column matches
                    mapping = {}
                    score = 0
                    
                    for target, patterns in column_patterns.items():
                        for col_idx, header in enumerate(headers):
                            if pd.isna(header) or 'nan' in str(header):
                                continue
                            
                            clean_header = re.sub(r'[^a-z0-9]', '', str(header))
                            
                            for pattern in patterns:
                                clean_pattern = re.sub(r'[^a-z0-9]', '', pattern)
                                if clean_pattern in clean_header:
                                    mapping[target] = col_idx
                                    score += 1
                                    break
                            
                            if target in mapping:
                                break
                    
                    if score >= 2:
                        try:
                            full_df = pd.read_excel(file_path, header=row_idx, nrows=10000)
                            
                            if not full_df.empty:
                                extracted = pd.DataFrame()
                                
                                for target in self.target_columns:
                                    if target in mapping and mapping[target] < len(full_df.columns):
                                        col_name = full_df.columns[mapping[target]]
                                        extracted[target] = full_df[col_name].astype(str).str.strip()
                                    else:
                                        extracted[target] = ''
                                
                                # Clean Item #
                                if 'Item #' in extracted.columns:
                                    def clean_item(val):
                                        try:
                                            clean_val = re.sub(r'[^\d]', '', str(val))
                                            return str(int(clean_val)) if clean_val.isdigit() else ''
                                        except:
                                            return ''
                                    
                                    extracted['Item #'] = extracted['Item #'].apply(clean_item)
                                    extracted = extracted[extracted['Item #'] != '']
                                
                                if len(extracted) > 0:
                                    extracted['Source_File'] = Path(file_path).name
                                    extracted['Source_Folder'] = Path(file_path).parent.name
                                    
                                    if score > best_score or len(extracted) > len(best_extraction):
                                        best_extraction = extracted.copy()
                                        best_score = score
                        except:
                            continue
                
                return best_extraction
                
            except:
                return pd.DataFrame()
        
        # Parallel extraction
        all_data = []
        successful = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(extract_file, fp) for fp in files_to_process]
            
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                if not result.empty:
                    all_data.append(result)
                    successful += 1
                
                if (i + 1) % 20 == 0 and callback:
                    progress = (i + 1) / len(files_to_process) * 100
                    Clock.schedule_once(
                        lambda dt: callback(f"⚡ Processing: {i + 1}/{len(files_to_process)} ({progress:.0f}%)"), 0
                    )
        
        extraction_time = time.time() - start_time
        
        if callback:
            Clock.schedule_once(
                lambda dt: callback(f"✅ Extracted from {successful}/{len(files_to_process)} files in {extraction_time:.2f}s"), 0
            )
        
        return all_data
    
    def lightning_project_tracker(self, callback=None):
        """Lightning-fast project tracker processing"""
        try:
            if not self.project_tracker_file or not os.path.exists(self.project_tracker_file):
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ Project tracker file not found"), 0)
                return False
            
            if callback:
                Clock.schedule_once(lambda dt: callback("📋 Processing project tracker data..."), 0)
            
            df = pd.read_excel(self.project_tracker_file)
            
            # Find columns with flexible matching
            def find_col(possible_names):
                for name in possible_names:
                    for col in df.columns:
                        if name.lower() in str(col).lower():
                            return col
                return None
            
            column_mappings = {
                'HUGO ID': ['PKG3'],
                'File Name': ['File Name', 'FileName'],
                'Rounds': ['Rounds', 'Round'],
                'Printer Company Name 1': ['PAComments', 'PA Comments'],
                'Vendor e-mail 1': ['VendorEmail', 'Vendor Email'],
                'Printer e-mail 1': ['PrinterEmail', 'Printer Email'],
                'PKG1': ['PKG1'],
                'Artwork Release Date': ['ReleaseDate', 'Release Date'],
                '5 Weeks After Artwork Release': ['5 Weeks After Artwork Release'],
                'Entered into HUGO Date': ['entered into HUGO Date'],
                'Entered in HUGO?': ['Entered in HUGO?'],
                'Store Date': ['Store Date'],
                'Packaging Format 1': ['Packaging Format 1'],
                'Printer Code 1 (LW Code)': ['Printer Code 1 (LW Code)'],
                'Re-Release Status': ['ReleaseStatus', 'Release Status']
            }
            
            found_columns = {}
            for target, names in column_mappings.items():
                col = find_col(names)
                if col:
                    found_columns[target] = col
            
            if 'Rounds' not in found_columns:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ Rounds column not found"), 0)
                return False
            
            # Filter data
            filter_values = ["File Release", "File Re-Release R2", "File Re-Release R3"]
            mask = df[found_columns['Rounds']].isin(filter_values)
            filtered_df = df[mask].copy()
            
            # Date filtering
            if 'Artwork Release Date' in found_columns and self.start_date_var and self.end_date_var:
                try:
                    start_date = datetime.strptime(self.start_date_var, "%Y-%m-%d")
                    end_date = datetime.strptime(self.end_date_var, "%Y-%m-%d")
                    
                    dates = pd.to_datetime(filtered_df[found_columns['Artwork Release Date']], errors='coerce')
                    date_mask = (dates >= start_date) & (dates <= end_date)
                    filtered_df = filtered_df[date_mask]
                except:
                    pass
            
            if len(filtered_df) == 0:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ No matching records found"), 0)
                return False
            
            # Create result with ALL required columns
            result = pd.DataFrame()
            
            # Map all columns
            for target in column_mappings.keys():
                if target in found_columns:
                    if target == 'Artwork Release Date':
                        # Format dates as DD/MM/YY
                        dates = pd.to_datetime(filtered_df[found_columns[target]], errors='coerce')
                        result[target] = dates.dt.strftime("%d/%m/%y").fillna("")
                    else:
                        result[target] = filtered_df[found_columns[target]].fillna("").astype(str)
                else:
                    result[target] = ""
            
            # Calculate Re-Release Status
            if 'Rounds' in found_columns:
                rounds_col = found_columns['Rounds']
                result['Re-Release Status'] = filtered_df[rounds_col].str.contains(
                    'R2|R3', case=False, na=False
                ).map({True: 'Yes', False: 'No'})
            
            self.project_tracker_data = result
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"✅ Project tracker: {len(result)} records processed"), 0
                )
            
            return True
            
        except Exception as e:
            if callback:
                Clock.schedule_once(lambda dt: callback(f"❌ Project tracker error: {e}"), 0)
            return False
    
    def smart_data_combination(self, callback=None):
        """Smart combination with NO pipe combining (separate rows preserved)"""
        try:
            if callback:
                Clock.schedule_once(lambda dt: callback("🔗 Combining shared drive + tracker data..."), 0)
            
            if self.consolidated_data.empty or self.project_tracker_data.empty:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ Missing data for combination"), 0)
                return False
            
            step1 = self.consolidated_data.copy()
            step2 = self.project_tracker_data.copy()
            
            # Smart key cleaning (handles trailing zeros)
            def smart_clean(value):
                try:
                    clean_val = re.sub(r'[^\d]', '', str(value))
                    if not clean_val:
                        return ''
                    # Remove trailing zeros intelligently
                    return clean_val.rstrip('0') or clean_val[-1]
                except:
                    return ''
            
            step1['Item_Clean'] = step1['Item #'].apply(smart_clean)
            step2['PKG1_Clean'] = step2['PKG1'].apply(smart_clean)
            
            # Remove empty keys
            step1_valid = step1[step1['Item_Clean'] != ''].copy()
            step2_valid = step2[step2['PKG1_Clean'] != ''].copy()
            
            # NO DUPLICATE MERGING - Keep separate rows as requested
            # Just proceed with merge without combining duplicates
            
            # Prepare for merge
            step1_merge = step1_valid.rename(columns={'Item_Clean': 'Merge_Key'})
            step2_merge = step2_valid.rename(columns={'PKG1_Clean': 'Merge_Key'})
            
            # Add prefixes to avoid conflicts
            step1_cols = {col: f"Step1_{col}" if col in step2_merge.columns and col != 'Merge_Key' else col 
                         for col in step1_merge.columns}
            step2_cols = {col: f"Step2_{col}" if col in step1_merge.columns and col != 'Merge_Key' else col 
                         for col in step2_merge.columns}
            
            step1_merge = step1_merge.rename(columns=step1_cols)
            step2_merge = step2_merge.rename(columns=step2_cols)
            
            # Inner join to get only matched records
            combined = pd.merge(step1_merge, step2_merge, on='Merge_Key', how='inner')
            
            if len(combined) == 0:
                if callback:
                    Clock.schedule_once(lambda dt: callback("❌ No matching records found"), 0)
                return False
            
            # Create final dataset with EXACT column mapping as specified
            final_data = pd.DataFrame()
            
            # Map columns according to specifications
            final_data['HUGO ID'] = combined.get('Step2_HUGO ID', combined.get('HUGO ID', ''))
            final_data['Product Vendor Company Name'] = combined.get('Step1_Vendor Name', combined.get('Vendor Name', ''))
            final_data['Item Number'] = combined.get('Step1_Item #', combined.get('Item #', ''))
            final_data['Product Name'] = combined.get('Step1_Item Description', combined.get('Item Description', ''))
            final_data['Brand'] = combined.get('Step1_Brand', combined.get('Brand', ''))
            final_data['New or Carry Forward'] = combined.get('Step1_SKU New/Existing', combined.get('SKU New/Existing', ''))
            final_data['Artwork Release Date'] = combined.get('Step2_Artwork Release Date', combined.get('Artwork Release Date', ''))
            final_data['5 Weeks After Artwork Release'] = combined.get('Step2_5 Weeks After Artwork Release', combined.get('5 Weeks After Artwork Release', ''))
            final_data['Entered into HUGO Date'] = combined.get('Step2_Entered into HUGO Date', combined.get('Entered into HUGO Date', ''))
            final_data['Entered in HUGO?'] = combined.get('Step2_Entered in HUGO?', combined.get('Entered in HUGO?', ''))
            final_data['Store Date'] = combined.get('Step2_Store Date', combined.get('Store Date', ''))
            final_data['Re-Release Status'] = combined.get('Step2_Re-Release Status', combined.get('Re-Release Status', ''))
            final_data['Packaging Format 1'] = combined.get('Step2_Packaging Format 1', combined.get('Packaging Format 1', ''))
            final_data['Printer Company Name 1'] = combined.get('Step2_Printer Company Name 1', combined.get('Printer Company Name 1', ''))
            final_data['Vendor e-mail 1'] = combined.get('Step2_Vendor e-mail 1', combined.get('Vendor e-mail 1', ''))
            final_data['Printer e-mail 1'] = combined.get('Step2_Printer e-mail 1', combined.get('Printer e-mail 1', ''))
            final_data['Printer Code 1 (LW Code)'] = combined.get('Step2_Printer Code 1 (LW Code)', combined.get('Printer Code 1 (LW Code)', ''))
            final_data['File Name'] = combined.get('Step2_File Name', combined.get('File Name', ''))
            
            self.combined_data = final_data
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"✅ Final data ready: {len(final_data)} artwork records"), 0
                )
            
            return True
            
        except Exception as e:
            if callback:
                Clock.schedule_once(lambda dt: callback(f"❌ Combination error: {e}"), 0)
            return False
    
    def dual_file_save(self, callback=None):
        """Save TWO files: Combined shared drive data + Final artwork data"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # File 1: Combined Shared Drive Data (updates with new files)
            combined_file = self.output_folder / f"Combined_SharedDrive_Data_{timestamp}.xlsx"
            
            with pd.ExcelWriter(combined_file, engine='xlsxwriter') as writer:
                self.consolidated_data.to_excel(writer, sheet_name='Combined Shared Drive Data', index=False)
                
                # Summary for combined data
                summary1 = pd.DataFrame({
                    'Metric': ['Total Records from Shared Drive', 'Processing Date', 'Source Folders'],
                    'Value': [
                        len(self.consolidated_data),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        self.consolidated_data['Source_Folder'].nunique() if not self.consolidated_data.empty else 0
                    ]
                })
                summary1.to_excel(writer, sheet_name='Summary', index=False)
            
            # File 2: Final Artwork Release Data
            final_file = self.output_folder / f"Artwork_Release_Data_{timestamp}.xlsx"
            
            with pd.ExcelWriter(final_file, engine='xlsxwriter') as writer:
                self.combined_data.to_excel(writer, sheet_name='Artwork Release Data', index=False)
                
                # Summary for final data
                summary2 = pd.DataFrame({
                    'Metric': ['Final Artwork Records', 'Processing Date', 'Date Range Used', 'Output Type'],
                    'Value': [
                        len(self.combined_data),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        f"{self.start_date_var} to {self.end_date_var}",
                        "Artwork Release Data (Matched Records Only)"
                    ]
                })
                summary2.to_excel(writer, sheet_name='Summary', index=False)
                
                # Format the main sheet
                workbook = writer.book
                worksheet = writer.sheets['Artwork Release Data']
                
                # Header formatting
                header_format = workbook.add_format({
                    'bold': True,
                    'bg_color': '#E3F2FD',  # Light blue background
                    'font_color': '#1565C0',  # Blue text
                    'align': 'center',
                    'border': 1
                })
                
                # Apply header formatting
                for col_num, value in enumerate(self.combined_data.columns):
                    worksheet.write(0, col_num, value, header_format)
                    # Auto-adjust column widths
                    if 'Name' in value or 'Description' in value:
                        worksheet.set_column(col_num, col_num, 25)
                    elif 'Date' in value:
                        worksheet.set_column(col_num, col_num, 12)
                    else:
                        worksheet.set_column(col_num, col_num, 15)
            
            if callback:
                Clock.schedule_once(
                    lambda dt: callback(f"💾 Saved 2 files: {combined_file.name} & {final_file.name}"), 0
                )
            
            return str(combined_file), str(final_file)
            
        except Exception as e:
            if callback:
                Clock.schedule_once(lambda dt: callback(f"❌ Save error: {e}"), 0)
            return None, None

class ArtworkReleaseApp(App):
    def build(self):
        self.title = "Artwork Release Data"
        
        # Initialize processor
        self.processor = ArtworkDataProcessor()
        
        # Main layout with light blue theme
        main_layout = BoxLayout(orientation='vertical', spacing=8, padding=15)
        main_layout.canvas.before.clear()
        
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
            text='Clean UI • Dual Output Files • Smart Processing',
            font_size='14sp',
            size_hint_y=None,
            height=30,
            color=(0.2, 0.5, 0.9, 1)  # Lighter blue
        )
        main_layout.add_widget(subtitle)
        
        # Configuration section with light blue background
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
        
        # Processing section with clean button layout
        process_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=65, spacing=15)
        
        self.process_btn = Button(
            text='🚀 START PROCESSING',
            size_hint_x=None,
            width=200,
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
        
        # Progress bar with clean styling
        self.progress = ProgressBar(
            size_hint_y=None, 
            height=25,
            value=0
        )
        main_layout.add_widget(self.progress)
        
        # Status label with good visibility
        self.status = Label(
            text='Ready • Select project tracker file to begin',
            font_size='13sp',
            size_hint_y=None,
            height=35,
            color=(0.15, 0.45, 0.8, 1)  # Clean blue text
        )
        main_layout.add_widget(self.status)
        
        # Log section with clean light background
        log_scroll = ScrollView()
        self.log_text = TextInput(
            text='[INFO] Artwork Release Data Processor Ready\n[INFO] Dual output: Combined drive data + Final artwork data\n[INFO] Clean light UI with excellent visibility\n[INFO] Smart caching for ultra-fast processing\n',
            multiline=True,
            readonly=True,
            font_size='11sp',
            background_color=(0.98, 0.99, 1, 1),  # Very light blue background
            foreground_color=(0.1, 0.1, 0.1, 1),  # Dark text for readability
            cursor_color=(0.2, 0.5, 0.9, 1)  # Blue cursor
        )
        log_scroll.add_widget(self.log_text)
        main_layout.add_widget(log_scroll)
        
        return main_layout
    
    def select_file(self, instance):
        """File selection popup with clean blue styling"""
        content = BoxLayout(orientation='vertical', spacing=10, padding=10)
        
        filechooser = FileChooserIconView(
            filters=['*.xlsx', '*.xls'],
            path=str(Path.home())
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
        """Load date ranges from project tracker"""
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
        """Start the processing with clean UI feedback"""
        if not self.processor.project_tracker_file:
            self.log("❌ Please select project tracker file first")
            return
        
        # Update date variables
        self.processor.start_date_var = self.start_date.text
        self.processor.end_date_var = self.end_date.text
        
        self.process_btn.text = '🔄 PROCESSING...'
        self.process_btn.disabled = True
        self.process_btn.background_color = (0.7, 0.7, 0.7, 1)  # Gray while processing
        self.progress.value = 0
        
        def process_thread():
            try:
                start_time = time.time()
                
                # Step 1: File scanning
                self.update_status("🔍 Scanning shared drive files...")
                self.update_progress(10)
                has_files, new_files, cached_files = self.processor.aggressive_file_scan(self.log)
                
                if not has_files:
                    self.log("❌ No files found in shared drive")
                    return
                
                # Step 2: Load cached data
                self.update_status("📦 Loading cached data...")
                self.update_progress(20)
                cached_data = []
                if self.processor.data_cache_file.exists() and cached_files:
                    try:
                        with open(self.processor.data_cache_file, 'rb') as f:
                            all_cached = pickle.load(f)
                        cached_data = [
                            data for data in all_cached
                            if any(data['Source_File'].iloc[0] == Path(cf).name for cf in cached_files)
                        ]
                        self.log(f"📦 Loaded {len(cached_data)} cached datasets")
                    except:
                        cached_data = []
                
                # Step 3: Extract new data
                self.update_status("⚡ Extracting shared drive data...")
                self.update_progress(40)
                new_data = []
                if new_files:
                    new_data = self.processor.turbo_extraction(new_files, self.log)
                
                # Step 4: Consolidate
                self.update_progress(55)
                all_data = cached_data + new_data
                if all_data:
                    self.processor.consolidated_data = pd.concat(all_data, ignore_index=True)
                    self.processor.consolidated_data = self.processor.consolidated_data.drop_duplicates(
                        subset=['Item #', 'Source_File'], keep='first'
                    )
                    
                    # Update cache
                    if new_data:
                        try:
                            with open(self.processor.data_cache_file, 'wb') as f:
                                pickle.dump(all_data, f)
                            self.processor.save_cache()
                        except:
                            pass
                    
                    self.log(f"✅ Combined shared drive data: {len(self.processor.consolidated_data)} records")
                else:
                    self.log("❌ No data extracted from shared drive")
                    return
                
                # Step 5: Process project tracker
                self.update_status("📋 Processing project tracker...")
                self.update_progress(70)
                if not self.processor.lightning_project_tracker(self.log):
                    return
                
                # Step 6: Combine data
                self.update_status("🔗 Creating final artwork data...")
                self.update_progress(85)
                if not self.processor.smart_data_combination(self.log):
                    return
                
                # Step 7: Save dual files
                self.update_status("💾 Saving both output files...")
                self.update_progress(95)
                combined_file, final_file = self.processor.dual_file_save(self.log)
                
                total_time = time.time() - start_time
                self.update_progress(100)
                
                self.log(f"🎉 COMPLETE! Processed in {total_time:.2f} seconds")
                self.log(f"📊 Final artwork records: {len(self.processor.combined_data)}")
                self.log(f"📁 Combined drive data: {len(self.processor.consolidated_data)} records")
                
                Clock.schedule_once(
                    lambda dt: self.show_success(
                        len(self.processor.combined_data), 
                        len(self.processor.consolidated_data), 
                        total_time
                    ), 0
                )
                
            except Exception as e:
                self.log(f"❌ Error: {e}")
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
        """Show success popup with clean blue styling"""
        content = Label(
            text=f'🎉 Processing Complete!\n\n📋 Final Artwork Records: {artwork_count}\n📁 Combined Drive Data: {combined_count}\n⏱️ Processing Time: {time_taken:.2f} seconds\n\n💾 Two files saved to Desktop/Data_Processing_Output:\n• Combined_SharedDrive_Data (updates with new files)\n• Artwork_Release_Data (final matched records)\n\n✨ Clean processing with smart caching applied',
            font_size='14sp',
            halign='center',
            color=(0.15, 0.45, 0.8, 1)
        )
        
        popup = Popup(
            title='Success - Dual Files Created',
            content=content,
            size_hint=(0.7, 0.6),
            background_color=(0.96, 0.98, 1, 1)
        )
        popup.open()
    
    def reset_ui(self, dt=None):
        """Reset UI after processing"""
        self.process_btn.text = '🚀 START PROCESSING'
        self.process_btn.disabled = False
        self.process_btn.background_color = (0.11, 0.73, 0.31, 1)  # Back to green
        self.progress.value = 0
        self.status.text = 'Ready • Processing complete'
        self.status.color = (0.11, 0.73, 0.31, 1)  # Green success color
    
    def clear_cache(self, instance):
        """Clear cache with clean feedback"""
        try:
            if self.processor.cache_file.exists():
                self.processor.cache_file.unlink()
            if self.processor.data_cache_file.exists():
                self.processor.data_cache_file.unlink()
            self.processor.file_cache = {}
            self.log("🗑️ Cache cleared successfully")
        except Exception as e:
            self.log(f"❌ Cache clear error: {e}")
    
    def open_output(self, instance):
        """Open output folder"""
        try:
            subprocess.run(['open', str(self.processor.output_folder)])
            self.log(f"📁 Opened output folder: {self.processor.output_folder}")
        except Exception as e:
            self.log(f"❌ Error opening folder: {e}")
    
    def log(self, message):
        """Add message to log with clean formatting"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"
        
        def update_log(dt):
            self.log_text.text += formatted_message
        
        Clock.schedule_once(update_log, 0)

if __name__ == '__main__':
    try:
        print("🎨 Starting Artwork Release Data Processor - Clean Blue UI")
        print("📊 Features: Dual output files | Clean visibility | Light blue theme")
        
        ArtworkReleaseApp().run()
        
    except ImportError as e:
        print(f"❌ Missing package: {e}")
        print("Install: pip install pandas openpyxl xlsxwriter kivy")
    except Exception as e:
        print(f"❌ Error: {e}")
        input("Press Enter to exit...")
