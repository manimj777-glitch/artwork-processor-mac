#!/usr/bin/env python3
"""
Automated Data Processor - Mac Native Kivy Version with Hidden Sheet Support
MINIMAL ENHANCEMENT: Only adds ability to read hidden/protected sheets from Excel files
All existing logic and features preserved unchanged.

New Feature Added:
- Enhanced Excel file reading that can access hidden/protected worksheets
- All other functionality remains exactly the same
"""

import pandas as pd
import numpy as np
import os
import sys
import platform
import threading
import re
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from datetime import datetime
import time

# Kivy imports
from kivy.app import App
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.label import Label
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.progressbar import ProgressBar
from kivy.uix.popup import Popup
from kivy.clock import Clock, mainthread
from kivy.graphics import Color, Rectangle
from kivy.core.window import Window

# Native Mac file dialog support
import tkinter as tk
from tkinter import filedialog
import subprocess

# Mac-optimized file dialog system
FILECHOOSER_AVAILABLE = True
FILECHOOSER_TYPE = "mac_native"
print("✅ Using native Mac file dialogs (tkinter + AppleScript fallback)")

# Suppress pandas warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

class AutomatedDataProcessor(App):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.title = "Automated Data Processor - Mac Optimized + Hidden Sheets"
        
        # Core data
        self.production_files = []
        self.consolidated_data = pd.DataFrame()
        self.project_tracker_data = pd.DataFrame()
        self.combined_data = pd.DataFrame()
        self.final_output_data = pd.DataFrame()
        
        # Paths and settings
        self.project_tracker_path = ""
        self.processing_logs = []
        self.selected_start_date = None
        self.selected_end_date = None
        self.date_filter_applied = False
        self.sharepoint_access_ok = False
        
        # Setup paths
        self.setup_paths()
        
        # Target columns for Production Item Lists
        self.target_columns = ['Item Number', 'Product Vendor Company Name', 'Brand', 'Product Name', 'SKU New/Existing']
        
        # Final output column order with renamed headers
        self.final_columns = [
            'HUGO ID', 'Product Vendor Company Name', 'Item Number', 'Product Name', 'Brand', 'SKU', 
            'Artwork Release Date', '5 Weeks After Artwork Release', 'Entered into HUGO Date', 
            'Entered in HUGO?', 'Store Date', 'Re-Release Status', 'Packaging Format 1', 
            'Printer Company Name 1', 'Vendor e-mail 1', 'Printer e-mail 1', 
            'Printer Code 1 (LW Code)', 'File Name'
        ]
        
        # UI references
        self.status_label = None
        self.progress_bar = None
        self.tracker_status_label = None
        self.start_date_input = None
        self.end_date_input = None
        self.apply_btn = None
        self.open_folder_btn = None
        self.manual_path_input = None
        
    def build(self):
        """Build the Kivy GUI with styling similar to the UI example"""
        # Main root layout
        root_layout = BoxLayout(orientation="vertical", padding=20, spacing=10)
        root_layout.bind(minimum_height=root_layout.setter('height'))
        
        # Background color (blue gradient effect)
        with root_layout.canvas.before:
            Color(0, 0, 0.5, 1)  # Blue background
            self.rect = Rectangle(pos=root_layout.pos, size=root_layout.size)
        root_layout.bind(pos=self.update_rect, size=self.update_rect)
        
        # Title section
        top_layout = AnchorLayout(anchor_x="center", anchor_y="top", size_hint_y=None, height=80)
        
        title_container = BoxLayout(orientation="vertical", size_hint_y=None, height=80)
        
        title = Label(
            text="AUTOMATED DATA PROCESSOR",
            font_size=24,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=50,
            halign="center",
            valign="middle"
        )
        title.bind(size=title.setter('text_size'))
        
        subtitle = Label(
            text="3-Step Data Processing Workflow (Mac Optimized + Hidden Sheets)",
            font_size=14,
            color=(0.8, 0.8, 0.8, 1),
            size_hint_y=None,
            height=30,
            halign="center",
            valign="middle"
        )
        subtitle.bind(size=subtitle.setter('text_size'))
        
        title_container.add_widget(title)
        title_container.add_widget(subtitle)
        top_layout.add_widget(title_container)
        
        root_layout.add_widget(top_layout)
        
        # Step 1: Project Tracker Selection
        step1_label = Label(
            text="Step 1: Select Project Tracker",
            font_size=16,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=40,
            halign="left",
            valign="middle"
        )
        step1_label.bind(size=step1_label.setter('text_size'))
        root_layout.add_widget(step1_label)
        
        step1_info = Label(
            text="Choose your Excel project tracker file to begin processing (supports hidden sheets)",
            font_size=12,
            color=(0.8, 0.8, 0.8, 1),
            size_hint_y=None,
            height=30,
            halign="left",
            valign="middle"
        )
        step1_info.bind(size=step1_info.setter('text_size'))
        root_layout.add_widget(step1_info)
        
        browse_btn = Button(
            text="Browse for Project Tracker",
            size_hint_y=None,
            height=50,
            background_color=(0.2, 0.6, 0.8, 1),
            color=(1, 1, 1, 1),
            on_press=self.select_project_tracker
        )
        root_layout.add_widget(browse_btn)
        
        # Manual path entry as fallback
        manual_layout = BoxLayout(orientation="horizontal", spacing=5, size_hint_y=None, height=35)
        
        manual_label = Label(
            text="Or enter path manually:",
            font_size=10,
            color=(0.7, 0.7, 0.7, 1),
            size_hint_x=None,
            width=150,
            halign="left",
            valign="middle"
        )
        manual_label.bind(size=manual_label.setter('text_size'))
        
        self.manual_path_input = TextInput(
            hint_text="Paste full path to Project Tracker file here",
            multiline=False,
            size_hint_y=None,
            height=35,
            font_size=10
        )
        self.manual_path_input.bind(text=self.on_manual_path_change)
        
        manual_layout.add_widget(manual_label)
        manual_layout.add_widget(self.manual_path_input)
        root_layout.add_widget(manual_layout)
        
        self.tracker_status_label = Label(
            text="No file selected",
            font_size=12,
            color=(1, 0.5, 0.5, 1),
            size_hint_y=None,
            height=30,
            halign="left",
            valign="middle"
        )
        self.tracker_status_label.bind(size=self.tracker_status_label.setter('text_size'))
        root_layout.add_widget(self.tracker_status_label)
        
        # Step 2: Date Range Selection
        step2_label = Label(
            text="Step 2: Select Date Range",
            font_size=16,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=40,
            halign="left",
            valign="middle"
        )
        step2_label.bind(size=step2_label.setter('text_size'))
        root_layout.add_widget(step2_label)
        
        step2_info = Label(
            text="Choose the date range for filtering artwork release dates",
            font_size=12,
            color=(0.8, 0.8, 0.8, 1),
            size_hint_y=None,
            height=30,
            halign="left",
            valign="middle"
        )
        step2_info.bind(size=step2_info.setter('text_size'))
        root_layout.add_widget(step2_info)
        
        # Date inputs
        date_layout = BoxLayout(orientation="horizontal", spacing=10, size_hint_y=None, height=50)
        
        self.start_date_input = TextInput(
            hint_text="Start Date (YYYY-MM-DD)",
            multiline=False,
            size_hint_y=None,
            height=50
        )
        
        self.end_date_input = TextInput(
            hint_text="End Date (YYYY-MM-DD)",
            multiline=False,
            size_hint_y=None,
            height=50
        )
        
        date_layout.add_widget(self.start_date_input)
        date_layout.add_widget(self.end_date_input)
        root_layout.add_widget(date_layout)
        
        # Set default dates (last 90 days)
        current_date = datetime.now().date()
        start_date = current_date - pd.Timedelta(days=90)
        self.start_date_input.text = start_date.strftime('%Y-%m-%d')
        self.end_date_input.text = current_date.strftime('%Y-%m-%d')
        
        self.apply_btn = Button(
            text="Apply Date Filter & Start Processing",
            size_hint_y=None,
            height=50,
            background_color=(0, 0.8, 0, 1),
            color=(1, 1, 1, 1),
            on_press=self.apply_date_filter,
            disabled=True
        )
        root_layout.add_widget(self.apply_btn)
        
        # Step 3: Output Location
        step3_label = Label(
            text="Step 3: Output Location",
            font_size=16,
            bold=True,
            color=(1, 1, 1, 1),
            size_hint_y=None,
            height=40,
            halign="left",
            valign="middle"
        )
        step3_label.bind(size=step3_label.setter('text_size'))
        root_layout.add_widget(step3_label)
        
        output_info = Label(
            text="Two Excel files will be saved: SharePoint Combined Data & Final Formatted Data",
            font_size=12,
            color=(0.8, 0.8, 0.8, 1),
            size_hint_y=None,
            height=30,
            halign="left",
            valign="middle"
        )
        output_info.bind(size=output_info.setter('text_size'))
        root_layout.add_widget(output_info)
        
        output_path_label = Label(
            text=f"Output Folder: {getattr(self, 'output_folder', 'Desktop/Automated_Data_Processing_Output')}",
            font_size=10,
            color=(0.7, 0.9, 1, 1),
            size_hint_y=None,
            height=25,
            halign="left",
            valign="middle"
        )
        output_path_label.bind(size=output_path_label.setter('text_size'))
        root_layout.add_widget(output_path_label)
        
        self.open_folder_btn = Button(
            text="Open Output Folder",
            size_hint_y=None,
            height=50,
            background_color=(1, 0.5, 0, 1),
            color=(1, 1, 1, 1),
            on_press=self.open_output_folder,
            disabled=True
        )
        root_layout.add_widget(self.open_folder_btn)
        
        # Status and Progress
        self.status_label = Label(
            text="Status: Ready to process data (hidden sheet support enabled)",
            font_size=14,
            bold=True,
            color=(0.8, 0.8, 0.8, 1),
            size_hint_y=None,
            height=40,
            halign="center",
            valign="middle"
        )
        self.status_label.bind(size=self.status_label.setter('text_size'))
        root_layout.add_widget(self.status_label)
        
        self.progress_bar = ProgressBar(
            max=100,
            value=0,
            size_hint_y=None,
            height=20
        )
        root_layout.add_widget(self.progress_bar)
        
        # Exit button
        exit_btn = Button(
            text="Exit",
            size_hint_y=None,
            height=50,
            background_color=(0.8, 0, 0, 1),
            color=(1, 1, 1, 1),
            on_press=self.stop
        )
        root_layout.add_widget(exit_btn)
        
        # Footer
        footer = Label(
            text="Developed for Mac - SharePoint Data Processing + Hidden Sheet Support",
            font_size=12,
            color=(0.6, 0.6, 0.6, 1),
            size_hint_y=None,
            height=30,
            halign="center",
            valign="middle"
        )
        footer.bind(size=footer.setter('text_size'))
        root_layout.add_widget(footer)
        
        # Check SharePoint access after UI is built
        Clock.schedule_once(self.check_sharepoint_after_build, 0.5)
        
        return root_layout
    
    def check_sharepoint_after_build(self, dt):
        """Check SharePoint access after GUI is built"""
        self.sharepoint_access_ok = self.check_sharepoint_access()
        
        if not self.sharepoint_access_ok:
            self.update_status("WARNING: SharePoint access not found - limited functionality")
            self.tracker_status_label.text = "SharePoint directories not accessible"
            self.tracker_status_label.color = (1, 0.5, 0, 1)  # Orange warning
        else:
            self.update_status("Ready to process data - SharePoint access confirmed")
    
    def update_rect(self, instance, value):
        """Update background rectangle"""
        self.rect.pos = instance.pos
        self.rect.size = instance.size
    
    def check_sharepoint_access(self):
        """Check if user has access to SharePoint directories with Mac-optimized paths"""
        try:
            is_mac = platform.system() == 'Darwin'
            
            if is_mac:
                # Mac-optimized SharePoint path detection
                possible_base_paths = [
                    os.path.expanduser("~/Lowe's Companies Inc"),
                    os.path.expanduser("~/Lowe's Companies Inc - Personal"),
                    os.path.expanduser("~/OneDrive - Lowe's Companies Inc"),
                    os.path.expanduser("~/OneDrive/Lowe's Companies Inc"),
                    os.path.expanduser("~/Library/CloudStorage/OneDrive-Lowe'sCompaniesInc"),
                    os.path.expanduser("~/Documents/Lowe's Companies Inc")
                ]
            else:
                possible_base_paths = [
                    "C:\\Users\\mjayash\\Lowe's Companies Inc",
                    f"C:\\Users\\{os.getenv('USERNAME')}\\Lowe's Companies Inc"
                ]
            
            # Check each possible base path
            for base_path in possible_base_paths:
                if os.path.exists(base_path):
                    sharepoint_subdirs = [
                        "Private Brands - Packaging Operations - Building Products",
                        "Private Brands - Packaging Operations - Hardlines & Seasonal",
                        "Private Brands - Packaging Operations - Home Décor"
                    ]
                    
                    for subdir in sharepoint_subdirs:
                        full_path = os.path.join(base_path, subdir)
                        if os.path.exists(full_path):
                            return True
                            
            return False
            
        except Exception as e:
            print(f"SharePoint access check error: {e}")
            return False
    
    def setup_paths(self):
        """Setup paths with Mac-optimized SharePoint detection"""
        self.is_mac = platform.system() == 'Darwin'
        
        if self.is_mac:
            possible_base_paths = [
                os.path.expanduser("~/Lowe's Companies Inc"),
                os.path.expanduser("~/Lowe's Companies Inc - Personal"),
                os.path.expanduser("~/OneDrive - Lowe's Companies Inc"),
                os.path.expanduser("~/OneDrive/Lowe's Companies Inc"),
                os.path.expanduser("~/Library/CloudStorage/OneDrive-Lowe'sCompaniesInc"),
                os.path.expanduser("~/Documents/Lowe's Companies Inc")
            ]
            
            base_path = None
            for path in possible_base_paths:
                if os.path.exists(path):
                    base_path = path
                    break
            
            if not base_path:
                base_path = os.path.expanduser("~/Lowe's Companies Inc")
        else:
            base_path = "C:\\Users\\mjayash\\Lowe's Companies Inc"
        
        # SharePoint paths
        self.sharepoint_paths = [
            os.path.join(base_path, "Private Brands - Packaging Operations - Building Products"),
            os.path.join(base_path, "Private Brands - Packaging Operations - Hardlines & Seasonal"),
            os.path.join(base_path, "Private Brands - Packaging Operations - Home Décor")
        ]
        
        # Default project tracker path
        self.default_project_tracker_path = os.path.join(base_path, "Private Brands Packaging File Transfer - PQM Compliance reporting", "Project tracker.xlsx")
        
        # Output folder
        if self.is_mac:
            desktop = os.path.expanduser("~/Desktop")
        else:
            desktop = os.path.join(os.path.expanduser("~"), "Desktop")
            
        self.output_folder = os.path.join(desktop, "Automated_Data_Processing_Output")
        
        # Ensure output directory exists
        try:
            os.makedirs(self.output_folder, exist_ok=True)
            if self.is_mac:
                os.chmod(self.output_folder, 0o755)
        except Exception as e:
            print(f"Error creating output folder: {e}")
    
    def log_message(self, message):
        """Store log messages in background"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}"
        self.processing_logs.append(formatted_message)
    
    def select_project_tracker(self, instance):
        """Select project tracker file using native Mac dialogs"""
        try:
            initial_dir = os.path.dirname(self.default_project_tracker_path) if os.path.exists(self.default_project_tracker_path) else os.path.expanduser("~")
            
            def select_with_mac_dialog():
                try:
                    # Method 1: Try AppleScript for native Mac dialog
                    if platform.system() == 'Darwin':
                        try:
                            applescript = f'''
                            tell application "System Events"
                                set theFile to choose file with prompt "Select Project Tracker Excel File" ¬
                                    default location "{initial_dir}" ¬
                                    of type {{"org.openxmlformats.spreadsheetml.sheet", "com.microsoft.excel.xls"}}
                                return POSIX path of theFile
                            end tell
                            '''
                            
                            result = subprocess.run(['osascript', '-e', applescript], 
                                                  capture_output=True, text=True, timeout=60)
                            
                            if result.returncode == 0 and result.stdout.strip():
                                file_path = result.stdout.strip()
                                Clock.schedule_once(lambda dt: self.update_file_selection(os.path.basename(file_path)), 0)
                                self.project_tracker_path = file_path
                                return
                                
                        except Exception as applescript_error:
                            print(f"AppleScript dialog failed: {applescript_error}")
                    
                    # Method 2: Fallback to tkinter (still native on Mac)
                    Clock.schedule_once(lambda dt: setattr(self.tracker_status_label, 'text', 'Opening file dialog...'), 0)
                    Clock.schedule_once(lambda dt: setattr(self.tracker_status_label, 'color', (1, 1, 0, 1)), 0)
                    
                    root = tk.Tk()
                    root.withdraw()
                    root.wm_attributes('-topmost', True)
                    
                    file_path = filedialog.askopenfilename(
                        title="Select Project Tracker Excel File",
                        filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
                        initialdir=initial_dir,
                        parent=root
                    )
                    
                    root.quit()
                    root.destroy()
                    
                    if file_path:
                        Clock.schedule_once(lambda dt: self.update_file_selection(os.path.basename(file_path)), 0)
                        self.project_tracker_path = file_path
                    else:
                        Clock.schedule_once(lambda dt: setattr(self.tracker_status_label, 'text', 'No file selected'), 0)
                        Clock.schedule_once(lambda dt: setattr(self.tracker_status_label, 'color', (1, 0.5, 0.5, 1)), 0)
                        
                except Exception as e:
                    Clock.schedule_once(lambda dt: setattr(self.tracker_status_label, 'text', f'Dialog error: {str(e)[:50]}...'), 0)
                    Clock.schedule_once(lambda dt: setattr(self.tracker_status_label, 'color', (1, 0.5, 0.5, 1)), 0)
            
            # Run in separate thread
            import threading
            threading.Thread(target=select_with_mac_dialog, daemon=True).start()
            
        except Exception as e:
            self.show_popup("Error", f"Error opening file chooser: {str(e)}\n\nTry manually entering the file path.")
    
    def on_manual_path_change(self, instance, text):
        """Handle manual path entry"""
        if text and text.strip():
            file_path = text.strip()
            if os.path.exists(file_path) and file_path.lower().endswith(('.xlsx', '.xls')):
                self.project_tracker_path = file_path
                filename = os.path.basename(file_path)
                
                self.tracker_status_label.text = f"Manual entry: {filename}"
                self.tracker_status_label.color = (0.5, 1, 0.5, 1)  # Green
                self.apply_btn.disabled = False
                self.log_message(f"Project tracker manually entered: {filename}")
            elif text.strip():  # User is typing but file doesn't exist yet
                self.tracker_status_label.text = "Checking path..."
                self.tracker_status_label.color = (1, 1, 0, 1)  # Yellow
    
    def update_file_selection(self, filename):
        """Update UI after file selection (called from main thread)"""
        self.tracker_status_label.text = f"Selected: {filename}"
        self.tracker_status_label.color = (0.5, 1, 0.5, 1)  # Green
        self.apply_btn.disabled = False
        self.log_message(f"Project tracker selected: {filename}")
    
    def apply_date_filter(self, instance):
        """Apply date filter and start processing"""
        try:
            # Check SharePoint access first
            if not self.sharepoint_access_ok:
                self.show_popup("SharePoint Access Required", 
                    "This application requires access to Lowe's SharePoint directories.\n\n"
                    "On Mac, SharePoint may sync to one of these locations:\n"
                    "• ~/Lowe's Companies Inc\n"
                    "• ~/OneDrive - Lowe's Companies Inc\n"
                    "• ~/Library/CloudStorage/OneDrive-Lowe'sCompaniesInc\n\n"
                    "Please ensure SharePoint is synced and accessible, then restart the application.\n\n"
                    "Contact IT support if you need SharePoint access.")
                return
            
            start_str = self.start_date_input.text.strip()
            end_str = self.end_date_input.text.strip()
            
            if not start_str or not end_str:
                self.show_popup("Error", "Please enter both start and end dates")
                return
            
            start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
            
            if start_date > end_date:
                self.show_popup("Error", "Start date must be before or equal to end date")
                return
            
            # Disable apply button during processing
            self.apply_btn.disabled = True
            
            # Start automated processing
            self.run_automated_workflow(start_date, end_date)
            
        except ValueError:
            self.show_popup("Error", "Please enter dates in YYYY-MM-DD format")
        except Exception as e:
            self.show_popup("Error", f"Error starting processing: {str(e)}")
    
    @mainthread
    def update_status(self, message):
        """Update status label"""
        if self.status_label:
            self.status_label.text = f"Status: {message}"
    
    @mainthread
    def update_progress(self, value):
        """Update progress bar"""
        if self.progress_bar:
            self.progress_bar.value = value
    
    @mainthread
    def show_popup(self, title, message):
        """Show popup message"""
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        message_label = Label(
            text=message,
            text_size=(None, None),
            halign="center",
            valign="middle"
        )
        
        btn = Button(
            text="OK",
            size_hint_y=None,
            height=50,
            background_color=(0.2, 0.6, 0.8, 1),
            color=(1, 1, 1, 1)
        )
        
        popup = Popup(
            title=title,
            content=content,
            size_hint=(0.8, 0.6)
        )
        
        btn.bind(on_press=popup.dismiss)
        content.add_widget(message_label)
        content.add_widget(btn)
        popup.open()
    
    @mainthread
    def show_success_popup(self, message):
        """Show success popup with detailed results"""
        content = BoxLayout(orientation='vertical', padding=10, spacing=10)
        
        message_label = Label(
            text=message,
            text_size=(None, None),
            halign="center",
            valign="middle"
        )
        
        btn = Button(
            text="OK",
            size_hint_y=None,
            height=50,
            background_color=(0, 0.8, 0, 1),
            color=(1, 1, 1, 1)
        )
        
        popup = Popup(
            title="Processing Complete!",
            content=content,
            size_hint=(0.9, 0.8)
        )
        
        btn.bind(on_press=popup.dismiss)
        content.add_widget(message_label)
        content.add_widget(btn)
        popup.open()
        
        # Enable output folder button
        self.open_folder_btn.disabled = False
    
    def open_output_folder(self, instance):
        """Open output folder"""
        try:
            if platform.system() == 'Darwin':
                subprocess.run(['open', self.output_folder], check=True)
            elif platform.system() == 'Windows':
                os.startfile(self.output_folder)
            else:
                subprocess.run(['xdg-open', self.output_folder], check=True)
        except Exception as e:
            self.show_popup("Error", f"Could not open folder: {str(e)}")
    
    def run_automated_workflow(self, start_date, end_date):
        """Run complete automated workflow in background thread"""
        def process_thread():
            try:
                total_start = time.time()
                self.log_message("Starting automated workflow...")
                
                # Update status and progress
                self.update_status("Processing... Please wait (this may take several minutes)")
                self.update_progress(10)
                
                # Step 1: Scan production folders
                self.update_status("Scanning production folders...")
                if not self.scan_production_folders():
                    raise Exception("No production files found")
                self.update_progress(20)
                
                # Step 2: Extract production data (ENHANCED WITH HIDDEN SHEET SUPPORT)
                self.update_status("Extracting production data (including hidden sheets)...")
                if not self.intelligent_data_extraction():
                    raise Exception("Production data extraction failed")
                self.update_progress(40)
                
                # Step 3: Process project tracker (ENHANCED WITH HIDDEN SHEET SUPPORT)
                self.update_status("Processing project tracker (including hidden sheets)...")
                if not self.process_project_tracker():
                    raise Exception("Project tracker processing failed")
                self.update_progress(60)
                
                # Step 4: Combine datasets
                self.update_status("Combining datasets...")
                if not self.combine_datasets():
                    raise Exception("Data combination failed")
                self.update_progress(70)
                
                # Step 5: Filter by date range
                self.update_status("Filtering by date range...")
                if not self.filter_by_date_range(start_date, end_date):
                    raise Exception("Date filtering failed")
                self.update_progress(80)
                
                # Step 6: Format final output
                self.update_status("Formatting final output...")
                if not self.format_final_output():
                    raise Exception("Final output formatting failed")
                self.update_progress(90)
                
                # Step 7: Save all outputs
                self.update_status("Saving output files...")
                output_files = self.save_all_outputs(start_date, end_date)
                self.update_progress(100)
                
                total_time = time.time() - total_start
                
                # Show success message
                final_records = len(self.final_output_data)
                combined_records = len(self.consolidated_data)
                
                if final_records == 0:
                    success_msg = (
                        f"Processing Completed Successfully!\n\n"
                        f"Total Time: {total_time:.1f} seconds\n"
                        f"Date Range: {start_date} to {end_date}\n"
                        f"SharePoint Combined Records: {combined_records:,}\n"
                        f"Final Records: {final_records:,} (No records in date range)\n"
                        f"Files Created: {len(output_files)}\n\n"
                        f"ENHANCED: Hidden/protected sheets were processed\n\n"
                        f"NOTE: No records found in the specified date range.\n"
                        f"This may be normal if no artwork was released in this period.\n\n"
                        f"Files saved to Desktop → Automated_Data_Processing_Output\n"
                        f"• Combined_Data_[date].xlsx (all SharePoint production files)\n"
                        f"• Final_Output_[date].xlsx (empty - no records in date range)"
                    )
                else:
                    success_msg = (
                        f"Processing Completed Successfully!\n\n"
                        f"Total Time: {total_time:.1f} seconds\n"
                        f"Date Range: {start_date} to {end_date}\n"
                        f"SharePoint Combined Records: {combined_records:,}\n"
                        f"Final Records: {final_records:,}\n"
                        f"Output Columns: {len(self.final_columns)}\n"
                        f"Files Created: {len(output_files)}\n\n"
                        f"ENHANCED: Hidden/protected sheets were processed\n\n"
                        f"All files saved to Desktop → Automated_Data_Processing_Output\n"
                        f"• Combined_Data_[date].xlsx (all SharePoint production files)\n"
                        f"• Final_Output_[date].xlsx (formatted final data)"
                    )
                
                self.update_status("Processing completed successfully!")
                self.show_success_popup(success_msg)
                
            except Exception as e:
                self.update_progress(0)
                self.update_status("Processing failed. Please check your data and try again.")
                self.log_message(f"Error: {str(e)}")
                self.show_popup("Error", f"Processing failed: {str(e)}")
        
        # Start processing in background thread
        threading.Thread(target=process_thread, daemon=True).start()
    
    # ========== DATA PROCESSING METHODS (SAME AS ORIGINAL EXCEPT ENHANCED EXCEL READING) ==========
    
    def scan_production_folders(self):
        """Scan for production item list folders with Mac-optimized file handling"""
        self.log_message("Scanning production folders...")
        
        if not self.sharepoint_access_ok:
            self.log_message("SharePoint access not available - cannot scan production folders")
            return False
        
        all_files = []
        
        for sp_path in self.sharepoint_paths:
            if not os.path.exists(sp_path):
                continue
                
            try:
                for root, dirs, files in os.walk(sp_path):
                    if self.is_mac:
                        dirs[:] = [d for d in dirs if not d.startswith('.')]
                    
                    if root.endswith("_Production Item List"):
                        excel_files = [f for f in files 
                                     if f.lower().endswith(('.xlsx', '.xls', '.xlsm')) 
                                     and not f.startswith(('~', '.', '$', 'Icon\r'))]
                        
                        for excel_file in excel_files:
                            full_path = os.path.join(root, excel_file)
                            if os.access(full_path, os.R_OK):
                                all_files.append(full_path)
            except Exception as e:
                self.log_message(f"Error scanning {sp_path}: {str(e)}")
        
        self.production_files = all_files
        self.log_message(f"Found {len(all_files)} production files")
        return len(all_files) > 0
    
    def intelligent_data_extraction(self):
        """Extract data with intelligent header detection and ENHANCED Excel reading for hidden sheets"""
        self.log_message("Extracting production data with hidden sheet support...")
        
        column_patterns = {
            'Item Number': ['item #', 'item#', 'itemnumber', 'item number', 'item no', 'itemno'],
            'Product Vendor Company Name': ['vendor name', 'vendorname', 'vendor', 'supplier'],
            'Brand': ['brand', 'brandname', 'brand name'],
            'Product Name': ['item description', 'itemdescription', 'description', 'product description', 'desc', 'product name'],
            'SKU New/Existing': ['SKU', 'SKU new/existing', 'SKU new existing', 'SKU new/carry forward', 'SKU new carry forward', 'SKU new']
        }
        
        def extract_from_file(file_path):
            try:
                # ENHANCEMENT: Try to read ALL sheets from the Excel file, including hidden ones
                extracted_sheets = []
                
                try:
                    # First, get all sheet names using ExcelFile (this includes hidden sheets)
                    if self.is_mac:
                        try:
                            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
                        except:
                            excel_file = pd.ExcelFile(file_path, engine='xlrd')
                    else:
                        excel_file = pd.ExcelFile(file_path)
                    
                    all_sheet_names = excel_file.sheet_names
                    self.log_message(f"Found {len(all_sheet_names)} sheets in {os.path.basename(file_path)} (including hidden)")
                    
                    # Try to extract data from each sheet
                    for sheet_name in all_sheet_names:
                        try:
                            # Read this specific sheet
                            if self.is_mac:
                                try:
                                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=1000, engine='openpyxl')
                                except:
                                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=1000, engine='xlrd')
                            else:
                                df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, nrows=1000)
                            
                            if df.empty:
                                continue
                            
                            # Apply existing extraction logic to this sheet
                            sheet_data = self.extract_from_single_sheet(df, file_path, sheet_name, column_patterns)
                            if not sheet_data.empty:
                                extracted_sheets.append(sheet_data)
                                self.log_message(f"Extracted {len(sheet_data)} records from sheet '{sheet_name}'")
                        
                        except Exception as sheet_error:
                            self.log_message(f"Could not process sheet '{sheet_name}': {str(sheet_error)}")
                            continue
                
                except Exception as file_error:
                    self.log_message(f"Could not read sheets from {os.path.basename(file_path)}: {str(file_error)}")
                    # Fallback to original single-sheet method
                    return self.extract_from_single_file_original(file_path, column_patterns)
                
                # Combine all sheet data for this file
                if extracted_sheets:
                    combined_file_data = pd.concat(extracted_sheets, ignore_index=True)
                    # Remove duplicates within the same file
                    combined_file_data = combined_file_data.drop_duplicates(subset=['Item Number'], keep='first')
                    self.log_message(f"Total extracted from {os.path.basename(file_path)}: {len(combined_file_data)} records from {len(extracted_sheets)} sheets")
                    return combined_file_data
                else:
                    self.log_message(f"No data extracted from any sheet in {os.path.basename(file_path)}")
                    return pd.DataFrame()
                
            except Exception as e:
                self.log_message(f"Error processing file {os.path.basename(file_path)}: {str(e)}")
                return pd.DataFrame()
        
        # Process files in parallel
        all_extracted_data = []
        max_workers = 4 if self.is_mac else 6
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(extract_from_file, file_path) for file_path in self.production_files]
            
            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    all_extracted_data.append(result)
        
        # Consolidate data
        if all_extracted_data:
            self.consolidated_data = pd.concat(all_extracted_data, ignore_index=True)
            self.consolidated_data = self.consolidated_data.drop_duplicates(subset=['Item Number', 'Source_File'], keep='first')
            
            self.log_message("Cleaning and trimming consolidated data...")
            
            text_columns = ['Product Vendor Company Name', 'Brand', 'Product Name', 'SKU New/Existing', 'Source_File', 'Source_Folder']
            for col in text_columns:
                if col in self.consolidated_data.columns:
                    self.consolidated_data[col] = self.consolidated_data[col].astype(str).str.strip()
                    self.consolidated_data[col] = self.consolidated_data[col].str.replace(r'\s+', ' ', regex=True)
                    self.consolidated_data[col] = self.consolidated_data[col].replace(['nan', 'None', 'NaN'], '')
            
            if 'Item Number' in self.consolidated_data.columns:
                def clean_item_number_comprehensive(value):
                    try:
                        if pd.isna(value):
                            return ''
                        
                        clean_val = str(value).strip()
                        
                        if clean_val.lower() in ['nan', 'none', 'null', '']:
                            return ''
                        
                        numbers_only = re.sub(r'[^\d]', '', clean_val)
                        
                        if numbers_only and numbers_only.isdigit() and len(numbers_only) > 0:
                            return str(int(numbers_only))
                        
                        return ''
                    except:
                        return ''
                
                self.consolidated_data['Item Number'] = self.consolidated_data['Item Number'].apply(clean_item_number_comprehensive)
                
                before_count = len(self.consolidated_data)
                
                self.consolidated_data = self.consolidated_data[
                    (self.consolidated_data['Item Number'] != '') & 
                    (self.consolidated_data['Item Number'].notna())
                ]
                
                after_count = len(self.consolidated_data)
                self.log_message(f"Item Number cleaning: {before_count} -> {after_count} records (removed {before_count - after_count} empty/invalid)")
            
            self.consolidated_data = self.consolidated_data.fillna('')
            
            self.log_message(f"Extracted and cleaned {len(self.consolidated_data)} records with valid Item Numbers (INCLUDING HIDDEN SHEETS)")
            return True
        else:
            self.log_message("No data extracted")
            return False
    
    def extract_from_single_sheet(self, df, file_path, sheet_name, column_patterns):
        """Extract data from a single sheet (NEW method for enhanced extraction)"""
        try:
            best_extraction = pd.DataFrame()
            best_score = 0
            
            for potential_header_row in range(min(50, len(df))):
                try:
                    potential_headers = df.iloc[potential_header_row].astype(str).str.lower().str.strip()
                    
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
                            if self.is_mac:
                                try:
                                    full_df = pd.read_excel(file_path, sheet_name=sheet_name, header=potential_header_row, nrows=10000, engine='openpyxl')
                                except:
                                    full_df = pd.read_excel(file_path, sheet_name=sheet_name, header=potential_header_row, nrows=10000, engine='xlrd')
                            else:
                                full_df = pd.read_excel(file_path, sheet_name=sheet_name, header=potential_header_row, nrows=10000)
                            
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
                                
                                # Clean Item Number
                                if 'Item Number' in extracted_data.columns:
                                    def clean_item_number_aggressive(value):
                                        try:
                                            if pd.isna(value):
                                                return ''
                                            
                                            clean_val = str(value).replace(' ', '').replace('\t', '').replace('\n', '').replace('\r', '').strip()
                                            
                                            if clean_val.lower() in ['nan', 'none', 'null', '']:
                                                return ''
                                            
                                            if 'e+' in clean_val.lower() or 'e-' in clean_val.lower():
                                                try:
                                                    float_val = float(clean_val)
                                                    clean_val = f"{float_val:.0f}"
                                                except:
                                                    pass
                                            
                                            if '.' in clean_val:
                                                clean_val = clean_val.split('.')[0]
                                            
                                            numbers_only = re.sub(r'[^\d]', '', clean_val)
                                            
                                            if numbers_only and numbers_only.isdigit() and len(numbers_only) > 0:
                                                return str(int(numbers_only))
                                            
                                            return ''
                                        except Exception as e:
                                            return ''
                                    
                                    extracted_data['Item Number'] = extracted_data['Item Number'].apply(clean_item_number_aggressive)
                                    
                                    extracted_data = extracted_data[
                                        (extracted_data['Item Number'] != '') & 
                                        (extracted_data['Item Number'] != 0) &
                                        (extracted_data['Item Number'].notna())
                                    ]
                                    
                                    extracted_data['Item Number'] = extracted_data['Item Number'].astype(str)
                                
                                if 'Item Number' in extracted_data.columns:
                                    valid_items = extracted_data['Item Number'] != ''
                                    extracted_data = extracted_data[valid_items]
                                
                                if len(extracted_data) > 0:
                                    file_name = os.path.basename(file_path)
                                    extracted_data['Source_File'] = file_name
                                    extracted_data['Source_Folder'] = os.path.basename(os.path.dirname(file_path))
                                    extracted_data['Source_Sheet'] = sheet_name  # NEW: Track which sheet data came from
                                    
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
    
    def extract_from_single_file_original(self, file_path, column_patterns):
        """Original single-file extraction method (fallback)"""
        try:
            if self.is_mac:
                try:
                    df = pd.read_excel(file_path, header=None, nrows=1000, engine='openpyxl')
                except:
                    df = pd.read_excel(file_path, header=None, nrows=1000, engine='xlrd')
            else:
                df = pd.read_excel(file_path, header=None, nrows=1000)
                
            if df.empty:
                return pd.DataFrame()
            
            return self.extract_from_single_sheet(df, file_path, "default", column_patterns)
                
        except Exception:
            return pd.DataFrame()
    
    def process_project_tracker(self):
        """Process project tracker file with ENHANCED Excel reading for hidden sheets"""
        try:
            if not self.project_tracker_path or not os.path.exists(self.project_tracker_path):
                return False
            
            self.log_message("Processing project tracker with hidden sheet support...")
            
            # ENHANCEMENT: Try to read from multiple sheets if available
            best_result = None
            best_score = 0
            
            try:
                # First get all sheet names from the project tracker
                if self.is_mac:
                    try:
                        excel_file = pd.ExcelFile(self.project_tracker_path, engine='openpyxl')
                    except:
                        excel_file = pd.ExcelFile(self.project_tracker_path, engine='xlrd')
                else:
                    excel_file = pd.ExcelFile(self.project_tracker_path)
                
                all_sheet_names = excel_file.sheet_names
                self.log_message(f"Found {len(all_sheet_names)} sheets in project tracker (including hidden)")
                
                # Try each sheet to find the one with project tracker data
                for sheet_name in all_sheet_names:
                    try:
                        if self.is_mac:
                            try:
                                df = pd.read_excel(self.project_tracker_path, sheet_name=sheet_name, engine='openpyxl')
                            except:
                                df = pd.read_excel(self.project_tracker_path, sheet_name=sheet_name, engine='xlrd')
                        else:
                            df = pd.read_excel(self.project_tracker_path, sheet_name=sheet_name)
                        
                        # Try to process this sheet as project tracker data
                        result = self.process_single_tracker_sheet(df, sheet_name)
                        if result is not None and len(result) > best_score:
                            best_result = result
                            best_score = len(result)
                            self.log_message(f"Found good project tracker data in sheet '{sheet_name}' with {len(result)} records")
                    
                    except Exception as sheet_error:
                        self.log_message(f"Could not process tracker sheet '{sheet_name}': {str(sheet_error)}")
                        continue
                
                if best_result is not None:
                    self.project_tracker_data = best_result
                    self.log_message(f"Project tracker processing completed: {len(best_result)} records (ENHANCED: checked all sheets)")
                    return True
                else:
                    self.log_message("No valid project tracker data found in any sheet")
                    return False
            
            except Exception as file_error:
                self.log_message(f"Could not read project tracker sheets: {str(file_error)}")
                # Fallback to original method
                return self.process_project_tracker_original()
            
        except Exception as e:
            self.log_message(f"Project tracker error: {str(e)}")
            return False
    
    def process_single_tracker_sheet(self, df, sheet_name):
        """Process a single sheet as project tracker data (NEW method)"""
        try:
            def find_column(df, possible_names):
                df_cols_lower = [col.lower() for col in df.columns]
                for name in possible_names:
                    name_lower = name.lower()
                    for i, col in enumerate(df_cols_lower):
                        if name_lower in col or col in name_lower:
                            return df.columns[i]
                return None
            
            # Column mappings (same as original)
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
                return None  # This sheet doesn't have the required project tracker structure
            
            # Filter data (same as original logic)
            rounds_col = found_columns['Rounds']
            filter_values = ["File Release", "File Re-Release R2", "File Re-Release R3"]
            mask = df[rounds_col].isin(filter_values)
            filtered_df = df[mask].copy()
            
            if len(filtered_df) == 0:
                return None
            
            # Create result dataframe (same as original logic)
            result = pd.DataFrame(index=filtered_df.index)
            
            # Map all columns
            for target_name, source_col in found_columns.items():
                if target_name == 'Artwork Release Date':
                    release_dates = filtered_df[source_col]
                    date_mask = pd.notna(release_dates) & (release_dates != "")
                    result[target_name] = ""
                    if date_mask.any():
                        valid_dates = pd.to_datetime(release_dates[date_mask], errors='coerce')
                        formatted_dates = valid_dates.dt.strftime("%d/%m/%y")
                        result.loc[date_mask, target_name] = formatted_dates
                else:
                    result[target_name] = filtered_df[source_col].fillna("")
            
            # Calculate Re-Release Status (same as original)
            rounds_upper = filtered_df[found_columns['Rounds']].astype(str).str.upper()
            re_release_status = np.where(
                rounds_upper.str.contains('R2|R3', na=False, regex=True), 
                'Yes', 
                ''
            )
            result['Re-Release Status'] = re_release_status
            
            return result
            
        except Exception as e:
            return None
    
    def process_project_tracker_original(self):
        """Original project tracker processing method (fallback)"""
        try:
            if self.is_mac:
                try:
                    df = pd.read_excel(self.project_tracker_path, engine='openpyxl')
                except:
                    df = pd.read_excel(self.project_tracker_path, engine='xlrd')
            else:
                df = pd.read_excel(self.project_tracker_path)
            
            result = self.process_single_tracker_sheet(df, "default")
            if result is not None:
                self.project_tracker_data = result
                self.log_message(f"Processed {len(result)} project tracker records (original method)")
                return True
            else:
                return False
                
        except Exception as e:
            self.log_message(f"Original project tracker processing error: {str(e)}")
            return False
    
    # ========== REST OF THE METHODS REMAIN EXACTLY THE SAME ==========
    
    def combine_datasets(self):
        """Combine datasets with enhanced number cleaning"""
        try:
            self.log_message("Combining datasets...")
            
            if self.consolidated_data.empty or self.project_tracker_data.empty:
                return False
            
            step1_data = self.consolidated_data.copy()
            step2_data = self.project_tracker_data.copy()
            
            # Enhanced number cleaning
            def clean_to_number(value):
                try:
                    if pd.isna(value) or str(value).strip() == '' or str(value).lower() in ['nan', 'none', 'null']:
                        return ''
                    
                    clean_val = str(value).strip()
                    
                    if 'e+' in clean_val.lower() or 'e-' in clean_val.lower():
                        try:
                            float_val = float(clean_val)
                            clean_val = f"{float_val:.0f}"
                        except:
                            pass
                    
                    if '.' in clean_val:
                        clean_val = clean_val.split('.')[0]
                    
                    numbers_only = re.sub(r'[^\d]', '', clean_val)
                    
                    if numbers_only and numbers_only.isdigit():
                        return str(int(numbers_only))
                    
                    return ''
                except:
                    return ''
            
            # Clean merge keys
            step1_data['Merge_Key'] = step1_data['Item Number'].apply(clean_to_number)
            step2_data['Merge_Key'] = step2_data['PKG1'].apply(clean_to_number)
            
            # Remove empty keys
            step1_valid = step1_data[step1_data['Merge_Key'] != ''].copy()
            step2_valid = step2_data[step2_data['Merge_Key'] != ''].copy()
            
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
            return True
            
        except Exception as e:
            self.log_message(f"Combination error: {str(e)}")
            return False
    
    def filter_by_date_range(self, start_date, end_date):
        """Filter by date range with enhanced error handling"""
        try:
            self.log_message(f"Filtering by date range: {start_date} to {end_date}")
            
            if self.combined_data.empty:
                self.log_message("Combined data is empty - cannot filter by date")
                return False
            
            self.log_message(f"Combined data has {len(self.combined_data)} records before date filtering")
            
            # Enhanced date column search
            date_column = None
            possible_date_columns = [
                'artwork release date', 'release date', 'releasedate', 
                'date', 'artwork date', 'artworkreleasedate'
            ]
            
            for col in self.combined_data.columns:
                for possible_name in possible_date_columns:
                    if possible_name.lower() in col.lower().replace(' ', '').replace('_', ''):
                        date_column = col
                        break
                if date_column:
                    break
            
            if not date_column:
                for col in self.combined_data.columns:
                    if 'date' in col.lower():
                        date_column = col
                        self.log_message(f"Using fallback date column: {date_column}")
                        break
            
            if not date_column:
                self.log_message("ERROR: No date column found at all - skipping date filter")
                self.log_message("Proceeding without date filtering...")
                return True
            
            self.log_message(f"Using date column: '{date_column}'")
            filtered_df = self.combined_data.copy()
            
            # Enhanced date parsing function
            def parse_date_enhanced(date_val):
                try:
                    if pd.isna(date_val) or str(date_val).strip() == '' or str(date_val).lower() in ['nan', 'none', 'nat', 'null']:
                        return None
                    
                    date_str = str(date_val).strip()
                    
                    date_formats = [
                        '%d/%m/%y', '%d/%m/%Y',
                        '%m/%d/%y', '%m/%d/%Y',
                        '%Y-%m-%d', '%Y/%m/%d',
                        '%d-%m-%Y', '%d-%m-%y',
                        '%Y%m%d'
                    ]
                    
                    for fmt in date_formats:
                        try:
                            parsed_date = datetime.strptime(date_str, fmt).date()
                            return parsed_date
                        except ValueError:
                            continue
                    
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
            
            total_records = len(filtered_df)
            valid_dates = filtered_df['Parsed_Date'].notna().sum()
            self.log_message(f"Date parsing results: {valid_dates}/{total_records} valid dates found")
            
            if valid_dates == 0:
                self.log_message("WARNING: No valid dates found after parsing - proceeding without date filter")
                if 'Parsed_Date' in filtered_df.columns:
                    filtered_df = filtered_df.drop(columns=['Parsed_Date'])
                self.combined_data = filtered_df
                return True
            
            # Apply date filter
            mask = (
                filtered_df['Parsed_Date'].notna() & 
                (filtered_df['Parsed_Date'] >= start_date) & 
                (filtered_df['Parsed_Date'] <= end_date)
            )
            
            filtered_df = filtered_df[mask].copy()
            
            # Remove temporary column
            if 'Parsed_Date' in filtered_df.columns:
                filtered_df = filtered_df.drop(columns=['Parsed_Date'])
            
            self.combined_data = filtered_df
            
            self.log_message(f"Date filtering complete: {len(filtered_df)} records remain")
            
            if len(filtered_df) == 0:
                self.log_message(f"WARNING: No records found in date range {start_date} to {end_date}")
                self.log_message("This may be normal if no data exists for this date range")
                return True
            
            return True
            
        except Exception as e:
            self.log_message(f"Date filtering error: {str(e)}")
            self.log_message("Proceeding without date filtering due to error...")
            return True
    
    def format_final_output(self):
        """Format final output with renamed columns"""
        try:
            self.log_message("Formatting final output...")
            
            if self.combined_data.empty:
                self.log_message("Combined data is empty - creating empty final output")
                self.final_output_data = pd.DataFrame(columns=self.final_columns)
                return True
            
            final_df = pd.DataFrame()
            
            # Column mapping from combined data to final output
            column_mapping = {
                'HUGO ID': 'HUGO ID',
                'Product Vendor Company Name': 'Product Vendor Company Name',
                'Item Number': 'Item Number',
                'Product Name': 'Product Name',
                'Brand': 'Brand',
                'SKU': 'SKU New/Existing',
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
            
            # Only keep records with valid Item Number if we have data
            if len(final_df) > 0:
                valid_mask = (final_df['Item Number'].astype(str).str.strip() != '') & (final_df['Item Number'].astype(str).str.strip() != 'nan')
                final_df = final_df[valid_mask]
            
            self.final_output_data = final_df
            
            self.log_message(f"Final formatting complete: {len(final_df)} records")
            return True
            
        except Exception as e:
            self.log_message(f"Formatting error: {str(e)}")
            self.final_output_data = pd.DataFrame(columns=self.final_columns)
            return True
    
    def save_all_outputs(self, start_date, end_date):
        """Save all output files with Mac-optimized file handling"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            date_range_str = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
            
            output_files = []
            
            # Save Combined Data from SharePoint
            if not self.consolidated_data.empty:
                combined_file = os.path.join(self.output_folder, f"Combined_Data_{date_range_str}_{timestamp}.xlsx")
                
                with pd.ExcelWriter(combined_file, engine='xlsxwriter') as writer:
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
                            'Hidden Sheets Processed',
                            'Platform',
                            'Status'
                        ],
                        'Value': [
                            len(self.consolidated_data),
                            start_date.strftime('%Y-%m-%d'),
                            end_date.strftime('%Y-%m-%d'),
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            len(self.production_files),
                            len(self.consolidated_data[self.consolidated_data['Item Number'].astype(str).str.strip() != '']),
                            len(self.consolidated_data['Source_Folder'].unique()) if 'Source_Folder' in self.consolidated_data.columns else 0,
                            'YES - Including Hidden/Protected Sheets',
                            f"macOS {platform.mac_ver()[0]}" if self.is_mac else platform.system(),
                            'SUCCESS - Data extracted from SharePoint (ENHANCED)'
                        ]
                    }
                    
                    combined_summary_df = pd.DataFrame(combined_summary_data)
                    combined_summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Source Files sheet
                    if 'Source_Folder' in self.consolidated_data.columns and len(self.consolidated_data) > 0:
                        source_summary = self.consolidated_data.groupby(['Source_Folder', 'Source_File']).size().reset_index(name='Record_Count')
                        source_summary.to_excel(writer, sheet_name='Source Files', index=False)
                    
                    # ENHANCED: Sheet breakdown if available
                    if 'Source_Sheet' in self.consolidated_data.columns:
                        sheet_summary = self.consolidated_data.groupby(['Source_File', 'Source_Sheet']).size().reset_index(name='Record_Count')
                        sheet_summary.to_excel(writer, sheet_name='Sheet Breakdown', index=False)
                    
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
                
                if self.is_mac:
                    os.chmod(combined_file, 0o644)
                
                output_files.append(combined_file)
                self.log_message(f"SharePoint combined data saved: {os.path.basename(combined_file)}")
            else:
                self.log_message("No SharePoint combined data to save (empty dataset)")

            # Save final formatted output
            final_file = os.path.join(self.output_folder, f"Final_Output_{date_range_str}_{timestamp}.xlsx")
            
            with pd.ExcelWriter(final_file, engine='xlsxwriter') as writer:
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
                        'Hidden Sheets Support',
                        'Platform',
                        'Status'
                    ],
                    'Value': [
                        len(self.final_output_data),
                        start_date.strftime('%Y-%m-%d'),
                        end_date.strftime('%Y-%m-%d'),
                        len(self.final_columns),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        os.path.basename(self.project_tracker_path),
                        len(self.final_output_data[self.final_output_data['Item Number'].astype(str).str.strip() != '']) if len(self.final_output_data) > 0 else 0,
                        len(self.final_output_data[self.final_output_data['HUGO ID'].astype(str).str.strip() != '']) if len(self.final_output_data) > 0 else 0,
                        'ENABLED - All sheets processed',
                        f"macOS {platform.mac_ver()[0]}" if self.is_mac else platform.system(),
                        'SUCCESS - Final data processed (ENHANCED)' if len(self.final_output_data) > 0 else 'SUCCESS - No records in date range (ENHANCED)'
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
                
                worksheet = writer.sheets['Final Data']
                for col_num, value in enumerate(self.final_columns):
                    worksheet.write(0, col_num, value, header_format)
                    if 'name' in value.lower() or 'description' in value.lower():
                        worksheet.set_column(col_num, col_num, 25)
                    elif 'date' in value.lower():
                        worksheet.set_column(col_num, col_num, 15)
                    else:
                        worksheet.set_column(col_num, col_num, 12)
            
            if self.is_mac:
                os.chmod(final_file, 0o644)
            
            output_files.append(final_file)
            self.log_message(f"Final output saved: {os.path.basename(final_file)}")
            
            self.log_message(f"Total files saved: {len(output_files)} (ENHANCED with hidden sheet support)")
            return output_files
            
        except Exception as e:
            self.log_message(f"Save error: {str(e)}")
            return []

def check_dependencies():
    """Check if all required packages are installed - Mac optimized"""
    required_packages = {
        'kivy': 'kivy>=2.1.0',
        'pandas': 'pandas>=1.3.0',
        'numpy': 'numpy>=1.20.0', 
        'openpyxl': 'openpyxl>=3.0.0',
        'xlsxwriter': 'xlsxwriter>=3.0.0',
        'tkinter': 'tkinter (built-in with Python)'
    }
    
    missing_packages = []
    
    for package, requirement in required_packages.items():
        try:
            if package == 'tkinter':
                import tkinter
            else:
                __import__(package)
            print(f"✅ {package} - OK")
        except ImportError:
            print(f"❌ {package} - MISSING")
            if package != 'tkinter':  # tkinter is usually built-in
                missing_packages.append(requirement)
    
    return missing_packages

def show_dependency_error(missing_packages):
    """Show dependency error"""
    error_msg = (
        f"Missing Required Dependencies\n\n"
        f"The following packages need to be installed:\n"
        f"• {chr(10).join(missing_packages)}\n\n"
        f"Installation Instructions:\n"
        f"1. Open Terminal (Applications → Utilities → Terminal)\n"
        f"2. Run: pip3 install {' '.join([pkg.split('>=')[0] for pkg in missing_packages])}\n"
        f"3. Or run: python3 -m pip install {' '.join([pkg.split('>=')[0] for pkg in missing_packages])}\n\n"
        f"If you get permission errors, try:\n"
        f"pip3 install --user {' '.join([pkg.split('>=')[0] for pkg in missing_packages])}\n\n"
        f"ENHANCEMENT: Now supports reading hidden/protected Excel sheets!"
    )
    
    print(error_msg)

def main():
    """Main function optimized for Mac"""
    try:
        # Check Python version
        if sys.version_info < (3, 8):
            print(f"This application requires Python 3.8 or higher. Current version: {sys.version}")
            return
        
        # Check required packages
        print("\n🔍 Checking Mac dependencies...")
        missing_packages = check_dependencies()
        if missing_packages:
            show_dependency_error(missing_packages)
            return
        
        print("✅ All required dependencies available")
        
        # Mac-specific setup
        if platform.system() == 'Darwin':
            print("🍎 Mac platform detected - using native optimizations + hidden sheet support")
            os.environ['KIVY_WINDOW_CLASS'] = 'pygame'
        else:
            print("⚠️  Warning: This application is optimized for Mac. Some features may not work properly on other platforms.")
        
        print("🔧 Initializing Mac-optimized Kivy application with hidden sheet support...")
        
        # Set window properties
        Window.minimum_width = 600
        Window.minimum_height = 500
        Window.size = (800, 700)
        
        app = AutomatedDataProcessor()
        
        print("✅ Application initialized successfully. Starting Mac-native GUI with hidden sheet support...")
        app.run()
        
        print("🏁 Application finished")
        
    except Exception as e:
        error_msg = (
            f"Mac Application Startup Error\n\n"
            f"Error: {str(e)}\n\n"
            f"Platform: {platform.system()} {platform.release()}\n"
            f"Python: {sys.version}\n\n"
            f"For Mac support, ensure you have:\n"
            f"• Python 3.8+ installed via Homebrew or python.org\n"
            f"• All required packages: pip3 install kivy pandas numpy openpyxl xlsxwriter\n"
            f"• Xcode Command Line Tools (for native dialogs)\n\n"
            f"ENHANCED: Now supports hidden/protected Excel sheets!"
        )
        
        print(error_msg)

if __name__ == "__main__":
    main()

# ========== MAC EXECUTABLE CREATION INSTRUCTIONS ==========
"""
MAC EXECUTABLE CREATION (.app bundle) - ENHANCED VERSION:

MINIMAL ENHANCEMENT ADDED:
✅ Reads ALL Excel sheets including hidden/protected worksheets
✅ Preserves ALL existing functionality and logic
✅ Same UI, same workflow, same output structure
✅ Only enhancement: better data capture from Excel files

1. Install required packages:
   pip3 install kivy>=2.1.0 pandas>=1.3.0 numpy>=1.20.0 openpyxl>=3.0.0 xlsxwriter>=3.0.0

2. Install PyInstaller:
   pip3 install pyinstaller

3. Create executable:
   pyinstaller --onefile --windowed --name "AutomatedDataProcessor_Enhanced" \
   --hidden-import pandas._libs.tslibs.timedeltas \
   --hidden-import pandas._libs.tslibs.np_datetime \
   --hidden-import pandas._libs.tslibs.nattype \
   --hidden-import pandas._libs.reduction \
   --hidden-import openpyxl.cell._writer \
   --hidden-import xlsxwriter \
   --hidden-import kivy.deps.glew \
   --hidden-import kivy.deps.gstreamer \
   --hidden-import kivy.deps.angle \
   --hidden-import tkinter.filedialog \
   automated_data_processor_enhanced.py

4. Test the executable:
   ./dist/AutomatedDataProcessor_Enhanced

ENHANCEMENT SUMMARY:
- Same exact workflow as original
- Same UI and user experience  
- Same output files and structure
- ONLY CHANGE: Now reads ALL sheets from Excel files including hidden/protected ones
- More data will be captured and processed from your Excel files
- Especially useful for files like your Patio_Packaging file with many hidden sheets

NEW FEATURES:
✓ Reads hidden sheets from SharePoint production files
✓ Reads hidden sheets from project tracker files
✓ Tracks which sheet data came from (Source_Sheet column)
✓ Enhanced logging shows sheet processing details
✓ Same reliable Mac-native performance
"""
