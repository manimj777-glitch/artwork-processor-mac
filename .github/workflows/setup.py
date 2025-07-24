"""
Setup script for creating macOS .app bundle
Run with: python setup.py py2app
"""

from setuptools import setup
import os

# Main application file
APP = ['Artwork_Processor.py']  # Replace with your actual filename

# Additional data files to include
DATA_FILES = []

# py2app options
OPTIONS = {
    'argv_emulation': True,
    'iconfile': None,  # Path to .icns icon file if you have one
    'plist': {
        'CFBundleName': 'Artwork Release Data Processor',
        'CFBundleDisplayName': 'Artwork Release Data Processor', 
        'CFBundleGetInfoString': 'Artwork Release Data Processor v1.0',
        'CFBundleIdentifier': 'com.yourcompany.artworkprocessor',
        'CFBundleVersion': '1.0.0',
        'CFBundleShortVersionString': '1.0.0',
        'NSHumanReadableCopyright': 'Copyright © 2025 Your Company',
        'NSHighResolutionCapable': True,
        'LSMinimumSystemVersion': '10.13',  # Support macOS High Sierra and newer
        'NSRequiresAquaSystemAppearance': False,  # Support dark mode
    },
    
    # Include required packages
    'packages': [
        'pandas', 
        'numpy', 
        'openpyxl', 
        'xlsxwriter', 
        'kivy',
        'concurrent.futures',
        'pathlib',
        'json',
        'pickle',
        're',
        'threading',
        'subprocess'
    ],
    
    # Include specific modules
    'includes': [
        'pkg_resources.py2_warn',
        'kivy.app',
        'kivy.uix.boxlayout',
        'kivy.uix.gridlayout', 
        'kivy.uix.label',
        'kivy.uix.button',
        'kivy.uix.progressbar',
        'kivy.uix.scrollview',
        'kivy.uix.textinput',
        'kivy.uix.filechooser',
        'kivy.uix.popup',
        'kivy.clock',
        'kivy.core.window',
        'kivy.logger'
    ],
    
    # Exclude unnecessary modules to reduce size
    'excludes': [
        'tkinter',
        'matplotlib',
        'scipy',
        'IPython',
        'jupyter'
    ],
    
    # Additional resources
    'resources': [],
    
    # Optimize bytecode
    'optimize': 2,
    
    # Strip debug symbols
    'strip': True,
    
    # App bundle structure
    'prefer_ppc': False,
    'semi_standalone': False,
    'site_packages': True,
}

setup(
    name='Artwork Release Data Processor',
    app=APP,
    data_files=DATA_FILES,
    options={'py2app': OPTIONS},
    setup_requires=['py2app'],
    install_requires=[
        'pandas>=1.3.0',
        'numpy>=1.20.0', 
        'openpyxl>=3.0.0',
        'XlsxWriter>=3.0.0',
        'kivy>=2.1.0'
    ],
)
