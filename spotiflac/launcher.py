#!/usr/bin/env python3
"""
Launcher script for SpotiFLAC
"""

import sys
import os
from spotiflac.SpotiFLAC import main

if getattr(sys, 'frozen', False):
    application_path = sys._MEIPASS
else:
    application_path = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, application_path)

if __name__ == "__main__":
    main()
