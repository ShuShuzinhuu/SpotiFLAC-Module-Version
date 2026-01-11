#!/usr/bin/env python3
"""
Launcher script for SpotiFLAC
"""

import sys
import os

if getattr(sys, 'frozen', False):
    application_path = sys._MEIPASS
else:
    application_path = os.path.dirname(os.path.abspath(__file__))

sys.path.insert(0, application_path)

# Now import and run the main SpotiFLAC module
if __name__ == '__main__':
    from spotiflac import spotiflac
    from spotiflac import SpotiFLAC
    args = SpotiFLAC.parse_args()
    spotiflac(args.url, args.output_dir, args.services)
