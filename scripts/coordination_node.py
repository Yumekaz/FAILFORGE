#!/usr/bin/env python3
"""Launch the sibling Coordination target with optional timed thread traces."""
import faulthandler
import os
from pathlib import Path
import sys

root = Path(os.environ.get("COORD_ROOT", "../Coordination-service")).resolve()
sys.path.insert(0, str(root))
dump_after = float(os.environ.get("FAILFORGE_DIAGNOSTIC_DUMP_AFTER", "0"))
if dump_after > 0:
    faulthandler.dump_traceback_later(dump_after)

from main import main

main()
