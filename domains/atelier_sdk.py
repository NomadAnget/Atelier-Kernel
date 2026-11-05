"""Repository demo shim: the packaged Python SDK is the single implementation."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sdk.python.atelier_sdk import *
