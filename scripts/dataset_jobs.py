"""Fetch, inspect and benchmark NOAA archives using the shared package."""
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from ecore_weather.jobs import main
if __name__=='__main__':raise SystemExit(main())
