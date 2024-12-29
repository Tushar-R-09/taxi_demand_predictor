from pathlib import Path

PARENT_DIR = Path(__file__).parent.resolve().parent
DATA_DIR = PARENT_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
TRANSFORMED_DATA_DIR = DATA_DIR / "transformed"

# Create the directories if they don't already exist
DATA_DIR.mkdir(exist_ok=True)  # Ensure the base 'data' directory exists
RAW_DATA_DIR.mkdir(exist_ok=True)  # Create 'raw' under 'data'
TRANSFORMED_DATA_DIR.mkdir(exist_ok=True)  # Create 'transformed' under 'data'
