import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SPARK_SCRIPTS = REPOSITORY_ROOT / "spark_scripts"
for search_path in (REPOSITORY_ROOT, SPARK_SCRIPTS):
    if str(search_path) not in sys.path:
        sys.path.insert(0, str(search_path))
