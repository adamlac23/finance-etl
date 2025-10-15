from pathlib import Path
import yaml

def load_cfg():
    return yaml.safe_load(open(Path(__file__).resolve().parents[1] / "config.yaml"))

def ensure_dir(p: Path):
    Path(p).mkdir(parents=True, exist_ok=True)