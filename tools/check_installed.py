"""Verify an installed wheel outside the source checkout; never opens hardware."""

import importlib
import importlib.metadata
import importlib.resources
from pathlib import Path

module = importlib.import_module("linkerbot_hand")
source = Path(__file__).resolve().parents[1]
assert not Path(module.__file__).resolve().is_relative_to(source), module.__file__
assert importlib.resources.files(module).joinpath("py.typed").is_file()
distribution = importlib.metadata.distribution("linkerbot-hand")
assert any("LICENSE" in str(path) for path in distribution.files or ()), (
    "Missing LICENSE"
)
print(
    f"Installed {distribution.metadata['Name']} {distribution.version}: {module.__file__}"
)
