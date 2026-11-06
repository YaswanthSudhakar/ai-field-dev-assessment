"""Puts this task's folder first on sys.path. Tasks 2 and 3 both have a gateway.py, so this stops them clashing."""
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
for _name in [p.stem for p in HERE.glob("*.py")]:
    mod = sys.modules.get(_name)
    if mod is not None and pathlib.Path(getattr(mod, "__file__", "") or "").parent != HERE:
        del sys.modules[_name]
