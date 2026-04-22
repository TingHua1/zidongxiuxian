from typing import Optional

from tg_game.models import FeatureModule
from tg_game.modules import ALL_MODULES


class ModuleRegistry:
    def __init__(self, modules: list[FeatureModule]):
        self._modules = {module.key: module for module in modules}

    def list_modules(self) -> list[FeatureModule]:
        return list(self._modules.values())

    def get_module(self, key: str) -> Optional[FeatureModule]:
        return self._modules.get(key)


module_registry = ModuleRegistry(ALL_MODULES)
