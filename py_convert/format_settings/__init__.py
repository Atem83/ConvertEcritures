import pkgutil
import importlib
from abc import ABC

from .base import SettingsBase

def get_allowed_settings() -> dict[str, list[str]]:
    """Liste des paramètres autorisés pour tous les imports."""
    settings_list = {}
    for cls in settings_classes:
        for imp in cls().get_allowed_import():
            if imp not in settings_list:
                settings_list[imp] = [""]
            settings_list[imp].append(cls().name())
    return settings_list

def _get_classes() -> list[type]:
    """Récupère dynamiquement toutes les classes du dossier."""
    classes = []
    for _, module_name, _ in pkgutil.iter_modules(__path__):
        try:
            # Importe dynamiquement le module
            module = importlib.import_module(f'.{module_name}', package=__name__)
            # Récupère toutes les classes du module
            for name in dir(module):
                cls = getattr(module, name)
                if (isinstance(cls, type) 
                        and hasattr(cls, 'name') 
                        and callable(getattr(cls, 'name'))
                        and ABC not in cls.__bases__):
                    classes.append(cls)
        except Exception as e:
            print(f"Erreur lors de l'import de {module_name}: {str(e)}")
    return classes

# Crée les listes de classes et de noms de settings
settings_classes = _get_classes()
settings_names = [cls().name() for cls in settings_classes]
__all__ = [cls.__name__ for cls in settings_classes] + ['settings_names']