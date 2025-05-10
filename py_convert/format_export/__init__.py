import pkgutil
import importlib
from abc import ABC

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

export_classes = _get_classes()
export_names = [cls().name() for cls in export_classes]
__all__ = [cls.__name__ for cls in export_classes] + ['export_names']