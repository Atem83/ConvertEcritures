import json
from pathlib import Path

from screeninfo import get_monitors

from py_convert.format_import import import_names, import_classes
from py_convert.format_export import export_names
from py_convert.format_settings import get_allowed_settings

class Settings():
    """Gestionnaire de paramètres pour sauvegarder des paramètres."""
    def __init__(self):
        self._directory = Path.home() / "Desktop"
        self._default_import = import_names[0] if import_names else None
        self._default_export = export_names[0] if export_names else None
        self._default_settings = {cls().name(): "" for cls in import_classes}
        self._delete_file = 0
        self._account_530 = "53000000"
        self._account_580 = "58020000"
        self._window_size = (100, 100, 300, 250)
        
        self.start_date = ""
        self.end_date = ""
        self.logs_include = []
        self.logs_exclude = []
    
    @property
    def directory(self):
        """Dossier dans lequel le fichier sera sauvegardé."""
        return self._directory
    
    @directory.setter
    def directory(self, value):
        if value is None:
            return
        else:
            path = Path(value)
        
        if path.exists() and path.is_dir():
            self._directory = path
        else:
            msg = f"Le dossier {value} n'existe pas"
            msg += f"\nLe chemin par défaut a été utilisé : {self._directory}"
            print(msg)
    
    @property
    def default_import(self):
        """Format d'import par défaut."""
        return self._default_import
    
    @default_import.setter
    def default_import(self, value):
        if not isinstance(value, str):
            print(f"Le format d'import doit être au format string, pas {type(value)}")
            return
    
        if value in import_names:
            self._default_import = str(value)
        else:
            print(f"Le format d'import '{value}' n'est pas disponible")
    
    @property
    def default_export(self):
        """Format d'export par défaut."""
        return self._default_export
    
    @default_export.setter
    def default_export(self, value):
        if not isinstance(value, str):
            print(f"Le format d'export doit être au format string, pas {type(value)}")
            return
    
        if value in export_names:
            self._default_export = str(value)
        else:
            print(f"Le format d'export '{value}' n'est pas disponible")

    @property
    def default_settings(self):
        """Paramètres par défaut des formats d'import."""
        return self._default_settings
    
    @default_settings.setter
    def default_settings(self, value):
        if not isinstance(value, dict):
            print(f"Les paramètres par défaut doivent être au format dict, pas {type(value)}")
            return
        
        # Supprime les valeurs invalides
        allowed_settings = get_allowed_settings()
        for key, val in value.copy().items():
            if not isinstance(val, str):
                msg = f"Le paramètre '{val}' de l'import '{key}' doit être au format string, pas {type(val)}"
                msg += "\nCe paramètre a été supprimé."
                print(msg)
                del value[key]
            elif key not in import_names:
                msg = f"L'import '{key}' n'existe pas"
                msg += "\nCet import a été supprimé."
                print(msg)
                del value[key]
            elif val not in allowed_settings.get(key, [""]):
                msg = f"Le paramètre '{val}' n'est pas disponible pour l'import '{key}'"
                msg += "\nCe paramètre a été supprimé."
                print(msg)
                del value[key]
        settings_list = value
        
        # Rajoute les paramètres manquants
        for key in import_names:
            if key not in settings_list:
                settings_list[key] = self._default_settings[key]
        
        self._default_settings = settings_list
    
    @property
    def delete_file(self):
        """Choix de suppression du fichier d'origine."""
        return self._delete_file
    
    @delete_file.setter
    def delete_file(self, value):
        if not isinstance(value, int):
            print(f"delete_file doit être au format int, pas {type(value)}")
            return
        
        if value not in [0, 1]:
            print(f"delete_file doit avoir pour valeur 0 ou 1, pas {value}")
            return
        
        self._delete_file = value
    
    @property
    def account_530(self):
        """Compte 530 par défaut."""
        return self._account_530
    
    @account_530.setter
    def account_530(self, value):
        if not isinstance(value, str):
            print(f"Le compte 530 par défaut doit être au format str, pas {type(value)}")
            return
        
        self._account_530 = value
    
    @property
    def account_580(self):
        """Compte 580 par défaut."""
        return self._account_580
    
    @account_580.setter
    def account_580(self, value):
        if not isinstance(value, str):
            print(f"Le compte 580 par défaut doit être au format str, pas {type(value)}")
            return
        
        self._account_580 = value
    
    @property
    def window_size(self):
        """Size of the GUI window."""
        return self._window_size

    @window_size.setter
    def window_size(self, value):
        if not isinstance(value, tuple):
            print(f"window_size doit être au format tuple, pas {type(value)}")
            return
        elif len(value) != 4:
            print(f"le tuple doit avoir pour valeur 4 éléments, pas {len(value)}")
            return
        
        # Vérifie si les coordonnées de la fenêtre sont dans les limites des écrans
        monitors = get_monitors()
        x_min = 0
        y_min = 0
        x_max = 0
        y_max = 0
        for monitor in monitors:
            if monitor.x < x_min:
                x_min = monitor.x
            if monitor.y < y_min:
                y_min = monitor.y
            if monitor.x + monitor.width > x_max:
                x_max = monitor.x + monitor.width
            if monitor.y + monitor.height > y_max:
                y_max = monitor.y + monitor.height
        
        x_margin = 0
        y_margin = 0
        if value[0] < x_min + x_margin or value[0] + value[2] > x_max - x_margin:
            self._window_size = (100, 100, 300, 250)
            return
        if value[1] < y_min + y_margin or value[1] + value[3] > y_max - y_margin:
            self._window_size = (100, 100, 300, 250)
            return
        
        self._window_size = value

    def path_save(self):
        """Chemin vers le fichier de sauvegarde."""
        file = "convert_parameters.json"
        folder = "Atem83"
        path_file = Path.home() / "Documents" / folder / file
        return path_file

    def save(self):
        """Sauvegarde les paramètres dans un fichier."""
        settings = {
            'directory': str(self.directory),
            'default_import': self.default_import,
            'default_export': self.default_export,
            'default_settings': self.default_settings,
            'delete_file': self.delete_file,
            'account_530': self.account_530,
            'account_580': self.account_580,
            'window_size': self.window_size
            }
        path_file = self.path_save()

        try:
            # Crée le dossier s'il n'existe pas
            path_folder = Path(path_file).parent
            path_folder.mkdir(parents=True, exist_ok=True)

            # Ecris ma sauvegarde
            with open(path_file, 'w', encoding='utf-8') as file:
                json.dump(settings, file, indent=4, ensure_ascii=False)
        except:
            print("Echec de la sauvegarde.")
            return

    def load(self):
        """Charge les paramètres depuis un fichier."""
        path_file = self.path_save()
        try:
            with open(path_file, "r", encoding='utf-8') as file:
                settings = json.load(file)
                self.directory = settings["directory"]
                self.default_import = settings["default_import"]
                self.default_export = settings["default_export"]
                self.default_settings = settings["default_settings"]
                self.delete_file = int(settings["delete_file"])
                self.account_530 = settings["account_530"]
                self.account_580 = settings["account_580"]
                self.window_size = tuple(settings["window_size"])
        except Exception as e:
            print(e)
            msg = "Echec du chargement de la sauvegarde."
            msg += "\nLes paramètres par défaut seront utilisés."
            print(msg)
            return
