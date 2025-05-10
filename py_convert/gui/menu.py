from PySide6 import QtWidgets
from .about import AboutWindow
from .settings import SettingsWindow

class MenuBar(QtWidgets.QMenuBar):
    """Barre des menus"""
    def __init__(self, parent):
        super().__init__(parent)
        self.app = parent
        
        # Créer le menu Fichier
        file_menu = self.addMenu("&Fichier")
        # Action Ouvrir
        self.open_action = file_menu.addAction("&Ouvrir")
        self.open_action.setShortcut("Ctrl+O")
        self.open_action.triggered.connect(self.app.main_frame.browse_file)
        # Action Quitter
        self.quit_action = file_menu.addAction("&Quitter")
        self.quit_action.setShortcut("Ctrl+Q")
        self.quit_action.triggered.connect(self.app.close)
        
        # Créer le menu Paramètres
        self.settingsMenu = self.addAction("&Paramètres")
        self.settingsMenu.triggered.connect(self.show_settings)
        
        # Créer le menu A propos
        self.about = self.addAction("À &propos")
        self.about.triggered.connect(self.show_about)
        
    def show_settings(self):
        """Affiche la boîte de dialogue Paramètres."""
        settings_dialog = SettingsWindow(self)
        settings_dialog.exec()
        
    def show_about(self):
        """Affiche la boîte de dialogue À propos."""
        about_dialog = AboutWindow(self)
        about_dialog.exec()
        