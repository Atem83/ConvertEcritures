import sys
from PySide6 import QtWidgets, QtGui
from py_convert.settingsManager import Settings
from .main import MyFrame
from .menu import MenuBar
from ..__about__ import __title__

class App(QtWidgets.QMainWindow):
    """Application graphique"""
    def __init__(self):
        self.qapp = QtWidgets.QApplication(sys.argv)
        font = QtGui.QFont()
        font.setPointSize(10)  # Taille par défaut
        self.qapp.setFont(font)
        
        super().__init__()
        self.settings = Settings()
        self.settings.load()
        self.setGeometry(
            self.settings.window_size[0],
            self.settings.window_size[1],
            self.settings.window_size[2],
            self.settings.window_size[3]
        )
        self.setFixedSize(
            self.settings.window_size[2],
            self.settings.window_size[3]
        )
        self.setWindowTitle(__title__)
        
        # Création de la fenêtre principale
        self.main_frame = MyFrame(self)
        self.setCentralWidget(self.main_frame)
        
        # Création de la barre de menu
        self.menu_bar = MenuBar(self)
        self.setMenuBar(self.menu_bar)
        
        # Connecte les évènements de déplacement
        self.moveEvent = self.on_move
        
        # Permet à la fenêtre d'accepter les événements de drag-and-drop
        self.setAcceptDrops(True)
    
    def run(self):
        """Exécution de l'application."""
        self.show()
        sys.exit(self.qapp.exec())
    
    def on_move(self, event):
        """Enregistre des actions quand la fenêtre principale est déplacée."""
        self.settings.window_size = (
            self.x(),
            self.y(),
            self.width(), 
            self.height()
            )
        self.settings.save()
        super().moveEvent(event)
    
    def dragEnterEvent(self, event):
        """Permet de gérer l'événement de drag & drop.
        On accepte uniquement les fichiers."""
        if event.mimeData().hasUrls():
            event.accept() # Accepte l'événement de drag
        else:
            event.ignore() # Ignore si ce n'est pas un fichier

    def dropEvent(self, event):
        """Gère l'événement de drop et récupère le fichier déposé."""
        if event.mimeData().hasUrls():
            # Récupérer la première URL du mimeData
            url = event.mimeData().urls()[0]
            # Convertir l'URL en chemin local
            file_path = url.toLocalFile()
            # Lancer la conversion avec les paramètres actuels
            self.main_frame.click_btn_convert(file_path)
            
            