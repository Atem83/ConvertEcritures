from PySide6 import QtWidgets, QtCore

class SettingsWindow(QtWidgets.QDialog):
    """Fenêtre de paramètres"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.menu = parent
        self.setWindowTitle("Paramètres")
        self.setModal(True)
        self.setGeometry(
            self.menu.app.x(),
            self.menu.app.y(),
            self.menu.app.width(),
            302
            )
        #self.resizeEvent = self.on_resize
        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(self.init_start_date())
        layout.addLayout(self.init_end_date())
        layout.addLayout(self.init_logs_label())
        layout.addLayout(self.init_logs_inout_label())
        layout.addLayout(self.init_logs_input())
        layout.addLayout(self.init_directory_label())
        layout.addLayout(self.init_directory_input())
        layout.addSpacing(10)
        layout.addLayout(self.init_buttons())
    
    def on_resize(self, event):
        """Execute actions when the main window is resized."""
        print(
            self.x(),
            self.y(),
            self.width(), 
            self.height()
            )
        super().resizeEvent(event)
    
    def init_start_date(self):
        """Initialise le champ de date de début."""
        layout = QtWidgets.QHBoxLayout()
        label_start_date = QtWidgets.QLabel(self, text="Date de début :")
        layout.addWidget(label_start_date)
        self.entry_start_date = QtWidgets.QLineEdit(self)
        self.entry_start_date.setText(self.menu.app.settings.start_date)
        #self.entry_start_date.setStyleSheet(f"""
        #    background-color: {self.bg_color}; 
        #    color: white
        #    """)
        self.entry_start_date.setFixedSize(150, 30)
        self.entry_start_date.setPlaceholderText("JJ/MM/AAAA")
        self.entry_start_date.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(self.entry_start_date)
        return layout
    
    def init_end_date(self):
        """Initialise le champ de date de fin."""
        layout = QtWidgets.QHBoxLayout()
        label_end_date = QtWidgets.QLabel(self, text="Date de fin :")
        layout.addWidget(label_end_date)
        self.entry_end_date = QtWidgets.QLineEdit(self)
        self.entry_end_date.setText(self.menu.app.settings.end_date)
        #self.entry_end_date.setStyleSheet(f"""
        #    background-color: {self.bg_color}; 
        #    color: white
        #    """)
        self.entry_end_date.setFixedSize(150, 30)
        self.entry_end_date.setPlaceholderText("JJ/MM/AAAA")
        self.entry_end_date.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(self.entry_end_date)
        return layout
    
    def init_logs_label(self):
        """Initialise le champ label de choix des journaux."""
        txt = "Sautez une ligne pour chaque journal additionnel"
        layout = QtWidgets.QHBoxLayout()
        logs_label = QtWidgets.QLabel("Codes journaux")
        logs_label.setAlignment(QtCore.Qt.AlignCenter)
        logs_label.setToolTip(txt)
        layout.addWidget(logs_label)
        return layout
    
    def init_logs_inout_label(self):
        """Initialise le champ label de choix des journaux."""
        txt = "Sautez une ligne pour chaque journal additionnel"
        layout = QtWidgets.QHBoxLayout()
        include_label = QtWidgets.QLabel("À conserver :")
        include_label.setAlignment(QtCore.Qt.AlignCenter)
        include_label.setToolTip(txt)
        layout.addWidget(include_label)
        exclude_label = QtWidgets.QLabel("À exclure :")
        exclude_label.setAlignment(QtCore.Qt.AlignCenter)
        exclude_label.setToolTip(txt)
        layout.addWidget(exclude_label)
        return layout
    
    def init_logs_input(self):
        """Initialise le champ texte de choix des journaux."""
        txt = "Sautez une ligne pour chaque journal additionnel"
        layout = QtWidgets.QHBoxLayout()
        self.logs_include = QtWidgets.QTextEdit()
        self.logs_include.setPlainText("\n".join(self.menu.app.settings.logs_include))
        self.logs_include.setToolTip(txt)
        layout.addWidget(self.logs_include)
        self.logs_exclude = QtWidgets.QTextEdit()
        self.logs_exclude.setPlainText("\n".join(self.menu.app.settings.logs_exclude))
        self.logs_exclude.setToolTip(txt)
        layout.addWidget(self.logs_exclude)
        return layout
    
    def init_directory_label(self):
        """Initialise le choix de l'emplacement où le fichier sera enregistré."""
        directory_label = QtWidgets.QLabel("Emplacement :")
        directory_button = QtWidgets.QPushButton("Parcourir")
        directory_button.clicked.connect(self.browse_directory)
        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(directory_label)
        layout.addWidget(directory_button)
        return layout
    
    def init_directory_input(self):
        """Initialise le champ texte de l'emplacement où le fichier sera enregistré."""
        self.directory_input = QtWidgets.QLineEdit()
        self.directory_input.setText(str(self.menu.app.settings.directory))
        layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.directory_input)
        return layout
    
    def init_buttons(self):
        """Initialise les boutons Confirmer et Annuler."""
        # Bouton Confirmer
        ok_button = QtWidgets.QPushButton("Confirmer")
        ok_button.clicked.connect(self.on_ok)
        
        # Bouton Annuler
        cancel_button = QtWidgets.QPushButton("Annuler")
        cancel_button.clicked.connect(self.on_cancel)
        
        # Layout des boutons
        buttons_layout = QtWidgets.QHBoxLayout()
        buttons_layout.addWidget(ok_button)
        buttons_layout.addWidget(cancel_button)
        return buttons_layout
    
    def browse_directory(self):
        """Ouvre une boîte de dialogue pour sélectionner un dossier."""
        directory = QtWidgets.QFileDialog.getExistingDirectory(
            self, "Sélectionnez un dossier"
            )
        if directory:
            self.directory_input.setText(directory)
    
    def on_ok(self):
        """Gestion du clic sur Confirmer."""
        logs_include = self.logs_include.toPlainText().upper()
        test_logs = logs_include.replace("\n", "").replace(" ", "")
        if test_logs == "":
            logs_include = []
        else:
            logs_include = logs_include.split("\n")
        
        logs_exclude = self.logs_exclude.toPlainText().upper()
        test_logs = logs_exclude.replace("\n", "").replace(" ", "")
        if test_logs == "":
            logs_exclude = []
        else:
            logs_exclude = logs_exclude.split("\n")
        
        self.menu.app.settings.directory = self.directory_input.text()
        self.menu.app.settings.start_date = self.entry_start_date.text()
        self.menu.app.settings.end_date = self.entry_end_date.text()
        self.menu.app.settings.logs_include = logs_include
        self.menu.app.settings.logs_exclude = logs_exclude
        self.menu.app.settings.save()
        self.accept()
    
    def on_cancel(self):
        """Gestion du clic sur Annuler."""
        self.reject()