import re
from pathlib import Path

import polars as pl
import send2trash as s2t
from PySide6 import QtWidgets, QtGui, QtCore

from py_convert.format_import import import_classes, import_names
from py_convert.format_export import export_classes, export_names
from py_convert.format_settings import settings_classes, settings_names, get_allowed_settings
from py_convert.gui.animated_toggle import QSwitch

class MyFrame(QtWidgets.QFrame):
    """Fenêtre principale"""
    def __init__(self, parent):
        super().__init__(parent)
        self.app = parent
        self.ignore_combo_changes = False
        self.bg_color = "#1C86EE"
        self.QCB_color = (f"""
            QComboBox {{ background-color: {self.bg_color}; color: white; }}
            QComboBox:disabled {{ background-color: gray; }}
            QListView {{ background-color: {self.bg_color}; color: white; }}
            """)
        
        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(self.init_import())
        layout.addLayout(self.init_export())
        layout.addLayout(self.init_param())
        layout.addLayout(self.init_switch())
        layout.addSpacing(5)
        layout.addWidget(self.init_convert_btn())
        
        # Je le place à la fin pour que toutes les variables soient initialisées
        self.cb_import.currentIndexChanged.connect(self.settings_cb_param)
        self.settings_cb_param(self.cb_import.currentText())
    
    def init_import(self):
        """Initialise les widgets format d'import."""
        layout = QtWidgets.QHBoxLayout()
        label_import = QtWidgets.QLabel(self, text="Format d'import :")
        layout.addWidget(label_import)
        self.cb_import = QtWidgets.QComboBox(self)
        self.cb_import.setStyleSheet(self.QCB_color)
        self.cb_import.setFixedSize(150, 30)
        self.cb_import.addItems(import_names)
        self.cb_import.setCurrentText(self.app.settings.default_import)
        
        txt = "Extensions attendues :"
        txt += "\nFEC :\t\t.txt"
        txt += "\nSAGE 20 :\t.txt"
        txt += "\nSEKUR :\t\t.xlsx"
        txt += "\nVOSFACTURES :\t.xls"
        txt += "\nCOURTAGE :\t.csv"
        self.cb_import.setToolTip(txt)
        layout.addWidget(self.cb_import)
        return layout
    
    def init_export(self):
        """Initialise les widgets format d'export."""
        layout = QtWidgets.QHBoxLayout()
        label_export = QtWidgets.QLabel(self, text="Format d'export :")
        layout.addWidget(label_export)
        self.cb_export = QtWidgets.QComboBox(self)
        self.cb_export.setStyleSheet(self.QCB_color)
        self.cb_export.setFixedSize(150, 30)
        self.cb_export.addItems(export_names)
        self.cb_export.setCurrentText(self.app.settings.default_export)
        self.cb_export.currentIndexChanged.connect(self.update_settings)
        layout.addWidget(self.cb_export)
        return layout
    
    def init_param(self):
        """Initialise les widgets des paramètres."""
        layout = QtWidgets.QHBoxLayout()
        label_param = QtWidgets.QLabel(self, text="Paramètres :")
        layout.addWidget(label_param)
        self.cb_param = QtWidgets.QComboBox(self)
        self.cb_param.setStyleSheet(self.QCB_color)
        self.cb_param.setSizeAdjustPolicy(QtWidgets.QComboBox.AdjustToContents)
        self.cb_param.view().setMinimumWidth(150)
        self.cb_param.setMinimumWidth(150)
        self.cb_param.setFixedHeight(30)
        self.cb_param.addItems(settings_names)
        self.cb_param.setCurrentText(
            self.app.settings.default_settings.get(self.cb_import.currentText(), ""))
        self.cb_param.currentIndexChanged.connect(self.update_settings)
        layout.addWidget(self.cb_param)
        return layout
    
    def init_switch(self):
        """Initialise le switch de suppression du fichier d'origine."""
        layout = QtWidgets.QHBoxLayout()
        self.switch = QSwitch(self, checked_color=self.bg_color)
        self.switch.stateChanged.connect(self.update_settings)
        if self.app.settings.delete_file:
            self.switch.setChecked(True)
        else:
            self.switch.setChecked(False)
        self.switch.setFixedSize(self.switch.sizeHint())
        layout.addWidget(self.switch)
        self.label_switch = QtWidgets.QLabel(self, text="Supprimer le fichier d'origine")
        layout.addWidget(self.label_switch)
        layout.setAlignment(QtCore.Qt.AlignTop | QtCore.Qt.AlignHCenter)
        return layout
    
    def init_convert_btn(self):
        """Initialise le bouton de conversion."""
        self.btn_convert = QtWidgets.QPushButton(self, text="Convertir un fichier")
        self.btn_convert.setStyleSheet(f"""
            QPushButton {{ 
                background-color: {self.bg_color}; 
                color: white;
                min-height: 27px;
            }}
            QPushButton:hover {{ 
                background-color: #1874CD; 
                color: white; 
                border: none;
                border-radius: 5px;
            }}
            QPushButton:pressed {{
                background-color: #104E8B; 
                color: white; 
                border: none;
                border-radius: 5px;
            }}
            """)
        txt = "Vous pouvez glisser-déposer votre fichier dans l'application"
        self.btn_convert.setToolTip(txt)
        self.btn_convert.clicked.connect(self.browse_file)
        return self.btn_convert
    
    def update_settings(self):
        """Met à jour les paramètres de configuration par défaut"""
        self.app.settings.default_import = self.cb_import.currentText()
        self.app.settings.default_export = self.cb_export.currentText()
        if not self.ignore_combo_changes:
            self.app.settings.default_settings = {
                self.cb_import.currentText(): self.cb_param.currentText()
                }
        self.app.settings.delete_file = self.switch.isChecked()
        self.app.settings.save()
    
    def settings_cb_param(self, index):
        """Configure la Combobox des paramètres en fonction du choix de l'import."""
        self.ignore_combo_changes = True
        choice = self.cb_import.currentText()
        allowed_settings = get_allowed_settings()
        
        # Paramètre le bouton switch pour la suppression du fichier
        for cls in import_classes:
            if choice == cls().name():
                self.switch.setEnabled(cls().file_deletion)
                if cls().file_deletion == False:
                    self.switch.setChecked(False)
                break

        self.set_delete_switch()
        self.cb_param.clear()

        # Paramètre la Combobox des paramètres
        if len(allowed_settings.get(choice, [""])) > 1:
            self.cb_param.setEnabled(True)
            self.cb_param.addItems(allowed_settings.get(choice, [""]))
            self.cb_param.setCurrentText(self.app.settings.default_settings[choice])
        else: 
            self.cb_param.setEnabled(False)
        
        self.ignore_combo_changes = False
        self.update_settings()
    
    def set_delete_switch(self):
        """Change la couleur du switch selon son état d'activation"""
        palette = QtWidgets.QApplication.palette()
        background_color = palette.color(QtGui.QPalette.Window)
        
        if background_color.lightness() < 128:
            # Thème sombre
            if self.switch.isEnabled():
                # 255 pour 0% de transparence
                translucent_color = f"rgba(255, 255, 255, 255)"
            else:
                # 128 pour 50% de transparence
                translucent_color = f"rgba(255, 255, 255, 128)"
        else:
            # Thème clair
            if self.switch.isEnabled():
                # 255 pour 0% de transparence
                translucent_color = f"rgba(0, 0, 0, 255)"
            else:
                # 128 pour 50% de transparence
                translucent_color = f"rgba(0, 0, 0, 128)"
        
        self.label_switch.setStyleSheet(f"color: {translucent_color};")

    def browse_file(self):
        """Ouvre une boîte de dialogue pour sélectionner un fichier."""
        if self.cb_import.currentText() != "PRESSE-PAPIER":
            file, _ = QtWidgets.QFileDialog.getOpenFileName(
                self, "Sélectionnez le fichier à convertir")
            if not file: return
        else:
            file = None
        
        self.click_btn_convert(file)

    def click_btn_convert(self, file):
        """Lancer la conversion d'un fichier."""
        try:
            methode_import = self.cb_import.currentText()
            methode_export = self.cb_export.currentText()
            methode_param = self.cb_param.currentText()
            start_date = self.app.settings.start_date
            end_date = self.app.settings.end_date
            delete_original = self.switch.isChecked()
            logs_include = self.app.settings.logs_include
            logs_exclude = self.app.settings.logs_exclude

            # Importe les écritures
            for cls in import_classes:
                if methode_import == cls().name():
                    accounts = cls(file)
                    accounts.import_data()
                    break
                
            if accounts.import_failed: return
            df = accounts.entries

            # Effectue des modifications aux écritures selon certains paramètres
            for cls in settings_classes:
                if methode_param == cls().name():
                    settings = cls(self, df, self.app.settings.directory)
                    settings.modify_data()
                    df = settings.entries
                    break

            # Filtre le dataframe en fonction de la date
            pattern = re.compile(r"^(0[1-9]|[12][0-9]|3[01])/(0[1-9]|1[0-2])/[0-9]{4}$")
            if pattern.match(start_date) and pattern.match(end_date):
                start = pl.lit(start_date).str.strptime(pl.Date, format="%d/%m/%Y")
                end = pl.lit(end_date).str.strptime(pl.Date, format="%d/%m/%Y")
                df = df.filter(
                    (pl.col("EcritureDate") >= start) &
                    (pl.col("EcritureDate") <= end)
                    )
            elif start_date == "" and end_date == "":
                pass
            else:
                message = "Les dates doivent être au format JJ/MM/AAAA !"
                QtWidgets.QMessageBox.warning(self, "Attention", message)
                return

            # Filtre le dataframe en fonction des journaux
            if logs_include:
                df = df.filter(pl.col("JournalCode").is_in(logs_include))
            if logs_exclude:
                df = df.filter(~pl.col("JournalCode").is_in(logs_exclude))

            # Génère le fichier d'export
            for cls in export_classes:
                if methode_export == cls().name():
                    cls(df, self.app.settings.directory).export_data()
                    break
                
            # Supprime le fichier d'origine si l'option est active
            if delete_original:
                file = file.replace('/', '\\')
                s2t.send2trash(file)

            # Met à jour le fichier de configuration par défaut
            self.app.settings.save()
        
        except Exception as e:
            file = Path.home() / "Desktop" / "error_log.txt"
            with open(file, 'a') as log_file:
                log_file.write(f"Erreur: \n{str(e)}")
                
            msg = "Une erreur est survenue lors de la conversion !\n\n"
            msg += f"Message d'erreur enregistré ici : {file}"
            QtWidgets.QMessageBox.warning(self, "Attention", msg)
            return
            