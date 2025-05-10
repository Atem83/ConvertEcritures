from PySide6 import QtWidgets, QtCore

class AskCash(QtWidgets.QDialog):
    """Fenêtre permettant de récupérer les numéros de comptes caisse"""
    def __init__(self, myframe):
        super().__init__()
        self.myframe = myframe
        self.setGeometry(100, 100, 310, 160)
        self.setWindowTitle("Paramétrage des comptes")

        texte_530 = "Numéro du compte de caisse :"
        texte_580 = "Numéro du compte de virement interne :"
        txt_tooltip = "Si aucune valeur n'est renseignée,\n"
        txt_tooltip += "la valeur par défaut sera utilisée."

        # Compte de caisse : configuration des widgets
        self.label_530 = QtWidgets.QLabel(self, text=texte_530)
        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(self.label_530)
        
        self.entry_530 = QtWidgets.QLineEdit(self)
        self.entry_530.setPlaceholderText(self.myframe.app.settings.account_530)
        self.entry_530.setAlignment(QtCore.Qt.AlignCenter)
        self.entry_530.setToolTip(txt_tooltip)
        layout.addWidget(self.entry_530)
        
        # Compte de virements internes : configuration des widgets
        self.label_580 = QtWidgets.QLabel(self, text=texte_580)
        layout.addWidget(self.label_580)

        self.entry_580 = QtWidgets.QLineEdit(self)
        self.entry_580.setPlaceholderText(self.myframe.app.settings.account_580)
        self.entry_580.setAlignment(QtCore.Qt.AlignCenter)
        self.entry_580.setToolTip(txt_tooltip)
        layout.addWidget(self.entry_580)
        
        # Bouton de validation : configuration du widget
        self.answer = QtWidgets.QPushButton(self, text="Valider")
        self.answer.clicked.connect(self.get_answer)
        layout.addWidget(self.answer)

    def get_answer(self):
        """Récupère les numéros de compte choisis par l'utilisateur"""
        
        if self.check_answer() == False:
            message = "Seuls les nombres entiers sont acceptés !"
            QtWidgets.QMessageBox.warning(self, "Attention", message)
            return
        
        if self.entry_530.text() != "":
            self.myframe.app.settings.account_530 = (
                self.entry_530.text().ljust(8, "0"))
        
        if self.entry_580.text() != "":
            self.myframe.app.settings.account_580 = (
                self.entry_580.text().ljust(8, "0"))
        
        self.myframe.app.settings.save()
        self.destroy()
        
    def check_answer(self):
        """Vérifie si les réponses données sont des nombres
        Returns:
            (bool): retourne vrai si la réponse est un nombre
        """
        
        check = True
        if (self.entry_530.text() != "" and
            self.entry_530.text().isdigit() == False):
            check = False
        
        if (self.entry_580.text() != "" and
            self.entry_580.text().isdigit() == False):
            check = False
        
        return check
