from PySide6 import QtWidgets, QtCore, QtGui
from ..__about__ import __version__, __url__, __author__, __license__, __description__

class AboutWindow(QtWidgets.QDialog):
    """Fenêtre À propos"""
    def __init__(self, parent):
        super().__init__(parent)
        self.menu = parent
        self.setWindowTitle("À propos")
        
        layout = QtWidgets.QVBoxLayout()
        layout.addLayout(self.init_version())
        layout.addLayout(self.init_author())
        layout.addLayout(self.init_license())
        layout.addSpacing(10)
        layout.addLayout(self.init_description())
        self.setLayout(layout)
        
    def init_version(self):
        """Initialise le label de la version."""
        layout_version = QtWidgets.QHBoxLayout()
        version_label = QtWidgets.QLabel("Version : ")
        layout_version.addWidget(version_label)
        version_text = QtWidgets.QLabel(__version__)
        layout_version.addWidget(version_text)
        layout_version.setAlignment(QtCore.Qt.AlignLeft)
        return layout_version
        
    def init_author(self):
        """Initialise le label de l'auteur."""
        layout_author = QtWidgets.QHBoxLayout()
        author_label = QtWidgets.QLabel("Autheur : ")
        author_text = QtWidgets.QLabel(__author__)
        layout_author.addWidget(author_label)
        layout_author.addWidget(author_text)
        layout_author.setAlignment(QtCore.Qt.AlignLeft)
        return layout_author

    def init_license(self):
        """Initialise la licence du programme et lien du code source."""
        layout_license = QtWidgets.QHBoxLayout()
        license_label = QtWidgets.QLabel("Licence : ")
        layout_license.addWidget(license_label)
        license_text = QtWidgets.QLabel(__license__)
        layout_license.addWidget(license_text)
        url_button = QtWidgets.QPushButton("code source")
        url_button.setStyleSheet(self.style_link())
        url_button.clicked.connect(
            lambda: QtGui.QDesktopServices.openUrl(QtCore.QUrl(__url__))
            )
        layout_license.addWidget(url_button)
        layout_license.setAlignment(QtCore.Qt.AlignLeft)
        return layout_license

    def init_description(self):
        """Initialise la description du programme."""
        layout_description = QtWidgets.QHBoxLayout()
        description_text = QtWidgets.QLabel(__description__)
        layout_description.addWidget(description_text)
        layout_description.setAlignment(QtCore.Qt.AlignLeft)
        return layout_description
        
    def style_link(self):
        """Style for links."""
        css = """
            QPushButton {
                border: none;  /* No border */
                background: none;  /* No background */
                color: DodgerBlue;  /* Text color */
                text-decoration: underline;  /* Underlined text to look like a link */
            }
            QPushButton:hover {
                color: RoyalBlue;  /* Text color on hover */
            }
        """
        return css