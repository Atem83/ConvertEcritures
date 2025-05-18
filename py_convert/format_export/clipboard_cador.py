import polars as pl
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMessageBox

from py_convert.format_export import ExportBase

class ExportCBCador(ExportBase):
    """Gestion d'export au format presse-papier de CADOR."""
    def name(self):
        return "PRESSE-PAPIER"
    
    def extension(self):
        return None
    
    def process_file(self):
        # Message d'avertissement si plusieurs journaux sont trouvés
        if self.entries["JournalCode"].n_unique() > 1:
            msg_box = QMessageBox()
            msg_box.setWindowFlags(msg_box.windowFlags() | Qt.WindowStaysOnTopHint)
            msg_box.setWindowTitle("Confirmation")
            msg = "Plusieurs journaux ont été détectés.\n"
            msg += "Toutes les écritures seront mélangées dans le même journal.\n\n"
            msg += "Êtes-vous sûr de vouloir continuer ?"
            msg_box.setText(msg)
            msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg_box.button(QMessageBox.Yes).setText("Oui")
            msg_box.button(QMessageBox.No).setText("Non")
            answer = msg_box.exec()
            if answer == QMessageBox.No:
                return

        # Remplace le compte général par le compte auxiliaire
        df = self.swapGenToAux(self.entries)

        # Réorganise l'ordre des colonnes à exporter
        df = df.select(
            pl.col("EcritureDate"),
            pl.col("PieceRef"),
            pl.col("CompteNum"),
            pl.col("EcritureLib"),
            pl.col("EcritureLet"),
            pl.lit(None).alias("Ext."),
            pl.col("Debit"),
            pl.col("Credit")
        )

        # Passage en string pour remplacer les points par des virgules
        df = df.cast({"Debit": pl.String, "Credit": pl.String})
        df = df.with_columns(pl.col("Debit").str.replace(".", ",", literal=True))
        df = df.with_columns(pl.col("Credit").str.replace(".", ",", literal=True))
        
        df.write_clipboard(
            separator="\t", 
            include_header=False, 
            date_format="%d/%m/%Y",
            float_precision=2
            )