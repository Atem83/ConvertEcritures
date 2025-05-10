import polars as pl
from .base_export import BaseExport

class ExcelExport(BaseExport):
    """Gestion d'export au format Excel."""
    def name(self):
        return "EXCEL"
    
    def extension(self):
        return ".xlsx"
    
    def process_file(self):
        # Remplace le compte général par le compte auxiliaire
        df = self.swapGenToAux(self.entries)

        # Réorganise l'ordre des colonnes à exporter et formate leur type
        df = df.select(
            pl.col("JournalCode").alias("Code Jrnl"),
            pl.col("EcritureDate").alias("Date Ecriture"),
            pl.col("PieceRef").alias("N° Piece"),
            pl.col("CompteNum").alias("N° Compte"),
            pl.col("EcritureLib").alias("Libellé"),
            pl.col("EcritureLet").alias("Lett."),
            pl.lit(None).alias("Ext."),
            pl.col("Debit").alias("Débit"),
            pl.col("Credit").alias("Crédit")
        )

        dict_col = {
            "Code Jrnl": {"align": "center"},
            "Date Ecriture": {"align": "center"},
            "N° Piece": {"align": "center"},
            "N° Compte": {"align": "center"},
            "Libellé": {"align": "left"},
            "Lett.": {"align": "center"},
            "Ext.": {"align": "center"},
            "Débit": {"align": "center"},
            "Crédit": {"align": "center"}
        }

        df.write_excel(
            workbook=self.path_export,
            table_style="TableStyleMedium2",
            dtype_formats={pl.Date:"dd/mm/yyyy"},
            header_format={"bold":True, "align":"center"},
            column_formats=dict_col,
            float_precision=2,
            include_header=True,
            autofit=True
        )