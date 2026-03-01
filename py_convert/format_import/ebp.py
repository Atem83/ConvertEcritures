import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportEBP(ImportBase):
    """Gestion d'import au format EBP"""
    
    def name(self):
        return "EBP"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".txt":
            run_error(f"Le format {self.name()} nécessite un fichier .txt")
            return False
        return True
    
    def process_file(self):
        cols = [
            "Line number",
            "EcritureDate",
            "JournalCode",
            "CompteNum",
            "Unknown1",
            "EcritureLib",
            "PieceRef",
            "Montant",
            "Sens",
            "EcheanceDate",
        ]
        
        # Permet de changer l'encodage si un problème d'import survient
        try:
            df = pl.read_csv(self.path, new_columns=cols, separator=",")
        except pl.exceptions.ComputeError:
            try:
                df = pl.read_csv(self.path, new_columns=cols, separator=",", encoding="windows-1252")
            except pl.exceptions.ComputeError:
                df = pl.read_csv(self.path, new_columns=cols, separator=",", encoding="ISO-8859-1")
        
        # Rajoutt de la date de pièce si elle n'existe pas
        if "PieceDate" not in df.columns:
            df = df.with_columns(pl.col("EcritureDate").alias("PieceDate"))

        if "Sens" in df.columns and "Montant" in df.columns:
            # Rajout de la colonne Débit
            df = df.with_columns(
                (pl.when(pl.col("Sens") == "D")
                   .then(pl.col("Montant"))
                   .otherwise(pl.lit(0.0))
                   .alias("Debit")
                ))
            # Rajout de la colonne Crédit
            df = df.with_columns(
                (pl.when(pl.col("Sens") == "C")
                   .then(pl.col("Montant"))
                   .otherwise(pl.lit(0.0))
                   .alias("Credit")
                ))
            # Suppression des colonnes Montant et Sens
            df = df.drop(["Montant", "Sens"])

        # Affecte None aux colonnes Date ne contenant que des espaces blancs
        # Permet d'éviter des erreurs dans des FEC avec des espaces dans la date
        # On pourrait le généraliser à toutes les colonnes String si nécessaire
        for col in ["EcritureDate", "PieceDate", "EcheanceDate"]:
            df = df.with_columns(pl.col(col).cast(pl.String))
            df = df.with_columns(
                pl.when(pl.col(col).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(col))
                .alias(col)
            )
            
            # Transforme les colonnes en type Date
            df = df.with_columns(pl.col(col).str.to_date("%d%m%Y"))

        return df
