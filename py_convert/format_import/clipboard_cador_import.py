import polars as pl
from .base_import import BaseImport

class ClipboardCadorImport(BaseImport):
    """Gestion d'import d'écritures du presse-papier au format CADOR."""
    
    def name(self):
        return "PRESSE-PAPIER"
    
    def validate_format(self):
        return True
    
    def file_delation(self) -> bool:
        return False
    
    def process_file(self):
        col_names = [
            "Date >", 
            "Jnl", 
            "Pièce", 
            "Libellé", 
            "Débit", 
            "Lettrage", 
            "Crédit"
            ]
        
        # Importe le presse-papier en dataframe avec l'en-tête
        df = pl.read_clipboard(
            separator="\t",
            columns=(0, 1, 2, 4, 5, 6, 7),
            new_columns=col_names,
            has_header=True,
            try_parse_dates=True,
            decimal_comma=True
            )
        
        # Supression de l'espace comme séparateur de millier
        df = df.with_columns(
            pl.col("Débit").cast(pl.String),
            pl.col("Crédit").cast(pl.String)
            )
        df = df.with_columns(
            pl.col("Débit").str.replace(" ", "", literal=True),
            pl.col("Crédit").str.replace(" ", "", literal=True)
            )
        
        # Remplacement de la virgule par le point comme séparateur décimal
        df = df.with_columns(
            pl.col("Débit").str.replace(",", ".", literal=True),
            pl.col("Crédit").str.replace(",", ".", literal=True)
            )
        
        # Création des colonnes manquantes
        df = df.with_columns(
            pl.lit(None).alias("JournalLib").cast(pl.String),
            pl.lit(None).alias("EcritureNum").cast(pl.String),
            pl.lit(None).alias("CompteNum").cast(pl.String),
            pl.lit(None).alias("CompteLib").cast(pl.String),
            pl.lit(None).alias("CompAuxNum").cast(pl.String),
            pl.lit(None).alias("CompAuxLib").cast(pl.String),
            pl.col("Date >").alias("PieceDate").cast(pl.Date),
            pl.lit(None).alias("DateLet").cast(pl.Date),
            pl.lit(None).alias("ValidDate").cast(pl.Date),
            pl.lit(None).alias("Montantdevise").cast(pl.Float64),
            pl.lit(None).alias("Idevise").cast(pl.String),
            pl.lit(None).alias("EcheanceDate").cast(pl.Date)
            )
        
        # Réorganise l'ordre des colonnes
        df = df.select(
            pl.col("Jnl").alias("JournalCode"),
            "JournalLib",
            "EcritureNum",
            pl.col("Date >").alias("EcritureDate"),
            "CompteNum",
            "CompteLib",
            "CompAuxNum",
            "CompAuxLib",
            pl.col("Pièce").alias("PieceRef"),
            "PieceDate",
            pl.col("Libellé").alias("EcritureLib"),
            pl.col("Débit").alias("Debit").cast(pl.Float64),
            pl.col("Crédit").alias("Credit").cast(pl.Float64),
            pl.col("Lettrage").alias("EcritureLet"),
            "DateLet",
            "ValidDate",
            "Montantdevise",
            "Idevise",
            "EcheanceDate"
            )
        
        return df
