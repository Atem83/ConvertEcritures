import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportJDC(ImportBase):
    """Gestion d'import d'un fichier CSV au format du logiciel de caisse JDC."""
    
    def name(self):
        return "CAISSE JDC"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".csv":
            run_error(f"Le format {self.name()} nécessite un fichier .csv")
            return False
        return True
    
    def process_file(self):
        cols_name = [
            "Date du jour", 
            "JournalCode", 
            "JournalLib", 
            "EcritureDate", 
            "CompteNum", 
            "EcritureLib", 
            "Debit", 
            "Credit"
        ]
        cols_type = {
            "Date du jour": pl.String,
            "JournalCode": pl.String,
            "JournalLib": pl.String,
            "EcritureDate": pl.Date,
            "CompteNum": pl.String,
            "EcritureLib": pl.String,
            "Debit": pl.Float64,
            "Credit": pl.Float64,
        }
        df = pl.read_csv(
            self.path, 
            separator=";", 
            has_header=False, 
            skip_lines=0,
            new_columns=cols_name,
            schema_overrides=cols_type,
            decimal_comma=True,
            ).drop("Date du jour")
        
        df = df.with_columns(pl.col("EcritureDate").alias("PieceDate"))
        
        # Ajout de la séparation compte général et auxiliaire
        df = df.with_columns(
            pl.when(pl.col("CompteNum").str.contains(r"^(F|C|401|411)"))
            .then(pl.col("CompteNum"))
            .otherwise(None)
            .alias("CompAuxNum")
        ).with_columns(
            pl.when(pl.col("CompAuxNum").str.contains(r"^(F|401)"))
            .then(pl.lit("40100000"))
            .otherwise(pl.col("CompteNum"))
            .alias("CompteNum")
        ).with_columns(
            pl.when(pl.col("CompAuxNum").str.contains(r"^(C|411)"))
            .then(pl.lit("41100000"))
            .otherwise(pl.col("CompteNum"))
            .alias("CompteNum")
        )
        
        return df