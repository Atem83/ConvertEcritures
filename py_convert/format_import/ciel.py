import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportCiel(ImportBase):
    """Gestion d'import au format Ciel"""
    
    def name(self):
        return "CIEL"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".txt":
            run_error(f"Le format {self.name()} nécessite un fichier .txt")
            return False
        return True
    
    def process_file(self):
        cols_type = {
            "PieceRef": pl.String,
            "PieceRef2": pl.String,
            "JournalCode": pl.String,
            "EcritureDate": pl.String,
            "CompteNum": pl.String,
            "EcritureLib": pl.String,
            "Debit": pl.Float64,
            "Credit": pl.Float64,
            "PostalCode": pl.String,
            "PieceRef3": pl.String,
            "PieceRef4": pl.String,
        }
        
        # Permet de changer l'encodage si un problème d'import survient
        try:
            df = pl.read_csv(
                self.path, 
                has_header=False, 
                new_columns=list(cols_type.keys()),
                separator="\t",
                schema_overrides=cols_type,
                eol_char=self.detect_eol()
                )
        except pl.exceptions.ComputeError:
            try:
                df = pl.read_csv(
                    self.path, 
                    has_header=False, 
                    new_columns=list(cols_type.keys()),
                    separator="\t", 
                    encoding="windows-1252",
                    schema_overrides=cols_type,
                    eol_char=self.detect_eol()
                    )
            except pl.exceptions.ComputeError:
                df = pl.read_csv(
                    self.path, 
                    has_header=False, 
                    new_columns=list(cols_type.keys()),
                    separator="\t", 
                    encoding="ISO-8859-1",
                    schema_overrides=cols_type,
                    eol_char=self.detect_eol()
                    )
        
        # Suppression des écritures CB car les cartes bleues sont les CBM
        df = df.filter(pl.col("PieceRef3") != "CB")
        
        # Suppression des espaces dans le numéro de compte
        df = df.with_columns(pl.col("CompteNum").str.replace_all(" ", ""))
        
        # Conservation des colonnes utiles
        df = df.drop(("PieceRef", "PostalCode", "PieceRef3", "PieceRef4"))
        df = df.rename({"PieceRef2": "PieceRef"})
        
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
        
        # Transforme les colonnes en type Date
        df = df.with_columns(pl.col("EcritureDate").str.to_date("%d/%m/%Y"))

        return df
