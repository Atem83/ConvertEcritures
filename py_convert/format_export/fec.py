import polars as pl

from py_convert.format_export import ExportBase

class ExportFEC(ExportBase):
    """Gestion d'export au format FEC."""
    def name(self):
        return "FEC"
    
    def extension(self):
        return ".txt"
    
    @property
    def mandatory_cols(self) -> list[str]:
        return [
            "JournalCode", 
            "JournalLib",
            "EcritureNum",
            "EcritureDate",
            "CompteNum",
            "CompteLib",
            "CompAuxNum",
            "CompAuxLib",
            "PieceRef",
            "PieceDate",
            "EcritureLib",
            "Debit",
            "Credit",
            "EcritureLet",
            "DateLet",
            "ValidDate",
            "Montantdevise",
            "Idevise"
            ]
    
    @property
    def mandatory_data(self) -> list[str]:
        return [
            "JournalCode",
            "EcritureDate", 
            "CompteNum", 
            "Debit", 
            "Credit"
        ]
    
    def process_file(self):
        # Conserve uniquement les colonnes obligatoires
        df = self.entries.select(self.mandatory_cols)
        
        # Remplace les points par des virgules comme séparateur décimaux
        currency_col = [
            "Debit",
            "Credit",
            "Montantdevise"
        ]
        for column in currency_col:
            df = df.with_columns(pl.col(column).round(2))
            df = df.with_columns(pl.col(column).cast(pl.String))
            df = df.with_columns(pl.col(column).str.replace(".", ",", literal=True))
        
        # Génère mon fichier d'export FEC
        df.write_csv(
            self.path_export, 
            separator="\t", 
            include_header=True, 
            date_format="%Y%m%d",
            float_precision=2
            )