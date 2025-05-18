import polars as pl

from py_convert.format_import import ImportBase

class ImportCBCador(ImportBase):
    """Gestion d'import d'écritures du presse-papier au format CADOR."""
    
    def name(self):
        return "PRESSE-PAPIER"
    
    def validate_format(self):
        return True
    
    @property
    def file_deletion(self) -> bool:
        return False
    
    def process_file(self):
        col_names = [
            "EcritureDate", 
            "JournalCode", 
            "PieceRef", 
            "EcritureLib", 
            "Debit", 
            "EcritureLet", 
            "Credit"
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

        return df
