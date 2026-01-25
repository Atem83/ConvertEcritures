from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from py_convert.error import run_error

class ImportBase(ABC):
    """Classe abstraite pour les méthodes d'importation d'écritures."""
    def __init__(self, filename: str | None = None):
        self.import_failed = False
        self._entries = None
        if filename is None:
            self.path = None
        else:
            self.path = Path(filename)
    
    @abstractmethod
    def name(self) -> str:
        """Nom de la méthode d'import."""
        pass
    
    @abstractmethod
    def validate_format(self) -> bool:
        """Vérifie si le format du fichier est valide."""
        pass
    
    @abstractmethod
    def process_file(self) -> pl.DataFrame:
        """Traite le fichier et retourne un dataframe d'écritures comptables."""
        pass
    
    @property
    def entries(self) -> pl.DataFrame:
        """Liste brute des écritures importées."""
        df = self._entries
        if df is None: return None
        df = self.num_ecritures(df)
        
        # Permet de régler les paramètres des variables float
        pl.Config(decimal_separator=",", float_precision=2)
        
        # Permet d'éviter un nombre anormal de chiffres après la virgule
        df = df.with_columns([
            pl.col(pl.Float64).round(2), 
            pl.col(pl.Float32).round(2)
            ])
        
        return df
    
    @property
    def file_deletion(self) -> bool:
        """Le fichier peut-il être supprimé ?"""
        return True
    
    @property
    def get_columns(self):
        """Retourne les colonnes et leur format nécessaire dans le dataframe."""
        return {
            "JournalCode": pl.String,
            "JournalLib": pl.String,
            "EcritureNum": pl.String,
            "EcritureDate": pl.Date,
            "CompteNum": pl.String,
            "CompteLib": pl.String,
            "CompAuxNum": pl.String,
            "CompAuxLib": pl.String,
            "PieceRef": pl.String,
            "PieceDate": pl.Date,
            "EcritureLib": pl.String,
            "Debit": pl.Float64,
            "Credit": pl.Float64,
            "EcritureLet": pl.String,
            "DateLet": pl.Date,
            "ValidDate": pl.Date,
            "Montantdevise": pl.Float64,
            "Idevise": pl.String,
            "EcheanceDate": pl.Date
        }
    
    def check_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Vérifie et corrige les colonnes du dataframe."""
        
        for col, dtype in self.get_columns.items():
            # Ajout des colonnes manquantes
            if col not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
            # Conversion des dtypes des colonnes existantes
            else:
                if col in ["Debit", "Credit", "Montantdevise"]:
                    df = df.with_columns(
                        pl.col(col)
                          .cast(pl.String)
                          .str.replace(",", ".")
                          .str.replace(" ", "", literal=True)
                          .cast(pl.Float64)
                        )
                else:
                    df = df.with_columns(pl.col(col).cast(dtype).alias(col))
        
        # Tri des colonnes
        df = df.select(list(self.get_columns))
        
        return df
    
    def import_data(self):
        """Méthode principale d'import."""
        if not self.validate_format():
            self.import_failed = True
            return
            
        try:
            self._entries = self.process_file()
            if self._entries is None:
                self.import_failed = True
            else:
                self._entries = self.check_columns(self._entries)
        except Exception as e:
            run_error("Une erreur est survenue lors de l'import.", details=str(e))
            print(e)
            self.import_failed = True
            return
    
    def num_ecritures(self, df: pl.DataFrame):
        """Attribue un numéro unique à chaque écriture dans une liste d'écritures."""

        # Calculer le solde cumulatif
        df = df.with_columns(
            (pl.col("Debit") - pl.col("Credit")).cum_sum().alias("solde")
        )

        # Marquer les endroits où le solde revient à zéro
        df = df.with_columns(
            (pl.col("solde").abs() < 1e-3).alias("reset")
        )

        # Décaler le signal de "reset" d'une ligne pour que l'incrément se fasse sur la ligne suivante
        df = df.with_columns(
            pl.col("reset").shift(1).fill_null(False).alias("shifted_reset")
        )

        # Générer un groupe d'incrémentation basé sur les resets décalés
        df = df.with_columns(
            pl.col("shifted_reset").cum_sum().alias("EcritureNum")
        )

        # Incrémentation finale et conversion de EcritureNum en String
        df = df.with_columns(
            (pl.col("EcritureNum") + 1).cast(pl.Utf8).alias("EcritureNum")
        )

        # Ajuster la longueur d'EcritureNum
        df = df.with_columns(
            pl.col("EcritureNum").str.zfill(
                pl.col("EcritureNum").str.len_chars().max()
                )
            )

        # Afficher le DataFrame final sans les colonnes temporaires
        df = df.drop(["solde", "reset", "shifted_reset"])

        return df
