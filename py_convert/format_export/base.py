import re
from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from py_convert.error import run_error

class ExportBase(ABC):
    """Classe abstraite pour les méthodes d'exportation d'écritures."""
    def __init__(
        self, 
        df: pl.DataFrame | None = None, 
        dir: Path = Path.home() / "Desktop"
        ):
        self._entries = df
        self._dir = dir
        
    @abstractmethod
    def name(self) -> str:
        """Nom de la méthode d'export."""
        pass
    
    @abstractmethod
    def extension(self) -> str:
        """Extension du fichier exporté."""
        pass
    
    @abstractmethod
    def process_file(self):
        """Exporte les écritures dans le format souhaité."""
        pass
    
    @property
    def entries(self) -> pl.DataFrame:
        """Liste des écritures à exporter."""
        return self._entries
    
    @property
    def name_export(self) -> Path:
        """Nom du fichier d'export."""
        return self._dir / ("Export" + self.extension())
    
    @property
    def path_export(self) -> Path:
        """Chemin du fichier d'export."""
        path = self.name_export
        
        while path.exists():
            # Sépare le nom du fichier et son extension
            raw_name = path.stem
            extension = path.suffix.lower()

            # Chercher le motif de la forme "(x)" à la fin du nom de fichier
            match = re.search(r'(\(\d+\))$', raw_name)

            if match:
                # Extraire le nombre entre parenthèses et l'incrémenter
                number = int(match.group(1).strip('()')) + 1
                # Remplacer l'ancien numéro par le nouveau
                new_name = re.sub(r'\(\d+\)$', f'({number})', raw_name)
            else:
                # Ajouter "(1)" à la fin du nom du fichier
                new_name = raw_name + " (1)"
            
            path = self.name_export.parent / (new_name + extension)
        return path
    
    @property
    def mandatory_cols(self) -> list[str]:
        """Liste des colonnes obligatoires pour l'export."""
        return [
            "JournalCode", 
            "EcritureDate", 
            "PieceRef", 
            "CompteNum", 
            "EcritureLib", 
            "EcritureLet", 
            "Debit", 
            "Credit"
            ]
    
    @property
    def mandatory_data(self) -> list[str]:
        """Liste des données obligatoires pour l'export."""
        return [
            "EcritureDate", 
            "CompteNum", 
            "Debit", 
            "Credit"
            ]
    
    def validate_entries(self) -> bool:
        """Vérifie si les écritures contiennent les données obligatoires pour l'export."""
        for col in self.mandatory_cols:
            if col not in self.entries.columns:
                msg = f"Export impossible,\nLa colonne '{col}' est manquante."
                run_error(msg)
                return False
        
        for col in self.mandatory_data:
            if self.entries[col].is_null().any():
                msg = f"Export impossible,\n"
                msg += f"Des valeurs de la colonne '{col}' sont vides."
                run_error(msg)
                return False

        return True
    
    def export_data(self):
        """Méthode principale d'export."""
        if not self.validate_entries():
            return
            
        try:
            self.process_file()
        except Exception as e:
            run_error("Une erreur est survenue lors de l'export.", details=str(e))
            print(e)
            return
    
    def swapGenToAux(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remplace le numéro de compte général par 
        le numéro de compte auxiliaire s'il y en a un."""
        return df.with_columns(
            pl.when(pl.col("CompAuxNum").is_null() == False)
            .then(pl.col("CompAuxNum"))
            .otherwise(pl.col("CompteNum"))
            .alias("CompteNum")
            )