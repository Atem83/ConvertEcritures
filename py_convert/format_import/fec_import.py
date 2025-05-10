import polars as pl
from .base_import import BaseImport
from ..error import run_error

class FECImport(BaseImport):
    """Gestion d'import au format FEC"""
    
    def name(self):
        return "FEC"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".txt":
            run_error(f"Le format {self.name()} nécessite un fichier .txt")
            return False
        return True
    
    def process_file(self):
        with open(self.path, "r") as file:
            lignes = file.readlines()

            # Reconnaissance auto du séparateur du fichier format FEC
            nb_tabulations = sum(ligne.count('\t') for ligne in lignes)
            nb_verticales = sum(ligne.count('|') for ligne in lignes)

            if nb_tabulations > nb_verticales:
                sep = '\t'
            elif nb_verticales > nb_tabulations:
                sep = '|'
            else:
                run_error("Séparateur FEC non identifié")
                return None

        # Permet de changer l'encodage si un problème d'import survient
        # Utile par exemple pour les imports de FEC de Sage
        try:
            df = pl.read_csv(self.path, separator=sep)
        except pl.exceptions.ComputeError:
            df = pl.read_csv(self.path, separator=sep, encoding='ISO-8859-1')

        # Renomme les colonnes si les majuscules et minuscules ne correspondent pas
        list_col = [
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
            "Idevise",
            "EcheanceDate"
        ]
        lower_list_col = [name.lower() for name in list_col]
        
        date_col = [
            "EcritureDate",
            "PieceDate",
            "DateLet",
            "ValidDate",
            "EcheanceDate"
        ]
        
        currency_col = [
            "Debit",
            "Credit",
            "Montantdevise"
        ]

        # Corrige la casse dans les noms de colonne
        for column in df.columns:
            if column.lower() in lower_list_col:
                new_name = list_col[lower_list_col.index(column.lower())]
                df = df.rename({column: new_name})

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

        # Rajout de la colonne EcheanceDate
        df = df.with_columns(pl.lit(None).alias("EcheanceDate").cast(pl.Date))

        # Transforme les colonnes en type String
        for column in list_col:
            df = df.with_columns(pl.col(column).cast(pl.String))

        # Affecte None aux colonnes Date ne contenant que des espaces blancs
        # Permet d'éviter des erreurs dans des FEC avec des espaces dans la date
        # On pourrait le généraliser à toutes les colonnes String si nécessaire
        for column in date_col:
            df = df.with_columns(
                pl.when(pl.col(column).str.strip_chars() == "")
                .then(None)
                .otherwise(pl.col(column))
                .alias(column)
            )
            
            # Transforme les colonnes en type Date
            df = df.with_columns(pl.col(column).str.to_date("%Y%m%d"))
            
        # Change les virgules en points pour les nombres
        for column in currency_col:
            df = df.with_columns(
                pl.col(column)
                  .cast(pl.String)
                  .str.replace(",", ".")
                  .cast(pl.Float64)
                )
        
        # Réorganise l'ordre des colonnes
        df = df.select(list_col)

        return df
