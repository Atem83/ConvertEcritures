from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl
from PySide6 import QtWidgets

from py_convert.error import run_error

class SettingsBase(ABC):
    """Classe abstraite pour les méthodes de paramétrage d'écritures."""
    def __init__(self, 
                 myframe: QtWidgets.QFrame | None = None, 
                 df: pl.DataFrame | None = None,
                 dir: Path = Path.home() / "Desktop"
                 ):
        self._entries = df
        self.myframe = myframe
        self._dir = dir
    
    @abstractmethod
    def name(self) -> str:
        """Nom de la méthode de paramétrage."""
        pass
    
    @abstractmethod
    def get_allowed_import(self) -> list[str]:
        """Liste des imports autorisés pour ce paramètre."""
        pass
    
    @abstractmethod
    def process_file(self):
        """Transforme le dataframe d'écritures comptables."""
        pass
    
    @property
    def entries(self) -> pl.DataFrame:
        """Liste des écritures à transformer."""
        return self._entries
    
    def modify_data(self):
        """Méthode principale de paramétrage."""
        try:
            self._entries = self.process_file()
        except Exception as e:
            run_error("Une erreur est survenue lors du paramétrage.", details=str(e))
            print(e)
            return
    
    def empty_col(self, df: pl.DataFrame, col: str | list[str]) -> pl.DataFrame:
        """Vide la ou les colonnes spécifiées dans les écritures."""
        if type(col) == str:
            col = [col]
        dtype = df.schema
        
        for c in col:
            df = df.with_columns(pl.lit(None).cast(dtype[c]).alias(c))
        
        return df
    
    def replace_str(
        self,
        df: pl.DataFrame, 
        col: str, 
        old_str: str, 
        new_str: str, 
        filter: dict[str] | None = None
        ) -> pl.DataFrame:
        """
        Remplace dans une colonne toutes les occurrences d'une chaine par une autre.

        Args:
            col (str): Le nom de la colonne à modifier.
            new_str (str): La nouvelle chaine à insérer.
            old_str (str): Le début de l'ancienne chaine à remplacer.
            filter (dict, optional): Un filtre pour la colonne.
        """
        
        dtype = df.schema
        if not (dtype[col] == pl.String or dtype[col] == pl.Utf8):
            msg = "Erreur de format de colonne.\n" 
            msg += f"Le format de la colonne {col} n'est pas string."
            run_error(msg)
            return
        
        if filter == None:
            df = df.with_columns(
                pl.when(pl.col(col).str.starts_with(old_str))
                .then(pl.lit(new_str))
                .otherwise(pl.col(col))
                .alias(col)
            )
        else:
            filtre_col = list(filter.keys())[0]
            filtre_val = filter[filtre_col]
            df = df.with_columns(
                pl.when(
                    pl.col(filtre_col) == filtre_val,
                    pl.col(col).str.starts_with(old_str)
                )
                .then(pl.lit(new_str))
                .otherwise(pl.col(col))
                .alias(col)
            )

        return df
    
    def swapGenToAux(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remplace le numéro de compte général par 
        le numéro de compte auxiliaire s'il y en a un."""
        return df.with_columns(
            pl.when(pl.col("CompAuxNum").is_null() == False)
            .then(pl.col("CompAuxNum"))
            .otherwise(pl.col("CompteNum"))
            .alias("CompteNum")
            )
    
    def group_accounts(
        self,
        df: pl.DataFrame, 
        by: str, 
        df_filtered: pl.DataFrame = None,
        group: list[str] | str = "7", 
        replace: list[str] | str = "7",
        exclude: list[str] | str = None
        ) -> pl.DataFrame:
        """Groupe les comptes de produits en fonction de divers paramètres.
        
        Args:
            df (polars.DataFrame): Le DataFrame à grouper.
            by (str): Le compte comptable de remplacement.
            df_filtered (polars.DataFrame, optional): permet de ne grouper qu'une liste de factures choisies. Defaults to None.
            group (list[str] | str, optional): Les numéros de compte qui déclenchent le groupement. Defaults to "7".
            replace (list[str] | str, optional): Les numéros de compte qui seront remplacés si un groupement est déclenché. Defaults to "7".
            exclude (list[str] | str, optional): Les numéros de compte qui ne seront pas remplacés même si un groupement est déclenché. Defaults to None.
        """
        
        # Si un seul CompteNum est fourni, le transforme en liste
        if isinstance(group, str):
            group = [group]
        if isinstance(exclude, str) and exclude:
            exclude = [exclude]
        if isinstance(replace, str) and replace:
            replace = [replace]
        if df_filtered is None:
            df_filtered = df
        
        # Conserver les lignes où CompteNum commence par replace
        replace_condition = pl.lit(False)
        for account in replace:
            replace_condition |= pl.col("CompteNum").str.starts_with(account)
        df_target = df_filtered.filter(replace_condition)
        
        # Exclure les PieceRef contenant des comptes exclus s'il y en a
        if exclude:
            exclude_condition = pl.lit(False)
            for account in exclude:
                exclude_condition |= pl.col("CompteNum").str.starts_with(account)
            
            # Filtrer pour conserver les écritures exclues à récupérer plus tard
            df_exclude = df_target.filter(exclude_condition)
            
            # Filtrer pour ne conserver que les écritures qui ne contiennent pas de comptes exclus
            df_target = df_target.filter(~exclude_condition)
        
        # Conserver uniquement les PieceRef contenant des comptes commençant par les valeurs group
        condition = pl.lit(False)
        for prefix in group:
            condition |= pl.col("CompteNum").str.starts_with(prefix)
        df_target = df_target.filter(condition.any().over("PieceRef"))
    
        # Regrouper les lignes par PieceRef pour les comptes commençant par replace
        df_target = df_target.group_by("PieceRef").agg([
            pl.col("JournalCode").first(),
            pl.col("JournalLib").first(),
            pl.col("EcritureNum").first(),
            pl.col("EcritureDate").first(),
            pl.lit(by).alias("CompteNum"),  # Remplacer par le compte by
            pl.col("CompteLib").first(),
            pl.col("CompAuxNum").first(),
            pl.col("CompAuxLib").first(),
            pl.col("PieceDate").first(),
            pl.col("EcritureLib").first(),
            pl.col("Debit").sum(),  # Somme des Debit
            pl.col("Credit").sum(),  # Somme des Credit
            pl.col("EcritureLet").first(),
            pl.col("DateLet").first(),
            pl.col("ValidDate").first(),
            pl.col("Montantdevise").first(),
            pl.col("Idevise").first(),
            pl.col("EcheanceDate").first()
        ])
        
        # ne permet qu'un Debit ou Credit par ligne en fonction du plus élevé des deux
        df_target = df_target.with_columns([
            # Calculer le nouveau Debit
            pl.when(pl.col("Debit") > pl.col("Credit"))
            .then(pl.col("Debit") - pl.col("Credit"))
            .otherwise(0).alias("Debit"),
            # Calculer le nouveau Credit
            pl.when(pl.col("Credit") > pl.col("Debit"))
            .then(pl.col("Credit") - pl.col("Debit"))
            .otherwise(0).alias("Credit")
        ])
        
        # Réorganise l'ordre des colonnes
        df_target = df_target.select(
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
            )
        
        # Récupération de toutes les lignes non modifiées
        df_reverse_target = df_filtered.filter(~(
            (replace_condition) & 
            (pl.col("PieceRef").is_in(df_target["PieceRef"]))
        ))
        
        # Concaténation des lignes non modifiées avec les lignes regroupées
        df_result = pl.concat([df_reverse_target, df_target])
        
        # Récupération des lignes exclues
        if exclude:
            # Création d'une clé composite pour vérifier la présence de chaque ligne
            df_result = df_result.with_columns(
                (pl.col("PieceRef").cast(pl.Utf8) + "_" + 
                 pl.col("CompteNum").cast(pl.Utf8) + "_" + 
                 pl.col("Debit").cast(pl.Utf8) + "_" + 
                 pl.col("Credit").cast(pl.Utf8)
                 )
                .alias("contains_excluded")
            )
            df_exclude = df_exclude.with_columns(
                (pl.col("PieceRef").cast(pl.Utf8) + "_" + 
                 pl.col("CompteNum").cast(pl.Utf8) + "_" + 
                 pl.col("Debit").cast(pl.Utf8) + "_" + 
                 pl.col("Credit").cast(pl.Utf8)
                 )
                .alias("contains_excluded")
            )
            
            # Utilisation de la clé composite pour filtrer les lignes exclues déjà présentes
            df_exclude = df_exclude.filter(
                ~pl.col("contains_excluded").is_in(df_result["contains_excluded"])
            )
            df_exclude = df_exclude.drop("contains_excluded")
            df_result = df_result.drop("contains_excluded")
            
            # Concaténation des lignes exclues qui ne sont pas déjà présente dans df_result
            df_result = pl.concat([df_result, df_exclude])
        
        # Récupération des factures non présentes dans df_filtered
        df = df.filter(~pl.col("PieceRef").is_in(df_filtered["PieceRef"]))
        df_result = pl.concat([df_result, df])
        
        # Trier le DataFrame final par PieceRef pour conserver l'ordre
        df_result = df_result.sort(["EcritureDate", "PieceRef", "CompteNum"])
        
        return df_result
    
    
    
    
    
    
    
    