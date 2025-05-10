import polars as pl
from .base_settings import BaseSettings

class CASH(BaseSettings):
    """Gestion des paramètres de l'import du Grand Livre d'un compte 580."""
    def name(self):
        return "CAISSE"
    
    def get_allowed_import(self) -> list[str]:
        return ["PRESSE-PAPIER"]
    
    def validate_accounts(self):
        if not isinstance(self.account_530, str):
            msg = "L'argument 'account_530' doit être une chaîne de caractères"
            raise TypeError(
                msg)
        
        if not isinstance(self.account_580, str):
            msg = "L'argument 'account_580' doit être une chaîne de caractères"
            raise TypeError(
                msg)

    def process_file(self):
        # Récupération des comptes de caisse
        from py_convert.gui import AskCash
        self.cash = AskCash(self.myframe)
        self.cash.exec()
        self.account_530 = self.myframe.app.settings.account_530
        self.account_580 = self.myframe.app.settings.account_580
        self.validate_accounts()
        
        # Création des écritures du compte 580
        df_580 = self.entries
        df_580 = df_580.with_columns(
            pl.col("Debit").alias("Credit"),
            pl.col("Credit").alias("Debit")
            )
        df_580 = df_580.with_columns(pl.col("CompteNum").fill_null(self.account_580))

        # Création des écritures du compte de caisse
        df_caisse = self.entries
        df_caisse = df_caisse.with_columns(pl.col("CompteNum").fill_null(self.account_530))

        # Concatenation des deux dataframes
        df_merged = pl.concat([df_caisse, df_580])
        df_merged = df_merged.sort("EcritureDate")

        return df_merged