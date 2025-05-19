import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportCourtage(ImportBase):
    """Gestion d'import d'un fichier Excel au format du logiciel vosfactures.fr."""
    
    def name(self):
        return "COURTAGE"
    
    def validate_format(self):
        if self.path.suffix.lower() == ".csv":
            return True
        else:
            run_error(f"Le format {self.name()} nécessite un fichier .csv")
            return False
    
    def process_file(self):
        try:
            df = pl.read_csv(self.path, separator=";")
        except pl.exceptions.ComputeError:
            df = pl.read_csv(self.path, separator=";", encoding='ISO-8859-1')
            
        df = df.rename({
            "NumFacture": "PieceRef",
            "DateFacture": "EcritureDate",
            "MontantTTC": "Debit",
            "DateEcheance": "EcheanceDate",
            })
        
        df = df.with_columns(
            pl.col("EcritureDate").str.strptime(pl.Date, "%d/%m/%Y"),
            pl.col("Debit").str.replace(",", ".").cast(pl.Float64),
            pl.col("EcheanceDate").str.strptime(pl.Date, "%d/%m/%Y"),
            )
        
        # Liste des banques reconnues
        client_list = [
            {"entity": "BANQUE POSTALE", "account": "CBAN", "label": "BP", "product": "70651300"},
            {"entity": "BNP PARIBAS", "account": "CBNP", "label": "BNP", "product": "70650100"},
            {"entity": "BANQUE POPULAIRE MEDITERRANEE", "account": "CBPC", "label": "BPMED", "product": "70651100"},
            {"entity": "CAISSE D'EPARGNE", "account": "CCAI", "label": "CE", "product": "70650700"},
        ]
        
        df = df.with_columns(
            pl.lit("VE").alias("JournalCode"),
            pl.lit(0.0).alias("Credit"),
            pl.lit("41100000").alias("CompteNum"),
            pl.lit(None).alias("CompAuxNum"),
            pl.lit(None).alias("EcritureLib"),
            pl.col("LibAffaire").str.to_uppercase(),
            pl.col("EcritureDate").alias("PieceDate"),
        )

        # Remplace le compte auxiliaire et le libellé de l'écriture si la banque est reconnue
        for client in client_list:
            df = df.with_columns(
                pl.when(pl.col("BqDestRaison").str.contains(client["entity"]))
                .then(pl.lit(client["account"]))
                .otherwise(pl.col("CompAuxNum"))
                .alias("CompAuxNum")
            )
            
            df = df.with_columns(
                pl.when(pl.col("BqDestRaison").str.contains(client["entity"]))
                .then(pl.lit(client["label"]) + " - " + pl.col("LibAffaire"))
                .otherwise(pl.col("EcritureLib"))
                .alias("EcritureLib")
            )
        
        # Remplace le libellé de l'écriture si la banque n'est pas reconnue
        df = df.with_columns(
            pl.when(
                (pl.col("EcritureLib").is_null()) &
                (pl.col("BqDestRaison").is_not_null())
                )
            .then(pl.col("BqDestRaison") + " - " + pl.col("LibAffaire"))
            .otherwise(pl.col("EcritureLib"))
            .alias("EcritureLib")
            )
        
        # Remplace le libellé de l'écriture s'il n'y a pas de banque
        df = df.with_columns(
            pl.when(pl.col("EcritureLib").is_null())
            .then(pl.col("LibAffaire"))
            .otherwise(pl.col("EcritureLib"))
            .alias("EcritureLib")
            )
        
        # Remplace le compte auxiliaire les autres clients
        df = df.with_columns(
            pl.when((pl.col("CompAuxNum").is_null()))
              .then(pl.lit("C") + pl.col("EcritureLib").str.slice(0,1))
              .otherwise(pl.col("CompAuxNum"))
              .alias("CompAuxNum")
        )
        
        # Créer un deuxième df pour les comptes de produits
        df_7 = df.with_columns(
            pl.col("Credit").alias("Debit"),
            pl.col("Debit").alias("Credit"),
            pl.lit(None).alias("CompteNum")
            )
        
        # Remplace les comptes généraux pour les banques reconnues
        for client in client_list:
            df_7 = df_7.with_columns(
                pl.when(pl.col("CompAuxNum") == client["account"])
                .then(pl.lit(client["product"]))
                .otherwise(pl.col("CompteNum"))
                .alias("CompteNum")
            )
        
        # Remplace les comptes généraux pour les autres clients
        df_7 = df_7.with_columns(
            pl.when((pl.col("CompteNum").is_null()))
              .then(pl.lit("70620000"))
              .otherwise(pl.col("CompteNum"))
              .alias("CompteNum")
        )
        
        # Vide les comptes auxiliaires
        df_7 = df_7.with_columns(pl.lit(None).alias("CompAuxNum"))
        
        # Fusionne les deux DataFrames
        df = df.vstack(df_7)
        
        # Tri par numéro de facture et par numéro de compte
        df = df.sort(["PieceRef", "CompteNum"])
        
        return df