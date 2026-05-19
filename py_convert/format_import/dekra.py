from datetime import date

import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportDekra(ImportBase):
    """Gestion d'import d'un fichier CSV au format rapport de remises CB DEKRA."""
    
    def name(self):
        return "DEKRA CB INTERNET"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".csv":
            run_error(f"Le format {self.name()} nécessite un fichier .csv")
            return False
        return True
    
    def process_file(self):
        cols_name = {
            "NOM_CLIENT": pl.String,
            "AUROREXP_FACTID": pl.String,
            "DATERESA": pl.Date,
            "DATE_INTEG_COMPTA_PAIEMENT": pl.String,
            "MONTANT1": pl.String,
        }
        df = pl.read_csv(
            self.path, 
            has_header=True, 
            columns=list(cols_name.keys()),
            separator=",", 
            skip_lines=6,
            schema_overrides=cols_name,
            null_values=["-"]
        ).rename({"MONTANT1": "Credit"})
        
        # Conversion des montants au Credit
        df = df.with_columns(
            pl.col("Credit")
            .str.replace(" €", "")
            .str.replace(",", ".")
            .cast(pl.Float64)
        )
        
        # Séparation des données de deux colonnes
        df = df.with_columns(
            pl.col("AUROREXP_FACTID")
                .str.split("\n").list.get(0)
                .str.strip_chars().alias("PieceRef"),
            pl.col("DATE_INTEG_COMPTA_PAIEMENT")
                .str.split("\n").list.get(0)
                .str.strip_chars().alias("EcritureDate"),
            pl.col("DATE_INTEG_COMPTA_PAIEMENT")
                .str.split("\n").list.get(1)
                .str.strip_chars().alias("NUM_VIREMENT"),
        ).drop(["AUROREXP_FACTID", "DATE_INTEG_COMPTA_PAIEMENT"])
        
        # Suppression des lignes en attente de virement
        df = df.filter(pl.col("NUM_VIREMENT").is_not_null())
        
        # Ajout de certaines colonnes
        df = df.with_columns(
            pl.lit("ENC").alias("JournalCode"),
            pl.lit("58040000").alias("CompteNum"),
            (pl.lit("REM CB INTERNET ") + 
             pl.col("NOM_CLIENT")).str.to_uppercase().alias("EcritureLib"),
            pl.lit(0.00).alias("Debit"),
        ).drop("NOM_CLIENT")
        
        # Conversion de la date
        df = df.with_columns(
            pl.col("EcritureDate").str.to_date("%d/%m/%Y").alias("EcritureDate")
        )
        
        # Ajout de la mention sur les réservations annulées
        df = df.with_columns(
            pl.when((pl.col("PieceRef").is_null()) & 
                    (pl.col("DATERESA") < pl.lit(date.today())))
            .then(pl.lit("ANNUL. ") + pl.col("EcritureLib"))
            .otherwise(pl.col("EcritureLib"))
            .alias("EcritureLib")
        )
        
        # Ajout de la mention sur les réservations lointaines
        df = df.with_columns(
            pl.when((pl.col("PieceRef").is_null()) & 
                    (pl.col("DATERESA") > pl.lit(date.today())))
            .then(pl.col("EcritureLib") + " " + pl.col("DATERESA").dt.strftime("%d/%m/%Y"))
            .otherwise(pl.col("EcritureLib"))
            .alias("EcritureLib")
        ).drop("DATERESA")
        
        # Calcul de la somme des crédits par NUM_VIREMENT
        debit = df.group_by("NUM_VIREMENT").agg(
            pl.col("JournalCode").first(),
            pl.lit("58040000").alias("CompteNum"),
            (pl.lit("REM CB INTERNET ") + 
             pl.col("EcritureDate").first()
             .dt.strftime("%d/%m/%Y")).alias("EcritureLib"),
            pl.lit(None).alias("PieceRef"),
            pl.col("EcritureDate").first(),
            pl.col("Credit").sum().alias("Debit"),
            pl.lit(0.0).alias("Credit"),
        )
        
        # Concaténation avec le dataframe original
        df = pl.concat([df, debit.select(df.columns)])
        
        # Tri par virement puis par PieceRef
        df = df.sort(["NUM_VIREMENT", "PieceRef"]).drop("NUM_VIREMENT")
        
        # Limitation des PieceRef à 5 caractères en partant de la fin
        # Permet de faire correspondre les numéros quand ils reset
        df = df.with_columns(pl.col("PieceRef").str.slice(-5).alias("PieceRef"))

        return df