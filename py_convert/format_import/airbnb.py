import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportUberEats(ImportBase):
    """Gestion d'import d'un fichier CSV au format rapport de AIRBNB."""
    
    def name(self):
        return "AIRBNB"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".csv":
            run_error(f"Le format {self.name()} nécessite un fichier .csv")
            return False
        return True
    
    def process_file(self):
        cols_name = {
            "Date": pl.String,
            "Date de début": pl.String,
            "Date de fin": pl.String,
            "Logement": pl.String,
            "Montant": pl.Float64,
            "Frais de service": pl.Float64,
            "Revenus bruts": pl.Float64,
        }
        df = pl.read_csv(
            self.path, 
            has_header=True, 
            columns=list(cols_name.keys()),
            separator=",", 
            skip_lines=0,
            schema_overrides=cols_name,
            )
        
        # Suppression des lignes Payout
        df = df.filter(pl.col("Logement").is_not_null())
        
        # Remplissage des valeurs manquantes
        df = df.with_columns(
            pl.when(pl.col("Revenus bruts").is_null())
            .then(pl.col("Montant") + pl.col("Frais de service"))
            .otherwise(pl.col("Revenus bruts"))
            .alias("Revenus bruts")
            ).drop("Frais de service")
        
        # Conversion des colonnes de dates
        for col in ("Date", "Date de début", "Date de fin"):
            df = df.with_columns(pl.col(col).str.strptime(pl.Date, "%m/%d/%Y"))
        
        # Tri par date
        df = df.sort("Date")
        
        # Création des écritures
        entries = []
        for row in df.iter_rows(named=True):
            lib = (f"AIRBNB {row["Date de début"].strftime("%d/%m/%Y")} - "
                   f"{row["Date de fin"].strftime("%d/%m/%Y")} "
                   f"{row["Logement"]}"
                   )
            # Ligne du compte de recette nette
            entries.append({
            "JournalCode": "ENC",
            "EcritureDate": row["Date"],
            "CompteNum": "51100000",
            "PieceDate": row["Date"],
            "EcritureLib": lib, 
            "Debit": row["Montant"], 
            "Credit": 0.0,
            })
            
            # Ligne du compte de recette brute
            entries.append({
            "JournalCode": "ENC",
            "EcritureDate": row["Date"],
            "CompteNum": "70600000",
            "PieceDate": row["Date"],
            "EcritureLib": lib, 
            "Debit": 0.0, 
            "Credit": row["Revenus bruts"],
            })
            
            # Ligne des frais AIRBNB
            entries.append({
            "JournalCode": "ENC",
            "EcritureDate": row["Date"],
            "CompteNum": "62200000",
            "PieceDate": row["Date"],
            "EcritureLib": lib, 
            "Debit": row["Revenus bruts"] - row["Montant"], 
            "Credit": 0.0,
            })

        df = pl.DataFrame(entries)
        
        # Inversion des débits et crédits pour les montants négatifs
        df = df.with_columns([
            pl.when((pl.col("Debit") < 0) | (pl.col("Credit") < 0))
            .then(-pl.col("Credit"))
            .otherwise(pl.col("Debit"))
            .alias("Debit"),

            pl.when((pl.col("Debit") < 0) | (pl.col("Credit") < 0))
            .then(-pl.col("Debit"))
            .otherwise(pl.col("Credit"))
            .alias("Credit")
        ])
        
        # Suppression des lignes avec débit et crédit à 0
        df = df.filter((pl.col("Debit") != 0) | (pl.col("Credit") != 0))

        return df