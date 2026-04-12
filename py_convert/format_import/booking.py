import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportBooking(ImportBase):
    """Gestion d'import d'un fichier CSV de booking.com."""
    # Je n'ai reçu qu'un fichier au format numbers que j'ai converti en CSV
    def name(self):
        return "BOOKING.COM"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".csv":
            run_error(f"Le format {self.name()} nécessite un fichier .csv")
            return False
        return True
    
    def process_file(self):
        cols_name = {
            "Libellé de relevé": pl.String,
            "Date d'arrivée": pl.String,
            "Date de départ": pl.String,
            "Date du versement": pl.String,
            "Nom de l'établissement": pl.String,
            "Montant brut": pl.Float64,
            "Montant de la transaction": pl.Float64,
        }
        df = pl.read_csv(
            self.path, 
            has_header=True, 
            columns=list(cols_name.keys()),
            separator=",", 
            skip_lines=0,
            schema_overrides=cols_name,
            null_values=["-"],
            )
        
        # Suppression des lignes Payout
        df = df.filter(pl.col("Date de départ").is_not_null())
        
        # Conversion des colonnes de dates
        for col in ("Date d'arrivée", "Date de départ", "Date du versement"):
            df = df.with_columns(pl.col(col).str.strptime(pl.Date, "%Y-%m-%d"))
            
        # Remplacement des dates de début et de fin par le minimum et le maximum du versement
        df = df.with_columns(
            pl.col("Date d'arrivée").min().over("Libellé de relevé").alias("Date d'arrivée"),
            pl.col("Date de départ").max().over("Libellé de relevé").alias("Date de départ"),
        )
        
        # Création des écritures
        entries = []
        for row in df.iter_rows(named=True):
            lib = (f"BOOKING.COM {row["Date d'arrivée"].strftime("%d/%m/%Y")} - "
                   f"{row["Date de départ"].strftime("%d/%m/%Y")} "
                   f"{row["Nom de l'établissement"]}"
                   )
            # Ligne du compte de recette nette
            entries.append({
            "JournalCode": "ENC",
            "EcritureDate": row["Date du versement"],
            "CompteNum": "51100000",
            "PieceDate": row["Date du versement"],
            "EcritureLib": lib, 
            "PieceRef": row["Libellé de relevé"],
            "Debit": row["Montant de la transaction"], 
            "Credit": 0.0,
            })
            
            # Ligne du compte de recette brute
            entries.append({
            "JournalCode": "ENC",
            "EcritureDate": row["Date du versement"],
            "CompteNum": "70600000",
            "PieceDate": row["Date du versement"],
            "EcritureLib": lib, 
            "PieceRef": row["Libellé de relevé"],
            "Debit": 0.0, 
            "Credit": row["Montant brut"],
            })
            
            # Ligne des frais BOOKING
            entries.append({
            "JournalCode": "ENC",
            "EcritureDate": row["Date du versement"],
            "CompteNum": "62200000",
            "PieceDate": row["Date du versement"],
            "EcritureLib": lib, 
            "PieceRef": row["Libellé de relevé"],
            "Debit": row["Montant brut"] - row["Montant de la transaction"], 
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
        
        # Regroupement des comptes qui ont le même numéro de pièce
        df = df.group_by(["PieceRef", "CompteNum", "EcritureLib"]).agg([
            pl.col("JournalCode").first().alias("JournalCode"),
            pl.col("EcritureDate").first().alias("EcritureDate"),
            pl.col("PieceDate").first().alias("PieceDate"),
            pl.col("Debit").sum().alias("Debit"),
            pl.col("Credit").sum().alias("Credit")
        ])
        
        # Tri par date
        df = df.sort("EcritureDate", "PieceRef")

        return df