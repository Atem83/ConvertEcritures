import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportBiborne(ImportBase):
    """Gestion d'import d'un fichier CSV/XLS au format du logiciel Biborne."""
    
    def name(self):
        return "BIBORNE"
    
    def validate_format(self):
        if self.path.suffix.lower() not in [".csv", ".xls", ".xlsx"]:
            run_error(f"Le format {self.name()} nécessite un fichier .csv, .xls ou .xlsx")
            return False
        return True
    
    def process_file(self):
        titles = {
            "Le jour": pl.Date,
            "CA HT": pl.Float64,
            "CA_5_5%_HT": pl.Float64,
            "CA_10%_HT": pl.Float64,
            "CA_20%_HT": pl.Float64,
            "TVA 5.5%": pl.Float64,
            "TVA 10%": pl.Float64,
            "TVA 20%": pl.Float64,
            "Total Espèce": pl.Float64,
            "Total Twint": pl.Float64,
            "Total Tr": pl.Float64,
            "Total CB": pl.Float64,
            "Cach Glory": pl.Float64,
            "cheque_vacances": pl.Float64,
            "Total Avoir Decaisse": pl.Float64,
            "Promotion": pl.Float64,
            "Total Dishop": pl.Float64,
            "TOTAL_ESPÈCES_MONNAYEUR": pl.Float64,
        }
        
        if self.path.suffix.lower() == ".csv":
            df = pl.read_csv(
            source=self.path, 
            separator=",", 
            columns=list(titles),
            has_header=True, 
            schema_overrides=dict.fromkeys(titles, pl.String),
            )
        elif self.path.suffix.lower() in [".xls", ".xlsx"]:
            df = pl.read_excel(
            source=self.path, 
            engine="calamine",
            columns=list(titles),
            has_header=True,
            schema_overrides=dict.fromkeys(titles, pl.String),
            )
        
        # Suppression des données inutiles
        df = df.filter(pl.col("Le jour") != "TOTAL")
        df = df.with_columns(
            pl.all()
            .str.replace("€", "") # Retirer €
            .str.strip_chars() # Espaces début/fin
        )

        # Conversion des types des colonnes
        for col, dtype in titles.items():
            try:
                df = df.with_columns(pl.col(col).cast(dtype))
            except:
                df = df.with_columns(pl.col(col).str.strptime(pl.Date, "%d/%m/%Y").cast(dtype))
           
        # Renommage de certaines colonnes
        df = df.rename({
            "CA_5_5%_HT": "CA HT 5.5%",
            "CA_10%_HT": "CA HT 10%",
            "CA_20%_HT": "CA HT 20%",
        })
        
        
        # Calcul des CA HT par taux de TVA
        if "CA HT 5.5%" not in df.columns:
            df = df.with_columns((pl.col("TVA 5.5%") / 0.055).alias("CA HT 5.5%"))
        if "CA HT 10%" not in df.columns:
            df = df.with_columns((pl.col("TVA 10%") / 0.10).alias("CA HT 10%"))
        if "CA HT 20%" not in df.columns:
            df = df.with_columns((pl.col("TVA 20%") / 0.20).alias("CA HT 20%"))

        # Arrondir toutes les colonnes float à 2 décimales
        df = df.with_columns(pl.col(pl.Float64).round(2))
        
        # Calcul de l'écart entre le total et les montants
        df = df.with_columns(
            (pl.col("CA HT")
             - pl.col("CA HT 5.5%")
             - pl.col("CA HT 10%")
             - pl.col("CA HT 20%")
             ).round(2).alias("ECART CA HT")
            )
        
        # Rajout des écarts de CA HT < 1 au CA HT à 10% puis je réinitialise l'écart à 0
        df = df.with_columns(
            pl.when(pl.col("ECART CA HT").abs() < 1)
            .then(pl.col("CA HT 10%") + pl.col("ECART CA HT"))
            .otherwise(pl.col("CA HT 10%"))
            .alias("CA HT 10%"),
            pl.when(pl.col("ECART CA HT").abs() < 1)
            .then(0)
            .otherwise(pl.col("ECART CA HT"))
            .alias("ECART CA HT")
        )
        
        # Calcul de l'écart entre le CA TTC recalculé et le total des moyens de paiement
        df = df.with_columns(
            (pl.col("CA HT 5.5%")
             + pl.col("CA HT 10%")
             + pl.col("CA HT 20%")
             + pl.col("TVA 5.5%")
             + pl.col("TVA 10%")
             + pl.col("TVA 20%")
             + pl.col("ECART CA HT")
             - pl.col("Total Espèce")
             - pl.col("Total Twint")
             - pl.col("Total Tr")
             - pl.col("Total CB")
             - pl.col("Cach Glory")
             - pl.col("cheque_vacances")
             - pl.col("Total Avoir Decaisse")
             - pl.col("Total Dishop")
             - pl.col("TOTAL_ESPÈCES_MONNAYEUR")
             ).round(2).alias("ECART ECRITURE")
            )
        
        # Création des écritures
        entries = []
        jrn = "CAI"
        for row in df.iter_rows(named=True):
            # CA HT 5,5%
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "70150000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "CA HT 5,5%", 
            "Debit": 0.0, 
            "Credit": row["CA HT 5.5%"],
            })
            
            # TVA 5,5%
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "44571500",
            "PieceDate": row["Le jour"],
            "EcritureLib": "TVA COLL 5,5%", 
            "Debit": 0.0, 
            "Credit": row["TVA 5.5%"],
            })
            
            # CA HT 10%
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "70110000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "CA HT 10%", 
            "Debit": 0.0, 
            "Credit": row["CA HT 10%"],
            })
            
            # TVA 10%
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "44571100",
            "PieceDate": row["Le jour"],
            "EcritureLib": "TVA COLL 10%", 
            "Debit": 0.0, 
            "Credit": row["TVA 10%"],
            })
            
            # CA HT 20%
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "70120000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "CA HT 20%", 
            "Debit": 0.0, 
            "Credit": row["CA HT 20%"],
            })
            
            # TVA 20%
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "44571200",
            "PieceDate": row["Le jour"],
            "EcritureLib": "TVA COLL 20%", 
            "Debit": 0.0, 
            "Credit": row["TVA 20%"],
            })
            
            # Total Espèce
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "53000000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. ESPECES", 
            "Debit": row["Total Espèce"], 
            "Credit": 0.0,
            })
            
            # Total Twint
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "58060000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. TWINT", 
            "Debit": row["Total Twint"], 
            "Credit": 0.0,
            })
            
            # Total Tr
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "58020000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. TR", 
            "Debit": row["Total Tr"], 
            "Credit": 0.0,
            })
            
            # Total CB
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "58010000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. CB", 
            "Debit": row["Total CB"], 
            "Credit": 0.0,
            })
            
            # Cach Glory
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "58070000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. CACH GLORY", 
            "Debit": row["Cach Glory"], 
            "Credit": 0.0,
            })
            
            # cheque_vacances
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "58040000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. CHQ VAC", 
            "Debit": row["cheque_vacances"], 
            "Credit": 0.0,
            })
            
            # Total Avoir Decaisse
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "75800000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "AVOIR DECAISSE", 
            "Debit": row["Total Avoir Decaisse"],
            "Credit": 0.0, 
            })
            
            # TOTAL STRIPE DISHOP
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "58050000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. STRIPE", 
            "Debit": row["Total Dishop"], 
            "Credit": 0.0,
            })
            
            # TOTAL_ESPÈCES_MONNAYEUR
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "53000000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ENC. ESPÈCES MONNAYEUR", 
            "Debit": row["TOTAL_ESPÈCES_MONNAYEUR"], 
            "Credit": 0.0,
            })
            
            # ECART CA HT
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "47100000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ECART CA HT", 
            "Debit": 0.0, 
            "Credit": row["ECART CA HT"],
            })
            
            # ECART ECRITURE
            entries.append({
            "JournalCode": jrn,
            "EcritureDate": row["Le jour"],
            "CompteNum": "65800000",
            "PieceDate": row["Le jour"],
            "EcritureLib": "ECART ECRITURE", 
            "Debit": row["ECART ECRITURE"], 
            "Credit": 0.0,
            })
        
        # Ajout de l'écriture concernant les promotions
        entries.append({
            "JournalCode": "OD",
            "EcritureDate": df[-1, "Le jour"],
            "CompteNum": "62340000",
            "PieceDate": df[-1, "Le jour"],
            "EcritureLib": "PROMOTIONS", 
            "Debit": df["Promotion"].sum(), 
            "Credit": 0.0,
        })
        entries.append({
            "JournalCode": "OD",
            "EcritureDate": df[-1, "Le jour"],
            "CompteNum": "60150000",
            "PieceDate": df[-1, "Le jour"],
            "EcritureLib": "PROMOTIONS", 
            "Debit": 0.0, 
            "Credit": df["Promotion"].sum(),
        })
        
        # Création du dataframe des écritures
        df = pl.DataFrame(entries)
        df = df.filter((pl.col("Credit") != 0) | (pl.col("Debit") != 0))
        
        # Recalcule des débits et crédits si valeur négative
        df = df.with_columns((pl.col("Debit") - pl.col("Credit")).alias("Solde"))
        df = df.with_columns([
            pl.when(pl.col("Solde") > 0.0)
              .then(pl.col("Solde").abs())
              .otherwise(0.0)
              .alias("Debit"),
            pl.when(pl.col("Solde") < 0.0)
              .then(pl.col("Solde").abs())
              .otherwise(0.0)
              .alias("Credit")
        ]).drop("Solde")

        return df