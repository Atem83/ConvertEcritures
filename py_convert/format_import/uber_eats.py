import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportUberEats(ImportBase):
    """Gestion d'import d'un fichier CSV au format liste de paiements d'Uber Eats."""
    
    def name(self):
        return "UBER EATS"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".csv":
            run_error(f"Le format {self.name()} nécessite un fichier .csv")
            return False
        return True
    
    def process_file(self):
        df = pl.read_csv(
            self.path, 
            separator=",", 
            has_header=True, 
            skip_lines=1,
            infer_schema=False
            )
        
        cols_name = {
            "Numéro de référence du versement": "ID",
            "Id. de référence du versement": "ID",
            "Date du versement": "DATE VIREMENT",
            "Date de la commande": "DATE COMMANDE",
            "Ventes (TVA comprise)": "BRUT",
            "Ventes (TVA incluses)": "BRUT",
            "Promotions sur des articles (TVA comprise)": "PROMOS",
            "Promotion sur les plats/articles (TVA incluse)": "PROMOS",
            "Frais de livraison (TVA incluse)": "LIVRAISON",
            "Paiements divers (TVA comprise)": "DEPENSES PUB",
            "Autres paiements (TVA incluse)": "DEPENSES PUB",
            "Bon de réduction-restaurant": "TICKETS RESTO",
            "Titre-restaurant": "TICKETS RESTO",
            "Remboursements (TVA comprise)": "ERREURS COMMANDE",
            "Remboursements (TVA incluse)": "ERREURS COMMANDE",
            "Frais de mise en marché après rabais (TVA en sus)": "FRAIS UBER HT",
            "Frais de service de la Marketplace / frais de mise en relation après promotion (hors TVA)": "FRAIS UBER HT",
            "TVA sur les frais de mise en marché après rabais ": "FRAIS UBER TVA",
            "TVA sur les frais de service de la Marketplace / frais de mise en relation après offre": "FRAIS UBER TVA",
            "Versement total de la promotion ": "TOTAL",
            "Montant total ": "TOTAL"
        }
        cols_type = {
            "ID": pl.String,
            "DATE VIREMENT": pl.Date,
            "DATE COMMANDE": pl.Date,
            "BRUT": pl.Float64,
            "PROMOS": pl.Float64,
            "LIVRAISON": pl.Float64,
            "DEPENSES PUB": pl.Float64,
            "TICKETS RESTO": pl.Float64,
            "ERREURS COMMANDE": pl.Float64,
            "FRAIS UBER HT": pl.Float64,
            "FRAIS UBER TVA": pl.Float64,
            "TOTAL": pl.Float64
        }
        
        # Renommage des colonnes
        for old_col, new_col in cols_name.items():
            if old_col in df.columns:
                df = df.rename({old_col: new_col})
        
        # Conservation des colonnes pertinentes uniques
        df = df.select(list(dict.fromkeys(cols_name.values())))
        
        # Conversion des types des colonnes
        for col, dtype in cols_type.items():
            try:
                df = df.with_columns(pl.col(col).cast(dtype))
            except:
                df = df.with_columns(pl.col(col).str.strptime(pl.Date, "%d/%m/%Y").cast(dtype))
        
        # Agrégation des valeurs par date de commande puis par ID
        df = df.group_by(["DATE COMMANDE", "ID"]).agg([
            pl.col("DATE VIREMENT").first().alias("DATE VIREMENT"),
            pl.col("BRUT").sum().alias("BRUT"),
            pl.col("PROMOS").sum().alias("PROMOS"),
            pl.col("LIVRAISON").sum().alias("LIVRAISON"),
            pl.col("DEPENSES PUB").sum().alias("DEPENSES PUB"),
            pl.col("TICKETS RESTO").sum().alias("TICKETS RESTO"),
            pl.col("ERREURS COMMANDE").sum().alias("ERREURS COMMANDE"),
            pl.col("FRAIS UBER HT").sum().alias("FRAIS UBER HT"),
            pl.col("FRAIS UBER TVA").sum().alias("FRAIS UBER TVA"),
            pl.col("TOTAL").sum().alias("TOTAL")
            ]).sort("DATE COMMANDE")
        
        # Arrondir toutes les colonnes float à 2 décimales
        df = df.with_columns(pl.col(pl.Float64).round(2))
        
        # Calcul de l'écart entre le total et les montants
        df = df.with_columns(
            (pl.col("BRUT")
             + pl.col("PROMOS")
             + pl.col("LIVRAISON")
             + pl.col("DEPENSES PUB")
             - pl.col("TICKETS RESTO")
             + pl.col("ERREURS COMMANDE")
             + pl.col("FRAIS UBER HT")
             + pl.col("FRAIS UBER TVA")
             - pl.col("TOTAL")
             ).round(2).alias("ECART")
            )
        
        # Création des écritures
        entries = []
        for row in df.iter_rows(named=True):
            # Ligne du chiffre d'affaires TTC
            entries.append({
            "PieceRef": row["ID"],
            "JournalCode": "ENC",
            "EcritureDate": row["DATE VIREMENT"],
            "CompteNum": "51100000",
            "PieceDate": row["DATE VIREMENT"],
            "EcritureLib": f"UBER EATS BRUT {row["DATE COMMANDE"].strftime("%d/%m/%Y")}", 
            "Debit": 0.0, 
            "Credit": abs(row["BRUT"]) - abs(row["PROMOS"]) + abs(row["LIVRAISON"]),
            })
            
            # Ligne des dépenses publicitaires HT
            ads = round(abs(row["DEPENSES PUB"]) / 1.2, 2)
            entries.append({
            "PieceRef": row["ID"],
            "JournalCode": "ENC",
            "EcritureDate": row["DATE VIREMENT"],
            "CompteNum": "62300000",
            "PieceDate": row["DATE VIREMENT"],
            "EcritureLib": "UBER EATS PUB", 
            "Debit": ads, 
            "Credit": 0.0,
            })
            
            # Ligne des dépenses publicitaires TVA
            entries.append({
            "PieceRef": row["ID"],
            "JournalCode": "ENC",
            "EcritureDate": row["DATE VIREMENT"],
            "CompteNum": "44566000",
            "PieceDate": row["DATE VIREMENT"],
            "EcritureLib": "UBER EATS PUB", 
            "Debit": round(abs(row["DEPENSES PUB"]) - ads, 2), 
            "Credit": 0.0,
            })
            
            # Ligne des tickets restaurants
            entries.append({
            "PieceRef": row["ID"],
            "JournalCode": "ENC",
            "EcritureDate": row["DATE VIREMENT"],
            "CompteNum": "53000000",
            "PieceDate": row["DATE VIREMENT"],
            "EcritureLib": "UBER EATS TICKETS RESTO", 
            "Debit": abs(row["TICKETS RESTO"]), 
            "Credit": 0.0,
            })
            
            # Ligne des erreurs de commande
            entries.append({
            "PieceRef": row["ID"],
            "JournalCode": "ENC",
            "EcritureDate": row["DATE VIREMENT"],
            "CompteNum": "70960000",
            "PieceDate": row["DATE VIREMENT"],
            "EcritureLib": "UBER EATS ERREURS COMMANDE", 
            "Debit": abs(row["ERREURS COMMANDE"]), 
            "Credit": 0.0,
            })
            
            # Ligne des frais Uber HT
            entries.append({
            "PieceRef": row["ID"],
            "JournalCode": "ENC",
            "EcritureDate": row["DATE VIREMENT"],
            "CompteNum": "62220000",
            "PieceDate": row["DATE VIREMENT"],
            "EcritureLib": "UBER EATS COMMISSIONS", 
            "Debit": abs(row["FRAIS UBER HT"]), 
            "Credit": 0.0,
            })
            
            # Ligne des frais Uber TVA
            entries.append({
            "PieceRef": row["ID"],
            "JournalCode": "ENC",
            "EcritureDate": row["DATE VIREMENT"],
            "CompteNum": "44566000",
            "PieceDate": row["DATE VIREMENT"],
            "EcritureLib": "UBER EATS COMMISSIONS", 
            "Debit": abs(row["FRAIS UBER TVA"]), 
            "Credit": 0.0,
            })
            
            # Ligne du total
            if row["TOTAL"] > 0:
                entries.append({
                "PieceRef": row["ID"],
                "JournalCode": "ENC",
                "EcritureDate": row["DATE VIREMENT"],
                "CompteNum": "58000000",
                "PieceDate": row["DATE VIREMENT"],
                "EcritureLib": "UBER EATS NET", 
                "Debit": abs(row["TOTAL"]), 
                "Credit": 0.0,
                })
            elif row["TOTAL"] < 0:
                entries.append({
                "PieceRef": row["ID"],
                "JournalCode": "ENC",
                "EcritureDate": row["DATE VIREMENT"],
                "CompteNum": "58000000",
                "PieceDate": row["DATE VIREMENT"],
                "EcritureLib": "UBER EATS NET", 
                "Debit": 0.0, 
                "Credit": abs(row["TOTAL"]),
                })
            
            # Ligne des écarts
            if row["ECART"] > 0:
                entries.append({
                "PieceRef": row["ID"],
                "JournalCode": "ENC",
                "EcritureDate": row["DATE VIREMENT"],
                "CompteNum": "62220000",
                "PieceDate": row["DATE VIREMENT"],
                "EcritureLib": "UBER EATS ECART", 
                "Debit": abs(row["ECART"]), 
                "Credit": 0.0,
                })
            elif row["ECART"] < 0:
                entries.append({
                "PieceRef": row["ID"],
                "JournalCode": "ENC",
                "EcritureDate": row["DATE VIREMENT"],
                "CompteNum": "62220000",
                "PieceDate": row["DATE VIREMENT"],
                "EcritureLib": "UBER EATS ECART", 
                "Debit": 0.0, 
                "Credit": abs(row["ECART"]),
                })
        
        # Création du dataframe des écritures hebdomadaires
        df = pl.DataFrame(entries)
        df = df.filter((pl.col("Credit") != 0) | (pl.col("Debit") != 0))
        df = df.group_by(["PieceRef", "CompteNum", "EcritureLib"]).agg([
            pl.col("JournalCode").first().alias("JournalCode"),
            pl.col("EcritureDate").first().alias("EcritureDate"),
            pl.col("PieceDate").first().alias("PieceDate"),
            pl.col("Debit").sum().alias("Debit"),
            pl.col("Credit").sum().alias("Credit"),
            ]).sort(["EcritureDate", "EcritureLib"])
        
        # Recalcule des débits et crédits
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
        df = df.with_columns(pl.col(pl.Float64).round(2))

        return df