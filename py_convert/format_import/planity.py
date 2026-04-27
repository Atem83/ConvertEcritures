import polars as pl

from py_convert.error import run_error
from py_convert.format_import import ImportBase

class ImportPlanity(ImportBase):
    """Gestion d'import d'un fichier Excel au format rapport de Planity."""
    
    def name(self):
        return "PLANITY"
    
    def validate_format(self):
        if self.path.suffix.lower() == ".xls":
            return True
        elif self.path.suffix.lower() == ".xlsx":
            return True
        else:
            run_error(f"Le format {self.name()} nécessite un fichier .xls ou .xlsx")
            return False
    
    def process_file(self):
        cols_name = {
            "Jour": pl.String,
            "CA prestations TTC": pl.Float64,
            "CA produits TTC": pl.Float64,
            "CA Divers TTC": pl.Float64,
            "TVA 20,00%": pl.Float64,
            "Total TTC": pl.Float64,
            "Règlements Espèces": pl.Float64,
            "Règlements CB": pl.Float64,
            "Règlements Chèque": pl.Float64,
        }
        try:
            df = pl.read_excel(
                self.path, 
                sheet_name="Synthèse",
                has_header=True, 
                columns=list(cols_name.keys()),
                schema_overrides=cols_name,
            )
        except Exception as e:
            df = pl.read_excel(
                self.path, 
                sheet_id=1,
                has_header=True, 
                columns=list(cols_name.keys()),
                schema_overrides=cols_name,
            )
        
        # Filtrage des lignes inutiles
        df = df.filter(pl.col("Jour") != "Total")
        
        # Transformation des dates
        df = df.with_columns(
            pl.col("Jour").str.to_date("%d/%m/%Y").alias("Date")
        ).drop("Jour")
        
        # Création des écritures
        entries = []
        for row in df.iter_rows(named=True):
            total_HT = 0.0
            
            # Ligne du compte de prestations TTC
            CA_HT = round(row["CA prestations TTC"] / 1.2, 2)
            total_HT += CA_HT
            entries.append({
                "JournalCode": "CS",
                "EcritureDate": row["Date"],
                "CompteNum": "70600000",
                "PieceDate": row["Date"],
                "EcritureLib": "PRESTATIONS DE SERVICE", 
                "Debit": 0.0, 
                "Credit": CA_HT,
            })
            
            # Ligne du compte de produits TTC
            CA_HT = round(row["CA produits TTC"] / 1.2, 2)
            total_HT += CA_HT
            entries.append({
                "JournalCode": "CS",
                "EcritureDate": row["Date"],
                "CompteNum": "70610000",
                "PieceDate": row["Date"],
                "EcritureLib": "VENTES DE PRODUITS", 
                "Debit": 0.0, 
                "Credit": CA_HT,
            })
            
            # Ligne du compte de divers TTC
            CA_HT = round(row["CA Divers TTC"] / 1.2, 2)
            total_HT += CA_HT
            entries.append({
                "JournalCode": "CS",
                "EcritureDate": row["Date"],
                "CompteNum": "70620000",
                "PieceDate": row["Date"],
                "EcritureLib": "VENTES DIVERSES", 
                "Debit": 0.0, 
                "Credit": CA_HT,
            })
            
            # Ligne du compte de TVA
            entries.append({
                "JournalCode": "CS",
                "EcritureDate": row["Date"],
                "CompteNum": "44571000",
                "PieceDate": row["Date"],
                "EcritureLib": "TVA 20,00%", 
                "Debit": 0.0, 
                "Credit": row["Total TTC"] - total_HT,
            })
            
            # Ligne du règlement espèces
            entries.append({
                "JournalCode": "CS",
                "EcritureDate": row["Date"],
                "CompteNum": "58100000",
                "PieceDate": row["Date"],
                "EcritureLib": "Règlements Espèces", 
                "Debit": row["Règlements Espèces"], 
                "Credit": 0.0,
            })
            
            # Ligne du règlement chèque
            entries.append({
                "JournalCode": "CS",
                "EcritureDate": row["Date"],
                "CompteNum": "58200000",
                "PieceDate": row["Date"],
                "EcritureLib": "Règlements Chèque", 
                "Debit": row["Règlements Chèque"], 
                "Credit": 0.0,
            })
            
            # Ligne du règlement CB
            entries.append({
                "JournalCode": "CS",
                "EcritureDate": row["Date"],
                "CompteNum": "58300000",
                "PieceDate": row["Date"],
                "EcritureLib": "Règlements CB", 
                "Debit": row["Règlements CB"], 
                "Credit": 0.0,
            })
            
        df = pl.DataFrame(entries)
        
        # Suppression des lignes avec débit et crédit à 0
        df = df.filter((pl.col("Debit") != 0) | (pl.col("Credit") != 0))

        return df