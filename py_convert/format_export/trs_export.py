import polars as pl
from .base_export import BaseExport
from ..error import run_error

class TRSExport(BaseExport):
    """Gestion d'export au format TRS."""
    def name(self):
        return "TRS"
    
    def extension(self):
        return ".TRS"
    
    @property
    def mandatory_cols(self) -> list[str]:
        return [
            "JournalCode", 
            "JournalLib",
            "EcritureDate",
            "CompteNum",
            "EcritureLib",
            "Debit",
            "Credit"
            ]
    
    @property
    def mandatory_data(self) -> list[str]:
        return [
            "JournalCode",
            "EcritureDate", 
            "CompteNum", 
            "Debit", 
            "Credit"
        ]
    
    def process_file(self):
        # Remplace le compte général par le compte auxiliaire
        df = self.swapGenToAux(self.entries)
        
        # Conserve uniquement les colonnes obligatoires
        df = df.select(self.mandatory_cols)

        # Reformate certaines colonnes
        df = df.with_columns(pl.col("EcritureDate").dt.strftime("%m%y").alias("periode"))
        df = df.with_columns(pl.col("EcritureDate").dt.strftime("%Y%m%d"))

        list_entries = df.to_dicts()
        limit_global = 1023
        limit_JournalCode = 2
        limit_JournalLib = 9
        limit_CompteNum = 9
        limit_sens = 1
        limit_amount = 14
        limit_EcritureLib = 49
        limit_EcritureDate = 8

        # Ouvrir le fichier en mode écriture
        with open(self.path_export, "w", encoding="utf-8") as file:        
            # Enregistrement des écritures dans le fichier
            for entry in list_entries:
                export = ""
                space = " "

                # Écrire le code journal
                value = entry["JournalCode"][:limit_JournalCode]
                export += value + space

                # Écrire le libellé journal
                if entry["JournalLib"] == None:
                    value = ""
                else:
                    value = entry["JournalLib"]
                value = value[:limit_JournalLib - 4]
                value += entry["periode"]
                value += space * (limit_JournalLib - len(value))
                export += value + space

                # Écrire le numéro de compte
                value = entry["CompteNum"][:limit_CompteNum]
                value += space * (limit_CompteNum - len(value))
                export += value + space

                # Écrire le sens du montant
                if entry["Debit"] > 0:
                    value = "D"
                elif entry["Credit"] > 0:
                    value = "C"
                else:
                    value = ""
                    run_error("Un montant débit ou crédit est manquant")
                value += space * (limit_sens - len(value))
                export += value + space    

                # Écrire le montant
                if entry["Debit"] > 0:
                    value = round(entry["Debit"], 2)
                    value = str(value)
                    value = value.replace(".", "")
                elif entry["Credit"] > 0:
                    value = round(entry["Credit"], 2)
                    value = str(value)
                    value = value.replace(".", "")
                else:
                    value = 0.0
                    run_error("Un montant débit ou crédit est manquant")
                value = f"{value: >14}"
                value += space * (limit_amount - len(value))
                export += value + space

                # Écrire le libellé de l'écriture
                value = entry["EcritureLib"][:limit_EcritureLib]
                value += space * (limit_EcritureLib - len(value))
                export += value + space

                # Écrire la date de l'écriture
                value = entry["EcritureDate"]
                value += space * (limit_EcritureDate - len(value))
                export += value
                export += space * (limit_global - len(export)) + "\n"

                # Écrire la ligne d'écriture dans le fichier
                file.write(export)