import polars as pl
from .base_import import BaseImport
from ..error import run_error

class Sage20Import(BaseImport):
    """Gestion d'import de frulog au format Sage"""
    
    def name(self):
        return "SAGE 20"
    
    def validate_format(self):
        if self.path.suffix.lower() != ".txt":
            run_error(f"Le format {self.name()} nécessite un fichier .txt")
            return False
        return True
    
    def process_file(self):
        liste_ecritures = []

        with open(self.path, "r") as file:
            lignes = file.readlines()

            # Test si le fichier est bien un fichier format Sage
            if lignes[0].startswith('#FLG'):
                pass
            elif lignes[-1].startswith('#FIN'):
                pass
            else:
                run_error("Fichier incorrect ou endommagé")
                return None

            # Récupère les écritures contenues dans le fichier
            pos_ligne = 0
            pos_approved = [1, 2, 4, 7, 9, 11, 13]
            ecriture = []
            sens = None

            for ligne in lignes[2:]:
                pos_ligne += 1
                valeur = ligne.strip()

                # On redémarre une nouvelle écriture
                if valeur == "#MECG":
                    pos_ligne = 0

                # Ajoute à la liste les valeurs des lignes approuvées
                if pos_ligne in pos_approved:
                    ecriture.append(valeur)

                # Ajoute le sens de l'écriture
                if pos_ligne == 17:
                    if valeur == "0":
                        sens = "Debit"
                    elif valeur == "1":
                        sens = "Credit"
                    else:
                        run_error("Le sens D/C d'une écriture n'a pas pu être déterminé")
                        return None

                # Ajoute le montant débit et crédit
                if pos_ligne == 18:
                    if sens == "Debit":
                        ecriture.append(valeur)
                        ecriture.append("0.00")
                    elif sens == "Credit":
                        ecriture.append("0.00")
                        ecriture.append(valeur)

                # Réorganise les données et ajoute mon écriture à la liste
                if pos_ligne == 36:
                    # Rajoute la date de pièce
                    ecriture.append(ecriture[1])

                    # Permet d'éviter les valeurs "" pour le dataframe
                    for i, valeur in enumerate(ecriture):
                        if valeur == "":
                            ecriture[i] = None

                    # Fait correspondre ma liste aux colonnes du dataframe
                    for _ in range(19 - len(ecriture)):
                        ecriture.append(None)

                    liste_ecritures.append(ecriture)
                    ecriture = []

        entetes = {
            "JournalCode": pl.Utf8,
            "EcritureDate": pl.Utf8,
            "PieceRef": pl.Utf8,
            "CompteNum": pl.Utf8,
            "CompAuxNum": pl.Utf8,
            "EcritureLib": pl.Utf8,
            "EcheanceDate": pl.Utf8,
            "Debit": pl.Float64,
            "Credit": pl.Float64,
            "PieceDate": pl.Utf8,
            "JournalLib": pl.Utf8,
            "EcritureNum": pl.Utf8,
            "CompteLib": pl.Utf8,
            "CompAuxLib": pl.Utf8,
            "EcritureLet": pl.Utf8,
            "DateLet": pl.Utf8,
            "ValidDate": pl.Utf8,
            "Montantdevise": pl.Float64,
            "Idevise": pl.Utf8,
        }
        df = pl.DataFrame(liste_ecritures, schema=entetes, orient="row")
        
        # Je réorganise l'ordre des colonnes de mon dataframe
        df = df.select("JournalCode",
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

        # Je transforme le type des colonnes de date et de montant
        df = df.with_columns(pl.col("EcritureDate", 
                                "PieceDate",
                                "DateLet",
                                "ValidDate",
                                "EcheanceDate")
                             .str.strptime(pl.Date, "%d%m%y"))

        return df
