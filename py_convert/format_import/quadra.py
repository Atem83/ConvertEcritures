import polars as pl

from py_convert.format_import import ImportBase
from py_convert.error import run_error

class ImportQuadra(ImportBase):
    """Gestion d'import au format ASCII de Quadra."""
    
    def name(self):
        return "QUADRA (ASCII)"
    
    def validate_format(self):
        return True
    
    def process_file(self):
        liste_ecritures = []
        liste_libelles = []

        with open(self.path, "r") as file:
            lignes = file.readlines()

            # Test si le fichier est bien un fichier format ASCII Quadra
            if lignes[0].startswith(('M', 'C')):
                pass
            elif lignes[-2].startswith(('M', 'C')):
                pass
            else:
                run_error("Fichier incorrect ou endommagé")
                return None

            # Récupération des lignes d'écritures
            for ligne in lignes[0:]:
                # Enregistre les libellés de compte
                if ligne.startswith('C'):
                    libelle = {
                        "CompteNum": ligne[1:9].rstrip().rstrip('_'),
                        "CompteLib": ligne[9:39].rstrip()
                    }

                    # Création de la liste des libellés de compte
                    liste_libelles.append(libelle)

                # Enregistre les écritures comptables
                if ligne.startswith('M'):
                    # Récupère les montants débits et crédits
                    if ligne[41] == 'D':
                        if ligne[42] == '+':
                            debit = int(ligne[43:55]) / 100
                            credit = 0.00
                        elif ligne[42] == '-':
                            debit = -int(ligne[43:55]) / 100
                            credit = 0.00
                    elif ligne[41] == 'C':
                        if ligne[42] == '+':
                            credit = int(ligne[43:55]) / 100
                            debit = 0.00
                        elif ligne[42] == '-':
                            credit = -int(ligne[43:55]) / 100
                            debit = 0.00
                    else:
                        debit = 0.00
                        credit = 0.00

                    # Récupération du numéro de compte
                    compte = ligne[1:9].rstrip().rstrip('_')
                    if compte.startswith(("401", "08")):
                        compteGen = "40100000"
                        compteAux = compte
                    elif compte.startswith(("411", "01")):
                        compteGen = "41100000"
                        compteAux = compte
                    else:
                        compteGen = compte
                        compteAux = None

                    # Récupération du numéro de pièce
                    numPiece = ligne[99:107].rstrip()
                    if numPiece == "":
                        numPiece = None

                    ecriture = {
                        "JournalCode": ligne[9:11],
                        "EcritureDate": ligne[14:20],
                        "CompteNum": compteGen,
                        "CompAuxNum": compteAux,
                        "PieceRef": numPiece,
                        "PieceDate": ligne[14:20],
                        "EcritureLib": ligne[116:148].rstrip(),
                        "Debit": debit,
                        "Credit": credit,
                    }

                    # Création de la liste d'écritures
                    liste_ecritures.append(ecriture)

        columns = self.get_columns
        columns["EcritureDate"] = pl.String
        columns["PieceDate"] = pl.String

        # Création du dataframe à partir de la liste
        df = pl.DataFrame(liste_ecritures, schema=columns, orient="row")

        # Rajout du libellé de chaque compte
        for libelle in liste_libelles:
            # Rajout des libellés des comptes généraux
            df = df.with_columns(
                pl.when(pl.col("CompteNum") == libelle["CompteNum"])
                  .then(pl.lit(libelle["CompteLib"]))
                  .otherwise(pl.col("CompteLib"))
                  .alias("CompteLib")
            )

            # Rajout des libellés des comptes auxiliaires
            df = df.with_columns(
                pl.when(pl.col("CompAuxNum") == libelle["CompteNum"])
                  .then(pl.lit(libelle["CompteLib"]))
                  .otherwise(pl.col("CompAuxLib"))
                  .alias("CompAuxLib")
            )

        # Je transforme le type des colonnes de date
        for col in ["EcritureDate", "PieceDate"]:
            df = df.with_columns(pl.col(col).str.to_date("%d%m%y"))

        return df