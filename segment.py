"""
La segmentation est implémentée comme une UDF Spark.
La UDF reçoit le texte brut et retourne une structure contenant des infos sur le format
"""

import re
from pyspark.sql.functions import udf, explode, col, avg, count
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField, IntegerType, DoubleType
)


# Pattern de début de ligne dialogue :
#   ^\s*      : ignore les espaces en début de ligne
#   [«""„—–\-] : premier caractère non-espace = guillemet ou tiret
#   \s*       : espace optionnel après le marqueur
DIALOG_PATTERN = re.compile(
    r'^\s*[«""„—–\-]\s*'
)

# Pattern de fragment entre guillemets dans une ligne de narration :
#   [«"]      : guillemet ouvrant (français ou anglais)
#   [^»"]*    : contenu quelconque (non-gourmand)
#   [»"]      : guillemet fermant correspondant
# Capture ex. : Elle dit « bonjour » et s'en alla.
DIALOG_INNER = re.compile(
    r'[«"][^»"]*[»"]'
)




def classify_line(line: str) -> str:
    """
    Classifie une seule ligne comme 'dialog' ou 'narration'
    """
    line = line.strip()
    if not line:
        return "narration"          # ligne vide = narration par défaut
    if DIALOG_PATTERN.match(line):  # début de ligne avec guillemet/tiret
        return "dialog"
    if DIALOG_INNER.search(line):   # fragment entre guillemets dans la ligne
        return "dialog"
    return "narration"


def segment_text(text: str) -> dict:
    """
    Segmente un texte complet en blocs dialogue / narration.
    """
    lines = text.splitlines()   # découpage en lignes (conserve les lignes vides)
    dialog_lines = []
    narration_lines = []

    for line in lines:
        if classify_line(line) == "dialog":
            dialog_lines.append(line)
        else:
            narration_lines.append(line)

    # max(..., 1) évite la division par zéro sur un texte vide
    total = max(len(lines), 1)

    return {
        "dialog_lines"    : len(dialog_lines),
        "narration_lines" : len(narration_lines),
        "dialog_ratio"    : len(dialog_lines) / total,   # fraction dans [0, 1]
        "dialog_text"     : "\n".join(dialog_lines),
        "narration_text"  : "\n".join(narration_lines),
    }



#schéma Spark pour la UDF


SEGMENT_SCHEMA = StructType([
    StructField("dialog_lines",    IntegerType(), True),  # nb lignes dialogue
    StructField("narration_lines", IntegerType(), True),  # nb lignes narration
    StructField("dialog_ratio",    DoubleType(),  True),  # ratio [0.0, 1.0]
    StructField("dialog_text",     StringType(),  True),  # texte dialogue concaténé
    StructField("narration_text",  StringType(),  True),  # texte narration concaténé
])


#udf spark

@udf(SEGMENT_SCHEMA)
def segment_udf(text: str):

    if text is None:
        # Valeur neutre pour les lignes sans texte
        return (0, 0, 0.0, "", "")
    result = segment_text(text)
    # Retour en tuple ordonné selon SEGMENT_SCHEMA
    return (
        result["dialog_lines"],
        result["narration_lines"],
        result["dialog_ratio"],
        result["dialog_text"],
        result["narration_text"],
    )




def add_segmentation(df):
    """
    Ajoute au DataFrame Spark une colonne "seg" contenant les métriques
    de segmentation dialogue / narration.
    """

    df = df.withColumn("seg", segment_udf(col("text")))
    return df


def segmentation_stats(df):
    """
    Calcule les statistiques de segmentation agrégées par auteur et genre.
    """
    
    stats = df.groupBy("author", "genre").agg(
        avg("seg.dialog_ratio").alias("avg_dialog_ratio"),
        avg("seg.dialog_lines").alias("avg_dialog_lines"),
        avg("seg.narration_lines").alias("avg_narration_lines"),
        count("*").alias("nb_texts"),
    )

    pdf = stats.toPandas()
    print(pdf)
    return pdf
