"""
sliding_readability.py

indice de lisibilité sur différentes fenêtres glissantes du texte

"""

from pyspark.sql import functions as F
from pyspark.sql.functions import stddev
from pyspark.sql.functions import mean as spark_mean


def compute_sliding_readability(annotated_df, window_configs=None):
    """
    Calcule la lisibilité glissante
    """
    if window_configs is None:
        window_configs = [(500, 250), (1000, 500)]

    #posexplode => 1 ligne par (document, position_token)
    # pos est l'index absolu du token dans le document
    tokens_df = (
        annotated_df
        .select(
            "id", "author", "genre", "title",
            F.posexplode(F.col("token.result")).alias("pos", "word")
        )
        # Comptage des syllabes par token
        .withColumn(
            "syllables",
            F.greatest(
                F.size(
                    F.split(
                        F.regexp_replace(
                            F.lower(F.col("word")),
                            r"[aeiouyàâéèêëîïôùûü]+",
                            "-"
                        ),
                        "-"
                    )
                ) - 1,
                F.lit(1)
            )
        )
    ).cache()  # réutilisé pour chaque config → mise en cache

    # CORRECTION 7 : matérialisation explicite de tokens_df avant la boucle.
    # Sans ce count(), le cache n'est effectif qu'à la première action dans la boucle,
    # ce qui peut provoquer un recalcul complet du posexplode à chaque itération
    # si Spark invalide le plan entre les deux configs.
    _ = tokens_df.count()

    dfs = []
    for ws, st in window_configs:

        windowed = (
            tokens_df

            .withColumn("window_index", (F.col("pos") / F.lit(st)).cast("int"))

            .filter(F.col("pos") < (F.col("window_index") * F.lit(st) + F.lit(ws)))
            # Agrégation par (document, fenêtre)
            .groupBy("id", "author", "genre", "title", "window_index")
            .agg(
                F.count("word").alias("word_count"),
                F.sum("syllables").alias("syllable_count"),
                # Estimation du nombre de phrases : 1 phrase ≈ 20 mots
                (F.count("word") / F.lit(20.0)).alias("sentence_count"),
            )
            # Exclure les fenêtres trop courtes (bord de texte)
            .filter(F.col("word_count") >= F.lit(ws) * 0.5)
            # Calcul des indices de lisibilité en SQL pur
            .withColumn("flesch", F.expr("""
                CASE WHEN word_count = 0 OR sentence_count = 0 THEN 0.0
                ELSE 206.835
                     - 1.015 * (word_count / sentence_count)
                     - 84.6  * (syllable_count / word_count)
                END
            """))
            .withColumn("km", F.expr("""
                CASE WHEN word_count = 0 OR sentence_count = 0 THEN 0.0
                ELSE 207.0
                     - 1.015 * (word_count / sentence_count)
                     - 73.6  * (syllable_count / word_count)
                END
            """))
            .withColumn("window_size", F.lit(ws))
            .withColumn("step", F.lit(st))
            # Tri explicite pour garantir l'ordre croissant du window_index
            # (important pour les graphiques de courbes)
            .orderBy("id", "window_index")
        )
        dfs.append(windowed)

    result = dfs[0]
    for d in dfs[1:]:
        result = result.union(d)

    # CORRECTION 8 : libération de tokens_df après la construction du plan logique.
    # tokens_df n'est plus nécessaire une fois les DataFrames windowed créés.
    tokens_df.unpersist()

    return result


def sliding_stability_stats(sliding_df):
    """
    Calcule le coefficient de variation de Flesch et KM par
    auteur, genre et taille de fenêtre.

    """
    stats = (
        sliding_df
        .groupBy("author", "genre", "window_size")
        .agg(
            spark_mean("flesch").alias("mean_flesch"),
            stddev("flesch").alias("std_flesch"),
            spark_mean("km").alias("mean_km"),
            stddev("km").alias("std_km"),
        )
    )

    pdf = stats.toPandas()
    pdf["cv_flesch"] = pdf["std_flesch"] / pdf["mean_flesch"].abs().clip(lower=1e-9)
    pdf["cv_km"] = pdf["std_km"] / pdf["mean_km"].abs().clip(lower=1e-9)

    return pdf
