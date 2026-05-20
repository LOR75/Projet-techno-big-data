import re
import os
from collections import Counter

from pyspark.sql.functions import (
    col, explode, lower, regexp_replace,
    count, desc, rank, log, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import log10



# On retire les mots grammaticaux très fréquents mais qui ne révèlent rien sur le style

STOPWORDS = {
    "le", "la", "les", "de", "du", "des", "un", "une",
    "et", "en", "à", "au", "aux", "que", "qui", "dans",
    "il", "elle", "ils", "elles", "on", "nous", "vous",
    "je", "tu", "se", "sa", "son", "ses", "ce", "est",
    "pas", "ne", "plus", "par", "sur", "mais", "ou",
    "donc", "or", "ni", "car", "avec", "pour", "que", "me",
    "ma", "mon", "mes", "te", "ton", "ta", "tes", "leur", "leurs"
}



def compute_zipf_spark(annotated_df, top_n=200):
    """
    Calcule la distribution Zipf (rang / fréquence) pour chaque document.

    """


    # Étape 1 : explode tokens => 1 ligne par token

    words_df = annotated_df.select(
        "id", "author", "genre", "title",
        explode(col("token.result")).alias("word_raw")  # 1 ligne par token brut
    )

    # Étape 2 : nettoyage et filtrage
    # regexp_replace supprime tout ce qui n'est pas une lettre

    words_df = words_df.withColumn(
        "word",
        lower(regexp_replace(col("word_raw"), r"[^a-zA-ZÀ-ÿ]", ""))
    ).filter(
        (col("word") != "") &                        # retire les tokens vides
        (~col("word").isin(list(STOPWORDS)))         # retire les stopwords
    )

    # Étape 3 : comptage par (document × mot)
    # groupBy sur toutes les colonnes d'identité + "word"

    freq_df = words_df.groupBy(
        "id", "author", "genre", "title", "word"
    ).agg(
        count("*").alias("frequency")   # fréquence absolue
    )

    # Étape 4 : rang par document (Window function)

    window_spec = Window.partitionBy("id").orderBy(desc("frequency"))
    freq_df = freq_df.withColumn("rank", rank().over(window_spec))

    # on ne garde que les n mots les plus fréquentspar document
    freq_df = freq_df.filter(col("rank") <= top_n)


    # ppassage au log pour les axes log-log de Zipf

    freq_df = freq_df \
        .withColumn("log_rank", log10(col("rank").cast("double"))) \
        .withColumn("log_freq", log10(col("frequency").cast("double")))


    pdf = freq_df.toPandas()
    return pdf


#graphe zipf

def plot_zipf(zipf_pdf, output_dir="output"):
    
    import plotly.express as px
    import plotly.graph_objects as go
    import pandas as pd

    os.makedirs(output_dir, exist_ok=True)


    genre_avg = (
        zipf_pdf
        .groupby(["genre", "rank"])[["log_freq"]]
        .mean()
        .reset_index()
    )

    fig_genre = px.line(
        genre_avg,
        x="rank",
        y="log_freq",
        color="genre",
        log_x=True,
        title="Loi de Zipf — distribution moyenne par genre littéraire",
        labels={
            "rank"    : "Rang (échelle log₁₀)",
            "log_freq": "Fréquence log₁₀",
            "genre"   : "Genre littéraire"
        },
    )
    fig_genre.update_layout(
        template="plotly_white",
        xaxis_title="Rang du mot (échelle logarithmique)",
        yaxis_title="log₁₀(fréquence)",
        legend_title="Genre",
    )
    path = os.path.join(output_dir, "zipf_genre.html")
    fig_genre.write_html(path, auto_open=False)
    print(f"Saved figure: {path}")


    return fig_genre