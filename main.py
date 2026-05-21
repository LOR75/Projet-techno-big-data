import os
import sys


import sparknlp
import sparknlp
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, size, col
from pyspark.sql.types import IntegerType, DoubleType

from create_corpus import create_my_corpus
from create_pipeline import build_pipeline, build_df
import stylometrie
from sliding_readability import compute_sliding_readability, sliding_stability_stats
from zipf import compute_zipf_spark, plot_zipf
from segment import add_segmentation, segmentation_stats
from visualize import (
    plot_flesch, plot_km, plot_complexity, plot_complexityBis,
    plot_sliding_readability, plot_segmentation,
)
from save import save_results


#### session spark ########


os.environ["PYSPARK_PYTHON"] = sys.executable         #Python utilisé par les workers
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable  #Python utilisé par le main

spark = sparknlp.start()

# CORRECTION 1 : shuffle.partitions abaissé de 200 à 8 pour un usage local mono-nœud.
# 200 partitions vides génèrent une surcharge mémoire et CPU sans bénéfice.
spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.default.parallelism",    "8")

print("Spark version :", spark.version)
print("SparkNLP version :", sparknlp.version())


# corpus
#charge localement si possible ou télécharge sur internet sinon


romanticCorpus, realismCorpus = create_my_corpus(max_books_per_author=10) #paramètre pour limiter la durée d'exécution


#DataFrames Spark
df_romantic = build_df(romanticCorpus, "romantic", spark)
df_realism  = build_df(realismCorpus,  "realism",  spark)

# Union des dataFrames
# .cache() : marque le DataFrame pour mise en mémoire après la première action
df = df_romantic.union(df_realism).cache()


print(f"Documents chargés : {df.count()}")



#####" pipeline #########

# model.transform(df) : applique la pipeline sur chaque ligne.
# colonnes : document, sentence, token, pos


pipeline     = build_pipeline(romanticCorpus, realismCorpus)
model        = pipeline.fit(df)
annotated_df = model.transform(df).cache()  # mise en cache après la première action


#### stylométrie ########

#   word_count     : nb d'éléments dans le tableau token
#
#   sentence_count : nb de phrases détectées
#
#   syllable_count : nb de syllabes
#
#   flesch_score   : formule Flesch
#
#   km_score       : formule Kandel-Moles.


from pyspark.sql.functions import col, size, expr, avg

_ = annotated_df.count()   # force la matérialisation du cache NLP


# df n'est plus utilisé jusqu'à save_results ; on le dépersiste pour récupérer la mémoire.
df.unpersist()


# Conserver document/sentence/token/pos (colonnes NLP lourdes) en mémoire toute la session
# est la principale cause de crash. On les utilise ici pour les calculs puis on les drop.
annotated_df = annotated_df \
    .withColumn("word_count", size(col("token.result"))) \
    .withColumn("sentence_count", size(col("sentence.result"))) \
    .withColumn(
        "syllable_count",
        # Pour chaque token, on compte les syllabes via une regex qui détecte
        # les groupes voyelle consécutifs 
        expr("""
            aggregate(
                token.result,
                0,
                (acc, word) -> acc + greatest(
                    size(split(regexp_replace(lower(word), '[aeiouyàâéèêëîïôùûü]+', '-'), '-')) - 1,
                    1
                )
            )
        """)
    ) \
    .withColumn(
        "flesch_score",
        # CASE WHEN évite ZeroDivisionError sur les textes vides ou non tokenisés
        expr("""
            CASE
              WHEN word_count = 0 OR sentence_count = 0 THEN 0.0
              ELSE 206.835 - 1.015 * (word_count / sentence_count)
                           - 84.6  * (syllable_count / word_count)
            END
        """)
    ) \
    .withColumn(
        "km_score",
        expr("""
            CASE
              WHEN word_count = 0 OR sentence_count = 0 THEN 0.0
              ELSE 207.0 - 1.015 * (word_count / sentence_count)
                        - 73.6  * (syllable_count / word_count)
            END
        """)
    ) \
    .cache()

_ = annotated_df.count()   # force la matérialisation des nouveaux calculs

# Agrégation par auteur × genre : moyennes des scores
author_stats = annotated_df.groupBy("author", "genre").agg(
    avg("flesch_score").alias("avg_flesch"),
    avg("km_score").alias("avg_km"),
    avg("word_count").alias("avg_words"),
    avg("sentence_count").alias("avg_sentences"),
)

# toPandas() : ramène la petite table agrégée sur le driver pour Plotly
pdf_stats = author_stats.toPandas()
print(pdf_stats)

# Pour les graphiques

books_pdf = annotated_df.select(
    "id",
    "title",
    "author",
    "genre",
    "word_count",
    "sentence_count",
    "flesch_score",
    "km_score"
).toPandas()



##### lisibilité glissante ########

# compute_sliding_readability() retourne un DataFrame "long" :
#   1 ligne par (document × fenêtre × configuration)
#
# window_configs : liste de (window_size, step)
#   (n, 250)  : fenêtres de n tokens avec 50% de chevauchement
#
# sliding_stability_stats() calcule le coefficient de variation
# de Flesch et KM par auteur et taille de fenêtre.

sliding_df  = compute_sliding_readability(
    annotated_df,
    window_configs=[(500, 250), (1000, 500)]
)

# mise en cache de sliding_df car il est parcouru deux fois
# (sliding_stability_stats + filter/toPandas). Sans cache, Spark recalcule
# l'intégralité du posexplode × fenêtres deux fois.
sliding_df = sliding_df.cache()
_ = sliding_df.count()

stability_pdf = sliding_stability_stats(sliding_df)

sliding_pdf = (
    sliding_df
    .filter(F.col("window_size") == 500)
    .toPandas()
)

# on libère sliding_df après usage pour libérer la mémoire
# avant zipf et segmentation.
sliding_df.unpersist()

print("Stabilité des indices :")
print(stability_pdf)


##### zipf ########
# compute_zipf_spark() calcule le rang et la fréquence de chaque mot
# par document, puis retourne un DataFrame Pandas avec les top 200 mots.
#calcul distribué par Spark (explode + groupBy + Window function).

zipf_pdf = compute_zipf_spark(annotated_df, top_n=200)

zipf_pdf.to_csv("data/zipf.csv", index=False)

# dialogue / narration
# add_segmentation() applique une UDF Python qui classifie chaque ligne
# du texte brut comme dialogue ou narration


annotated_df = add_segmentation(annotated_df)
seg_pdf = segmentation_stats(annotated_df)

seg_pdf.to_csv("data/segmentation.csv", index=False)


#### graphiques ########

os.makedirs("output", exist_ok=True)

plot_flesch(pdf_stats)                      # Barplot Flesch par auteur
plot_km(pdf_stats)                          # Barplot Kandel-Moles par auteur
plot_complexity(pdf_stats)                  # Nuage complexité agrégée par auteur
plot_complexityBis(books_pdf)               # Nuage complexité par livre
plot_sliding_readability(sliding_pdf)       # Courbes glissantes
plot_zipf(zipf_pdf)                         # zipf par auteur
plot_segmentation(seg_pdf)                  # Barplot ratio dialogue


### save ####

# on recharge df depuis Parquet plutôt que de le garder en cache.
# Au moment de save_results, df a été dépersisté; on le recrée
# à la volée depuis les corpus déjà en mémoire Python, ce qui est négligeable.
df = df_romantic.union(df_realism)

save_results(df, annotated_df, author_stats, sliding_df)

spark.stop()
