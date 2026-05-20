import os


PARQUET_RAW = "data/parquet/raw_corpus"                 #stockage textes bruts
PARQUET_ANNOTATED = "data/parquet/annotated_corpus"     # textes annotés + scores
PARQUET_SLIDING = "data/parquet/sliding_readability"    # lisibilité glissante
ORC_STYLOMETRY = "data/orc/stylometry"                  # statistiques


def save_results(df_raw, annotated_df, author_stats, sliding_df):
   
    #création des dossiers de sauvegarde s'ils n'existent pas déjà
    for path in [PARQUET_RAW, PARQUET_ANNOTATED, PARQUET_SLIDING, ORC_STYLOMETRY]:
        os.makedirs(os.path.dirname(path), exist_ok=True)

    #stockage des textes bruts
    df_raw.write.mode("overwrite").parquet(PARQUET_RAW)
    print(f"Saved: {PARQUET_RAW}")

    cols_to_drop = []

    #stockage des textes annotés
    for c in ["seg", "document", "sentence", "token", "pos"]:
        if c in annotated_df.columns:
            cols_to_drop.append(c)


    light_df = annotated_df.drop(*cols_to_drop)  # DataFrame allégé

    light_df.write \
        .mode("overwrite") \
        .partitionBy("genre") \
        .parquet(PARQUET_ANNOTATED)
    
    print(f"Saved: {PARQUET_ANNOTATED}")

    #stockage des slidability stats
    sliding_df.write \
        .mode("overwrite") \
        .partitionBy("window_size") \
        .parquet(PARQUET_SLIDING)
    print(f"Saved: {PARQUET_SLIDING}")

    #stockage des stats
    author_stats.write.mode("overwrite").orc(ORC_STYLOMETRY)
    print(f"Saved: {ORC_STYLOMETRY}")
