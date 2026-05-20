

import os
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

os.makedirs("output", exist_ok=True)




def _save(fig, path):
    fig.write_html(path, auto_open=False)
    print(f"Saved figure: {path}")
    return fig




def plot_flesch(pdf, output_dir="output"):
    """
    Barplot de l'indice de Flesch moyen par auteur, groupé par genre.

    """
    fig = px.bar(
        pdf,
        x="author",
        y="avg_flesch",
        color="genre",
        barmode="group", 
        title="Lisibilité moyenne par auteur — Indice de Flesch",
        labels={
            "author"    : "Auteur",
            "avg_flesch": "Score Flesch moyen",
            "genre"     : "Genre littéraire",
        },
        text_auto=".2f",
    )
    fig.update_layout(
        xaxis_title="Auteur",
        yaxis_title="Score de lisibilité (Flesch)\n[>70 facile | 30-70 moyen | <30 difficile]",
        template="plotly_white",
        legend_title="Genre",
    )
    return _save(fig, os.path.join(output_dir, "flesch.html"))


def plot_km(pdf, output_dir="output"):
    """
    Barplot de l'indice Kandel-Moles moyen par auteur, groupé par genre.

    """
    fig = px.bar(
        pdf,
        x="author",
        y="avg_km",
        color="genre",
        barmode="group",
        title="Lisibilité moyenne par auteur — Indice Kandel-Moles",
        labels={
            "author": "Auteur",
            "avg_km": "Score Kandel-Moles moyen",
            "genre" : "Genre littéraire",
        },
        text_auto=".2f",
    )
    fig.update_layout(
        xaxis_title="Auteur",
        yaxis_title="Indice Kandel-Moles moyen",
        template="plotly_white",
        legend_title="Genre",
    )
    return _save(fig, os.path.join(output_dir, "km.html"))


def plot_complexity(pdf, output_dir="output"):
    """
    Nuage de points : complexité moyenne par auteur.

    """
    fig = px.scatter(
        pdf,
        x="avg_sentences",
        y="avg_words",
        color="genre",
        size="avg_words",           # taille du rond proportionnelle au nb de mots
        hover_name="author",        # nom de l'auteur affiché au survol
        title="Complexité structurelle par auteur — Mots × Phrases (moyenne par texte)",
        labels={
            "avg_sentences": "Nb moyen de phrases par texte",
            "avg_words"    : "Nb moyen de mots par texte",
            "genre"        : "Genre",
        },
    )
    fig.update_layout(
        template="plotly_white",
        legend_title="Genre",
    )
    return _save(fig, os.path.join(output_dir, "complexity.html"))

def plot_complexityBis(pdf, output_dir="output"):
    """
    Nuage de points : complexité par livre.

    """

    fig = px.scatter(
        pdf,

        x="sentence_count",
        y="word_count",

        color="author",   # chaque auteur une couleur

        symbol="genre",

        hover_name="title",

        hover_data={
            "author": True,
            "genre": True,
            "word_count": True,
            "sentence_count": True,
            "flesch_score": ":.2f",
            "km_score": ":.2f",
        },

        title="Complexité structurelle par livre — Mots × Phrases",

        labels={
            "sentence_count": "Nombre de phrases",
            "word_count": "Nombre de mots",
            "author": "Auteur",
            "genre": "Genre",
        },
    )

    fig.update_traces(
        marker=dict(size=8, opacity=0.75)
    )

    fig.update_layout(
        template="plotly_white",
        legend_title="Auteur",
    )

    return _save(fig, os.path.join(output_dir, "complexityBis.html"))




def plot_sliding_readability(sliding_pdf, output_dir="output"):

    # On filtre sur window_size=500 pour n'afficher qu'une résolution
    sample = sliding_pdf[sliding_pdf["window_size"] == 500].copy()

    # On prend le premier id de chaque auteur
    first_ids = (
        sample
        .drop_duplicates(subset="author")[["author", "id"]]
        .rename(columns={"id": "first_id"})
    )
    sample = sample.merge(first_ids, on="author")
    sample = sample[sample["id"] == sample["first_id"]].copy()
    # Tri explicite pour garantir des courbes sans marches arrière
    sample = sample.sort_values(["author", "window_index"])

    fig_lines = px.line(
        sample,
        x="window_index",
        y="flesch",
        color="author",
        line_dash="genre",      # tirets pour un genre, ligne pleine pour l'autre
        title="Évolution de la lisibilité (Flesch) au fil du texte — Fenêtre 500 tokens",
        labels={
            "window_index": "Numéro de fenêtre (position dans le texte →)",
            "flesch"      : "Score Flesch",
            "author"      : "Auteur",
            "genre"       : "Genre",
        },
    )
    fig_lines.update_layout(
        template="plotly_white",
        legend_title="Auteur / Genre",
        yaxis_title="Score Flesch [>70=facile | 30-70=moyen | <30=difficile]",
    )
    _save(fig_lines, os.path.join(output_dir, "sliding_flesch_lines.html"))




def plot_segmentation(seg_pdf, output_dir="output"):
    """
    Barplot du ratio dialogue / total par auteur, groupé par genre.
    """
    fig = px.bar(
        seg_pdf,
        x="author",
        y="avg_dialog_ratio",
        color="genre",
        barmode="group",
        title="Proportion de dialogue par auteur (heuristique typographique)",
        labels={
            "author"           : "Auteur",
            "avg_dialog_ratio" : "Ratio dialogue / total des lignes",
            "genre"            : "Genre littéraire",
        },
        text_auto=".2%",       
    )
    fig.update_yaxes(
        tickformat=".0%",   
        title="Part de lignes dialogue [0% = tout narration | 100% = tout dialogue]",
    )
    fig.update_layout(
        template="plotly_white",
        legend_title="Genre",
    )
    return _save(fig, os.path.join(output_dir, "segmentation.html"))