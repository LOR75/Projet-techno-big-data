import re
from pyspark.sql.types import ArrayType, StringType


#comptage de syllabe

def count_syllables(word: str) -> int:
    """
    Estime le nombre de syllabes d'un mot français ou anglais.

    """
    word = word.lower()

    vowels = "aeiouyàâéèêëîïôùûü"

    count = 0
    prev_is_vowel = False  # indique si le caractère précédent était une voyelle

    for char in word:
        is_vowel = char in vowels
        # On compte une nouvelle syllabe uniquement si :
        #   - le caractère courant est une voyelle
        #   - et le caractère précédent n'était pas une voyelle
        if is_vowel and not prev_is_vowel:
            count += 1
        prev_is_vowel = is_vowel

    # Garantit au moins 1 syllabe
    return max(count, 1)


def text_syllables(tokens) -> int:
    """
    Somme des syllabes sur une liste de tokens
    """
    return sum(count_syllables(w) for w in tokens)



#indice de lisibilité

def flesch(words: int, sentences: int, syllables: int) -> float:
    """
    Indice de lisibilité selon formule de Flesch
    """
    if words == 0 or sentences == 0:
        return 0.0

    return (
        206.835
        - 1.015  * (words / sentences)   
        - 84.6   * (syllables / words) 
    )


def kandel_moles(words: int, sentences: int, syllables: int) -> float:
    """
    Indice de lisibilité selon la formue de Kandel-Moles

    """
    if words == 0 or sentences == 0:
        return 0.0

    return (
        207.0
        - 1.015  * (words / sentences)
        - 73.6   * (syllables / words)
    )


#fenêtre glissante

def sliding_windows(tokens, window_size: int = 500, step: int = 250):
    """
    Découpe une liste de tokens en fenêtres glissantes.
    """
    windows = []
    n = len(tokens)

    # range(0, max(n - window_size + 1, 1), step) génère les indices de début
    # La borne supérieure garantit qu'on n'extrait pas de fenêtre vide au-delà
    # de la fin du texte, tout en forçant au moins une fenêtre.
    for start in range(0, max(n - window_size + 1, 1), step):
        end = start + window_size
        windows.append(tokens[start:end])

    return windows
