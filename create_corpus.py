import os
import time
import requests

from bs4 import BeautifulSoup
from urllib.parse import urljoin

from ebooklib import epub

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# path pour les livres téléchargés

DATA_DIR_ROMANTIC = "DATA/Romantic"
DATA_DIR_REALISM  = "DATA/Realism"

os.makedirs(DATA_DIR_ROMANTIC, exist_ok=True)
os.makedirs(DATA_DIR_REALISM, exist_ok=True)


#sources

github_url = "https://github.com/dh-trier/balzac/tree/master/plain"

gutenberg_url = (
    "https://www.gutenberg.org/ebooks/search/"
    "?query=la+com%C3%A9die+humaine"
)

beq_urls_romantic = [
    "https://beq.ebooksgratuits.com/vents/dumas.htm",
    "https://beq.ebooksgratuits.com/vents/sand.htm"
]

beq_urls_realism = [
    "https://beq.ebooksgratuits.com/vents/Maupassant.htm",
    "https://beq.ebooksgratuits.com/vents/zola.htm",
    "https://beq.ebooksgratuits.com/vents/dickens.htm",
]


headers = {
    "User-Agent": (
        "Mozilla/5.0 "
        "(X11; Linux x86_64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}


# création session pour web scrap

session = requests.Session()

retry = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504],
)

adapter = HTTPAdapter(max_retries=retry)

session.mount("http://", adapter)
session.mount("https://", adapter)


## fonctions utiles pour web scraping et gestion du corpus

def safe_get(url, timeout=(10, 60)):
    """
    Requête HTTP robuste.
    """

    try:

        r = session.get(
            url,
            headers=headers,
            timeout=timeout
        )

        r.raise_for_status()

        return r

    except Exception as e:

        print(f"Erreur HTTP sur {url}")
        print(e)

        return None


def get_soup(url):

    r = safe_get(url)

    if r is None:
        return None

    return BeautifulSoup(r.text, "html.parser")




#extraction des liens

def extract_beq_links(page_url):

    soup = get_soup(page_url)

    if soup is None:
        return []

    links = []

    for a in soup.find_all("a", href=True):

        href = a["href"]

        if (
            ".epub" in href.lower()
            or ".txt" in href.lower()         # on ne prends que les fichiers textes disponibles sur la page
        ):

            full = urljoin(page_url, href)

            links.append(full)

    return list(set(links))


#attribution auteur

def findAuthorInUrl(url):

    url_lower = url.lower()

    if ("balzac" in url_lower or "gutenberg" in url_lower):
        return "balzac"

    elif "dumas" in url_lower:
        return "dumas"
    
    elif "sand" in url_lower:
        return "sand"
    
    elif "maupassant" in url_lower:
        return "maupassant"

    elif "zola" in url_lower:
        return "zola"

    elif "dickens" in url_lower:
        return "dickens"

    else:
        return "unknown"



#limite de livres pour avoir un corpus équilibré

def limit_per_author(links, max_books):

    counts = {}
    filtered = []

    for url in links:

        author = findAuthorInUrl(url)

        if author not in counts:
            counts[author] = 0

        if counts[author] < max_books:

            filtered.append(url)
            counts[author] += 1

    return filtered


#extraction pub

def extract_epub_text(epub_path):

    try:

        book = epub.read_epub(epub_path)

        chapters = []

        for item in book.get_items():

            # DOCUMENT
            if item.get_type() == 9:

                soup = BeautifulSoup(
                    item.get_content(),
                    "html.parser"
                )

                text = soup.get_text(separator=" ")

                chapters.append(text)

        return "\n".join(chapters)

    except Exception as e:

        print(f"Erreur lecture epub {epub_path}")
        print(e)

        return ""



#téléchargement 

def download_text(url, folder):

    try:

        print(f"Téléchargement : {url}")

        author = findAuthorInUrl(url)

        base_filename = url.split("/")[-1]

        filename = f"{author}_{base_filename}"

        filepath = os.path.join(folder, filename)

        r = safe_get(url)

        if r is None:
            return None

        # fichiers format epub

        if base_filename.lower().endswith(".epub"):

            with open(filepath, "wb") as f:
                f.write(r.content)

            text = extract_epub_text(filepath)

        # fichiers formats texte

        else:

            text = r.text

            with open(
                filepath,
                "w",
                encoding="utf-8"
            ) as f:

                f.write(text)


        text = text[:300000]

        return (
            filename,
            url,
            text,
            author
        )

    except Exception as e:

        print(f"Erreur téléchargement {url}")
        print(e)

        return None



def download(links, folder):

    corpus = []

    for url in links:

        try:

            author = findAuthorInUrl(url)
            if(author == "unknown"):

                print(f"Auteur inconnu pour {url}, skipping.")
                continue
            
            base_filename = url.split("/")[-1]

            filename = f"{author}_{base_filename}"

            filepath = os.path.join(folder, filename)

            #si fichies existe déjà

            if os.path.exists(filepath):

                print(f"Déjà présent : {filename}")

                try:

                    if filename.lower().endswith(".epub"):

                        text = extract_epub_text(filepath)

                    else:

                        with open(
                            filepath,
                            "r",
                            encoding="utf-8"
                        ) as f:

                            text = f.read()

                    text = text[:300000]

                    corpus.append(
                        (
                            filename,
                            filepath,
                            text,
                            author
                        )
                    )

                except Exception as e:

                    print(f"Erreur lecture locale {filename}")
                    print(e)

                continue

            #si existe pas on télécharge chaque fichier trouvés dans les listes

            result = download_text(url, folder)

            if result:
                corpus.append(result)

            # important :
            # évite de spam Gutenberg
            time.sleep(1)

        except Exception as e:

            print(f"Erreur globale sur {url}")
            print(e)

            continue

    return corpus


#chargement local (si déjà présent en local)

def load_local_corpus(
    folder,
    max_books_per_author=5
):

    corpus = []

    counts = {}

    for filename in os.listdir(folder):

        filepath = os.path.join(folder, filename)

        if not os.path.isfile(filepath):
            continue

        author = findAuthorInUrl(filename)

        if author not in counts:
            counts[author] = 0

        if counts[author] >= max_books_per_author:
            continue

        counts[author] += 1

        try:

            if filename.lower().endswith(".epub"):

                text = extract_epub_text(filepath)

            else:

                with open(
                    filepath,
                    "r",
                    encoding="utf-8"
                ) as f:

                    text = f.read()

            text = text[:300000]

            corpus.append(
                (
                    filename,
                    filepath,
                    text,
                    author
                )
            )

            print(f"Chargement local : {filename}")

        except Exception as e:

            print(f"Erreur lecture locale {filename}")
            print(e)

    return corpus




def corpus_already_exists():

    romantic_exists = (
        os.path.exists(DATA_DIR_ROMANTIC)
        and len(os.listdir(DATA_DIR_ROMANTIC)) > 0
    )

    realism_exists = (
        os.path.exists(DATA_DIR_REALISM)
        and len(os.listdir(DATA_DIR_REALISM)) > 0
    )

    return romantic_exists and realism_exists


#fonction principale

def create_my_corpus(max_books_per_author=5):


    if corpus_already_exists():

        print("Chargement du corpus local...")

        romanticCorpus = load_local_corpus(
            DATA_DIR_ROMANTIC,
            max_books_per_author
        )

        realismCorpus = load_local_corpus(
            DATA_DIR_REALISM,
            max_books_per_author
        )

        return romanticCorpus, realismCorpus


    linksRomantic = []
    linksRealism  = []

    #github balzac

    print("Scraping GitHub Balzac...")

    soup_git = get_soup(github_url)

    if soup_git is not None:

        for a in soup_git.find_all("a", href=True):

            href = a["href"]

            if href.endswith(".txt"):

                full = (
                    "https://raw.githubusercontent.com"
                    + href.replace("/blob", "")
                )

                linksRomantic.append(full)

    #BEQ romantique

    print("Scraping BEQ romantique...")

    for url in beq_urls_romantic:

        linksRomantic.extend(
            extract_beq_links(url)
        )

    #BEQ réalisme

    print("Scraping BEQ réalisme...")

    for url in beq_urls_realism:

        linksRealism.extend(
            extract_beq_links(url)
        )

    #gutenberg
    #on peu plus complexe à cause du format du site
    print("Scraping Gutenberg...")

    soup_gutenberg = get_soup(gutenberg_url)

    if soup_gutenberg is not None:

        linkTemp = []

        for a in soup_gutenberg.find_all("a"):

            href = a.get("href")

            if href and "/ebooks/" in href:

                full = (
                    "https://www.gutenberg.org"
                    + href
                )

                linkTemp.append(full)

        linkTemp = list(set(linkTemp))

        #on visite chaque page pour récupérer le livre

        for ebook in linkTemp:

            try:

                print(f"Analyse Gutenberg : {ebook}")

                r = safe_get(
                    ebook,
                    timeout=(10, 60)
                )

                if r is None:
                    continue

                soup = BeautifulSoup(
                    r.text,
                    "html.parser"
                )

                for row in soup.find_all("tr"):

                    text = row.get_text(
                        " ",
                        strip=True
                    ).lower()

                    if "plain text utf-8" in text:

                        a = row.find("a")

                        if (
                            a
                            and a.get("href")
                        ):

                            full_link = (
                                "https://www.gutenberg.org"
                                + a["href"]
                            )

                            linksRomantic.append(
                                full_link
                            )

                # anti rate-limit
                time.sleep(1)

            except Exception as e:

                print(f"Erreur Gutenberg {ebook}")
                print(e)

                continue

    #on a la liste des liens url
    #on supprime les doublons

    linksRomantic = list(set(linksRomantic))
    linksRealism  = list(set(linksRealism))

    linksRomantic = limit_per_author(
        linksRomantic,
        max_books_per_author
    )

    linksRealism = limit_per_author(
        linksRealism,
        max_books_per_author
    )

    print(
        f"Livres romantiques : "
        f"{len(linksRomantic)}"
    )

    print(
        f"Livres réalistes : "
        f"{len(linksRealism)}"
    )


    #à partir des liens on télécharge les fichiers

    romanticCorpus = download(
        linksRomantic,
        DATA_DIR_ROMANTIC
    )

    realismCorpus = download(
        linksRealism,
        DATA_DIR_REALISM
    )

    print(
        f"Corpus romantique : "
        f"{len(romanticCorpus)} documents"
    )

    print(
        f"Corpus réalisme : "
        f"{len(realismCorpus)} documents"
    )

    return romanticCorpus, realismCorpus