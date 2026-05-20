"""

Étapes de la pipeline

1. DocumentAssembler : convertit la colonne string en colonne 'document'

2. SentenceDetector : segmente le Document en phrases individuelles

3. Tokenizer : découpe chaque phrase en tokens

4. PerceptronModel : ajoute un tag à chaque token (une info grammaticale)

"""

from pyspark.sql import Row

from sparknlp.base      import DocumentAssembler
from sparknlp.annotator import SentenceDetector, Tokenizer, PerceptronModel
from pyspark.ml         import Pipeline



#créé le dataframe spark à partir du corpus
def build_df(corpus, genre, spark):

    rows = []

    for i, (filename, url, text, author) in enumerate(corpus):
        rows.append(Row(
            id     = f"{genre}_{i}",   # ex. "romantic_0", "realism_3"
            author = author,
            title  = filename,
            genre  = genre,
            text   = text,
        ))

    return spark.createDataFrame(rows)


#pipeline

def build_pipeline(romanticCorpus, realismCorpus):



    document_assembler = (
        DocumentAssembler()
        .setInputCol("text")      
        .setOutputCol("document")  
    )

    sentence_detector = (
        SentenceDetector()
        .setInputCols(["document"])  
        .setOutputCol("sentence") 
    )

    tokenizer = (
        Tokenizer()
        .setInputCols(["sentence"]) 
        .setOutputCol("token")
    )


    pos = (
        PerceptronModel.pretrained()
        .setInputCols(["sentence", "token"])  
        .setOutputCol("pos")
    )


    pipeline = Pipeline(stages=[
        document_assembler,
        sentence_detector,
        tokenizer,
        pos,
    ])

    return pipeline
