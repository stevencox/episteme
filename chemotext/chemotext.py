import json
import glob
import HTMLParser
import logging
import nltk
import nltk.data
import os
import string
import socket
import cStringIO
import sys
import traceback
import xml.parsers.expat
from mesh import MeSH
from nltk import word_tokenize
try:
    from lxml import etree as et
except ImportError:
    import xml.etree.cElementTree as et
from pyspark.mllib.feature import Word2Vec
from pyspark.mllib.feature import Word2VecModel
        
def init_logging ():
    FORMAT = '%(asctime)-15s %(filename)s %(funcName)s %(levelname)s: %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    return logging.getLogger(__file__)

logger = init_logging ()

def get_spark_context ():
    os.environ['PYSPARK_PYTHON'] = "/projects/stars/venv/bin/python"
    from pyspark import SparkConf, SparkContext
    from StringIO import StringIO
    ip = socket.gethostbyname(socket.gethostname())
    conf = (SparkConf()
            .setMaster("mesos://{0}:5050".format (ip))
            .setAppName("ChemoText Analytic")
#            .set("spark.mesos.coarse", "true")
            .set("spark.executor.memory", "5g"))
    return SparkContext(conf = conf)

class Cache(object):
    def __init__(self):
        self.path = "cache"
        if not os.path.isdir (self.path):
            os.makedirs (self.path)
    def get (self, name):
        val = None
        obj = os.path.join (self.path, name)
        if os.path.exists (obj):
            with open (obj, 'r') as stream:
                val = json.loads (stream.read ())
        return val
    def put (self, name, obj):
        obj_path = os.path.join (self.path, name)
        with open (obj_path, 'w') as stream:
            stream.write (json.dumps (obj, sort_keys=True, indent=2))

def get_dirs (root, ):
    dirs = []
    subs = glob.glob (root, "*")
    for s in subs:
        if os.path.isdir (s):
            dirs.append (s)
    return dirs

def get_article_dirs (articles):
    cache = Cache ()
    dirs = cache.get ('pubmed_dirs.json')
    c = 0
    if dirs is None:
        dirs = []
        for root, dirnames, files in os.walk (articles):
            for d in dirnames:
                dirs.append (os.path.join (root, d))
                c = c + 1
                if c > 100:
                    break
        cache.put ('pubmed_dirs.json', dirs)
    return dirs

def get_corpus_sentences (article):
    results = []
    try:
        sentence_detector = nltk.data.load('tokenizers/punkt/english.pickle')
        print "@-article: {0}".format (article)
        with open (article) as stream:
            tree = et.parse (article)
            paragraphs = tree.findall ('.//p')
            if paragraphs is not None:
                for para in paragraphs:
                    try:
                        text = "".join( [ "" if para.text is None else para.text ] +
                                        [ et.tostring (e, encoding='UTF-8', method='text') for e in para.getchildren() ] )
                        text = delete_non_unicode_chars (text)
                        text = text.decode ("utf8", "ignore")
                        results.append ( sentence_detector.tokenize (text.strip ()) )
                    except:
                        traceback.print_exc ()
    except:
        traceback.print_exc ()
    return results

corpus_file_name = "pubmed.corpus"

def generate_corpus (articles, app_home):
    path = os.path.join (app_home, corpus_file_name)
    if not os.path.exists (path):
        sentences = articles.flatMap (generate_corpus_sentences).cache ()
        corpus = sentences.collect ()
        with open (path, 'w') as stream:
            for sentence in corpus:
                stream.write ("{0}\n".format (sentence))

word2vec_model_path = "pubmed.word2vec"

def get_word2vec_model (sc, app_home):
    path = os.path.join (app_home, word2vec_model_path)
    model = None
    if os.path.exists (path):
        model = Word2VecModel.load(sc, word2vec_model_path)
    elif os.path.exists (corpus_file_name):
        inp = sc.textFile (corpus_file_name).map(lambda row: row.split(" "))
        word2vec = Word2Vec()
        #word2vec.setNumPartitions (200)
        model = word2vec.fit(inp)
        model.save (sc, path)
    else:
        logger.error ("No pubmed corpus file found at {0}.".format (corpus_file_name))
    return model

extended_mesh = "mesh.ext"
def find_synonyms (model, word, radius):
    try:
        model.findSynonyms (word, radius)
    except:
        logger.error ("unable to find word {0}".format (word))

def get_synonyms (model, mesh_xml, app_home, radius=5):
    if os.path.exists (extended_mesh):
        mesh = MeSH (extended_mesh)
    else:
        src_path = os.path.join (app_home, mesh_xml)
        mesh = MeSH (src_path)
        for word in mesh.proteins:
            synonyms = find_synonyms (model, word, radius) #model.findSynonyms (protein, radius)
            if synonyms is not None:
                mesh.proteins = mesh.proteins + synonyms
        for word in mesh.chemicals:
            synonyms = find_synonyms (model, word, radius) #model.findSynonyms (chemical, radius)
            if synonyms is not None:
                mesh.chemicals = mesh.chemicals + synonyms
        for word in mesh.diseases:
            synonyms = find_synonyms (model, word, radius) #model.findSynonyms (disease, radius)
            if synonyms is not None:
                mesh.diseases = mesh.diseases + synonyms
        mesh.save (extended_mesh)
    return mesh

def process_article (item): #article, mesh_xml):
    
    article = item [0]
    mesh_xml = item [1]
    results = []

    sentence_detector = nltk.data.load('tokenizers/punkt/english.pickle')

    def vocab_nlp (words, text, article):
        result = None
        if words and text:
            sentences = sentence_detector.tokenize(text.strip())
            for sentence in sentences:
                for term in words:
                    if sentence.find (term) > -1:
                        tokens = word_tokenize (sentence)
                        tags = nltk.pos_tag (tokens)
                        for tag in tags:
                            if term == tag[0]:
                                result = ( term, tags )
                                print "--> word:{0} tags:{1} article:{2}".format (term, tags, article)
                                break
        return result
        
    # return [ ( socket.gethostname (), [ 0, 1, 2 ], [ 3, 4, 5 ]  ) ]

    try:
        mesh = MeSH (mesh_xml)
        print "@-article: {0}".format (article)
        with open (article) as stream:
            '''
            data = delete_xml_char_refs (stream.read ())
            filtered = cStringIO.StringIO (data)
            tree = et.parse (filtered)
            '''
            tree = et.parse (article)
            paragraphs = tree.findall ('.//p')
            if paragraphs is not None:
                for para in paragraphs:
                    try:
                        text = "".join( [ "" if para.text is None else para.text ] +
                                        [ et.tostring (e, encoding='UTF-8', method='text') for e in para.getchildren() ] )
                        text = delete_non_unicode_chars (text)
                        text = text.decode ("utf8", "ignore")
                        chemical = vocab_nlp (mesh.chemicals, text, article)
                        disease  = vocab_nlp (mesh.diseases, text, article)
                        protein  = vocab_nlp (mesh.proteins, text, article)
                        if protein and chemical and disease:
                            results.append ( ( article,
                                               [ chemical [0], protein [0], disease [0] ],
                                               [ chemical [1], protein [1], disease [1] ]
                                           ) )
                    except:
                        traceback.print_exc ()
    except:
        traceback.print_exc ()

    return results

def find_relationships (app_home, articles, mesh_xml):
    sc = get_spark_context ()

    logger.info ("Getting Pubmed dirs")
    dirs = get_article_dirs (articles)
    logger.debug ("dirs: {0}".format (dirs))

    cache = Cache ()
    article_list = cache.get ('articles')
    if article_list is None:
        articles = sc.parallelize (dirs, 150)
        logger.debug ("dirs -> {0}".format (articles.count ()))
        articles = articles.flatMap (lambda d : glob.glob (os.path.join (d, "*.nxml") )).cache ()
        logger.info ("articles -> {0}".format (articles.count ()))
        article_list = articles.collect ()
        cache.put ('articles', article_list)

    logger.info ("Processing {0} articles".format (len (article_list)))
    articles = sc.parallelize (article_list, 190).cache ()

    generate_corpus (articles, app_home)
    model = get_word2vec_model (sc, app_home)
    get_synonyms (model, mesh_xml, app_home)

    articles = articles.map (lambda a : ( a, mesh_xml ))

    logger.info ("intermediate: {0} articles to process".format (articles.count ()))
    articles = articles.flatMap (process_article).cache ().collect ()

    for hit in hits:
        article = hit [0]
        triangle = hit [1]
        logger.info ("{0} contains \n: \tchemical:{1}\n\tprotein:{2}\n\tdisease:{3}".format (article, triangle[0], triangle[1], triangle[2]))

def delete_non_unicode_chars (text):
    c = 0
    while c < len(text):
        if ord(text[c]) > 128:
            text = text[0:c-1] + text[c+1:len(text)]
        c = c + 1
    return text

def delete_xml_char_refs (text):
    c = 0
    while c < len(text):
        if text[c] == '&' and c < len(text) + 1 and text[c+1] == '#':
            mark = c
            while text[c] != ';':
                c = c + 1
            text = text[0:mark] + text[c+1:len(text)]
        c = c + 1
    return text

def main ():
    app_home = sys.argv [1]
    articles = sys.argv [2]
    mesh_xml = sys.argv [3]
    find_relationships (app_home, articles, mesh_xml)
    #print process_article (articles, mesh_xml)

main ()

