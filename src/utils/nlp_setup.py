import nltk
from src.utils.logger import get_logger

logger = get_logger(__name__)

def setup_nlp_environment():
    """
    Verify and download the language models required for TextBlob.
    """
    # These are the 4 basic packages that TextBlob needs to function properly
    needed_packages = [
        'punkt',                       # To separate phrases and words
        'wordnet',                     # Lexical dictionary
        'brown',                       # General corpus of text
        'averaged_perceptron_tagger'   # To identify verbs, nouns, etc.
    ]
    
    logger.info("Verifying NLP dependencies (TextBlob/NLTK)...")
    
    for pack in needed_packages:
        try:
            # nltk automatically manages whether it already exists in ~/nltk_data/
            nltk.download(pack, quiet=True)
        except Exception as e:
            logger.error(f"Critical error downloading NLP package '{pack}': {e}")
            raise # We stop the pipeline if a model is missing, to avoid processing garbage.
            
    logger.info("Ready-to-use NLP environment.")