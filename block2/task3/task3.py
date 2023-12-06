
from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
import string
import nltk

class MRBigramsCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init, mapper=self.mapper, combiner=self.combiner, reducer=self.reducer_cnt),
            MRStep(reducer=self.reducer_max)
        ]
        
    def mapper_init(self):
        nltk.download('punkt')
        nltk.download('stopwords')

    def mapper(self, _, line):
        line = line.split('\"')[-2].translate(str.maketrans('', '', string.punctuation))
        tokens = word_tokenize(line.lower())
        stop_words = set(stopwords.words('english'))
        tokens = [token for token in tokens if token not in stop_words]
        for bigram in ngrams(tokens, 2):
            yield (bigram, 1)

    def combiner(self, bigram, counts):
        yield (bigram, sum(counts))

    def reducer_cnt(self, bigram, counts):
        yield None, (bigram, sum(counts))

    def reducer_max(self, _, counts):
        for count in sorted(counts, key=lambda x: x[1], reverse=True)[:20]:
            yield count
        
if __name__ == "__main__":
    MRBigramsCount.run()
