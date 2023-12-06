
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRLinesCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer_cnt),
            MRStep(reducer=self.reducer_max)
        ]

    def mapper(self, _, line):
        result = line.split('\"')
        yield (result[3], result[-2])

    def combiner(self, character, lines):
        yield (character, max(lines, key=len))

    def reducer_cnt(self, character, lines):
        yield None, (character, max(lines, key=len))

    def reducer_max(self, _, lines):
        for line in sorted(lines, key=lambda x: len(x[1]), reverse=True):
            yield line
        
if __name__ == "__main__":
    MRLinesCount.run()
