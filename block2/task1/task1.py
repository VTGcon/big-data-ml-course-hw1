
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRLinesCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner,reducer=self.reducer_count),
            MRStep(reducer=self.reducer_max)
        ]

    def mapper(self, _, line):
        character = line.split('" "')[1]
        yield (character, 1)

    def combiner(self, character, counts):
        yield (character, sum(counts))

    def reducer_count(self, character, count):
        yield None, (character, sum(count))

    def reducer_max(self, _, character_counts):
        for character_count in sorted(character_counts, key=lambda x: x[1], reverse=True)[:20]:
            yield character_count
        
if __name__ == "__main__":
    MRLinesCount.run()
