import luigi

class InputText(luigi.ExternalTask):
    input_dataset = luigi.Parameter()
    ''' This class represents something that was created elsewhere by an external process,
    so all we want to do is to implement the output method.
    '''
    def output(self):
        return luigi.LocalTarget(self.input_dataset)

class WordCount(luigi.Task):
    input_dataset = luigi.Parameter()
    output_dataset = luigi.Parameter()

    def requires(self):
        return [InputText(self.input_dataset)]

    def output(self):
        return luigi.LocalTarget(self.output_dataset)

    def run(self):
        count = {}
        print self.input()
        for file in self.input(): # The input() method is a wrapper around requires() that returns Target objects
            for line in file.open('r'): # Target objects are a file system/format abstraction and this will return a file stream object
                for word in line.strip().split():
                    count[word] = count.get(word, 0) + 1

        # output data
        f = self.output().open('w')
        for word, count in count.iteritems():
            f.write("%s\t%d\n" % (word, count))
        f.close() # Note that this is essential because file system operations are atomic

if __name__ == '__main__':
    luigi.run(main_task_cls=WordCount)
