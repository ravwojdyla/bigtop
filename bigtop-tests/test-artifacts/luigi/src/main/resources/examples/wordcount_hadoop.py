import luigi, luigi.hadoop, luigi.hdfs

# To make this run, you probably want to edit /etc/luigi/client.cfg and add something like:
#
# [hadoop]
# jar: /usr/lib/hadoop-xyz/hadoop-streaming-xyz-123.jar

class InputText(luigi.ExternalTask):
    input_dataset = luigi.Parameter()
    def output(self):
        return luigi.hdfs.HdfsTarget(self.input_dataset)


class WordCount(luigi.hadoop.JobTask):
    input_dataset = luigi.Parameter()
    output_dataset = luigi.Parameter()

    n_reduce_tasks = 1
    streaming_args = ['-D', 'mapreduce.output.fileoutputformat.compress=false']

    def job_runner(self):
        job_runner = super(WordCount, self).job_runner()
        job_runner.streaming_args += self.streaming_args
        return job_runner

    def requires(self):
        return [InputText(self.input_dataset)]

    def output(self):
        return luigi.hdfs.HdfsTarget(self.output_dataset)

    def mapper(self, line):
        for word in line.strip().split():
            yield word, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    luigi.run()
