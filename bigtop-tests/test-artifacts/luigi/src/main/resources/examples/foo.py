import luigi
import time
import os
import shutil
import fileinput
import sys

class Foo(luigi.Task):
    num = luigi.IntParameter()
    out = luigi.Parameter(default='/tmp/bar')

    def run(self):
        print "Running Foo"

    def requires(self):
        for i in xrange(self.num):
            yield Bar(i, self.out)


class Bar(luigi.Task):
    num = luigi.IntParameter()
    out = luigi.Parameter(default='/tmp/bar')

    def run(self):
        time.sleep(1)
        self.output().open('w').close()

    def output(self):
        time.sleep(1)
        return luigi.LocalTarget('%s/%d' % (self.out, self.num))


if __name__ == "__main__":
    (output_dir, num_iterations) = (sys.argv[1], sys.argv[2])

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    luigi.run(['--task', 'Foo', '--num', num_iterations, '--out', output_dir, '--workers', '2'], use_optparse=True)
