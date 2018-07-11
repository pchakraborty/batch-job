#!/usr/bin/env python
"""Unit tests for batch_job.py"""

import unittest
import time
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from batch_job import BatchJob

LOG_FMT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOG_FMT)
LOGGER = logging.getLogger(__name__)

class TestBatchJob(unittest.TestCase):
    """Docstring"""

    def setUp(self):
        self.utest_dir = os.path.dirname(os.path.realpath(__file__))
        LOGGER.info("Unit test location: %s" % self.utest_dir)
        self.slurm_output = None

    def test_batch_script_submission(self):
        """
        Test building suite definition
        """
        job_script = os.path.join(self.utest_dir, "abc.sh")
        job = BatchJob.factory("slurm") # BatchSlurm object
        job_id = job.submit(job_script)
        while True:
            if job.is_complete(job_id):
                LOGGER.info("%s: %s" % (job_id, job.completion_status(job_id)))
                break
            LOGGER.debug("waiting...")
            time.sleep(15)
        self.slurm_output = "slurm-%s.out" % job_id

    def tearDown(self):
        if self.slurm_output:
            os.remove(os.path.join(self.utest_dir, self.slurm_output))

if __name__ == "__main__":
    unittest.main()
