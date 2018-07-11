#!/usr/bin/env python
"""Unit tests for batch_job.py"""

import unittest
import time
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from batch_job import BatchJob

log_fmt = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
logging.basicConfig(level=logging.DEBUG, format=log_fmt)
logger = logging.getLogger(__name__)

class TestBatchJob(unittest.TestCase):
    """
    TODO: add docstring
    """

    def setUp(self):
        self.utest_dir = os.path.dirname(os.path.realpath(__file__))
        logger.info("Unit test location: %s" % self.utest_dir)
        
    def test_batch_script_submission(self):
        """
        Test building suite definition
        """
        job_script = os.path.join(self.utest_dir, "abc.sh")
        job = BatchJob.factory("slurm") # BatchSlurm object
        job_id = job.submit(job_script)
        self.slurm_output = "slurm-%s.out" % job_id

        while True:
            if job.is_complete(job_id):
                logger.info("%s: %s" % (job_id, job.completion_status(job_id)))
                break
            logger.debug("waiting...")
            time.sleep(15)
        
    def tearDown(self):
        os.remove(os.path.join(self.utest_dir, self.slurm_output))

if __name__ == "__main__":
    unittest.main()
