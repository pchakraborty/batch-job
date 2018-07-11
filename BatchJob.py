import os
import time
import logging

import subprocess as sp

from datetime import datetime

logger = logging.getLogger(__name__)

class BatchJob(object):
    """
    Base class defining interfaces to interact with job schedulers
    """

    def submit(self, jobScript):
        """
        Submit job defined in jobScript and return job id
        """
        raise NotImplementedError()

    def completed(self, jobid):
        """
        Return True if job has finished, else return False
        """
        raise NotImplementedError()

    def CompletionStatus(self, jobid):
        """
        Return completion status (COMPLETED, FAILED etc.) of job jobid.
        """
        raise NotImplementedError()

    @staticmethod
    def factory(type):
        """
        Factory method returning an instance of a derived class
        based on the input type
        """
        if type.strip().lower()=="slurm":
            return BatchSlurm()
        else:
            raise ValueError("batch type [%s] is not supported" % type)



class BatchSlurm(BatchJob):
    """
    Implement SLURM specific code
    """

    def submit(self, jobScript):
        """
        Submit job defined in jobScript and return job id
        """

        if not os.path.isfile(jobScript):
            raise IOError("job script [%s] not found" % jobScript)

        jobDir = os.path.dirname(os.path.abspath(jobScript))

        # submit job
        cmd = ["sbatch", jobScript]
        logger.info("cmd: %s" % " ".join(cmd))
        run = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, cwd=jobDir)
        output = run.communicate()
        rc = run.wait()
        if rc !=0:
            logger.debug("0: %s" % output[0])
            logger.debug("1: %s" % output[1])
            raise RuntimeError("'sbatch [%s]' failed" % jobScript)
        jobid = output[0].split()[-1].strip()
        logger.info("jobid: %s" % jobid)

        return jobid


    def completed(self, jobid):
        """
        Return true if job is not in queue anymore, i.e. squeue returns
        a non-zero exit code
        """

        cmd = "squeue -j %s" % jobid
        rc = sp.call(cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE)
        if rc==0:
            return False
        else:
            return True

            
    def CompletionStatus(self, jobid):
        """
        Return the completion status (COMPLETED, FAILED etc.)
        of a completed job
        """

        cmd = 'sacct --format state -n -j %s.batch' % jobid
        run = sp.Popen(cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE)
        output = run.communicate()
        rc = run.wait()
        if rc != 0:
            logger.debug("0: %s" % output[0])
            logger.debug("1: %s" % output[1])
            raise RuntimeError('command [%s] failed' % cmd)
        status = output[0].strip()

        return status
