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

    def submit(self, job_script):
        """
        Submit job defined in job_script and return job id
        """
        raise NotImplementedError()

    def completed(self, job_id):
        """
        Return True if job has finished, else return False
        """
        raise NotImplementedError()

    def completion_status(self, job_id):
        """
        Return completion status (COMPLETED, FAILED etc.) of job job_id.
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

    def submit(self, job_script):
        """
        Submit job defined in job_script and return job id
        """

        if not os.path.isfile(job_script):
            raise IOError("job script [%s] not found" % job_script)

        jobDir = os.path.dirname(os.path.abspath(job_script))

        # submit job
        cmd = ["sbatch", job_script]
        logger.info("cmd: %s" % " ".join(cmd))
        run = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, cwd=jobDir)
        output = run.communicate()
        rc = run.wait()
        if rc !=0:
            logger.debug("0: %s" % output[0])
            logger.debug("1: %s" % output[1])
            raise RuntimeError("'sbatch [%s]' failed" % job_script)
        job_id = output[0].split()[-1].strip()
        logger.info("job_id: %s" % job_id)

        return job_id


    def completed(self, job_id):
        """
        Return true if job is not in queue anymore, i.e. squeue returns
        a non-zero exit code
        """

        cmd = "squeue -j %s" % job_id
        rc = sp.call(cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE)
        if rc==0:
            return False
        else:
            return True

            
    def completion_status(self, job_id):
        """
        Return the completion status (COMPLETED, FAILED etc.)
        of a completed job
        """

        cmd = 'sacct --format state -n -j %s.batch' % job_id
        run = sp.Popen(cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE)
        output = run.communicate()
        rc = run.wait()
        if rc != 0:
            logger.debug("0: %s" % output[0])
            logger.debug("1: %s" % output[1])
            raise RuntimeError('command [%s] failed' % cmd)
        status = output[0].strip()

        return status
