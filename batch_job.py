"""Docstring:"""
import os
import logging
import subprocess as sp
from abc import ABCMeta, abstractmethod

LOGGER = logging.getLogger(__name__)

class BatchJob(object):
    """
    Base class defining interfaces to interact with job schedulers
    """
    __metaclass__ = ABCMeta

    @staticmethod
    def factory(batch_type):
        """
        Factory method returning an instance of a derived class
        based on the input type
        """
        if batch_type.strip().lower()=="slurm":
            return BatchSlurm()
        else:
            raise ValueError("batch type [%s] is not supported" % batch_type)

    @abstractmethod
    def submit(self, job_script):
        """
        Submit job defined in job_script and return job id
        """
        pass

    @abstractmethod
    def is_complete(self, job_id):
        """
        Return True if job has finished, else return False
        """
        pass

    @abstractmethod
    def completion_status(self, job_id):
        """
        Return completion status (COMPLETED, FAILED etc.) of job job_id.
        """
        pass

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

        # submit job
        job_dir = os.path.dirname(os.path.abspath(job_script))
        cmd = ["sbatch", job_script]
        LOGGER.info("Submit command: %s" % " ".join(cmd))
        run = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, cwd=job_dir)
        output = run.communicate()
        return_code = run.wait()
        if return_code != 0:
            LOGGER.debug("0: %s" % output[0])
            LOGGER.debug("1: %s" % output[1])
            raise RuntimeError("'sbatch [%s]' failed" % job_script)
        job_id = output[0].split()[-1].strip()
        LOGGER.info("Job id: %s" % job_id)

        return job_id

    def is_complete(self, job_id):
        """
        Return true if job is not in queue anymore, i.e. squeue returns
        a non-zero exit code
        """
        cmd = "squeue -j %s" % job_id
        return_code = sp.call(cmd.split(), stdout=sp.PIPE, stderr=sp.PIPE)
        if return_code == 0:
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
        return_code = run.wait()
        if return_code != 0:
            LOGGER.debug("0: %s" % output[0])
            LOGGER.debug("1: %s" % output[1])
            raise RuntimeError('command [%s] failed' % cmd)
        status = output[0].strip()

        return status
