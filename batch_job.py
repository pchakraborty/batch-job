"""Docstring:"""
import os
import logging
import subprocess as sp
import abc
# compatible with Python 2 and 3
ABC = abc.ABCMeta("ABC", (object,), {'__slots__': ()})

LOGGER = logging.getLogger(__name__)

class BatchJob(ABC):
    """
    Base class defining interfaces to interact with job schedulers
    """

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

    @abc.abstractmethod
    def submit(self, job_script):
        """
        Submit job defined in job_script and return job id
        """
        pass

    @abc.abstractmethod
    def is_complete(self, job_id):
        """
        Return True if job has finished, else return False
        """
        pass

    @abc.abstractmethod
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
        job_dir = os.path.dirname(os.path.abspath(job_script))
        cmd = ["sbatch", job_script]
        LOGGER.info("Submit command: %s" % " ".join(cmd))
        output = sp.check_output(cmd, cwd=job_dir)
        job_id = output.decode("utf-8").strip().split()[-1]
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
        output = sp.check_output(cmd.split())
        status = output.decode("utf-8").strip()
        return status
