"""This module defines the ``Tuner`` class, which all Rekall auto-tuners should
inherit from.
"""

from time import strftime, time
import os
import pickle

class Tuner:
    def __init__(
        self, 
        search_space, 
        eval_fn, 
        maximize=True, 
        budget=500,
        log=True,
        log_dir=None,
        run_dir=None,
        run_name=None
    ):
        """Initializes a tuner (see sub-classes for explicit instantiations).
        
        args:
            search_space (dict): A dictionary of parameters to search over.
                See note below for more details.
            eval_fn: Given a configuration, evaluate the black box function and
                return the score.
            maximize (bool): Maximize the output of ``eval_fn`` if True,
                otherwise minimize it.
            budget (int): Maximum number of times to call the evaluation
                function.
            log (bool): Whether to log results
            log_dir (string): Directory to log all results to
            run_dir (string): Directory to log results from a set of runs
            run_name (string): Name of this run

        Example:
            search_space = {
                'param1': [0.0, 1.0, 2.0],          # discrete
                'param2': { 'range': (10.0, 20.0) } # linear range
            }
        """
        self.scores = []
        self.execution_times = []
        self.best_score = None
        self.best_config = None
        self.cost = 0
        self.search_space = search_space
        self.eval_fn = eval_fn
        self.maximize = maximize
        self.budget = budget
        self.log = log

        if self.log:
            # Logging subdirectory
            self.init_date = strftime("%Y_%m_%d")
            self.init_time = strftime("%H_%M_%S")
            self.log_dir = log_dir or os.getcwd()
            run_dir = run_dir or self.init_date
            run_name = run_name or self.init_time
            self.log_rootdir = os.path.join(self.log_dir, run_dir)
            self.log_subdir = os.path.join(self.log_dir, run_dir, run_name)

            if not os.path.exists(self.log_subdir):
                os.makedirs(self.log_subdir)

            self.save_path = os.path.join(self.log_subdir, 'best_config.pkl')
            self.report_path = os.path.join(self.log_subdir, 'tuner_report.pkl')
            self.log_path = os.path.join(self.log_subdir, 'log.txt')

    def evaluate_config(self, config):
        """Evaluate the config."""
        start = time()
        score = self.eval_fn(config)
        self.cost += 1
        self.scores.append(score)
        if (self.best_score is None or
            (self.maximize and score > self.best_score) or
            (not self.maximize and score < self.best_score)):
            self.best_score = score
            self.best_config = config
            if self.log:
                self.log_msg('New best score: {}, current cost: {}'.format(
                    score, self.cost))
        end = time()
        self.execution_times.append(end - start)

        return score

    def log_msg(self, msg):
        """Log something to the log file."""
        if self.log:
            with open(self.log_path, 'a') as f:
                f.write('{}\n'.format(msg))
                f.close()

    def tune(self, **kwargs):
        """Run the tuning algorithm!"""
        self.tune_impl(**kwargs)
        
        if self.log:
            with open(self.report_path, 'wb') as f:
                pickle.dump({
                    'best_score': self.best_score,
                    'best_config': self.best_config,
                    'scores': self.scores,
                    'execution_times': self.execution_times,
                    'cost': self.cost
                }, f)

        return (self.best_score, self.best_config, self.scores,
            self.execution_times, self.cost)

    def tune_impl(self, **kwargs):
        """The implementation of the tuning algorithm.

        Sub-classes should implement this!"""
        pass
