"""This module performs a hyperband search over the search space."""

import math
from rekall.tuner import Tuner
from rekall.tuner.random import RandomTuner
from rekall.tuner.successive_halving import SuccessiveHalvingTuner

class HyperbandTuner(Tuner):
    """This tuner performs a hyperband search over the search space.
    
    See https://arxiv.org/abs/1603.06560.
    """    
    @classmethod
    def finite_horizon_hyperband_schedule(cls, max_iter, eta):
        logeta = lambda x: math.log(x) / math.log(eta)
        s_max = int(logeta(max_iter))
        B = (s_max + 1) * max_iter

        hyperband_schedule = []

        for s in reversed(range(s_max + 1)):
            n = int(math.ceil(int(B/max_iter/(s+1))*eta**s))
            r = max_iter*eta**(-s)

            # n is the number of configurations to start with
            # r is the number of iterations to start with
            # number of rounds is ceiling(logeta(n+.01))

            hyperband_schedule.append({
                'K': int(n),
                'eta': int(eta),
                'T': int(r),
                'N': math.ceil(logeta(n+.01))
            })

        return hyperband_schedule
    
    @classmethod
    def estimate_cost(cls, schedule):
        cost = 0
    
        for bracket in schedule:
            cost += SuccessiveHalvingTuner.estimate_cost(
                bracket['eta'], bracket['N'], bracket['K'], bracket['T']
            )

        return cost
    
    def tune_impl(self, **kwargs):
        """
        Implement hyperband search over parameter space, with a given tuner to
        train iterations.
        
        See ``finite_horizon_hyperband_schedule`` to print out the schedule for
        given values of `max_iter` and `eta`.
        
        args:
            max_iter: Maximum number of iterations.
            eta: Proportion of configs to cut in each round of successive halving.
            tuner: ``Tuner`` class to use for internal training rounds.
            tuner_params: Optional params to pass to the internal tuner.
        """
        if ('eta' not in kwargs or
            'max_iter' not in kwargs or
            'tuner' not in kwargs):
            print('SuccessiveHalvingTuner requires max_itera, eta, tuner params.')
            return
        
        eta = kwargs['eta']
        max_iter = kwargs['max_iter']
        tuner = kwargs['tuner']
        
        if 'tuner_params' in kwargs:
            tuner_params = kwargs['tuner_params']
        else:
            tuner_params = {}
        
        schedule = HyperbandTuner.finite_horizon_hyperband_schedule(max_iter, eta)
        
        for bracket in schedule:
            self.log_msg('Bracket: {}'.format(bracket))
            if self.cost >= self.budget:
                self.log_msg('Cost {} surpassed budget {}, ending rounds early.'.format(
                    self.cost, self.budget
                ))
                break
            
            bracket_cost = SuccessiveHalvingTuner.estimate_cost(
                bracket['eta'], bracket['N'], bracket['K'], bracket['T']
            )
            successive_halving_tuner = SuccessiveHalvingTuner(
                self.search_space, self.eval_fn, maximize=self.maximize,
                budget = bracket_cost, log=False
            )
            
            (best_score_bracket, best_config_bracket, scores,
                execution_times, cost) = successive_halving_tuner.tune(
                eta = bracket['eta'], N = bracket['N'], K = bracket['K'], T = bracket['T'],
                tuner = tuner, tuner_params = tuner_params
            )
            
            self.scores += scores
            self.cost += cost
            self.execution_times += execution_times

            if (self.best_score is None or
                (self.maximize and best_score_bracket > self.best_score) or
                (not self.maximize and best_score_bracket < self.best_score)):
                self.best_score = best_score_bracket
                self.best_config = best_config_bracket
                self.log_msg('New best score: {}, current cost: {}'.format(
                    best_score_bracket, self.cost))
