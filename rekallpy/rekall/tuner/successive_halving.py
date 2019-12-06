"""This module performs successive halving."""

from rekall.tuner import Tuner
from rekall.tuner.random import RandomTuner

class SuccessiveHalvingTuner(Tuner):
    """This tuner does successive halving over the search space."""

    @classmethod
    def estimate_cost(cls, eta, N, K, T):
        '''Estimate the cost of successive halving'''
        cost = 0

        num_configs = K
        num_epochs = T

        for cur_round in range(N):
            cost += num_configs * num_epochs
            num_configs = int(num_configs / eta)
            num_epochs = int(num_epochs * eta)

        return cost
    
    def tune_impl(self, **kwargs):
        """
        Performs successive halving - start with K random configurations, each
        running for T iterations of some sub-tuner.
        In each round, take the 1 / eta top configurations, and in the next round
        train for eta times more iterations.
        
        args:
            eta: Halving ratio.
            N: Number of rounds.
            K: Initial number of configurations.
            T: Number of rounds.
            tuner: ``Tuner`` class to use for internal training rounds.
            tuner_params: Optional params to pass to the internal tuner.
        """
        if ('eta' not in kwargs or
            'N' not in kwargs or
            'K' not in kwargs or
            'T' not in kwargs or
            'tuner' not in kwargs):
            print('SuccessiveHalvingTuner requires eta, N, K, T, tuner params.')
            return
        
        eta = kwargs['eta']
        N = kwargs['N']
        K = kwargs['K']
        T = kwargs['T']
        tuner = kwargs['tuner']
        
        if 'tuner_params' in kwargs:
            tuner_params = kwargs['tuner_params']
        else:
            tuner_params = {}
           
        num_configs = K
        
        cur_configs = RandomTuner.generate_configs(self.search_space, num_configs)
        config_scores = []
        num_epochs = T
        
        for cur_round in range(N):
            self.log_msg('Round {}, {} configs, {} epochs'.format(cur_round, len(cur_configs), num_epochs))
            
            best_configs_and_scores = []
            for i, config in enumerate(cur_configs):
                if self.cost >= self.budget:
                    self.log_msg('Cost {} surpassed budget {}, ending rounds early.'.format(
                        self.cost, self.budget
                    ))
                    break
                
                cur_best_config = config
                if len(config_scores) <= i:
                    config_scores.append(self.evaluate_config(config))
                cur_best_score = config_scores[i]
                
                training_iterations = num_epochs if cur_round > 0 else num_epochs - 1
                
                new_tuner = tuner(
                    self.search_space,
                    self.eval_fn,
                    maximize=self.maximize,
                    budget=training_iterations,
                    log=False,
                    start_config=cur_best_config.copy(),
                    start_score=cur_best_score
                )
                
                (cur_best_score, cur_best_config, scores,
                     execution_times, cost) = new_tuner.tune(**tuner_params)
                
                self.scores += scores
                self.cost += cost
                self.execution_times += execution_times
                
                if (self.best_score is None or
                    (self.maximize and cur_best_score > self.best_score) or
                    (not self.maximize and cur_best_score < self.best_score)):
                    self.best_score = cur_best_score
                    self.best_config = cur_best_config
                    self.log_msg('New best score: {}, current cost: {}'.format(
                        cur_best_score, self.cost))

                best_configs_and_scores.append((cur_best_score, cur_best_config))

            best_configs_and_scores = sorted(best_configs_and_scores, key=lambda score_and_config: score_and_config[0])

            num_configs = int(num_configs / eta)
            num_epochs = int(num_epochs * eta)

            if num_configs < 1:
                num_configs = 1
            cur_configs = [ config for score, config in best_configs_and_scores[:num_configs] ]
            config_scores = [ score for score, config in best_configs_and_scores[:num_configs] ]
