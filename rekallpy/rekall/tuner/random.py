"""This module defines a tuner that randomly searches the state space."""

from rekall.tuner import Tuner
import random
from tqdm import tqdm

class RandomTuner(Tuner):
    """This tuner randomly searches the state space."""
    @classmethod
    def generate_configs(cls, search_space, num_configs, seed = None):
        """Randomly generate ``num_configs`` configurations."""
        if seed is not None:
            random.seed(seed)

        configs = []
        for i in range(num_configs):
            config = {}
            for k in search_space:
                param = search_space[k]
                if isinstance(param, dict):
                    if 'range' in param:
                        minval, maxval = param['range']
                        config[k] = random.uniform(minval, maxval)
                    # elif 'subset' in param:
                    #     choices = param['subset']
                    #     config[k] = choices[:random.randint(1, len(param['subset']))]
                elif isinstance(param, list):
                    config[k] = random.choice(param)
            configs.append(config)

        return configs

    def tune_impl(self, **kwargs):
        """Randomly searches through the search space.
        
        args:
            seed (int): Random seed to initialize things.
        """

        if 'seed' in kwargs:
            seed = kwargs['seed']
        else:
            seed = None

        configs = RandomTuner.generate_configs(
            self.search_space,
            self.budget,
            seed = seed
        )

        for config in tqdm(configs):
            self.evaluate_config(config)
