"""This module defines a tuner that performs a grid search over the state space."""

from rekall.tuner import Tuner
from tqdm import tqdm
from itertools import product, combinations
import numpy as np

class GridTuner(Tuner):
    """This tuner conducts a grid search over the search space."""
    @classmethod
    def generate_configs(cls, search_space, budget):
        """Generate configs that cover the search space in a grid as densely as
        possible given the budget."""
        configs = []

        param_choices = {}
        def cross_product_size():
            cp_size = 1
            for k in param_choices:
                cp_size *= len(param_choices[k])
            return cp_size

        # Add in all discrete variables
        non_discrete_variables = []
        for k in search_space:
            param = search_space[k]
            if isinstance(param, list):
                param_choices[k] = param
            else:
                non_discrete_variables.append(k)

        while cross_product_size() < budget:
            for k in non_discrete_variables:
                if cross_product_size() > budget:
                    break

                param = search_space[k]

                # Add an extra choice for this variable
                if k in param_choices:
                    cur_len = len(param_choices[k])
                else:
                    cur_len = 0

                new_len = cur_len + 1.

                if 'range' in param:
                    minval = param['range'][0]
                    maxval = param['range'][1]

                    step = (maxval - minval) / (new_len + 1)

                    param_choices[k] = list(np.arange(minval, maxval, step)[1:])
                # elif 'subset' in param:
                #     set_choices = param['subset']

                #     def max_combinations(set_choices):
                #         import math

                #         def nCr(n,r):
                #             f = math.factorial
                #             return f(n) / f(r) / f(n-r)

                #         num_choices = len(set_choices)
                #         total_combinations = 0
                #         for i in range(1, num_choices + 1):
                #             total_combinations += nCr(num_choices, i)

                #         return total_combinations

                #     if max_combinations(set_choices) < new_len:
                #         continue

                #     param_choices[k] = []
                #     for subset_len in range(len(set_choices), 0, -1):
                #         subsets = combinations(set_choices, subset_len)
                #         param_choices[k] += subsets

                #         if len(param_choices[k]) >= new_len:
                #             param_choices[k] = param_choices[k][:new_len]
                #             break

        def dict_product(d):
            keys = d.keys()
            for element in product(*d.values()):
                yield dict(zip(keys, element))

        configs = dict_product(param_choices)

        return list(configs)[:budget]

    def tune_impl(self, **kwargs):
        """Performs a grid search through the search space."""

        configs = GridTuner.generate_configs(self.search_space, self.budget)

        for config in tqdm(configs):
            self.evaluate_config(config)
