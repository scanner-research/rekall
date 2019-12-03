"""
NOT FUNCTIONAL YET. DO NOT USE.

This module uses scipy's implementation of the L-BFGS-B optimization
method for searching through bounded search spaces.

Reference: https://docs.scipy.org/doc/scipy/reference/optimize.minimize-lbfgsb.html#optimize-minimize-lbfgsb"""

from scipy import optimize
from rekall.tuner import Tuner

class ScipyLBFGSBTuner(Tuner):
    
    def tune_impl(self, **kwargs):
        """Call scipy's L-BFGS-B optimizer for bounded optimization.
        Currently does not work with discrete variables!
        
        Reference: https://docs.scipy.org/doc/scipy/reference/optimize.minimize-lbfgsb.html#optimize-minimize-lbfgsb
        
        args in kwargs:
            init_method: How to initialize the first config.
                One of ['average', 'random'].
                If not specified, default to 'average'.
                'average' initializes the config at the average of continuous ranges,
                'random' randomly initializes the config.
                If start_config was specified upon initialization, use that value always.
        """
        
        if 'init_method' in kwargs:
            init_method = kwargs['init_method']
        else:
            init_method = 'average'
        
        if self.start_config is not None:
            config = self.start_config
        elif init_method is 'average':
            coordinates = list(self.search_space.keys())
            
            config = {}

            # Initialize the config
            for coordinate in coordinates:
                param = self.search_space[coordinate]
                if isinstance(param, dict):
                    if 'range' in param:
                        minval, maxval = param['range']
                        config[coordinate] = (maxval + minval) / 2
        #             elif 'subset' in param:
        #                 choices = param['subset']
        #                 config[k] = choices[:random.randint(1, len(param['subset']))]
                elif isinstance(param, list):
                    config[k] = param[0]
        elif init_method is 'random':
            config = RandomTuner.generate_configs(self.search_space, 1)[0]
        else:
            print('{} is invalid init_method!'.format(init_method))
            return
        
        sorted_vars = sorted(list(config.keys()))
        
        def config_to_array(config):
            return [
                config[var]
                for var in sorted_vars
            ]
        
        def array_to_config(arr):
            return {
                var: value
                for var, value in zip(sorted_vars, arr)
            }
        
        def optimization_function(arr):
            config = array_to_config(arr)
            score = self.evaluate_config(config)
            if self.maximize:
                score = -1 * score
            return score
        
        bounds = [
            self.search_space[var]['range']
            for var in sorted_vars
        ]
        
        optimize.minimize(
            optimization_function,
            config_to_array(config),
            method = 'L-BFGS-B',
            bounds = bounds,
            options = {
                'maxfun': self.budget
            }
        )
