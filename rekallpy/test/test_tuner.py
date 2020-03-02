from rekall.tuner import RandomTuner
import unittest

class TestTuner(unittest.TestCase):
    def test_tuner_runs(self):
        def eval_config(params):
            return 0.5

        search_space = {
            'param1': [0.0, 1.0, 2.0],          # discrete
            'param2': { 'range': (10.0, 20.0) } # linear range
        }

        tuner = RandomTuner(search_space, eval_config, budget = 50)
