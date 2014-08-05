import MethodThreader
import unittest


def jobOne(value=None):
    """
    Helper function to return a param
    """
    return value

def bigRedButton():
    """
    Helper function to raise a ValueError
    """
    raise ValueError("KABOOM")

def smallRedButton():
    """
    Helper function to raise an IOError
    """
    raise IOError("BOOM")

def raiseIt(exception):
    """
    Helper function to raise an exception by passing the type
    """
    raise exception


class test_MethodThreader(unittest.TestCase):

    def test_noJobs(self):
        """
        if we pass in an empty TaskList it should return an empty list
        """
        task_list = MethodThreader.TaskList()
        threader = MethodThreader.MethodThreader(task_list=task_list)
        result = threader.run()
        self.assertListEqual(result, [])

    def test_oneGoodJob(self):
        """
        if we pass in a single TaskList it should return
        """
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {'value':'one'})
        threader = MethodThreader.MethodThreader(task_list=task_list)
        result = threader.run()

        #Verify return elements
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].error)
        self.assertIsNone(result[0].name)
        self.assertIsNone(result[0].exception)
        self.assertEqual(result[0].result, 'one')
        self.assertEqual(result[0].exec_method_name, 'jobOne')

        #test again but with name
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {'value':'one'}, name='GoodJob1')
        threader = MethodThreader.MethodThreader(task_list=task_list)
        result = threader.run()

        #Verify return elements
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].error)
        self.assertIsNone(result[0].exception)
        self.assertEqual(result[0].name, 'GoodJob1')
        self.assertEqual(result[0].result, 'one')
        self.assertEqual(result[0].exec_method_name, 'jobOne')

        #test again but with raise_on_error flag disabled.  Should be identical
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {'value':'one'})
        threader = MethodThreader.MethodThreader(task_list=task_list, raise_on_error=False)
        result = threader.run()

        #Verify return elements
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].error)
        self.assertIsNone(result[0].name)
        self.assertIsNone(result[0].exception)
        self.assertEqual(result[0].result, 'one')
        self.assertEqual(result[0].exec_method_name, 'jobOne')

    def test_oneBadJob(self):
        """
        A single job that raises an exception
        """
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(bigRedButton, {}, name='NukeIt')
        threader = MethodThreader.MethodThreader(task_list=task_list)
        self.assertRaises(ValueError, threader.run)

        #Same test but with raise_on_error flag disabled
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(bigRedButton, {}, name='NukeIt')
        threader = MethodThreader.MethodThreader(task_list=task_list, raise_on_error=False)
        result = threader.run()

        #Verify payload
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].error)
        self.assertEqual(result[0].name, 'NukeIt')
        self.assertIsNotNone(result[0].exception)
        self.assertRaises(ValueError, raiseIt, result[0].exception[0])
        self.assertIsNone(result[0].result)
        self.assertEqual(result[0].exec_method_name, 'bigRedButton')

    def test_fiveJobs(self):
        """
        testing multiple jobs in success and fail scenarios
        """
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {}, name='1')
        task_list.addTaskToList(jobOne, {'value':1}, name='2')
        task_list.addTaskToList(jobOne, {'value': {'embedded' : 'dict'} }, name='3')
        test_obj = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {'value': test_obj}, name='4')
        task_list.addTaskToList(jobOne, {'value': 'one'}, name='5')
        threader = MethodThreader.MethodThreader(task_list=task_list)
        result = threader.run()

        #Verify the return payload of varying successful jobs
        self.assertEqual(len(result), 5)

        self.assertFalse(result[0].error)
        self.assertIsNone(result[0].exception)
        self.assertEqual(result[0].exec_method_name, 'jobOne')
        self.assertEqual(result[0].name, '1')
        self.assertIsNone(result[0].result)

        self.assertFalse(result[1].error)
        self.assertIsNone(result[1].exception)
        self.assertEqual(result[1].exec_method_name, 'jobOne')
        self.assertEqual(result[1].name, '2')
        self.assertEqual(result[1].result, 1)

        self.assertFalse(result[2].error)
        self.assertIsNone(result[2].exception)
        self.assertEqual(result[2].exec_method_name, 'jobOne')
        self.assertEqual(result[2].name, '3')
        self.assertEqual(result[2].result, {'embedded': 'dict'})

        self.assertFalse(result[3].error)
        self.assertIsNone(result[3].exception)
        self.assertEqual(result[3].exec_method_name, 'jobOne')
        self.assertEqual(result[3].name, '4')
        self.assertEqual(result[3].result, test_obj)

        self.assertFalse(result[4].error)
        self.assertIsNone(result[4].exception)
        self.assertEqual(result[4].exec_method_name, 'jobOne')
        self.assertEqual(result[4].name, '5')
        self.assertEqual(result[4].result, 'one')

        #Same test with raise_on_error disabled.  Should be identical
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {}, name='1')
        task_list.addTaskToList(jobOne, {'value':1}, name='2')
        task_list.addTaskToList(jobOne, {'value': {'embedded' : 'dict'} }, name='3')
        test_obj = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {'value': test_obj}, name='4')
        task_list.addTaskToList(jobOne, {'value': 'one'}, name='5')
        threader = MethodThreader.MethodThreader(task_list=task_list)
        result = threader.run()

        #Verify the return payload of varying successful jobs
        self.assertEqual(len(result), 5)

        self.assertFalse(result[0].error)
        self.assertIsNone(result[0].exception)
        self.assertEqual(result[0].exec_method_name, 'jobOne')
        self.assertEqual(result[0].name, '1')
        self.assertIsNone(result[0].result)

        self.assertFalse(result[1].error)
        self.assertIsNone(result[1].exception)
        self.assertEqual(result[1].exec_method_name, 'jobOne')
        self.assertEqual(result[1].name, '2')
        self.assertEqual(result[1].result, 1)

        self.assertFalse(result[2].error)
        self.assertIsNone(result[2].exception)
        self.assertEqual(result[2].exec_method_name, 'jobOne')
        self.assertEqual(result[2].name, '3')
        self.assertEqual(result[2].result, {'embedded': 'dict'})

        self.assertFalse(result[3].error)
        self.assertIsNone(result[3].exception)
        self.assertEqual(result[3].exec_method_name, 'jobOne')
        self.assertEqual(result[3].name, '4')
        self.assertEqual(result[3].result, test_obj)

        self.assertFalse(result[4].error)
        self.assertIsNone(result[4].exception)
        self.assertEqual(result[4].exec_method_name, 'jobOne')
        self.assertEqual(result[4].name, '5')
        self.assertEqual(result[4].result, 'one')

        #Test with multiple fails with error
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {}, name='1')
        task_list.addTaskToList(jobOne, {'value':1}, name='2')
        task_list.addTaskToList(bigRedButton, {}, name='3')
        task_list.addTaskToList(jobOne, {'value': 2}, name='4')
        task_list.addTaskToList(smallRedButton, {}, name='5')
        threader = MethodThreader.MethodThreader(task_list=task_list)

        self.assertRaises(ValueError, threader.run)

        #Test with multiple fails without error
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {}, name='1')
        task_list.addTaskToList(jobOne, {'value':1}, name='2')
        task_list.addTaskToList(bigRedButton, {}, name='3')
        task_list.addTaskToList(jobOne, {'value': 2}, name='4')
        task_list.addTaskToList(smallRedButton, {}, name='5')
        threader = MethodThreader.MethodThreader(task_list=task_list, raise_on_error=False)
        result = threader.run()

        #Verify the return payload of varying jobs
        self.assertEqual(len(result), 5)

        self.assertFalse(result[0].error)
        self.assertIsNone(result[0].exception)
        self.assertEqual(result[0].exec_method_name, 'jobOne')
        self.assertEqual(result[0].name, '1')
        self.assertIsNone(result[0].result)

        self.assertFalse(result[1].error)
        self.assertIsNone(result[1].exception)
        self.assertEqual(result[1].exec_method_name, 'jobOne')
        self.assertEqual(result[1].name, '2')
        self.assertEqual(result[1].result, 1)

        self.assertTrue(result[2].error)
        self.assertIsNotNone(result[2].exception)
        self.assertRaises(ValueError, raiseIt, result[2].exception[0])
        self.assertEqual(result[2].exec_method_name, 'bigRedButton')
        self.assertEqual(result[2].name, '3')
        self.assertIsNone(result[2].result)

        self.assertFalse(result[3].error)
        self.assertIsNone(result[3].exception)
        self.assertEqual(result[3].exec_method_name, 'jobOne')
        self.assertEqual(result[3].name, '4')
        self.assertEqual(result[3].result, 2)

        self.assertTrue(result[4].error)
        self.assertIsNotNone(result[4].exception)
        self.assertRaises(IOError, raiseIt, result[4].exception[0])
        self.assertEqual(result[4].exec_method_name, 'smallRedButton')
        self.assertEqual(result[4].name, '5')
        self.assertIsNone(result[4].result)

    def test_threadNum(self):
        """
        test valid and invalid thread numbers
        """
        #test queue size on None thread
        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {}, name='1')
        task_list.addTaskToList(jobOne, {}, name='2')
        task_list.addTaskToList(jobOne, {}, name='3')
        task_list.addTaskToList(jobOne, {}, name='4')
        task_list.addTaskToList(jobOne, {}, name='5')

        #test error on less than zero
        self.assertRaises(ValueError, MethodThreader.MethodThreader, task_list=task_list, threads=-1)

        #test 5 on None and five inputs
        threader = MethodThreader.MethodThreader(task_list=task_list)
        self.assertEqual(threader._getThreadNum(None), 5)

        #test 3 on 3 and five inputs
        threader = MethodThreader.MethodThreader(task_list=task_list, threads=3)
        self.assertEqual(threader._getThreadNum(3), 3)

        task_list = MethodThreader.TaskList()
        task_list.addTaskToList(jobOne, {}, name='1')

        #test 1 on None and one input
        threader = MethodThreader.MethodThreader(task_list=task_list)
        self.assertEqual(threader._getThreadNum(None), 1)

        #test 1 on 3 and one input
        threader = MethodThreader.MethodThreader(task_list=task_list)
        self.assertEqual(threader._getThreadNum(3), 1)






if __name__ == '__main__':
    unittest.main()