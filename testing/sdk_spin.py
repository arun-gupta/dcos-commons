'''Utilities relating to waiting for an operation to complete'''

import shakedown

import time
import traceback

DEFAULT_TIMEOUT=15 * 60

#TODO(nickbp): Upstream into shakedown's time_wait()


def time_wait_return(predicate, timeout_seconds=DEFAULT_TIMEOUT, ignore_exceptions=True):
    '''Wrapper of shakedown's spinner which returns the first value that doesn't evaluate as falsy'''
    ret = None
    def wrapper():
        nonlocal ret
        try:
            result = predicate()
            if result:
                ret = result
                return True
            else:
                return False
        except Exception as e:
            if ignore_exceptions:
                traceback.print_exc()
            else:
                raise
    time_wait_noisy(
        lambda: wrapper(), timeout_seconds=timeout_seconds, ignore_exceptions=ignore_exceptions)
    return ret


def time_wait_noisy(predicate, timeout_seconds=DEFAULT_TIMEOUT, ignore_exceptions=True):
    '''Wrapper of shakedown's spinner which logs the duration of the spin'''
    start = time.time()
    def wrapper():
        try:
            result = predicate()
        except Exception as e:
            if ignore_exceptions:
                traceback.print_exc()
                return False
            else:
                raise
        if not result:
            print('[{}/{}] Waiting...'.format(
                pretty_time(time.time() - start),
                pretty_time(timeout_seconds)))
        return result
    # we perform our own custom handling of exceptions, disable the underlying version:
    duration = shakedown.time_wait(
        lambda: wrapper(), timeout_seconds=timeout_seconds, ignore_exceptions=False)


def pretty_time(seconds):
    ret = ''
    if seconds >= 86400:
        ret += '{:.0f}d'.format(seconds / 86400)
        seconds = seconds % 86400
    if seconds >= 3600:
        ret += '{:.0f}h'.format(seconds / 3600)
        seconds = seconds % 3600
    if seconds >= 60:
        ret += '{:.0f}m'.format(seconds / 60)
        seconds = seconds % 60
    if seconds > 0:
        ret += '{:.1f}s'.format(seconds)
    return ret


def spin(fn, success_predicate, *args, **kwargs):
    now = time.time()
    end_time = now + DEFAULT_TIMEOUT
    while now < end_time:
        print('{}: [ {} left ]'.format(pretty_time(now), pretty_time(end_time - now)))
        result = fn(*args, **kwargs)
        is_successful, error_message = success_predicate(result)
        if is_successful:
            print('Success state reached, exiting spin.')
            break
        print('Waiting for success state... err={}'.format(error_message))
        time.sleep(1)
        now = time.time()

    assert is_successful, error_message
    return result