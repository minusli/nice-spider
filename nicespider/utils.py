def retry(count=3):
    def retry_(f):
        def wrap(*args, **kwargs):
            count_ = count
            error = None
            while count_ > 0:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    error = e
                    count_ -= 1
            raise error

        return wrap

    return retry_
