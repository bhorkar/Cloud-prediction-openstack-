import errno    
import os


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
def verifyconfig(config):
	if config['debug']['debug_file_flag'] == 1:
		assert config['rabbitmq_input']['type'] == 'file', "Debugging only possible in file mode";
