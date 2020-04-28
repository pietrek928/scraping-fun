from IPython import get_ipython

if __name__ == '__main__':
    ipython = get_ipython()
    ipython.magic('load_ext autoreload')
    ipython.magic('autoreload 2')
