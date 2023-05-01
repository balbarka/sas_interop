import subprocess
import sys
from IPython.core.magic import register_line_magic, register_cell_magic, register_line_cell_magic
from IPython.display import HTML 

# Install saspy
subprocess.check_call([sys.executable, "-m", "pip", "install", "saspy"])

# assign path variable to our eventual personal config
import saspy
sascfg_personal_path = saspy.__file__.replace('__init__.py', 'sascfg_personal.py')

# write config file
sas_config_dict = {'url':  spark.conf.get("spark.sas_url"),
                   'user': spark.conf.get("spark.sas_user"),
                   'pw':   spark.conf.get("spark.sas_pwd"),
                   'context': 'SAS Job Execution compute context'}
sascfg_personal = \
f'''SAS_config_names   = ['hlssaspaygo']
hlssaspaygo = {str(sas_config_dict)}'''
with open(sascfg_personal_path,'w') as f:
    f.write(sascfg_personal)

# instantiate sas_magic connection
sas_magic = saspy.SASsession(results='HTML', context='SAS Job Execution compute context')

#TODO: create class to not instantiate until first call to magic

@register_line_cell_magic
def SAS(line, cell):
    args = [a.lower() for a in line.split(' ')]
    show_LST = False if 'log' in args and 'lst' not in args else True
    show_LOG = True if 'log' in args else False
    c = sas_magic.submit(cell)
    if show_LOG:
        print(c['LOG'])
    if show_LST:
        return HTML(c['LST'])
    else:
        return None

@register_line_magic
def SAS_file(line):
    args = [a.lower() for a in line.split(' ')]
    show_LST = False if 'log' in args and 'lst' not in args else True
    show_LOG = True if 'log' in args else False
    with open(args[0],'r') as f:
        c = sas_magic.submit(f.read())
    if show_LOG:
        print(c['LOG'])
    if show_LST:
        return HTML(c['LST'])
    else:
        return None
