from os.path import expanduser

c = get_config()
c.NotebookApp.ip = '*'
c.NotebookApp.open_browser = False
c.NotebookApp.notebook_dir = expanduser('~/workspace')
# c.NotebookApp.password = u'<your_password>'
