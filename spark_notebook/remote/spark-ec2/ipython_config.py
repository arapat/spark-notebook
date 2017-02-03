from os.path import expanduser

c = get_config()
c.InteractiveShellApp.exec_files = [
    expanduser("~/s3helper.py"), expanduser("~/init_sc.py"), expanduser("~/init_s3.py")]
