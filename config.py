import os
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'fuchicaca'
    
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        'sqlite:///' + os.path.join(basedir, 'app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    UPLOAD_DIR = os.path.join(basedir, 'app', 'uploads')
    PKL_DIR = os.path.join(basedir, 'app', 'data_sets')
    REPORT_DIR = os.path.join(basedir, 'app', 'reports')
