import os
from werkzeug.utils import secure_filename
from flask import current_app

def file_to_pkl(data, filename, parse_function=None, pklname=None):
    filename = secure_filename(filename)
    path = os.path.join(current_app.config['UPLOAD_DIR'], filename)
    data.save(path)
    if pklname:
        df = parse_function(path)
        pkl_path = os.path.join(
                current_app.config['PKL_DIR'], pklname)
        df.to_pickle(pkl_path)
