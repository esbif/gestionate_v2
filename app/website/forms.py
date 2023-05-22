from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileRequired
from wtforms import SubmitField, SelectField, BooleanField, TextAreaField
from wtforms import DateField
from wtforms.validators import DataRequired, Length

from werkzeug.utils import secure_filename

class ReportForm(FlaskForm):
    
    op_file = FileField('Operativos', validators=[FileRequired()])
    non_op_file = FileField('No operativos', validators=[FileRequired()])
    remove_vsats = TextAreaField(
            'Remove IDs', validators=[Length(min=0, max=500)])
    accept_concurrency = BooleanField('Accept concurrency')
    submit = SubmitField('Submit')

class FileForm(FlaskForm):
    
    file = FileField('File', validators=[FileRequired()])
    submit = SubmitField('Load')
