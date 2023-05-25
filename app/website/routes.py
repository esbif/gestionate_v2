import os
from datetime import datetime
import pandas as pd
from flask import render_template, current_app, flash, redirect, url_for
from flask import send_file
from flask_login import login_required

from app import db
from app.website import bp
from app.website.forms import ReportForm, FileForm
import app.functions as functions
import app.utils as utils


@bp.route('/', methods=['GET', 'POST'])
@login_required
def index():

    form = ReportForm()
    if form.validate_on_submit():

        # Read input files.
        utils.file_to_pkl(form.op_file.data, 'op.xlsx')
        op_file_path = os.path.join(
                current_app.config['UPLOAD_DIR'], 'op.xlsx')
        utils.file_to_pkl(form.non_op_file.data, 'nop.xlsx')
        nop_file_path = os.path.join(
                current_app.config['UPLOAD_DIR'], 'nop.xlsx')
        tmp_test_data = functions.tests_df_from_excel(
                op_file_path, nop_file_path)

        # Filter locations.
        locations = pd.read_pickle(os.path.join(
                current_app.config['PKL_DIR'], 'locations.pkl'))
        test_data = functions.filter_locations(tmp_test_data, locations)

        # Evaluate tests.
        evaluated = functions.eval_tests(test_data)
        evaluated['date'] = evaluated['timestamp'].dt.date

        # Remove concurrent tests.
        if not form.accept_concurrency.data:
            evaluated = evaluated[evaluated['concurrent_tests']<1]

        # Remove unwanted sites.
        remove_list = None
        if form.remove_vsats.data != "":
            try:
                remove_vsats = [
                        int(x) for x in form.remove_vsats.data.split(',')]
                remove_list = remove_vsats
                evaluated = functions.remove_locations(
                        evaluated, remove_vsats)
            except Exception as e:
                flash('Wrong remove list format. No VSATs removed.')

        # Build report with remaining tests.
        report_path = os.path.join(
                current_app.config['REPORT_DIR'], 'report.xlsx')
        tickets = pd.read_pickle(os.path.join(
                current_app.config['PKL_DIR'], 'tickets.pkl'))
        report_path = functions.build_report(
                evaluated, tickets, locations, report_path, remove_list)

        return send_file(report_path, as_attachment=True)

    data_info = {}

    # Get locations file info.
    loc_path = os.path.join(
            current_app.config['PKL_DIR'], 'locations.pkl')
    locations = pd.read_pickle(loc_path)
    data_info.update({'loc_qty': locations['site_code'].unique().size})
    data_info.update({'loc_last_mod': datetime.fromtimestamp(
            os.path.getctime(loc_path))})

    # Get tickets file info.
    tkt_path = os.path.join(current_app.config['PKL_DIR'], 'tickets.pkl')
    tmp_tickets = pd.read_pickle(tkt_path)
    tickets = functions.filter_tickets(
            tmp_tickets, locations, datetime.min, datetime.now())
    data_info.update({'tickets_qty': len(tickets.index)})
    data_info.update({'first_ticket': tickets['start'].min()})
    data_info.update({'last_ticket': tickets['start'].max()})
    data_info.update({'tkt_last_mod': datetime.fromtimestamp(
            os.path.getctime(tkt_path))})

    return render_template('website/index.html', 
            data_info=data_info, form=form, title='Index')


@bp.route('/load_tickets', methods=['GET', 'POST'])
@login_required
def load_tickets():
    form = FileForm()

    if form.validate_on_submit():
        utils.file_to_pkl(
                form.file.data, 'tickets.xlsx', 
                functions.tickets_df_from_excel,'tickets.pkl')
        flash('Tickets loaded.')
        return redirect(url_for('website.index'))

    return render_template(
            'website/generic_form.html', title='Index', form=form)


@bp.route('/load_locations', methods=['GET', 'POST'])
@login_required
def load_locations():
    form = FileForm()
    if form.validate_on_submit():
        utils.file_to_pkl(
                form.file.data, 'locations.xlsx', 
                functions.locations_df_from_excel, 'locations.pkl')
        flash('Locations loaded.')
        return redirect(url_for('website.index'))

    return render_template(
            'website/generic_form.html', title='Index', form=form)
