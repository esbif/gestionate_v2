import pandas as pd
import numpy as np
from datetime import timedelta, datetime


LOCATIONS_KEY_MAPPING = {
        "Sitio (código)": "site_code",
        "ID Beneficiario": "ben_id"}


TESTS_KEY_MAPPING = {
        "Ubicación": "site",
        "BW Bajada Esperado": "exp_dn_br",
        "BW Bajada Encontrado": "dn_br",
        "BW Subida Esperado": "exp_up_br",
        "BW Subida Encontrado": "up_br",
        "Resultado": "res",
        "Fecha de la Prueba": "timestamp",
        "Hora de la Prueba": "hour",
        "Perfil de Velocidad": "profile",
        "Tipo de prueba": "type",
        "Tipo de Prueba": "type",
        "Error": "error",
        "origin_file": "origin_file"}


TICKETS_KEY_MAPPING = {
        "ID_BENEFICIARIO": "ben_id",
        "ID_MINTIC": "site_code",
        "FECHA_HORA_DE_APERTURA": "start",
        "FECHA_HORA_DE_RESOLUCION": "end"}

def log(message):

    print(message)


def clean_data(test_data):
    clean = pd.DataFrame()
    for key, value in TESTS_KEY_MAPPING.items():
        if key not in test_data.keys():
            continue
        clean[value] = test_data[key]

    # Parse timestamp column as datetime. 
    clean["timestamp"] = pd.to_datetime(clean["timestamp"],
            format="%Y-%m-%d %H:%M:%S.%f")

    # Remove duplicate tests.
    clean.drop_duplicates(
            ['site', 'dn_br', 'up_br', 'timestamp', 'hour'])

    return clean


def tickets_df_from_excel(tickets_filename):

    tickets_data = pd.read_excel(
            tickets_filename, sheet_name=0, header=0, parse_dates=True)

    clean = pd.DataFrame()
    for key, value in TICKETS_KEY_MAPPING.items():
        if key not in tickets_data.keys():
            continue

        clean[value] = tickets_data[key]

    clean['start'] = pd.to_datetime(clean['start'])
    clean['end'] = pd.to_datetime(clean['end'])

    return clean 


def filter_tickets(tickets_data, location_data, start_date, end_date):

    sites = location_data["ben_id"]
    sites.drop_duplicates()
    tickets = tickets_data[tickets_data["ben_id"].isin(sites)]
    tickets['end'] = tickets['end'].fillna(end_date)
    tickets = tickets[tickets["start"].dt.date >= start_date.date()]
    tickets = tickets[tickets["start"].dt.date <= end_date.date()]

    return tickets


def tests_df_from_excel(op_sites_filename, non_op_sites_filename):

    # Read "operativos" and set origin_file column.
    op_data = pd.read_excel(
            op_sites_filename, sheet_name = "ReportSheet",
            header=1, parse_dates=True)
    op_data["origin_file"] = "operativos"
    op_data = clean_data(op_data)

    # Read "no operativos" and set origin_file column.
    non_op_data = pd.read_excel(
            non_op_sites_filename, sheet_name = "ReportSheet",
            header=1, parse_dates=True)
    non_op_data["origin_file"] = "no operativos"
    non_op_data = clean_data(non_op_data)

    # Concat "operativos" and "no operativos".
    test_data = pd.concat([op_data, non_op_data],
            ignore_index = True, axis=0)
    test_data.reset_index()

    return test_data


def locations_df_from_excel(locations_filename):

    locations = pd.read_excel(
            locations_filename, sheet_name = "Sheet1",
            header=0, parse_dates=True)
    clean = pd.DataFrame()
    for key, value in LOCATIONS_KEY_MAPPING.items():
        clean[value] = locations[key]

    return clean 


def filter_locations(test_data, location_data):

    sites = location_data["ben_id"]
    sites = sites.drop_duplicates()

    # Remove unused data from site id. "Locations" only includes the 
    # location bit in the site id. 
    test_data["clean_loc"] = test_data.apply(
            lambda row: int(row.site.split("-")[0]), axis=1)

    filtered = test_data[test_data["clean_loc"].isin(sites)]

    expanded = pd.merge(filtered, location_data, how="left", 
            left_on="clean_loc", right_on="ben_id")

    return expanded.drop(columns=["clean_loc"])


def eval_tests(test_data, pickle_name=None, avg_test_duration=60):

    def eval_func(row, direction):
        expected = getattr(row, "exp_"+direction+"_br")
        found = getattr(row, direction+"_br")
        if expected > found:
            return False 
        return True

    # Use eval_func to check tests.
    test_data["dn_pass"] = test_data.apply(
            lambda row: eval_func(row, "dn"), axis=1)
    test_data["up_pass"] = test_data.apply(
            lambda row: eval_func(row, "up"), axis=1)

    test_data["pass"] = test_data.dn_pass & test_data.up_pass

    def concurrency(row):
        if row["res"] == "succeeded":
            start = row["timestamp"] - timedelta(
                    seconds=avg_test_duration)
            end = row["timestamp"] + timedelta(
                    seconds=avg_test_duration)
            mask = ((test_data["timestamp"] >= start) & 
                    (test_data["timestamp"] <= end))
            tmp_df = test_data.loc[mask]
            tmp_df = tmp_df[tmp_df["res"] != "failed"]
            concurrent_tests = tmp_df["site"].size - 1
            return concurrent_tests
        
    test_data["concurrent_tests"] = test_data.apply(
            lambda row: concurrency(row), axis=1)

    if pickle_name:
        pickle_path = "data_sets/" + pickle_name + ".pkl"
        test_data.to_pickle(pickle_path)
        log("data saved in " + pickle_path)

    return test_data


def summary(evaluated_tests):
    
    succeeded = evaluated_tests[evaluated_tests["res"] == "succeeded"]
    failed = evaluated_tests[evaluated_tests["res"] == "failed"]

    passed = succeeded[succeeded["pass"] == True]
    non_passed = succeeded[succeeded["pass"] == False]

    dn_non_passed = non_passed[
            (non_passed["dn_pass"] == False) & 
            (non_passed["up_pass"] == True)]
    up_non_passed = non_passed[
            (non_passed["dn_pass"] == True) & 
            (non_passed["up_pass"] == False)]
    both_non_passed = non_passed[
            (non_passed["dn_pass"] == False) & 
            (non_passed["up_pass"] == False)]

    dn_nth_value = succeeded["dn_br"].quantile(
            0.05, interpolation="higher")
    up_nth_value = succeeded["up_br"].quantile(
            0.05, interpolation="higher")

    dn_mean = succeeded["dn_br"].mean()
    up_mean = succeeded["up_br"].mean()

    concurrent = evaluated_tests[
            evaluated_tests["concurrent_tests"] >= 1]
            
    return pd.Series({
            "total": evaluated_tests["site"].size,
            "succeeded": succeeded["site"].size,
            "failed": failed["site"].size,
            "passed": passed["site"].size,
            "non_passed": non_passed["site"].size,
            "up_non_passed": up_non_passed["site"].size,
            "dn_non_passed": dn_non_passed["site"].size,
            "both_non_passed": both_non_passed["site"].size,
            "dn_nth_value": dn_nth_value,
            "up_nth_value": up_nth_value,
            "dn_mean": dn_mean,
            "up_mean": up_mean,
            "concurrency": concurrent["site"].size})


def get_progress(evaluated_tests):

    res = evaluated_tests.set_index(
            "timestamp").resample("D").apply(summary).reset_index()
    return(res.set_index("timestamp"))


def get_vsats(
        evaluated_tests, tickets=pd.DataFrame(), locations=pd.DataFrame()):

    res = evaluated_tests.groupby("site_code").apply(summary).reset_index()

    if not tickets.empty and not locations.empty:

        start = evaluated_tests["timestamp"].min()
        end = evaluated_tests["timestamp"].max()
        tmp_tickets = filter_tickets(tickets, locations, start, end)
        tmp_tickets['dn_time'] = tmp_tickets['end'] - tmp_tickets['start']
        tmp_tickets['dn_time'] = tmp_tickets['dn_time'].fillna(timedelta(0))
        
        def calc_dn_time(tickets):

            return pd.Series({
                    "total_dn_time": tickets["dn_time"].sum()})
            
        tmp_tickets = tmp_tickets.groupby("site_code").apply(
                calc_dn_time).reset_index()

        res = pd.merge(res, tmp_tickets, on="site_code", how="left")

        def check_valid_sites(row):
            if row["succeeded"] < 30:
                if row["total_dn_time"] >= timedelta(1):
                    if row["succeeded"] < 15:
                        return "> 24 hr & < 15 tests"
                    else:
                        return "valid"
                else:
                    return "< 30 tests"
            else:
                return "valid"

        res["validity"] = res.apply(
                lambda row: check_valid_sites(row), axis=1)

    return(res.set_index("site_code"))


def get_hours(evaluated_tests):

    res = evaluated_tests.groupby("hour").apply(summary).reset_index()
    return(res.set_index("hour"))


def filter_tests(evaluated_tests, **kwargs):

    tmp_df = evaluated_tests
    for key, value in kwargs.items():
        if key == "date":
            tmp_df = tmp_df[tmp_df[
                    "timestamp"].dt.date == datetime.strptime(
                            value, "%Y-%m-%d").date()]
        elif key == "passing":
            tmp_df = tmp_df[tmp_df["pass"] == value]
        else:
            tmp_df = tmp_df[tmp_df[key] == value]
    return tmp_df


def remove_locations(evaluated_tests, remove_list):
    
    for item in remove_list:
        evaluated_tests = evaluated_tests[
                evaluated_tests["site_code"] != item]

    return evaluated_tests


def build_report(tests, tickets, locations, path, remove_list=None):
    
    with pd.ExcelWriter(path) as writer:
        for profile in tests["profile"].unique():
            tmp_tests = filter_tests(tests, profile=profile)

            profile_suffix = ""
            profile_suffix += profile.split("-")[0].split(":")[1]
            profile_suffix += "X"
            profile_suffix += profile.split("-")[1].split(":")[1]

            profile_progress = get_progress(tmp_tests)
            profile_vsats = get_vsats(tmp_tests, tickets, locations)

            succeeded_tests = filter_tests(tmp_tests, res="succeeded")
            profile_hours = get_hours(succeeded_tests)

            dn_br = pd.pivot_table(
                   succeeded_tests, values="dn_br", index="date", 
                   columns="site_code", aggfunc=np.max, fill_value=np.nan)
            up_br = pd.pivot_table(
                   succeeded_tests, values="up_br", index="date", 
                   columns="site_code", aggfunc=np.max, fill_value=np.nan)

            count = pd.pivot_table(
                   succeeded_tests, values="site", index="date", 
                   columns="site_code", aggfunc="count", 
                   fill_value=np.nan)

            profile_progress.to_excel(
                   writer, sheet_name="Progress " + profile_suffix)
            profile_vsats.to_excel(
                   writer, sheet_name="VSATs " + profile_suffix)
            profile_hours.to_excel(
                   writer, sheet_name="Hours " + profile_suffix)
            dn_br.to_excel(writer, sheet_name="Download " + profile_suffix)
            up_br.to_excel(writer, sheet_name="Upload " + profile_suffix)
            count.to_excel(writer, sheet_name="Count " + profile_suffix)
                       
        general_progress = get_progress(tests)
        general_progress.to_excel(writer, sheet_name="General Progress")
        tests.to_excel(writer, sheet_name="All Tests")

        if remove_list:
            pd.Series(remove_list).to_excel(writer, sheet_name="Remove List")

        return path
