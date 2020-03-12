from openpyxl import load_workbook
from openpyxl.formula.translate import Translator
from openpyxl.utils.cell import coordinate_from_string, column_index_from_string

import re
import json
from datetime import datetime


wb = load_workbook(filename='/opt/input/pipe.xlsx', data_only=True)
plataformas_file = "/opt/statics/plataformas.json"
produtos_file = "/opt/statics/produtos.json"
databaseFile = "/opt/output/teste.sql"
premissas = 'PREMISSAS'
pipe = 'PIPE 2019'

implantation_type_sql = "insert into ImplantationTypes (id, name) values (\"%s\",\"%s\");\n"
tam_sql = "insert into Tams (id, name) values (\"%s\",\"%s\");\n"
integration_statuses_sql = "insert into IntegrationStatuses (id, name) values (\"%s\",\"%s\");\n"
integration_type_sql = "insert into IntegrationTypes (id, name) values (\"%s\",\"%s\");\n"
sell_stages_sql = "insert into SellStages (id, label) values (\"%s\",\"%s\");\n"
sells_sql = "insert into Sells (id,Hunter,ClientApikey,platformProviderId) values (%s,%s,%s,%s);\n"
product_sells_sql = "insert into ProductSells (id, Name, ProductId, SignatureDate, Cpa, BillingConditionManager, BillingConditionRevenues, CommercialMonitoring,RecurrentBilling, RecurrentBillingObs,RnrTotalValue,RnrServiceObs,ImplantationTypeId,SellId,SellStageId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s);\n"
integrations_sql = "insert into Integrations (id,KickoffDate,ClientInitialDate,ActivationDate,EstimatedActivationDate,RealActivationDate,CsTransferDate,RealTransferDate,ActivationGoalDate,KickoffSignatureInterval,IntegrationTime,ApiLiberationTime,RevenuesSignatureInterval,IntegrationStatusId,IntegrationTypeId,TamId,ProductSellId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n"
integrations_histories_sql = "insert into IntegrationHistories (id,HistoryDate, Description, IntegrationId) values (%s,%s,%s,%s);\n"
integrations_integration_suspensions_sql = "insert into IntegrationSuspensions (id,InitialDate,EndDate,Reason, IntegrationId) values (%s,%s,%s,%s,%s);\n"


def null_for_none(value):
    if value is None:
        return 'null'
    return "\"%s\"" % value


def null_for_none_and_empty(value):
    if value is None or value == '':
        return 'null'
    return "\"%s\"" % value


def null_for_none_number(value):
    if value is None:
        return 'null'
    return float(value)


def coalesce(*arg):
    for el in arg:
        if el is not None:
            return '"%s"' % el

    return 'null'


def parse_date(datestr, pattern="%d/%m/%Y", stopcall=False):
    if datestr == "" or datestr is None or datestr == 'null':
        return "null"
    datestr = re.sub('"', '', datestr)
    try:
        dd = datetime.strptime(datestr, pattern)
    except Exception as err:
        if stopcall is not True:
            dd = parse_date(datestr, '%Y-%m-%d', True)
        else:
            dd = "null"
    if type(dd) == str:
        return dd

    dd = dd.date()
    return '"%s"' % dd.strftime('%Y-%m-%d')


def get_id(cell):
    value = cell.value
    if value is not None:
        return value.lower()
    return ''


def get_implantation_types():
    sqlf = open(databaseFile, "w+")
    ws = wb[premissas]
    int_types = {}
    i = 1

    sqlf.write("# Implantation Types\n")

    for row in ws.iter_rows(min_row=2, min_col=18, max_col=18):
        for cell in row:
            if cell.value is None:
                break
            int_types[cell.value.lower()] = {"id": i, "value": cell.value}
        i += 1

    for value in int_types.values():
        sqlf.write(implantation_type_sql % (value.get('id'), value.get('value')))

    return int_types


def get_column(column):
    idx = column_index_from_string(column.upper()) 
    return idx - 1


def get_tams():
    sqlf = open(databaseFile, "a+")
    ws = wb[premissas]
    tam = {}
    i = 1

    sqlf.write("\n# Tams\n")

    for row in ws.iter_rows(min_row=2, min_col=17, max_col=17):
        for cell in row:
            if cell.value is None:
                break
            tam[cell.value.lower()] = {"id": i, "value": cell.value}
        i += 1

    for value in tam.values():
        sqlf.write(tam_sql % (value.get('id'), value.get('value')))

    return tam


def get_integration_statuses():
    sqlf = open(databaseFile, "a+")
    ws = wb[premissas]
    intstats = {}
    i = 1

    sqlf.write("\n# Integration status\n")

    for row in ws.iter_rows(min_row=3, min_col=10, max_col=10):
        for cell in row:
            if cell.value is None:
                break
            intstats[cell.value.lower()] = {"id": i, "value": cell.value}
        i += 1

    for value in intstats.values():
        sqlf.write(integration_statuses_sql % (value.get('id'), value.get('value')))

    return intstats


def get_integration_types():
    sqlf = open(databaseFile, "a+")
    ws = wb[premissas]
    int_types = {}
    i = 1

    sqlf.write("\n# Integration Types\n")

    for row in ws.iter_rows(min_row=2, min_col=8, max_col=8):
        for cell in row:
            if cell.value is None:
                break
            int_types[cell.value.lower()] = {"id": i, "value": cell.value}
        i += 1

    for value in int_types.values():
        sqlf.write(integration_type_sql % (value.get('id'), value.get('value')))

    return int_types


def get_sell_stages():
    sqlf = open(databaseFile, "a+")
    ws = wb[premissas]
    sell_stages = {}
    i = 1

    sqlf.write("\n# Sell Stages\n")

    for row in ws.iter_rows(min_row=3, min_col=3, max_col=3):
        for cell in row:
            if cell.value is None:
                break
            sell_stages[cell.value.lower()] = {"id": i, "value": cell.value}
        i += 1

    for value in sell_stages.values():
        sqlf.write(sell_stages_sql % (value.get('id'), value.get('value')))

    return sell_stages


def get_plataformas():
    f = open(plataformas_file, 'r')
    return json.load(f)


def get_produtos():
    f = open(produtos_file, 'r')
    return json.load(f)


def get_sells(plataformas):
    sqlf = open(databaseFile, "a+")
    ws = wb[pipe]
    sells = {}
    i = 1

    sqlf.write("\n# Sells\n")

    for row in ws.iter_rows(min_row=2):
        if row[1].value is None:
            break

        if row[1].value.lower() not in sells.keys():
            sells[row[1].value.lower()] = {
                "id": i,
                "Hunter": null_for_none(row[8].value),
                "ClientApikey": null_for_none(row[1].value),
                "platformProviderId": plataformas.get(null_for_none(row[20].value).lower(), {}).get('id', "null"),
            }
            i += 1

    for value in sells.values():
        sqlf.write(sells_sql % (value.get('id'), value.get('Hunter'), value.get('ClientApikey'), value.get('platformProviderId')))

    return sells


def get_product_sells(produtos, imp_types, sells, sell_stages):
    sqlf = open(databaseFile, "a+")
    ws = wb[pipe]
    prod_sells = {}
    i = 1

    sqlf.write("\n# Product Sells\n")

    for row in ws.iter_rows(min_row=2):
        if row[1].value is None:
            break
        prod_sells[i] = {
            "id": i,
            "Name": null_for_none(row[6].value),
            "ProductId": produtos.get(get_id(row[2]), {}).get('id', "null"),
            "SignatureDate":  parse_date(null_for_none(row[13].value)),
            "Cpa": coalesce(row[15].value, ''),
            "BillingConditionManager": coalesce(row[10].value, ''),
            "BillingConditionRevenues": re.sub('\n', '', coalesce(row[11].value, '')),
            "CommercialMonitoring": re.sub('\n', '', coalesce(row[19].value)),
            "RecurrentBilling": null_for_none(row[14].value),
            "RecurrentBillingObs": "''",  # ver oq eue é
            "RnrTotalValue": null_for_none(row[18].value),
            "RnrServiceObs": null_for_none(row[16].value),
            "ImplantationTypeId": imp_types.get(get_id(row[3]), {}).get('id', "null"),
            "SellId": sells.get(get_id(row[1]), {}).get('id', "null"),
            "SellStageId": coalesce(sell_stages.get(get_id(row[9]), {}).get('id', None), sell_stages.get('99. outro', {}).get('id', '')),
        }
        i += 1

    for value in prod_sells.values():
        sqlf.write(product_sells_sql % (value.get('id'),
                                        value.get('Name'),
                                        value.get('ProductId'),
                                        value.get('SignatureDate'),
                                        value.get('Cpa'),
                                        value.get('BillingConditionManager'),
                                        value.get('BillingConditionRevenues'),
                                        value.get('CommercialMonitoring'),
                                        value.get('RecurrentBilling'),
                                        value.get('RecurrentBillingObs'),
                                        value.get('RnrTotalValue'),
                                        value.get('RnrServiceObs'),
                                        value.get('ImplantationTypeId'),
                                        value.get('SellId'),
                                        value.get('SellStageId')))

    return prod_sells


def get_integrations(prod_sell, int_stats, int_types, tams):
    sqlf = open(databaseFile, "a+")
    ws = wb[pipe]
    integrations = {}
    i = 1

    sqlf.write("\n# Integrations\n")
    for row in ws.iter_rows(min_row=2):
        if row[1].value is None:
            break
        integrations[i] = {
            "id": i,
            "KickoffDate":  parse_date(null_for_none(row[get_column('ac')].value)),
            "ClientInitialDate": parse_date(null_for_none(row[get_column('ad')].value)),
            "ActivationDate": parse_date(null_for_none(row[get_column('ae')].value)),
            "EstimatedActivationDate": parse_date(null_for_none(row[get_column('ag')].value)),
            "RealActivationDate": parse_date(null_for_none(row[get_column('ah')].value)),
            "CsTransferDate": parse_date(null_for_none(row[get_column('ai')].value)),
            "RealTransferDate": parse_date(null_for_none(row[get_column('aj')].value)),
            "ActivationGoalDate": parse_date(null_for_none(row[get_column('al')].value)),
            "KickoffSignatureInterval": null_for_none_and_empty(row[get_column('ap')].value),  # ver oq eue é
            "IntegrationTime": null_for_none_and_empty(row[get_column('as')].value),
            "ApiLiberationTime":  parse_date(null_for_none(row[get_column('af')].value)),
            "RevenuesSignatureInterval": null_for_none_and_empty(row[get_column('au')].value),
            "IntegrationStatusId": int_stats.get(get_id(row[get_column('x')]), {}).get('id', "null"),
            "IntegrationTypeId": coalesce(int_types.get(get_id(row[get_column('v')]), {}).get('id', None)),
            "TamId": coalesce(tams.get(get_id(row[get_column('w')]), {}).get('id', None)),
            "ProductSellId": prod_sell.get(i, {}).get('id', "null"),
        }
        i += 1

    for value in integrations.values():
        sqlf.write(integrations_sql % (value.get('id'),
                                       value.get('KickoffDate'),
                                       value.get('ClientInitialDate'),
                                       value.get('ActivationDate'),
                                       value.get('EstimatedActivationDate'),
                                       value.get('RealActivationDate'),
                                       value.get('CsTransferDate'),
                                       value.get('RealTransferDate'),
                                       value.get('ActivationGoalDate'),
                                       value.get('KickoffSignatureInterval'),
                                       value.get('IntegrationTime'),
                                       value.get('ApiLiberationTime'),
                                       value.get('RevenuesSignatureInterval'),
                                       value.get('IntegrationStatusId'),
                                       value.get('IntegrationTypeId'),
                                       value.get('TamId'),
                                       value.get('ProductSellId')))

    return integrations


def get_integrations_histories():
    sqlf = open(databaseFile, "a+")
    ws = wb[pipe]
    integrations_histories = {}
    i = 1
    j = 1

    sqlf.write("\n# Integrations Histories\n")
    for row in ws.iter_rows(min_row=2):
        if row[get_column('b')].value is None:
            break

        value = row[get_column('ak')].value
        if value is None:
            i += 1
            continue

        for text in value.splitlines():
            integrations_histories[j] = {
                "id": j,
                "HistoryDate":  parse_date("%s/2019" % text[0:5]),
                "Description": null_for_none(re.sub('"', '\\"', text)),
                "IntegrationId": i,
            }
            j += 1
        i += 1

    for value in integrations_histories.values():
        sqlf.write(integrations_histories_sql % (value.get('id'),
                                                 value.get('HistoryDate'),
                                                 value.get('Description'),
                                                 value.get('IntegrationId')))

    return integrations_histories


def get_integrations_suspensions():
    sqlf = open(databaseFile, "a+")
    ws = wb[pipe]
    init_datecolumns = ['bb', 'be', 'bh']
    end_datecolumns = ['bc', 'bf', 'bi']
    integrations_suspensions = {}

    i = 1
    j = 1

    sqlf.write("\n# Integrations Suspensions\n")
    for row in ws.iter_rows(min_row=2):
        k = 0

        if row[get_column('b')].value is None:
            break

        for coluna in init_datecolumns:
            value = row[get_column(coluna)].value
            if value is not None:
                integrations_suspensions[j] = {
                    "id": j,
                    "InitialDate":  parse_date(value),
                    "EndDate":  parse_date(row[get_column(end_datecolumns[k])].value),
                    "Reason": re.sub('\n', '',null_for_none(row[get_column('ba')].value)),
                    "IntegrationId": i,
                }
                j += 1
            k += 1
        i += 1

    for value in integrations_suspensions.values():
        sqlf.write(integrations_integration_suspensions_sql % (value.get('id'),
                                                               value.get('InitialDate'),
                                                               value.get('EndDate'),
                                                               value.get('Reason'),
                                                               value.get('IntegrationId')))

    return integrations_suspensions

imp_types = get_implantation_types()
tam = get_tams()
int_stats = get_integration_statuses()
int_types = get_integration_types()
sell_stages = get_sell_stages()
plataformas = get_plataformas()
produtos = get_produtos()
sells = get_sells(plataformas)
produc_sells = get_product_sells(produtos, imp_types, sells, sell_stages)
integrations = get_integrations(produc_sells, int_stats, int_types, tam)
integrations_histories = get_integrations_histories()
integrations_suspensions = get_integrations_suspensions()
