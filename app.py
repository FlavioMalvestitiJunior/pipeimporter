from openpyxl.utils.cell import column_index_from_string

import re
import json
import gspread
from datetime import datetime
import mysql.connector

gc = gspread.service_account('./secret.json')
sh = gc.open("[NOVO] Pipe Integração + Faturamento - NEEMU/CHAORDIC")

plataformas_file = "/opt/statics/plataformas.json"
produtos_file = "/opt/statics/produtos.json"
mysqlconfig = "/opt/mysqlconfig.json"
databaseFileSheets = "/opt/output/sql2import.sql"
premissas = 'PREMISSAS'
pipe = 'PIPE 2019'

trucate = "SET FOREIGN_KEY_CHECKS = 0;\nTRUNCATE table atlas.ImplantationTypes;\nTRUNCATE table atlas.IntegrationHistories;\nTRUNCATE table atlas.Integrations;\nTRUNCATE table atlas.IntegrationStatuses;\nTRUNCATE table atlas.IntegrationSuspensions;\nTRUNCATE table atlas.IntegrationTypes;\nTRUNCATE table atlas.ProductSells;\nTRUNCATE table atlas.Sells;\nTRUNCATE table atlas.SellStages;\nTRUNCATE table atlas.Tams;\nSET FOREIGN_KEY_CHECKS = 1;\n\n"
implantation_type_sql = "insert into atlas.ImplantationTypes (id, name) values (\"%s\",\"%s\");\n"
tam_sql = "insert into atlas.Tams (id, name) values (\"%s\",\"%s\");\n"
integration_statuses_sql = "insert into atlas.IntegrationStatuses (id, name) values (\"%s\",\"%s\");\n"
integration_type_sql = "insert into atlas.IntegrationTypes (id, name) values (\"%s\",\"%s\");\n"
sell_stages_sql = "insert into atlas.SellStages (id, label) values (\"%s\",\"%s\");\n"
sells_sql = "insert into atlas.Sells (id,Hunter,ClientApikey,platformProviderId) values (%s,%s,%s,%s);\n"
product_sells_sql = "insert into atlas.ProductSells (id, Name, ProductId, SignatureDate, Cpa, BillingConditionManager, BillingConditionRevenues, CommercialMonitoring,RecurrentBilling, RecurrentBillingObs,RnrTotalValue,RnrServiceObs,ImplantationTypeId,SellId,SellStageId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s);\n"
integrations_sql = "insert into atlas.Integrations (id,KickoffDate,ClientInitialDate,ActivationDate,EstimatedActivationDate,RealActivationDate,CsTransferDate,RealTransferDate,ActivationGoalDate,KickoffSignatureInterval,IntegrationTime,ApiLiberationTime,RevenuesSignatureInterval,IntegrationStatusId,IntegrationTypeId,TamId,ProductSellId) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);\n"
integrations_histories_sql = "insert into atlas.IntegrationHistories (id,HistoryDate, Description, IntegrationId) values (%s,%s,%s,%s);\n"
integrations_integration_suspensions_sql = "insert into atlas.IntegrationSuspensions (id,InitialDate,EndDate,Reason, IntegrationId) values (%s,%s,%s,%s,%s);\n"


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
    if type(cell) == str:
        value = cell
    else:
        value = cell.value

    if value is not None:
        return value.lower()
    return ''


def get_implantation_types():
    sqlfs = open(databaseFileSheets, "w+")
    wss = sh.worksheet(premissas)
    int_types = {}
    i = 1

    sqlfs.write("#trucate\n")
    sqlfs.write(trucate)
    sqlfs.write("# Implantation Types\n")
    rows = wss.get_all_records()

    for row in rows:
        cell = row.get('TIPO IMPLANTAÇÃO')

        if cell == '':
            break

        int_types[cell.lower()] = {"id": i, "value": cell}
        i += 1

    for value in int_types.values():
        sqlfs.write(implantation_type_sql % (value.get('id'), value.get('value')))

    return int_types


def get_column(column):
    idx = column_index_from_string(column.upper())
    return idx - 1


def get_tams():
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(premissas)
    tam = {}
    i = 1

    sqlfs.write("\n# Tams\n")

    rows = wss.get_all_records()
    for row in rows:
        cell = row.get('TAM')

        if cell == '':
            break

        tam[cell.lower()] = {"id": i, "value": cell}
        i += 1

    for value in tam.values():
        sqlfs.write(tam_sql % (value.get('id'), value.get('value')))

    return tam


def get_integration_statuses():
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(premissas)
    intstats = {}
    i = 1

    sqlfs.write("\n# Integration status\n")
    rows = wss.get_all_values()
    rows = rows[2:len(rows)-1]
    for row in rows:
        cell = row[get_column('j')]
        if cell == '':
            break

        intstats[cell.lower()] = {"id": i, "value": cell}
        i += 1

    for value in intstats.values():
        sqlfs.write(integration_statuses_sql % (value.get('id'), value.get('value')))

    return intstats


def get_integration_types():
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(premissas)
    int_types = {}
    i = 1

    sqlfs.write("\n# Integration Types\n")

    rows = wss.get_all_records()
    for row in rows:
        cell = row.get('TIPO')

        if cell == '':
            break

        int_types[cell.lower()] = {"id": i, "value": cell}
        i += 1

    for value in int_types.values():
        sqlfs.write(integration_type_sql % (value.get('id'), value.get('value')))

    return int_types


def get_sell_stages():
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(premissas)
    sell_stages = {}
    i = 1

    sqlfs.write("\n# Sell Stages\n")

    rows = wss.get_all_values()
    rows = rows[2:len(rows)-1]
    for row in rows:
        cell = row[get_column('c')]
        if cell == '':
            break

        sell_stages[cell.lower()] = {"id": i, "value": cell}
        i += 1

    for value in sell_stages.values():
        sqlfs.write(sell_stages_sql % (value.get('id'), value.get('value')))

    return sell_stages


def get_plataformas():
    f = open(plataformas_file, 'r')
    return json.load(f)


def get_produtos():
    f = open(produtos_file, 'r')
    return json.load(f)


def mysql_configs():
    f = open(mysqlconfig, 'r')
    return json.load(f)


def get_sells(plataformas):
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(pipe)
    rows = wss.get_all_records()
    sells = {}
    i = 1

    sqlfs.write("\n# Sells\n")

    for row in rows:
        if row.get('CLIENTE/PROSPECT') == '':
            break

        if row.get('CLIENTE/PROSPECT').lower() not in sells.keys():
            sells[row.get('CLIENTE/PROSPECT').lower()] = {
                "id": i,
                "Hunter": null_for_none_and_empty(re.sub(r'[\n\r]','  ', row.get('HUNTER'))),
                "ClientApikey": null_for_none(row.get('CLIENTE/PROSPECT')),
                "platformProviderId": plataformas.get(null_for_none_and_empty(row.get('Plataforma')).lower(), {}).get('id', "null"),
            }
            i += 1

    for value in sells.values():
        sqlfs.write(sells_sql % (value.get('id'), value.get('Hunter'), value.get('ClientApikey'), value.get('platformProviderId')))

    return sells


def get_float(toNumber):
    if toNumber == 'null':
        return toNumber
    toNumber = re.sub(r'\.', '', toNumber)
    toNumber = re.sub(r',', '.', toNumber)
    return toNumber


def get_product_sells(produtos, imp_types, sells, sell_stages):
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(pipe)
    rows = wss.get_all_values()
    rows = rows[1:len(rows)-1]
    prod_sells = {}
    i = 1

    sqlfs.write("\n# Product Sells\n")

    for row in rows:
        if row[1] == '':
            break
        
        prod_sells[i] = {
            "id": i,
            "Name": null_for_none_and_empty(row[6]),
            "ProductId": produtos.get(get_id(row[2]), {}).get('id', "null"),
            "SignatureDate":  parse_date(null_for_none(row[13])),
            "Cpa": coalesce(row[15], ''),
            "BillingConditionManager": coalesce(row[10], ''),
            "BillingConditionRevenues": re.sub(r'[\n\r]',' ', coalesce(row[11], '')).rstrip() ,
            "CommercialMonitoring": re.sub(r'[\n\r]','  ', null_for_none_and_empty(row[19])),
            "RecurrentBilling": get_float(null_for_none_and_empty(row[14])),
            "RecurrentBillingObs": "''",  # ver oq eue é
            "RnrTotalValue": get_float(null_for_none_and_empty(row[18])),
            "RnrServiceObs": null_for_none_and_empty(row[16]),
            "ImplantationTypeId": imp_types.get(get_id(row[3]), {}).get('id', "null"),
            "SellId": sells.get(get_id(row[1]), {}).get('id', "null"),
            "SellStageId": coalesce(sell_stages.get(get_id(row[9]), {}).get('id', None), sell_stages.get('99. outro', {}).get('id', '')),
        }
        i += 1

    for value in prod_sells.values():
        sqlfs.write(product_sells_sql % (value.get('id'),
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
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(pipe)
    rows = wss.get_all_values()
    rows = rows[1:len(rows)-1]
    integrations = {}
    i = 1

    sqlfs.write("\n# Integrations\n")
    for row in rows:
        if row[1] == '':
            break
        integrations[i] = {
            "id": i,
            "KickoffDate":  parse_date(null_for_none_and_empty(row[get_column('ac')])),
            "ClientInitialDate": parse_date(null_for_none_and_empty(row[get_column('ad')])),
            "ActivationDate": parse_date(null_for_none_and_empty(row[get_column('ae')])),
            "EstimatedActivationDate": parse_date(null_for_none_and_empty(row[get_column('ag')])),
            "RealActivationDate": parse_date(null_for_none_and_empty(row[get_column('ah')])),
            "CsTransferDate": parse_date(null_for_none_and_empty(row[get_column('ai')])),
            "RealTransferDate": parse_date(null_for_none_and_empty(row[get_column('aj')])),
            "ActivationGoalDate": parse_date(null_for_none_and_empty(row[get_column('al')])),
            "KickoffSignatureInterval": null_for_none_and_empty(row[get_column('ap')]),  # ver oq eue é
            "IntegrationTime": null_for_none_and_empty(row[get_column('as')]),
            "ApiLiberationTime":  parse_date(null_for_none_and_empty(row[get_column('af')])),
            "RevenuesSignatureInterval": null_for_none_and_empty(row[get_column('au')]),
            "IntegrationStatusId": int_stats.get(get_id(row[get_column('x')]), {}).get('id', "null"),
            "IntegrationTypeId": coalesce(int_types.get(get_id(row[get_column('v')]), {}).get('id', None)),
            "TamId": coalesce(tams.get(get_id(row[get_column('w')]), {}).get('id', None)),
            "ProductSellId": prod_sell.get(i, {}).get('id', "null"),
        }
        i += 1

    for value in integrations.values():
        sqlfs.write(integrations_sql % (value.get('id'),
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
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(pipe)
    rows = wss.get_all_values()
    rows = rows[1:len(rows)-1]
    integrations_histories = {}
    i = 1
    j = 1

    sqlfs.write("\n# Integrations Histories\n")
    for row in rows:
        if row[get_column('b')] == '':
            break

        value = row[get_column('ak')]
        if value == '':
            i += 1
            continue

        for text in value.splitlines():
            integrations_histories[j] = {
                "id": j,
                "HistoryDate":  parse_date("%s/2019" % text[0:5]),
                "Description": null_for_none_and_empty(re.sub('"', '\\"', text)),
                "IntegrationId": i,
            }
            j += 1
        i += 1

    for value in integrations_histories.values():
        sqlfs.write(integrations_histories_sql % (value.get('id'),
                                                  value.get('HistoryDate'),
                                                  value.get('Description'),
                                                  value.get('IntegrationId')))

    return integrations_histories


def get_integrations_suspensions():
    sqlfs = open(databaseFileSheets, "a+")
    wss = sh.worksheet(pipe)
    rows = wss.get_all_values()
    rows = rows[1:len(rows)-1]
    init_datecolumns = ['bb', 'be', 'bh']
    end_datecolumns = ['bc', 'bf', 'bi']
    integrations_suspensions = {}

    i = 1
    j = 1

    sqlfs.write("\n# Integrations Suspensions\n")
    for row in rows:
        k = 0

        if row[get_column('b')] == '':
            break

        for coluna in init_datecolumns:
            value = row[get_column(coluna)]
            if value != '':
                integrations_suspensions[j] = {
                    "id": j,
                    "InitialDate":  parse_date(value),
                    "EndDate":  parse_date(row[get_column(end_datecolumns[k])]),
                    "Reason": re.sub(r'[\n\r]','  ', null_for_none_and_empty(row[get_column('ba')])),
                    "IntegrationId": i,
                }
                j += 1
            k += 1
        i += 1

    for value in integrations_suspensions.values():
        sqlfs.write(integrations_integration_suspensions_sql % (value.get('id'),
                                                                value.get('InitialDate'),
                                                                value.get('EndDate'),
                                                                value.get('Reason'),
                                                                value.get('IntegrationId')))

    return integrations_suspensions


def importToSql():
    cnx = mysql.connector.connect(**mysql_configs())
    cursor = cnx.cursor()
    sqlFile = open(databaseFileSheets)
    for line in sqlFile:
        if not line.startswith('#') and line != '\n':
            print(line.strip())
            cursor.execute(line.strip())
    cnx.commit()
    cursor.close()
    cnx.close()

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
importToSql()
