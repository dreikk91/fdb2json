import json
import logging
import os
import re
import time
import copy
from datetime import datetime
import fdb
import yaml

logging.basicConfig(filename='export_phoenix_employee.log',
                    format='%(asctime)s-%(process)d-%(levelname)s-%(message)s', level=logging.DEBUG)
logging.info("Starting program")
logging.info("Try to open config")

try:
    with open('export_dnb_employee_config.yaml') as f:
        yaml_config = yaml.safe_load(f)
        logging.info("Config opened successful")
except FileNotFoundError:
    logging.info("Can't open config, generating new")
    path_to_db = 'D:\\Venbest\\DanubePro\\Data\\dpc2.fdb'
    to_yaml = { 'path_to_db': path_to_db }

    with open('export_dnb_employee_config.yaml', 'w') as f:
        yaml.dump(to_yaml, f, default_flow_style=False)

    with open('export_dnb_employee_config.yaml') as f:
        yaml_config = yaml.safe_load(f)

json_exemple = {
    "export_date": "2020-8-15",
    "type": "users",
    "data": [
        {
            "email": "",
            "last_name": "_",
            "first_name": "Денис",
            "middle_name": "_",
            "role": "IN_CHARGE",
            "images": [
                None
            ],
            "phone_numbers": [
                {
                    "active": True,
                    "number": "+38 (093) 111-11-11"
                }
            ]
        }
    ]
}


usercount = 0
json_user = copy.deepcopy(json_exemple['data'][0])
json_exemple['export_date'] = datetime.now().strftime("%Y-%m-%d")
json_phone_numbers = copy.deepcopy(json_user['phone_numbers'][0])
print(yaml_config['path_to_db'])
conn = fdb.connect(
    dsn=yaml_config['path_to_db'],
    user='SYSDBA', password='idonotcare',
    # necessary for all dialect 1 databases
    charset='WIN1251'  # specify a character set for the connection
)
cursor = conn.cursor()


cursor.execute("""
                SELECT e.ID, e.PHONE_HOME, e.PHONE_MOBILE, o.NAME
                FROM EMPLOYEE e 
                INNER join OBJECTS o ON e.ID = o.ID """)

rows = cursor.fetchmany(100000)
# print(list(set(rows)))


def remove_duplicates(listy):
    new_listy = []
    for i in listy:
        if i not in new_listy:
            new_listy.append(i)
    return new_listy

def remove_extra_characters(number):
    new_number = re.sub(r'[^0-9+]+', r'',number)
    return new_number

def format_phone_number(number):
    new_phone_number = '%s (%s) %s-%s-%s' % number[0:1], number[2:5], number[6:8], number[9:10], number[11:12]
    print(new_phone_number)
    return new_phone_number


newrows = remove_duplicates(rows)
for row in newrows:
    print(row)
    home_phone = None
    mobile_number = None
    # print(row[4][0])
    json_phone_numbers = {}
    json_phone_numbers.clear()
    if usercount > 0:
        json_exemple['data'].insert(copy.deepcopy(usercount), copy.deepcopy(json_user))
        json_exemple['data'][usercount]['phone_numbers'].clear()
        json_exemple['data'][usercount]['phone_numbers'].insert(0, copy.deepcopy(json_phone_numbers))
    json_phone_numbers = copy.deepcopy(json_user['phone_numbers'][0])
    try:
        username = row[3].split()
    except AttributeError as err:
        logging.info(err)
    try:
        home_phone = copy.deepcopy(remove_extra_characters(row[1]))
    except:
        print("error")
    try:
        mobile_number = copy.deepcopy(remove_extra_characters(row[2]))
    except:
        logging.info("error")
    try:
        json_user['last_name'] = copy.deepcopy(username[0])
    except IndexError as err:
        logging.info(err)
        json_user['last_name'] = copy.deepcopy('_')
    except KeyError as err:
        logging.info(err)
        json_user['last_name'] = copy.deepcopy('_')
    try:
        json_user['first_name'] = copy.deepcopy(username[1])
    except IndexError as err:
        logging.info(err)
        json_user['first_name'] = copy.deepcopy('_')
    except KeyError as err:
        logging.info(err)
        json_user['first_name'] = copy.deepcopy('_')
    try:
        json_user['middle_name'] = copy.deepcopy(username[-1])
    except IndexError as err:
        logging.info(err)
        json_user['middle_name'] = copy.deepcopy('_')
    except KeyError as err:
        logging.info(err)
        json_user['middle_name'] = copy.deepcopy('_')
    json_exemple['data'][usercount].update(copy.deepcopy(json_user))
    # json_exemple['data'][usercount]['phone_numbers'].insert(0, copy.deepcopy(json_phone_numbers))

    phone_count = 0

    if str(home_phone) != None:
        # json_exemple['data'][usercount]['phone_numbers'].insert(0, copy.deepcopy(json_phone_numbers))
        json_exemple['data'][usercount]['phone_numbers'][0].update({'active' : False})
        try:
            if str(home_phone)[0] == '3':
                json_exemple['data'][usercount]['phone_numbers'][0]['number'] = copy.deepcopy('+' + str(home_phone))
            elif str(home_phone)[0] == '2':
                json_exemple['data'][usercount]['phone_numbers'][0]['number'] = copy.deepcopy('+38032' + str(home_phone))
            elif str(home_phone)[0] == '0':
                json_exemple['data'][usercount]['phone_numbers'][0]['number'] = copy.deepcopy('+38' + str(home_phone))
            else:
                json_exemple['data'][usercount]['phone_numbers'][0]['number'] = ''

        except IndexError as err:
            logging.info(err)
            json_exemple['data'][usercount]['phone_numbers'][0]['number'] = ''

        except TypeError as err:
            logging.info(err)
            logging.info("error")
        phone_count += 1
    if str(mobile_number) != None:
        json_exemple['data'][usercount]['phone_numbers'].insert(1, copy.deepcopy(json_phone_numbers))
        json_exemple['data'][usercount]['phone_numbers'][1].update({'active' : True})
        try:
            if str(mobile_number)[0] == '3':
                json_exemple['data'][usercount]['phone_numbers'][1]['number'] = copy.deepcopy('+' + str(mobile_number))
            elif str(mobile_number)[0] == '2':
                json_exemple['data'][usercount]['phone_numbers'][1]['number'] = copy.deepcopy('+38032' + str(mobile_number))
            elif str(mobile_number)[0] == '0':
                json_exemple['data'][usercount]['phone_numbers'][1]['number'] = copy.deepcopy('+38' + str(mobile_number))
            else:
                json_exemple['data'][usercount]['phone_numbers'][1]['number'] = ''
        except IndexError as err:
            logging.info(err)
            json_exemple['data'][usercount]['phone_numbers'][1]['number'] = ''
            logging.info("error")
        except TypeError as err:
            logging.info(err)

    if json_exemple['data'][usercount]['phone_numbers'][1]['number'] == '':
        json_exemple['data'][usercount]['phone_numbers'][0].update({ 'active': True })
        json_exemple['data'][usercount]['phone_numbers'][1].update({ 'active': False })
        json_exemple['data'][usercount]['phone_numbers'].pop(1)
    elif json_exemple['data'][usercount]['phone_numbers'][0]['number'] == '':
        json_exemple['data'][usercount]['phone_numbers'][0].update({ 'active': False })
        json_exemple['data'][usercount]['phone_numbers'][1].update({ 'active': True })
        json_exemple['data'][usercount]['phone_numbers'].pop(0)
    else:
        json_exemple['data'][usercount]['phone_numbers'][0].update({ 'active': False })
        json_exemple['data'][usercount]['phone_numbers'][1].update({ 'active': True })

    usercount +=1

json_result = json.dumps(json_exemple, ensure_ascii=False, indent=4).encode('utf8').decode('utf8')
with open('converted_sql_dnb.json', 'w', encoding='utf8') as outfile:
    outfile.write(json_result)
print('Total exported employees %s' % usercount)