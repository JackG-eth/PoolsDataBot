
import json
import requests
import pandas as pd
import schedule
import time
import openpyxl
import xlsxwriter
from openpyxl import load_workbook
from os.path import exists


def write_to_excel(tokenAddress):
    request = requests.get("https://api.tracer.finance/poolsv2/upkeeps?network=42161&poolAddress=" + tokenAddress)
    request_text = request.text
    JSON = json.loads(request_text)
    excel_file = 'data/' + tokenAddress + '.xlsx'

    filterJSON = {
        'timestamp': time.strftime('%Y-%m-%d T %H:%M:%S %Z',time.gmtime(JSON['rows'][0]['blockTimestamp'])),
        'index': int(JSON['rows'][0]['endPrice'])/1e18,
        'ltoken': int(JSON['rows'][0]['longTokenPrice'])/1e06,
        'stoken': int(JSON['rows'][0]['shortTokenPrice'])/1e06,
        
    }

    df = pd.DataFrame(filterJSON, index=[filterJSON['timestamp']]
                    ,columns=['timestamp','index', 'ltoken', 'stoken'])
    
    if exists(excel_file) == False:
        workbook = xlsxwriter.Workbook(excel_file)
        worksheet = workbook.add_worksheet("Time Series Data")
        worksheet.write('A1', 'timestamp') 
        worksheet.write('B1', 'index') 
        worksheet.write('C1', 'ltoken') 
        worksheet.write('D1', 'stoken') 
        workbook.close()

    else:    
        rows = df.values.tolist()
        workbook = load_workbook(excel_file)
        sheet = workbook['Time Series Data']
        for row in rows:
            sheet.append(row)
        workbook.save(excel_file)

if __name__ == "__main__":
    schedule.every(1).hour.do(lambda: write_to_excel('0x3c16b9efe5e4fc0ec3963f17c64a3dcbf7269207'))
    schedule.every(1).hour.do(lambda: write_to_excel('0x6d3fb4aa7ddca8cbc88f7ba94b36ba83ff6ba234'))
    while True:
        schedule.run_pending()
   

