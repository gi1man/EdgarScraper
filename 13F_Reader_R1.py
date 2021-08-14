#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul  3 23:55:26 2021

@author: gilcooper
"""

from sec_edgar_downloader import Downloader
import os
import csv
import re
import psycopg2


form = '13F-HR'
ciks = '1423053'



def get_files(filing, CIK):
    save_path = "/Volumes/Seagate/FinanceData/13F"
    dl = Downloader(save_path)
    dl.get(filing, '0001350694', amount=26, download_details=False)
#    dl.get("8-K", "AAPL", amount=1)
#    dl.get("13F-HR", "1423053", amount=5)
    CIK = CIK.lstrip("0")
#    files = os.listdir("Volumes/Seagate/FinanceData/13F/sec_edgar_filings/" + CIK + "/" + filing)
    

df = get_files(form, ciks)
    
rootdir = "/Volumes/Seagate/FinanceData/13F"


conn = psycopg2.connect(
    host="localhost",
    database="13F",
    user="postgres",
    password="snowden")

for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        curfile =  (os.path.join(subdir, file))
        with open(curfile, 'r') as reader:
            for line in reader: 
                 
                m=(re.search('ACCESSION NUMBER.\s*(\S.*)', line))
                if m is not None:
                    v1 = m.group(1)         
                m=(re.search('CONFORMED PERIOD OF REPORT.\s*(\S.*)', line))
                if m is not None:
                    v2 = m.group(1)         
                m=(re.search('FILED AS OF DATE.\s*(\S.*)', line))
                if m is not None:
                    v3 = m.group(1)         
                m=(re.search('DATE AS OF CHANGE.\s*(\S.*)', line))
                if m is not None:
                    v4 = m.group(1)             
                m=(re.search('EFFECTIVENESS DATE.\s*(\S.*)', line))
                if m is not None:
                    v5 = m.group(1)             
                m=(re.search('COMPANY.CONFORMED.NAME.\s*(\S.*)', line))
                if m is not None:
                    v6 = m.group(1)             
                m=(re.search('<nameOfIssuer>(.*)</nameOfIssuer>', line))
                if m is not None:
                    v7 = m.group(1)             
                m=(re.search('<cusip>(.*)</cusip>', line))
                if m is not None:
                    v8 = m.group(1)  
                m=(re.search('<titleOfClass>(.*)</titleOfClass>', line))
                if m is not None:
                    v9 = m.group(1)             
                m=(re.search('<value>(.*)</value>', line))
                if m is not None:
                    v10 = m.group(1)             
                m=(re.search('<sshPrnamt>(.*)</sshPrnamt>', line))
                if m is not None:
                    v11 = m.group(1)             
                m=(re.search('<sshPrnamtType>(.*)</sshPrnamtType>', line))
                if m is not None:
                    v12 = m.group(1)             
                m=(re.search('<investmentDiscretion>(.*)</investmentDiscretion>', line))
                if m is not None:
                    v13 = m.group(1)             
                m=(re.search('<otherManager>(.*)</otherManager>', line))
                if m is not None:
                    v14 = m.group(1)   
                else:
                    v14 = None
                m=(re.search('<Sole>(.*)</Sole>', line))
                if m is not None:
                    v15 = m.group(1)             
                m=(re.search('<Shared>(.*)</Shared>', line))
                if m is not None:
                    v16 = m.group(1)             
                m=(re.search('<None>(.*)</None>', line))
                if m is not None:
                    v17 = m.group(1)             
                m=(re.search('(</infoTable>)', line))
                if m is not None:
                    print(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17) 
                    
                    cursor = conn.cursor()
                    cursor.execute('INSERT INTO public."13F_Archive"("ACCNum", "PeriodEnd", "FiledDate", "ChangedDate", "EffectiveDate", "CompanyName", "Holding", "Cusip", "TitleOfCLass", "Value", "sshPrnAmt", "sshPrnAmtType", "InvestmentDiscretion", "otherManager", "Sole", "Shared", "None") VALUES (%s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)', (v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,v17))
                    conn.commit() # <- We MUST commit to reflect the inserted data
                    cursor.close()
                    
                    v7=v8=v9=v10=v11=v12=v13=v14=v15=v16=v17=None  


                
        
                
                
                
      
