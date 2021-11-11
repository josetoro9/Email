
"""
Created on Wed Feb 22 14:34:34 2017

@author: NERIVERA
aportes: JOTORO 
         JGVALENC
"""
import pyodbc
import re 
import os
import traceback
from datetime import datetime
import logging
import sys
import io
import time
import json 
from collections import OrderedDict
import inspect

#%%
logger =  logging.getLogger(__name__)
# Funciones este proceso de querys funciona con regular ex por lo tanto
# Es importante no comentar querys en el archivo SQL.
def inic_logger(path, namelog):
    global logger
    logger = logging.getLogger(namelog)
    
    directory = '/'.join([path, 'logs/'])
    print(directory) 
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    
    hdlr = logging.FileHandler(directory + namelog + '.log')
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s] :%(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)

# Mensajes en print y en logger
def print_(mensaje):
    print(mensaje)
    logger.info(mensaje)

def test_log():
    global logger
    logger.error('ESTO ES UNA PRUEBA')

int_rx = re.compile( '^(?:0*$|(?!0)[-+]?[0-9]*$)' )
createt_rx = '(create)\s*(table)\s*((if)\s*(not)\s*(exists)\s*)?(\w)+\.(\w)+[^;]+(;)'
table_rx ='\s*?(\w)+\.(\w)+\s*' 
dropt_rx = '(drop)\s*(table)\s*((if)\s*(exists)\s*)?(\w)+\.(\w)+\s*(purge)\s*?(;)'
truncatet_rx='(truncate)\s*(\w)+\.(\w)+\s*[^;]+(;)'
insert_rx = '(insert)\s*(into)\s*((table)\s*)?(\w)+\.(\w)+\s*(values)?[^;]+(;)'
insert_over = '(insert)\s*(overwrite)\s*((table)\s*)?(\w)+\.(\w)+\s*(values)?[^;]+(;)'
sample_control_rx = '(\-)(\-)(sample_control)(\-)(\-)'
compute_stats_rx='(compute)\s*((incremental)\s*)?(stats)\s*(\w)+\.(\w)+\s*(values)?[^;]+(;)'
alter_table_rx='(alter)\s*(table)\s*(\w)+\.(\w)+\s*(add)\s*((if)\s*(not)\s*(exists)\s*)?(partition)+\s*(values)?[^;]+(;)'
alter_table_drx='(alter)\s*(table)\s*(\w)+\.(\w)+\s*(drop)\s*((if)\s*(exists)\s*)?(partition)+\s*(values)?[^;]+(;)'
refresh_rx='(refresh)\s*(\w)+\.(\w)+\s*[^;]+(;)'
set_rx = 'set\s* +[^;]+(;)'

t_TRASH = "create|table|=|;|\s+|,|\t|\r"
double_rx = re.compile( r'^[-+]?([0-9]*\.[0-9]*((e|E)[-+]?[0-9]+)?'+
        r'|NaN|inf|Infinity)$')
date_rx = re.compile( r'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]$' );
time_rx = re.compile( r'[0-9][0-9]:[0-9][0-9](:[0-9][0-9])?' );
datetime_rx = re.compile(r'^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9](T| ).+')
datetime_rx2 = re.compile(r'\w\w\w \w\w\w \d\d \d\d:\d\d:\d\d \w\w\w \d\d\d\d')

list_sp_char = ['~', ':', "'", '+', '[', '\\', '@', '^', '{', '%', '(', '-', '"', '*', '|', ',', '&', '<', '`', '}', '_', '=', ']', '!', '>', ';', '?', '#', '$', ')', '/']

#%%
# Se revisa la conexion para poder establecerla de forma diferente si se 
# conecta a traves de Windows o linux
def impala_connect(cert):
    trusted_cert = cert
    if os.name == 'nt': # Windows
        CONN_STR =  "Driver=Cloudera ODBC Driver for Impala;Host=impala.bancolombia.corp;" \
           "Port=21050;AuthMech=1;SSL=1;KrbRealm=BANCOLOMBIA.CORP;KrbFQDN=impala.bancolombia.corp;KrbServiceName=impala;" \
           "TrustedCerts={trusted_cert}".format(trusted_cert = trusted_cert)
        print_("Se identifica ejecu cion en Windows")
    else:
        CONN_STR = "DSN=impala-prod"  
        print_("Se identifica ejecucion en Linux")
    try:  
        print_("INICIA CONEXION CON IMPALA")
        cn = pyodbc.connect( "DSN=impala-prod" , autocommit = True )
        print_("Conexion exitosa") 
        return cn
    except:
        print_("Error en la conexion.. no se continua ejecucion") 
        sys.exit(1) 



# Obtiene la lista de queries a ejecutar:
def getQueriesList(path, file_name):
    
    
    f = io.open( path + file_name, "r", encoding='utf8')
    content = f.read()
    f.close()
    
    it = re.finditer('|'.join([insert_rx, insert_over, createt_rx, dropt_rx, compute_stats_rx, alter_table_rx, alter_table_drx, truncatet_rx, refresh_rx, set_rx]),  content, re.IGNORECASE)     
    sizeit = 0
    queries = list()
    
    for item in it:
        sizeit += 1
        itg = item.group()
        try: 
            tname = re.search(table_rx, itg).group()
        except:
            tname = "Parameters command"
        tsample = 1 if re.search(sample_control_rx, itg) else 0
        try: 
            re.search(table_rx, itg).group()
        except:
            "Parameters command"
        typeq = itg.partition(' ')[0]
        queries.append([itg, typeq.lower(), tname.strip(), tsample])

    return queries
    
# Ejecuta la lista de queries determinada, crea archivo de muestra 
# ocurrencia errores, intenta de nuevo ejecuciones.
def runQueriesV2(path, file_name, cn):
     
    print_("Inicia lectura de archivo: " + file_name) 
    
    ql = getQueriesList(path, file_name)
    d = [{'query': x[0], 'typeq': x[1], 'tname': x[2], 'tsample': x[3]} for x in ql]
    totalqueries = len(d)
    cursor = cn.cursor()
    print_("Se encontraron: " + repr(totalqueries) + " queries") 
    print_("Para proceder se crea tabla de repositorio de errores.")  

    largo_file=len(file_name)-4
    file_error=file_name[1:largo_file]
    codigo_query = 'create table if not exists proceso_clientes_personas_y_pymes.' + file_error +' (error int) stored as parquet'
    codigo_error_truncate='truncate proceso_clientes_personas_y_pymes.' + file_error
    
    # Introduccion JSON para errores
    data = {}
    data['error'] = 0
    # .json error
    json_error = file_error + '.json'

    
    try:
        with open(os.path.join(path, json_error), 'w') as file:
                json.dump(data, file, indent=4)
    except:
        print_('Imposible crear el json.')

    
    error_creacion=0

    while error_creacion<5:
        try:
            cursor.execute(codigo_query)
            cursor.execute(codigo_error_truncate)
            break
        except:
            time.sleep(10)
            print_('Falla query, repitiendo... (intentos: '+str(error_creacion+1)+') \n')
            print_('\n')
            error_creacion+=1
            continue
    if error_creacion == 5:
        error_msg_lz='Imposible ejecutar el query, es posible que la conexion sea inestable, se crea solamente JSON.'
        raise Exception(error_msg_lz)
        print_(error_msg_lz)
        email(error_msg_lz,path)
        sys.exit(1) 


    print_("INICIA LA EJECUCION DE LAS QUERIES") 


    i = 0
    exito = 0
    errores = ''
    currentTime2 = datetime.now()
    for di in d:    
        
        query = di['query']
        tname = di['tname']
        typeq = di['typeq']
        tsample = di['tsample']
        
        try:
            currentTime = datetime.now()
            ###########################
            atp = 0
            while atp < 3:
                try:
                    cursor.execute(query)
                    break
                except:
                    time.sleep(10)
                    print_('Falla query, repitiendo... (intentos: '+str(atp+1)+') \n')
                    if typeq == 'create':
                        q_del = 'drop table if exists ' + tname + ' purge'
                        cursor.execute(q_del)
                        time.sleep(10)
                        print('\n')
                    atp+=1
                    continue
            if atp == 3:
                error_msg_lz='Imposible ejecutar el query, buscar razon del error. guia: ' + typeq + " " + tname
                print_(error_msg_lz)
                error_mail = 1
                email(error_msg_lz,path,error_mail)
                raise Exception('El proceso se interrumpe')

            ###########################
            print(datetime.now()-currentTime)
            logger.info(str(datetime.now()-currentTime))
            savelogV2(typeq,tname, i)
            i+=1
            
            if typeq in ('insert', 'create'):
                executeConteoV2(tname, cn)
                
                #para las tablas que tienen la bandera --sample_control-- en el sql
                if tsample != 0:
                   executeSampleV2(tname, cn, 20) 
            exito+=1
        except:
            errores = '\n'.join([errores, typeq + ' - ' + tname])

            traceback.print_exc()
            print_("Se encontro un error en el query ")
            print_("Continua la ejecucion ...  ")
            break          
       
    if exito ==  totalqueries:
        print_(str(datetime.now()-currentTime2))
        exito_msg = "Se ejecutaron todas las queries EXITOSAMENTE " + file_name
        print_(exito_msg)
    else:
        print_("Fallo la ejecucion de  " + repr(totalqueries - exito) + " queries, abajo el detalle:")
        print_(errores)
        print_('Error ejecutando: Se crea tabla de respuesta.')

        codigo_query_errores = 'INSERT INTO proceso_clientes_personas_y_pymes.' + file_error +' values '+' (cast(1 as int));'
        compute_errores= 'compute incremental stats proceso_clientes_personas_y_pymes.' + file_error +';'
        data['error'] = 1
        try:
            with open(os.path.join(path, json_error), 'w') as file:
                json.dump(data, file, indent=4)
        except:
            print_('Imposible actualizar el json conteniendo el error.')
        try:
            cursor.execute(codigo_query_errores)
            cursor.execute(compute_errores)
        except:
            print_("Falla la insercion de datos en tablas de error, rutina de errores ejecuta con JSON.")
        sys.exit(1) 
   
#Guarda log de los queries que va ejecutando
def savelogV2(typeq, tname, i):
    print_("query *****  " + repr(i) + " ******  ejecutada")
    print_(typeq + "\t" + tname)


def executeConteoV2(tname, cn):

    cursor = cn.cursor()
        
    ###SAMPLE
    query = """
    select count(*) from %s    
    """
    query = query % (tname)
    try:        
        cursor.execute( query)
        conteo_pre = cursor.fetchone()
        conteo = conteo_pre[0] if conteo_pre else 0
        print_( repr(conteo) + " registros")

    except:
        print_("Encontrar el error")

def executeSampleV2(tname, cn, nrows):
    
    cursor = cn.cursor()
        
    ###CONTEO
    query = """
    select * from %s 
    limit %s
    """
    query = query % (tname, nrows)
    try:        
        cursor.execute( query)
        dataall = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        
        print_( "Muestra de " + repr(nrows) + " de la tabla " + tname)
        
        ncols = 0
        cols_ol = '\t'
        for cl in columns:
            cols_ol = '\t'.join([cols_ol, cl])
            ncols+=1
        
        
        for data in dataall:            
            
            rinfo = ''
            for i in range(0, ncols):
                rinfo = '\t'.join([rinfo, str(data[i]) ])
            cols_ol = '\n'.join([cols_ol, str(rinfo)])
        logger.info( cols_ol)              
        print(cols_ol)
        
    except:
        print_("Encontrar el error")


# ------------- Funcion reemplaza variables -------------------

def replaceVarFile(filename_in, variables, filename_out):
    print('Leyendo archivo: ' + filename_in.split('\\')[-1])
    logger.info('Leyendo archivo: ' + filename_in.split('\\')[-1])
    # Open and read the file as a single buffer
    try:    
        fd = io.open(filename_in, 'r', encoding="utf8")
    except Exception:
        print_('No se puede abrir archivo.')
        return        
    sqlFile = fd.read()
    fd.close()
    # changing variables
    for var in variables:
        # Counting
        print_('Se encontraron ' + str(sqlFile.count(var)) + ' ocurrencias de ' + var)
        sqlFile = sqlFile.replace(var, variables[var])
        print_('Se cambia por ' + variables[var])
    # Saving new file
    print_('Guardando archivo:' + filename_out)
    escritura_replace=0
    while escritura_replace<2:
        try:
            fdestino = io.open(filename_out, 'w', encoding="utf8")
            fdestino.write(sqlFile)
            fdestino.close() 
            break
        except:
            time.sleep(10)
            print_('Falla escritura, repitiendo... (intentos: '+str(escritura_replace+1)+') \n')
            escritura_replace+=1
            continue
    if escritura_replace == 2:
        print_('Imposible crear el archivo. puede usarse archivo anterior')


# Mandar Email cuando esté en windows notificando finalizacion del proceso
def email(mensaje,basePath, error=None, adjunto=None):
    try:
        import win32com.client as win32
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        print_("Inicia lectura de JSON config: ") 
        params_fn = os.path.join(basePath, "config.json")
        params_io = io.open(params_fn)
        params_str = params_io.read()
        params_io.close()
        params = json.loads(params_str , object_pairs_hook=OrderedDict)
        print_("Finalizacion lectura de JSON config cerrando...")
        CodeOwnerMail = params['CodeOwnerMail']
        CodeUserMail = params['CodeUserMail']
        #Email Personalizado, los detinatarios se deben separa por ;
        mail.To = CodeOwnerMail + ";" + CodeUserMail
        if error == 1:
            mail.Subject = '📰 Error, leer contenido!'
        else:
            mail.Subject = '📰 Proceso de carga ha finalizado!'
        
        Format = { 'UNSPECIFIED' : 0, 'PLAIN' : 1, 'HTML' : 2, 'RTF'  : 3}
        mail.BodyFormat = Format['HTML']
        try:  
            if error == 1:
                http = 'mailError.html'
            else:
                http = 'mail.html'
            fd = io.open(os.path.join(os.path.join(basePath,'httpBodyMail'),http), 'r', encoding="utf8")    
        except Exception:
            print('No se puede abrir archivo.')
            return        
        mensajehttp = fd.read()
        fd.close()
        mensajehttp = mensajehttp.replace("{mensaje}", mensaje)
        mail.HTMLBody = mensajehttp
        if adjunto is None:
            mail.Send()
        else:
            mail.Attachments.Add(Source=adjunto)
            mail.Send()
        # end if
        print_('Email de finalizacion del proceso enviado')
    except:
        print_('Ya que se ejecuta en OS diferente a Windows no se envia mensaje de finalizacion.')
   
    


#%%
        